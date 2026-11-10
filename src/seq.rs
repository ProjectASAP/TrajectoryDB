//! seq_match: MATCH_RECOGNIZE-style row-pattern matching as an aggregate.
//!
//!   SELECT session_id, seq_match(step_id, step_id, sym, 'A X{0,2} M') AS matches FROM ... GROUP BY session_id
//!
//!   PARTITION BY -> GROUP BY; ORDER BY -> first arg (ord); DEFINE -> a CASE expression producing `sym`;
//!   PATTERN -> regex over symbol names: identifiers are symbols, `.` is any row (incl. NULL sym),
//!   plus ( ) | * + ? {m,n}. Returns list<list<int64>>: the `id`s of each match's rows, in order.
//!   Matches are leftmost and non-overlapping (MATCH_RECOGNIZE's AFTER MATCH SKIP PAST LAST ROW).
// ponytail: buffers each partition and sorts in evaluate(); does not exploit an input sort order yet (C5).

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int64Type};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{Accumulator, AggregateUDF, Signature, SimpleAggregateUDF, Volatility};
use std::collections::HashMap;
use std::sync::Arc;

/// Compile `pattern` over symbol names into a regex over one char per row; `syms` assigns chars.
fn compile(pattern: &str, syms: &mut HashMap<String, char>) -> Result<regex::Regex> {
    let mut re = String::new();
    let mut it = pattern.chars().peekable();
    while let Some(c) = it.next() {
        if c.is_ascii_alphabetic() || c == '_' {
            let mut name = c.to_string();
            while let Some(&n) = it.peek() { if n.is_ascii_alphanumeric() || n == '_' { name.push(n); it.next(); } else { break } }
            let n = syms.len() as u32;
            let ch = *syms.entry(name).or_insert_with(|| char::from_u32(0xE001 + n).unwrap());
            re.push_str(&regex::escape(&ch.to_string()));
        } else if c.is_whitespace() {
        } else if "()|*+?{},.0123456789".contains(c) {
            re.push(c);
        } else {
            return exec_err!("seq_match: unexpected {c:?} in pattern {pattern:?}");
        }
    }
    regex::Regex::new(&re).or_else(|e| exec_err!("seq_match: bad pattern {pattern:?}: {e}"))
}

type Compiled = Arc<(regex::Regex, HashMap<String, char>)>;

/// Compiled patterns, shared across partitions (one accumulator per group would otherwise recompile per group).
fn compiled(pattern: &str) -> Result<Compiled> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<HashMap<String, Compiled>>> = std::sync::OnceLock::new();
    let mut c = CACHE.get_or_init(Default::default).lock().unwrap();
    if let Some(x) = c.get(pattern) { return Ok(x.clone()) }
    let mut syms = HashMap::new();
    let x: Compiled = Arc::new((compile(pattern, &mut syms)?, syms));
    c.insert(pattern.to_string(), x.clone());
    Ok(x)
}

const NONE: char = '\u{E000}'; // symbol not named in the pattern (or NULL): matched only by `.`

/// Leftmost non-overlapping matches; `s` has one pattern char per row, `ids` the row ids in the same order.
fn find(re: &regex::Regex, s: &str, ids: &[i64]) -> Vec<Vec<i64>> {
    // every row is one char in U+E000..U+F8FF, i.e. exactly 3 UTF-8 bytes
    re.find_iter(s).filter(|m| !m.is_empty()).map(|m| ids[m.start() / 3..m.end() / 3].to_vec()).collect()
}

/// Convenience over (id, sym) rows already in order (unit tests).
#[cfg(test)]
pub fn matches(rows: &[(i64, Option<&str>)], pattern: &str) -> Result<Vec<Vec<i64>>> {
    let x = compiled(pattern)?;
    let s: String = rows.iter().map(|(_, y)| y.and_then(|y| x.1.get(y).copied()).unwrap_or(NONE)).collect();
    Ok(find(&x.0, &s, &rows.iter().map(|r| r.0).collect::<Vec<_>>()))
}

/// Per-group state: (ord, id, pattern char) rows. Symbols are mapped to chars on arrival, so no per-row strings.
#[derive(Debug, Default)]
struct Acc { rows: Vec<(i64, i64, char)>, pattern: Option<String>, re: Option<Compiled> }

fn ints(a: &ArrayRef) -> Result<Vec<Option<i64>>> {
    Ok(cast(a, &DataType::Int64)?.as_primitive::<Int64Type>().iter().collect())
}

impl Acc {
    fn set_pattern(&mut self, pat: &ArrayRef) -> Result<()> {
        if self.re.is_some() { return Ok(()) }
        let pat = cast(pat, &DataType::Utf8)?;
        if let Some(p) = pat.as_string::<i32>().iter().flatten().next() {
            self.re = Some(compiled(p)?);
            self.pattern = Some(p.to_string());
        }
        Ok(())
    }
    /// `to_char` turns one sym value into its pattern char.
    fn push(&mut self, ord: &ArrayRef, id: &ArrayRef, sym: &ArrayRef, to_char: impl Fn(Option<&str>) -> char) -> Result<()> {
        let (o, i, sym) = (ints(ord)?, ints(id)?, cast(sym, &DataType::Utf8)?);
        let sym = sym.as_string::<i32>();
        for k in 0..o.len() {
            if let (Some(o), Some(i)) = (o[k], i[k]) { self.rows.push((o, i, to_char(sym.is_valid(k).then(|| sym.value(k))))) }
        }
        Ok(())
    }
}

fn list_of(t: DataType) -> DataType { DataType::List(Arc::new(Field::new_list_field(t, true))) }

impl Accumulator for Acc {
    fn update_batch(&mut self, v: &[ArrayRef]) -> Result<()> {
        self.set_pattern(&v[3])?;
        let Some(re) = self.re.clone() else { return Ok(()) };
        self.push(&v[0], &v[1], &v[2], |y| y.and_then(|y| re.1.get(y).copied()).unwrap_or(NONE))
    }

    /// Partial state carries the already-mapped chars (consistent across partitions: patterns are cached globally).
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let l = |v: Vec<ScalarValue>, t: &DataType| ScalarValue::List(ScalarValue::new_list_nullable(&v, t));
        Ok(vec![
            l(self.rows.iter().map(|r| ScalarValue::Int64(Some(r.0))).collect(), &DataType::Int64),
            l(self.rows.iter().map(|r| ScalarValue::Int64(Some(r.1))).collect(), &DataType::Int64),
            l(self.rows.iter().map(|r| ScalarValue::Utf8(Some(r.2.to_string()))).collect(), &DataType::Utf8),
            ScalarValue::Utf8(self.pattern.clone()),
        ])
    }

    fn merge_batch(&mut self, st: &[ArrayRef]) -> Result<()> {
        self.set_pattern(&st[3])?;
        let (o, i, s) = (st[0].as_list::<i32>(), st[1].as_list::<i32>(), st[2].as_list::<i32>());
        for k in 0..o.len() {
            self.push(&o.value(k), &i.value(k), &s.value(k), |y| y.and_then(|y| y.chars().next()).unwrap_or(NONE))?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let Some(re) = &self.re else { return exec_err!("seq_match: NULL pattern") };
        self.rows.sort_unstable_by_key(|r| r.0);
        let s: String = self.rows.iter().map(|r| r.2).collect();
        let ids: Vec<i64> = self.rows.iter().map(|r| r.1).collect();
        let inner: Vec<ScalarValue> = find(&re.0, &s, &ids).iter().map(|m| {
            ScalarValue::List(ScalarValue::new_list_nullable(&m.iter().map(|x| ScalarValue::Int64(Some(*x))).collect::<Vec<_>>(), &DataType::Int64))
        }).collect();
        Ok(ScalarValue::List(ScalarValue::new_list_nullable(&inner, &list_of(DataType::Int64))))
    }

    fn size(&self) -> usize { std::mem::size_of_val(self) + self.rows.capacity() * std::mem::size_of::<(i64, i64, char)>() }
}

pub fn udaf() -> AggregateUDF {
    // any(4): accumulator casts ord/id to Int64 and sym/pattern to Utf8, so row_number() (UInt64) etc. just work
    let state = [list_of(DataType::Int64), list_of(DataType::Int64), list_of(DataType::Utf8), DataType::Utf8];
    AggregateUDF::from(SimpleAggregateUDF::new_with_signature(
        "seq_match",
        Signature::any(4, Volatility::Immutable),
        list_of(list_of(DataType::Int64)),
        Arc::new(|_| Ok(Box::<Acc>::default())),
        state.into_iter().enumerate().map(|(i, t)| Arc::new(Field::new(format!("seq_match_state_{i}"), t, true))).collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matching() {
        let rows: Vec<_> = "A X M - L A M".split(' ').enumerate().map(|(i, x)| (i as i64 + 1, (x != "-").then_some(x))).collect();
        assert_eq!(matches(&rows, "A .{0,2} M").unwrap(), [vec![1, 2, 3], vec![6, 7]]);
        assert_eq!(matches(&rows, "A .{0,2} M .{0,2} L").unwrap(), [vec![1, 2, 3, 4, 5]]);
        assert_eq!(matches(&rows, "M+").unwrap(), [vec![3], vec![7]]);
        assert_eq!(matches(&rows, "(A|M) (A|M)").unwrap(), [vec![6, 7]]);
        assert_eq!(matches(&rows, "Q").unwrap(), Vec::<Vec<i64>>::new());
        assert!(matches(&rows, "A ;").is_err());
    }
}
