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

/// Leftmost non-overlapping matches over (id, sym) rows already in order.
pub fn matches(rows: &[(i64, Option<&str>)], pattern: &str) -> Result<Vec<Vec<i64>>> {
    let mut syms = HashMap::new();
    let re = compile(pattern, &mut syms)?;
    let none = '\u{E000}';
    let s: String = rows.iter().map(|(_, y)| y.and_then(|y| syms.get(y).copied()).unwrap_or(none)).collect();
    // byte offset -> row index (each row is one 3-byte char)
    let row = |b: usize| s[..b].chars().count();
    Ok(re.find_iter(&s).filter(|m| !m.is_empty()).map(|m| rows[row(m.start())..row(m.end())].iter().map(|r| r.0).collect()).collect())
}

#[derive(Debug, Default)]
struct Acc { rows: Vec<(i64, i64, Option<String>)>, pattern: Option<String> }

fn ints(a: &ArrayRef) -> Result<Vec<Option<i64>>> {
    Ok(cast(a, &DataType::Int64)?.as_primitive::<Int64Type>().iter().collect())
}
fn strs(a: &ArrayRef) -> Result<Vec<Option<String>>> {
    Ok(cast(a, &DataType::Utf8)?.as_string::<i32>().iter().map(|x| x.map(str::to_string)).collect())
}

impl Acc {
    fn push(&mut self, ord: &ArrayRef, id: &ArrayRef, sym: &ArrayRef, pat: &ArrayRef) -> Result<()> {
        let (o, i, s, p) = (ints(ord)?, ints(id)?, strs(sym)?, strs(pat)?);
        for k in 0..o.len() {
            if let (Some(o), Some(i)) = (o[k], i[k]) { self.rows.push((o, i, s[k].clone())) }
            if self.pattern.is_none() { self.pattern = p[k].clone() }
        }
        Ok(())
    }
}

fn list_of(t: DataType) -> DataType { DataType::List(Arc::new(Field::new_list_field(t, true))) }

impl Accumulator for Acc {
    fn update_batch(&mut self, v: &[ArrayRef]) -> Result<()> { self.push(&v[0], &v[1], &v[2], &v[3]) }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let l = |v: Vec<ScalarValue>, t: &DataType| ScalarValue::List(ScalarValue::new_list_nullable(&v, t));
        Ok(vec![
            l(self.rows.iter().map(|r| ScalarValue::Int64(Some(r.0))).collect(), &DataType::Int64),
            l(self.rows.iter().map(|r| ScalarValue::Int64(Some(r.1))).collect(), &DataType::Int64),
            l(self.rows.iter().map(|r| ScalarValue::Utf8(r.2.clone())).collect(), &DataType::Utf8),
            ScalarValue::Utf8(self.pattern.clone()),
        ])
    }

    fn merge_batch(&mut self, st: &[ArrayRef]) -> Result<()> {
        let (o, i, s, pats) = (st[0].as_list::<i32>(), st[1].as_list::<i32>(), st[2].as_list::<i32>(), strs(&st[3])?);
        for k in 0..o.len() {
            let (ok, ik, sk) = (ints(&o.value(k))?, ints(&i.value(k))?, strs(&s.value(k))?);
            for j in 0..ok.len() { if let (Some(a), Some(b)) = (ok[j], ik[j]) { self.rows.push((a, b, sk[j].clone())) } }
            if self.pattern.is_none() { self.pattern = pats[k].clone() }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.rows.sort_by_key(|r| r.0);
        let rows: Vec<_> = self.rows.iter().map(|r| (r.1, r.2.as_deref())).collect();
        let ms = matches(&rows, self.pattern.as_deref().unwrap_or(""))?;
        let inner: Vec<ScalarValue> = ms.iter().map(|m| {
            ScalarValue::List(ScalarValue::new_list_nullable(&m.iter().map(|x| ScalarValue::Int64(Some(*x))).collect::<Vec<_>>(), &DataType::Int64))
        }).collect();
        Ok(ScalarValue::List(ScalarValue::new_list_nullable(&inner, &list_of(DataType::Int64))))
    }

    fn size(&self) -> usize { std::mem::size_of_val(self) + self.rows.capacity() * 48 }
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
