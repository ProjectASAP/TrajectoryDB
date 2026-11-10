//! seq_match: MATCH_RECOGNIZE-style row-pattern matching as an aggregate.
//!
//!   SELECT session_id, seq_match(pos, step_id, sym, 'A .{0,2} M') AS matches FROM ... GROUP BY session_id
//!   SELECT session_id, seq_match(pos, step_id, syms, 'M M[!=M]{2,}', metric_key) ... -- with a key column
//!
//!   PARTITION BY -> GROUP BY; ORDER BY -> first arg (ord); DEFINE -> SQL producing `sym`, either one symbol (utf8)
//!   or a set of symbols (list<utf8>): a row matches atom X if X is among its symbols.
//!   PATTERN: identifiers are symbols, `.` is any row (incl. none/NULL), plus ( ) | * + ? {n} {m,} {m,n}.
//!   Back-references (optional 5th arg `key`, any type, compared as text): `X[=Y]` / `X[!=Y]` is a row with symbol X
//!   whose key equals / differs from the key of the most recent row this match bound to symbol Y
//!   (MATCH_RECOGNIZE's DEFINE X AS key = LAST(Y.key)). False when there is no such row or either key is NULL.
//!   Returns list<list<int64>>: the `id`s of each match's rows, in order. Matches are leftmost, greedy, and
//!   non-overlapping (AFTER MATCH SKIP PAST LAST ROW); empty matches are dropped.
// ponytail: backtracking matcher, exponential on pathological patterns (nested unbounded quantifiers);
// compile to an NFA with per-thread binding state if real workloads hit that. Buffers and sorts each partition (C5).

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int64Type};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{Accumulator, AggregateUDF, Signature, SimpleAggregateUDF, TypeSignature, Volatility};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
enum Node {
    Any,
    Sym { name: String, cond: Option<(bool, String)> }, // (equal?, variable)
    Seq(Vec<Node>),
    Alt(Vec<Node>),
    Rep(Box<Node>, u32, Option<u32>),
}

struct Parser<'a> { s: &'a [u8], i: usize, src: &'a str }

impl Parser<'_> {
    fn ws(&mut self) { while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() { self.i += 1 } }
    fn peek(&mut self) -> Option<u8> { self.ws(); self.s.get(self.i).copied() }
    fn eat(&mut self, c: u8) -> bool { if self.peek() == Some(c) { self.i += 1; true } else { false } }
    fn err<T>(&self, what: &str) -> Result<T> { exec_err!("seq_match: {what} at {} in pattern {:?}", self.i, self.src) }
    fn ident(&mut self) -> Option<String> {
        self.ws();
        let st = self.i;
        while self.i < self.s.len() && (self.s[self.i].is_ascii_alphanumeric() || self.s[self.i] == b'_') { self.i += 1 }
        (self.i > st && !self.s[st].is_ascii_digit()).then(|| self.src[st..self.i].to_string())
    }
    fn num(&mut self) -> Option<u32> {
        self.ws();
        let st = self.i;
        while self.i < self.s.len() && self.s[self.i].is_ascii_digit() { self.i += 1 }
        self.src[st..self.i].parse().ok()
    }
    fn alt(&mut self) -> Result<Node> {
        let mut v = vec![self.seq()?];
        while self.eat(b'|') { v.push(self.seq()?) }
        Ok(if v.len() == 1 { v.pop().unwrap() } else { Node::Alt(v) })
    }
    fn seq(&mut self) -> Result<Node> {
        let mut v = vec![];
        while !matches!(self.peek(), None | Some(b'|') | Some(b')')) {
            let atom = self.atom()?;
            v.push(self.quant(atom)?);
        }
        Ok(Node::Seq(v))
    }
    fn atom(&mut self) -> Result<Node> {
        if self.eat(b'(') {
            let n = self.alt()?;
            if !self.eat(b')') { return self.err("expected )") }
            return Ok(n);
        }
        if self.eat(b'.') { return Ok(Node::Any) }
        let Some(name) = self.ident() else { return self.err("unexpected character") };
        let cond = if self.eat(b'[') {
            let eq = if self.eat(b'=') { true } else if self.eat(b'!') && self.eat(b'=') { false } else { return self.err("expected = or !=") };
            let Some(var) = self.ident() else { return self.err("expected variable") };
            if !self.eat(b']') { return self.err("expected ]") }
            Some((eq, var))
        } else { None };
        Ok(Node::Sym { name, cond })
    }
    fn quant(&mut self, n: Node) -> Result<Node> {
        let (lo, hi) = match self.peek() {
            Some(b'*') => (0, None),
            Some(b'+') => (1, None),
            Some(b'?') => (0, Some(1)),
            Some(b'{') => {
                self.i += 1;
                let Some(lo) = self.num() else { return self.err("expected number") };
                let hi = if self.eat(b',') { self.num() } else { Some(lo) };
                if self.peek() != Some(b'}') { return self.err("expected }") }
                (lo, hi)
            }
            _ => return Ok(n),
        };
        self.i += 1;
        self.quant(Node::Rep(Box::new(n), lo, hi))
    }
}

fn parse(pattern: &str) -> Result<Node> {
    let mut p = Parser { s: pattern.as_bytes(), i: 0, src: pattern };
    let n = p.alt()?;
    if p.peek().is_some() { return p.err("unbalanced )") }
    Ok(n)
}

/// Parsed patterns, shared across partitions (one accumulator per group would otherwise reparse per group).
fn compiled(pattern: &str) -> Result<Arc<Node>> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<HashMap<String, Arc<Node>>>> = std::sync::OnceLock::new();
    let mut c = CACHE.get_or_init(Default::default).lock().unwrap();
    if let Some(x) = c.get(pattern) { return Ok(x.clone()) }
    let x = Arc::new(parse(pattern)?);
    c.insert(pattern.to_string(), x.clone());
    Ok(x)
}

/// One input row of a partition: symbols and optional key.
#[derive(Debug, Clone)]
struct Row { syms: Vec<String>, key: Option<String> }

type Binds<'a> = Vec<(&'a str, usize)>; // (symbol, row) bound so far in the current match attempt
type K<'k, 'a> = &'k mut dyn FnMut(usize, &mut Binds<'a>) -> bool;

/// Continuation-passing backtracking: match `n` at row `i`, then call `k` with the end row; true = accepted.
fn go<'a>(rows: &[Row], n: &'a Node, i: usize, b: &mut Binds<'a>, k: K<'_, 'a>) -> bool {
    match n {
        Node::Any => i < rows.len() && k(i + 1, b),
        Node::Sym { name, cond } => {
            if i >= rows.len() || !rows[i].syms.iter().any(|s| s == name) { return false }
            if let Some((eq, var)) = cond {
                let last = b.iter().rev().find(|(v, _)| v == var).and_then(|&(_, r)| rows[r].key.as_ref());
                match (last, &rows[i].key) { (Some(a), Some(c)) if (a == c) == *eq => {}, _ => return false }
            }
            b.push((name, i));
            let r = k(i + 1, b);
            b.pop();
            r
        }
        Node::Seq(v) => seq(rows, v, i, b, k),
        Node::Alt(v) => v.iter().any(|x| go(rows, x, i, b, k)),
        Node::Rep(x, lo, hi) => rep(rows, x, *lo, *hi, 0, i, b, k),
    }
}

fn seq<'a>(rows: &[Row], v: &'a [Node], i: usize, b: &mut Binds<'a>, k: K<'_, 'a>) -> bool {
    match v.split_first() {
        None => k(i, b),
        Some((h, t)) => go(rows, h, i, b, &mut |j, b| seq(rows, t, j, b, k)),
    }
}

#[allow(clippy::too_many_arguments)]
fn rep<'a>(rows: &[Row], x: &'a Node, lo: u32, hi: Option<u32>, n: u32, i: usize, b: &mut Binds<'a>, k: K<'_, 'a>) -> bool {
    // greedy: try one more iteration first (must consume a row, so no infinite loops), then stop if n >= lo
    if hi.is_none_or(|h| n < h) && go(rows, x, i, b, &mut |j, b| j > i && rep(rows, x, lo, hi, n + 1, j, b, k)) { return true }
    n >= lo && k(i, b)
}

/// Leftmost, non-overlapping, non-empty matches as row-index ranges.
fn find(n: &Node, rows: &[Row]) -> Vec<std::ops::Range<usize>> {
    let (mut out, mut s) = (vec![], 0);
    while s < rows.len() {
        let mut end = None;
        go(rows, n, s, &mut vec![], &mut |j, _| { if j > s { end = Some(j); true } else { false } });
        match end { Some(e) => { out.push(s..e); s = e } None => s += 1 }
    }
    out
}

/// Convenience over (id, syms, key) rows already in order (unit tests).
#[cfg(test)]
pub fn matches(rows: &[(i64, &str, Option<&str>)], pattern: &str) -> Result<Vec<Vec<i64>>> {
    let n = compiled(pattern)?;
    let r: Vec<Row> = rows.iter().map(|(_, s, k)| Row { syms: s.split('+').filter(|x| *x != "-").map(String::from).collect(), key: k.map(String::from) }).collect();
    Ok(find(&n, &r).into_iter().map(|m| rows[m].iter().map(|x| x.0).collect()).collect())
}

const SEP: char = '\u{1f}'; // joins a row's symbols in the partial state

/// Per-group state: (ord, id, row) plus the pattern.
#[derive(Debug, Default)]
struct Acc { rows: Vec<(i64, i64, Row)>, pattern: Option<String>, re: Option<Arc<Node>> }

fn ints(a: &ArrayRef) -> Result<Vec<Option<i64>>> {
    Ok(cast(a, &DataType::Int64)?.as_primitive::<Int64Type>().iter().collect())
}

fn strs(a: &ArrayRef) -> Result<Vec<Option<String>>> {
    Ok(cast(a, &DataType::Utf8)?.as_string::<i32>().iter().map(|x| x.map(String::from)).collect())
}

/// Symbol sets for a utf8 column (one symbol or NULL) or a list<utf8> column.
fn sym_sets(a: &ArrayRef) -> Result<Vec<Vec<String>>> {
    if let DataType::List(_) | DataType::LargeList(_) = a.data_type() {
        let l = cast(a, &DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))))?;
        let l = l.as_list::<i32>();
        (0..l.len()).map(|k| Ok(if l.is_null(k) { vec![] } else { strs(&l.value(k))?.into_iter().flatten().collect() })).collect()
    } else {
        Ok(strs(a)?.into_iter().map(|x| x.into_iter().collect()).collect())
    }
}

impl Acc {
    fn set_pattern(&mut self, pat: &ArrayRef) -> Result<()> {
        if self.re.is_some() { return Ok(()) }
        if let Some(p) = strs(pat)?.into_iter().flatten().next() {
            self.re = Some(compiled(&p)?);
            self.pattern = Some(p);
        }
        Ok(())
    }
    fn push(&mut self, o: &ArrayRef, i: &ArrayRef, syms: Vec<Vec<String>>, key: Option<&ArrayRef>) -> Result<()> {
        let (o, i) = (ints(o)?, ints(i)?);
        let key = match key { Some(k) => strs(k)?, None => vec![None; o.len()] };
        for (k, syms) in syms.into_iter().enumerate() {
            if let (Some(o), Some(i)) = (o[k], i[k]) { self.rows.push((o, i, Row { syms, key: key[k].clone() })) }
        }
        Ok(())
    }
}

fn list_of(t: DataType) -> DataType { DataType::List(Arc::new(Field::new_list_field(t, true))) }

impl Accumulator for Acc {
    fn update_batch(&mut self, v: &[ArrayRef]) -> Result<()> {
        self.set_pattern(&v[3])?;
        self.push(&v[0], &v[1], sym_sets(&v[2])?, v.get(4))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let l = |v: Vec<ScalarValue>, t: &DataType| ScalarValue::List(ScalarValue::new_list_nullable(&v, t));
        let r = &self.rows;
        Ok(vec![
            l(r.iter().map(|r| ScalarValue::Int64(Some(r.0))).collect(), &DataType::Int64),
            l(r.iter().map(|r| ScalarValue::Int64(Some(r.1))).collect(), &DataType::Int64),
            l(r.iter().map(|r| ScalarValue::Utf8(Some(r.2.syms.join(&SEP.to_string())))).collect(), &DataType::Utf8),
            l(r.iter().map(|r| ScalarValue::Utf8(r.2.key.clone())).collect(), &DataType::Utf8),
            ScalarValue::Utf8(self.pattern.clone()),
        ])
    }

    fn merge_batch(&mut self, st: &[ArrayRef]) -> Result<()> {
        self.set_pattern(&st[4])?;
        let (o, i, s, k) = (st[0].as_list::<i32>(), st[1].as_list::<i32>(), st[2].as_list::<i32>(), st[3].as_list::<i32>());
        for j in 0..o.len() {
            let syms = strs(&s.value(j))?.into_iter().map(|x| x.unwrap_or_default().split(SEP).filter(|x| !x.is_empty()).map(String::from).collect()).collect();
            self.push(&o.value(j), &i.value(j), syms, Some(&k.value(j)))?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let Some(re) = &self.re else { return exec_err!("seq_match: NULL pattern") };
        self.rows.sort_by_key(|r| r.0);
        let rows: Vec<Row> = self.rows.iter().map(|r| r.2.clone()).collect();
        let inner: Vec<ScalarValue> = find(re, &rows).into_iter().map(|m| {
            let ids: Vec<_> = self.rows[m].iter().map(|r| ScalarValue::Int64(Some(r.1))).collect();
            ScalarValue::List(ScalarValue::new_list_nullable(&ids, &DataType::Int64))
        }).collect();
        Ok(ScalarValue::List(ScalarValue::new_list_nullable(&inner, &list_of(DataType::Int64))))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.rows.iter().map(|r| 48 + r.2.syms.iter().map(|s| 24 + s.len()).sum::<usize>()).sum::<usize>()
    }
}

pub fn udaf() -> AggregateUDF {
    // any(4|5): the accumulator casts ord/id to Int64, sym to utf8 or list<utf8>, key/pattern to Utf8
    let state = [list_of(DataType::Int64), list_of(DataType::Int64), list_of(DataType::Utf8), list_of(DataType::Utf8), DataType::Utf8];
    AggregateUDF::from(SimpleAggregateUDF::new_with_signature(
        "seq_match",
        Signature::one_of(vec![TypeSignature::Any(4), TypeSignature::Any(5)], Volatility::Immutable),
        list_of(list_of(DataType::Int64)),
        Arc::new(|_| Ok(Box::<Acc>::default())),
        state.into_iter().enumerate().map(|(i, t)| Arc::new(Field::new(format!("seq_match_state_{i}"), t, true))).collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rows(s: &str) -> Vec<(i64, &str, Option<&str>)> {
        s.split(' ').enumerate().map(|(i, x)| {
            let (sym, key) = x.split_once(':').map_or((x, None), |(a, b)| (a, Some(b)));
            (i as i64 + 1, sym, key)
        }).collect()
    }
    #[test]
    fn matching() {
        let r = rows("A X M - L A M");
        assert_eq!(matches(&r, "A .{0,2} M").unwrap(), [vec![1, 2, 3], vec![6, 7]]);
        assert_eq!(matches(&r, "A .{0,2} M .{0,2} L").unwrap(), [vec![1, 2, 3, 4, 5]]);
        assert_eq!(matches(&r, "M+").unwrap(), [vec![3], vec![7]]);
        assert_eq!(matches(&r, "(A|M) (A|M)").unwrap(), [vec![6, 7]]);
        assert_eq!(matches(&r, "A .* M").unwrap(), [vec![1, 2, 3, 4, 5, 6, 7]]); // greedy
        assert_eq!(matches(&r, "Q").unwrap(), Vec::<Vec<i64>>::new());
        assert!(matches(&r, "A ;").is_err());
        assert!(matches(&r, "(A").is_err());
    }
    #[test]
    fn sets_and_backrefs() {
        // set-valued rows: step 1 has both A and M
        let r = rows("A+M L");
        assert_eq!(matches(&r, "M L").unwrap(), [vec![1, 2]]);
        assert_eq!(matches(&r, "A L").unwrap(), [vec![1, 2]]);
        // keys: metric names; refine = same metric as the previous M, switch = different
        let r = rows("M:x M:x M:y - M:y M:z");
        assert_eq!(matches(&r, "M M[=M]+").unwrap(), [vec![1, 2]]);
        assert_eq!(matches(&r, "M M[!=M]").unwrap(), [vec![2, 3], vec![5, 6]]);
        assert_eq!(matches(&r, "M (.? M[=M])+").unwrap(), [vec![1, 2], vec![3, 4, 5]]);
        // FIRST-like anchor: L in the same namespace as the A that started the match
        let r = rows("A:ns1 L:ns2 L:ns1");
        assert_eq!(matches(&r, "A .* L[=A]").unwrap(), [vec![1, 2, 3]]);
        assert_eq!(matches(&r, "A L[=A]").unwrap(), Vec::<Vec<i64>>::new());
        assert_eq!(matches(&r, "L[=A]").unwrap(), Vec::<Vec<i64>>::new()); // no bound A -> false
    }
}
