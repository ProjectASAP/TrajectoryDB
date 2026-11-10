//! PromQL extraction UDFs (deterministic, built on promql-parser).
//!   promql_metrics(q)  -> list<utf8>  metric names (selector name or __name__="x")
//!   promql_funcs(q)    -> list<utf8>  function calls, e.g. rate
//!   promql_aggs(q)     -> list<utf8>  aggregations, e.g. "sum by (pod)"
//!   promql_matchers(q) -> list<utf8>  non-name label matchers, e.g. instance=~svc.*
//!   promql_ranges(q)   -> list<utf8>  range / subquery windows, e.g. 5m
//!   promql_error(q)    -> utf8        parse error, NULL if it parses
//!   promql_normalize(q, mode) -> utf8 modes, each coarser than the last:
//!     'exact' (as sent) | 'canonical' (re-printed, matchers/by-labels sorted)
//!     | 'no_window' (+ ranges, subquery windows and offsets blanked) | 'shape' (+ label values -> "_")
//!     non-exact modes return NULL for unparseable queries
//!   url_promql(text)   -> list<utf8>  URL-decoded query= params (PromQL inside curl)
//! Every list UDF returns NULL when q is NULL or fails to parse.

use datafusion::arrow::array::{Array, ArrayRef, ListBuilder, StringArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};
use datafusion::prelude::SessionContext;
use promql_parser::label::{MatchOp, METRIC_NAME};
use promql_parser::parser::{parse, Expr, LabelModifier, VectorSelector};
use promql_parser::util::display_duration;
use std::sync::Arc;

#[derive(Default, Debug, PartialEq)]
pub struct Facts { pub metrics: Vec<String>, pub funcs: Vec<String>, pub aggs: Vec<String>, pub matchers: Vec<String>, pub ranges: Vec<String> }

pub fn facts(q: &str) -> Option<Facts> {
    let mut f = Facts::default();
    walk(&parse(q).ok()?, &mut f);
    Some(f)
}

fn walk(e: &Expr, f: &mut Facts) {
    match e {
        Expr::Aggregate(a) => {
            let m = match &a.modifier {
                Some(LabelModifier::Include(l)) => format!(" by ({})", l.labels.join(",")),
                Some(LabelModifier::Exclude(l)) => format!(" without ({})", l.labels.join(",")),
                None => String::new(),
            };
            f.aggs.push(format!("{}{m}", a.op));
            walk(&a.expr, f);
            if let Some(p) = &a.param { walk(p, f) }
        }
        Expr::Unary(u) => walk(&u.expr, f),
        Expr::Binary(b) => { walk(&b.lhs, f); walk(&b.rhs, f) }
        Expr::Paren(p) => walk(&p.expr, f),
        Expr::Subquery(s) => { f.ranges.push(display_duration(&s.range)); walk(&s.expr, f) }
        Expr::MatrixSelector(m) => { f.ranges.push(display_duration(&m.range)); walk(&Expr::VectorSelector(m.vs.clone()), f) }
        Expr::VectorSelector(v) => {
            f.metrics.extend(v.name.clone());
            let ms = v.matchers.matchers.iter().chain(v.matchers.or_matchers.iter().flatten());
            for m in ms {
                if m.name == METRIC_NAME && matches!(m.op, MatchOp::Equal) {
                    if v.name.is_none() { f.metrics.push(m.value.clone()) }
                } else {
                    f.matchers.push(format!("{}{}{}", m.name, m.op, m.value));
                }
            }
        }
        Expr::Call(c) => { f.funcs.push(c.func.name.to_string()); for a in &c.args.args { walk(a, f) } }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {}
    }
}

/// Re-print q after canonicalizing; `win` blanks windows/offsets, `vals` blanks label values.
pub fn normalize(q: &str, win: bool, vals: bool) -> Option<String> {
    let mut e = parse(q).ok()?;
    canon(&mut e, win, vals);
    Some(e.to_string())
}

fn canon(e: &mut Expr, win: bool, vals: bool) {
    let sort = |m: &mut Option<LabelModifier>| if let Some(LabelModifier::Include(l) | LabelModifier::Exclude(l)) = m { l.labels.sort() };
    match e {
        Expr::Aggregate(a) => { sort(&mut a.modifier); canon(&mut a.expr, win, vals); if let Some(p) = &mut a.param { canon(p, win, vals) } }
        Expr::Unary(u) => canon(&mut u.expr, win, vals),
        Expr::Binary(b) => { if let Some(m) = &mut b.modifier { sort(&mut m.matching) } canon(&mut b.lhs, win, vals); canon(&mut b.rhs, win, vals) }
        Expr::Paren(p) => canon(&mut p.expr, win, vals),
        Expr::Subquery(s) => { if win { s.range = Default::default(); s.offset = None } canon(&mut s.expr, win, vals) }
        Expr::MatrixSelector(m) => { if win { m.range = Default::default() } sel(&mut m.vs, win, vals) }
        Expr::VectorSelector(v) => sel(v, win, vals),
        Expr::Call(c) => for a in c.args.args.iter_mut() { canon(a, win, vals) },
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {}
    }
}

fn sel(v: &mut VectorSelector, win: bool, vals: bool) {
    if win { v.offset = None }
    for m in v.matchers.matchers.iter_mut().chain(v.matchers.or_matchers.iter_mut().flatten()) {
        if vals && m.name != METRIC_NAME { m.value = "_".into() }
    }
    v.matchers.matchers.sort_by(|a, b| (&a.name, a.op.to_string(), &a.value).cmp(&(&b.name, b.op.to_string(), &b.value)));
}

/// Every `query=` URL parameter in free text, percent-decoded.
pub fn url_promql(s: &str) -> Vec<String> {
    s.match_indices("query=").filter(|(i, _)| *i == 0 || !s.as_bytes()[i - 1].is_ascii_alphanumeric())
        .map(|(i, _)| {
            let v = &s[i + 6..];
            pct_decode(&v[..v.find(|c: char| matches!(c, '&' | '"' | '\'' | ' ' | '\n')).unwrap_or(v.len())])
        }).collect()
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let (mut out, mut i) = (Vec::with_capacity(b.len()), 0);
    let hex = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    while i < b.len() {
        match (b[i], b.get(i + 1).copied().and_then(hex), b.get(i + 2).copied().and_then(hex)) {
            (b'%', Some(h), Some(l)) => { out.push(h << 4 | l); i += 3 }
            (b'+', ..) => { out.push(b' '); i += 1 }
            (c, ..) => { out.push(c); i += 1 }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn strings(v: &ColumnarValue) -> Result<(ArrayRef, bool)> {
    let scalar = matches!(v, ColumnarValue::Scalar(_));
    Ok((cast(&v.to_array(1)?, &DataType::Utf8)?, scalar))
}

fn out(a: ArrayRef, scalar: bool) -> Result<ColumnarValue> {
    Ok(if scalar { ColumnarValue::Scalar(datafusion::common::ScalarValue::try_from_array(&a, 0)?) } else { ColumnarValue::Array(a) })
}

pub fn list_udf(name: &str, f: fn(&str) -> Option<Vec<String>>) -> ScalarUDF {
    let ty = DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true)));
    create_udf(name, vec![DataType::Utf8], ty, Volatility::Immutable, Arc::new(move |args: &[ColumnarValue]| {
        let (a, scalar) = strings(&args[0])?;
        let a = a.as_any().downcast_ref::<StringArray>().unwrap();
        let mut b = ListBuilder::new(StringBuilder::new());
        for q in a.iter() {
            match q.and_then(f) {
                Some(v) => { for x in v { b.values().append_value(x) } b.append(true) }
                None => b.append(false),
            }
        }
        out(Arc::new(b.finish()), scalar)
    }))
}

pub fn str_udf(name: &str, n: usize, f: fn(&[Option<&str>]) -> Result<Option<String>>) -> ScalarUDF {
    create_udf(name, vec![DataType::Utf8; n], DataType::Utf8, Volatility::Immutable, Arc::new(move |args: &[ColumnarValue]| {
        let scalar = args.iter().all(|x| matches!(x, ColumnarValue::Scalar(_)));
        let len = args.iter().find_map(|x| match x { ColumnarValue::Array(a) => Some(a.len()), _ => None }).unwrap_or(1);
        let cols = args.iter().map(|x| Ok(cast(&x.to_array(len)?, &DataType::Utf8)?)).collect::<Result<Vec<_>>>()?;
        let cols: Vec<&StringArray> = cols.iter().map(|c| c.as_any().downcast_ref().unwrap()).collect();
        let mut b = StringBuilder::new();
        for i in 0..len {
            let row: Vec<_> = cols.iter().map(|c| c.is_valid(i).then(|| c.value(i))).collect();
            b.append_option(f(&row)?);
        }
        out(Arc::new(b.finish()), scalar)
    }))
}

pub fn register(c: &SessionContext) {
    c.register_udf(list_udf("promql_metrics", |q| facts(q).map(|f| f.metrics)));
    c.register_udf(list_udf("promql_funcs", |q| facts(q).map(|f| f.funcs)));
    c.register_udf(list_udf("promql_aggs", |q| facts(q).map(|f| f.aggs)));
    c.register_udf(list_udf("promql_matchers", |q| facts(q).map(|f| f.matchers)));
    c.register_udf(list_udf("promql_ranges", |q| facts(q).map(|f| f.ranges)));
    c.register_udf(list_udf("url_promql", |s| Some(url_promql(s))));
    c.register_udf(str_udf("promql_error", 1, |r| Ok(r[0].and_then(|q| parse(q).err()))));
    c.register_udf(str_udf("promql_normalize", 2, |r| {
        let (win, vals) = match r[1] {
            Some("exact") => return Ok(r[0].map(str::to_string)),
            Some("canonical") => (false, false),
            Some("no_window") => (true, false),
            Some("shape") => (true, true),
            m => return exec_err!("promql_normalize: unknown mode {m:?} (exact|canonical|no_window|shape)"),
        };
        Ok(r[0].and_then(|q| normalize(q, win, vals)))
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn extracts() {
        let f = facts(r#"sum by (pod) (rate(http_requests_total{job="api",code=~"5.."}[5m])) / on() group_left {__name__="up"}"#).unwrap();
        assert_eq!(f.metrics, ["http_requests_total", "up"]);
        assert_eq!(f.funcs, ["rate"]);
        assert_eq!(f.aggs, ["sum by (pod)"]);
        assert_eq!(f.matchers, ["job=api", "code=~5.."]);
        assert_eq!(f.ranges, ["5m"]);
        assert!(facts("rate(x[5m]").is_none());
        assert_eq!(url_promql("curl 'http://p:9090/api/v1/query_range?query=up%7Bjob%3D%22a%22%7D&step=30' | jq"), [r#"up{job="a"}"#]);
        assert_eq!(url_promql("x --data-urlencode query=sum(up)"), ["sum(up)"]);
        let q = r#"sum by (b,a)(rate(x{z="1",a=~"2"}[5m] offset 1m))"#;
        let n = |w, v| normalize(q, w, v).unwrap();
        assert_eq!(n(false, false), normalize(r#"sum by(a, b) (rate(x{a=~"2", z="1"}[5m] offset 1m))"#, false, false).unwrap());
        assert_ne!(n(true, false), n(false, false));
        assert_eq!(n(true, false), normalize(r#"sum by (a,b)(rate(x{a=~"2",z="1"}[1h]))"#, true, false).unwrap());
        assert_eq!(n(true, true), normalize(r#"sum by (a,b)(rate(x{a=~"9",z="8"}[1h]))"#, true, true).unwrap());
        assert_ne!(n(true, true), normalize(r#"sum by (a,b)(rate(y{a=~"9",z="8"}[1h]))"#, true, true).unwrap());
    }
}
