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
//!   promql_asap_class(q) -> utf8     which ASAPQuery planner pattern q matches (see asap_class)
//!   promql_asap_blocker(q) -> utf8  for 'unsupported' queries: the outermost reason (see asap_blocker)
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

/// Mirror of ASAPQuery's planner pattern table (ASAPQuery@c439bf4: asap-planner-rs/src/planner/patterns.rs,
/// promql_utilities/src/ast_matching/collapsable_patterns.rs, query_logics/logics.rs::get_is_collapsable,
/// query_logics/enums.rs::is_approximate). Classes:
///   asap_approx  matches a pattern whose statistic is approximate (sum/count/avg/quantile/topk family)
///   asap_exact   matches a pattern but statistic is exact (rate, increase, min, max)
///   asap_binary  top-level arithmetic whose every arm is asap_* or a number (planner splits binary arms)
///   raw_selector plain vector/matrix selector    discovery  no metric name
///   unsupported  anything else                   unparsed   parser rejects it
// ponytail: re-implemented, not linked; parens around a pattern count as unsupported (planner does not strip them).
pub fn asap_class(q: &str) -> &'static str {
    let Ok(e) = parse(q) else { return "unparsed" };
    if facts(q).is_some_and(|f| f.metrics.is_empty()) { return "discovery" }
    match asap_match(&e) {
        Some(true) => "asap_approx",
        Some(false) => "asap_exact",
        None => match &e {
            Expr::VectorSelector(_) | Expr::MatrixSelector(_) => "raw_selector",
            Expr::Binary(b) if matches!(b.op.to_string().as_str(), "+" | "-" | "*" | "/" | "%" | "^")
                && [&b.lhs, &b.rhs].iter().all(|x| matches!(***x, Expr::NumberLiteral(_)) || asap_match(x).is_some()) => "asap_binary",
            _ => "unsupported",
        },
    }
}

/// Why an 'unsupported' query misses every pattern (outermost construct). NULL for other classes.
///   comparison: top level is a comparison (x > 0); `_on_asap` if its non-scalar arm(s) would match a pattern
///   agg_over_rate: sum/avg/topk/... over rate/increase (not collapsable in ASAPQuery)
///   arithmetic: + - * / with an arm that does not match; histogram_quantile; subquery (x[10m:1m]); other_agg; other_func; other
pub fn asap_blocker(q: &str) -> Option<&'static str> {
    if asap_class(q) != "unsupported" { return None }
    let e = parse(q).ok()?;
    let arm_ok = |x: &Expr| matches!(x, Expr::NumberLiteral(_)) || asap_match(x).is_some();
    Some(match &e {
        Expr::Binary(b) if matches!(b.op.to_string().as_str(), "==" | "!=" | ">" | "<" | ">=" | "<=") =>
            if arm_ok(&b.lhs) && arm_ok(&b.rhs) { "comparison_on_asap" } else { "comparison" },
        Expr::Binary(_) => "arithmetic",
        Expr::Aggregate(a) => match &*a.expr {
            Expr::Call(c) if matches!(c.func.name, "rate" | "increase" | "irate") => "agg_over_rate",
            Expr::Aggregate(i) if matches!(&*i.expr, Expr::Call(c) if matches!(c.func.name, "rate" | "increase" | "irate")) => "agg_over_rate",
            _ => "other_agg",
        },
        Expr::Call(c) if c.func.name == "histogram_quantile" => "histogram_quantile",
        Expr::Call(_) => "other_func",
        Expr::Subquery(_) => "subquery",
        _ => "other",
    })
}

/// Some(approximate?) if e matches one of the planner's patterns.
fn asap_match(e: &Expr) -> Option<bool> {
    let metric = |e: &Expr| matches!(e, Expr::VectorSelector(_));
    // temporal function over metric[range] -> its name
    let temporal = |e: &Expr| -> Option<&'static str> {
        let Expr::Call(c) = e else { return None };
        let a = &c.args.args;
        let ok = match c.func.name {
            "quantile_over_time" => a.len() == 2 && matches!(*a[0], Expr::NumberLiteral(_)) && matches!(*a[1], Expr::MatrixSelector(_)),
            "sum_over_time" | "count_over_time" | "avg_over_time" | "min_over_time" | "max_over_time" | "increase" | "rate" =>
                a.len() == 1 && matches!(*a[0], Expr::MatrixSelector(_)),
            _ => false,
        };
        ok.then_some(c.func.name)
    };
    let approx_fn = |f: &str| matches!(f, "quantile_over_time" | "sum_over_time" | "count_over_time" | "avg_over_time");
    let collapsable = |f: &str, op: &str| match op {
        "sum" | "topk" => matches!(f, "sum_over_time" | "count_over_time"),
        "min" => f == "min_over_time",
        "max" => f == "max_over_time",
        _ => false,
    };
    match e {
        Expr::Call(_) => temporal(e).map(approx_fn),
        Expr::Aggregate(a) => {
            let op = a.op.to_string().to_lowercase();
            if !matches!(op.as_str(), "sum" | "count" | "avg" | "quantile" | "min" | "max" | "topk") { return None }
            if metric(&a.expr) { return Some(!matches!(op.as_str(), "min" | "max")) }
            if let Some(f) = temporal(&a.expr) { return collapsable(f, &op).then(|| !matches!(op.as_str(), "min" | "max")) }
            if let (Expr::Aggregate(inner), "topk") = (&*a.expr, op.as_str()) {
                if inner.op.to_string().to_lowercase() == "sum" && temporal(&inner.expr).is_some_and(|f| collapsable(f, "sum")) { return Some(true) }
            }
            None
        }
        _ => None,
    }
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
    c.register_udf(str_udf("promql_asap_class", 1, |r| Ok(r[0].map(|q| asap_class(q).to_string()))));
    c.register_udf(str_udf("promql_asap_blocker", 1, |r| Ok(r[0].and_then(asap_blocker).map(str::to_string))));
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
        // cases from ASAPQuery's patterns.rs tests
        for (q, c) in [("quantile_over_time(0.99, x[5m])", "asap_approx"), ("rate(x[5m])", "asap_exact"), ("sum by (a) (x)", "asap_approx"),
            ("max(x)", "asap_exact"), ("sum(sum_over_time(x[5m]))", "asap_approx"), ("min(min_over_time(x[5m]))", "asap_exact"),
            ("topk by (job) (3, count_over_time(x[5m]))", "asap_approx"), ("topk(32, sum by (l) (sum_over_time(d[15m])))", "asap_approx"),
            ("sum(rate(x[5m]))", "unsupported"), ("avg(rate(x[5m]))", "unsupported"), ("topk(1, rate(x[5m]))", "unsupported"),
            ("topk(32, sum by (l) (rate(d[15m])))", "unsupported"), ("sum(min_over_time(x[5m]))", "unsupported"),
            ("x", "raw_selector"), ("x[5m]", "raw_selector"), ("{__name__=~\"a.*\"}", "discovery"), ("rate(x[5m]", "unparsed"),
            ("sum(x) / count(x)", "asap_binary"), ("sum(x) > 0", "unsupported"), ("histogram_quantile(0.9, x)", "unsupported")] {
            assert_eq!(asap_class(q), c, "{q}");
        }
        for (q, b) in [("sum(rate(x[5m]))", Some("agg_over_rate")), ("sum(x) > 0", Some("comparison_on_asap")), ("sum(rate(x[1m])) > 0", Some("comparison")),
            ("histogram_quantile(0.9, x)", Some("histogram_quantile")), ("sum(x)", None)] {
            assert_eq!(asap_blocker(q), b, "{q}");
        }
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
