//! interval_union(start, end): total length of the union of [start, end) intervals in a group.
//!
//!   SELECT session_id, interval_union(start_unix_s, end_unix_s) AS tool_busy_s FROM tool_calls GROUP BY session_id
//!
//! Overlapping intervals (parallel tool calls) count once. Rows with a NULL bound are skipped;
//! a group with no complete interval gives NULL (unknown), not 0.

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Float64Type};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{Accumulator, AggregateUDF, Signature, SimpleAggregateUDF, Volatility};
use std::sync::Arc;

/// Sort by start, merge overlapping or touching intervals, sum their lengths.
fn union_length(mut v: Vec<(f64, f64)>) -> Option<f64> {
    if v.is_empty() { return None }
    v.sort_by(|a, b| a.0.total_cmp(&b.0));
    let (mut total, (mut lo, mut hi)) = (0.0, v[0]);
    for &(s, e) in &v[1..] {
        if s > hi { total += hi - lo; (lo, hi) = (s, e) } else { hi = hi.max(e) }
    }
    Some(total + hi - lo)
}

#[derive(Debug, Default)]
struct Acc { v: Vec<(f64, f64)> }

impl Acc {
    fn add(&mut self, starts: &ArrayRef, ends: &ArrayRef) -> Result<()> {
        let (s, e) = (cast(starts, &DataType::Float64)?, cast(ends, &DataType::Float64)?);
        let (s, e) = (s.as_primitive::<Float64Type>(), e.as_primitive::<Float64Type>());
        for i in 0..s.len() {
            if s.is_valid(i) && e.is_valid(i) && e.value(i) >= s.value(i) { self.v.push((s.value(i), e.value(i))) }
        }
        Ok(())
    }
}

impl Accumulator for Acc {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> { self.add(&values[0], &values[1]) }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let list = |f: fn(&(f64, f64)) -> f64| {
            let xs: Vec<_> = self.v.iter().map(|x| ScalarValue::Float64(Some(f(x)))).collect();
            ScalarValue::List(ScalarValue::new_list_nullable(&xs, &DataType::Float64))
        };
        Ok(vec![list(|x| x.0), list(|x| x.1)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let (s, e) = (states[0].as_list::<i32>(), states[1].as_list::<i32>());
        for i in 0..s.len() {
            if s.is_valid(i) { self.add(&s.value(i), &e.value(i))? }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> { Ok(ScalarValue::Float64(union_length(self.v.clone()))) }

    fn size(&self) -> usize { std::mem::size_of_val(self) + 16 * self.v.capacity() }
}

pub fn udaf() -> AggregateUDF {
    let list = DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true)));
    AggregateUDF::from(SimpleAggregateUDF::new_with_signature(
        "interval_union",
        Signature::any(2, Volatility::Immutable),
        DataType::Float64,
        Arc::new(|_| Ok(Box::<Acc>::default())),
        vec![Arc::new(Field::new("interval_union_starts", list.clone(), true)),
             Arc::new(Field::new("interval_union_ends", list, true))],
    ))
}

#[cfg(test)]
mod tests {
    use super::union_length;
    #[test]
    fn union() {
        assert_eq!(union_length(vec![]), None);
        assert_eq!(union_length(vec![(0.0, 1.0)]), Some(1.0));
        // overlapping (parallel calls) count once; a gap is not counted
        assert!((union_length(vec![(2.2, 3.5), (2.0, 3.0), (5.6, 6.0)]).unwrap() - 1.9).abs() < 1e-9);
        // nested and touching
        assert_eq!(union_length(vec![(0.0, 10.0), (1.0, 2.0), (10.0, 11.0)]), Some(11.0));
    }
}
