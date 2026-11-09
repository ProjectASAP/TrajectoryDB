-- W3b: on each linked step pair inside a chain, which parts of the PromQL changed (sets compared order-insensitively).
-- Percentages are over linked pairs. A pair can change several parts. 'unchanged' = all five sets identical.
WITH p AS (
  SELECT stage,
    array_sort(metrics)  IS DISTINCT FROM array_sort(prev_metrics)  AS d_metrics,
    array_sort(matchers) IS DISTINCT FROM array_sort(prev_matchers) AS d_matchers,
    array_sort(ranges)   IS DISTINCT FROM array_sort(prev_ranges)   AS d_ranges,
    array_sort(funcs)    IS DISTINCT FROM array_sort(prev_funcs)    AS d_funcs,
    array_sort(aggs)     IS DISTINCT FROM array_sort(prev_aggs)     AS d_aggs
  FROM promql_steps WHERE linked
)
SELECT stage, count(*) AS linked_pairs,
  round(avg(CASE WHEN d_metrics  THEN 100.0 ELSE 0 END), 1) AS pct_metrics_changed,
  round(avg(CASE WHEN d_matchers THEN 100.0 ELSE 0 END), 1) AS pct_matchers_changed,
  round(avg(CASE WHEN d_ranges   THEN 100.0 ELSE 0 END), 1) AS pct_ranges_changed,
  round(avg(CASE WHEN d_funcs    THEN 100.0 ELSE 0 END), 1) AS pct_funcs_changed,
  round(avg(CASE WHEN d_aggs     THEN 100.0 ELSE 0 END), 1) AS pct_aggs_changed,
  round(avg(CASE WHEN NOT (d_metrics OR d_matchers OR d_ranges OR d_funcs OR d_aggs) THEN 100.0 ELSE 0 END), 1) AS pct_unchanged
FROM p
GROUP BY stage
ORDER BY stage;
