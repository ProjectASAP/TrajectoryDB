-- T4b: linear walk. After a get_metrics result of each kind, what is the agent's NEXT PromQL
-- (first get_metrics step strictly later in the same session+stage; calls in the same step ran as one batch)?
-- Relation, first match wins:
--   none: no later PromQL in the stage; same_query: exact query again; same_metric: overlapping metric names;
--   to_discovery: next query names no metric (e.g. {__name__=~".*lock.*"}, count by (__name__) (...));
--   discovery_to_named: this query named no metric, next one does; different_metric: otherwise.
-- 'data' is the baseline. Unparseable queries have NULL metrics and fall into different_metric.
WITH nxt AS (
  SELECT a.session_id, a.call_id, min(b.step_id) AS next_step
  FROM metric_results a JOIN metric_results b
    ON a.session_id = b.session_id AND a.stage = b.stage AND b.step_id > a.step_id
  GROUP BY a.session_id, a.call_id
), rel AS (
  SELECT a.kind, max(cardinality(promql_metrics(a.query))) = 0 AS is_discovery,
    CASE WHEN n.next_step IS NULL THEN 'none'
         WHEN bool_or(b.query = a.query) THEN 'same_query'
         WHEN bool_or(array_has_any(promql_metrics(a.query), promql_metrics(b.query))) THEN 'same_metric'
         WHEN bool_or(cardinality(promql_metrics(b.query)) = 0) THEN 'to_discovery'
         WHEN max(cardinality(promql_metrics(a.query))) = 0 THEN 'discovery_to_named'
         ELSE 'different_metric' END AS next_promql
  FROM metric_results a
  LEFT JOIN nxt n ON n.session_id = a.session_id AND n.call_id = a.call_id
  LEFT JOIN metric_results b ON b.session_id = a.session_id AND b.step_id = n.next_step
  GROUP BY a.kind, a.session_id, a.call_id, n.next_step
)
SELECT kind, count(*) AS calls,
  sum(CASE WHEN is_discovery THEN 1 ELSE 0 END) AS discovery_calls,
  round(avg(CASE WHEN next_promql = 'same_query' THEN 100.0 ELSE 0 END), 1)       AS pct_same_query,
  round(avg(CASE WHEN next_promql = 'same_metric' THEN 100.0 ELSE 0 END), 1)      AS pct_same_metric,
  round(avg(CASE WHEN next_promql = 'to_discovery' THEN 100.0 ELSE 0 END), 1)     AS pct_to_discovery,
  round(avg(CASE WHEN next_promql = 'discovery_to_named' THEN 100.0 ELSE 0 END), 1) AS pct_discovery_to_named,
  round(avg(CASE WHEN next_promql = 'different_metric' THEN 100.0 ELSE 0 END), 1) AS pct_different_metric,
  round(avg(CASE WHEN next_promql = 'none' THEN 100.0 ELSE 0 END), 1)             AS pct_none
FROM rel
GROUP BY kind
ORDER BY calls DESC, kind;
