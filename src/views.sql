-- Views registered on every tdb context. Statements are split on semicolons, so never put one inside a string or comment.

-- promql: every PromQL string the agent sent: native get_metrics + query= params inside bash (curl to /api/v1/query*).
CREATE VIEW promql AS
SELECT session_id, step_id, call_idx, call_id, 'get_metrics' AS via, query FROM tool_calls WHERE fn = 'get_metrics'
UNION ALL
SELECT session_id, step_id, call_idx, call_id, 'bash_url' AS via, unnest(url_promql(command)) AS query
FROM tool_calls WHERE fn = 'bash' AND command LIKE '%query=%';

-- metric_results: each get_metrics call with its result classified from content (observations.status is unreliable).
CREATE VIEW metric_results AS
SELECT t.session_id, t.step_id, t.call_idx, t.call_id, st.stage, t.query,
  CASE
    WHEN o.content LIKE 'Refusing to run this tool call because it has already been called%' THEN 'refused_duplicate'
    WHEN o.content LIKE 'The tool call result is too large%' THEN 'too_large'
    WHEN o.content LIKE 'Timed out%' THEN 'timeout'
    WHEN o.content LIKE '%Error querying%' THEN 'backend_error'
    WHEN o.content LIKE '%''result'': []%' THEN 'empty'
    WHEN o.content LIKE '{''resultType''%' THEN 'data'
    ELSE 'other'
  END AS kind
FROM tool_calls t
JOIN steps st USING (session_id, step_id)
LEFT JOIN observations o ON o.session_id = t.session_id AND o.call_id = t.call_id
WHERE t.fn = 'get_metrics';

-- promql_steps: one row per agent step that issued get_metrics, with the union of what its queries touch.
-- linked = shares a metric name with the previous PromQL step of the same session+stage (other tools may sit between).
-- chain_id numbers maximal runs of linked steps (gaps-and-islands). Discovery-only steps (no metric name) never link.
CREATE VIEW promql_steps AS
WITH s AS (
  SELECT session_id, stage, step_id, count(*) AS calls,
    array_distinct(flatten(array_agg(promql_metrics(query))  FILTER (WHERE promql_error(query) IS NULL))) AS metrics,
    array_distinct(flatten(array_agg(promql_funcs(query))    FILTER (WHERE promql_error(query) IS NULL))) AS funcs,
    array_distinct(flatten(array_agg(promql_aggs(query))     FILTER (WHERE promql_error(query) IS NULL))) AS aggs,
    array_distinct(flatten(array_agg(promql_ranges(query))   FILTER (WHERE promql_error(query) IS NULL))) AS ranges,
    array_distinct(flatten(array_agg(promql_matchers(query)) FILTER (WHERE promql_error(query) IS NULL))) AS matchers
  FROM metric_results GROUP BY session_id, stage, step_id
), l AS (
  SELECT *, row_number() OVER w AS pos,
    lag(metrics) OVER w AS prev_metrics, lag(funcs) OVER w AS prev_funcs, lag(aggs) OVER w AS prev_aggs,
    lag(ranges) OVER w AS prev_ranges, lag(matchers) OVER w AS prev_matchers
  FROM s WINDOW w AS (PARTITION BY session_id, stage ORDER BY step_id)
), k AS (
  SELECT *, coalesce(array_has_any(metrics, prev_metrics), false) AS linked FROM l
)
SELECT *, sum(CASE WHEN linked THEN 0 ELSE 1 END) OVER (PARTITION BY session_id, stage ORDER BY step_id) AS chain_id
FROM k;
