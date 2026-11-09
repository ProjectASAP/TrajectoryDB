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
