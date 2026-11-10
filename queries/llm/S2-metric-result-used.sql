-- S2 (LLM judgment): was a metric result used? For each diagnosis get_metrics call that returned data, ask whether the
-- agent's next step builds on it (follows up on a series, label value or anomaly in it). Full result text, no truncation.
-- Sample of 10 sessions (~25 calls); by diagnosis outcome.
WITH sample AS (SELECT session_id FROM sessions ORDER BY session_id LIMIT 10),
r AS (
  SELECT r.session_id, r.step_id, r.query, o.content
  FROM metric_results r JOIN observations o ON o.session_id = r.session_id AND o.call_id = r.call_id
  WHERE r.kind = 'data' AND r.stage = 'diagnosis' AND r.session_id IN (SELECT session_id FROM sample)
), nxt AS (
  SELECT r.session_id, r.step_id, min(st.step_id) AS next_step FROM r
  JOIN steps st ON st.session_id = r.session_id AND st.step_id > r.step_id AND st.source = 'agent' AND st.stage = 'diagnosis'
  GROUP BY 1, 2
), next_text AS (
  SELECT n.session_id, n.step_id,
    coalesce(max(st.message), '') || chr(10) || coalesce(string_agg(t.fn || '(' || coalesce(t.args, '') || ')', chr(10) ORDER BY t.call_idx), '') AS txt
  FROM nxt n JOIN steps st ON st.session_id = n.session_id AND st.step_id = n.next_step
  LEFT JOIN tool_calls t ON t.session_id = n.session_id AND t.step_id = n.next_step
  GROUP BY 1, 2
), judged AS (
  SELECT r.session_id, lower(trim(llm(
    'An SRE agent ran a PromQL query and got the result below, then took the next step shown. Did the next step use '
    || 'information from this result (e.g. follow up on a series, label value, pod or anomaly that appears in it)? '
    || 'Answer only yes or no.' || chr(10) || chr(10) || 'Query: ' || r.query || chr(10) || 'Result:' || chr(10) || r.content
    || chr(10) || chr(10) || 'Next step:' || chr(10) || x.txt))) AS used
  FROM r JOIN next_text x USING (session_id, step_id)
)
SELECT s.diagnosis_passed AS passed, count(*) AS data_results, count(*) FILTER (WHERE j.used LIKE 'yes%') AS used,
  count(*) FILTER (WHERE j.used NOT LIKE 'yes%' AND j.used NOT LIKE 'no%') AS unparsed
FROM judged j JOIN sessions s USING (session_id)
GROUP BY 1 ORDER BY 1 NULLS LAST;
