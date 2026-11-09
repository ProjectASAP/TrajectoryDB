-- W1b: tree walk + linear replay. Everything the agent had seen when it issued the W1a target call:
-- all earlier steps' messages, calls and observations, plus the target step's message and calls (not its results).
-- Text truncated to 70 chars, newlines flattened. Empty agent messages are skipped.
WITH tgt AS (
  SELECT session_id, step_id FROM metric_results WHERE kind = 'too_large'
  ORDER BY session_id, step_id, call_idx LIMIT 1
), ev AS (
  SELECT session_id, step_id, 0 AS k, 0 AS i, source AS what, message AS txt FROM steps WHERE message <> ''
  UNION ALL
  SELECT session_id, step_id, 1, call_idx, 'call ' || fn, coalesce(query, command, args) FROM tool_calls
  UNION ALL
  SELECT session_id, step_id, 2, obs_idx, 'result', content FROM observations
)
SELECT ev.step_id, ev.what, substr(replace(ev.txt, chr(10), ' '), 1, 70) AS text
FROM ev JOIN tgt ON ev.session_id = tgt.session_id
WHERE ev.step_id < tgt.step_id OR (ev.step_id = tgt.step_id AND ev.k < 2)
ORDER BY ev.step_id, ev.k, ev.i;
