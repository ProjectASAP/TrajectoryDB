-- W1a: tree walk upward. For a target observation (the first 'too_large' get_metrics result, by session/step/call),
-- return its ancestry: observation -> tool call -> step -> stage -> session (+ problem outcome).
WITH tgt AS (
  SELECT session_id, step_id, call_id, stage FROM metric_results WHERE kind = 'too_large'
  ORDER BY session_id, step_id, call_idx LIMIT 1
)
SELECT lvl, level, id, detail FROM (
  SELECT 1 AS lvl, 'observation' AS level, o.call_id AS id, substr(replace(o.content, chr(10), ' '), 1, 70) AS detail
  FROM observations o JOIN tgt ON o.session_id = tgt.session_id AND o.call_id = tgt.call_id
  UNION ALL
  SELECT 2, 'tool_call', t.call_id, t.fn || ': ' || t.query
  FROM tool_calls t JOIN tgt ON t.session_id = tgt.session_id AND t.call_id = tgt.call_id
  UNION ALL
  SELECT 3, 'step', CAST(st.step_id AS VARCHAR),
         st.source || ' ' || st.ts || ' prompt_tok=' || CAST(st.prompt_tok AS VARCHAR) || ' cached_tok=' || CAST(st.cached_tok AS VARCHAR)
  FROM steps st JOIN tgt ON st.session_id = tgt.session_id AND st.step_id = tgt.step_id
  UNION ALL
  SELECT 4, 'stage', tgt.stage, 'steps ' || CAST(min(st.step_id) AS VARCHAR) || '..' || CAST(max(st.step_id) AS VARCHAR)
  FROM steps st JOIN tgt ON st.session_id = tgt.session_id AND st.stage = tgt.stage
  GROUP BY tgt.stage
  UNION ALL
  SELECT 5, 'session', s.session_id,
         s.run || ' ' || s.problem_id || ' diag_passed=' || coalesce(CAST(s.diagnosis_passed AS VARCHAR), 'unknown')
  FROM sessions s JOIN tgt ON s.session_id = tgt.session_id
)
ORDER BY lvl;
