-- W7a (seq_match operator): same question as the baseline, as one row-pattern aggregate per (session, stage).
WITH hit AS (
  SELECT session_id, stage FROM step_symbols
  GROUP BY session_id, stage
  HAVING cardinality(seq_match(pos, step_id, sym, 'A .{0,2} M .{0,2} L')) > 0
), stg AS (
  SELECT session_id, 'diagnosis' AS stage, diagnosis_passed AS passed FROM sessions
  UNION ALL SELECT session_id, 'mitigation', mitigation_passed FROM sessions
)
SELECT s.stage, s.passed, count(*) AS sessions, count(h.session_id) AS with_pattern
FROM stg s LEFT JOIN hit h USING (session_id, stage)
GROUP BY 1, 2 ORDER BY 1, 2 NULLS LAST;
