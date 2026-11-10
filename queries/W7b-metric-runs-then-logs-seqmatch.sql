-- W7b (seq_match operator): same question as the baseline. array_length(m) = #matches, array_length(flatten(m)) = #matched rows.
-- (Not cardinality(m): DataFusion 54 treats nested lists as rectangular, outer len x first inner len.)
WITH hit AS (
  SELECT session_id, stage, array_length(m) AS matches, array_length(flatten(m)) AS matched_steps
  FROM (SELECT session_id, stage, seq_match(pos, step_id, sym, 'M{3,} L') AS m FROM step_symbols GROUP BY session_id, stage)
  WHERE array_length(m) > 0
), stg AS (
  SELECT session_id, 'diagnosis' AS stage, diagnosis_passed AS passed FROM sessions
  UNION ALL SELECT session_id, 'mitigation', mitigation_passed FROM sessions
)
SELECT s.stage, s.passed, count(*) AS sessions, count(h.session_id) AS with_pattern,
  coalesce(sum(h.matches), 0) AS matches, coalesce(sum(h.matched_steps), 0) AS matched_steps
FROM stg s LEFT JOIN hit h USING (session_id, stage)
GROUP BY 1, 2 ORDER BY 1, 2 NULLS LAST;
