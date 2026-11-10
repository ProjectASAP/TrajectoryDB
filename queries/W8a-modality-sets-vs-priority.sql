-- W8a: how lossy is one-symbol-per-step? W7a's pattern over the priority symbol (sym) vs the step's full symbol set
-- (syms; a step with get_alerts + get_metrics is both A and M). 37% of agent steps carry more than one modality.
WITH g AS (
  SELECT session_id, stage,
    array_length(seq_match(pos, step_id, sym,  'A .{0,2} M .{0,2} L')) AS by_priority,
    array_length(seq_match(pos, step_id, syms, 'A .{0,2} M .{0,2} L')) AS by_set
  FROM step_symbols GROUP BY session_id, stage
), stg AS (
  SELECT session_id, 'diagnosis' AS stage, diagnosis_passed AS passed FROM sessions
  UNION ALL SELECT session_id, 'mitigation', mitigation_passed FROM sessions
)
SELECT s.stage, s.passed, count(*) AS sessions,
  count(*) FILTER (WHERE g.by_priority > 0) AS with_pattern_priority, count(*) FILTER (WHERE g.by_set > 0) AS with_pattern_set
FROM stg s JOIN g USING (session_id, stage)
GROUP BY 1, 2 ORDER BY 1, 2 NULLS LAST;
