-- W8b: back-references. Runs of >=3 metric steps (up to 2 other steps between) where each next one queries the SAME
-- metric set as the previous metric step of the match (refine) vs a DIFFERENT one (switch). `M[!=M]` compares the
-- row's metric_key with the last row the match bound to M, not the previous row, so gap rows don't break it.
-- Plain SQL needs a recursive CTE for this (unbounded chain, each link conditioned on the previous link).
WITH g AS (
  SELECT session_id, stage,
    array_length(seq_match(pos, step_id, syms, 'M (.{0,2} M[=M]){2,}', metric_key)) AS refine,
    array_length(seq_match(pos, step_id, syms, 'M (.{0,2} M[!=M]){2,}', metric_key)) AS switch
  FROM step_symbols GROUP BY session_id, stage
), stg AS (
  SELECT session_id, 'diagnosis' AS stage, diagnosis_passed AS passed FROM sessions
  UNION ALL SELECT session_id, 'mitigation', mitigation_passed FROM sessions
)
SELECT s.stage, s.passed, count(*) AS sessions,
  count(*) FILTER (WHERE g.refine > 0) AS with_refine_run, count(*) FILTER (WHERE g.switch > 0) AS with_switch_run
FROM stg s JOIN g USING (session_id, stage)
GROUP BY 1, 2 ORDER BY 1, 2 NULLS LAST;
