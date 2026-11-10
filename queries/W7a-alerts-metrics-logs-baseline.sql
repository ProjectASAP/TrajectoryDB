-- W7a (SQL baseline): sessions whose stage contains alerts → metrics → logs, each next element at most
-- 2 agent steps of gap later (step_symbols view). 3-way positional self-join. Must equal W7a-...-seqmatch.
WITH hit AS (
  SELECT DISTINCT a.session_id, a.stage
  FROM step_symbols a
  JOIN step_symbols m ON m.session_id = a.session_id AND m.stage = a.stage AND m.sym = 'M' AND m.pos BETWEEN a.pos + 1 AND a.pos + 3
  JOIN step_symbols l ON l.session_id = m.session_id AND l.stage = m.stage AND l.sym = 'L' AND l.pos BETWEEN m.pos + 1 AND m.pos + 3
  WHERE a.sym = 'A'
), stg AS (
  SELECT session_id, 'diagnosis' AS stage, diagnosis_passed AS passed FROM sessions
  UNION ALL SELECT session_id, 'mitigation', mitigation_passed FROM sessions
)
SELECT s.stage, s.passed, count(*) AS sessions, count(h.session_id) AS with_pattern
FROM stg s LEFT JOIN hit h USING (session_id, stage)
GROUP BY 1, 2 ORDER BY 1, 2 NULLS LAST;
