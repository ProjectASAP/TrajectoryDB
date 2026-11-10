-- W7b (SQL baseline): per (session, stage), non-overlapping matches of `M{3,} L` (>=3 consecutive metric steps, then a
-- logs step). The unbounded run needs gaps-and-islands (window functions) instead of W7a's fixed self-join.
-- Must equal W7b-...-seqmatch. Note: runs of M are maximal, so leftmost non-overlapping matches = qualifying islands.
WITH runs AS (
  SELECT session_id, stage, min(pos) AS lo, max(pos) AS hi
  FROM (SELECT session_id, stage, pos,
          pos - row_number() OVER (PARTITION BY session_id, stage ORDER BY pos) AS island
        FROM step_symbols WHERE sym = 'M')
  GROUP BY session_id, stage, island HAVING count(*) >= 3
), hit AS (
  SELECT r.session_id, r.stage, count(*) AS matches, sum(r.hi - r.lo + 2) AS matched_steps
  FROM runs r JOIN step_symbols n ON n.session_id = r.session_id AND n.stage = r.stage AND n.pos = r.hi + 1 AND n.sym = 'L'
  GROUP BY 1, 2
), stg AS (
  SELECT session_id, 'diagnosis' AS stage, diagnosis_passed AS passed FROM sessions
  UNION ALL SELECT session_id, 'mitigation', mitigation_passed FROM sessions
)
SELECT s.stage, s.passed, count(*) AS sessions, count(h.session_id) AS with_pattern,
  coalesce(sum(h.matches), 0) AS matches, coalesce(sum(h.matched_steps), 0) AS matched_steps
FROM stg s LEFT JOIN hit h USING (session_id, stage)
GROUP BY 1, 2 ORDER BY 1, 2 NULLS LAST;
