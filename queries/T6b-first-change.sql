-- T6b: first-violation style walk (paper: "the first step whose tool call violates a constraint").
-- Constraint: no mutating kubectl (k8s_change) during diagnosis. Per stage: sessions with a change, and how many
-- agent steps into the stage the first change happened; split by that stage's outcome.
WITH pos AS (
  SELECT session_id, stage, step_id, row_number() OVER (PARTITION BY session_id, stage ORDER BY step_id) AS n
  FROM steps WHERE source = 'agent'
), first AS (
  SELECT tc.session_id, tc.stage, min(p.n) AS first_change_at
  FROM telemetry_calls tc JOIN pos p USING (session_id, stage, step_id)
  WHERE tc.modality = 'k8s_change' GROUP BY 1, 2
), stg AS (
  SELECT session_id, 'diagnosis' AS stage, diagnosis_passed AS passed FROM sessions
  UNION ALL SELECT session_id, 'mitigation', mitigation_passed FROM sessions
)
SELECT s.stage, s.passed, count(*) AS sessions, count(f.session_id) AS with_change,
  round(avg(f.first_change_at), 1) AS avg_first_change_step, max(f.first_change_at) AS max_first_change_step
FROM stg s LEFT JOIN first f USING (session_id, stage)
GROUP BY 1, 2
ORDER BY 1, 2 NULLS LAST;
