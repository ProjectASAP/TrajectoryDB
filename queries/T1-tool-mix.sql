-- T1: tool mix per stage, split by that stage's outcome (diagnosis_passed / mitigation_passed).
-- calls per session = calls / sessions in that outcome group. NULL outcome = unknown (run-01).
WITH s AS (
  SELECT session_id, 'diagnosis' AS stage, diagnosis_passed AS passed FROM sessions
  UNION ALL SELECT session_id, 'mitigation', mitigation_passed FROM sessions
), grp AS (
  SELECT stage, passed, count(*) AS n FROM s GROUP BY stage, passed
), c AS (
  SELECT t.fn, st.stage, s.passed, count(*) AS calls
  FROM tool_calls t JOIN steps st USING (session_id, step_id)
  JOIN s ON s.session_id = t.session_id AND s.stage = st.stage
  GROUP BY t.fn, st.stage, s.passed
)
SELECT c.fn, c.stage,
  round(sum(CASE WHEN c.passed THEN c.calls * 1.0 / g.n END), 2)     AS per_sess_pass,
  round(sum(CASE WHEN NOT c.passed THEN c.calls * 1.0 / g.n END), 2) AS per_sess_fail,
  round(sum(CASE WHEN c.passed IS NULL THEN c.calls * 1.0 / g.n END), 2) AS per_sess_unknown,
  sum(c.calls) AS calls
FROM c JOIN grp g ON g.stage = c.stage AND g.passed IS NOT DISTINCT FROM c.passed
GROUP BY c.fn, c.stage
ORDER BY calls DESC, c.fn, c.stage;
