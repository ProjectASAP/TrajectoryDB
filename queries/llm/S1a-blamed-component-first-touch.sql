-- S1a (LLM extraction -> walk): which workload does each final diagnosis blame, and how early in the diagnosis stage did
-- the agent first touch it (first tool call whose arguments mention it)? Sample of 40 sessions; drop the LIMIT for all
-- (601 calls). Dry run: TDB_LLM_DRY=1 reports how many uncached calls it would make.
WITH sample AS (SELECT session_id FROM sessions ORDER BY session_id LIMIT 40),
final AS (
  SELECT st.session_id, st.message FROM steps st
  JOIN (SELECT session_id, max(step_id) AS step_id FROM steps
        WHERE source = 'agent' AND stage = 'diagnosis' AND length(message) > 0 GROUP BY 1) f USING (session_id, step_id)
  WHERE st.session_id IN (SELECT session_id FROM sample)
), blamed AS (
  SELECT session_id, lower(trim(llm(
    'Below is an SRE agent''s final diagnosis of a Kubernetes incident. Reply with only the name of the single workload '
    || '(deployment, statefulset or service name, without pod hash suffixes) it blames as the root cause, in lowercase. '
    || 'If it blames no specific workload, reply NONE.' || chr(10) || chr(10) || message))) AS blamed
  FROM final
), touch AS (
  SELECT b.session_id, min(y.pos) AS first_touch
  FROM blamed b
  JOIN tool_calls t ON t.session_id = b.session_id AND strpos(lower(coalesce(t.args, '')), b.blamed) > 0
  JOIN step_symbols y ON y.session_id = t.session_id AND y.step_id = t.step_id AND y.stage = 'diagnosis'
  WHERE b.blamed <> 'none' GROUP BY 1
), len AS (SELECT session_id, max(pos) AS steps FROM step_symbols WHERE stage = 'diagnosis' GROUP BY 1)
SELECT s.diagnosis_passed AS passed, count(*) AS sessions, count(*) FILTER (WHERE b.blamed = 'none') AS no_blame,
  count(t.first_touch) AS touched, round(avg(t.first_touch), 1) AS avg_first_touch,
  round(avg(t.first_touch * 1.0 / l.steps), 2) AS avg_first_touch_frac
FROM blamed b JOIN sessions s ON s.session_id = b.session_id
LEFT JOIN touch t ON t.session_id = b.session_id LEFT JOIN len l ON l.session_id = b.session_id
GROUP BY 1 ORDER BY 1 NULLS LAST;
