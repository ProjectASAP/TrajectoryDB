-- S1b (LLM extraction x cross-run): do runs of the same problem blame the same workload? Same prompt as S1a, so every
-- call is a cache hit after S1a. Pairs by diagnosis outcome.
WITH sample AS (SELECT session_id FROM sessions ORDER BY session_id LIMIT 40),
final AS (
  SELECT st.session_id, st.message FROM steps st
  JOIN (SELECT session_id, max(step_id) AS step_id FROM steps
        WHERE source = 'agent' AND stage = 'diagnosis' AND length(message) > 0 GROUP BY 1) f USING (session_id, step_id)
  WHERE st.session_id IN (SELECT session_id FROM sample)
), blamed AS (
  SELECT f.session_id, s.problem_id, s.run, s.diagnosis_passed AS ok, lower(trim(llm(
    'Below is an SRE agent''s final diagnosis of a Kubernetes incident. Reply with only the name of the single workload '
    || '(deployment, statefulset or service name, without pod hash suffixes) it blames as the root cause, in lowercase. '
    || 'If it blames no specific workload, reply NONE.' || chr(10) || chr(10) || f.message))) AS blamed
  FROM final f JOIN sessions s USING (session_id)
)
SELECT CASE WHEN a.ok AND b.ok THEN 'both_pass' WHEN NOT a.ok AND NOT b.ok THEN 'both_fail' ELSE 'one_pass' END AS outcomes,
  count(*) AS pairs, count(*) FILTER (WHERE a.blamed = b.blamed) AS same_blame
FROM blamed a JOIN blamed b ON a.problem_id = b.problem_id AND a.run < b.run
WHERE a.ok IS NOT NULL AND b.ok IS NOT NULL
GROUP BY 1 ORDER BY 1;
