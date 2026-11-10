-- T6a: what the agent does with kubectl inside bash (kubectl_targets UDF: "verb resource"), top 20, by stage.
SELECT tgt,
  count(*) FILTER (WHERE stage = 'diagnosis')  AS diagnosis,
  count(*) FILTER (WHERE stage = 'mitigation') AS mitigation,
  count(*) AS total
FROM (SELECT t.session_id, st.stage, unnest(kubectl_targets(t.command)) AS tgt
      FROM tool_calls t JOIN steps st USING (session_id, step_id) WHERE t.fn = 'bash')
GROUP BY tgt
ORDER BY total DESC, tgt
LIMIT 20;
