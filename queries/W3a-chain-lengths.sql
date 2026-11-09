-- W3a: refinement chains (runs of PromQL steps sharing a metric, see promql_steps) by length and stage.
WITH c AS (SELECT session_id, stage, chain_id, count(*) AS len FROM promql_steps GROUP BY 1, 2, 3)
SELECT CASE WHEN len >= 6 THEN '6+' ELSE CAST(len AS VARCHAR) END AS chain_len,
  count(*) FILTER (WHERE stage = 'diagnosis')  AS diagnosis_chains,
  count(*) FILTER (WHERE stage = 'mitigation') AS mitigation_chains,
  count(*) AS chains, sum(len) AS steps
FROM c
GROUP BY 1
ORDER BY 1;
