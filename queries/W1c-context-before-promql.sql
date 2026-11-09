-- W1c: tree walk, aggregated. Context the agent carried when it issued PromQL: for each get_metrics call,
-- prior steps in the session, cumulative bytes of earlier tool results, and the issuing step's prompt/cached tokens.
WITH ob AS (SELECT session_id, step_id, sum(length(content)) AS b FROM observations GROUP BY 1, 2),
cum AS (
  SELECT st.session_id, st.step_id, st.prompt_tok, st.cached_tok,
    row_number() OVER (PARTITION BY st.session_id ORDER BY st.step_id) - 1 AS prior_steps,
    -- inline spec on purpose: DataFusion 54 drops PARTITION BY for OVER (w ROWS ...) on a named window
    coalesce(sum(coalesce(ob.b, 0)) OVER (PARTITION BY st.session_id ORDER BY st.step_id
                                          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS prior_result_bytes
  FROM steps st LEFT JOIN ob USING (session_id, step_id)
)
SELECT m.stage, count(*) AS promql_calls,
  round(avg(c.prior_steps), 1) AS avg_prior_steps,
  round(avg(c.prior_result_bytes) / 1024, 1) AS avg_prior_result_kib,
  round(avg(c.prompt_tok)) AS avg_prompt_tok,
  round(sum(c.cached_tok) * 100.0 / sum(c.prompt_tok), 1) AS cached_pct
FROM metric_results m JOIN cum c USING (session_id, step_id)
GROUP BY m.stage
ORDER BY m.stage;
