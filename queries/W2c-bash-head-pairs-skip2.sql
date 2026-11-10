-- W2c: linear walk over bash command heads (bash_heads UDF). Each head occurrence in an agent step is an
-- attribute; pattern A → B pairs every head of one step with every head of a step up to 2 LLM steps later
-- (same session+stage). Oracle: oracle/analysis.md "Bash commands · One selected attribute · max skipped steps 2"
-- (attempts/problems match exactly; occurrences within 0.03% — the splitter is hand-rolled).
WITH h AS (SELECT session_id, step_id, unnest(bash_heads(command)) AS head FROM tool_calls WHERE fn = 'bash'),
seq AS (
  SELECT session_id, stage, step_id, row_number() OVER (PARTITION BY session_id, stage ORDER BY step_id) AS pos
  FROM steps WHERE source = 'agent'
),
hp AS (SELECT seq.*, h.head FROM seq JOIN h USING (session_id, step_id))
SELECT a.head || ' → ' || b.head AS pattern, count(*) AS occurrences,
  count(DISTINCT a.session_id) AS attempts, count(DISTINCT s.problem_id) AS problems
FROM hp a JOIN hp b ON a.session_id = b.session_id AND a.stage = b.stage AND b.pos BETWEEN a.pos + 1 AND a.pos + 3
JOIN sessions s ON s.session_id = a.session_id
GROUP BY 1
ORDER BY attempts DESC, problems DESC, occurrences DESC, pattern
LIMIT 20;
