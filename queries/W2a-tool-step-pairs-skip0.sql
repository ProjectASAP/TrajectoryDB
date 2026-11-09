-- W2a: linear walk. Consecutive agent (LLM) steps within one stage; each step labeled by its tool multiset
-- ("a + b ×2", names sorted). Pattern A → B where both steps call tools and 0 LLM steps lie between them.
-- Steps without tools still occupy a position (they break adjacency). Oracle: oracle/analysis.md, first table.
WITH lab AS (
  SELECT session_id, step_id, string_agg(CASE WHEN n > 1 THEN fn || ' ×' || n ELSE fn END, ' + ' ORDER BY fn) AS label
  FROM (SELECT session_id, step_id, fn, count(*) AS n FROM tool_calls GROUP BY 1, 2, 3)
  GROUP BY 1, 2
), seq AS (
  SELECT st.session_id, st.stage, lab.label,
         row_number() OVER (PARTITION BY st.session_id, st.stage ORDER BY st.step_id) AS pos
  FROM steps st LEFT JOIN lab USING (session_id, step_id)
  WHERE st.source = 'agent'
), pairs AS (
  SELECT a.session_id, a.label || ' → ' || b.label AS pattern
  FROM seq a JOIN seq b ON a.session_id = b.session_id AND a.stage = b.stage AND b.pos = a.pos + 1
  WHERE a.label IS NOT NULL AND b.label IS NOT NULL
)
SELECT p.pattern, count(*) AS occurrences, count(DISTINCT p.session_id) AS attempts, count(DISTINCT s.problem_id) AS problems
FROM pairs p JOIN sessions s USING (session_id)
GROUP BY p.pattern
ORDER BY attempts DESC, problems DESC, occurrences DESC, p.pattern
LIMIT 20;
