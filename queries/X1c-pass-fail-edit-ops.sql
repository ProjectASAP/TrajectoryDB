-- X1c: aligned differences between passing (left) and failing (right) runs of the same problem, diagnosis stage.
-- '->M' = the failing run has an extra metric step, 'K>L' = passing did K where failing did L, 'K>-' = only passing.
WITH seqs AS (
  SELECT s.session_id, s.problem_id, s.diagnosis_passed AS ok, array_agg(coalesce(y.sym, '-') ORDER BY y.pos) AS q
  FROM sessions s JOIN step_symbols y ON y.session_id = s.session_id AND y.stage = 'diagnosis'
  GROUP BY 1, 2, 3
), ops AS (
  SELECT unnest(seq_align(p.q, f.q)) AS op
  FROM seqs p JOIN seqs f ON p.problem_id = f.problem_id AND p.ok AND NOT f.ok
)
SELECT op, count(*) AS n FROM ops WHERE op LIKE '%>%' GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 12;
