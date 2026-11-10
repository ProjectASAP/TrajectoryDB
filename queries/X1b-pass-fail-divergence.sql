-- X1b: where a passing and a failing run of the same problem first diverge (diagnosis stage), and what each did there.
-- diverge_at = 1-based position after the common prefix; 'end' = that run had already stopped.
WITH seqs AS (
  SELECT s.session_id, s.problem_id, s.diagnosis_passed AS ok, array_agg(coalesce(y.sym, '-') ORDER BY y.pos) AS q
  FROM sessions s JOIN step_symbols y ON y.session_id = s.session_id AND y.stage = 'diagnosis'
  GROUP BY 1, 2, 3
), mixed AS (
  SELECT p.q AS pass_q, f.q AS fail_q, seq_prefix(p.q, f.q) AS pre
  FROM seqs p JOIN seqs f ON p.problem_id = f.problem_id AND p.ok AND NOT f.ok
)
SELECT coalesce(pass_q[pre + 1], 'end') AS pass_next, coalesce(fail_q[pre + 1], 'end') AS fail_next,
  count(*) AS pairs, round(avg(pre + 1), 1) AS avg_diverge_at
FROM mixed GROUP BY 1, 2 ORDER BY pairs DESC, 1, 2;
