# TrajectoryDB prototype on DataFusion — plan

Source: Zheng & Yang, *TrajectoryDB: A New Database for Agent Trajectories*, arXiv 2609.07782 (vision paper: no system, no grammar, no benchmark).
Goal: build a Rust/DataFusion engine that answers **tool-call-centric** and **walk (tree/linear)** queries over agent trajectories. The first workload is HolmesGPT's PromQL usage.

## 1. Data we have (M0 survey, 2026-11-08)

`ProjectASAP/ASAPWorkloads` PR #10 → `traces/holmesgpt-sregym/run-0{1,2,3}/<problem>/trajectory.json` (fetched locally as branch `pr-10`).

- 601 trajectories, ATIF-v1.7, agent HolmesGPT 0.45.0 on Claude Sonnet 5.5, 190 MB. 14,576 steps (12,171 agent, 1,203 user, 1,202 system); the longest trajectory has 105 steps.
- Outcomes come from `run-*/results.csv` (problem_id, session_id, diagnosis_accuracy, diagnosis_passed, mitigation_passed). run-01 has 76 unknown.
- Each session has two **stages**, diagnosis and mitigation (`extra.holmesgpt.stages` and `step.extra.stage`). **There are no subagents.**
- Tool calls by count: bash 7,098 · fetch_pod_logs 3,744 · **get_metrics (PromQL) 2,738** · kubernetes_tabular_query 1,470 · get_alerts 825 · kubernetes_jq_query 698 · get_traces 230 · plus a long tail.
- PromQL also appears inside bash, as `curl .../api/v1/query_range?query=<urlencoded>` (rare, but it exists).
- Observations link back to calls through `source_call_id`. `extra.params` / `invocation` / `status` / `return_code` / `elapsed_seconds` are present.
- Per-step metrics: prompt, completion, cached, and cache-write tokens, plus `duration_s`.
- The PR already includes a Python pattern-mining viewer (`generated/analysis.json`, `index.json`). **We can use it as an oracle** for golden outputs on linear-pattern queries.

## 2. Shape of the trajectory data → what "walks" mean here

The tree is session → stage → step → tool_call → observation. Sessions also group into run × problem (the same problem appears up to three times, once per run).
- **Tree walk:** go up or down this hierarchy. For example, from an observation back to its call, step, stage, and session, or from a session down to all of its PromQL.
- **Linear walk:** move through ordered steps or calls inside a (session, stage). Examples are next/previous call, "within N steps after X", sequence patterns, and refinement chains.
- **Cross walk:** line up corresponding calls in sibling sessions (the same problem in different runs), or between the diagnosis and mitigation stages.

Subagent DAGs (the paper's partial-order case) are not in this data. Defer them until we have such a trace.

## 3. End-to-end behavior (v1; this definition will keep changing)

```
tdb ingest <trace-root>   -> ./db/*.parquet
tdb sql "<query>"         -> table on stdout
tdb bench                 -> runs queries/*.sql, diffs against queries/*.golden, prints timings
```

The **query catalog (`queries/`) is the spec**. Each entry is `NN-name.sql` plus `.golden`, with the English intent written as a comment at the top.

**Tool-call queries**
- T1 Tool mix per (stage, outcome): do passing sessions use get_metrics more?
- T2 PromQL anatomy: for each get_metrics call, extract metric names, functions, aggregation and `by` labels, label matchers, range window, and instant vs. range. Roll up per application and problem.
- T3 PromQL found in bash (URL-decode, then the same anatomy as T2), unioned with T2 to give one "all PromQL sent" view.
- T4 Failed or empty metric results: the status/error/empty share for each metric, and what tool the agent called next.
- T5 Repeated queries: identical (normalized) PromQL inside a session and across runs of the same problem. This is the redundancy signal relevant to ASAP caching and sketches.

**Walk queries**
- W1 Tree: for an observation, rebuild its ancestry and the full context before it (replay up to a call).
- W2 Linear pattern: `get_alerts → get_metrics → fetch_pod_logs` within ≤ k steps per stage, checked against the viewer oracle.
- W3 Refinement chain: consecutive get_metrics calls in a stage that share a metric while the selector, function, or window changes. Report chain length against outcome.
- W4 Stage crossing: metrics queried in diagnosis and queried again in mitigation (verification behavior).
- W5 Cross-run: for each problem, the PromQL common to all runs versus queries specific to one run.

## 4. Semantic queries: do we need them?

**Not for the MVP.** Almost all of this workload needs **deterministic extraction UDFs**, not LLM UDFs:
- `promql_metrics(q)`, `promql_funcs(q)`, `promql_matchers(q)`, `promql_range(q)`, `promql_normalize(q)`, built on the `promql-parser` crate (local clone at `../promql-parser`)
- `bash_head(cmd)` / `kubectl_verb(cmd)`, and `url_promql(cmd)` to pull PromQL out of curl calls

Cases where an LLM UDF would actually earn its place (post-MVP, C6/C7):
- S1 Was the observation used? Does the agent's next message or diagnosis rely on this metric result? (This separates wasted queries from useful ones.)
- S2 Query intent label, when the PromQL alone is ambiguous: saturation, error rate, latency, or existence check.
- S3 Unsupported claim, where the agent asserts something no prior observation shows (the paper's example).

S1 matters most for the ASAP angle ("which PromQL actually mattered"). Add it once T* and W* are green.

## 5. Build approach

- **Single Rust binary**: `serde_json` for ingest, `datafusion` for queries, and `promql-parser` for scalar UDFs that return lists or structs.
- **Tables** are sorted by `(session_id, step_id)`, and that order is declared via `file_sort_order`:
  - `sessions(session_id, run, problem_id, application, model, diag_passed, mitig_passed, accuracy, …tokens)`
  - `steps(session_id, step_id, stage, source, ts, prompt_tok, completion_tok, cached_tok, duration_s, message)`
  - `tool_calls(session_id, step_id, call_idx, call_id, fn, args JSON, + promoted cols: query, command, kind, namespace)`
  - `observations(session_id, step_id, call_id, status, error, return_code, elapsed_s, content)`
- **Walks, baseline first:** start with plain SQL, using window functions (`LEAD/LAG`, `ROW_NUMBER`) and self-joins. Only after that, add custom operators where the baseline is ugly or slow: a `seq_match` table function and a `walk()` ancestry/descendant operator that exploits the sort order (C2, C5). Both versions must produce the same golden output, and the timing difference is the result.
- **Scale:** 190 MB is too small to show performance differences. Add `tdb synth --x N`, which replicates sessions with fresh IDs, when timings start to matter.

## 6. Milestones

| M | Deliverable | Done when |
|---|-------------|-----------|
| M0 ✅ | Trace survey | Section 1 |
| M1 ✅ | Ingest and SQL | Parquet tables built; T1 and T5 (exact-string version) pass |
| M2 ✅ | PromQL UDFs | T2, T3, and T5 (normalized version) pass; parse coverage reported (% of get_metrics parsed) |
| M3 ✅ | Linear walks (SQL baseline) | W2 matches the viewer oracle; T4, W3, and W4 pass |
| M4 ✅ | Tree and cross walks | W1 and W5 pass |
| M5 ✅ | Walk operators | `seq_match` gives the same output as the baseline (W7a); timings at 1×, 10×, and 50× (STATUS). `next_in`/`replay` were dropped: `LEAD` over step views covers "next X" |
| M5b ✅ | ASAP workload + caching | `promql_asap_class`/`promql_asap_blocker`, T7, export/agent_promql.sql; `promql_normalize` modes + T5d |
| M5c ✅ | Other tools | bash/kubectl/log UDFs, `telemetry_calls`, T6/W6; W2c matches the viewer's bash table |
| M7 ✅ | Eager materialization (C1) | `step_symbols`/`telemetry_calls` built at ingest; `scale.sh` rerun |
| M6 | Semantic (optional) | S1 on a hand-labeled subset of about 50 |

**MVP = M1–M3**: every PromQL query HolmesGPT sent, dissected, with linear walks around those calls, from one engine, and `tdb bench` green.

## 7. Handoff discipline

- `PLAN.md`: this file. Only changes when scope changes.
- `STATUS.md`: append-only, one entry per checkpoint (done / numbers / next / open questions).
- `queries/`: the living e2e spec. Changing behavior means changing the catalog first.

## 8. Risks and open questions

- The ATIF stores **steps, not raw requests**, so prefix redundancy (C4) can't be measured directly. This is parked.
- About 25 `*_placeholder` tool names look like capture artifacts. Decide whether to filter them or keep them as a quality signal.
- Golden outputs for T*/W* are produced by our own first run. Cross-check them against the viewer oracle and spot-check by hand before freezing.
