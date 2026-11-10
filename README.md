# TrajectoryDB

A SQL engine for agent trajectories, prototyping [TrajectoryDB (arXiv 2609.07782)](https://arxiv.org/abs/2609.07782) on Apache DataFusion (stock, unmodified). It loads ATIF traces into Parquet tables and adds functions for what plain SQL handles badly: parsing tool calls (PromQL, bash, kubectl), sequence patterns over steps, comparing runs, and LLM-labelled meaning.

## Quick start

```
cargo build --release
./target/release/tdb ingest data          # ATIF trajectory.json + run-*/results.csv -> db/*.parquet
./target/release/tdb sql "SELECT run, count(*), avg(n_steps) FROM sessions GROUP BY run ORDER BY run"
```

`data/` is the HolmesGPT/SREGym trace set from ProjectASAP/ASAPWorkloads PR #10 (`traces/holmesgpt-sregym`), not checked in.

## Commands

| | |
|---|---|
| `tdb ingest <root>` | load traces, then build materialized views |
| `tdb sql "<stmts>"` | run `;`-separated SQL, print the last result |
| `tdb bench [--update]` | run `queries/*.sql`, diff against `.golden` |
| `tdb examples [--update]` | check every example in EXAMPLES.md against `examples/fixture` |
| `tdb synth <N> <dir>` | N× replicated corpus, for timing |

Env: `TDB_DB` (db dir), `TDB_DEFS` (startup SQL, default `defs.sql`), `TDB_LAZY=1` (ignore materialized views), `TDB_LLM_*` (see EXAMPLES §6).

## Where to look

- **[EXAMPLES.md](EXAMPLES.md)**: the user guide. Tables, functions and every kind of analysis, with runnable examples.
- `queries/`: full-corpus analyses with golden outputs (`T*` tool calls, `W*` walks and patterns, `X*` run comparisons, `llm/` LLM-based).
- `src/`: `main.rs` (CLI, ingest), `views.sql` (derived views), `promql.rs`/`shell.rs` (parsers), `seq.rs` (`seq_match`), `align.rs` (run alignment), `llm.rs` (`llm()`), `udf.rs` (`CREATE FUNCTION`).
- [PLAN.md](PLAN.md) (design and milestones), [STATUS.md](STATUS.md) (what is done, findings, open issues).
