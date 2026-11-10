#!/bin/bash
# Time baseline vs operator query pairs on N-times replicated data. Usage: scripts/scale.sh [N...] (default 1 10 50)
# Prints median wall-clock ms of 3 runs per query (includes ~process startup; see the 1x row for that floor).
set -e
cd "$(dirname "$0")/.."
T=./target/release/tdb
for n in "${@:-1 10 50}"; do
  d=target/synth-x$n
  [ -d "$d" ] || $T synth "$n" "$d" >/dev/null
  for q in queries/*-baseline.sql queries/*-seqmatch.sql; do
    ms=$(for i in 1 2 3; do s=$(date +%s%N); TDB_DB=$d $T sql "$(cat "$q")" >/dev/null; echo $(( ($(date +%s%N) - s) / 1000000 )); done | sort -n | sed -n 2p)
    printf "%4sx  %7s ms  %s\n" "$n" "$ms" "$(basename "$q" .sql)"
  done
done
