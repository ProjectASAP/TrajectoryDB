#!/bin/bash
# Diff W2 goldens against the HolmesGPT viewer's pattern tables (oracle/analysis.md, from ASAPWorkloads PR #10).
cd "$(dirname "$0")/.." || exit 1
rows() { sed -n "/$1/,/^## /p" oracle/analysis.md | grep inspect | sed 's/\[inspect\]([^)]*) //' | tr -s ' '; }
ours() { sed -n '4,23p' "$1" | tr -s ' '; }
rc=0
diff <(rows 'Tools · All selected attributes · max skipped steps 0') <(ours queries/W2a-tool-step-pairs-skip0.golden) && echo "W2a == oracle" || rc=1
diff <(rows 'Tools · All selected attributes · max skipped steps 2') <(ours queries/W2b-tool-step-pairs-skip2.golden) && echo "W2b == oracle" || rc=1
# W2c: bash heads come from a hand-rolled splitter, so compare pattern/attempts/problems only (occurrences drift <0.03%).
cols() { awk -F'|' 'NF>4 {print $2 "|" $4 "|" $5}' | tr -s ' '; }
diff <(rows 'Bash commands · One selected attribute · max skipped steps 2' | cols) <(ours queries/W2c-bash-head-pairs-skip2.golden | cols) && echo "W2c == oracle (pattern, attempts, problems)" || rc=1
exit $rc
