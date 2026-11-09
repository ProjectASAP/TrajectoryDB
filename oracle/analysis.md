# Recorded trajectory patterns

Counts describe the selected attributes of recorded occurrences across all stages and runs. Patterns are observed subsequences, not dependencies. Batch membership does not establish parallel execution.

[Explore patterns and full trajectories](trajectory-viewer.html) · [Machine-readable counts and witnesses](analysis.json)

Each pattern reports occurrences, distinct attempts and distinct known problems. Default two-step patterns stay within one stage segment; gaps count intervening LLM steps, including turns without tools. Individual attribute occurrences and projected whole batches are different counting units. Repeated fields and duplicate calls remain visible.

## Tools · All selected attributes · max skipped steps 0

Complete search; 635 evaluated frequent patterns; 8,509 joint source matches. The table shows the first 20; JSON retains evaluated counts, source examples, support and work limits.

| Selected attributes | Occurrences | Attempts | Problems |
| --- | --- | --- | --- |
| [inspect](trajectory-viewer.html#preset=0&pattern=0) bash → bash | 3180 | 530 | 199 |
| [inspect](trajectory-viewer.html#preset=0&pattern=1) get_alerts + kubernetes_tabular_query → bash | 206 | 203 | 109 |
| [inspect](trajectory-viewer.html#preset=0&pattern=2) bash → bash + fetch_pod_logs | 219 | 173 | 109 |
| [inspect](trajectory-viewer.html#preset=0&pattern=3) bash + fetch_pod_logs → bash | 217 | 170 | 108 |
| [inspect](trajectory-viewer.html#preset=0&pattern=4) bash → kubernetes_tabular_query | 222 | 169 | 105 |
| [inspect](trajectory-viewer.html#preset=0&pattern=5) get_alerts + kubernetes_tabular_query → bash + fetch_pod_logs | 123 | 123 | 82 |
| [inspect](trajectory-viewer.html#preset=0&pattern=6) bash + fetch_pod_logs → bash + fetch_pod_logs | 128 | 99 | 73 |
| [inspect](trajectory-viewer.html#preset=0&pattern=7) kubernetes_tabular_query → bash | 115 | 91 | 67 |
| [inspect](trajectory-viewer.html#preset=0&pattern=8) bash → fetch_pod_logs | 131 | 76 | 60 |
| [inspect](trajectory-viewer.html#preset=0&pattern=9) bash → bash + get_metrics | 88 | 71 | 55 |
| [inspect](trajectory-viewer.html#preset=0&pattern=10) bash + get_metrics → bash | 61 | 55 | 43 |
| [inspect](trajectory-viewer.html#preset=0&pattern=11) kubernetes_tabular_query → kubernetes_tabular_query | 71 | 49 | 38 |
| [inspect](trajectory-viewer.html#preset=0&pattern=12) bash → fetch_pod_logs ×2 | 55 | 49 | 34 |
| [inspect](trajectory-viewer.html#preset=0&pattern=13) fetch_pod_logs → bash | 96 | 42 | 35 |
| [inspect](trajectory-viewer.html#preset=0&pattern=14) bash → bash ×2 | 49 | 41 | 35 |
| [inspect](trajectory-viewer.html#preset=0&pattern=15) fetch_pod_logs → fetch_pod_logs | 81 | 40 | 31 |
| [inspect](trajectory-viewer.html#preset=0&pattern=16) bash ×2 → bash | 44 | 39 | 35 |
| [inspect](trajectory-viewer.html#preset=0&pattern=17) fetch_pod_logs ×2 → bash | 43 | 37 | 31 |
| [inspect](trajectory-viewer.html#preset=0&pattern=18) bash → kubernetes_jq_query | 48 | 37 | 29 |
| [inspect](trajectory-viewer.html#preset=0&pattern=19) bash → fetch_pod_logs + get_metrics | 50 | 35 | 25 |
## Tools · All selected attributes · max skipped steps 2

Complete search; 1,201 evaluated frequent patterns; 23,231 joint source matches. The table shows the first 20; JSON retains evaluated counts, source examples, support and work limits.

| Selected attributes | Occurrences | Attempts | Problems |
| --- | --- | --- | --- |
| [inspect](trajectory-viewer.html#preset=1&pattern=0) bash → bash | 7849 | 540 | 202 |
| [inspect](trajectory-viewer.html#preset=1&pattern=1) get_alerts + kubernetes_tabular_query → bash | 610 | 348 | 169 |
| [inspect](trajectory-viewer.html#preset=1&pattern=2) get_alerts + kubernetes_tabular_query → bash + fetch_pod_logs | 321 | 244 | 141 |
| [inspect](trajectory-viewer.html#preset=1&pattern=3) bash + fetch_pod_logs → bash | 556 | 217 | 130 |
| [inspect](trajectory-viewer.html#preset=1&pattern=4) bash → bash + fetch_pod_logs | 569 | 211 | 125 |
| [inspect](trajectory-viewer.html#preset=1&pattern=5) bash → kubernetes_tabular_query | 730 | 205 | 117 |
| [inspect](trajectory-viewer.html#preset=1&pattern=6) bash + fetch_pod_logs → bash + fetch_pod_logs | 251 | 141 | 101 |
| [inspect](trajectory-viewer.html#preset=1&pattern=7) bash → fetch_pod_logs | 500 | 141 | 99 |
| [inspect](trajectory-viewer.html#preset=1&pattern=8) kubernetes_tabular_query → bash | 320 | 117 | 81 |
| [inspect](trajectory-viewer.html#preset=1&pattern=9) bash → bash + get_metrics | 286 | 102 | 72 |
| [inspect](trajectory-viewer.html#preset=1&pattern=10) bash → fetch_pod_logs ×2 | 240 | 102 | 70 |
| [inspect](trajectory-viewer.html#preset=1&pattern=11) kubernetes_tabular_query → kubernetes_tabular_query | 167 | 84 | 56 |
| [inspect](trajectory-viewer.html#preset=1&pattern=12) bash → fetch_pod_logs + get_metrics | 221 | 83 | 58 |
| [inspect](trajectory-viewer.html#preset=1&pattern=13) bash → fetch_pod_logs + kubernetes_tabular_query | 188 | 80 | 62 |
| [inspect](trajectory-viewer.html#preset=1&pattern=14) bash + get_metrics → bash | 155 | 74 | 58 |
| [inspect](trajectory-viewer.html#preset=1&pattern=15) get_alerts + kubernetes_tabular_query → bash + fetch_pod_logs ×2 | 69 | 68 | 47 |
| [inspect](trajectory-viewer.html#preset=1&pattern=16) bash + fetch_pod_logs → bash + get_metrics | 84 | 61 | 42 |
| [inspect](trajectory-viewer.html#preset=1&pattern=17) bash ×2 → bash | 138 | 59 | 49 |
| [inspect](trajectory-viewer.html#preset=1&pattern=18) fetch_pod_logs ×2 → bash | 115 | 58 | 47 |
| [inspect](trajectory-viewer.html#preset=1&pattern=19) bash → kubernetes_jq_query | 121 | 56 | 46 |
## Bash commands · One selected attribute · max skipped steps 2

Complete search; 640 evaluated frequent patterns; 136,408 joint source matches. The table shows the first 20; JSON retains evaluated counts, source examples, support and work limits.

| Selected attributes | Occurrences | Attempts | Problems |
| --- | --- | --- | --- |
| [inspect](trajectory-viewer.html#preset=2&pattern=0) kubectl get → kubectl get | 22400 | 558 | 208 |
| [inspect](trajectory-viewer.html#preset=2&pattern=1) tail → kubectl get | 4696 | 486 | 193 |
| [inspect](trajectory-viewer.html#preset=2&pattern=2) grep → kubectl get | 7422 | 456 | 189 |
| [inspect](trajectory-viewer.html#preset=2&pattern=3) kubectl get → grep | 7841 | 450 | 190 |
| [inspect](trajectory-viewer.html#preset=2&pattern=4) sleep → kubectl get | 1403 | 426 | 179 |
| [inspect](trajectory-viewer.html#preset=2&pattern=5) grep → grep | 4534 | 374 | 173 |
| [inspect](trajectory-viewer.html#preset=2&pattern=6) tail → grep | 1679 | 345 | 176 |
| [inspect](trajectory-viewer.html#preset=2&pattern=7) kubectl rollout → kubectl get | 1929 | 335 | 148 |
| [inspect](trajectory-viewer.html#preset=2&pattern=8) kubectl get → jq | 4715 | 309 | 168 |
| [inspect](trajectory-viewer.html#preset=2&pattern=9) kubectl describe → kubectl get | 1856 | 308 | 141 |
| [inspect](trajectory-viewer.html#preset=2&pattern=10) jq → kubectl get | 3770 | 296 | 160 |
| [inspect](trajectory-viewer.html#preset=2&pattern=11) head → kubectl get | 1757 | 278 | 160 |
| [inspect](trajectory-viewer.html#preset=2&pattern=12) sleep → grep | 592 | 278 | 141 |
| [inspect](trajectory-viewer.html#preset=2&pattern=13) kubectl get → sleep | 703 | 266 | 141 |
| [inspect](trajectory-viewer.html#preset=2&pattern=14) kubectl get → head | 1567 | 261 | 156 |
| [inspect](trajectory-viewer.html#preset=2&pattern=15) kubectl get → kubectl rollout | 1195 | 254 | 137 |
| [inspect](trajectory-viewer.html#preset=2&pattern=16) kubectl get → kubectl logs | 2829 | 248 | 136 |
| [inspect](trajectory-viewer.html#preset=2&pattern=17) tail → jq | 1075 | 247 | 139 |
| [inspect](trajectory-viewer.html#preset=2&pattern=18) kubectl patch → kubectl get | 1549 | 239 | 107 |
| [inspect](trajectory-viewer.html#preset=2&pattern=19) kubectl get → tail | 1937 | 236 | 139 |
## Telemetry · One selected attribute · max skipped steps 2

Telemetry includes metrics, fields; Fields context: Resource. Pure and mixed patterns are included.

Complete search; 6,056 evaluated frequent patterns; 40,994 joint source matches. The table shows the first 20; JSON retains evaluated counts, source examples, support and work limits.

| Selected attributes | Occurrences | Attempts | Problems |
| --- | --- | --- | --- |
| [inspect](trajectory-viewer.html#preset=3&pattern=0) Field: pods · .metadata.name · columns → Field: pods · .metadata.name · columns | 407 | 152 | 94 |
| [inspect](trajectory-viewer.html#preset=3&pattern=1) Field: pods · .status.containerStatuses[0].ready · columns → Field: pods · .metadata.name · columns | 397 | 149 | 93 |
| [inspect](trajectory-viewer.html#preset=3&pattern=2) Field: pods · .metadata.name · columns → Field: pods · .status.containerStatuses[0].ready · columns | 384 | 140 | 87 |
| [inspect](trajectory-viewer.html#preset=3&pattern=3) Field: pods · .status.containerStatuses[0].ready · columns → Field: pods · .status.containerStatuses[0].ready · columns | 379 | 138 | 86 |
| [inspect](trajectory-viewer.html#preset=3&pattern=4) Field: pods · .status.containerStatuses[0].restartCount · columns → Field: pods · .metadata.name · columns | 281 | 116 | 73 |
| [inspect](trajectory-viewer.html#preset=3&pattern=5) Field: pods · .status.phase · columns → Field: pods · .metadata.name · columns | 232 | 109 | 76 |
| [inspect](trajectory-viewer.html#preset=3&pattern=6) Field: pods · .status.containerStatuses[0].restartCount · columns → Field: pods · .status.containerStatuses[0].ready · columns | 271 | 107 | 66 |
| [inspect](trajectory-viewer.html#preset=3&pattern=7) Field: pods · .metadata.name · columns → Field: pods · .status.containerStatuses[0].restartCount · columns | 256 | 102 | 65 |
| [inspect](trajectory-viewer.html#preset=3&pattern=8) Field: pods · .status.containerStatuses[0].ready · columns → Field: pods · .status.containerStatuses[0].restartCount · columns | 254 | 101 | 64 |
| [inspect](trajectory-viewer.html#preset=3&pattern=9) Field: pods · .status.phase · columns → Field: pods · .status.containerStatuses[0].ready · columns | 213 | 97 | 67 |
| [inspect](trajectory-viewer.html#preset=3&pattern=10) Field: pods · .metadata.name · columns → Field: pods · .metadata.name · jq | 133 | 92 | 66 |
| [inspect](trajectory-viewer.html#preset=3&pattern=11) Field: pods · .status.containerStatuses[0].ready · columns → Field: pods · .metadata.name · jq | 131 | 91 | 66 |
| [inspect](trajectory-viewer.html#preset=3&pattern=12) Field: pods · .status.containerStatuses[0].restartCount · columns → Field: pods · .status.containerStatuses[0].restartCount · columns | 221 | 89 | 57 |
| [inspect](trajectory-viewer.html#preset=3&pattern=13) Field: pods · .status.containerStatuses[0].restartCount · columns → Field: pods · .metadata.name · jq | 120 | 86 | 61 |
| [inspect](trajectory-viewer.html#preset=3&pattern=14) Field: pods · .status.phase · columns → Field: pods · .metadata.name · jq | 107 | 85 | 60 |
| [inspect](trajectory-viewer.html#preset=3&pattern=15) Field: pods · .metadata.name · columns → Field: pods · .status.phase · columns | 186 | 82 | 60 |
| [inspect](trajectory-viewer.html#preset=3&pattern=16) Field: pods · .status.containerStatuses[0].ready · columns → Field: pods · .status.phase · columns | 178 | 79 | 58 |
| [inspect](trajectory-viewer.html#preset=3&pattern=17) Field: pods · .status.phase · columns → Field: pods · .status.containerStatuses[0].restartCount · columns | 146 | 73 | 52 |
| [inspect](trajectory-viewer.html#preset=3&pattern=18) Field: pods · .status.phase · columns → Field: pods · .status.phase · columns | 147 | 68 | 51 |
| [inspect](trajectory-viewer.html#preset=3&pattern=19) Field: pods · .status.containerStatuses[0].restartCount · columns → Field: pods · .status.phase · columns | 119 | 56 | 41 |

## Interpretation

Full source turns retain sibling requests, model-message availability and exact requests/responses. A filtered view exposes skipped source turns on inspection. Three stored examples per pattern are illustrative witnesses, not a sampled denominator; matching all source paths uses the compact occurrence index.

Unsupported parsing, missing/censored evidence and unknown outcomes are retained. Exact request agreement does not establish earlier observation equivalence, backend reuse or preserved diagnosis accuracy. Counts describe the collected recordings only.
