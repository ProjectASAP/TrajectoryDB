"""Write examples/fixture/run-03: session s3 in the capture format (holmesgpt-sregym#9).

The trajectory comes from holmesgpt-sregym's own converter, run over hand-written recorder records,
so the fixture matches what that exporter produces.

    python3 scripts/capture_fixture.py <holmesgpt-sregym checkout>

s3, problem p2, diagnosis only (seconds after 2026-11-01T00:00:00Z):
  1000.0-1002.0  LLM: get_metrics (c1) and fetch_pod_logs (c2) in parallel
  1002.0-1003.0  c1 tool                  (overlaps c2)
  1002.2-1003.5  c2 tool, result spilled  (48 log lines, 4070 chars before trimming)
  1004.0-1005.5  LLM: get_metrics (c3)
  1005.6-1006.0  c3 tool
  1006.1-1007.1  LLM: compaction
  1007.2-1008.0  LLM: final answer; its input has the summary instead of the tool exchange
"""
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, sys.argv[1])
from runtime.convert import SPILL_MARKER, convert  # noqa: E402
from runtime.recording import message_summary, new_messages  # noqa: E402

STAGE = "diagnosis"
SYSTEM = {"role": "system", "content": "You are HolmesGPT, an SRE assistant. Use tools to find the root cause."}
USER = {"role": "user", "content": "Diagnose why checkout requests fail in namespace shop."}
LOG_LINE = "2026-11-01T00:00:00Z ERROR checkout: connection refused to payment:8080 (attempt {n})\n"
ORIGINAL_LOGS = "".join(LOG_LINE.format(n=n) for n in range(48))
SPILLED = f"{SPILL_MARKER} 1500/1000 tokens. Saved to /tmp/holmes/c2.txt"
# get_metrics results are Python-repr text in real HolmesGPT/SREGym runs.
METRICS = "{'resultType': 'vector', 'result': [{'metric': {'job': 'checkout'}, 'value': [1000, '0.42']}]}"
METRICS_2 = "{'resultType': 'vector', 'result': [{'metric': {'job': 'payment'}, 'value': [1005, '0']}]}"
SUMMARY = {"role": "assistant", "content": "Summary so far: checkout errors are 42%; logs show connection refused to payment; payment is down."}
ANSWER = "root cause: payment service down"


def call(cid, name, args):
    return {"id": cid, "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}


C1 = call("c1", "get_metrics", {"query": 'rate(http_requests_failed_total{job="checkout"}[5m])'})
C2 = call("c2", "fetch_pod_logs", {"namespace": "shop", "pod_name": "checkout-0"})
C3 = call("c3", "get_metrics", {"query": 'up{job="payment"}'})
A1 = {"role": "assistant", "content": None, "tool_calls": [C1, C2]}
A2 = {"role": "assistant", "content": None, "tool_calls": [C3]}
T1 = {"role": "tool", "tool_call_id": "c1", "name": "get_metrics", "content": METRICS}
T2 = {"role": "tool", "tool_call_id": "c2", "name": "fetch_pod_logs", "content": SPILLED}
T3 = {"role": "tool", "tool_call_id": "c3", "name": "get_metrics", "content": METRICS_2}
T0 = datetime(2026, 11, 1, tzinfo=timezone.utc).timestamp()
COMPACT = {"role": "system", "content": "Summarize the conversation so far for an SRE agent."}


def iso(t):
    return datetime.fromtimestamp(t, timezone.utc).isoformat()


def main():
    seen = set()
    records, events = [], []

    def completion(purpose, sent, start, duration, usage, content=None, calls=None):
        records.append({"kind": "completion", "purpose": purpose, "usage": usage, "response_id": f"r{len(records)}",
                        "response_model": "gpt-5.1", "choices": [{"index": 0, "content": content, "tool_calls": calls}],
                        "input_messages": [message_summary(m) for m in sent], "new_input_messages": new_messages(sent, seen),
                        "tool_schema_chars": 900, "tool_schema_count": 3,
                        "start_unix_s": start, "duration_s": duration, "at": start + duration})

    def event(name, data, at):
        events.append({"event": name, "data": data})
        records.append({"kind": "event_received", "native_line": len(events), "at": at})

    def tool(c, start, duration, thread, result_chars, message_chars, data, original=None):
        event("tool_calling_result", {"tool_call_id": c["id"], "result": {"status": "success", "data": data}}, start + duration)
        # The recorder writes this for every result, spilled or not; tokens are HolmesGPT's count of the model message.
        records.append({"kind": "tool_result_limit", "tool_call_id": c["id"], "spilled": original is not None,
                        "original_result_chars": len(original or data),
                        "original_message_tokens": 1500 if original else len(data) // 4,
                        "original_result": original, "at": start + duration})
        records.append({"kind": "tool_timing", "tool_call_id": c["id"], "tool_name": c["function"]["name"],
                        "toolset_name": "sregym", "status": "success", "native_size": None,
                        "start_unix_s": start, "duration_s": duration, "thread_id": thread,
                        "result_chars": result_chars, "model_message_chars": message_chars, "at": start + duration})

    usage = lambda p, c, r, w: {"prompt_tokens": p, "completion_tokens": c, "cache_read_input_tokens": r, "cache_creation_input_tokens": w}
    records.append({"kind": "capture_start", "session_id": "s3", "stages": [STAGE], "model": "gpt-5.1", "at": 999.0})
    records.append({"kind": "stage_start", "configuration": {"prompt": USER["content"], "tools": []}, "at": 999.5})
    completion("agent", [SYSTEM, USER], 1000.0, 2.0, usage(400, 60, 0, 400), calls=[C1, C2])
    event("start_tool_calling", {"id": "c1"}, 1002.0)
    event("start_tool_calling", {"id": "c2"}, 1002.2)
    tool(C1, 1002.0, 1.0, 101, len(METRICS), len(METRICS), METRICS)
    tool(C2, 1002.2, 1.3, 102, len(SPILLED), len(SPILLED), SPILLED, original=ORIGINAL_LOGS)
    completion("agent", [SYSTEM, USER, A1, T1, T2], 1004.0, 1.5, usage(620, 40, 400, 220), calls=[C3])
    event("start_tool_calling", {"id": "c3"}, 1005.6)
    tool(C3, 1005.6, 0.4, 101, len(METRICS_2), len(METRICS_2), METRICS_2)
    completion("compaction", [COMPACT, USER, A1, T1, T2, A2, T3], 1006.1, 1.0, usage(700, 50, 0, 700), content=SUMMARY["content"])
    completion("agent", [SYSTEM, USER, SUMMARY], 1007.2, 0.8, usage(300, 20, 100, 200), content=ANSWER)
    event("ai_answer_end", {"content": ANSWER}, 1008.0)
    records.append({"kind": "submission", "receipt": {"status": "200", "message": "Submission received", "stage": STAGE}, "at": 1008.1})
    records.append({"kind": "capture_end", "at": 1008.2})
    for number, record in enumerate(records, 1):
        at = T0 + record.pop("at")
        if "start_unix_s" in record:
            record["start_unix_s"] += T0
        record.update(sequence=number, recorded_at=iso(at),
                      stage=None if record["kind"] in ("capture_start", "capture_end") else STAGE)

    with tempfile.TemporaryDirectory() as tmp:
        case = Path(tmp)
        (case / "overlay").mkdir()
        (case / "overlay/records.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
        native = case / "native" / STAGE
        native.mkdir(parents=True)
        (native / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
        (native / "result.json").write_text(json.dumps({"prompt_tokens": 2020, "num_llm_calls": 4, "num_compactions": 1}))
        trajectory = convert(case)
    assert trajectory["extra"]["capture_complete"], trajectory["extra"]["capture_gaps"]
    assert trajectory["extra"]["capture"]["coverage"] == [], trajectory["extra"]["capture"]["coverage"]
    # Fields that depend on the converter's checkout, not on the trajectory.
    trajectory["extra"]["converter_revision"] = trajectory["extra"]["converter_working_tree_modified"] = None
    # What holmesgpt-sregym's collect.py adds for SREGym.
    trajectory["extra"]["sregym"] = {"run": "run-03", "problem_id": "p2", "session_id": "s3", "graded": True}
    out = Path(__file__).resolve().parents[1] / "examples/fixture/run-03"
    (out / "p2").mkdir(parents=True, exist_ok=True)
    (out / "p2/trajectory.json").write_text(json.dumps(trajectory, ensure_ascii=False, indent=2) + "\n")
    (out / "results.csv").write_text("problem_id,session_id,max_steps,diagnosis_accuracy,diagnosis_passed,mitigation_passed\n"
                                     "p2,s3,100,100.0,True,\n")


if __name__ == "__main__":
    main()
