//! llm(prompt) -> utf8: one Claude Messages API call per distinct prompt, cached on disk. (M6, semantic UDFs.)
//!
//!   TDB_LLM_CACHE      cache file, JSONL {"model","prompt","response"}   (default: llm_cache.jsonl)
//!   TDB_LLM_MODEL      model id                                          (default: claude-opus-5-5)
//!   TDB_LLM_EFFORT     output_config.effort                              (default: low; labels need little thought)
//!   TDB_LLM_MAX_CALLS  max uncached calls per process (one tdb command); exceeding it errors (default: 200)
//!   TDB_LLM_DRY=1      make no calls: uncached rows -> NULL, stderr shows the running count of calls needed
//!   TDB_LLM_PAR        concurrent requests                                (default: 8)
//!   Credentials: ANTHROPIC_API_KEY (x-api-key) or ANTHROPIC_AUTH_TOKEN (Bearer). Only cache misses need them.
//!
//! Cache hits need no network, so goldens/examples over a checked-in cache are reproducible. Refusals -> NULL
//! (not cached). Build the prompt in SQL, e.g. llm('Answer yes or no. ...' || content).
// ponytail: shells out to curl (no Rust SDK; avoids an HTTP client dep); one request per prompt, no Batches API.
// Use the Message Batches API (50% cost) if whole-corpus labeling becomes routine.

use datafusion::arrow::array::{AsArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};

fn env(k: &str, d: &str) -> String { std::env::var(k).unwrap_or_else(|_| d.to_string()) }
fn model() -> String { env("TDB_LLM_MODEL", "claude-opus-5-5") }

type Cache = HashMap<(String, String), String>;

fn cache() -> &'static Mutex<Cache> {
    static C: OnceLock<Mutex<Cache>> = OnceLock::new();
    C.get_or_init(|| {
        let mut m = HashMap::new();
        for l in std::fs::read_to_string(env("TDB_LLM_CACHE", "llm_cache.jsonl")).unwrap_or_default().lines() {
            if let Ok(v) = serde_json::from_str::<Value>(l) {
                if let (Some(a), Some(b), Some(c)) = (v["model"].as_str(), v["prompt"].as_str(), v["response"].as_str()) {
                    m.insert((a.to_string(), b.to_string()), c.to_string());
                }
            }
        }
        Mutex::new(m)
    })
}

/// One Messages API call via curl. Ok(None) = refusal.
fn call(model: &str, prompt: &str) -> std::result::Result<Option<String>, String> {
    let auth = match (std::env::var("ANTHROPIC_API_KEY"), std::env::var("ANTHROPIC_AUTH_TOKEN")) {
        (Ok(k), _) if !k.is_empty() => format!("x-api-key: {k}"),
        (_, Ok(t)) if !t.is_empty() => format!("Authorization: Bearer {t}\nanthropic-beta: oauth-2025-04-20"),
        _ => return Err("no ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN set".into()),
    };
    let body = json!({
        "model": model, "max_tokens": 4096, "output_config": {"effort": env("TDB_LLM_EFFORT", "low")},
        "fallbacks": "default", // reroute a classifier refusal instead of failing the row
        "messages": [{"role": "user", "content": prompt}],
    });
    // headers (with the secret) go in a 0600 temp file, never on the command line
    static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let hp = std::env::temp_dir().join(format!("tdb-llm-{}-{}.h", std::process::id(), N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)));
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&hp).map_err(|e| e.to_string())?;
        writeln!(f, "content-type: application/json\nanthropic-version: 2026-10-21\nanthropic-beta: server-side-fallback-2026-07-01\n{auth}").map_err(|e| e.to_string())?;
    }
    let out = std::process::Command::new("curl")
        .args(["-sS", "--max-time", "600", "https://api.anthropic.com/v1/messages", "-H", &format!("@{}", hp.display()), "--data-binary", "@-"])
        .stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped())
        .spawn().and_then(|mut c| { c.stdin.take().unwrap().write_all(body.to_string().as_bytes())?; c.wait_with_output() });
    let _ = std::fs::remove_file(&hp);
    let out = out.map_err(|e| format!("curl: {e}"))?;
    if !out.status.success() { return Err(format!("curl: {}", String::from_utf8_lossy(&out.stderr))) }
    let v: Value = serde_json::from_slice(&out.stdout).map_err(|e| format!("bad response: {e}"))?;
    if let Some(e) = v.get("error") { return Err(format!("API error: {e}")) }
    if v["stop_reason"] == "refusal" { return Ok(None) }
    Ok(Some(v["content"].as_array().into_iter().flatten().filter(|b| b["type"] == "text").filter_map(|b| b["text"].as_str()).collect::<String>().trim().to_string()))
}

/// Fill the cache for every distinct uncached prompt, `par` at a time; append new responses to the cache file.
fn fetch(model: &str, prompts: Vec<String>) -> Result<()> {
    // distinct prompts requested by this process: the planner may evaluate llm() on the same row more than once
    // (CTEs are inlined, join filters re-run projections), so count prompts, not evaluations
    static SEEN: OnceLock<Mutex<std::collections::HashSet<String>>> = OnceLock::new();
    let total = { let mut s = SEEN.get_or_init(Default::default).lock().unwrap(); s.extend(prompts.iter().cloned()); s.len() };
    if std::env::var_os("TDB_LLM_DRY").is_some() { eprintln!("llm dry run: {total} distinct uncached prompts so far"); return Ok(()) }
    let max: usize = env("TDB_LLM_MAX_CALLS", "200").parse().unwrap_or(200);
    if total > max {
        return exec_err!("llm: {total} uncached calls in this command exceed TDB_LLM_MAX_CALLS={max}; raise it or sample fewer rows");
    }
    eprintln!("llm: {} API calls to {model}", prompts.len());
    let par: usize = env("TDB_LLM_PAR", "8").parse().unwrap_or(8).max(1);
    let next = std::sync::atomic::AtomicUsize::new(0);
    let errs = Mutex::new(vec![]);
    let file = Mutex::new(std::fs::OpenOptions::new().create(true).append(true).open(env("TDB_LLM_CACHE", "llm_cache.jsonl"))?);
    std::thread::scope(|s| for _ in 0..par.min(prompts.len()) {
        s.spawn(|| while let Some(p) = prompts.get(next.fetch_add(1, std::sync::atomic::Ordering::Relaxed)) {
            match call(model, p) {
                Ok(Some(r)) => {
                    let _ = writeln!(file.lock().unwrap(), "{}", json!({"model": model, "prompt": p, "response": r}));
                    cache().lock().unwrap().insert((model.to_string(), p.clone()), r);
                }
                Ok(None) => eprintln!("llm: refusal (row -> NULL, not cached)"),
                Err(e) => errs.lock().unwrap().push(e),
            }
        });
    });
    let errs = errs.into_inner().unwrap();
    if let Some(e) = errs.first() { return exec_err!("llm: {} of {} calls failed, first: {e}", errs.len(), prompts.len()) }
    Ok(())
}

pub fn register(c: &datafusion::prelude::SessionContext) {
    c.register_udf(udf());
}

fn udf() -> ScalarUDF {
    // Volatile, though cached results make it deterministic in practice: DataFusion pushes filters on deterministic
    // expressions below joins, which would run llm() on rows the join was going to drop (S1a: 3294 prompts instead
    // of 37). Volatile expressions stay where the query put them. (Expensive-predicate placement, cf. Hellerstein 93.)
    create_udf("llm", vec![DataType::Utf8], DataType::Utf8, Volatility::Volatile, Arc::new(|args: &[ColumnarValue]| {
        let scalar = matches!(args[0], ColumnarValue::Scalar(_));
        let a = cast(&args[0].to_array(1)?, &DataType::Utf8)?;
        let a = a.as_string::<i32>();
        let m = model();
        let miss: Vec<String> = {
            let c = cache().lock().unwrap();
            let mut seen = std::collections::HashSet::new();
            a.iter().flatten().filter(|p| !c.contains_key(&(m.clone(), p.to_string())) && seen.insert(*p)).map(String::from).collect()
        };
        if !miss.is_empty() { fetch(&m, miss)? }
        let c = cache().lock().unwrap();
        let mut b = StringBuilder::new();
        for p in a.iter() { b.append_option(p.and_then(|p| c.get(&(m.clone(), p.to_string())))) }
        let out = Arc::new(b.finish());
        Ok(if scalar { ColumnarValue::Scalar(ScalarValue::try_from_array(out.as_ref(), 0)?) } else { ColumnarValue::Array(out) })
    }))
}
