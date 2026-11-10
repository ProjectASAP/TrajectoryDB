//! Bash / kubectl / tool-argument UDFs.
//!   bash_heads(cmd)      -> list<utf8>  head of every simple command; kubectl expanded to "kubectl <verb>"
//!   kubectl_targets(cmd) -> list<utf8>  "<verb> <resource|subcommand>" per kubectl invocation, e.g. "get pods"
//!   bash_namespaces(cmd) -> list<utf8>  values of -n/--namespace; -A/--all-namespaces -> "*"
//!   json_get(json, key)  -> utf8        top-level field of a JSON object (strings unquoted, others as JSON text)
// ponytail: hand-rolled splitter (quotes, |, ;, &&, newline, $( ), backticks). Heredocs, nested quoting
// inside $( ) and aliases are not modelled; swap for a real bash parser if exactness matters.

use crate::promql::{list_udf, str_udf};
use datafusion::prelude::SessionContext;

/// Split into simple commands, each a list of words with quotes removed.
/// Drop heredoc bodies (`<<EOF ... EOF`, `<<-'X' ... X`); here-strings (`<<<`) are kept.
fn strip_heredocs(s: &str) -> String {
    let delim = |l: &str| {
        let i = l.match_indices("<<").find(|(i, _)| !l[i + 2..].starts_with('<') && (*i == 0 || !l[..*i].ends_with('<')))?.0;
        let d: String = l[i + 2..].trim_start_matches('-').trim_start().trim_start_matches(['\'', '"'])
            .chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '_').collect();
        (!d.is_empty()).then_some(d)
    };
    let (mut out, mut lines) = (String::new(), s.lines());
    while let Some(l) = lines.next() {
        out.push_str(l);
        out.push('\n');
        if let Some(d) = delim(l) { for b in lines.by_ref() { if b.trim() == d { break } } }
    }
    out
}

#[allow(unused_assignments)] // end_word! resets in_word even where it is not read again
pub fn commands(s: &str) -> Vec<Vec<String>> {
    let (mut cmds, mut cur, mut w) = (vec![], vec![], String::new());
    let s = strip_heredocs(s);
    let (mut q, mut chars) = (None::<char>, s.chars().peekable());
    let mut in_word = false;
    macro_rules! end_word { () => {{ if in_word { cur.push(std::mem::take(&mut w)); in_word = false } }} }
    macro_rules! end_cmd { () => {{ end_word!(); if !cur.is_empty() { cmds.push(std::mem::take(&mut cur)) } }} }
    let (mut prev, mut dq_subs) = (' ', 0); // dq_subs: open $( ) entered from inside "..."
    while let Some(c) = chars.next() {
        match (q, c) {
            (Some('"'), '$') if chars.peek() == Some(&'(') => { end_cmd!(); chars.next(); q = None; dq_subs += 1 }
            (None, ')') if dq_subs > 0 => { end_cmd!(); dq_subs -= 1; q = Some('"') }
            (Some(x), c) if c == x => q = None,
            (Some('"'), '\\') => { if let Some(n) = chars.next() { w.push(n) } }
            (Some(_), c) => w.push(c),
            (None, '\'' | '"') => { q = Some(c); in_word = true }
            (None, '\\') => { if let Some(n) = chars.next() { if n != '\n' { w.push(n); in_word = true } } }
            (None, '&') if prev == '>' || chars.peek() == Some(&'>') => { w.push(c); in_word = true }
            (None, '$') if chars.peek() == Some(&'(') => { end_cmd!(); chars.next(); }
            (None, '|' | ';' | '&' | '\n' | '(' | ')' | '`') => end_cmd!(),
            (None, c) if c.is_whitespace() => end_word!(),
            (None, c) => { w.push(c); in_word = true }
        }
        prev = c;
    }
    end_cmd!();
    cmds
}

const KEYWORDS: &[&str] = &["if", "then", "else", "elif", "fi", "while", "until", "do", "done", "esac", "{", "}", "!", "time", "sudo", "nohup", "env"];
const KUBECTL_VALUE_FLAGS: &[&str] = &["-n", "--namespace", "-o", "--output", "-l", "--selector", "-c", "--container", "--context",
    "--field-selector", "--sort-by", "-p", "--patch", "--type", "-f", "--filename", "--tail", "--since", "--kubeconfig"];

/// Words of a command with keywords, wrappers and VAR=val prefixes stripped; None for for/case/select headers.
fn strip(c: &[String]) -> Option<&[String]> {
    let mut i = 0;
    while i < c.len() {
        let x = c[i].as_str();
        if matches!(x, "for" | "case" | "select") { return None }
        if KEYWORDS.contains(&x) { i += 1 }
        else if x == "timeout" { i += 2 }
        else if x.split_once('=').is_some_and(|(k, _)| !k.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')) { i += 1 }
        else { break }
    }
    (i < c.len()).then(|| &c[i..])
}

/// Positional (non-flag) words after `kubectl`, skipping values of known value-taking flags.
fn positionals(args: &[String]) -> Vec<&str> {
    let (mut out, mut it) = (vec![], args.iter());
    while let Some(a) = it.next() {
        if a.starts_with('-') { if KUBECTL_VALUE_FLAGS.contains(&a.as_str()) { it.next(); } } else { out.push(a.as_str()) }
    }
    out
}

pub fn heads(s: &str) -> Vec<String> {
    commands(s).iter().filter_map(|c| strip(c)).map(|c| {
        let h = c[0].rsplit('/').next().unwrap_or(&c[0]).to_string();
        match (h.as_str(), positionals(&c[1..]).first()) { ("kubectl", Some(v)) => format!("kubectl {v}"), _ => h }
    }).collect()
}

pub fn kubectl_targets(s: &str) -> Vec<String> {
    commands(s).iter().filter_map(|c| strip(c)).filter(|c| c[0].ends_with("kubectl")).filter_map(|c| {
        let p = positionals(&c[1..]);
        let verb = *p.first()?;
        let raw = verb == "get" && c.iter().any(|x| x == "--raw");
        Some(match (raw, p.get(1)) { (true, _) => "get --raw".into(), (_, Some(r)) => format!("{verb} {}", r.to_lowercase()), _ => verb.to_string() })
    }).collect()
}

pub fn namespaces(s: &str) -> Vec<String> {
    let mut out = vec![];
    for c in commands(s) {
        let mut it = c.iter();
        while let Some(a) = it.next() {
            match a.as_str() {
                "-n" | "--namespace" => out.extend(it.next().cloned()),
                "-A" | "--all-namespaces" => out.push("*".into()),
                x => if let Some(v) = x.strip_prefix("--namespace=").or_else(|| x.strip_prefix("-n=")) { out.push(v.into()) },
            }
        }
    }
    out
}

pub fn register(c: &SessionContext) {
    c.register_udf(list_udf("bash_heads", |s| Some(heads(s))));
    c.register_udf(list_udf("kubectl_targets", |s| Some(kubectl_targets(s))));
    c.register_udf(list_udf("bash_namespaces", |s| Some(namespaces(s))));
    c.register_udf(str_udf("json_get", 2, |r| Ok(match (r[0], r[1]) {
        (Some(j), Some(k)) => match serde_json::from_str::<serde_json::Value>(j).ok().and_then(|v| v.get(k).cloned()) {
            Some(serde_json::Value::String(s)) => Some(s),
            Some(serde_json::Value::Null) | None => None,
            Some(v) => Some(v.to_string()),
        },
        _ => None,
    })));
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses() {
        let c = r#"sleep 20; kubectl get pods -n shop -o wide | grep -E 'a|b' && kubectl -n x rollout restart deploy/api 2>&1; for s in a b; do echo $s; done"#;
        assert_eq!(heads(c), ["sleep", "kubectl get", "grep", "kubectl rollout", "echo"]);
        assert_eq!(kubectl_targets(c), ["get pods", "rollout restart"]);
        assert_eq!(namespaces(c), ["shop", "x"]);
        assert_eq!(heads("FOO=1 timeout 5 /usr/bin/curl -s \"$(kubectl get svc -A)\""), ["curl", "kubectl get"]);
        assert_eq!(kubectl_targets("kubectl get --raw /api/v1/x"), ["get --raw"]);
        assert_eq!(heads("cat <<'EOF' | kubectl apply -f -\nspec:\n  - name: x\nEOF\necho ok"), ["cat", "kubectl apply", "echo"]);
        assert_eq!(heads("grep a <<< \"$x\""), ["grep"]);
        assert_eq!(namespaces("kubectl get po --namespace=a -A"), ["a", "*"]);
    }
}
