//! Codex CLI 适配器 —— 依据 docs/数据源档案/02-codex.md v1.1。
//!
//! 根:~/.codex(CODEX_HOME)+ sessions/ 与 archived_sessions/ 双目录
//! 用量:event_msg.payload.type == "token_count";last_token_usage 为主增量源
//! 陷阱:total_token_usage 是会话累计(可变快照),只做基线/退化 delta;
//!       请求在当前 thread 域内去重;继承前缀只有归属可证明时才入账
//! 口径:input 含 cached+cache_write(inclusive,须减);output 含 reasoning

use crate::common::*;
use crate::model::*;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

pub struct CodexSource;

fn project_usage(v: &Value) -> Option<Value> {
    match v.get("type").and_then(Value::as_str) {
        Some("session_meta") => {
            let mut out = usage_projection(
                v,
                &[
                    "/type",
                    "/ordinal",
                    "/payload/session_id",
                    "/payload/id",
                    "/payload/history_mode",
                    "/payload/history_base/thread_id",
                    "/payload/history_base/end_ordinal_exclusive",
                    "/payload/forked_from_id",
                    "/payload/forked_from_ordinal_exclusive",
                    "/payload/subagent_history_start_ordinal",
                ],
            );
            if let Some(base) = v.pointer("/payload/history_base") {
                if out.pointer("/payload/history_base").is_none() {
                    out["payload"]["history_base"] = if base.is_null() {
                        Value::Null
                    } else {
                        serde_json::json!({})
                    };
                }
            }
            // Projection drops malformed nested/oversized values for privacy.
            // Keep a small invalid marker so dropped boundary data cannot be
            // mistaken for an absent optional field and silently accepted.
            for field in [
                "history_mode",
                "subagent_history_start_ordinal",
                "forked_from_ordinal_exclusive",
            ] {
                let pointer = format!("/payload/{field}");
                if v.pointer(&pointer).is_some() && out.pointer(&pointer).is_none() {
                    out["payload"][field] = if field == "history_mode" {
                        Value::Null
                    } else {
                        serde_json::json!(-1)
                    };
                }
            }
            Some(out)
        }

        Some("turn_context") => Some(usage_projection(
            v,
            &["/type", "/ordinal", "/payload/model", "/payload/turn_id"],
        )),
        Some("token_usage_record") => {
            let mut paths = vec![
                "/type".to_string(),
                "/ordinal".into(),
                "/payload/thread_id".into(),
                "/payload/turn_id".into(),
                "/payload/response_id".into(),
            ];
            for field in USAGE_FIELDS {
                paths.push(format!("/payload/usage/{field}"));
                paths.push(format!("/payload/thread_token_usage/{field}"));
            }
            Some(usage_projection(
                v,
                &paths.iter().map(String::as_str).collect::<Vec<_>>(),
            ))
        }
        Some("event_msg")
            if matches!(
                v.pointer("/payload/type").and_then(Value::as_str),
                Some("thread_settings_applied" | "task_started" | "turn_started")
            ) =>
        {
            Some(usage_projection(
                v,
                &[
                    "/type",
                    "/ordinal",
                    "/payload/type",
                    "/payload/thread_id",
                    "/payload/turn_id",
                ],
            ))
        }
        Some("event_msg")
            if v.pointer("/payload/type").and_then(Value::as_str) == Some("token_count") =>
        {
            let mut paths = vec![
                "/type".to_string(),
                "/timestamp".into(),
                "/ordinal".into(),
                "/payload/type".into(),
            ];
            for side in ["last_token_usage", "total_token_usage"] {
                for field in [
                    "input_tokens",
                    "cached_input_tokens",
                    "cache_write_input_tokens",
                    "output_tokens",
                    "reasoning_output_tokens",
                    "total_tokens",
                ] {
                    paths.push(format!("/payload/info/{side}/{field}"));
                }
            }
            let mut out =
                usage_projection(v, &paths.iter().map(String::as_str).collect::<Vec<_>>());
            // Preserve null/missing/empty-object semantics used by the delta state machine.
            if let Some(info) = v.pointer("/payload/info") {
                if info.is_null() {
                    out["payload"]["info"] = Value::Null;
                } else {
                    if out.pointer("/payload/info").is_none() {
                        out["payload"]["info"] = serde_json::json!({});
                    }
                    for side in ["last_token_usage", "total_token_usage"] {
                        if let Some(original) = info.get(side) {
                            if original.is_null() {
                                out["payload"]["info"][side] = Value::Null;
                            } else if original.is_object()
                                && out["payload"]["info"].get(side).is_none()
                            {
                                out["payload"]["info"][side] = serde_json::json!({});
                            }
                        }
                    }
                }
            }
            Some(out)
        }
        Some(
            "event_msg"
            | "response_item"
            | "compacted"
            | "ghost_snapshot"
            | "inter_agent_communication_metadata"
            | "world_state",
        ) => None,
        _ => Some(usage_projection(v, &["/type"])),
    }
}

/// [input, cached, cache_write, output, reasoning]
type Buckets = [u64; 5];

#[derive(Default)]
struct FileState {
    model: String,
    /// 累计基线(总量 + 分量),仅用于"只有 total"时的退化 delta 与单调校验
    prev_total: Option<u64>,
    prev_split: Option<Buckets>,
    /// 文件内首个累计值:新会话=基线0+首turn增量;fork=父前缀累计+首turn(档案 §5.3)
    first_total: Option<u64>,
    /// 首个带 last 事件的 last.total_tokens:与 first_total 同事件时基线=0,否则=继承前缀
    first_last: Option<u64>,
    /// 双法互验:Σlast vs 末条 total
    sum_last: u64,
    last_total: Option<u64>,
}

fn usage_tokens(u: &Value) -> Buckets {
    [
        u.get("input_tokens").and_then(|x| x.as_u64()).unwrap_or(0),
        u.get("cached_input_tokens")
            .and_then(|x| x.as_u64())
            .unwrap_or(0),
        u.get("cache_write_input_tokens")
            .and_then(|x| x.as_u64())
            .unwrap_or(0),
        u.get("output_tokens").and_then(|x| x.as_u64()).unwrap_or(0),
        u.get("reasoning_output_tokens")
            .and_then(|x| x.as_u64())
            .unwrap_or(0),
    ]
}

fn sum(b: &Buckets) -> u64 {
    b.iter().fold(0u64, |a, x| a.saturating_add(*x))
}

const USAGE_FIELDS: [&str; 6] = [
    "input_tokens",
    "cached_input_tokens",
    "cache_write_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
];

fn keep_usage(line: &str) -> bool {
    [
        "token_count",
        "session_meta",
        "turn_context",
        "token_usage_record",
        "thread_settings_applied",
        "task_started",
        "turn_started",
    ]
    .iter()
    .any(|kind| line.contains(kind))
}

fn nonempty(v: &Value, pointer: &str) -> Option<String> {
    v.pointer(pointer)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct TurnUsage {
    turn: String,
    buckets: Buckets,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct UsageSnapshot {
    last: Buckets,
    total: Buckets,
    total_tokens: u64,
}

#[derive(Clone)]
struct ReferencedStart {
    physical_base: String,
    prefix_end: u64,
}

struct ExplicitRequest {
    thread: String,
    turn: String,
    usage: Buckets,
    cumulative: Option<(Buckets, u64)>,
}

struct RequestCandidate {
    canonical: String,
    root: String,
    turn: String,
    /// None means a copied prefix whose owner still needs identity evidence.
    owner: Option<String>,
    ancestors: Arc<HashSet<String>>,
    model: String,
    ts_ms: i64,
    buckets: Buckets,
    total: Option<u64>,
    snapshot: Option<UsageSnapshot>,
    ordinal: Option<u64>,
    referenced_start: Option<ReferencedStart>,
    explicit_local: bool,
    rollout_id: String,
}

/// Canonical file metadata is immutable. Later metadata is inherited history,
/// never a replacement for the file's own thread identity.
struct FileIdentity {
    canonical: String,
    root: String,
    paginated: bool,
    prefix_end: Option<u64>,
    local_start: Option<u64>,
    physical_base: Option<String>,
    rollout_id: String,
    ancestors: Arc<HashSet<String>>,
}

fn file_identity(events: &[(u64, Value)], filename: String) -> Result<FileIdentity, &'static str> {
    let mut metadata = events.iter().filter_map(|(_, value)| {
        (value.get("type").and_then(Value::as_str) == Some("session_meta")).then_some(value)
    });
    let first = metadata.next();
    let canonical = first
        .and_then(|v| nonempty(v, "/payload/id").or_else(|| nonempty(v, "/payload/session_id")))
        .unwrap_or_else(|| filename.clone());
    // A revert retains canonical thread identity but writes a different physical
    // rollout UUID in its filename. Plain legacy fixture names fall back to the
    // original thread ID; metadata's history_base ID is never assumed a thread.
    let stem = filename.strip_suffix(".jsonl").unwrap_or(&filename);
    let rollout_id = stem
        .get(stem.len().saturating_sub(36)..)
        .filter(|id| uuid::Uuid::parse_str(id).is_ok())
        .map(str::to_owned)
        .unwrap_or_else(|| canonical.clone());
    let root = first
        .and_then(|v| nonempty(v, "/payload/session_id"))
        .unwrap_or_else(|| canonical.clone());
    let mode = first.and_then(|v| v.pointer("/payload/history_mode"));
    let paginated = match mode {
        None => false,
        Some(Value::String(s)) if s == "legacy" => false,
        Some(Value::String(s)) if s == "paginated" => true,
        _ => return Err("unknown_history_mode"),
    };
    let optional_ordinal = |pointer| -> Result<Option<u64>, &'static str> {
        match first.and_then(|v| v.pointer(pointer)) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value.as_u64().map(Some).ok_or("invalid_history_boundary"),
        }
    };
    let prefix_end = optional_ordinal("/payload/history_base/end_ordinal_exclusive")?;
    let local_start = optional_ordinal("/payload/subagent_history_start_ordinal")?;
    optional_ordinal("/payload/forked_from_ordinal_exclusive")?;
    let physical_base = first.and_then(|v| nonempty(v, "/payload/history_base/thread_id"));
    if first
        .and_then(|v| v.pointer("/payload/history_base"))
        .is_some_and(|v| !v.is_null())
        && (prefix_end.is_none()
            || first
                .and_then(|v| nonempty(v, "/payload/history_base/thread_id"))
                .is_none())
    {
        return Err("invalid_history_base");
    }
    if !paginated && (prefix_end.is_some() || local_start.is_some()) {
        return Err("invalid_history_boundary");
    }
    // history_base.thread_id names a physical rollout (possibly a revert), not
    // necessarily its canonical thread. It is deliberately NOT an owner here.
    let mut ancestors: HashSet<String> = metadata
        .filter_map(|v| nonempty(v, "/payload/id"))
        .filter(|id| id != &canonical)
        .collect();
    let fork_parent = first.and_then(|v| nonempty(v, "/payload/forked_from_id"));
    if let Some(parent) = &fork_parent {
        if parent != &canonical {
            ancestors.insert(parent.clone());
        }
    }
    Ok(FileIdentity {
        canonical,
        root,
        paginated,
        prefix_end,
        local_start,
        physical_base,
        rollout_id,
        ancestors: Arc::new(ancestors),
    })
}

/// An absent usage candidate is not evidence of zero. A zero baseline needs a
/// fully covered, contiguous original prefix with no inherited dependency. The
/// source line numbers reject gaps inside the proven prefix without retaining
/// text; filtered records after that prefix cannot invalidate it.
fn proven_empty_prefix(identity: &FileIdentity, lines: &UsageLines) -> Option<u64> {
    if !lines.readable
        || lines.bad != 0
        || !identity.paginated
        || identity.prefix_end.is_some()
        || identity.local_start.is_some()
        || !identity.ancestors.is_empty()
    {
        return None;
    }
    let mut end = 0;
    for (line, value) in &lines.events {
        if *line != end + 1
            || *line > lines.committed_lines
            || value.get("ordinal").and_then(Value::as_u64) != Some(end)
        {
            break;
        }
        let empty = match value.get("type").and_then(Value::as_str) {
            Some("session_meta") => end == 0,
            Some("turn_context") => end > 0,
            Some("event_msg") if end > 0 => {
                match value.pointer("/payload/type").and_then(Value::as_str) {
                    Some("thread_settings_applied" | "task_started" | "turn_started") => true,
                    Some("token_count") => value.pointer("/payload/info").is_none_or(|info| {
                        info.is_null()
                            || (info.is_object()
                                && ["last_token_usage", "total_token_usage"]
                                    .iter()
                                    .all(|side| {
                                        info.get(side).is_none_or(|usage| {
                                            usage.is_null()
                                                || (usage.is_object()
                                                    && USAGE_FIELDS.iter().all(|field| {
                                                        usage.get(field).and_then(Value::as_u64)
                                                            == Some(0)
                                                    }))
                                        })
                                    }))
                    }),
                    _ => false,
                }
            }
            _ => false,
        };
        if !empty {
            break;
        }
        end += 1;
    }
    (end > 0).then_some(end)
}

fn failure(stats: &mut ScanStats, code: &'static str) {
    stats.failed = true;
    let note = format!("codex:{code}");
    if !stats.notes.contains(&note) {
        stats.notes.push(note);
    }
}

impl UsageSource for CodexSource {
    fn id(&self) -> &'static str {
        "codex"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("CODEX_HOME");
        if roots.is_empty() {
            let p = home().join(".codex");
            if p.is_dir() {
                roots.push(p);
            }
        }
        roots
    }

    fn input_containers(&self, roots: &[PathBuf]) -> Vec<PathBuf> {
        roots
            .iter()
            .flat_map(|root| [root.join("sessions"), root.join("archived_sessions")])
            .filter(|path| path.is_dir())
            .collect()
    }

    fn scan_roots(
        &self,
        roots: &[std::path::PathBuf],
        max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            prune_usage_cache("codex", &[]);
            stats.notes.push("未发现 ~/.codex 或 CODEX_HOME".into());
            return (vec![], stats);
        }
        let mut files = Vec::new();
        for r in roots {
            for sub in ["sessions", "archived_sessions"] {
                let d = r.join(sub);
                if d.is_dir() {
                    files.extend(walk_jsonl(
                        &d,
                        max_files.map(|m| m.saturating_sub(files.len())),
                    ));
                }
            }
        }
        stats.files_seen = files.len();
        prune_usage_cache("codex", &files);
        let mut records: Vec<UsageRecord> = Vec::new();
        let mut candidates: Vec<RequestCandidate> = Vec::new();
        // Explicit request records are identity evidence only, never a second
        // billable channel. Index by turn + usage, not by numbers across a tree.
        let mut response_signatures: HashMap<(String, String), TurnUsage> = HashMap::new();
        let mut caliber_suspect = 0usize;
        let mut empty_prefixes: HashMap<String, Option<u64>> = HashMap::new();

        for path in &files {
            let fname = path
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            let mut st = FileState {
                model: "unknown".into(),
                ..Default::default()
            };
            let (lines, outcome) = cached_usage_file(
                "codex",
                path,
                Some(FileParser {
                    project: project_usage,
                    keep: keep_usage,
                }),
                || read_usage_jsonl_committed(path, project_usage, keep_usage),
            );
            lines.account(&mut stats, outcome);
            let identity = match file_identity(&lines.events, fname) {
                Ok(identity) => identity,
                Err(code) => {
                    failure(&mut stats, code);
                    continue;
                }
            };
            let empty_end = proven_empty_prefix(&identity, &lines);
            empty_prefixes
                .entry(identity.rollout_id.clone())
                .and_modify(|old| {
                    // Every discovered mirror must support the claimed prefix.
                    *old = old.zip(empty_end).map(|(a, b)| a.min(b));
                })
                .or_insert(empty_end);
            let mut request_owners: HashMap<TurnUsage, HashSet<String>> = HashMap::new();
            for (_, value) in &lines.events {
                if value.get("type").and_then(Value::as_str) != Some("token_usage_record") {
                    continue;
                }
                if let (Some(thread), Some(turn), Some(usage)) = (
                    nonempty(value, "/payload/thread_id"),
                    nonempty(value, "/payload/turn_id"),
                    value.pointer("/payload/usage").filter(|v| v.is_object()),
                ) {
                    let signature = TurnUsage {
                        turn,
                        buckets: usage_tokens(usage),
                    };
                    if let Some(response) = nonempty(value, "/payload/response_id") {
                        let key = (thread.clone(), response);
                        if response_signatures
                            .get(&key)
                            .is_some_and(|old| old != &signature)
                        {
                            failure(&mut stats, "conflicting_request_identity");
                        }
                        response_signatures.insert(key, signature.clone());
                    }
                    request_owners.entry(signature).or_default().insert(thread);
                }
            }
            let mut turn = String::new();
            let mut seen_meta = false;
            let mut copied = false;
            let mut local_seen = HashSet::new();
            let mut previous_snapshot = None;
            let mut preceding_request: Option<ExplicitRequest> = None;
            let mut current_owner = Some(identity.canonical.clone());
            let mut consume = |v: &Value| {
                let ts_ms = v
                    .get("timestamp")
                    .and_then(|x| x.as_str())
                    .and_then(iso_to_ms)
                    .unwrap_or(0);
                match v.get("type").and_then(|x| x.as_str()) {
                    Some("session_meta") => {
                        if seen_meta
                            && nonempty(v, "/payload/id").as_deref()
                                != Some(identity.canonical.as_str())
                        {
                            copied = true;
                            current_owner = None;
                        }
                        seen_meta = true;
                    }
                    Some("token_usage_record") => {
                        preceding_request = (|| {
                            nonempty(v, "/payload/response_id")?;
                            let usage = usage_tokens(
                                v.pointer("/payload/usage").filter(|v| v.is_object())?,
                            );
                            let cumulative = v
                                .pointer("/payload/thread_token_usage")
                                .filter(|v| v.is_object())
                                .map(|v| {
                                    let split = usage_tokens(v);
                                    (
                                        split,
                                        v.get("total_tokens")
                                            .and_then(Value::as_u64)
                                            .unwrap_or_else(|| split[0].saturating_add(split[3])),
                                    )
                                });
                            Some(ExplicitRequest {
                                thread: nonempty(v, "/payload/thread_id")?,
                                turn: nonempty(v, "/payload/turn_id")?,
                                usage,
                                cumulative,
                            })
                        })();
                    }
                    Some("turn_context") => {
                        turn = nonempty(v, "/payload/turn_id").unwrap_or_default();
                        // 模型权威来源(档案 §5.5)
                        if let Some(m) = v.pointer("/payload/model").and_then(|x| x.as_str()) {
                            st.model = m.to_string();
                        }
                    }
                    Some("event_msg") => {
                        match v.pointer("/payload/type").and_then(Value::as_str) {
                            Some("thread_settings_applied") => {
                                // Upstream writes this after a copied fork prefix. A
                                // parent's own boundary is also preserved in nested forks.
                                if let Some(thread) = nonempty(v, "/payload/thread_id") {
                                    if thread == identity.canonical
                                        || identity.ancestors.contains(&thread)
                                    {
                                        current_owner = Some(thread);
                                    } else {
                                        failure(&mut stats, "unknown_history_owner");
                                    }
                                }
                                return;
                            }
                            Some("task_started" | "turn_started") => {
                                if let Some(id) = nonempty(v, "/payload/turn_id") {
                                    turn = id;
                                }
                                return;
                            }
                            Some("token_count") => {}
                            _ => return,
                        }
                        // #9660:info 可为 null —— 跳过计数
                        let Some(info) = v.pointer("/payload/info").filter(|i| !i.is_null()) else {
                            return;
                        };
                        // 文件首事件常为空 info 引导行(last/total 字段为 null):
                        // 继承基线 = first_total − 首个 last(同事件 ⇒ 基线 0)
                        if st.first_last.is_none() {
                            st.first_last = info
                                .pointer("/last_token_usage/total_tokens")
                                .and_then(|x| x.as_u64());
                        }
                        let last_split = info
                            .get("last_token_usage")
                            .filter(|v| v.is_object())
                            .map(usage_tokens);
                        let total_split = info
                            .get("total_token_usage")
                            .filter(|v| v.is_object())
                            .map(usage_tokens);
                        // total 语义 = total_tokens 字段(= input 含缓存 + output 含推理);
                        // 五桶直接相加会把 cached 双计(inclusive),缺字段时退化为 b0+b3
                        let total_now = info
                            .pointer("/total_token_usage/total_tokens")
                            .and_then(|x| x.as_u64())
                            .or_else(|| total_split.map(|t| t[0].saturating_add(t[3])));

                        let snapshot = last_split.zip(total_split).zip(total_now).map(
                            |((last, total), total_tokens)| UsageSnapshot {
                                last,
                                total,
                                total_tokens,
                            },
                        );
                        // subagent_history_start_ordinal bounds physically copied
                        // model context. It is NOT a reference into the parent's
                        // rollout and must not be used as its token-info cutoff.
                        let referenced_start = if identity.paginated
                            && identity.prefix_end.is_some()
                            && identity.local_start.is_none()
                            && previous_snapshot.is_none()
                            && last_split.is_some_and(|last| sum(&last) > 0)
                        {
                            identity
                                .prefix_end
                                .filter(|_| last_split.is_some())
                                .map(|prefix_end| ReferencedStart {
                                    physical_base: identity
                                        .physical_base
                                        .clone()
                                        .unwrap_or_default(),
                                    prefix_end,
                                })
                        } else {
                            None
                        };

                        // —— 增量状态机(档案 §5.1,tokscale 同义)——
                        let delta: Option<Buckets> = match last_split {
                            Some(l) => Some(l), // last 为主增量源
                            None => match (total_split, st.prev_split, st.prev_total) {
                                // 只有 total:退化分量差;不前进/乱序回归则跳过
                                (Some(t), Some(prev_s), Some(prev_t))
                                    if total_now.is_some_and(|n| n > prev_t) && prev_t > 0 =>
                                {
                                    Some([
                                        t[0].saturating_sub(prev_s[0]),
                                        t[1].saturating_sub(prev_s[1]),
                                        t[2].saturating_sub(prev_s[2]),
                                        t[3].saturating_sub(prev_s[3]),
                                        t[4].saturating_sub(prev_s[4]),
                                    ])
                                }
                                _ => None,
                            },
                        };
                        // 基线推进:全零快照不推进;单调才更新(档案 §5.1)
                        if let Some(t) = total_now {
                            if st.first_total.is_none() {
                                st.first_total = Some(t);
                            }
                            if t > 0 && st.prev_total.is_none_or(|p| t >= p) {
                                st.prev_total = Some(t);
                                st.prev_split = total_split;
                            }
                            st.last_total = Some(t);
                        }
                        let Some(b) = delta else { return };
                        if sum(&b) == 0 {
                            return;
                        }
                        // Fork initialization seeds the parent's entire token_info.
                        // A later child boundary/model/turn does not make an identical
                        // last+total emission a new request (rate-limit updates replay it).
                        // Keep the original prefix candidate and its original ownership.
                        if snapshot.is_some() && snapshot == previous_snapshot {
                            return;
                        }
                        previous_snapshot = snapshot;
                        // inclusive 减除(档案 §4):input 含 cached+cache_write;output 含 reasoning
                        let mut owner = match (
                            identity.paginated,
                            identity.local_start,
                            identity.prefix_end,
                        ) {
                            (true, Some(local_start), _) => {
                                match v.get("ordinal").and_then(Value::as_u64) {
                                    Some(ordinal) if ordinal >= local_start => {
                                        Some(identity.canonical.clone())
                                    }
                                    Some(_) => current_owner
                                        .clone()
                                        .filter(|owner| owner != &identity.canonical),
                                    None => {
                                        failure(&mut stats, "missing_history_ordinal");
                                        None
                                    }
                                }
                            }
                            (true, None, Some(prefix_end)) => {
                                // Referenced history is not stored in this file. Its
                                // local delta begins at the exclusive physical base.
                                match v.get("ordinal").and_then(Value::as_u64) {
                                    Some(ordinal) if ordinal >= prefix_end && !copied => {
                                        Some(identity.canonical.clone())
                                    }
                                    _ => {
                                        failure(&mut stats, "ambiguous_history_prefix");
                                        None
                                    }
                                }
                            }
                            _ => current_owner.clone(),
                        };
                        if let Some(known) = request_owners.get(&TurnUsage {
                            turn: turn.clone(),
                            buckets: b,
                        }) {
                            if known.len() == 1 {
                                let thread = known.iter().next().unwrap();
                                if thread == &identity.canonical
                                    || identity.ancestors.contains(thread)
                                {
                                    owner = Some(thread.clone());
                                } else {
                                    failure(&mut stats, "unknown_history_owner");
                                    owner = None;
                                }
                            } else {
                                failure(&mut stats, "conflicting_request_owner");
                                owner = None;
                            }
                        }
                        if local_seen.insert((b, total_now)) {
                            st.sum_last = st.sum_last.saturating_add(b[0]).saturating_add(b[3]);
                        }
                        candidates.push(RequestCandidate {
                            canonical: identity.canonical.clone(),
                            root: identity.root.clone(),
                            turn: turn.clone(),
                            owner,
                            ancestors: identity.ancestors.clone(),
                            model: st.model.clone(),
                            ts_ms,
                            buckets: b,
                            total: total_now,
                            snapshot,
                            ordinal: v.get("ordinal").and_then(Value::as_u64),
                            referenced_start,
                            explicit_local: preceding_request.as_ref().is_some_and(|request| {
                                request.thread == identity.canonical
                                    && request.turn == turn
                                    && request.usage == b
                                    && request.cumulative.is_some_and(|(split, total)| {
                                        total_split == Some(split) && total_now == Some(total)
                                    })
                            }),
                            rollout_id: identity.rollout_id.clone(),
                        });
                    }
                    // response_item / compacted / ghost_snapshot:非用量行。
                    // 全量扫描新证(档案 02 §3):较新 build 另有三种顶层类型——
                    // token_usage_record(请求级用量,与 token_count.last 逐字段相等,双表示;
                    //   本机 852 个含它的文件全部同时含 token_count,故跳过不双计;若未来
                    //   build 只写它,须切换主源并按 response_id 去重)、
                    // inter_agent_communication_metadata / world_state(编排元数据,非用量)
                    Some("response_item")
                    | Some("compacted")
                    | Some("ghost_snapshot")
                    | Some("inter_agent_communication_metadata")
                    | Some("world_state") => {}
                    Some(_) => stats.lines_unknown_type += 1,
                    None => stats.lines_unknown_type += 1,
                }
            };
            for (_, value) in &lines.events {
                consume(value);
            }
            stats.files_parsed += usize::from(lines.readable);
            // 双法互验(档案 §5.1/§5.3):
            // 继承基线 = first_total − first_last(新会话=0,fork 子会话=父前缀累计);
            // Σlast ≈ last_total − 继承基线
            if let (Some(t), true) = (st.last_total, st.sum_last > 0) {
                let inherited = st
                    .first_total
                    .unwrap_or(0)
                    .saturating_sub(st.first_last.unwrap_or(0));
                let expect = t.saturating_sub(inherited);
                if expect.abs_diff(st.sum_last).saturating_mul(100) > st.sum_last {
                    caliber_suspect += 1;
                }
            }
        }
        // Resolve referenced first snapshots as a dependency graph. Physical
        // history_base selects the actual prefix, including a revert's previous
        // rollout; forked_from_id/ordinal are a separate logical relationship.
        // Exact preceding request evidence can establish local attribution even
        // when the referenced file has disappeared. Later request hints cannot.
        let mut physical_index: HashMap<&str, Vec<usize>> = HashMap::new();
        let mut physical_snapshots = HashMap::new();
        for (index, candidate) in candidates.iter().enumerate() {
            if let (Some(ordinal), Some(snapshot)) = (candidate.ordinal, candidate.snapshot) {
                if physical_snapshots
                    .insert((candidate.rollout_id.as_str(), ordinal), snapshot)
                    .is_some_and(|previous| previous != snapshot)
                {
                    failure(&mut stats, "conflicting_physical_snapshot");
                }
            }
            if candidate.snapshot.is_some() {
                physical_index
                    .entry(&candidate.rollout_id)
                    .or_default()
                    .push(index);
            }
        }
        let mut resolved: Vec<bool> = candidates
            .iter()
            .map(|candidate| candidate.referenced_start.is_none() || candidate.explicit_local)
            .collect();
        let mut omitted = HashSet::new();
        loop {
            let mut progress = false;
            for (index, candidate) in candidates.iter().enumerate() {
                if resolved[index] {
                    continue;
                }
                let (Some(reference), Some(snapshot)) =
                    (&candidate.referenced_start, candidate.snapshot)
                else {
                    continue;
                };
                let entries = physical_index.get(reference.physical_base.as_str());
                let eligible: Vec<_> = entries
                    .into_iter()
                    .flatten()
                    .copied()
                    .filter(|&parent| {
                        candidates[parent]
                            .ordinal
                            .is_none_or(|ordinal| ordinal < reference.prefix_end)
                    })
                    .collect();
                if eligible.is_empty()
                    && empty_prefixes
                        .get(&reference.physical_base)
                        .is_some_and(|end| end.is_some_and(|end| reference.prefix_end <= end))
                    && snapshot.last == snapshot.total
                    && snapshot.last[0].checked_add(snapshot.last[3]) == Some(snapshot.total_tokens)
                {
                    resolved[index] = true;
                    progress = true;
                    continue;
                }
                let final_ordinal = eligible
                    .iter()
                    .filter_map(|&parent| candidates[parent].ordinal)
                    .max();
                let boundary_indices: Vec<_> = eligible
                    .iter()
                    .copied()
                    .filter(|&parent| candidates[parent].ordinal == final_ordinal)
                    .collect();
                if boundary_indices.is_empty()
                    || boundary_indices.iter().any(|&parent| !resolved[parent])
                {
                    continue;
                }
                let boundary = candidates[boundary_indices[0]].snapshot.unwrap();
                if boundary_indices
                    .iter()
                    .any(|&parent| candidates[parent].snapshot != Some(boundary))
                    || eligible.iter().any(|&parent| {
                        candidates[parent].ordinal.is_none()
                            && candidates[parent].snapshot != Some(boundary)
                    })
                {
                    continue;
                }
                if snapshot == boundary {
                    omitted.insert(index);
                    resolved[index] = true;
                    progress = true;
                    continue;
                }
                let mut baseline = [0; 5];
                let has_baseline = snapshot.total.iter().zip(snapshot.last).enumerate().all(
                    |(i, (total, last))| {
                        total
                            .checked_sub(last)
                            .map(|value| {
                                baseline[i] = value;
                            })
                            .is_some()
                    },
                );
                let baseline_total = snapshot.last[0]
                    .checked_add(snapshot.last[3])
                    .and_then(|last| snapshot.total_tokens.checked_sub(last));
                if has_baseline
                    && boundary.total == baseline
                    && Some(boundary.total_tokens) == baseline_total
                {
                    resolved[index] = true;
                    progress = true;
                }
            }
            if !progress {
                break;
            }
        }
        for (index, known) in resolved.iter().enumerate() {
            if !known {
                failure(&mut stats, "ambiguous_referenced_start");
                omitted.insert(index);
            }
        }
        // First establish turns in files/sections whose own-thread attribution
        // is proven. Then match inherited events by turn identity + exact usage.
        // File ordering and a missing parent file cannot change that decision.
        let mut known_owners: HashMap<TurnUsage, HashSet<String>> = HashMap::new();
        for (index, candidate) in candidates.iter().enumerate() {
            if omitted.contains(&index) {
                continue;
            }
            if let Some(owner) = &candidate.owner {
                if !candidate.turn.is_empty() {
                    known_owners
                        .entry(TurnUsage {
                            turn: candidate.turn.clone(),
                            buckets: candidate.buckets,
                        })
                        .or_default()
                        .insert(owner.clone());
                }
            }
        }
        let mut seen_global: HashSet<String> = HashSet::new();
        let mut clash = DedupTracker::default();
        for (index, candidate) in candidates.into_iter().enumerate() {
            if omitted.contains(&index) {
                continue;
            }
            let identity_key = TurnUsage {
                turn: candidate.turn.clone(),
                buckets: candidate.buckets,
            };
            let owners = known_owners.get(&identity_key);
            let owner = match candidate.owner {
                Some(owner) => owner,
                None => {
                    let mut eligible = owners.into_iter().flatten().filter(|owner| {
                        !candidate.turn.is_empty()
                            && (*owner == &candidate.canonical
                                || candidate.ancestors.contains(*owner))
                    });
                    let Some(owner) = eligible.next() else {
                        failure(&mut stats, "ambiguous_history_owner");
                        continue;
                    };
                    if eligible.next().is_some() {
                        failure(&mut stats, "ambiguous_history_owner");
                        continue;
                    }
                    owner.clone()
                }
            };
            let b = candidate.buckets;
            let in_unc = b[0].saturating_sub(b[1]).saturating_sub(b[2]);
            let out_ex = b[3].saturating_sub(b[4]);
            // Structured encoding avoids delimiter collisions in source IDs.
            // Timestamp and the current turn are deliberately absent: paired
            // emissions and next-turn startup can replay the same last/total.
            // Turn identity is used above for ownership, not as another charge.
            // Root is display grouping; only owner thread dedups.
            let key = format!(
                "codex:{}",
                serde_json::to_string(&(owner, &candidate.model, b, candidate.total)).unwrap()
            );
            if clash.track(
                &key,
                candidate.ts_ms,
                &candidate.model,
                [in_unc, b[1], b[2], out_ex, b[4]],
            ) {
                stats.key_clash += 1;
            }
            if seen_global.insert(key.clone()) {
                records.push(UsageRecord {
                    source: "codex",
                    session_id: candidate.root,
                    agent: None,
                    model: candidate.model,
                    ts_ms: candidate.ts_ms,
                    caliber: Caliber::RequestDelta,
                    request_count: 1,
                    input_uncached: in_unc,
                    cache_read: b[1],
                    cache_write: b[2],
                    cache_write_1h: 0,
                    output: out_ex,
                    reasoning_output: b[4],
                    cost_usd: None,
                    dedup_key: key,
                });
            }
        }
        if caliber_suspect > 0 {
            stats.notes.push(format!(
                "{} 个会话的请求增量与累计校验相差>1%,保留已去重的请求增量",
                caliber_suspect
            ));
        }
        (records, stats)
    }
}
