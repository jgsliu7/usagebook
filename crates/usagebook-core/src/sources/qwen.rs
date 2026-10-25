//! Qwen Code 适配器 —— 依据 docs/数据源档案/14-qwen.md。
//!
//! 同时读取 usage/token-usage-*.jsonl 与 projects/**/*.jsonl。月度日志不回填
//! 历史；只有可信 sessionId 不相交时才合并聊天。相同会话双通道出现正用量
//! 无法证明逐请求覆盖，返回月度及不相交聊天的已知前缀并标记 partial。
//! 月度 authType 五桶矩阵及聊天同文件 uuid fragment last-wins 保持不变。

use crate::common::{env_roots, home, iso_to_ms, walk_with, FileStamp, SourceFingerprint};
use crate::model::{Caliber, PreparedSourceScan, ScanStats, UsageRecord, UsageSource};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

pub struct QwenSource;

impl UsageSource for QwenSource {
    fn id(&self) -> &'static str {
        "qwen"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        // Runtime roots can move while old chats remain in the global home.
        let mut roots = env_roots("QWEN_RUNTIME_DIR");
        roots.extend(env_roots("QWEN_HOME"));
        roots.push(home().join(".qwen"));
        let mut seen = HashSet::new();
        roots.retain(|p| p.is_dir() && p.canonicalize().ok().is_some_and(|c| seen.insert(c)));
        roots
    }

    fn prepare_scan(&self, roots: &[PathBuf]) -> Option<Box<dyn PreparedSourceScan>> {
        Some(Box::new(QwenInputs::discover(roots, None)))
    }

    fn scan_roots(
        &self,
        roots: &[PathBuf],
        max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        QwenInputs::discover(roots, max_files).scan()
    }
}

struct QwenInputs {
    monthly: Vec<PathBuf>,
    chats: Vec<PathBuf>,
    fingerprint: SourceFingerprint,
    stats: ScanStats,
}

impl QwenInputs {
    fn discover(roots: &[PathBuf], max_files: Option<usize>) -> Self {
        let mut input = Self {
            monthly: Vec::new(),
            chats: Vec::new(),
            fingerprint: Vec::new(),
            stats: ScanStats::default(),
        };
        let mut canonical_roots = Vec::new();
        for root in roots {
            match root.canonicalize() {
                Ok(root) if root.is_dir() => canonical_roots.push(root),
                _ => fail(&mut input.stats, "ROOT_READ"),
            }
        }
        canonical_roots.sort();
        canonical_roots.dedup();
        let mut candidates = Vec::new();
        for root in canonical_roots {
            input
                .fingerprint
                .push((root.clone(), FileStamp::membership()));
            for (monthly, directory) in [(true, "usage"), (false, "projects")] {
                let container = root.join(directory);
                // Optional absent containers are empty. A present dangling link
                // is an unreadable input, not proof of a successful empty scan.
                match std::fs::symlink_metadata(&container) {
                    Ok(_) if !container.is_dir() => {
                        fail(&mut input.stats, "INPUT_CONTAINER");
                        continue;
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(_) => {
                        fail(&mut input.stats, "INPUT_DISCOVERY");
                        continue;
                    }
                }
                let _ = crate::common::take_walk_error();
                let paths = walk_with(&container, None, &|p| {
                    p.extension().is_some_and(|e| e == "jsonl")
                        && (!monthly
                            || p.file_name()
                                .is_some_and(|n| n.to_string_lossy().starts_with("token-usage-")))
                });
                if crate::common::take_walk_error() {
                    fail(&mut input.stats, "INPUT_DISCOVERY");
                }
                candidates.extend(paths.into_iter().map(|p| (monthly, p)));
            }
        }
        // Stable ordering and real filesystem identities prevent aliases from
        // being interpreted as separate requests. Content equality is not proof.
        candidates.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let mut seen = HashSet::new();
        for (monthly, alias) in candidates {
            let (Some(stamp), Ok(path)) = (crate::common::file_stamp(&alias), alias.canonicalize())
            else {
                fail(&mut input.stats, "INPUT_METADATA");
                continue;
            };
            if !path.is_file() {
                fail(&mut input.stats, "INPUT_NOT_FILE");
                continue;
            }
            input.fingerprint.push((alias, stamp.clone()));
            input.fingerprint.push((path.clone(), stamp.clone()));
            let identity = stamp.identity_key();
            let pieces = identity.split('/').take(2).collect::<Vec<_>>();
            let physical = if pieces.len() == 2 && pieces.iter().all(|p| !p.is_empty()) {
                format!("file:{}:{}", pieces[0], pieces[1])
            } else {
                format!("path:{path:?}")
            };
            if seen.insert(physical) {
                if monthly {
                    input.monthly.push(path)
                } else {
                    input.chats.push(path)
                }
            }
        }
        input.fingerprint.sort_by(|a, b| a.0.cmp(&b.0));
        input.fingerprint.dedup_by(|a, b| a.0 == b.0);
        if let Some(max) = max_files {
            if input.monthly.len() + input.chats.len() > max {
                fail(&mut input.stats, "INPUT_LIMIT");
                input.monthly.truncate(max);
                input
                    .chats
                    .truncate(max.saturating_sub(input.monthly.len()));
            }
        }
        input
    }
}

impl PreparedSourceScan for QwenInputs {
    fn fingerprint(&self) -> Option<SourceFingerprint> {
        (!self.stats.failed).then(|| self.fingerprint.clone())
    }

    fn scan(&self) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = self.stats.clone();
        stats.files_seen = self.monthly.len() + self.chats.len();
        if stats.files_seen == 0 && !stats.failed {
            stats.notes.push("未发现 Qwen 用量文件".into());
        }
        let mut monthly = Rows::default();
        let mut covered = HashSet::new();
        // A missing/unreadable monthly identity might belong to any chat. Such
        // chats cannot be labelled disjoint; valid monthly rows remain known.
        let mut unknown_coverage = self.stats.failed;
        for path in &self.monthly {
            scan_monthly_file(
                path,
                &mut monthly,
                &mut covered,
                &mut unknown_coverage,
                &mut stats,
            );
            stats.files_parsed += 1;
        }
        let mut chats = Rows::default();
        for path in &self.chats {
            scan_chat_file(path, &mut chats, &mut stats);
            stats.files_parsed += 1;
        }
        let mut records = monthly.records.into_values().collect::<Vec<_>>();
        let mut overlap = HashSet::new();
        for row in chats.records.into_values() {
            if covered.contains(&row.session_id) {
                overlap.insert(row.session_id);
            } else if !unknown_coverage {
                records.push(row);
            }
        }
        if !overlap.is_empty() {
            fail(&mut stats, "CHANNEL_OVERLAP");
        }
        if unknown_coverage && !self.chats.is_empty() {
            fail(&mut stats, "MONTHLY_COVERAGE_UNKNOWN");
        }
        records.sort_by(|a, b| a.dedup_key.cmp(&b.dedup_key));
        stats.notes.push(format!(
            "Qwen 双通道: 月度 {} 文件，聊天 {} 文件，重叠 {} 会话",
            self.monthly.len(),
            self.chats.len(),
            overlap.len()
        ));
        (records, stats)
    }
}

fn fail(stats: &mut ScanStats, reason: &str) {
    stats.failed = true;
    let note = format!("qwen {reason}");
    if !stats.notes.contains(&note) {
        stats.notes.push(note);
    }
}

fn identity<'a>(v: &'a Value, field: &str) -> Option<&'a str> {
    v.get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.trim() == *s)
}

fn same_projection(a: &UsageRecord, b: &UsageRecord) -> bool {
    a.session_id == b.session_id
        && a.model == b.model
        && a.agent == b.agent
        && a.ts_ms == b.ts_ms
        && a.token_buckets() == b.token_buckets()
        && a.request_count == b.request_count
}

#[derive(Default)]
struct Rows {
    records: BTreeMap<String, UsageRecord>,
    conflicts: HashSet<String>,
}
impl Rows {
    fn insert(&mut self, row: UsageRecord, stats: &mut ScanStats) {
        let key = row.dedup_key.clone();
        if self.conflicts.contains(&key) {
            return;
        }
        if let Some(previous) = self.records.get(&key) {
            if !same_projection(previous, &row) {
                self.records.remove(&key);
                self.conflicts.insert(key);
                stats.key_clash += 1;
                fail(stats, "REQUEST_ID_CONFLICT");
            }
        } else {
            self.records.insert(key, row);
        }
    }
}

fn file_time(path: &Path) -> i64 {
    path.metadata()
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn timestamp(v: &Value, fallback: i64) -> i64 {
    v.get("timestamp")
        .and_then(Value::as_str)
        .and_then(iso_to_ms)
        .unwrap_or(fallback)
}

// Missing request IDs are not compared by timestamp or token amount. Retain a
// uniquely located diagnostic row, mark partial, and never cache it as complete.
fn request_key(
    v: &Value,
    field: &str,
    prefix: &str,
    path: &Path,
    line: u64,
    stats: &mut ScanStats,
) -> String {
    if let Some(id) = identity(v, field) {
        return format!("{prefix}{id}");
    }
    fail(stats, "REQUEST_ID_MISSING");
    format!("{prefix}unidentified:{path:?}:{line}")
}

fn scan_chat_file(path: &Path, rows: &mut Rows, stats: &mut ScanStats) {
    let mtime = file_time(path);
    let mut pending: HashMap<String, UsageRecord> = HashMap::new();
    let mut conflicts = HashSet::new();
    let mut line_number = 0;
    let read = crate::common::for_each_line(path, |line| {
        line_number += 1;
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            stats.lines_bad_json += 1;
            return;
        };
        if v.get("type").and_then(Value::as_str) != Some("assistant") {
            stats.lines_unknown_type += 1;
            return;
        }
        let Some(um) = v.get("usageMetadata") else {
            stats.lines_unknown_type += 1;
            return;
        };
        let g = |k: &str| crate::common::json_u64(um.get(k));
        let (prompt, cand, thoughts, cached) = (
            g("promptTokenCount"),
            g("candidatesTokenCount"),
            g("thoughtsTokenCount"),
            g("cachedContentTokenCount"),
        );
        if prompt == 0 && cand == 0 {
            return;
        }
        let Some(session) = identity(&v, "sessionId") else {
            fail(stats, "SESSION_ID_MISSING");
            return;
        };
        let input_uncached = if cached > prompt {
            if !stats
                .notes
                .iter()
                .any(|n| n == "Qwen cached>prompt: 使用既有 exclusive 形状守卫")
            {
                stats
                    .notes
                    .push("Qwen cached>prompt: 使用既有 exclusive 形状守卫".into());
            }
            prompt
        } else {
            prompt - cached
        };
        let key = request_key(&v, "uuid", "qwen:", path, line_number, stats);
        let row = UsageRecord {
            source: "qwen",
            session_id: session.to_owned(),
            agent: None,
            model: v
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
            ts_ms: timestamp(&v, mtime),
            caliber: Caliber::RequestDelta,
            request_count: 1,
            input_uncached,
            cache_read: cached,
            cache_write: 0,
            cache_write_1h: 0,
            output: cand.saturating_sub(thoughts),
            reasoning_output: thoughts.min(cand),
            cost_usd: None,
            dedup_key: key.clone(),
        };
        if conflicts.contains(&key) {
            return;
        }
        if pending
            .get(&key)
            .is_some_and(|prev| prev.session_id != row.session_id || prev.model != row.model)
        {
            pending.remove(&key);
            conflicts.insert(key);
            stats.key_clash += 1;
            fail(stats, "REQUEST_ID_CONFLICT");
        } else {
            // Upstream supports fragments within a transcript. Last nonzero
            // usage wins only inside that file; competing files need agreement.
            pending.insert(key, row);
        }
    });
    read.apply_to_stats(stats);
    for key in conflicts {
        rows.records.remove(&key);
        rows.conflicts.insert(key);
    }
    for row in pending.into_values() {
        rows.insert(row, stats);
    }
}

fn scan_monthly_file(
    path: &Path,
    rows: &mut Rows,
    sessions: &mut HashSet<String>,
    unknown: &mut bool,
    stats: &mut ScanStats,
) {
    let mtime = file_time(path);
    let previous_bad = stats.lines_bad_json;
    let mut line_number = 0;
    let read = crate::common::for_each_line(path, |line| {
        line_number += 1;
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            stats.lines_bad_json += 1;
            return;
        };
        let g = |k: &str| crate::common::json_u64(v.get(k));
        let (input, output, cached, thoughts) = (
            g("inputTokens"),
            g("outputTokens"),
            g("cachedTokens"),
            g("thoughtsTokens"),
        );
        if input == 0 && output == 0 && cached == 0 && thoughts == 0 {
            return;
        }
        let Some(session) = identity(&v, "sessionId") else {
            *unknown = true;
            fail(stats, "SESSION_ID_MISSING");
            return;
        };
        sessions.insert(session.to_owned());
        let auth = v.get("authType").and_then(Value::as_str).unwrap_or("");
        let (out_net, reasoning) = match auth {
            "" | "openai" | "openai-responses" | "qwen-oauth" => {
                (output.saturating_sub(thoughts), thoughts.min(output))
            }
            "gemini" | "vertex-ai" => (output, thoughts),
            "anthropic" if thoughts == 0 => (output, 0),
            _ if thoughts == 0 => {
                if !stats
                    .notes
                    .iter()
                    .any(|n| n == "月度协议未识别但 thoughts=0，使用无歧义输出拆分")
                {
                    stats
                        .notes
                        .push("月度协议未识别但 thoughts=0，使用无歧义输出拆分".into());
                }
                (output, 0)
            }
            _ => {
                fail(stats, "UNSUPPORTED_REASONING_PROTOCOL");
                return;
            }
        };
        let source = v.get("source").and_then(Value::as_str).unwrap_or("main");
        let key = request_key(&v, "id", "qwen:monthly:", path, line_number, stats);
        rows.insert(
            UsageRecord {
                source: "qwen",
                session_id: session.to_owned(),
                agent: (!source.is_empty() && source != "main").then(|| source.to_owned()),
                model: v
                    .get("model")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned(),
                ts_ms: timestamp(&v, mtime),
                caliber: Caliber::RequestDelta,
                request_count: 1,
                input_uncached: input.saturating_sub(cached),
                cache_read: cached,
                cache_write: 0,
                cache_write_1h: 0,
                output: out_net,
                reasoning_output: reasoning,
                cost_usd: None,
                dedup_key: key,
            },
            stats,
        );
    });
    let incomplete = !read.reached_eof || read.error.is_some() || read.invalid_utf8_lines > 0;
    read.apply_to_stats(stats);
    *unknown |= incomplete || stats.lines_bad_json > previous_bad;
}
