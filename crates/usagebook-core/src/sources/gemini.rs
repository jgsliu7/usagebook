//! Gemini CLI 适配器 —— 依据 docs/数据源档案/18-gemini.md(Community,本机未实证)。
//!
//! 根:<GEMINI_CLI_HOME 或 HOME>/.gemini/tmp;兼容别名见 §7.1,显式配置不回落默认
//! 文件:递归 *.jsonl + *.json 双格式(§2)
//! jsonl:type=="gemini"(逐字节精确)且带 tokens;同 id 后值覆盖前值(流式更新,§4)
//! inclusive 推断:total==input+output+thoughts+tool 且 cached>0 → input 减 cached(§3)
//! .json:messages 数组同 jsonl;stats.models 会话聚合 → 每模型一条 @startTime(§2b)
//! 时间戳:行 timestamp|created_at(ISO)> startTime > 文件 mtime(§5)
//! 去重:有 id 用 id;无 id 内容键;stats 用 文件名:模型(§4)

use crate::common::{home, iso_to_ms, json_u64, walk_with};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

pub struct GeminiSource;

#[derive(Default)]
struct SessionMetadata {
    session: Option<String>,
    session_history: HashSet<String>,
    model: Option<String>,
    start_ms: Option<i64>,
}

impl SessionMetadata {
    fn apply(&mut self, value: &Value) {
        if let Some(session) = string_field(value, &["sessionId", "session_id"]) {
            self.session = Some(session.to_owned());
            self.session_history.insert(session.to_owned());
        }
        if let Some(model) = string_field(value, &["model"]) {
            self.model = Some(model.to_owned());
        }
        if let Some(start) = time_field(value, &["startTime", "timestamp"]) {
            self.start_ms = Some(start);
        }
    }
}

fn string_field<'a>(value: &'a Value, names: &[&str]) -> Option<&'a str> {
    names
        .iter()
        .find_map(|name| value.get(*name)?.as_str().filter(|v| !v.is_empty()))
}

fn time_field(value: &Value, names: &[&str]) -> Option<i64> {
    names
        .iter()
        .find_map(|name| value.get(*name)?.as_str().and_then(iso_to_ms))
}

struct SavedMessage {
    record: UsageRecord,
    path: PathBuf,
    explicit_identity: bool,
    time_quality: u8,
}

/// Same-file records are revisions. Across formats only a sibling migration
/// with the same full request identity proves which copy can supersede another.
fn remember(
    records: &mut HashMap<String, SavedMessage>,
    mut record: UsageRecord,
    path: &Path,
    explicit_identity: bool,
    message_model: bool,
    mut time_quality: u8,
    stats: &mut ScanStats,
) {
    if let Some(previous) = records.get(&record.dedup_key) {
        let previous_jsonl = previous.path.extension().is_some_and(|x| x == "jsonl");
        let incoming_jsonl = path.extension().is_some_and(|x| x == "jsonl");
        let proven_identity = previous.explicit_identity && explicit_identity;
        let migration = proven_identity
            && previous_jsonl != incoming_jsonl
            && previous.path.with_extension("") == path.with_extension("");
        if proven_identity && (previous.path == path || migration) {
            // An incomplete revision cannot erase known metadata for this
            // exact request. Retain evidence already present in its lineage.
            if !message_model && previous.record.model != "unknown" {
                record.model = previous.record.model.clone();
            }
            if time_quality < previous.time_quality {
                record.ts_ms = previous.record.ts_ms;
                time_quality = previous.time_quality;
            }
        }
        if previous.path != path {
            let old = &previous.record;
            let equal = old.session_id == record.session_id
                && old.model == record.model
                && old.ts_ms == record.ts_ms
                && old.caliber == record.caliber
                && old.request_count == record.request_count
                && old.input_uncached == record.input_uncached
                && old.cache_read == record.cache_read
                && old.cache_write == record.cache_write
                && old.cache_write_1h == record.cache_write_1h
                && old.output == record.output
                && old.reasoning_output == record.reasoning_output
                && old.cost_usd == record.cost_usd;
            if migration {
                if !incoming_jsonl {
                    return;
                }
            } else if equal && proven_identity {
                return;
            } else {
                stats.failed = true;
                if !stats
                    .notes
                    .iter()
                    .any(|n| n == "Gemini 同一消息存在无法确认先后关系的版本，下轮重试")
                {
                    stats
                        .notes
                        .push("Gemini 同一消息存在无法确认先后关系的版本，下轮重试".into());
                }
                return;
            }
        }
    }
    records.insert(
        record.dedup_key.clone(),
        SavedMessage {
            record,
            path: path.to_path_buf(),
            explicit_identity,
            time_quality,
        },
    );
}

fn usage_fields(value: &Value) -> Value {
    let mut out = serde_json::Map::new();
    for key in [
        "id",
        "type",
        "model",
        "timestamp",
        "created_at",
        "sessionId",
        "session_id",
    ] {
        if let Some(v) = value.get(key).filter(|v| v.is_string()) {
            // Do not truncate known identifiers into an unknown fallback. The
            // existing normalization boundary validates their permitted length.
            out.insert(key.into(), v.clone());
        }
    }
    let mut tokens = serde_json::Map::new();
    for key in [
        "input",
        "prompt",
        "input_tokens",
        "prompt_tokens",
        "output",
        "candidates",
        "output_tokens",
        "candidates_tokens",
        "cached",
        "cached_tokens",
        "thoughts",
        "reasoning",
        "thoughts_tokens",
        "reasoning_tokens",
        "tool",
        "tool_tokens",
        "total",
        "total_tokens",
    ] {
        if let Some(v) = value
            .get("tokens")
            .and_then(|t| t.get(key))
            .filter(|v| !v.is_object() && !v.is_array())
        {
            tokens.insert(key.into(), v.clone());
        }
    }
    out.insert("tokens".into(), Value::Object(tokens));
    Value::Object(out)
}

fn rebind_migrated_session(
    records: &mut HashMap<String, SavedMessage>,
    metadata: &SessionMetadata,
    path: &Path,
    stats: &mut ScanStats,
) {
    let Some(session) = &metadata.session else {
        return;
    };
    let legacy_path = path.with_extension("json");
    let previous_keys: Vec<_> = records
        .iter()
        .filter(|(_, saved)| {
            saved.explicit_identity
                && saved.path == legacy_path
                && saved.record.session_id != *session
                && metadata.session_history.contains(&saved.record.session_id)
        })
        .map(|(key, _)| key.clone())
        .collect();
    for key in previous_keys {
        let Some(mut saved) = records.remove(&key) else {
            continue;
        };
        let old_prefix = format!("gemini:{}:", saved.record.session_id);
        let Some(message_id) = key.strip_prefix(&old_prefix) else {
            continue;
        };
        // The sibling JSONL explicitly names both the prior and final session
        // in its metadata history. This is a supported rename, not an inference
        // from filenames or equal token amounts.
        saved.record.session_id = session.clone();
        saved.record.dedup_key = format!("gemini:{session}:{message_id}");
        remember(
            records,
            saved.record,
            &legacy_path,
            true,
            true,
            saved.time_quality,
            stats,
        );
    }
}

/// tokens 对象(别名组)→ [input_uncached, cache_read, output, reasoning](§2/§3)。
fn map_tokens(t: &Value) -> [u64; 4] {
    let g = |keys: &[&str]| {
        for k in keys {
            if let Some(x) = t.get(*k) {
                if !x.is_null() {
                    return json_u64(Some(x));
                }
            }
        }
        0
    };
    let input = g(&["input", "prompt", "input_tokens", "prompt_tokens"]);
    let output = g(&["output", "candidates", "output_tokens", "candidates_tokens"]);
    let cached = g(&["cached", "cached_tokens"]);
    let thoughts = g(&[
        "thoughts",
        "reasoning",
        "thoughts_tokens",
        "reasoning_tokens",
    ]);
    let tool = g(&["tool", "tool_tokens"]);
    let total = g(&["total", "total_tokens"]);
    // inclusive 推断(§3):total 恰为四项和且 cached>0 → input 含缓存,减除
    if cached > 0
        && total
            == input
                .saturating_add(output)
                .saturating_add(thoughts)
                .saturating_add(tool)
    {
        [
            input.saturating_sub(cached.min(input)),
            cached,
            output,
            thoughts,
        ]
    } else {
        [input, cached, output, thoughts]
    }
}

impl UsageSource for GeminiSource {
    fn id(&self) -> &'static str {
        "gemini"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        // Preserve explicit configuration before checking existence: a missing
        // selected home must not silently read a different user's default data.
        let mut roots = Vec::new();
        if let Some(p) = std::env::var_os("GEMINI_CLI_HOME").filter(|p| !p.is_empty()) {
            roots.push(PathBuf::from(p).join(".gemini/tmp"));
        }
        // Only the compatibility aliases use the existing comma/trim convention.
        for (key, suffix) in [("GEMINI_DATA_DIR", ""), ("GEMINI_HOME", "tmp")] {
            if let Ok(value) = std::env::var(key) {
                for segment in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    roots.push(PathBuf::from(segment).join(suffix));
                }
            }
        }
        if roots.is_empty() {
            roots.push(home().join(".gemini/tmp"));
        }
        let mut seen = HashSet::new();
        roots.retain(|p| p.is_dir() && p.canonicalize().ok().is_some_and(|p| seen.insert(p)));
        roots
    }

    fn scan_roots(
        &self,
        roots: &[std::path::PathBuf],
        max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            stats
                .notes
                .push("未发现 Gemini 用量目录（GEMINI_CLI_HOME/.gemini/tmp；兼容 GEMINI_DATA_DIR/GEMINI_HOME；显式配置不回落默认 HOME）".into());
            return (vec![], stats);
        }
        let mut files: Vec<PathBuf> = Vec::new();
        let mut per_root = Vec::new();
        for r in roots {
            let mut got = walk_with(r, None, &|p| {
                p.extension().is_some_and(|x| x == "jsonl" || x == "json")
            });
            got.sort();
            per_root.push((r.display().to_string(), got.len()));
            files.extend(got);
            if max_files.is_some_and(|m| files.len() >= m) {
                files.truncate(max_files.unwrap());
                break;
            }
        }
        files.sort_by_key(|path| (path.extension().is_some_and(|x| x == "jsonl"), path.clone()));
        stats.files_seen = files.len();

        let mut records: Vec<UsageRecord> = Vec::new();
        let mut last_wins: HashMap<String, SavedMessage> = HashMap::new();

        for path in &files {
            let mtime_ms = path
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            let is_jsonl = path.extension().is_some_and(|x| x == "jsonl");

            // 逐行处理(jsonl)或 messages 数组处理(.json,§2)
            let feed_line = |v: &Value,
                             sticky_model: &mut String,
                             file_model: &str,
                             start_ms: i64,
                             file_session: Option<&str>|
             -> Option<UsageRecord> {
                let tokens = v.get("tokens").filter(|t| t.is_object())?;
                let line_type = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
                if line_type != "gemini" {
                    return None; // 非用量行(首行会话元数据等)
                }
                let [i, cr, o, r] = map_tokens(tokens);
                if i == 0 && cr == 0 && o == 0 && r == 0 {
                    return None;
                }
                let ts_ms = time_field(v, &["timestamp", "created_at"]).unwrap_or(start_ms);
                let session_id = file_session
                    .or_else(|| string_field(v, &["sessionId", "session_id"]))
                    .unwrap_or(&stem)
                    .to_string();
                // 模型:行 > sticky 继承 > 文件级(§5)
                let model = string_field(v, &["model"])
                    .map(|m| {
                        *sticky_model = m.to_string();
                        m.to_string()
                    })
                    .unwrap_or_else(|| {
                        if !sticky_model.is_empty() {
                            sticky_model.clone()
                        } else if !file_model.is_empty() {
                            file_model.to_string()
                        } else {
                            "unknown".into()
                        }
                    });
                let rec = UsageRecord {
                    source: "gemini",
                    session_id,
                    agent: None,
                    model,
                    ts_ms,
                    caliber: Caliber::RequestDelta,
                    request_count: 1,
                    input_uncached: i,
                    cache_read: cr,
                    cache_write: 0,
                    cache_write_1h: 0,
                    output: o,
                    reasoning_output: r,
                    cost_usd: None,
                    dedup_key: String::new(),
                };
                Some(rec)
            };

            if is_jsonl {
                stats.files_parsed += 1;
                let mut metadata = SessionMetadata::default();
                let mut messages = Vec::new();
                let read_outcome = crate::common::for_each_line(path, |line| {
                    let Ok(v) = serde_json::from_str::<Value>(line) else {
                        stats.lines_bad_json += 1;
                        return;
                    };
                    if v.get("type").and_then(Value::as_str) == Some("gemini")
                        && v.get("tokens").is_some_and(Value::is_object)
                    {
                        // Retain usage fields only, never conversation content. The
                        // final file metadata applies to all messages, including
                        // messages preceding a $set session identity update.
                        messages.push(usage_fields(&v));
                    } else if v.get("id").is_none()
                        && v.get("$patch").is_none()
                        && v.get("$rewindTo").is_none()
                    {
                        if let Some(update) = v.get("$set").filter(|v| v.is_object()) {
                            // Conversation-window rewinds and $set.messages do
                            // not undo tokens already consumed by requests.
                            metadata.apply(update);
                        } else if string_field(&v, &["sessionId", "session_id"]).is_some() {
                            metadata.apply(&v);
                        }
                    }
                });
                read_outcome.apply_to_stats(&mut stats);
                rebind_migrated_session(&mut last_wins, &metadata, path, &mut stats);
                let mut sticky_model = String::new();
                for v in &messages {
                    if let Some(mut rec) = feed_line(
                        v,
                        &mut sticky_model,
                        metadata.model.as_deref().unwrap_or(""),
                        metadata.start_ms.unwrap_or(mtime_ms),
                        metadata.session.as_deref(),
                    ) {
                        // 同 id 后值覆盖;无 id 内容键直接入队(§4)
                        match v.get("id").and_then(|x| x.as_str()) {
                            Some(id) => {
                                rec.dedup_key = format!("gemini:{}:{id}", rec.session_id);
                                remember(
                                    &mut last_wins,
                                    rec,
                                    path,
                                    string_field(v, &["id"]).is_some()
                                        && (metadata.session.is_some()
                                            || string_field(v, &["sessionId", "session_id"])
                                                .is_some()),
                                    string_field(v, &["model"]).is_some(),
                                    if time_field(v, &["timestamp", "created_at"]).is_some() {
                                        2
                                    } else {
                                        u8::from(metadata.start_ms.is_some())
                                    },
                                    &mut stats,
                                );
                            }
                            None => {
                                rec.dedup_key = format!(
                                    "gemini:{}:{}:{}:{}",
                                    rec.session_id, rec.ts_ms, rec.model, rec.input_uncached
                                );
                                remember(
                                    &mut last_wins,
                                    rec,
                                    path,
                                    string_field(v, &["id"]).is_some()
                                        && (metadata.session.is_some()
                                            || string_field(v, &["sessionId", "session_id"])
                                                .is_some()),
                                    string_field(v, &["model"]).is_some(),
                                    if time_field(v, &["timestamp", "created_at"]).is_some() {
                                        2
                                    } else {
                                        u8::from(metadata.start_ms.is_some())
                                    },
                                    &mut stats,
                                );
                            }
                        }
                    }
                }
            } else {
                // .json(§2):messages 形状或 stats.models 会话聚合
                let doc = std::fs::read_to_string(path)
                    .ok()
                    .and_then(|t| serde_json::from_str::<Value>(&t).ok());
                let Some(doc) = doc else {
                    stats.lines_bad_json += 1;
                    continue;
                };
                stats.files_parsed += 1;
                // Gemini can initialize logs.json as an empty array before any
                // chat exists. This is recognized empty data, not a new usage shape.
                if path.file_name().is_some_and(|n| n == "logs.json")
                    && doc.as_array().is_some_and(|items| items.is_empty())
                {
                    continue;
                }
                let file_model = doc.get("model").and_then(|x| x.as_str()).unwrap_or("");
                let start_ms = time_field(&doc, &["startTime", "timestamp"]).unwrap_or(mtime_ms);
                let file_session = doc
                    .get("sessionId")
                    .or_else(|| doc.get("session_id"))
                    .and_then(|x| x.as_str())
                    .unwrap_or(&stem)
                    .to_string();
                if let Some(msgs) = doc.get("messages").and_then(|x| x.as_array()) {
                    stats.lines_total += msgs.len() as u64;
                    let mut sticky = String::new();
                    for v in msgs {
                        let mut sticky_model = sticky.clone();
                        if let Some(mut rec) = feed_line(
                            v,
                            &mut sticky_model,
                            file_model,
                            start_ms,
                            Some(&file_session),
                        ) {
                            rec.session_id = file_session.clone();
                            match v.get("id").and_then(|x| x.as_str()) {
                                Some(id) => {
                                    rec.dedup_key = format!("gemini:{}:{id}", rec.session_id)
                                }
                                None => {
                                    rec.dedup_key = format!(
                                        "gemini:{}:{}:{}",
                                        rec.session_id, rec.ts_ms, rec.model
                                    )
                                }
                            }
                            remember(
                                &mut last_wins,
                                rec,
                                path,
                                string_field(&doc, &["sessionId", "session_id"]).is_some()
                                    && string_field(v, &["id"]).is_some(),
                                string_field(v, &["model"]).is_some(),
                                if time_field(v, &["timestamp", "created_at"]).is_some() {
                                    2
                                } else {
                                    u8::from(
                                        time_field(&doc, &["startTime", "timestamp"]).is_some(),
                                    )
                                },
                                &mut stats,
                            );
                        }
                        sticky = sticky_model;
                    }
                } else if let Some(models) =
                    doc.pointer("/stats/models").and_then(|x| x.as_object())
                {
                    // 会话级聚合(§2b):每模型一条 @startTime
                    stats.lines_total += models.len() as u64;
                    for (model, agg) in models {
                        let Some(tokens) = agg.get("tokens") else {
                            continue;
                        };
                        let [i, cr, o, r] = map_tokens(tokens);
                        if i == 0 && cr == 0 && o == 0 && r == 0 {
                            continue;
                        }
                        let key = format!("gemini:{}:{model}", stem);
                        remember(
                            &mut last_wins,
                            UsageRecord {
                                source: "gemini",
                                session_id: file_session.clone(),
                                agent: None,
                                model: model.clone(),
                                ts_ms: start_ms,
                                caliber: Caliber::RequestDelta,
                                request_count: 1,
                                input_uncached: i,
                                cache_read: cr,
                                cache_write: 0,
                                cache_write_1h: 0,
                                output: o,
                                reasoning_output: r,
                                cost_usd: None,
                                dedup_key: key,
                            },
                            path,
                            false,
                            true,
                            u8::from(time_field(&doc, &["startTime", "timestamp"]).is_some()),
                            &mut stats,
                        );
                    }
                } else {
                    stats.lines_unknown_type += 1;
                }
            }
        }

        let mut keys: Vec<String> = last_wins.keys().cloned().collect();
        keys.sort();
        for k in keys {
            if let Some(rec) = last_wins.remove(&k) {
                records.push(rec.record);
            }
        }

        for (root, n) in per_root {
            stats.notes.push(format!("根 {}:{n} 个 json/jsonl", root));
        }
        (records, stats)
    }
}
