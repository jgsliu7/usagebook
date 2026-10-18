//! OpenClaw 适配器 —— 依据 docs/数据源档案/17-openclaw.md(源码一手取证+实库对账)。
//!
//! 双后端(§7.2,SQLite 正典优先):agents/<agentId>/agent/openclaw-agent.sqlite 的
//!     transcript_events(event_json / event_zstd 二选一)为正典;agents/<agentId>/sessions/*.jsonl
//!     为遗留冷层,仅当该 agent 无 SQLite 时兜底。同 (agentId,sessionId) 不双读。
//! 发现(§7.1):OPENCLAW_STATE_DIR(单根)> 默认 ~/.openclaw(缺失回退 ~/.clawdbot 遗留)。
//! 语义(§7.3):input **exclusive 直取**(官方已归一,勿减 cache);total=in+out+cr+cw 恒等式;
//!     reasoningTokens 是 output 明细 → output 落库拆分为 output−reasoning + reasoning 单列;
//!     cost 分治:totalOrigin==provider-billed 直取,估算不落(model.rs 纪律,2026-10-17 定稿)。
//! 去重(§7.4):openclaw:{agentId}:{session}:{event_id},(session,seq) 兜底。

use crate::common::{env_roots, flex_to_ms, home, json_u64};
use crate::model::{Caliber, ScanStats, UsageSource, UsageRecord};
use serde_json::Value;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

pub struct OpenclawSource;

/// 只读打开(收编至 common,2026-10-17):此前自持样板且 URI flag 对纯 PathBuf 无意义——
/// SQLITE_OPEN_URI 仅影响 file: URI 解析,传 PathBuf 反而在路径含 `?` 时有解析歧义。
fn open_ro(db: &Path) -> Option<rusqlite::Connection> {
    crate::common::open_sqlite_readonly(db).ok()
}

/// 别名组取值:首个存在且非 null 的键(遗留 JSONL 通道别名宽度,§2)。
fn alias(v: &Value, keys: &[&str]) -> u64 {
    for k in keys {
        if let Some(x) = v.get(*k) {
            if !x.is_null() {
                return json_u64(Some(x));
            }
        }
    }
    0
}

/// 从 assistant message 的 usage 构造五桶(exclusive 语义,§7.3)。
fn buckets_from_usage(usage: &Value) -> Option<(u64, u64, u64, u64, u64)> {
    let i = alias(usage, &["input", "inputTokens", "input_tokens"]);
    let cr = alias(usage, &["cacheRead", "cache_read_input_tokens", "cachedTokens"]);
    let cw = alias(usage, &["cacheWrite", "cache_creation_input_tokens", "cacheCreationTokens"]);
    let o = alias(usage, &["output", "outputTokens", "output_tokens"]);
    let rs = alias(usage, &["reasoningTokens", "reasoning_tokens", "reasoning"]);
    if i == 0 && cr == 0 && cw == 0 && o == 0 {
        return None; // 全零 usage 不入账(三跳过)
    }
    // output 含 reasoning(明细)→ 拆分,防 token_total 双计(model.rs output 语义)
    let out_net = o.saturating_sub(rs);
    Some((i, cr, cw, out_net, rs))
}

impl UsageSource for OpenclawSource {
    fn id(&self) -> &'static str {
        "openclaw"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("OPENCLAW_STATE_DIR");
        if roots.is_empty() {
            let oc = home().join(".openclaw");
            if oc.is_dir() {
                roots.push(oc);
            } else {
                // 遗留目录兜底(§7.1:.clawdbot 仍存在;.moltbot 迁移已移除)
                let legacy = home().join(".clawdbot");
                if legacy.is_dir() {
                    roots.push(legacy);
                }
            }
        }
        roots
    }

    fn scan_roots(&self, roots: &[std::path::PathBuf], _max_files: Option<usize>) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            stats.notes.push("未发现 ~/.openclaw 或 OPENCLAW_STATE_DIR".into());
            return (vec![], stats);
        }

        let mut records: Vec<UsageRecord> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut zstd_decoded = 0u64;
        let mut sqlite_agents = 0u64;
        let mut jsonl_agents = 0u64;

        for root in roots {
            let agents_dir = root.join("agents");
            let Ok(rd) = std::fs::read_dir(&agents_dir) else { continue };
            let mut agent_ids: Vec<String> = rd
                .flatten()
                .filter(|e| e.path().is_dir())
                .filter_map(|e| e.file_name().to_str().map(str::to_string))
                .collect();
            agent_ids.sort();

            for agent_id in agent_ids {
                let agent_dir = agents_dir.join(&agent_id);
                let db = agent_dir.join("agent").join("openclaw-agent.sqlite");
                let agent_tag = if agent_id == "main" { None } else { Some(agent_id.clone()) };

                if db.is_file() {
                    // ===== 正典通道:SQLite transcript_events(§7.2)=====
                    sqlite_agents += 1;
                    scan_sqlite_agent(
                        &db, &agent_id, agent_tag.as_deref(), &mut records, &mut seen,
                        &mut stats, &mut zstd_decoded,
                    );
                } else {
                    // ===== 遗留通道:agents/<id>/sessions/*.jsonl(§7.2 冷层)=====
                    let mut files: Vec<PathBuf> = Vec::new();
                    if let Ok(sd) = std::fs::read_dir(agent_dir.join("sessions")) {
                        for f in sd.flatten() {
                            let p = f.path();
                            // §7.2:官方把 reset/deleted 归档也计入用量 —— 文件名含
                            // ".jsonl" 即收(含 .jsonl.reset.<ts>/.jsonl.deleted. 变体),
                            // 归档后缀经 file_stem 自然并入 session_id → 跨代去重键独立。
                            let is_jsonl = p
                                .file_name()
                                .and_then(|n| n.to_str())
                                .is_some_and(|n| n.contains(".jsonl"));
                            if p.is_file() && is_jsonl {
                                files.push(p);
                            }
                        }
                    }
                    if !files.is_empty() {
                        jsonl_agents += 1;
                        files.sort();
                        scan_jsonl_agent(&files, &agent_id, agent_tag.as_deref(), &mut records, &mut seen, &mut stats);
                    }
                }
            }
        }

        stats.files_seen = (sqlite_agents + jsonl_agents) as usize;
        stats.files_parsed = stats.files_seen;
        stats.notes.push(format!("SQLite 正典 agent {sqlite_agents} 个 / JSONL 遗留 agent {jsonl_agents} 个(§7.2)"));
        if zstd_decoded > 0 {
            stats.notes.push(format!("event_zstd 解压 {zstd_decoded} 行"));
        }
        (records, stats)
    }
}

/// SQLite 正典:读 transcript_events 全量,过滤 assistant usage 行。
fn scan_sqlite_agent(
    db: &Path,
    agent_id: &str,
    agent_tag: Option<&str>,
    records: &mut Vec<UsageRecord>,
    seen: &mut HashSet<String>,
    stats: &mut ScanStats,
    zstd_decoded: &mut u64,
) {
    let Some(conn) = open_ro(db) else {
        stats.notes.push(format!("{} 打开失败,已跳过(若该库应有数据请提 issue)", db.display()));
        stats.failed = true;
        return;
    };
    if !has_table(&conn, "transcript_events") {
        stats.notes.push(format!("{} 无 transcript_events,忽略", db.display()));
        return;
    }
    let mut stmt = match conn.prepare(
        "SELECT session_id, seq, event_json, event_zstd FROM transcript_events ORDER BY session_id, seq",
    ) {
        Ok(s) => s,
        Err(_) => {
            stats.notes.push(format!("{} transcript_events 查询构建失败", db.display()));
            stats.failed = true;
            return;
        }
    };
    // 列缺省防御:event_json/event_zstd 二选一(CHECK 约束,§7.2)
    let mut rows = match stmt.query([]) {
        Ok(r) => r,
        Err(_) => {
            stats.notes.push(format!("{} transcript_events 查询执行失败", db.display()));
            stats.failed = true;
            return;
        }
    };
    while let Ok(Some(row)) = rows.next() {
        stats.lines_total += 1;
        let session_id: String = row.get::<_, String>(0).unwrap_or_default();
        let seq: i64 = row.get::<_, i64>(1).unwrap_or(0);
        let event_json: Option<String> = row.get(2).ok().flatten();
        let event_zstd: Option<Vec<u8>> = row.get(3).ok().flatten();

        let raw: Vec<u8> = match (event_json, event_zstd) {
            (Some(s), _) => s.into_bytes(),
            (None, Some(blob)) => match zstd::decode_all(&blob[..]) {
                Ok(d) => {
                    *zstd_decoded += 1;
                    d
                }
                Err(_) => {
                    stats.lines_bad_json += 1;
                    continue;
                }
            },
            (None, None) => {
                stats.lines_bad_json += 1;
                continue;
            }
        };
        let Ok(v) = serde_json::from_str::<Value>(&String::from_utf8_lossy(&raw)) else {
            stats.lines_bad_json += 1;
            continue;
        };
        if v.get("type").and_then(|x| x.as_str()) != Some("message") {
            continue; // 非消息行(已知类型,不计未知)
        }
        let msg = v.get("message").unwrap_or(&Value::Null);
        if msg.get("role").and_then(|x| x.as_str()) != Some("assistant") {
            continue; // user/系统行不计未知(已知角色)
        }
        let Some(usage) = msg.get("usage").filter(|u| u.is_object()) else {
            continue; // assistant 无 usage = 非请求行
        };
        let Some((i, cr, cw, out_net, rs)) = buckets_from_usage(usage) else {
            stats.notes.push(format!("session {session_id} 全零 usage 跳过(§7.2)"));
            continue;
        };
        // ts:message.timestamp(epoch 毫秒数字)优先,回退事件 envelope timestamp(ISO)(§7.4)
        let ts_ms = flex_to_ms(msg.get("timestamp"))
            .or_else(|| flex_to_ms(v.get("timestamp")))
            .unwrap_or(0);
        let model = msg
            .get("model")
            .or_else(|| msg.get("modelId"))
            .and_then(|x| x.as_str())
            .map(str::to_string)
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| "unknown".into());
        // 去重(§7.4):(agentId, sessionId, event_id);event_id 缺失回退 (session,seq)
        let event_id = v.get("id").and_then(|x| x.as_str()).unwrap_or("");
        let key = if event_id.is_empty() {
            format!("openclaw:{agent_id}:{session_id}:seq{seq}")
        } else {
            format!("openclaw:{agent_id}:{session_id}:{event_id}")
        };
        if !seen.insert(key.clone()) {
            stats.notes.push("去重键命中丢弃(§7.4)".into());
            continue;
        }
        records.push(UsageRecord {
            source: "openclaw",
            session_id,
            agent: agent_tag.map(str::to_string),
            model,
            ts_ms,
            caliber: Caliber::RequestDelta,
            input_uncached: i, // exclusive 直取(§7.3,勿减 cache)
            cache_read: cr,
            cache_write: cw,
            cache_write_1h: 0,
            output: out_net,
            reasoning_output: rs,
            // 成本分治(2026-10-17,档案 §7.3 定稿):仅 totalOrigin=="provider-billed" 直取,
            // 本地计价估算不落——混合口径按总览纪律"有来源标记只取直报"
            cost_usd: usage
                .pointer("/cost/totalOrigin")
                .and_then(|x| x.as_str())
                .and_then(|o| if o == "provider-billed" {
                    usage.pointer("/cost/total").and_then(|x| x.as_f64()).filter(|c| *c > 0.0)
                } else { None }),
            dedup_key: key,
        });
    }
}

/// 遗留 JSONL 通道(同构事件流;exclusive 语义修正,§7.3)。
fn scan_jsonl_agent(
    files: &[PathBuf],
    agent_id: &str,
    agent_tag: Option<&str>,
    records: &mut Vec<UsageRecord>,
    seen: &mut HashSet<String>,
    stats: &mut ScanStats,
) {
    for path in files {
        let session_id = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
        crate::common::for_each_line(path, |line| {
            stats.lines_total += 1;
            let Ok(v) = serde_json::from_str::<Value>(line) else {
                stats.lines_bad_json += 1;
                return;
            };
            if v.get("type").and_then(|x| x.as_str()) != Some("message") {
                return;
            }
            let msg = v.get("message").unwrap_or(&Value::Null);
            if msg.get("role").and_then(|x| x.as_str()) != Some("assistant") {
                return;
            }
            let Some(usage) = msg.get("usage").filter(|u| u.is_object()) else {
                return;
            };
            let Some((i, cr, cw, out_net, rs)) = buckets_from_usage(usage) else {
                return; // 全零(如 429 失败行)
            };
            let ts_ms = flex_to_ms(msg.get("timestamp"))
                .or_else(|| flex_to_ms(v.get("timestamp")))
                .unwrap_or(0);
            let model = msg
                .get("model")
                .or_else(|| msg.get("modelId"))
                .and_then(|x| x.as_str())
                .map(str::to_string)
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| "unknown".into());
            let event_id = v.get("id").and_then(|x| x.as_str()).unwrap_or("");
            let key = if event_id.is_empty() {
                format!("openclaw:{agent_id}:{session_id}:{}:{ts_ms}:{i}:{cr}:{cw}:{out_net}:{rs}",
                    msg.get("id").and_then(|x| x.as_str()).unwrap_or(""))
            } else {
                format!("openclaw:{agent_id}:{session_id}:{event_id}")
            };
            if !seen.insert(key.clone()) {
                return;
            }
            records.push(UsageRecord {
                source: "openclaw",
                session_id: session_id.clone(),
                agent: agent_tag.map(str::to_string),
                model,
                ts_ms,
                caliber: Caliber::RequestDelta,
                input_uncached: i,
                cache_read: cr,
                cache_write: cw,
                cache_write_1h: 0,
                output: out_net,
                reasoning_output: rs,
                cost_usd: usage   // 成本分治同 SQLite 通道(2026-10-17)

                    .pointer("/cost/totalOrigin")

                    .and_then(|x| x.as_str())

                    .and_then(|o| if o == "provider-billed" {

                        usage.pointer("/cost/total").and_then(|x| x.as_f64()).filter(|c| *c > 0.0)

                    } else { None }),
                dedup_key: key,
            });
        });
        stats.files_parsed += 1;
    }
}

fn has_table(conn: &rusqlite::Connection, name: &str) -> bool {
    conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
        [name],
        |r| r.get::<_, i64>(0),
    )
    .unwrap_or(0)
        > 0
}
