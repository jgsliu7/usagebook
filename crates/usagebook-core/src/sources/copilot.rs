//! Copilot CLI 适配器 —— 依据 docs/数据源档案/19-copilot.md(二进制取证+实装对账)。
//!
//! 根:~/.copilot 或 COPILOT_HOME(CLI 单路径,逗号兼容保留 §7.1)
//! 三通道(§7.1,优先级):session-store.db 的 assistant_usage_events(逐请求,免差分,
//!     含官方 total_nano_aiu 成本)存在且非空 → 独占;否则 session-state shutdown 相邻差分
//!     (§2)+ OTel(默认关闭用户,§7.1)
//! inputTokens 同时含 cacheRead+cacheWrite → 双减除(§7.3 一手证实,官方 tokenDetails 背书)
//! outputTokens 已含 reasoningTokens → 落库拆分 output−reasoning(§7.3)
//! 成本:total_nano_aiu/1e9×0.01(1 AIU=1 credit=$0.01,schema overageEntitlement,§7.3)
//! OTel:四种记录类型判别 + 同 (session,model) 时间 ≤ 末次 shutdown 的行丢弃防双计;
//!     同 traceId 优先级 ChatSpan > InferenceLog > AgentTurnLog > AgentSummarySpan(§3/§4)
//! 成本:requests.cost 口径不明(ccusage 忽略)→ 不取(§6)

use crate::common::{env_roots, flex_to_ms, home, json_u64, walk_with, SnapshotDiff};
use crate::model::{Caliber, ScanStats, UsageSource, UsageRecord};
use rusqlite::types::Value as SqlValue;
use rusqlite::Connection;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

pub struct CopilotSource;

/// OTel 行类型(§3;优先级 = 判定难度,数字越小优先级越高)。
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
enum OtelKind {
    AgentSummarySpan,
    AgentTurnLog,
    InferenceLog,
    ChatSpan,
}

struct ShutdownSnap {
    ts_ms: i64,
    event_id: String,
    usage: Value,
}

/// OTel 行(§3):内容键 + 级别 + 五桶,第二阶段统一做抑制过滤。
struct OtelRow {
    ts_ms: i64,
    trace: String,
    session: String,
    model: String,
    content_key: String,
    kind: OtelKind,
    buckets: [u64; 5],
}

impl UsageSource for CopilotSource {
    fn id(&self) -> &'static str {
        "copilot"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("COPILOT_HOME");
        if roots.is_empty() {
            let d = home().join(".copilot");
            if d.is_dir() {
                roots.push(d);
            }
        }
        roots
    }

    fn scan_roots(&self, roots: &[std::path::PathBuf], max_files: Option<usize>) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            stats.notes.push("未发现 ~/.copilot 或 COPILOT_HOME".into());
            return (vec![], stats);
        }
        let mut records: Vec<UsageRecord> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut ss_files = 0usize;
        let mut otel_files = 0usize;
        // (session, model) → 该组最后一个 shutdown 时间(OTel 双计防御,§4)
        let mut last_shutdown: HashMap<(String, String), i64> = HashMap::new();

        for r in roots {
            // ===== 首选通道:session-store.db 逐请求(§7.1)=====
            let store_db = r.join("session-store.db");
            if store_db.is_file() {
                let ledger_rows = scan_session_store(&store_db, &mut records, &mut seen, &mut stats);
                if ledger_rows > 0 {
                    stats.files_seen += 1;
                    stats.files_parsed += 1;
                    stats
                        .notes
                        .push(format!("session-store.db 逐请求通道 {} 行(§7.1,免差分)", ledger_rows));
                    continue; // 该根独占,session-state/otel 让位防双计
                }
                stats
                    .notes
                    .push(format!("{} 空/不可读,回退 events.jsonl(§7.1)", store_db.display()));
            }

            // ===== 通道 1:session-state(§2)=====
            // (session, model) → 按时间排序的 shutdown 快照序列
            let mut snaps: BTreeMap<(String, String), Vec<ShutdownSnap>> = BTreeMap::new();
            if let Ok(rd) = std::fs::read_dir(r.join("session-state")) {
                for e in rd.flatten() {
                    let p = e.path().join("events.jsonl");
                    if !p.is_file() {
                        continue;
                    }
                    let Some(session) = e
                        .path()
                        .file_name()
                        .and_then(|s| s.to_str())
                        .map(str::to_string)
                    else {
                        continue;
                    };
                    ss_files += 1;
                    crate::common::for_each_line(&p, |line| {
                        stats.lines_total += 1;
                        let Ok(v) = serde_json::from_str::<Value>(line) else {
                            stats.lines_bad_json += 1;
                            return;
                        };
                        if v.get("type").and_then(|x| x.as_str()) != Some("session.shutdown") {
                            if v.get("type").and_then(|x| x.as_str()).is_some_and(|t| t.starts_with("session.")) {
                                return; // start/resume:已知类型,静默跳过(§2)
                            }
                            stats.lines_unknown_type += 1;
                            return;
                        }
                        let Some(metrics) = v.pointer("/data/modelMetrics").and_then(|x| x.as_object()) else {
                            stats.lines_unknown_type += 1;
                            return;
                        };
                        let ts_ms = v
                            .get("timestamp")
                            .and_then(|x| x.as_str())
                            .and_then(crate::common::iso_to_ms)
                            .unwrap_or(0);
                        let event_id = v.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
                        for (model, _m) in metrics {
                            let Some(usage) = _m.pointer("/usage") else { continue };
                            snaps
                                .entry((session.clone(), model.clone()))
                                .or_default()
                                .push(ShutdownSnap { ts_ms, event_id: event_id.clone(), usage: usage.clone() });
                        }
                    });
                    stats.files_parsed += 1;
                    if max_files.is_some_and(|m| ss_files >= m) {
                        break;
                    }
                }
            }
            // 累计快照 → 相邻差分拆区间增量(§2;SnapshotDiff 首见/下降取全量)
            for ((session, model), mut seq) in snaps {
                seq.sort_by_key(|s| s.ts_ms);
                let mut diff = SnapshotDiff::default();
                for s in &seq {
                    last_shutdown.insert((session.clone(), model.clone()), s.ts_ms);
                    let g = |k: &str| json_u64(s.usage.get(k));
                    let (i_raw, o, rs) = (g("inputTokens"), g("outputTokens"), g("reasoningTokens"));
                    let (cr, cw) = (g("cacheReadTokens"), g("cacheWriteTokens"));
                    let cur = [
                        i_raw.saturating_sub(cr.saturating_add(cw)), // 双减除(§2)
                        cr,
                        cw,
                        o,
                        rs,
                    ];
                    let d = diff.push(cur);
                    if d.iter().all(|&x| x == 0) {
                        continue; // 全零增量不入账
                    }
                    let key = if s.event_id.is_empty() {
                        format!("copilot:s:{}:{}:{}:{}", session, model, s.ts_ms, d[0])
                    } else {
                        format!("copilot:s:{}:{}:{}:{}", session, model, s.event_id, model)
                    };
                    if !seen.insert(key.clone()) {
                        continue;
                    }
                    records.push(UsageRecord {
                        source: "copilot",
                        session_id: session.clone(),
                        agent: None,
                        model: model.clone(),
                        ts_ms: s.ts_ms,
                        caliber: Caliber::RequestDelta,
                        input_uncached: d[0],
                        cache_read: d[1],
                        cache_write: d[2],
                        cache_write_1h: 0,
                        output: d[3],
                        reasoning_output: d[4],
                        cost_usd: None, // requests.cost 口径不明,不取(§6)
                        dedup_key: key,
                    });
                }
            }

            // ===== 通道 2:OTel(§3/§4)=====
            let otel_files_list = walk_with(&r.join("otel"), None, &|p| {
                p.extension().is_some_and(|x| x == "jsonl")
            });
            // traceId → 最高级别(§4):低级别行整组抑制,最高级别的行全部保留
            let mut trace_best: HashMap<String, OtelKind> = HashMap::new();
            let mut otel_rows: Vec<OtelRow> = Vec::new();
            for p in &otel_files_list {
                otel_files += 1;
                let mtime_ms = p
                    .metadata()
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                crate::common::for_each_line(p, |line| {
                    stats.lines_total += 1;
                    let Ok(v) = serde_json::from_str::<Value>(line) else {
                        stats.lines_bad_json += 1;
                        return;
                    };
                    let attrs = v.get("attributes").unwrap_or(&Value::Null);
                    let a = |k: &str| attrs.get(k);
                    let is_span = v.get("type").and_then(|x| x.as_str()) == Some("span");
                    let name = v.get("name").and_then(|x| x.as_str()).unwrap_or("");
                    let event_name = v.pointer("/event.name").and_then(|x| x.as_str()).unwrap_or("");
                    let body = v.get("body").and_then(|x| x.as_str()).unwrap_or("");
                    // 类型判别(§3)
                    let kind = if is_span
                        && (a("gen_ai.operation.name").and_then(|x| x.as_str()) == Some("chat")
                            || name.starts_with("chat "))
                    {
                        Some(OtelKind::ChatSpan)
                    } else if !is_span
                        && (event_name == "gen_ai.client.inference.operation.details"
                            || body.starts_with("GenAI inference:"))
                    {
                        Some(OtelKind::InferenceLog)
                    } else if !is_span && event_name == "copilot_chat.agent.turn" {
                        Some(OtelKind::AgentTurnLog)
                    } else if is_span && a("gen_ai.operation.name").and_then(|x| x.as_str()) == Some("invoke_agent") {
                        Some(OtelKind::AgentSummarySpan)
                    } else {
                        None
                    };
                    let Some(kind) = kind else {
                        stats.lines_unknown_type += 1;
                        return;
                    };
                    // 用量(§3;数值可为数字或字符串)
                    let input = json_u64(a("gen_ai.usage.input_tokens"));
                    let cr = json_u64(a("gen_ai.usage.cache_read.input_tokens"));
                    let o = json_u64(a("gen_ai.usage.output_tokens"));
                    let cw = json_u64(a("gen_ai.usage.cache_write.input_tokens"))
                        .max(json_u64(a("gen_ai.usage.cache_creation.input_tokens")));
                    let rs = json_u64(a("gen_ai.usage.reasoning.output_tokens"))
                        .max(json_u64(a("gen_ai.usage.reasoning_tokens")));
                    if input == 0 && o == 0 && cr == 0 && cw == 0 && rs == 0 {
                        return;
                    }
                    let input_uncached = input.saturating_sub(cr.min(input)); // input 含 cache_read(§3)
                    // 会话(§5)
                    let session = ["gen_ai.conversation.id", "copilot_chat.session_id", "copilot_chat.chat_session_id", "session.id", "github.copilot.interaction_id", "gen_ai.response.id"]
                        .iter()
                        .find_map(|k| a(k).and_then(|x| x.as_str()))
                        .map(str::to_string);
                    let trace = v.get("traceId").and_then(|x| x.as_str()).unwrap_or("").to_string();
                    // 模型(§5):response.model > request.model,剥 -1m 后缀
                    let model = ["gen_ai.response.model", "gen_ai.request.model"]
                        .iter()
                        .find_map(|k| a(k).and_then(|x| x.as_str()))
                        .map(|m| {
                            m.strip_suffix("-1m-internal")
                                .or_else(|| m.strip_suffix("-1m"))
                                .unwrap_or(m)
                                .to_string()
                        })
                        .unwrap_or_else(|| "unknown".into());
                    // 时间戳(§3):数组 > 标量 > timeUnixNano > mtime
                    let ts_ms = ["endTime", "startTime", "hrTime", "_hrTime", "time"]
                        .iter()
                        .find_map(|k| flex_to_ms(v.get(*k)))
                        .or_else(|| v.get("timeUnixNano").and_then(|x| x.as_f64()).map(|n| (n / 1e6) as i64))
                        .unwrap_or(mtime_ms);
                    let content_key = format!(
                        "copilot:o:{}:{}:{}:{}:{}",
                        v.get("spanId").and_then(|x| x.as_str()).unwrap_or(trace.as_str()),
                        ts_ms,
                        model,
                        input_uncached,
                        o
                    );
                    // 同 traceId 优先级登记(§4):记录该 trace 出现过的最高级别
                    if !trace.is_empty() {
                        let e = trace_best.entry(trace.clone()).or_insert(kind);
                        if kind > *e {
                            *e = kind;
                        }
                    }
                    let sess = session.unwrap_or_else(|| trace.clone());
                    otel_rows.push(OtelRow {
                        ts_ms,
                        trace,
                        session: sess,
                        model,
                        content_key,
                        kind,
                        buckets: [input_uncached, cr, cw, o, rs],
                    });
                });
                stats.files_parsed += 1;
                if max_files.is_some_and(|m| otel_files >= m) {
                    break;
                }
            }
            for row in otel_rows {
                // 双计防御(§4):≤ 同 (session,model) 末次 shutdown 的 OTel 行丢弃
                if let Some(&cut) = last_shutdown.get(&(row.session.clone(), row.model.clone())) {
                    if cut > 0 && row.ts_ms <= cut {
                        continue;
                    }
                }
                // 同 traceId 优先级抑制(§4):该 trace 存在更高级别 → 低级别行丢弃
                if !row.trace.is_empty() {
                    if let Some(&best) = trace_best.get(&row.trace) {
                        if best > row.kind {
                            continue;
                        }
                    }
                }
                if !seen.insert(row.content_key.clone()) {
                    continue;
                }
                records.push(UsageRecord {
                    source: "copilot",
                    session_id: row.session,
                    agent: None,
                    model: row.model,
                    ts_ms: row.ts_ms,
                    caliber: Caliber::RequestDelta,
                    input_uncached: row.buckets[0],
                    cache_read: row.buckets[1],
                    cache_write: row.buckets[2],
                    cache_write_1h: 0,
                    output: row.buckets[3],
                    reasoning_output: row.buckets[4],
                    cost_usd: None,
                    dedup_key: row.content_key,
                });
            }
        }

        stats.notes.push(format!("session-state 文件 {ss_files} · otel 文件 {otel_files}"));
        if !last_shutdown.is_empty() {
            stats.notes.push(format!("shutdown 快照组 {}(相邻差分)", last_shutdown.len()));
        }
        (records, stats)
    }
}

/// session-store.db 的 assistant_usage_events 逐请求通道(§7.1)。
/// 列防御:total_nano_aiu 等为后期迁移列,缺失按 NULL 读。
fn scan_session_store(
    db: &Path,
    records: &mut Vec<UsageRecord>,
    seen: &mut HashSet<String>,
    stats: &mut ScanStats,
) -> usize {
    let Some(conn) = open_ro(db) else {
        return 0;
    };
    if !has_table(&conn, "assistant_usage_events") {
        return 0;
    }
    let has_nano = has_column(&conn, "assistant_usage_events", "total_nano_aiu");
    let has_reason = has_column(&conn, "assistant_usage_events", "reasoning_tokens");
    let nano_sel = if has_nano { "total_nano_aiu" } else { "NULL" };
    let reason_sel = if has_reason { "reasoning_tokens" } else { "0" };
    let sql = format!(
        "SELECT id, session_id, agent_id, model, input_tokens, output_tokens, \
                cache_read_tokens, cache_write_tokens, {reason_sel}, {nano_sel}, created_at \
                FROM assistant_usage_events ORDER BY id"
    );
    let Ok(mut stmt) = conn.prepare(&sql) else {
        stats.notes.push(format!("{} assistant_usage_events 查询构建失败", db.display()));
        stats.failed = true;
        return 0;
    };
    let Ok(mut rows) = stmt.query([]) else {
        stats.notes.push(format!("{} assistant_usage_events 查询执行失败", db.display()));
        stats.failed = true;
        return 0;
    };
    let mut n = 0usize;
    while let Ok(Some(row)) = rows.next() {
        let vals: Vec<SqlValue> = (0..11).filter_map(|i| row.get::<_, SqlValue>(i).ok()).collect();
        if vals.len() < 11 {
            stats.lines_bad_json += 1;
            continue;
        }
        let Some(lid) = v_text(&vals[0]) else {
            stats.lines_bad_json += 1;
            continue;
        };
        stats.lines_total += 1;
        let session_id = v_text(&vals[1]).unwrap_or_default();
        let agent_raw = v_text(&vals[2]).unwrap_or_default();
        let model = v_text(&vals[3]).unwrap_or_default();
        let model = if model.trim().is_empty() { "unknown".into() } else { model };
        let input = v_u64(&vals[4]); // 已含 cacheRead+cacheWrite(§7.3)
        let output_gross = v_u64(&vals[5]); // 已含 reasoning(§7.3)
        let cr = v_u64(&vals[6]);
        let cw = v_u64(&vals[7]);
        let reasoning = v_u64(&vals[8]);
        let nano = match &vals[9] {
            SqlValue::Integer(i) if *i > 0 => *i as u64,
            SqlValue::Real(f) if *f > 0.0 => *f as u64,
            _ => 0,
        };
        if input == 0 && output_gross == 0 && cr == 0 && cw == 0 {
            continue; // 全零跳过(三跳过)
        }
        // created_at:SQLite datetime('now') naive UTC "YYYY-MM-DD HH:MM:SS"(§7)
        let ts_ms = v_text(&vals[10])
            .and_then(|s| {
                chrono::NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S")
                    .ok()
                    .map(|t| t.and_utc().timestamp_millis())
            })
            .unwrap_or(0);
        // 成本(§7.3):美元 ≈ total_nano_aiu/1e9 × 0.01(1 AIU=1 credit=$0.01)
        let cost = if nano > 0 { Some(nano as f64 / 1e9 * 0.01) } else { None };
        let agent = match agent_raw.trim() {
            "" | "main" => None,
            a => Some(a.to_string()),
        };
        let key = format!("copilot:store:{}:{}", db.display(), lid);
        if seen.insert(key.clone()) {
            records.push(UsageRecord {
                source: "copilot",
                session_id,
                agent,
                model,
                ts_ms,
                caliber: Caliber::RequestDelta, // 逐请求,免差分(§7.1)
                input_uncached: input.saturating_sub(cr).saturating_sub(cw),
                cache_read: cr,
                cache_write: cw,
                cache_write_1h: 0,
                output: output_gross.saturating_sub(reasoning),
                reasoning_output: reasoning,
                cost_usd: cost,
                dedup_key: key,
            });
            n += 1;
        }
    }
    n
}

fn open_ro(db: &Path) -> Option<Connection> {
    let conn = crate::common::open_sqlite_readonly(db).ok()?;
    Some(conn)
}

fn has_table(conn: &Connection, name: &str) -> bool {
    conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
        [name],
        |r| r.get::<_, i64>(0),
    )
    .unwrap_or(0)
        > 0
}

fn has_column(conn: &Connection, table: &str, col: &str) -> bool {
    let Ok(mut stmt) = conn.prepare(&format!("PRAGMA table_info({table})")) else {
        return false;
    };
    let Ok(mut rows) = stmt.query([]) else {
        return false;
    };
    while let Ok(Some(r)) = rows.next() {
        if let Ok(name) = r.get::<_, String>(1) {
            if name == col {
                return true;
            }
        }
    }
    false
}

fn v_text(v: &SqlValue) -> Option<String> {
    match v {
        SqlValue::Text(s) => Some(s.clone()),
        SqlValue::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}

fn v_u64(v: &SqlValue) -> u64 {
    match v {
        SqlValue::Integer(i) => (*i).max(0) as u64,
        SqlValue::Real(f) if *f > 0.0 => *f as u64,
        SqlValue::Text(s) => s.trim().parse().unwrap_or(0),
        _ => 0,
    }
}
