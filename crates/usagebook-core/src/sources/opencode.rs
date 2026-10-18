//! OpenCode 适配器 —— 依据 docs/数据源档案/28-opencode.md(Community,本机未实证)。
//!
//! 根(§1):OPENCODE_DATA_DIR(多根)或 $XDG_DATA_HOME/opencode → ~/.local/share/opencode,
//!     macOS 候选 ~/Library/Application Support/opencode
//! 存储(§2):主库 opencode.db > channel 库 opencode-*.db 排序取首;
//!     表内 v1 message / v2 session_message 并存逐一探测;无 db 时 JSON storage/message 兜底
//! payload(§3):tokens 五桶互斥直取;模型 modelID>model.id>modelId;time.created 秒/毫秒自适应
//! 去重键(§4):opencode:{db名}:{id}(JSON 兜底用文件路径);fork 复制行 v0.1 不处理,注记上报

use crate::common::{env_roots, home, json_u64, walk_with};
use crate::model::{Caliber, ScanStats, UsageSource, UsageRecord};
use rusqlite::types::Value as SqlValue;
use std::collections::HashSet;
use std::path::PathBuf;

pub struct OpencodeSource;

/// SQLite 值 → 文本。
fn v_text(v: &SqlValue) -> Option<String> {
    match v {
        SqlValue::Text(s) => Some(s.clone()),
        SqlValue::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}

/// payload data JSON → 五桶 + 模型 + 时间 + 成本(§3)。role/type 判定由调用方注入。
struct Payload {
    input: u64,
    cache_read: u64,
    cache_write: u64,
    output: u64,
    reasoning: u64,
    model: String,
    ts_ms: i64,
    cost_usd: Option<f64>,
}

/// 选择性反序列化(内存审计):只声明被消费的字段,serde 跳过未知键——
/// 尤其 parts/content 等大体积字段完全不建 DOM(实测峰值 582MB 的主因)。
#[derive(serde::Deserialize)]
struct OcMsg<'a> {
    #[serde(rename = "type", alias = "role")]
    role: Option<&'a str>,
    tokens: Option<OcTokens>,
    #[serde(rename = "modelID", alias = "modelId")]
    model_id: Option<&'a str>,
    model: Option<OcModel<'a>>,
    time: Option<OcTime>,
    cost: Option<f64>,
}
#[derive(serde::Deserialize)]
struct OcTokens {
    input: Option<u64>,
    output: Option<u64>,
    reasoning: Option<u64>,
    cache: Option<OcCache>,
}
#[derive(serde::Deserialize)]
struct OcCache {
    read: Option<u64>,
    write: Option<u64>,
}
#[derive(serde::Deserialize)]
struct OcModel<'a> {
    id: Option<&'a str>,
}
#[derive(serde::Deserialize)]
struct OcTime {
    created: Option<i64>,
}

fn parse_msg(m: &OcMsg) -> Option<Payload> {
    let tk = m.tokens.as_ref()?;
    let (i, o, rs) = (tk.input.unwrap_or(0), tk.output.unwrap_or(0), tk.reasoning.unwrap_or(0));
    let (cr, cw) = match tk.cache.as_ref() {
        Some(c) => (c.read.unwrap_or(0), c.write.unwrap_or(0)),
        None => (0, 0),
    };
    if i == 0 && o == 0 && cr == 0 && cw == 0 && rs == 0 {
        return None; // 五桶全 0 不入账(§3)
    }
    let model = m
        .model_id
        .or(m.model.as_ref().and_then(|x| x.id))
        .unwrap_or("unknown")
        .to_string();
    let ts_ms = m.time.as_ref().and_then(|t| t.created).map(crate::common::secs_or_ms).unwrap_or(0);
    Some(Payload {
        input: i,
        cache_read: cr,
        cache_write: cw,
        output: o,
        reasoning: rs,
        model,
        ts_ms,
        cost_usd: m.cost,
    })
}

fn parse_payload(doc: &serde_json::Value) -> Option<Payload> {
    let tk = doc.get("tokens")?;
    let (i, o, rs) = (
        json_u64(tk.get("input")),
        json_u64(tk.get("output")),
        json_u64(tk.get("reasoning")),
    );
    let (cr, cw) = match tk.get("cache") {
        Some(c) if c.is_object() => (json_u64(c.get("read")), json_u64(c.get("write"))),
        _ => (0, 0),
    };
    if i == 0 && o == 0 && cr == 0 && cw == 0 && rs == 0 {
        return None; // 五桶全 0 不入账;不按 total 估算(§3)
    }
    let model = ["modelID", "modelId"]
        .iter()
        .find_map(|k| doc.get(*k).and_then(|x| x.as_str()))
        .or_else(|| doc.pointer("/model/id").and_then(|x| x.as_str()))
        .unwrap_or("unknown")
        .to_string();
    let ts_ms = doc
        .pointer("/time/created")
        .and_then(|x| x.as_i64())
        .map(crate::common::secs_or_ms)
        .unwrap_or(0);   // 缺失不再静默丢行:落 0 由调用方计数上报(2026-10-17,对齐 claude 处理)
    Some(Payload {
        input: i,
        cache_read: cr,
        cache_write: cw,
        output: o,
        reasoning: rs,
        model,
        ts_ms,
        cost_usd: doc.get("cost").and_then(|x| x.as_f64()),
    })
}

/// 单个 db 内三通道互斥探测(§2,本机实证):session_message(v2)> message(v1)>
/// session 聚合兜底。聚合 = 明细之和(本机对拍五列全吻合),同取两通道必双计。
fn scan_db(db: &std::path::Path, records: &mut Vec<UsageRecord>, seen: &mut HashSet<String>, stats: &mut ScanStats) -> bool {
    let Ok(conn) = crate::common::open_sqlite_readonly(db) else { return false; };

    let db_alias = db.file_name().and_then(|s| s.to_str()).unwrap_or("opencode.db");
    // 通道一/二:逐行明细(v2 session_message 判 type;v1 message 判 role)
    for (table, v2) in [("session_message", true), ("message", false)] {
        let exists = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0);
        if exists == 0 {
            continue;
        }
        let nonempty: i64 = conn
            .query_row(&format!("SELECT EXISTS(SELECT 1 FROM {table} LIMIT 1)"), [], |r| r.get(0))
            .unwrap_or(0);
        if nonempty == 0 {
            continue; // 空表(本机实证:v2 表建好未用)→ 下一通道
        }
        let Ok(mut stmt) = conn.prepare(&format!("SELECT id, session_id, data FROM {table}")) else {
            continue;
        };
        let Ok(mut rows) = stmt.query([]) else {
            continue;
        };
        let mut table_rows = 0u64;
        while let Ok(Some(row)) = rows.next() {
            // 流式改造(内存审计):get_ref 零拷贝借用替代 SqlValue collect+clone,
            // data 大列(均值 45KB,max 59.6MB)不再整体拷贝两份;再加 assistant 子串预过滤
            // (用户行占大头,宽松匹配误报只多一次解析,零正确性风险)
            let (Some(id), session) = (
                row.get_ref(0).ok().and_then(|r| match r {
                    rusqlite::types::ValueRef::Text(t) => std::str::from_utf8(t).ok().map(str::to_string),
                    _ => None,
                }),
                row.get_ref(1).ok().and_then(|r| match r {
                    rusqlite::types::ValueRef::Text(t) => std::str::from_utf8(t).ok().map(str::to_string),
                    _ => None,
                }).unwrap_or_default(),
            ) else {
                stats.lines_bad_json += 1;
                continue;
            };
            // 借用期内完成预判 + 选择性解析(from_slice 零 String 拷贝,借用不跨 rows.next)
            let doc = match row.get_ref(2).ok() {
                Some(rusqlite::types::ValueRef::Text(t)) => {
                    if !t.windows(9).any(|w| w == b"assistant") {
                        table_rows += 1;
                        continue;
                    }
                    match serde_json::from_slice::<OcMsg>(t) {
                        Ok(d) => d,
                        Err(_) => {
                            stats.lines_bad_json += 1;
                            continue;
                        }
                    }
                }
                _ => {
                    stats.lines_bad_json += 1;
                    continue;
                }
            };
            table_rows += 1;
            if doc.role != Some("assistant") {
                continue; // v1 "role"/v2 "type" 经 alias 归一
            }
            let Some(p) = parse_msg(&doc) else { continue };
            if p.ts_ms == 0 { stats.lines_unknown_type += 1; }   // 缺时间戳:入账但计数可见
            let key = format!("opencode:{}:{}", db_alias, id);
            if seen.insert(key.clone()) {
                records.push(UsageRecord {
                    source: "opencode",
                    session_id: session,
                    agent: None,
                    model: p.model,
                    ts_ms: p.ts_ms,
                    caliber: Caliber::RequestDelta,
                    input_uncached: p.input,
                    cache_read: p.cache_read,
                    cache_write: p.cache_write,
                    cache_write_1h: 0,
                    output: p.output,
                    reasoning_output: p.reasoning,
                    cost_usd: p.cost_usd,
                    dedup_key: key,
                });
            }
        }
        if table_rows > 0 {
            stats
                .notes
                .push(format!("{} {} 表 {} 行(v{})", db.display(), table, table_rows, if v2 { 2 } else { 1 }));
            stats.files_parsed += 1;
            return true; // 通道互斥(§2)
        }
    }
    // 通道三:session 表聚合列兜底(会话级累计,锚 time_created,§2)
    let has_cols = conn
        .prepare("PRAGMA table_info(session)")
        .map(|mut s| {
            s.query_map([], |r| r.get::<_, String>(1))
                .map(|it| it.flatten().any(|c| c == "tokens_input"))
                .unwrap_or(false)
        })
        .unwrap_or(false);
    if has_cols {
        let sql = "SELECT id, model, cost, tokens_input, tokens_output, tokens_reasoning, \
                   tokens_cache_read, tokens_cache_write, time_created FROM session \
                   WHERE tokens_input>0 OR tokens_output>0 OR tokens_reasoning>0 \
                      OR tokens_cache_read>0 OR tokens_cache_write>0";
        let Ok(mut stmt) = conn.prepare(sql) else { return false };
        let Ok(mut rows) = stmt.query([]) else { return false };
        let mut n = 0u64;
        while let Ok(Some(row)) = rows.next() {
            let vals: Vec<SqlValue> = (0..9).filter_map(|i| row.get::<_, SqlValue>(i).ok()).collect();
            if vals.len() < 9 {
                continue;
            }
            let Some(id) = v_text(&vals[0]) else { continue };
            let ts_ms = match &vals[8] {
                SqlValue::Integer(t) if *t > 0 => crate::common::secs_or_ms(*t),
                _ => 0,
            };
            if ts_ms <= 0 {
                continue;
            }
            let g = |i: usize| match &vals[i] {
                SqlValue::Integer(v) => (*v).max(0) as u64,
                SqlValue::Real(f) if *f > 0.0 => *f as u64,
                _ => 0,
            };
            let model = v_text(&vals[1])
                .and_then(|s| {
                    serde_json::from_str::<serde_json::Value>(&s)
                        .ok()
                        .and_then(|m| m.get("id").and_then(|x| x.as_str()).map(|s| s.to_string()))
                })
                .filter(|m| !m.is_empty())
                .or_else(|| v_text(&vals[1]).filter(|m| !m.is_empty()))
                .unwrap_or_else(|| "unknown".into());
            let cost_usd = match &vals[2] {
                SqlValue::Real(f) => Some(*f),
                SqlValue::Integer(i) => Some(*i as f64),
                _ => None,
            };
            let key = format!("opencode:session:{}", id);
            if seen.insert(key.clone()) {
                n += 1;
                records.push(UsageRecord {
                    source: "opencode",
                    session_id: id.clone(),
                    agent: None,
                    model,
                    ts_ms,
                    caliber: Caliber::RequestDelta, // 会话终态一次性入账(§4 同 droid 裁决)
                    input_uncached: g(3),
                    cache_read: g(6),
                    cache_write: g(7),
                    cache_write_1h: 0,
                    output: g(4),
                    reasoning_output: g(5),
                    cost_usd,
                    dedup_key: key,
                });
            }
        }
        if n > 0 {
            stats
                .notes
                .push(format!("{} session 聚合 {} 行(明细表空,兜底通道)", db.display(), n));
            stats.files_parsed += 1;
            return true;
        }
    }
    false
}

impl UsageSource for OpencodeSource {
    fn id(&self) -> &'static str {
        "opencode"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("OPENCODE_DATA_DIR");
        if roots.is_empty() {
            if let Ok(x) = std::env::var("XDG_DATA_HOME") {
                let p = PathBuf::from(x).join("opencode");
                if p.is_dir() {
                    roots.push(p);
                }
            }
        }
        if roots.is_empty() {
            let p = home().join(".local/share/opencode");
            if p.is_dir() {
                roots.push(p);
            }
        }
        if roots.is_empty() {
            let p = home().join("Library/Application Support/opencode");
            if p.is_dir() {
                roots.push(p);
            }
        }
        roots
    }

    fn scan_roots(&self, roots: &[std::path::PathBuf], _max_files: Option<usize>) -> (Vec<UsageRecord>, ScanStats) {
        // max_files 对本源无意义:sqlite 全量读;JSON 兜底量小
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            stats
                .notes
                .push("未发现 opencode 数据目录或 OPENCODE_DATA_DIR".into());
            return (vec![], stats);
        }

        let mut records: Vec<UsageRecord> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut fork_note = false;

        for root in roots {
            // 库选择(§2):主库 > channel 库排序取首。
            // 2026-10-17 修:主库存在但表空/读取失败时,回退 channel 库——
            // 此前 main.is_file() 即锁定主库,空表时数据静默丢失且无 note
            let main = root.join("opencode.db");
            let mut done = false;
            if main.is_file() {
                stats.files_seen += 1;
                done = scan_db(&main, &mut records, &mut seen, &mut stats);
                if !done {
                    stats.notes.push(format!(
                        "opencode 主库 {} 无用量记录,回退 channel 库/JSON",
                        main.display()
                    ));
                }
            }
            if !done {
                let mut chans: Vec<PathBuf> = walk_with(root, None, &|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("opencode-") && n.ends_with(".db"))
                });
                // walk_with 含子目录,限定根一层
                chans.retain(|p| p.parent() == Some(root));
                chans.sort();
                for d in chans {
                    stats.files_seen += 1;
                    if scan_db(&d, &mut records, &mut seen, &mut stats) {
                        done = true;
                        break;
                    }
                }
            }
            if done {
                // v2 库存在即注记 fork 风险(§4)
                fork_note = true;
                continue;
            }
            // JSON 兜底(§2):storage/message/**/*.json
            let msg_dir = root.join("storage/message");
            if msg_dir.is_dir() {
                let files = walk_with(&msg_dir, None, &|p| p.extension().is_some_and(|x| x == "json"));
                stats.files_seen += files.len();
                let mut parsed = 0u64;
                for path in &files {
                    let Ok(text) = std::fs::read_to_string(path) else {
                        stats.lines_bad_json += 1;
                        continue;
                    };
                    let Ok(doc) = serde_json::from_str::<serde_json::Value>(&text) else {
                        stats.lines_bad_json += 1;
                        continue;
                    };
                    let role = doc.get("role").or_else(|| doc.get("type"));
                    if role.and_then(|x| x.as_str()) != Some("assistant") {
                        continue;
                    }
                    let Some(p) = parse_payload(&doc) else { continue };
                    if p.ts_ms == 0 { stats.lines_unknown_type += 1; }   // 缺时间戳:入账但计数可见(2026-10-17)
                    // 会话 id = storage/message/<sid>/<mid>.json 的上级目录
                    let session = path
                        .parent()
                        .and_then(|d| d.file_name())
                        .and_then(|s| s.to_str())
                        .unwrap_or("opencode")
                        .to_string();
                    let key = format!("opencode:json:{}", path.display());
                    if seen.insert(key.clone()) {
                        parsed += 1;
                        records.push(UsageRecord {
                            source: "opencode",
                            session_id: session,
                            agent: None,
                            model: p.model,
                            ts_ms: p.ts_ms,
                            caliber: Caliber::RequestDelta,
                            input_uncached: p.input,
                            cache_read: p.cache_read,
                            cache_write: p.cache_write,
                            cache_write_1h: 0,
                            output: p.output,
                            reasoning_output: p.reasoning,
                            cost_usd: p.cost_usd,
                            dedup_key: key,
                        });
                    }
                }
                stats.files_parsed += files.len();
                if parsed > 0 {
                    stats.notes.push(format!("JSON 兜底 {} 条(storage/message)", parsed));
                }
            } else if !main.is_file() && !done {
                stats.notes.push(format!("根 {} 下无 opencode.db/opencode-*.db/storage", root.display()));
            }
        }

        if fork_note {
            stats
                .notes
                .push("fork 会话复制行未剔除,若使用过 fork 可能双计(v0.2 处理,§4)".into());
        }
        (records, stats)
    }
}
