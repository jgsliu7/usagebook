//! MiniMax 适配器 —— 依据 docs/数据源档案/08-minimax.md v1.0(冻结)。
//!
//! 统计主源:<root>/v2/sqlite/runtime-state.sqlite 的 local_runtime_token_usage 表
//! (本机 61MB 主库 + 7MB WAL / 1299 行);根目录 sqlite.db 是配置库干扰项,不读(档案 §2/§5.6)。
//! SQLite 纪律(档案 §5.5/§8):只读连接 + busy_timeout + query_only,不动 journal mode
//! —— 桌面端常驻 WAL,**绝不 checkpoint**;打开失败重试一次。
//! 口径(档案 §4 硬约束):input_tokens 为 **exclusive(不含缓存)** —— 与 ZCode/WorkBuddy
//! 相反,五列直取、切勿再减 cache(本机形状判定 1299/1299:totalTokens == 五分量和)。
//! 成本:cost_usd 列在场但本机全 0.0,不采成本(非零才作源自带成本采入并注记)。
//! 项目归属(档案 §5.2,与 juejin 分歧点):必须从 local_runtime_sessions.record_json 的
//! workspaceDir 取 —— juejin 直列 SQL 在本机 prepare 失败跑出 0 事件,本适配器独立实现;
//! 统一模型暂无项目字段,归属以扫描注记(分布)形式上报。
//! 去重键:minimax-sqlite:{db路径}:{id} —— id AUTOINCREMENT 主键天然幂等(档案 §5.1);
//! juejin 水位同样按 dbPath 分库记录,多 profile 根 id 序列独立,键带库路径防串。

use crate::common::*;
use crate::model::*;
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

pub struct MinimaxSource;

/// epoch 值归一为毫秒:<1e12 视为秒 ×1000(档案 00-总览 §五 横切规范;本源 ts 恒为毫秒,防御性保留)。
fn norm_ms(t: i64) -> i64 {
    if t > 0 && t < 1_000_000_000_000 {
        t * 1000
    } else {
        t
    }
}

impl UsageSource for MinimaxSource {
    fn id(&self) -> &'static str {
        "minimax"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        // env 覆盖三选一(档案 §1,与 juejin 相同的覆盖顺序);设置后不回退默认根
        for var in ["MINIMAX_CODE_HOME", "MINIMAX_DATA_DIR", "MAVIS_DATA_DIR"] {
            let roots = env_roots(var);
            if !roots.is_empty() {
                return roots;
            }
        }
        // 多 profile 根:枚举 ~/.minimax* 而非仅 ~/.minimax(档案 §8);
        // ~/.minimax-code 是安装目录(程序本体),按名排除(档案 §1/§2),
        // 其余干扰由"必须有 local_runtime_token_usage 表"判据兜底(档案 §5.6)。
        let mut roots = Vec::new();
        if let Ok(rd) = std::fs::read_dir(home()) {
            for e in rd.flatten() {
                let Some(name) = e.file_name().to_str().map(|s| s.to_string()) else {
                    continue;
                };
                if name.starts_with(".minimax") && name != ".minimax-code" && e.path().is_dir() {
                    roots.push(e.path());
                }
            }
        }
        roots.sort(); // read_dir 顺序不定,排序保证确定性
        roots
    }

    fn scan_roots(&self, roots: &[std::path::PathBuf], _max_files: Option<usize>) -> (Vec<UsageRecord>, ScanStats) {
        // max_files 对本源无意义:sqlite 单库全量读,只 SELECT 需要的列
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            stats
                .notes
                .push("未发现 ~/.minimax* 或 MINIMAX_CODE_HOME/MINIMAX_DATA_DIR/MAVIS_DATA_DIR".into());
            return (vec![], stats);
        }

        let mut records: Vec<UsageRecord> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new(); // 幂等兜底(键含库路径:同库经不同路径写法发现两次可去重;不同路径的真拷贝会各自入账,已知取舍)
        let mut sessions: HashSet<String> = HashSet::new();
        let mut models: HashSet<String> = HashSet::new();
        let mut agent_dist: HashMap<String, u64> = HashMap::new(); // agent_name 子代理/任务维度(档案 §4)
        let mut workspace_dist: HashMap<String, u64> = HashMap::new(); // 项目归属分布(record_json.workspaceDir)
        let mut ts_min = i64::MAX;
        let mut ts_max = i64::MIN;
        let mut skipped_no_time = 0u64;
        let mut skipped_zero = 0u64; // 全零 usage 跳过(档案 00-总览 §五.5 三跳过)
        let mut unknown_model = 0u64; // model NULL 记 "unknown" 并计数,不猜(档案 §8)
        let mut cost_nonzero = 0u64; // cost_usd 非零(本机全 0,出现即注记)
        let mut no_workspace = 0u64; // 有用量但查不到 workspace 归属的行

        for root in roots {
            // sessions/observability 仅探测计数,统计不读(档案 §2:ledger/display/OTEL 均无用量)
            let sessions_jsonl = walk_jsonl(&root.join("v2/sessions"), None).len();
            let obs_jsonl = walk_jsonl(&root.join("v2/observability"), None).len();
            if sessions_jsonl + obs_jsonl > 0 {
                stats.notes.push(format!(
                    "v2/sessions {} 个 + observability {} 个 jsonl 无用量,仅探测不读(档案 §2)",
                    sessions_jsonl, obs_jsonl
                ));
            }
            let db = root.join("v2/sqlite/runtime-state.sqlite");
            stats.files_seen += sessions_jsonl + obs_jsonl + 1;
            if !db.is_file() {
                stats
                    .notes
                    .push(format!("根 {} 下无 v2/sqlite/runtime-state.sqlite(空 profile 根,跳过)", root.display()));
                continue;
            }

            // 只读打开 + busy_timeout + query_only;失败重试一次;不动 journal mode(档案 §5.5/§8)
            let try_open = || {
                Connection::open_with_flags(
                    &db,
                    OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
            };
            let conn = match try_open().or_else(|_| try_open()) {
                Ok(c) => c,
                Err(e) => {
                    stats.notes.push(format!("runtime-state.sqlite 打开失败: {}", e));
                    stats.failed = true;
                    continue;
                }
            };
            if let Err(e) = conn.busy_timeout(Duration::from_secs(5)) {
                stats.notes.push(format!("busy_timeout 设置失败: {}", e));
                stats.failed = true;
            }
            let _ = conn.pragma_update(None, "query_only", true);

            // 干扰判据:必须有 local_runtime_token_usage 表(根 sqlite.db/安装目录由此排除,档案 §5.6)
            let has_table = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='local_runtime_token_usage'",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .map(|n| n > 0)
                .unwrap_or(false);
            if !has_table {
                stats
                    .notes
                    .push("库中无 local_runtime_token_usage 表,整库跳过(非账本/干扰库,档案 §5.6)".into());
                continue;
            }

            // 列探测:缺必需列 → 整库忽略(两代 schema 防御,档案 §8)
            let usage_cols: Vec<String> = conn
                .prepare("PRAGMA table_info(local_runtime_token_usage)")
                .and_then(|mut s| Ok(s.query_map([], |r| r.get::<_, String>(1))?.flatten().collect()))
                .unwrap_or_default();
            const REQUIRED: [&str; 9] = [
                "id",
                "session_id",
                "agent_name",
                "ts",
                "input_tokens",
                "output_tokens",
                "reasoning_tokens",
                "cache_read_tokens",
                "cache_write_tokens",
            ];
            let missing: Vec<&str> = REQUIRED
                .iter()
                .filter(|c| !cols_contains(&usage_cols, c))
                .cloned()
                .collect();
            if !missing.is_empty() {
                stats.notes.push(format!(
                    "local_runtime_token_usage 缺必需列 {:?},整库忽略(布局待适配)",
                    missing
                ));
                continue;
            }

            // 项目归属:sessions 表两代 schema 探测(档案 §8)——优先 record_json→workspaceDir
            // (本机形态,juejin 在此分歧跑出 0 事件);直列 workspace_dir/project_workspace_dir
            // 在场则直读;均无则归属记空并计数。
            let mut workspace: HashMap<String, String> = HashMap::new();
            let mut record_bad = 0u64; // record_json 解析失败/无 workspaceDir 的会话数
            let sess_cols: Vec<String> = conn
                .prepare("PRAGMA table_info(local_runtime_sessions)")
                .and_then(|mut s| Ok(s.query_map([], |r| r.get::<_, String>(1))?.flatten().collect()))
                .unwrap_or_default();
            let sess_has = |c: &str| cols_contains(&sess_cols, c);
            if sess_has("record_json") {
                stats
                    .notes
                    .push("项目归属: local_runtime_sessions.record_json.workspaceDir(本机 blob 形态,档案 §5.2)".into());
                if let Ok(mut stmt) =
                    conn.prepare("SELECT session_id, record_json FROM local_runtime_sessions")
                {
                    if let Ok(mut rows) = stmt.query([]) {
                        while let Ok(Some(row)) = rows.next() {
                            let sid: String = row.get(0).unwrap_or_default();
                            let json: Option<String> = row.get(1).ok();
                            let dir = json
                                .as_deref()
                                .and_then(|j| serde_json::from_str::<Value>(j).ok())
                                .and_then(|v| {
                                    v.get("workspaceDir")
                                        .and_then(|w| w.as_str())
                                        .map(|s| s.to_string())
                                });
                            match dir {
                                Some(d) => {
                                    workspace.insert(sid, d);
                                }
                                None => record_bad += 1,
                            }
                        }
                    }
                }
            } else if sess_has("workspace_dir") || sess_has("project_workspace_dir") {
                let col = if sess_has("workspace_dir") { "workspace_dir" } else { "project_workspace_dir" };
                stats
                    .notes
                    .push(format!("项目归属: local_runtime_sessions 直列 {}(档案 §8)", col));
                let sql = format!("SELECT session_id, {} FROM local_runtime_sessions", col);
                if let Ok(mut stmt) = conn.prepare(&sql) {
                    if let Ok(mut rows) = stmt.query([]) {
                        while let Ok(Some(row)) = rows.next() {
                            let sid: String = row.get(0).unwrap_or_default();
                            if let Some(d) = row.get::<_, Option<String>>(1).ok().flatten() {
                                workspace.insert(sid, d);
                            } else {
                                record_bad += 1;
                            }
                        }
                    }
                }
            } else {
                stats
                    .notes
                    .push("sessions 表无 record_json 亦无直列 workspace,项目归属不可得(档案 §8)".into());
            }
            if record_bad > 0 {
                stats
                    .notes
                    .push(format!("{} 个会话 record_json 坏行/无 workspaceDir,归属记空", record_bad));
            }

            // 用量主查询:固定列序直取(五列 exclusive,档案 §4);ORDER BY id 保证确定性
            let sql = "SELECT id, session_id, agent_name, model, ts, input_tokens, output_tokens, \
                       reasoning_tokens, cache_read_tokens, cache_write_tokens, cost_usd \
                       FROM local_runtime_token_usage ORDER BY id";
            let Ok(mut stmt) = conn.prepare(sql) else {
                stats.notes.push("local_runtime_token_usage 查询构建失败".into());
                stats.failed = true;
                continue;
            };
            let Ok(mut rows) = stmt.query([]) else {
                stats.notes.push("local_runtime_token_usage 查询执行失败".into());
                stats.failed = true;
                continue;
            };
            while let Ok(Some(row)) = rows.next() {
                // 0 id, 1 session_id, 2 agent_name, 3 model, 4 ts,
                // 5 input, 6 output, 7 reasoning, 8 cache_read, 9 cache_write, 10 cost_usd
                let (Ok(id), Ok(session_id), Ok(agent_name)) =
                    (row.get::<_, i64>(0), row.get::<_, String>(1), row.get::<_, String>(2))
                else {
                    stats.lines_bad_json += 1;
                    continue;
                };
                stats.lines_total += 1;
                let model: Option<String> = row.get(3).ok().flatten();
                let ts_raw: Option<i64> = row.get(4).ok().flatten();
                let Some(ts_ms) = ts_raw.filter(|t| *t > 0).map(norm_ms) else {
                    skipped_no_time += 1;
                    continue;
                };

                // 五列 exclusive 直取,不减除(档案 §4 硬约束)。
                // sql_u64 宽松读(2026-10-17):列类型为 REAL 时 get::<i64> 失败→丢量
                let sql_u64 = |i: usize| match row.get::<_, rusqlite::types::Value>(i) {
                    Ok(rusqlite::types::Value::Integer(n)) => (n.max(0)) as u64,
                    Ok(rusqlite::types::Value::Real(f)) if f > 0.0 => f as u64,
                    _ => 0,
                };
                let input_uncached = sql_u64(5);
                let output = sql_u64(6);
                let reasoning_output = sql_u64(7);
                let cache_read = sql_u64(8);
                let cache_write = sql_u64(9);
                if input_uncached + output + reasoning_output + cache_read + cache_write == 0 {
                    skipped_zero += 1; // 全零 usage 跳过(三跳过纪律)
                    continue;
                }
                // cost_usd 本机全 0 → None;非零才作源自带成本采入(档案 §4/§8)
                let cost = row
                    .get::<_, Option<f64>>(10)
                    .ok()
                    .flatten()
                    .filter(|c| *c != 0.0);
                if cost.is_some() {
                    cost_nonzero += 1;
                }

                let key = format!("minimax-sqlite:{}:{}", db.display(), id);
                if seen.insert(key.clone()) {
                    let model = model.unwrap_or_else(|| {
                        unknown_model += 1;
                        "unknown".into() // model NULL 不猜(档案 §8)
                    });
                    sessions.insert(session_id.clone());
                    models.insert(model.clone());
                    *agent_dist.entry(agent_name.clone()).or_insert(0) += 1;
                    match workspace.get(&session_id) {
                        Some(w) => {
                            *workspace_dist.entry(w.clone()).or_insert(0) += 1;
                        }
                        None => no_workspace += 1,
                    }
                    ts_min = ts_min.min(ts_ms);
                    ts_max = ts_max.max(ts_ms);
                    records.push(UsageRecord {
                        source: "minimax",
                        session_id,
                        agent: Some(agent_name), // agent_name 白送子代理/任务维度(档案 §4)
                        model,
                        ts_ms,
                        caliber: Caliber::RequestDelta,
                        input_uncached,
                        cache_read,
                        cache_write,
                        cache_write_1h: 0,
                        output,
                        reasoning_output,
                        cost_usd: cost,
                        dedup_key: key,
                    });
                }
            }
            stats.files_parsed += 1;
        }

        if !records.is_empty() {
            let agents = sorted_dist(&agent_dist);
            stats.notes.push(format!(
                "时间范围 {} ~ {} · 会话 {} · 模型 {}",
                ms_to_local_date(ts_min),
                ms_to_local_date(ts_max),
                sessions.len(),
                models.iter().cloned().collect::<Vec<_>>().join(", ")
            ));
            stats.notes.push(format!("agent 分布: {}", agents));
            // 项目归属分布(统一模型暂无项目字段,以注记上报;档案 §5.2 分歧点的落地形态)
            stats
                .notes
                .push(format!("项目归属(workspaceDir): {}", sorted_dist(&workspace_dist)));
        }
        if skipped_no_time > 0 {
            stats
                .notes
                .push(format!("{} 行无有效时间戳(ts 缺失/非正),跳过", skipped_no_time));
        }
        if skipped_zero > 0 {
            stats.notes.push(format!("{} 行全零 usage,跳过(三跳过纪律)", skipped_zero));
        }
        if unknown_model > 0 {
            stats
                .notes
                .push(format!("{} 行 model 为 NULL,记 unknown(messages.jsonl 回填路径无实测样本,v1 不实现,档案 §8)", unknown_model));
        }
        if cost_nonzero > 0 {
            stats
                .notes
                .push(format!("{} 行 cost_usd 非零,作源自带成本采入(本机档案实证为全 0)", cost_nonzero));
        }
        if no_workspace > 0 {
            stats
                .notes
                .push(format!("{} 行用量查不到 workspace 归属(sessions 表缺该会话)", no_workspace));
        }
        (records, stats)
    }
}

/// 列集合包含判断(Vec<String> 含指定列名)。
fn cols_contains(cols: &[String], c: &str) -> bool {
    cols.iter().any(|x| x == c)
}

/// HashMap 分布 → "k=v, k=v"(按值降序再按键,保证确定性)。
fn sorted_dist(m: &HashMap<String, u64>) -> String {
    let mut v: Vec<(&String, &u64)> = m.iter().collect();
    v.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
    v.iter()
        .map(|(k, n)| format!("{}={}", k, n))
        .collect::<Vec<_>>()
        .join(", ")
}
