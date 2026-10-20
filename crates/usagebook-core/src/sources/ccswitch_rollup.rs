//! CC Switch 日汇总回填 —— 依据 docs/数据源档案/13-cc-switch.md §5.5/§9 与
//! docs/跨源去重与重叠源规范.md §五(C 类二手镜像库消费纪律)。
//!
//! 根:~/.cc-switch/cc-switch.db(只读;App Support app_paths.json 定位留待后续)
//! 输入:usage_daily_rollups(日粒度,剪枝固化,本机唯一跨转录 GC 空窗的历史)
//! 纪律:只补原生适配器"无覆盖"的日期,拼接处按 日期×app 整日替换,禁止同日混采两源;
//!       行级表 proxy_request_logs 不采(与原生适配器 100% 重叠,双计);
//!       小时粒度不可得——整日挂本地 0 点桶,禁止均匀分摊编造。
//! 口径:新版 rollups 的 input 已归一 fresh(input_token_semantics=2,与 input_uncached 同义);
//!       老版库 codex 行(semantics 0/1)input 含缓存,按 cc-switch sql_helpers::fresh_input_sql
//!       同规则逐行减去后再聚合(claude/opencode/pi 存储即 exclusive,直取);
//!       output 为 cc-switch 合并口径(不含单列 reasoning);

use crate::common::*;
use crate::model::*;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

pub struct CcswitchRollupSource;

/// 回填覆盖的原生源,同时是 rollups.app_type → 本项目源 id 的映射(两者同名)。
/// snapshot.rs 的原生覆盖集过滤与此处 native_of 共用这一份清单——两处漂移会双计或漏回填。
pub const BACKFILL_NATIVE_SOURCES: &[&str] = &["claude", "codex", "opencode", "pi"];

/// cc-switch 侧"存储 input 含缓存"的 app(sql_helpers::CACHE_INCLUSIVE_APP_TYPES 镜像);
/// 仅 codex 在本项目映射范围内,其余列出以保持与上游规则逐字一致。
const CACHE_INCLUSIVE_APPS_SQL: &str = "'codex', 'gemini', 'grokbuild'";

/// rollups.app_type → 本项目原生源 id。未列出的 app_type 不回填(计数上报)。
fn native_of(app_type: &str) -> Option<&'static str> {
    BACKFILL_NATIVE_SOURCES
        .iter()
        .copied()
        .find(|s| *s == app_type)
}

fn db_path() -> Option<PathBuf> {
    let p = home().join(".cc-switch").join("cc-switch.db");
    p.is_file().then_some(p)
}

impl UsageSource for CcswitchRollupSource {
    fn id(&self) -> &'static str {
        "ccswitch-rollup"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        db_path()
            .and_then(|p| p.parent().map(|d| d.to_path_buf()))
            .into_iter()
            .collect()
    }

    fn scan_roots(
        &self,
        _roots: &[std::path::PathBuf],
        _max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        // 回填需要"原生源覆盖集",trait 扫描路径拿不到该信息 → 恒空(静默)。
        // 真正的回填在快照装配层调用 backfill()(规范 §七实现落点)。
        (vec![], ScanStats::default())
    }
}

/// 回填入口:covered = 原生源已覆盖的 (源id, 本地日期) 集合。
/// 这些日期整日跳过(整日替换,规范 §五.3);未覆盖日期按 rollups 日聚合入账。
pub fn backfill(covered: &HashSet<(String, String)>) -> (Vec<UsageRecord>, ScanStats) {
    match db_path() {
        Some(p) => backfill_at(&p, covered),
        None => {
            let mut stats = ScanStats::default();
            stats
                .notes
                .push("未发现 ~/.cc-switch/cc-switch.db,无历史可回填".into());
            (vec![], stats)
        }
    }
}

/// 测试注入点:对指定库文件执行回填。
pub fn backfill_at(
    db: &Path,
    covered: &HashSet<(String, String)>,
) -> (Vec<UsageRecord>, ScanStats) {
    let mut stats = ScanStats::default();
    let Ok(conn) = open_sqlite_readonly(db) else {
        stats
            .notes
            .push("cc-switch 库打开失败(可能正被独占),本轮跳过回填".into());
        return (vec![], stats);
    };
    let has_table = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='usage_daily_rollups'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap_or(0);
    if has_table == 0 {
        stats
            .notes
            .push("cc-switch 库无 usage_daily_rollups 表,本轮跳过回填".into());
        return (vec![], stats);
    }
    // 六元组 PK 同日多行(model 维度拆分)→ SQL 端 GROUP BY 归并(档案 §8);
    // 显式列名,禁 SELECT *;缺列按 0 由 COALESCE 兜底 + 计数上报。
    // input 逐行归一 fresh 后再 SUM:镜像 cc-switch sql_helpers::fresh_input_sql——
    // semantics 2(fresh)直取;含缓存 app 的 1(total)减 cr+cw、0(legacy)减 cr(不够减则直取);
    // 其余 app 直取。input_token_semantics 列为 v13 迁移新增,老库缺列时整体按 legacy(0)处理。
    let has_semantics = conn
        .prepare("SELECT input_token_semantics FROM usage_daily_rollups LIMIT 0")
        .is_ok();
    let sem = if has_semantics {
        "COALESCE(input_token_semantics,0)"
    } else {
        "0"
    };
    let fresh_input = format!(
        "CASE \
           WHEN {sem} = 2 THEN COALESCE(input_tokens,0) \
           WHEN app_type IN ({apps}) AND {sem} = 1 \
                AND COALESCE(input_tokens,0) >= COALESCE(cache_read_tokens,0) + COALESCE(cache_creation_tokens,0) \
             THEN COALESCE(input_tokens,0) - COALESCE(cache_read_tokens,0) - COALESCE(cache_creation_tokens,0) \
           WHEN app_type IN ({apps}) AND {sem} = 0 \
                AND COALESCE(input_tokens,0) >= COALESCE(cache_read_tokens,0) \
             THEN COALESCE(input_tokens,0) - COALESCE(cache_read_tokens,0) \
           ELSE COALESCE(input_tokens,0) END",
        apps = CACHE_INCLUSIVE_APPS_SQL
    );
    let sql = format!(
        "SELECT date, app_type, model, \
                SUM({fresh_input}), SUM(COALESCE(cache_read_tokens,0)), \
                SUM(COALESCE(cache_creation_tokens,0)), SUM(COALESCE(output_tokens,0)), \
                SUM(COALESCE(request_count,0)), \
                SUM(CASE WHEN app_type IN ({apps}) AND {sem} IN (0,1) THEN 1 ELSE 0 END) \
         FROM usage_daily_rollups GROUP BY date, app_type, model ORDER BY date",
        apps = CACHE_INCLUSIVE_APPS_SQL
    );
    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(e) => {
            stats
                .notes
                .push(format!("cc-switch rollups 查询失败({e}),本轮跳过回填"));
            return (vec![], stats);
        }
    };
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, i64>(3)?,
            r.get::<_, i64>(4)?,
            r.get::<_, i64>(5)?,
            r.get::<_, i64>(6)?,
            r.get::<_, i64>(7)?,
            r.get::<_, i64>(8)?,
        ))
    });
    let mut records = Vec::new();
    let mut skipped_covered = 0u64;
    let mut skipped_unknown_app = 0u64;
    let mut skipped_empty = 0u64;
    let mut normalized_legacy_rows = 0i64;
    let Ok(rows) = rows else {
        stats
            .notes
            .push("cc-switch rollups 读取失败,本轮跳过回填".into());
        return (vec![], stats);
    };
    for row in rows.flatten() {
        let (date, app_type, model, i, cr, cw, o, n_req, legacy_rows) = row;
        let Some(native) = native_of(&app_type) else {
            skipped_unknown_app += 1;
            continue;
        };
        if covered.contains(&(native.to_string(), date.clone())) {
            skipped_covered += 1;
            continue;
        }
        if (i + cr + cw + o == 0) || n_req <= 0 {
            skipped_empty += 1;
            continue;
        }
        normalized_legacy_rows += legacy_rows;
        // 日期 → 本地 0 点(epoch 毫秒);小时曲线该日呈 0 点单桶,不做分摊编造。
        let ts_ms = chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d")
            .ok()
            .and_then(|d| {
                use chrono::TimeZone;
                chrono::Local
                    .from_local_datetime(&d.and_hms_opt(0, 0, 0)?)
                    .earliest()
                    .map(|t| t.timestamp_millis())
            })
            .unwrap_or(0);
        if ts_ms == 0 {
            skipped_empty += 1;
            continue;
        }
        records.push(UsageRecord {
            source: "ccswitch-rollup",
            session_id: date.clone(),
            agent: None,
            model: if model.is_empty() {
                "unknown".to_string()
            } else {
                model.clone()
            },
            ts_ms,
            caliber: Caliber::RequestDelta,
            input_uncached: i as u64,
            cache_read: cr as u64,
            cache_write: cw as u64,
            cache_write_1h: 0,
            request_count: n_req.clamp(1, u32::MAX as i64) as u32,
            output: o as u64,
            reasoning_output: 0,
            cost_usd: None,
            dedup_key: format!("ccsw:{}:{}:{}", native, date, model),
        });
    }
    stats.files_parsed = 1;
    stats.notes.push(format!(
        "cc-switch 回填 {} 行(原生覆盖整日跳过 {} 个日·app×model 行)",
        records.len(),
        skipped_covered
    ));
    if skipped_unknown_app > 0 {
        stats.notes.push(format!(
            "cc-switch 未映射 app_type 行 {skipped_unknown_app}(不回填)"
        ));
    }
    if skipped_empty > 0 {
        stats.notes.push(format!(
            "cc-switch 全零/无法解析日期行 {skipped_empty}(跳过)"
        ));
    }
    if normalized_legacy_rows > 0 {
        stats.notes.push(format!(
            "cc-switch 老口径 rollup 行 {normalized_legacy_rows}(semantics 0/1,input 含缓存)已按 fresh 规则减去缓存"
        ));
    }
    (records, stats)
}
