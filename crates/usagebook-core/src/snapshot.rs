//! 快照聚合:来源扫描 → 本地看板、缓存与 API 共用的统一快照。
//! 口径纪律与 report.rs 一致:曲线/桶只对 request_delta 求和;
//! session_total(claude cost-state)仅作官方成本基线,每会话取末值,不并入桶。
//!
//! ⚠️ 形状冻结:hours 的 (h, s, m, i, cr, cw, o, r, n, c) 是本地 API 与缓存
//! 共用的序列化形状,字段增删需兼容已有消费方,不可轻动。

use crate::model::{Caliber, ScanStats, SourceTier, UsageRecord};
use crate::{common, registered_sources, SOURCE_CATALOG};
use chrono::TimeZone;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// 源元数据(显示名 + 实证等级),随快照下发,前端不再硬编码源名。
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SourceInfo {
    pub id: &'static str,
    pub label: &'static str,
    pub tier: SourceTier,
}

/// 五桶紧凑序列化(字段名短,减小本地 /api/data 响应与缓存体积)。
#[derive(Serialize, Default, Clone, Deserialize)]
pub struct Buckets {
    pub i: u64,  // input_uncached
    pub cr: u64, // cache_read
    pub cw: u64, // cache_write
    pub o: u64,  // output(不含推理)
    pub r: u64,  // reasoning(输出子集)
    pub n: u64,  // 记录数 ≈ 请求数
    pub c: f64,  // 逐请求成本美元合计(request_delta 自带成本;无成本源为 0)
}

impl Buckets {
    fn add(&mut self, r: &UsageRecord) {
        self.i += r.input_uncached;
        self.cr += r.cache_read;
        self.cw += r.cache_write;
        self.o += r.output;
        self.r += r.reasoning_output;
        // 常规记录恒 1;ccswitch-rollup 日聚合行携带该日请求数(max 防 0 值漏计)
        self.n += u64::from(r.request_count).max(1);
        if let Some(c) = r.cost_usd {
            self.c += c;
        }
    }
}

#[derive(Serialize, Clone, Deserialize)]
pub struct HourRow {
    pub h: String, // 本地时区小时键 YYYY-MM-DDTHH
    pub s: String,
    pub m: String,
    pub b: Buckets,
}

#[derive(Serialize, Clone, Deserialize)]
pub struct SourceAgg {
    pub s: String,
    pub b: Buckets,
    pub sessions: u64,
    pub files: u64,
    /// 逐请求美元成本(= b.c,单独列出仅为兼容旧前端字段名)。0 = 该源无成本数据
    pub cost_usd: f64,
}

#[derive(Serialize, Clone, Deserialize)]
pub struct ModelAgg {
    pub m: String,
    pub sources: Vec<String>,
    pub b: Buckets,
}

#[derive(Serialize, Clone, Deserialize)]
pub struct Totals {
    pub b: Buckets,
    pub sessions: u64,
    /// claude cost-state 官方成本(每会话取末值求和;仅近 30 天转录窗口)
    pub cost_official_usd: f64,
    pub cost_sessions: u64,
    /// 逐请求美元成本合计(request_delta 口径,= b.c)
    pub cost_req_usd: f64,
}

#[derive(Serialize, Clone, Deserialize)]
pub struct PriorDayRow {
    pub s: String,
    pub m: String,
    pub b: Buckets,
}

#[derive(Serialize, Clone, Deserialize)]
pub struct SourceScan {
    pub source: String,
    pub records: usize,
    pub stats: ScanStats,
}

#[derive(Serialize, Clone)]
pub struct Snapshot {
    pub ready: bool,
    pub generated_at_ms: i64,
    /// Local calendar date of the fixed scan cutoff, for stale/midnight UI handling.
    pub local_date: String,
    pub prior_day_to_now: Option<Vec<PriorDayRow>>,
    pub source_scans: Vec<SourceScan>,
    pub scan_ms: u64,
    pub refresh_secs: u64,
    pub totals: Totals,
    /// 小时粒度聚合行,前端自行聚合为日/小时曲线
    pub hours: Vec<HourRow>,
    pub per_source: Vec<SourceAgg>,
    pub per_model: Vec<ModelAgg>,
    pub notes: Vec<String>,
    /// 源元数据目录(显示名/实证等级)
    pub sources: &'static [SourceInfo],
}

/// 全量扫描并构建快照。refresh_secs 仅随快照透出,不在此控制节奏。
/// verbose 控制逐源扫描进度输出(仅 serve 冷启动首轮传 true,后续刷新轮静音)。
pub fn build_snapshot(refresh_secs: u64, verbose: bool) -> Snapshot {
    let t0 = std::time::Instant::now();
    let generated_at_ms = chrono::Local::now().timestamp_millis();
    let mut records: Vec<UsageRecord> = Vec::new();
    let mut source_scans = Vec::new();

    // :25 源相互独立,并行扫描(串行 12s → 瓶颈=最慢单源约 5s);
    // collect 保序,聚合仍按注册序进行,notes 顺序确定
    let sources = registered_sources();
    let total = sources.len();
    let results: Vec<(String, Vec<UsageRecord>, crate::model::ScanStats)> = sources
        .par_iter()
        .enumerate()
        .map(|(i, a)| {
            if verbose {
                eprintln!("扫描 [{}/{}] {} …", i + 1, total, a.id());
            }
            let (recs, stats) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = common::take_walk_error();
                let roots = a.discover_roots();
                common::scan_source_cached(a.as_ref(), &roots)
            }))
            .unwrap_or_else(|_| {
                (
                    vec![],
                    ScanStats {
                        failed: true,
                        notes: vec!["此数据源扫描异常,本轮未计入;其他数据源继续统计".into()],
                        ..Default::default()
                    },
                )
            });
            (a.id().to_string(), recs, stats)
        })
        .collect();
    for (source, recs, stats) in results {
        source_scans.push(SourceScan {
            source,
            records: recs.len(),
            stats,
        });
        records.extend(recs);
    }
    // cc-switch rollups 空窗回填(跨源去重与重叠源规范 §五 C 类/§七):
    // 覆盖集来自本轮原生记录,回填行按 日期×app 整日替换,只补原生无覆盖的日期。
    // 单表直读(本机 261 行),不走 L1/L2 文件缓存。原生源清单与回填侧共用同一常量。
    let covered: std::collections::HashSet<(String, String)> = records
        .iter()
        .filter(|r| crate::sources::ccswitch_rollup::BACKFILL_NATIVE_SOURCES.contains(&r.source))
        .map(|r| (r.source.to_string(), common::ms_to_local_date(r.ts_ms)))
        .collect();
    let (backfilled, bf_stats) = crate::sources::ccswitch_rollup::backfill(&covered);
    source_scans.push(SourceScan {
        source: "ccswitch-rollup".to_string(),
        records: backfilled.len(),
        stats: bf_stats,
    });
    records.extend(backfilled);
    let mut snap = snapshot_from_records(&records, refresh_secs, generated_at_ms, source_scans);
    snap.scan_ms = t0.elapsed().as_millis() as u64;
    snap
}

/// Pure aggregation seam: a single fixed cutoff controls both today's hours and
/// yesterday's same-local-time comparison. Future rows cannot skew either side.
pub fn snapshot_from_records(
    records: &[UsageRecord],
    refresh_secs: u64,
    generated_at_ms: i64,
    source_scans: Vec<SourceScan>,
) -> Snapshot {
    let now = chrono::Local
        .timestamp_millis_opt(generated_at_ms)
        .single()
        .expect("snapshot cutoff is a valid epoch timestamp");
    let prior_bounds = now.date_naive().pred_opt().and_then(|day| {
        let start = chrono::Local
            .from_local_datetime(&day.and_hms_opt(0, 0, 0)?)
            .earliest()?;
        // end 保持 earliest()(2026-10-17 复核回退):chrono 的 Ambiguous 按偏移升序构造,
        // earliest=第二遍(较大 epoch)——含 DST 折叠的整个墙钟同期;latest 反而截掉第二遍。
        // 非 DST 时区两者恒等(实测),中国时区不受影响
        let end = chrono::Local
            .from_local_datetime(&day.and_time(now.time()))
            .earliest()?;
        Some((start.timestamp_millis(), end.timestamp_millis()))
    });
    let mut prior: BTreeMap<(String, String), Buckets> = BTreeMap::new();
    let src_files: BTreeMap<String, u64> = source_scans
        .iter()
        .map(|s| (s.source.clone(), s.stats.files_parsed as u64))
        .collect();
    let mut notes: Vec<String> = source_scans
        .iter()
        .filter(|s| !s.stats.notes.is_empty())
        .map(|s| format!("[{}] {}", s.source, s.stats.notes.join(";")))
        .collect();
    if prior_bounds.is_none() {
        notes.push("昨日本地同期时刻不可表示(时区切换),同期桶为空".into());
    }

    let mut hours: BTreeMap<(String, String, String), Buckets> = BTreeMap::new();
    let mut by_source: BTreeMap<String, Buckets> = BTreeMap::new();
    let mut src_sessions: BTreeMap<String, HashSet<String>> = BTreeMap::new();
    let mut by_model: BTreeMap<String, Buckets> = BTreeMap::new();
    // 模型显示名须跨扫描稳定(否则前端持久化的模型筛选会因拼写漂移而失配):
    // 按(小写键 → 原始拼写 → 出现次数)统计,取次数最多、并列取字典序最小
    let mut model_spell: HashMap<String, HashMap<String, u64>> = HashMap::new();
    let mut model_sources: HashMap<String, BTreeSet<String>> = HashMap::new();
    let mut sessions: HashSet<(String, String)> = HashSet::new();
    let mut totals = Buckets::default();
    // 官方成本:每会话取最大值(累计口径,末值最大;modelUsage 多行共享同一会话成本)
    let mut cost_by_session: HashMap<String, f64> = HashMap::new();

    for r in records {
        if r.ts_ms > generated_at_ms {
            continue;
        }
        if r.caliber == Caliber::RequestDelta {
            let hour = common::ms_to_local_hour(r.ts_ms);
            let mkey = r.model.to_lowercase();
            *model_spell
                .entry(mkey.clone())
                .or_default()
                .entry(r.model.clone())
                .or_insert(0) += 1;
            model_sources
                .entry(mkey.clone())
                .or_default()
                .insert(r.source.to_string());
            hours
                .entry((hour, r.source.to_string(), mkey.clone()))
                .or_default()
                .add(r);
            by_source.entry(r.source.to_string()).or_default().add(r);
            if prior_bounds.is_some_and(|(start, end)| r.ts_ms >= start && r.ts_ms <= end) {
                prior
                    .entry((r.source.to_string(), mkey.clone()))
                    .or_default()
                    .add(r);
            }
            by_model.entry(mkey).or_default().add(r);
            src_sessions
                .entry(r.source.to_string())
                .or_default()
                .insert(r.session_id.clone());
            sessions.insert((r.source.to_string(), r.session_id.clone()));
            totals.add(r);
        } else if let Some(c) = r.cost_usd {
            // claude cost-state 会话级官方成本:不进桶,单独通道(协议 §3)
            let e = cost_by_session.entry(r.session_id.clone()).or_insert(0.0);
            if c > *e {
                *e = c;
            }
        }
    }

    let model_disp: HashMap<String, String> = model_spell
        .iter()
        .map(|(k, spells)| {
            let best = spells
                .iter()
                .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0)))
                .map(|(s, _)| s.clone())
                .unwrap_or_else(|| k.clone());
            (k.clone(), best)
        })
        .collect();
    let hours: Vec<HourRow> = hours
        .into_iter()
        .map(|((h, s, m), b)| HourRow {
            h,
            s,
            m: model_disp.get(&m).cloned().unwrap_or(m),
            b,
        })
        .collect();
    let cost_sessions = cost_by_session.len() as u64;
    let cost_official_usd: f64 = cost_by_session.values().sum();

    Snapshot {
        ready: true,
        generated_at_ms,
        local_date: now.format("%Y-%m-%d").to_string(),
        prior_day_to_now: prior_bounds.map(|_| {
            prior
                .into_iter()
                .map(|((s, m), b)| PriorDayRow {
                    s,
                    m: model_disp.get(&m).cloned().unwrap_or(m),
                    b,
                })
                .collect()
        }),
        source_scans,
        scan_ms: 0,
        refresh_secs,
        totals: Totals {
            cost_official_usd,
            cost_sessions,
            cost_req_usd: totals.c,
            sessions: sessions.len() as u64,
            b: totals,
        },
        hours,
        per_source: by_source
            .into_iter()
            .map(|(s, b)| SourceAgg {
                cost_usd: b.c,
                sessions: src_sessions.get(&s).map_or(0, |x| x.len()) as u64,
                files: src_files.get(&s).copied().unwrap_or(0),
                s,
                b,
            })
            .collect(),
        per_model: by_model
            .into_iter()
            .map(|(m, b)| ModelAgg {
                m: model_disp.get(&m).cloned().unwrap_or_else(|| m.clone()),
                sources: model_sources
                    .remove(&m)
                    .map_or(vec![], |x| x.into_iter().collect()),
                b,
            })
            .collect(),
        notes,
        sources: SOURCE_CATALOG,
    }
}

/// 快照磁盘缓存载荷:与 Snapshot 同字段(除 sources——编译期内置目录,不入盘)。
/// 目的:服务重启热启动(先加载上轮快照秒开,后台重扫更新),消除冷启动 10~30s 等待。
#[derive(Clone, Deserialize)]
pub struct SnapshotDump {
    pub ready: bool,
    pub generated_at_ms: i64,
    #[serde(default)]
    pub local_date: String,
    #[serde(default)]
    pub prior_day_to_now: Option<Vec<PriorDayRow>>,
    #[serde(default)]
    pub source_scans: Vec<SourceScan>,
    pub scan_ms: u64,
    pub refresh_secs: u64,
    pub totals: Totals,
    pub hours: Vec<HourRow>,
    pub per_source: Vec<SourceAgg>,
    pub per_model: Vec<ModelAgg>,
    pub notes: Vec<String>,
}

impl From<SnapshotDump> for Snapshot {
    fn from(d: SnapshotDump) -> Self {
        Snapshot {
            ready: d.ready,
            generated_at_ms: d.generated_at_ms,
            local_date: d.local_date,
            prior_day_to_now: d.prior_day_to_now,
            source_scans: d.source_scans,
            scan_ms: d.scan_ms,
            refresh_secs: d.refresh_secs,
            totals: d.totals,
            hours: d.hours,
            per_source: d.per_source,
            per_model: d.per_model,
            notes: d.notes,
            sources: crate::SOURCE_CATALOG,
        }
    }
}
