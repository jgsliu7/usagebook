//! Bounded loopback query server. Only live adapters retain a process-local history snapshot.
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    sync::{mpsc, Arc, Mutex},
    time::{Duration, Instant},
};
#[path = "ledger_transport.rs"]
mod transport;
use tiny_http::{Header, Method, Response};
use transport::{Connection, Request};
use usagebook_core::ledger::{
    db,
    legacy::{LegacyEntry, LegacySnapshot, LegacySourceStatus},
    query::{ModelCatalogCache, QueryService},
    query_types::{CatalogRequest, QueryRequest, MAX_BODY},
    types::{new_epoch, now_ms},
    Ledger,
};
struct State {
    snapshot: Arc<LegacySnapshot>,
    snapshot_fingerprint: [u8; 32],
    refreshing: bool,
    pending_force: bool,
    error: Option<String>,
    refresh_secs: u64,
    collection_mode: CollectionMode,
    last_request_ms: i64,
    last_scan_started_ms: i64,
    last_scan_completed_ms: Option<i64>,
    refresh_generation: u64,
    durable_backlog: bool,
    durable_errors: Value,
    time_repair_error: Option<String>,
    writer_stopped: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum CollectionMode {
    Continuous,
    OnDemand,
}
impl CollectionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Continuous => "continuous",
            Self::OnDemand => "on-demand",
        }
    }
}
impl State {
    // A queued manual request is already busy. Requests waiting before the next
    // forced scan starts coalesce; requests arriving during it require one more.
    // All transitions use the same State mutex as status publication.
    fn request_refresh(&mut self) {
        if self.writer_stopped {
            return;
        }
        self.pending_force = true;
        self.refreshing = true;
    }
    fn stop_writer(&mut self, code: &str) {
        self.writer_stopped = true;
        self.pending_force = false;
        self.refreshing = false;
        self.durable_backlog = false;
        self.durable_errors["ledger"] = serde_json::json!(code);
        self.error = Some(code.to_owned());
        self.refresh_generation = self.refresh_generation.saturating_add(1);
    }
    fn begin_scan(&mut self, now: i64, first: bool, due: bool) -> Option<bool> {
        if self.writer_stopped {
            return None;
        }
        let forced = self.pending_force;
        if !(first || forced || (due && self.periodic_allowed())) {
            return None;
        }
        self.pending_force = false;
        self.refreshing = true;
        self.last_scan_started_ms = now;
        Some(forced)
    }
    fn complete_scan(&mut self, error: Option<String>) {
        if error.is_none() {
            self.last_scan_completed_ms = Some(now_ms());
        }
        self.error = error;
        self.refresh_generation = self.refresh_generation.saturating_add(1);
        self.refreshing = self.pending_force;
    }
    fn publish_snapshot(&mut self, mut snapshot: LegacySnapshot, fingerprint: [u8; 32]) {
        snapshot.generation = if fingerprint == self.snapshot_fingerprint {
            self.snapshot.generation.clone()
        } else {
            new_epoch()
        };
        self.snapshot = Arc::new(snapshot);
        self.snapshot_fingerprint = fingerprint;
    }
    fn periodic_allowed(&self) -> bool {
        self.collection_mode == CollectionMode::Continuous
            || self.last_scan_completed_ms.is_none()
            || self.last_request_ms >= self.last_scan_started_ms
    }
    fn collection_state(&self) -> &'static str {
        if self.writer_stopped {
            "idle"
        } else if self.refreshing || self.pending_force {
            "scanning"
        } else if self.periodic_allowed() {
            "waiting"
        } else {
            "idle"
        }
    }
    fn effective_refresh_ms(&self, _now: i64) -> u64 {
        self.refresh_secs * 1000
    }
    // Old clients may still send arf. It records a view but never changes the
    // configured interval or queues a manual scan, including arf=0.
    fn viewed(&mut self, _url: &str) {
        self.last_request_ms = now_ms();
    }
    fn snapshot_after_history_slice(
        &self,
        poll_succeeded: bool,
        scan: Option<bool>,
    ) -> Option<Arc<LegacySnapshot>> {
        // A live scan publishes its Arc only when the entire attempt has
        // finished. Reuse that fixed capture for individually ready sources;
        // another source's backlog must not prevent their continuation.
        (poll_succeeded
            && scan.is_none()
            && self.last_scan_completed_ms.is_some()
            && self.error.is_none())
        .then(|| self.snapshot.clone())
    }
}
fn begins_history_cycle(scan: Option<bool>, backlog: bool) -> bool {
    // Periodic live reads can continue during a long history drain. Only a
    // force request or a newly idle periodic cycle starts another full audit.
    scan.is_some() && (!backlog || scan == Some(true))
}
/// serve 失败源合并(2026-10-28 合流:判据与回滚为远端演进版,自扫描闭包提取保持可测)。
/// 输入容器缺失先标失败;stale/error 源回滚为上一轮完整读,上一轮无完整时间戳则
/// 不回滚(失败不得伪装成成功空读);诊断计数与 warning 随上一轮保留。
/// live 路由源清单(MIN==MAX=live)。守卫与巡检共用。
fn live_route_sources(
    conn: &rusqlite::Connection,
) -> usagebook_core::ledger::query_types::QueryResult<std::collections::BTreeSet<String>> {
    use usagebook_core::ledger::query_types::QueryError;
    let mut sources = std::collections::BTreeSet::new();
    let mut st = conn
        .prepare(
            "SELECT report_source, MIN(route), MAX(route) FROM source_roots GROUP BY report_source",
        )
        .map_err(|e| QueryError::new("LEDGER_UNREADABLE", format!("route read failed: {e}")))?;
    let mut rows = st
        .query([])
        .map_err(|e| QueryError::new("LEDGER_UNREADABLE", format!("route read failed: {e}")))?;
    while let Some(r) = rows.next()? {
        let source: String = r.get(0)?;
        let lo: String = r.get(1)?;
        let hi: String = r.get(2)?;
        if lo == hi && lo == "live" {
            sources.insert(source);
        }
    }
    Ok(sources)
}

/// P1(缺陷档案 20261110-live路由源历史用量可缩水,方案 v2):live 路由源的
/// 历史只增不减守卫入口。纳入条件:route=live 且非 claude(已有更强的
/// protect_claude_capture 条目级保护,双守卫会互相干扰)且非 ccswitch-rollup
/// (诊断日行不进总量,load_ccswitch_at 独立入口)。重基触发:PARSER_VERSION
/// 变更(解析修正可合法下修,留 GUARD_REBASED 痕)、账本时区变更(日界口径
/// 变了,旧按日基线不可比)、显式豁免(ledger guard-reset,一次性核销)。
/// 守卫状态存 ledger_runtime_state('guard/state' 与 'guard/rebase/<source>')。
fn guard_history(
    snapshot: &mut LegacySnapshot,
    previous: &LegacySnapshot,
    conn: &rusqlite::Connection,
) -> usagebook_core::ledger::query_types::QueryResult<std::collections::BTreeMap<String, usize>> {
    use std::collections::{BTreeMap, BTreeSet};
    use usagebook_core::ledger::query_types::{QueryError, QueryResult};
    fn state_row(conn: &rusqlite::Connection, key: &str) -> QueryResult<Option<String>> {
        use rusqlite::OptionalExtension;
        conn.query_row(
            "SELECT value_json FROM ledger_runtime_state WHERE state_key=?1",
            [key],
            |r| r.get(0),
        )
        .optional()
        .map_err(|e| QueryError::new("LEDGER_UNREADABLE", format!("guard state read failed: {e}")))
    }
    let mut eligible = BTreeSet::new();
    for source in live_route_sources(conn)? {
        if source != "claude" && source != "ccswitch-rollup" {
            eligible.insert(source);
        }
    }
    if eligible.is_empty() {
        return Ok(BTreeMap::new());
    }
    let stored = state_row(conn, "guard/state")?;
    let stored_json: Option<serde_json::Value> =
        stored.and_then(|value| serde_json::from_str(&value).ok());
    let generation = usagebook_core::store::PARSER_VERSION.to_string();
    let timezone: Option<String> = conn
        .query_row("SELECT report_timezone FROM ledger_meta", [], |r| r.get(0))
        .ok();
    let generation_changed = stored_json
        .as_ref()
        .and_then(|value| value.get("generation"))
        .and_then(|value| value.as_str())
        .is_some_and(|stored| stored != generation);
    let timezone_changed = stored_json
        .as_ref()
        .and_then(|value| value.get("timezone"))
        .and_then(|value| value.as_str())
        .zip(timezone.as_deref())
        .is_some_and(|(stored, current)| stored != current);
    let mut rebase = BTreeSet::new();
    if generation_changed || timezone_changed {
        rebase = eligible.clone();
    }
    for source in &eligible {
        let key = format!("guard/rebase/{source}");
        if state_row(conn, &key)?.is_some() {
            // 评审E-B P1①:豁免标记只在"本轮有新鲜成功读"的源上核销——失败/
            // 回滚轮不消耗标记,保证重基绑定的是首次成功发布。
            let fresh_ok = snapshot.sources.get(source.as_str()).is_some_and(|status| {
                status.last_scanned_ms.is_some() && !status.stale && status.error.is_none()
            });
            if fresh_ok {
                conn.execute("DELETE FROM ledger_runtime_state WHERE state_key=?1", [key])?;
                rebase.insert(source.clone());
            }
        }
    }
    let mut guarded = match snapshot.monotonic_history_guard(previous, conn, &eligible, &rebase) {
        Ok(guarded) => guarded,
        Err(error) => {
            // 评审D-A P1②:守卫执行失败按失败闭锁——eligible 源本轮回滚为
            // 上一轮已发布条目(与 merge_failed_sources 同款保留),绝不把
            // 未经守卫的变少读发布出去;增长下轮照常生效。回滚纯内存,不再失败。
            tracing::warn!("历史只增不减守卫执行失败,本轮 eligible 源按旧值保留:{error}");
            for source in &eligible {
                snapshot
                    .entries
                    .retain(|entry| entry.report_source() != source.as_str());
                snapshot.entries.extend(
                    previous
                        .entries
                        .iter()
                        .filter(|entry| entry.report_source() == source.as_str())
                        .cloned(),
                );
                if let Some(status) = snapshot.sources.get_mut(source.as_str()) {
                    status.warning = Some("GUARD_UNAVAILABLE_RETAINED".into());
                }
            }
            BTreeMap::new()
        }
    };
    // 告警转移过滤:变小的读每轮都会被检测与保留(数据防线是硬性的),但只在
    // warning 文本相对上一轮发布发生变化时才计入返回值——持续的已守卫状态
    // 不重复告警(评审C:防每轮重报的告警疲劳)。
    guarded.retain(|source, _| {
        let before = previous
            .sources
            .get(source)
            .and_then(|status| status.warning.clone());
        let after = snapshot
            .sources
            .get(source)
            .and_then(|status| status.warning.clone());
        before != after
    });
    let state = serde_json::json!({"generation": generation, "timezone": timezone});
    conn.execute(
        "INSERT INTO ledger_runtime_state(state_key,value_json) VALUES('guard/state',?1)
         ON CONFLICT(state_key) DO UPDATE SET value_json=excluded.value_json",
        [state.to_string()],
    )?;
    Ok(guarded)
}

/// P3(缺陷档案 20261110,方案 v2):按日单调巡检——只读告警,不做任何数据决策。
/// 对 live 路由源(含 claude:它有条目级保护层但缺日级告警)按(源×本地日)
/// 记录请求条数与六桶合计摘要于 ledger_runtime_state('guard/digest/<source>'),
/// 每轮与上轮比对,发现下降即 tracing::warn 并记入 'guard/day-alerts'(留最近
/// 100 条)。这是 P1 条目级守卫之外的纵深防御,也覆盖 P1 落地前的历史。
/// 必须在发布点(protect_claude_capture 之后、快照重载为已提交版本时)调用:
/// 2026-11-11 实测教训——若在扫描闭包(保护前)采样,claude 原始重扫的变少
/// 会被保护层复原,巡检对"已发布数据"产生假告警(原始信号真实但口径错位)。
fn day_digest_patrol(
    snapshot: &LegacySnapshot,
    conn: &rusqlite::Connection,
    timezone: &str,
    sources: &std::collections::BTreeSet<String>,
) {
    let patrol = || -> usagebook_core::ledger::query_types::QueryResult<()> {
        use usagebook_core::ledger::query_types::QueryError;
        let zone = usagebook_core::ledger::metrics::timezone(timezone)
            .map_err(|e| QueryError::new("LEDGER_UNREADABLE", format!("patrol tz: {e}")))?;
        let mut digests: BTreeMap<String, BTreeMap<String, (i64, i64)>> = BTreeMap::new();
        for entry in &snapshot.entries {
            let source = entry.report_source();
            if !sources.contains(source) {
                continue;
            }
            if let LegacyEntry::Request { record, .. } = entry {
                let day = chrono::DateTime::from_timestamp_millis(record.ts_ms)
                    .map(|at| at.with_timezone(&zone).date_naive().to_string())
                    .unwrap_or_else(|| "undated".into());
                let tokens = (record.input_uncached
                    + record.cache_read
                    + record.cache_write
                    + record.output
                    + record.reasoning_output) as i64;
                let day_map = digests.entry(source.to_owned()).or_default();
                let cell = day_map.entry(day).or_insert((0i64, 0i64));
                cell.0 += tokens;
                cell.1 += 1;
            }
        }
        let mut alerts: Vec<serde_json::Value> = conn
            .query_row(
                "SELECT value_json FROM ledger_runtime_state WHERE state_key='guard/day-alerts'",
                [],
                |r| r.get::<_, String>(0),
            )
            .ok()
            .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok())
            .and_then(|value| value.get("alerts").cloned())
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default();
        // 评审E-B P1②:保留最近 100 条(此前 truncate(len-100) 在 ≤100 时会清空)。
        if alerts.len() > 100 {
            alerts.drain(..alerts.len() - 100);
        }
        for (source, days) in &digests {
            let key = format!("guard/digest/{source}");
            let stored: Option<String> = conn
                .query_row(
                    "SELECT value_json FROM ledger_runtime_state WHERE state_key=?1",
                    [&key],
                    |r| r.get(0),
                )
                .ok();
            let previous_days: serde_json::Map<String, serde_json::Value> = stored
                .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok())
                .and_then(|value| value.as_object().cloned())
                .unwrap_or_default();
            for (day, (tokens, requests)) in days {
                if let Some(previous) = previous_days.get(day) {
                    let old_tokens = previous.get("tokens").and_then(|v| v.as_i64());
                    let old_requests = previous.get("requests").and_then(|v| v.as_i64());
                    if old_tokens.is_some_and(|old| *tokens < old)
                        || old_requests.is_some_and(|old| *requests < old)
                    {
                        tracing::warn!(
                            "按日单调巡检:{source} {day} 摘要下降 tokens {old_tokens:?}->{tokens} requests {old_requests:?}->{requests}"
                        );
                        alerts.push(serde_json::json!({
                            "source":source,"day":day,"tokens":{"from":old_tokens,"to":tokens},
                            "requests":{"from":old_requests,"to":requests},
                            "at_ms":usagebook_core::ledger::types::now_ms(),
                        }));
                    }
                }
            }
            conn.execute(
                "INSERT INTO ledger_runtime_state(state_key,value_json) VALUES(?1,?2)
                 ON CONFLICT(state_key) DO UPDATE SET value_json=excluded.value_json",
                rusqlite::params![
                    key,
                    serde_json::to_string(
                        &days
                            .iter()
                            .map(|(day, (tokens, requests))| {
                                (
                                    day.clone(),
                                    serde_json::json!({"tokens":tokens,"requests":requests}),
                                )
                            })
                            .collect::<serde_json::Map<String, serde_json::Value>>()
                    )
                    .map_err(|e| QueryError::new(
                        "LEDGER_UNREADABLE",
                        format!("patrol encode: {e}")
                    ))?
                ],
            )?;
        }
        let payload = serde_json::json!({"alerts":alerts});
        conn.execute(
            "INSERT INTO ledger_runtime_state(state_key,value_json) VALUES('guard/day-alerts',?1)
             ON CONFLICT(state_key) DO UPDATE SET value_json=excluded.value_json",
            rusqlite::params![payload.to_string()],
        )?;
        Ok(())
    };
    if let Err(error) = patrol() {
        tracing::warn!("按日单调巡检本轮跳过:{error}");
    }
}

/// `warning`(按源降级:该源有单条坏行被跳过)不在失败判据中:坏行降级的源保留
/// 本轮新鲜好数据,不回滚(见 docs/features/20261023-live快照管线加固与查询提速)。
fn merge_failed_sources(snapshot: &mut LegacySnapshot, previous_for_merge: &LegacySnapshot) {
    for (source, status) in &mut snapshot.sources {
        if previous_for_merge.has_missing_input_containers(source) {
            status.stale = true;
            if status.error.as_deref() != Some("LOADING") {
                status.error = Some("SOURCE_READ_FAILED".into());
            }
        }
    }
    let failed: Vec<String> = snapshot
        .sources
        .iter()
        // A successful empty read replaces old rows. Only
        // an explicit source failure may retain prior data.
        .filter(|(_, status)| status.stale || status.error.is_some())
        .map(|(source, _)| source.clone())
        .collect();
    for source in failed {
        // Only a complete read supplies a successful timestamp.
        // LOADING and first-failure placeholders are not retained
        // data; a successful empty read is still a complete result.
        let previous_complete = previous_for_merge
            .sources
            .get(&source)
            .filter(|status| status.last_scanned_ms.is_some());
        snapshot.entries.retain(|row| row.report_source() != source);
        if previous_complete.is_some() {
            snapshot.retain_input_containers(&source, previous_for_merge);
            snapshot.entries.extend(
                previous_for_merge
                    .entries
                    .iter()
                    .filter(|row| row.report_source() == source)
                    .cloned(),
            );
        }
        if let Some(status) = snapshot.sources.get_mut(&source) {
            // These rows still belong to the previous successful read.
            // A failed attempt must not make retained data look current.
            status.last_scanned_ms =
                previous_complete.and_then(|previous| previous.last_scanned_ms);
            if let Some(previous) = previous_complete {
                status.unaccounted_rows = previous.unaccounted_rows;
                status.unallocated_sessions = previous.unallocated_sessions;
                status.warning = previous.warning.clone();
            }
            status.stale = true;
            if status.error.as_deref() != Some("LOADING") {
                status.error = Some("SOURCE_READ_FAILED".into());
            }
        }
    }
}

struct Assets {
    index_html: String,
    dashboard_js: String,
}
impl Assets {
    fn new() -> Self {
        const I18N_JS: &str = concat!(
            "'use strict';const I18N={};",
            include_str!("i18n/zh-CN.js"),
            include_str!("i18n/en-US.js")
        );
        // Keep the original source file and CSP: serve its script as a
        // same-origin resource rather than allowing inline JavaScript.
        let (prefix, tail) = include_str!("dashboard.html")
            .split_once("<script>")
            .expect("dashboard script");
        let (script, suffix) = tail.split_once("</script>").expect("dashboard script end");
        Self {
            index_html: format!("{prefix}<script src=\"/dashboard.js\"></script>{suffix}"),
            dashboard_js: format!("{I18N_JS}{script}"),
        }
    }
}
struct ServerContext<'a> {
    shared: &'a Arc<Mutex<State>>,
    catalog_cache: &'a ModelCatalogCache,
    port: u16,
    durable_at_start: bool,
    ledger_path: &'a std::path::Path,
    sync: Option<&'a crate::sync::HostHandle>,
    sync_only: bool,
}
fn response(req: Request, body: String, ctype: &str, code: u16) {
    let resp=Response::from_string(body).with_status_code(code).with_header(Header::from_bytes("Content-Type",ctype).unwrap()).with_header(Header::from_bytes("Cache-Control","no-store").unwrap()).with_header(Header::from_bytes("X-Content-Type-Options","nosniff").unwrap()).with_header(Header::from_bytes("Content-Security-Policy","default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'").unwrap());
    if let Err(error) = req.respond(resp) {
        tracing::warn!(kind = ?error.kind(), "HTTP response transmission failed");
    }
}
fn error(req: Request, code: u16, message: &str) {
    response(
        req,
        json!({"error":{"code":message,"message":message,"retryable":code==429||code==503}})
            .to_string(),
        "application/json; charset=utf-8",
        code,
    )
}
pub fn run(
    port: u16,
    refresh_secs: u64,
    collection_mode: CollectionMode,
    open: bool,
    temporary: bool,
) {
    run_inner(port, refresh_secs, collection_mode, open, temporary, false)
}
pub fn run_sync_host() {
    run_inner(0, 60, CollectionMode::Continuous, false, false, true)
}
fn run_inner(
    port: u16,
    refresh_secs: u64,
    collection_mode: CollectionMode,
    open: bool,
    temporary: bool,
    sync_only: bool,
) {
    if (port == 0 && !sync_only) || !(5..=86400).contains(&refresh_secs) {
        crate::ledger_cli::fail(
            "port must be positive; refresh interval must be 5..=86400 seconds",
            2,
        )
    }
    let server =
        TcpListener::bind(("127.0.0.1", port)).unwrap_or_else(|e| crate::ledger_cli::fail(e, 3));
    let port = server.local_addr().unwrap().port();
    usagebook_core::store::prune_bad_backups();
    let _ = usagebook_core::logging::global().activate("serve");
    let path = db::default_path();
    let mut writer = if temporary {
        None
    } else if db::exists_or_recoverable(&path) {
        Some(
            Ledger::open(&path, true)
                .unwrap_or_else(|e| crate::ledger_cli::fail(&e, e.exit_code())),
        )
    } else {
        let timezone = db::host_timezone_hint().unwrap_or_else(|| {
            crate::ledger_cli::fail(
                "无法确定系统时区；请先用 ledger init --timezone <IANA 名> 初始化账本。",
                2,
            )
        });
        Some(
            Ledger::init(&path, &timezone)
                .unwrap_or_else(|e| crate::ledger_cli::fail(&e, e.exit_code())),
        )
    };
    let (sync_runtime, sync_handle) = if let Some(writer) = writer.as_mut() {
        match crate::sync::Runtime::new(writer, port) {
            Ok((runtime, handle)) => (Some(runtime), Some(handle)),
            Err(code) => {
                tracing::warn!(code=%code,"cloud sync host unavailable");
                (None, None)
            }
        }
    } else {
        (None, None)
    };
    // Freeze this once for both the writer and query workers. An init during
    // startup must not connect readers to a different database from the writer.
    let durable_at_start = writer.is_some();
    let transient = if writer.is_none() {
        let tz = db::host_timezone_hint().unwrap_or_else(|| {
            eprintln!(
                "LEDGER_TIMEZONE_HINT_FAILED：未能确定系统时区，未初始化看板临时按 UTC 折算日界。"
            );
            "UTC".to_string()
        });
        Some(
            db::live_connection(&tz).unwrap_or_else(|e| crate::ledger_cli::fail(&e, e.exit_code())),
        )
    } else {
        None
    };
    let conn = writer
        .as_ref()
        .map(Ledger::connection)
        .unwrap_or_else(|| transient.as_ref().unwrap());
    let stamp = db::stamp(conn).unwrap();
    let mut initial = LegacySnapshot::new(new_epoch(), QueryService::routing_hash(conn).unwrap());
    let restored = if durable_at_start {
        LegacySnapshot::restore_capture(conn).unwrap_or_else(|e| crate::ledger_cli::fail(e, 3))
    } else {
        None
    };
    let restored_completed_ms = restored
        .as_ref()
        .and_then(|s| s.sources.values().filter_map(|v| v.last_scanned_ms).max());
    if let Some(saved) = restored {
        initial = saved;
        initial.routing_hash = QueryService::routing_hash(conn).unwrap();
    }
    for source in usagebook_core::registered_sources() {
        initial
            .sources
            .entry(source.id().into())
            .or_insert(LegacySourceStatus {
                last_scanned_ms: None,
                stale: true,
                error: Some("LOADING".into()),
                unaccounted_rows: 0,
                unallocated_sessions: 0,
                warning: None,
            });
    }
    let shared = Arc::new(Mutex::new(State {
        snapshot_fingerprint: initial.content_fingerprint(),
        snapshot: Arc::new(initial),
        refreshing: true,
        pending_force: false,
        error: None,
        refresh_secs,
        collection_mode,
        last_request_ms: 0,
        last_scan_started_ms: 0,
        last_scan_completed_ms: restored_completed_ms,
        refresh_generation: 0,
        durable_backlog: durable_at_start,
        durable_errors: json!({}),
        time_repair_error: None,
        writer_stopped: false,
    }));
    {
        let shared = shared.clone();
        std::thread::spawn(move || {
            let mut writer = writer;
            let mut sync_runtime = sync_runtime;
            let mut history = usagebook_core::ledger::collection::Collector::default();
            let mut history_backlog = writer.is_some();
            let mut first = true;
            let mut last = Instant::now();
            let mut last_wall_ms = now_ms();
            let mut continuation_attempts = BTreeMap::<String, i64>::new();
            // This writer owns the maintenance lock for its lifetime: restore,
            // forget and epoch/control changes require restarting it. Routes
            // can change here, so they explicitly invalidate attempt memory.
            let mut continuation_route = String::new();
            let mut last_time_repair = None::<Instant>;
            let mut time_repair_interval = Duration::from_secs(1);
            loop {
                if let (Some(runtime), Some(ledger)) = (sync_runtime.as_mut(), writer.as_mut()) {
                    runtime.tick(ledger);
                }
                // Install/retry optional indexes only between writer batches.
                // Fatal cleanup errors stop and drop this connection entirely;
                // independent readers can still serve committed history.
                if let Some(ledger) = writer.as_mut() {
                    if let Err(error) = ledger.maintain_query_indexes_if_due() {
                        tracing::error!(
                            code = error.code(),
                            "查询索引维护失败，已停止后台写入；请重启服务后重试"
                        );
                        shared.lock().unwrap().stop_writer(error.code());
                        return;
                    }
                }
                // Historical time recovery consumes only committed ledger
                // evidence. It is independent of page views, source discovery,
                // and the source backlog, and cannot mark collection as busy.
                // Each batch persists its cursor and evidence dependency, so
                // unchanged failed candidates are not re-enumerated here.
                // Run the first batch before acquisition can replace the saved
                // compatibility capture from the previous application version.
                if last_time_repair.is_none_or(|last| last.elapsed() >= time_repair_interval) {
                    if let Some(ledger) = writer.as_mut() {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            ledger.repair_pending_time_allocations(32)
                        }));
                        let (error, has_more) = match result {
                            Ok(Ok(report)) => (None, report["has_more"].as_bool().unwrap_or(false)),
                            Ok(Err(error)) => (Some(error.code().to_owned()), false),
                            Err(_) => (Some("TIME_REPAIR_FAILED".to_owned()), false),
                        };
                        // Drain a known bounded backlog promptly. Once caught
                        // up or waiting for evidence, use a quiet retry cadence
                        // instead of joining the entire history every second.
                        time_repair_interval = Duration::from_secs(if has_more { 1 } else { 60 });
                        let mut state = shared.lock().unwrap();
                        if error.is_some() && error != state.time_repair_error {
                            tracing::warn!(code = ?error, "历史分时恢复暂未完成，已保存用量仍可查询");
                        }
                        state.time_repair_error = error;
                    }
                    last_time_repair = Some(Instant::now());
                }
                let now = now_ms();
                // Consume a manual request once for both acquisition and the
                // matching live scan. A request arriving during either phase
                // remains queued for the next complete cycle.
                let mut completed_sources = std::collections::BTreeSet::new();
                let mut scan = {
                    let mut state = shared.lock().unwrap();
                    let interval_ms = state.effective_refresh_ms(now);
                    let due = last.elapsed() >= Duration::from_millis(interval_ms)
                        || now.saturating_sub(last_wall_ms).max(0) as u64 >= interval_ms;
                    state.begin_scan(now, first, due)
                };
                if history_backlog || scan.is_some() {
                    if let Some(ledger) = writer.as_mut() {
                        if begins_history_cycle(scan, history_backlog) {
                            history.begin_audit_cycle();
                        }
                        if scan == Some(true) {
                            history.invalidate_discovery();
                        }
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            history.poll(ledger, None)
                        }));
                        let (backlog, mut errors, poll_succeeded) = match result {
                            Ok(Ok(summary)) => (
                                summary.backlog,
                                json!(summary.errors),
                                !summary.errors.contains_key("ledger"),
                            ),
                            Ok(Err(error)) => (false, json!({"ledger":error.code()}), false),
                            Err(_) => (false, json!({"ledger":"REFRESH_FAILED"}), false),
                        };
                        history_backlog = backlog;
                        if scan.is_none() && poll_succeeded {
                            let previous = shared.lock().unwrap().snapshot.clone();
                            for (source, status) in &previous.sources {
                                if status.error.as_deref() != Some("LOADING") {
                                    continue;
                                }
                                let ready = ledger.connection().query_row("SELECT EXISTS(SELECT 1 FROM source_projection_scopes p JOIN ledger_meta m ON p.epoch=m.epoch WHERE p.report_source=?1 AND p.complete=1 AND p.error_code IS NULL) AND NOT EXISTS(SELECT 1 FROM source_roots r LEFT JOIN source_projection_scopes p USING(root_id) WHERE r.report_source=?1 AND r.enabled=1 AND (r.collector_key='local-sqlite-history-v1' OR (r.collector_key='additional-native-v1' AND (p.root_id IS NOT NULL OR r.locator LIKE '%.db'))) AND (p.root_id IS NULL OR p.complete=0 OR p.epoch<>(SELECT epoch FROM ledger_meta) OR p.error_code IS NOT NULL OR r.error_code IS NOT NULL))", [source], |r|r.get::<_,bool>(0)).unwrap_or(false);
                                if ready
                                    && !usagebook_core::ledger::saved_database_pending(
                                        ledger.connection(),
                                        source,
                                    )
                                    .unwrap_or(true)
                                {
                                    completed_sources.insert(source.clone());
                                }
                            }
                            if !completed_sources.is_empty() {
                                scan = Some(false);
                                let mut state = shared.lock().unwrap();
                                state.refreshing = true;
                                state.last_scan_started_ms = now_ms();
                            }
                        }
                        let completed_snapshot = shared
                            .lock()
                            .unwrap()
                            .snapshot_after_history_slice(poll_succeeded, scan);
                        if let Some(snapshot) = completed_snapshot {
                            if continuation_route != snapshot.routing_hash {
                                continuation_attempts.clear();
                                continuation_route = snapshot.routing_hash.clone();
                            }
                            // Run outside the shared status lock. The ordinary
                            // continuation checks still reject gaps, changed
                            // identity/control state and incomplete live reads.
                            let resumed =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    for source in ledger.ready_legacy_continuations(
                                        &snapshot,
                                        &continuation_attempts,
                                    )? {
                                        // Mark before the attempt: an ordinary gate failure
                                        // retries on a new completed capture, not every slice.
                                        continuation_attempts.insert(
                                            source.clone(),
                                            snapshot.sources[&source].last_scanned_ms.unwrap(),
                                        );
                                        ledger
                                            .continue_selected_legacy(&snapshot, Some(&source))?;
                                    }
                                    Ok::<(), usagebook_core::ledger::LedgerError>(())
                                }));
                            match resumed {
                                Ok(Ok(_)) => {}
                                Ok(Err(error)) => errors["ledger"] = json!(error.code()),
                                Err(_) => errors["ledger"] = json!("REFRESH_FAILED"),
                            }
                        }
                        let mut state = shared.lock().unwrap();
                        state.durable_backlog = backlog;
                        state.durable_errors = errors;
                        // Route changes become visible together with a matching
                        // live snapshot token; the query layer selects one route.
                        if let Ok(hash) = QueryService::routing_hash(ledger.connection()) {
                            if state.snapshot.routing_hash != hash {
                                let mut snapshot = (*state.snapshot).clone();
                                snapshot.routing_hash = hash;
                                let fingerprint = snapshot.content_fingerprint();
                                state.publish_snapshot(snapshot, fingerprint);
                            }
                        }
                    }
                }
                if let Some(_forced) = scan {
                    first = false;
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                        || -> std::result::Result<LegacySnapshot, String> {
                            if std::env::var("USAGEBOOK_TEST_PANIC").as_deref() == Ok("1") {
                                panic!("injected test panic\nsecond line \u{1b}[31mUBCANARY-sk-testsecret123");
                            }
                            if let Some(ledger) = writer.as_mut() {
                                if let Err(e) = ledger.optimize_if_due() {
                                    tracing::warn!("查询统计维护暂未完成，将在空闲时重试: {e}");
                                }
                            }
                            let conn = writer
                                .as_ref()
                                .map(Ledger::connection)
                                .unwrap_or_else(|| transient.as_ref().unwrap());
                            let route_hash =
                                QueryService::routing_hash(conn).map_err(|e| e.to_string())?;
                            let previous_for_merge = shared.lock().unwrap().snapshot.clone();
                            {
                                let previous = &previous_for_merge;
                                if previous.routing_hash != route_hash {
                                    let mut snapshot = (**previous).clone();
                                    snapshot.routing_hash = route_hash;
                                    for status in snapshot.sources.values_mut() {
                                        status.stale = true;
                                        status.error = Some("LOADING".into());
                                    }
                                    let fingerprint = snapshot.content_fingerprint();
                                    shared
                                        .lock()
                                        .unwrap()
                                        .publish_snapshot(snapshot, fingerprint);
                                }
                            }
                            let mut snapshot = if !completed_sources.is_empty() {
                                crate::ledger_cli::load_completed_saved_sources(
                                    conn,
                                    &previous_for_merge,
                                    &completed_sources,
                                )
                            } else if durable_at_start {
                                crate::ledger_cli::load_legacy_saved(conn)
                            } else {
                                crate::ledger_cli::load_legacy(conn)
                            }
                            .map_err(|e| e.to_string())?;
                            // The interim LOADING snapshot may carry a new route hash while
                            // still containing old live rows. Merge only against the snapshot
                            // captured before that transition, and only for sources still live.
                            merge_failed_sources(&mut snapshot, &previous_for_merge);
                            // P1(缺陷档案 20261110):live 源历史只增不减守卫——
                            // 失败回滚之后、索引重建之前。仅 durable serve 生效
                            // (transient/CLI 冷启无发布语义,守卫静默);守卫自身
                            // 失败不阻断本轮发布(fail-open,只告警)。
                            if writer.is_some() {
                                match guard_history(&mut snapshot, &previous_for_merge, conn) {
                                    Ok(counts) => {
                                        for (source, count) in counts {
                                            tracing::warn!(
                                                "历史只增不减守卫:{source} 本轮 {count} 条变小的读已保留旧值"
                                            );
                                        }
                                    }
                                    Err(error) => {
                                        tracing::warn!(
                                            "历史只增不减守卫本轮跳过(状态读取失败):{error}"
                                        );
                                    }
                                }
                            }
                            // 合并可能改动 entries(retain+extend),索引必须在其后、
                            // 指纹/publish 之前重建(方案 M2 双构建点之二)。后续
                            // ledger 续跑对 snapshot 条目只读,不再变动 entries。
                            snapshot.build_index();
                            if let Some(ledger) = writer.as_mut() {
                                ledger
                                    .reconcile_and_activate_sources(&snapshot, None)
                                    .map_err(|e| e.to_string())?;
                                if continuation_route != snapshot.routing_hash {
                                    continuation_attempts.clear();
                                    continuation_route = snapshot.routing_hash.clone();
                                }
                                let ready = ledger
                                    .ready_legacy_continuations(&snapshot, &continuation_attempts)
                                    .map_err(|e| e.to_string())?;
                                for source in ready {
                                    continuation_attempts.insert(
                                        source.clone(),
                                        snapshot.sources[&source].last_scanned_ms.unwrap(),
                                    );
                                    ledger
                                        .continue_selected_legacy(&snapshot, Some(&source))
                                        .map_err(|e| e.to_string())?;
                                }
                                // A failed live read is an actual source error.
                                // Only complete captures waiting on audit are
                                // deferred silently to a background slice.
                                for source in ledger
                                    .failed_legacy_captures(&snapshot)
                                    .map_err(|e| e.to_string())?
                                {
                                    if snapshot
                                        .sources
                                        .get(&source)
                                        .and_then(|s| s.error.as_deref())
                                        == Some("LOADING")
                                    {
                                        continue;
                                    }
                                    ledger
                                        .continue_selected_legacy(&snapshot, Some(&source))
                                        .map_err(|e| e.to_string())?;
                                }
                                snapshot.routing_hash =
                                    QueryService::routing_hash(ledger.connection())
                                        .map_err(|e| e.to_string())?;
                            }
                            Ok(snapshot)
                        },
                    ));
                    let (snapshot, error) = match result {
                        Ok(Ok(snapshot)) => {
                            tracing::info!(
                                "本轮扫描 · live 来源 {} · 记录 {}",
                                snapshot.sources.len(),
                                snapshot.entries.len()
                            );
                            (snapshot, None)
                        }
                        failure => {
                            tracing::warn!("本轮扫描失败，保留上次结果");
                            let error = match failure {
                                Ok(Err(e)) => e,
                                _ => "REFRESH_FAILED".into(),
                            };
                            let previous = shared.lock().unwrap().snapshot.clone();
                            let mut stale = (*previous).clone();
                            for s in stale.sources.values_mut() {
                                s.stale = true;
                            }
                            (stale, Some(error))
                        }
                    };
                    // Hashing is proportional to live history; do it outside the
                    // shared status lock so readers can keep using the old snapshot.
                    let mut fingerprint = snapshot.content_fingerprint();
                    let mut snapshot = snapshot;
                    let mut error = error;
                    let capture_changed = {
                        let state = shared.lock().unwrap();
                        let changed = fingerprint != state.snapshot_fingerprint;
                        snapshot.generation = if !changed {
                            state.snapshot.generation.clone()
                        } else {
                            new_epoch()
                        };
                        changed
                    };
                    if let Some(ledger) = writer.as_mut() {
                        if let Err(e) = ledger.publish_compatibility_capture(&snapshot) {
                            error = Some(format!("CAPTURE_SAVE_FAILED:{}", e.code));
                        } else {
                            // Publication may preserve historical compatibility
                            // rows absent from the scan. Keep the in-memory
                            // snapshot and its fingerprint aligned with the
                            // exact version committed to the read model.
                            match LegacySnapshot::restore_capture(ledger.connection()) {
                                Ok(Some(saved)) => {
                                    snapshot = saved;
                                    fingerprint = snapshot.content_fingerprint();
                                    if capture_changed {
                                        // A new capture can resolve a pending bucket.
                                        last_time_repair = None;
                                    }
                                    // P3 按日单调巡检:在此(保护后、已提交版)采样——
                                    // 巡检的是"已发布数据",与看板口径一致。
                                    let patrol_conn = ledger.connection();
                                    let tz: Option<String> = patrol_conn
                                        .query_row(
                                            "SELECT report_timezone FROM ledger_meta",
                                            [],
                                            |r| r.get(0),
                                        )
                                        .ok();
                                    if let Some(tz) = tz.as_deref() {
                                        let mut digest_sources =
                                            live_route_sources(patrol_conn).unwrap_or_default();
                                        digest_sources.remove("ccswitch-rollup");
                                        day_digest_patrol(
                                            &snapshot,
                                            patrol_conn,
                                            tz,
                                            &digest_sources,
                                        );
                                    }
                                }
                                Ok(None) => error = Some("CAPTURE_READ_FAILED".into()),
                                Err(e) => error = Some(format!("CAPTURE_READ_FAILED:{}", e.code)),
                            }
                        }
                    }
                    let mut state = shared.lock().unwrap();
                    if error.is_none() {
                        state.snapshot = Arc::new(snapshot);
                        state.snapshot_fingerprint = fingerprint;
                    }
                    state.complete_scan(error);
                    last = Instant::now();
                    last_wall_ms = now_ms();
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        });
    }
    let (tx, rx) = mpsc::sync_channel::<Request>(8);
    let rx = Arc::new(Mutex::new(rx));
    let assets = Arc::new(Assets::new());
    let catalog_cache = Arc::new(ModelCatalogCache::default());
    for _ in 0..2 {
        let rx = rx.clone();
        let shared = shared.clone();
        let path = path.clone();
        let stamp = stamp.clone();
        let catalog_cache = catalog_cache.clone();
        let sync_handle = sync_handle.clone();
        std::thread::spawn(move || {
            let ledger = if durable_at_start {
                Some(
                    Ledger::open(&path, false)
                        .unwrap_or_else(|e| crate::ledger_cli::fail(&e, e.exit_code())),
                )
            } else {
                None
            };
            let memory = if ledger.is_none() {
                let c = db::live_connection(&stamp.report_timezone).unwrap();
                c.execute(
                    "UPDATE ledger_meta SET epoch=?1,ledger_uuid=?2",
                    [&stamp.epoch, &stamp.ledger_uuid],
                )
                .unwrap();
                Some(c)
            } else {
                None
            };
            let conn = ledger
                .as_ref()
                .map(Ledger::connection)
                .unwrap_or_else(|| memory.as_ref().unwrap());
            loop {
                let req = match rx.lock().unwrap().recv() {
                    Ok(r) => r,
                    Err(_) => break,
                };
                {
                    let context = ServerContext {
                        shared: &shared,
                        catalog_cache: &catalog_cache,
                        port,
                        durable_at_start,
                        ledger_path: &path,
                        sync: sync_handle.as_ref(),
                        sync_only,
                    };
                    handle(req, conn, &context);
                }
            }
        });
    }
    // Query work must never occupy the workers that serve the page's scripts.
    // Both pools and queues are fixed-size; incomplete headers retain a deadline.
    let (incoming_tx, incoming_rx) = mpsc::sync_channel::<Connection>(16);
    let incoming_rx = Arc::new(Mutex::new(incoming_rx));
    for _ in 0..2 {
        let rx = incoming_rx.clone();
        let queries = tx.clone();
        let assets = assets.clone();
        std::thread::spawn(move || loop {
            let connection = match rx.lock().unwrap().recv() {
                Ok(connection) => connection,
                Err(_) => break,
            };
            if let Some(request) = Request::receive(connection) {
                if let Some(request) = if sync_only {
                    Some(request)
                } else {
                    static_request(request, &assets, port)
                } {
                    match queries.try_send(request) {
                        Ok(()) => {}
                        Err(mpsc::TrySendError::Full(request)) => error(request, 429, "BUSY"),
                        Err(mpsc::TrySendError::Disconnected(request)) => {
                            error(request, 503, "UNAVAILABLE");
                            break;
                        }
                    }
                }
            }
        });
    }
    println!("usagebook http://127.0.0.1:{port} · 本地账本与按需查询");
    if open {
        crate::serve::open_dashboard(port)
    }
    for stream in server.incoming() {
        let stream = match stream {
            Ok(stream) => stream,
            Err(error) => {
                tracing::warn!("HTTP accept failed: {error}");
                continue;
            }
        };
        match incoming_tx.try_send(Connection::new(stream)) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(connection)) => connection.reject(),
            Err(mpsc::TrySendError::Disconnected(connection)) => {
                connection.reject();
                break;
            }
        }
    }
}
fn static_request(req: Request, assets: &Assets, port: u16) -> Option<Request> {
    if !crate::serve::local_request(req.headers(), port) {
        error(req, 403, "FORBIDDEN");
        return None;
    }
    let path = req.url().split('?').next().unwrap_or("/").to_owned();
    if req.method() == &Method::Get {
        match path.as_str() {
            "/" | "/index.html" => {
                response(
                    req,
                    assets.index_html.clone(),
                    "text/html; charset=utf-8",
                    200,
                );
                return None;
            }
            "/dashboard.js" => {
                response(
                    req,
                    assets.dashboard_js.clone(),
                    "application/javascript; charset=utf-8",
                    200,
                );
                return None;
            }
            "/dashboard_v2.js" => {
                response(
                    req,
                    include_str!("dashboard_v2.js").to_string(),
                    "application/javascript; charset=utf-8",
                    200,
                );
                return None;
            }
            "/vendor/chart.umd.min.js" => {
                response(
                    req,
                    include_str!("vendor/chart.umd.min.js").to_string(),
                    "application/javascript; charset=utf-8",
                    200,
                );
                return None;
            }
            "/dashboard_3d.bundle.js" => {
                response(
                    req,
                    include_str!("dashboard_3d.bundle.js").to_string(),
                    "application/javascript; charset=utf-8",
                    200,
                );
                return None;
            }
            "/api/data" => {
                error(req, 410, "USE_API_V2_QUERY");
                return None;
            }
            _ => {}
        }
    }
    Some(req)
}
fn handle(mut req: Request, conn: &rusqlite::Connection, ctx: &ServerContext) {
    let ServerContext {
        shared,
        catalog_cache,
        port,
        durable_at_start,
        ledger_path,
        sync,
        sync_only,
    } = *ctx;
    if !crate::serve::local_request(req.headers(), port) {
        return error(req, 403, "FORBIDDEN");
    }
    let path = req.url().split('?').next().unwrap_or("/").to_owned();
    if path == "/api/local/sync/status" && req.method() == &Method::Get {
        let Some(host) = sync else {
            return error(req, 409, "SYNC_REQUIRES_PERSISTENT_LEDGER");
        };
        return match usagebook_core::ledger::sync_state::status(conn) {
            Ok(status) => response(
                req,
                host.annotate(status).to_string(),
                "application/json",
                200,
            ),
            Err(_) => error(req, 503, "STORAGE_ERROR"),
        };
    }
    if path == "/api/local/sync/control" && req.method() == &Method::Post {
        let Some(host) = sync else {
            return error(req, 409, "SYNC_REQUIRES_PERSISTENT_LEDGER");
        };
        if !req
            .headers()
            .iter()
            .any(|h| h.field.equiv("X-Usagebook-Local") && h.value.as_str() == "1")
            || !req.headers().iter().any(|h| {
                h.field.equiv("Content-Type")
                    && h.value.as_str().split(';').next() == Some("application/json")
            })
        {
            return error(req, 403, "FORBIDDEN");
        }
        if req.body_length().is_some_and(|n| n > 1024) {
            return error(req, 413, "BODY_TOO_LARGE");
        }
        let bytes = match req.read_body() {
            Ok(b) if b.len() <= 1024 => b,
            _ => return error(req, 400, "INVALID_CONTROL"),
        };
        let value: Value = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(_) => return error(req, 400, "INVALID_CONTROL"),
        };
        if value.as_object().is_none_or(|o| o.len() != 2) {
            return error(req, 400, "INVALID_CONTROL");
        }
        return match host.control(
            value["action"].as_str().unwrap_or(""),
            value["instance_id"].as_str().unwrap_or(""),
        ) {
            Ok(v) => response(req, v.to_string(), "application/json", 200),
            Err(e) => error(req, 503, &e),
        };
    }
    if sync_only {
        return error(req, 404, "NOT_FOUND");
    }
    if path == "/api/v2/refresh" && req.method() == &Method::Post {
        let mut state = shared.lock().unwrap();
        if state.writer_stopped {
            let code = state
                .error
                .clone()
                .unwrap_or_else(|| "STORAGE_ERROR".into());
            drop(state);
            return error(req, 503, &code);
        }
        state.request_refresh();
        drop(state);
        return response(
            req,
            json!({"refreshing":true}).to_string(),
            "application/json",
            202,
        );
    }
    let status_request = matches!(path.as_str(), "/api/v2/status" | "/api/v2/dashboard/status")
        && req.method() == &Method::Get;
    let (snapshot, refreshing, refresh_error, refresh_status) = {
        let mut s = shared.lock().unwrap();
        if status_request {
            s.viewed(req.url());
        }
        (
            s.snapshot.clone(),
            s.refreshing || s.durable_backlog,
            s.error.clone(),
            json!({
                "version": env!("CARGO_PKG_VERSION"),
                "refresh_generation": s.refresh_generation.to_string(),
                "generated_at_ms": s.last_scan_completed_ms.map(|n|n.to_string()),
                "refresh_secs": s.refresh_secs,
                "eff_refresh_secs": s.effective_refresh_ms(now_ms()) / 1000,
                "history_import": {"backlog":s.durable_backlog,"errors":s.durable_errors},
                "time_repair_runtime_error": s.time_repair_error,
                "collection": {"mode":s.collection_mode.as_str(),"interval_secs":s.refresh_secs,"state":s.collection_state()}
            }),
        )
    };
    let service = QueryService::new(
        conn,
        if durable_at_start {
            None
        } else {
            Some(&snapshot)
        },
    )
    .with_catalog_cache(catalog_cache);
    let result: std::result::Result<Value, usagebook_core::ledger::query_types::QueryError> =
        if status_request {
            let status = if path == "/api/v2/dashboard/status" {
                usagebook_core::ledger::dashboard_status::read(
                    conn,
                    if durable_at_start {
                        None
                    } else {
                        Some(&snapshot)
                    },
                )
            } else {
                service.status()
            };
            status.map(|mut s| {
                s["capabilities"] = json!({
                    "query_bundle_metrics": true,
                    "model_selector_exact": true,
                    "model_selector_max_ids": usagebook_core::ledger::query_types::MAX_MODEL_SELECTOR_IDS,
                    "model_selector_large_threshold": usagebook_core::ledger::query_types::LARGE_SELECTOR_THRESHOLD,
                    "model_selector_large_max_ids_bytes": usagebook_core::ledger::query_types::MAX_LARGE_SELECTOR_IDS_BYTES,
                    "max_body_bytes": MAX_BODY,
                });
                if durable_at_start {
                    s["ready"] = json!(true);
                }
                s["storage"] = json!({
                    "mode":if durable_at_start {"durable"} else {"temporary"},
                    "restart_required":!durable_at_start && db::exists_or_recoverable(ledger_path),
                    "durable_supported_sources":usagebook_core::ledger::DURABLE_SUPPORTED_SOURCES
                    ,"durable_evidence_sources":usagebook_core::ledger::DURABLE_EVIDENCE_SOURCES
                });
                s["refreshing"] = json!(refreshing);
                for (key, value) in refresh_status.as_object().unwrap() {
                    s[key] = value.clone();
                }
                if let Some(error) = s["time_repair_runtime_error"].as_str().map(str::to_owned) {
                    if !s["time_repair"].is_object() {
                        s["time_repair"] = json!({});
                    }
                    s["time_repair"]["error"] = json!(error);
                    s["time_repair"]["state"] = json!("error");
                }
                s.as_object_mut()
                    .unwrap()
                    .remove("time_repair_runtime_error");
                for source in s["sources"].as_array_mut().into_iter().flatten() {
                    if let Some(info) = usagebook_core::SOURCE_CATALOG
                        .iter()
                        .find(|info| source["id"] == info.id)
                    {
                        source["label"] = json!(info.label);
                    }
                }
                // A full disk may prevent saving the last failure itself.
                // Preserve the live failure alongside persistent recovery state.
                let history_errors = s["history_import"]["errors"].clone();
                for source in s["sources"].as_array_mut().into_iter().flatten() {
                    let id = source["id"].as_str().unwrap_or_default();
                    if let Some(error) = history_errors.get(id).and_then(Value::as_str) {
                        if source["error"].is_null() || source["error"] == "LOADING" {
                            source["error"] = json!(error);
                            source["state"] = json!(if matches!(
                                error,
                                "DISK_FULL" | "STORAGE_ERROR" | "IO_ERROR" | "PERMISSION_DENIED"
                            ) {
                                "save_error"
                            } else {
                                "read_error"
                            });
                        }
                    }
                }
                if let Some(error) = history_errors.get("ledger").and_then(Value::as_str) {
                    s["storage"]["error"] = json!(error);
                }
                if let Some(e) = refresh_error {
                    s["refresh_error"] = json!(e)
                }
                s
            })
        } else if req.method() == &Method::Post
            && (path == "/api/v2/query" || path == "/api/v2/catalog")
        {
            if req.body_length().is_some_and(|n| n > MAX_BODY) {
                return error(req, 413, "BODY_TOO_LARGE");
            }
            let bytes = match req.read_body() {
                Ok(bytes) if bytes.len() <= MAX_BODY => bytes,
                Ok(_) => return error(req, 413, "BODY_TOO_LARGE"),
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                    return error(req, 408, "REQUEST_TIMEOUT");
                }
                Err(_) => return error(req, 400, "INVALID_QUERY"),
            };
            if path == "/api/v2/query" {
                QueryRequest::from_json(&bytes)
                    .and_then(|q| service.query(q))
                    .map(|r| json!(r))
            } else {
                CatalogRequest::from_json(&bytes).and_then(|q| service.catalog(q))
            }
        } else {
            let known = matches!(
                path.as_str(),
                "/api/v2/status"
                    | "/api/v2/dashboard/status"
                    | "/api/v2/query"
                    | "/api/v2/catalog"
                    | "/api/v2/refresh"
                    | "/api/data"
                    | "/"
                    | "/index.html"
                    | "/dashboard.js"
                    | "/dashboard_v2.js"
                    | "/vendor/chart.umd.min.js"
                    | "/dashboard_3d.bundle.js"
            );
            return if known {
                error(req, 405, "METHOD_NOT_ALLOWED")
            } else {
                error(req, 404, "NOT_FOUND")
            };
        };
    match result {
        Ok(value) => {
            let bytes = value.to_string();
            if bytes.len() > 8 * 1024 * 1024 {
                error(req, 422, "QUERY_LIMIT")
            } else {
                response(req, bytes, "application/json; charset=utf-8", 200)
            }
        }
        Err(e) => response(
            req,
            json!({"error":{"code":e.code,"message":e.message,"retryable":e.retryable}})
                .to_string(),
            "application/json; charset=utf-8",
            e.http_status(),
        ),
    }
}

#[cfg(test)]
mod merge_tests {
    use super::*;
    use usagebook_core::ledger::legacy::{LegacyEntry, LegacySourceStatus};
    use usagebook_core::model::{Caliber, UsageRecord};

    fn entry(source: &'static str, ts: i64) -> LegacyEntry {
        LegacyEntry::Request {
            record: UsageRecord {
                source,
                session_id: "s".into(),
                agent: None,
                model: "m".into(),
                ts_ms: ts,
                caliber: Caliber::RequestDelta,
                input_uncached: 1,
                cache_read: 0,
                cache_write: 0,
                cache_write_1h: 0,
                output: 0,
                reasoning_output: 0,
                cost_usd: None,
                request_count: 1,
                dedup_key: format!("k-{ts}"),
            },
            native_tool: source.into(),
            namespace: None,
            known_fields: 0,
        }
    }

    fn snap_of(source: &'static str, ts: i64) -> LegacySnapshot {
        let mut s = LegacySnapshot::new("g".into(), "r".into());
        s.entries.push(entry(source, ts));
        s.sources.insert(
            source.into(),
            LegacySourceStatus {
                last_scanned_ms: Some(1),
                stale: false,
                error: None,
                unaccounted_rows: 0,
                unallocated_sessions: 0,
                warning: None,
            },
        );
        s
    }

    fn ts_of(s: &LegacySnapshot, source: &str) -> i64 {
        s.entries
            .iter()
            .find(|e| e.report_source() == source)
            .map(|e| match e {
                LegacyEntry::Request { record, .. } | LegacyEntry::SessionTotal { record, .. } => {
                    record.ts_ms
                }
                LegacyEntry::Day(_) => -1,
            })
            .unwrap_or(-1)
    }

    #[test]
    fn warning_only_source_keeps_fresh_entries() {
        let previous = snap_of("claude", 1);
        let mut current = snap_of("claude", 2);
        current.sources.get_mut("claude").unwrap().warning =
            Some("skipped 1 record(s); first: legacy model exceeds 1024 bytes".into());
        merge_failed_sources(&mut current, &previous);
        assert_eq!(ts_of(&current, "claude"), 2, "降级源的新鲜好数据不得回滚");
        let st = &current.sources["claude"];
        assert!(
            st.error.is_none() && !st.stale,
            "不得标为 SOURCE_READ_FAILED"
        );
        assert!(st.warning.is_some(), "warning 在合并后幸存");
    }

    #[test]
    fn error_source_rolls_back_to_previous() {
        let previous = snap_of("claude", 1);
        let mut current = snap_of("claude", 2);
        current.sources.get_mut("claude").unwrap().error = Some("boom".into());
        merge_failed_sources(&mut current, &previous);
        assert_eq!(ts_of(&current, "claude"), 1, "真失败源回滚到上一轮条目");
        let st = &current.sources["claude"];
        assert_eq!(st.error.as_deref(), Some("SOURCE_READ_FAILED"));
        assert!(st.stale);
    }
}
