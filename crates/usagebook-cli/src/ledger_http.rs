//! Bounded loopback query server. Only live adapters retain a process-local history snapshot.
use serde_json::{json, Value};
use std::{
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
    legacy::{LegacySnapshot, LegacySourceStatus},
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
        self.pending_force = true;
        self.refreshing = true;
    }
    fn begin_scan(&mut self, now: i64, first: bool, due: bool) -> Option<bool> {
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
        if self.refreshing || self.pending_force {
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
    assets: &'a Assets,
    catalog_cache: &'a ModelCatalogCache,
    port: u16,
    durable_at_start: bool,
    ledger_path: &'a std::path::Path,
}
fn response(req: Request, body: String, ctype: &str, code: u16) {
    let resp=Response::from_string(body).with_status_code(code).with_header(Header::from_bytes("Content-Type",ctype).unwrap()).with_header(Header::from_bytes("Cache-Control","no-store").unwrap()).with_header(Header::from_bytes("X-Content-Type-Options","nosniff").unwrap()).with_header(Header::from_bytes("Content-Security-Policy","default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'").unwrap());
    let _ = req.respond(resp);
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
pub fn run(port: u16, refresh_secs: u64, collection_mode: CollectionMode, open: bool) {
    if port == 0 || !(5..=86400).contains(&refresh_secs) {
        crate::ledger_cli::fail(
            "port must be positive; refresh interval must be 5..=86400 seconds",
            2,
        )
    }
    let server =
        TcpListener::bind(("127.0.0.1", port)).unwrap_or_else(|e| crate::ledger_cli::fail(e, 3));
    usagebook_core::store::prune_bad_backups();
    let _ = usagebook_core::logging::global().activate("serve");
    let path = db::default_path();
    let writer = if db::exists_or_recoverable(&path) {
        Some(
            Ledger::open(&path, true)
                .unwrap_or_else(|e| crate::ledger_cli::fail(&e, e.exit_code())),
        )
    } else {
        None
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
    for source in usagebook_core::registered_sources() {
        initial.sources.insert(
            source.id().into(),
            LegacySourceStatus {
                last_scanned_ms: None,
                stale: true,
                error: Some("LOADING".into()),
                unaccounted_rows: 0,
            },
        );
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
        last_scan_completed_ms: None,
        refresh_generation: 0,
    }));
    {
        let shared = shared.clone();
        std::thread::spawn(move || {
            let mut writer = writer;
            let mut first = true;
            let mut last = Instant::now();
            let mut last_wall_ms = now_ms();
            loop {
                let now = now_ms();
                let scan = {
                    let mut state = shared.lock().unwrap();
                    let interval_ms = state.effective_refresh_ms(now);
                    let due = last.elapsed() >= Duration::from_millis(interval_ms)
                        || now.saturating_sub(last_wall_ms).max(0) as u64 >= interval_ms;
                    state.begin_scan(now, first, due)
                };
                if let Some(forced) = scan {
                    first = false;
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                        || -> std::result::Result<LegacySnapshot, String> {
                            if std::env::var("USAGEBOOK_TEST_PANIC").as_deref() == Ok("1") {
                                panic!("injected test panic\nsecond line \u{1b}[31mUBCANARY-sk-testsecret123");
                            }
                            if let Some(ledger) = writer.as_mut() {
                                if forced {
                                    ledger.refresh_claude(&crate::ledger_cli::roots(), false)
                                } else {
                                    ledger.poll_claude(&crate::ledger_cli::roots())
                                }
                                .map_err(|e| e.to_string())?;
                                ledger.publish_pending().map_err(|e| e.to_string())?;
                                if !crate::ledger_cli::roots().is_empty() {
                                    ledger.activate_claude_ledger().map_err(|e| e.to_string())?;
                                }
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
                            let mut snapshot =
                                crate::ledger_cli::load_legacy(conn).map_err(|e| e.to_string())?;
                            // The interim LOADING snapshot may carry a new route hash while
                            // still containing old live rows. Merge only against the snapshot
                            // captured before that transition, and only for sources still live.
                            for (source, status) in &mut snapshot.sources {
                                if previous_for_merge.has_missing_input_containers(source) {
                                    status.stale = true;
                                    status.error = Some("SOURCE_READ_FAILED".into());
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
                                    snapshot.retain_input_containers(&source, &previous_for_merge);
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
                                    status.last_scanned_ms = previous_complete
                                        .and_then(|previous| previous.last_scanned_ms);
                                    if source == "ccswitch-rollup" {
                                        if let Some(previous) = previous_complete {
                                            status.unaccounted_rows = previous.unaccounted_rows;
                                        }
                                    }
                                    status.stale = true;
                                    status.error = Some("SOURCE_READ_FAILED".into());
                                }
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
                    let fingerprint = snapshot.content_fingerprint();
                    let mut state = shared.lock().unwrap();
                    state.publish_snapshot(snapshot, fingerprint);
                    state.complete_scan(error);
                    last = Instant::now();
                    last_wall_ms = now_ms();
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        });
    }
    let (tx, rx) = mpsc::sync_channel::<Connection>(2);
    let rx = Arc::new(Mutex::new(rx));
    let assets = Arc::new(Assets::new());
    let catalog_cache = Arc::new(ModelCatalogCache::default());
    for _ in 0..2 {
        let rx = rx.clone();
        let shared = shared.clone();
        let path = path.clone();
        let stamp = stamp.clone();
        let assets = assets.clone();
        let catalog_cache = catalog_cache.clone();
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
                if let Some(req) = Request::receive(req) {
                    let context = ServerContext {
                        shared: &shared,
                        assets: &assets,
                        catalog_cache: &catalog_cache,
                        port,
                        durable_at_start,
                        ledger_path: &path,
                    };
                    handle(req, conn, &context);
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
        match tx.try_send(Connection::new(stream)) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(connection)) => connection.reject(),
            Err(mpsc::TrySendError::Disconnected(connection)) => {
                connection.reject();
                break;
            }
        }
    }
}
fn handle(mut req: Request, conn: &rusqlite::Connection, ctx: &ServerContext) {
    let ServerContext {
        shared,
        assets,
        catalog_cache,
        port,
        durable_at_start,
        ledger_path,
    } = *ctx;
    if !crate::serve::local_request(req.headers(), port) {
        return error(req, 403, "FORBIDDEN");
    }
    let path = req.url().split('?').next().unwrap_or("/").to_owned();
    if req.method() == &Method::Get {
        match path.as_str() {
            "/" | "/index.html" => {
                return response(
                    req,
                    assets.index_html.clone(),
                    "text/html; charset=utf-8",
                    200,
                )
            }
            "/dashboard.js" => {
                return response(
                    req,
                    assets.dashboard_js.clone(),
                    "application/javascript; charset=utf-8",
                    200,
                )
            }
            "/dashboard_v2.js" => {
                return response(
                    req,
                    include_str!("dashboard_v2.js").to_string(),
                    "application/javascript; charset=utf-8",
                    200,
                )
            }
            "/vendor/chart.umd.min.js" => {
                return response(
                    req,
                    include_str!("vendor/chart.umd.min.js").to_string(),
                    "application/javascript; charset=utf-8",
                    200,
                )
            }
            "/api/data" => return error(req, 410, "USE_API_V2_QUERY"),
            _ => {}
        }
    }
    if path == "/api/v2/refresh" && req.method() == &Method::Post {
        shared.lock().unwrap().request_refresh();
        return response(
            req,
            json!({"refreshing":true}).to_string(),
            "application/json",
            202,
        );
    }
    let (snapshot, refreshing, refresh_error, refresh_status) = {
        let mut s = shared.lock().unwrap();
        if path == "/api/v2/status" && req.method() == &Method::Get {
            s.viewed(req.url());
        }
        (
            s.snapshot.clone(),
            s.refreshing,
            s.error.clone(),
            json!({
                "version": env!("CARGO_PKG_VERSION"),
                "refresh_generation": s.refresh_generation.to_string(),
                "generated_at_ms": s.last_scan_completed_ms.map(|n|n.to_string()),
                "refresh_secs": s.refresh_secs,
                "eff_refresh_secs": s.effective_refresh_ms(now_ms()) / 1000,
                "collection": {"mode":s.collection_mode.as_str(),"interval_secs":s.refresh_secs,"state":s.collection_state()}
            }),
        )
    };
    let service = QueryService::new(conn, Some(&snapshot)).with_catalog_cache(catalog_cache);
    let result: std::result::Result<Value, usagebook_core::ledger::query_types::QueryError> =
        if path == "/api/v2/status" && req.method() == &Method::Get {
            service.status().map(|mut s| {
                s["storage"] = json!({
                    "mode":if durable_at_start {"durable"} else {"temporary"},
                    "restart_required":!durable_at_start && db::exists_or_recoverable(ledger_path),
                    "durable_supported_sources":usagebook_core::ledger::DURABLE_SUPPORTED_SOURCES
                });
                s["refreshing"] = json!(refreshing);
                for (key, value) in refresh_status.as_object().unwrap() {
                    s[key] = value.clone();
                }
                for source in s["sources"].as_array_mut().into_iter().flatten() {
                    if let Some(info) = usagebook_core::SOURCE_CATALOG
                        .iter()
                        .find(|info| source["id"] == info.id)
                    {
                        source["label"] = json!(info.label);
                    }
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
                    | "/api/v2/query"
                    | "/api/v2/catalog"
                    | "/api/v2/refresh"
                    | "/api/data"
                    | "/"
                    | "/index.html"
                    | "/dashboard.js"
                    | "/dashboard_v2.js"
                    | "/vendor/chart.umd.min.js"
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
