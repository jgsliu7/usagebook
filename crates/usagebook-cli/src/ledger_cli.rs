use clap::{Args, Subcommand};
use serde_json::json;
use std::path::{Path, PathBuf};
use usagebook_core::ledger::{
    db,
    legacy::{LegacySnapshot, LegacySourceStatus},
    maintenance::{ForgetScope, Maintenance},
    query::{ModelCatalogCache, QueryService},
    query_types::*,
    types::{new_epoch, now_ms},
    Ledger, LedgerError,
};
#[derive(Subcommand)]
pub enum LedgerCommand {
    Init {
        /// IANA 时区名（如 Asia/Shanghai）；缺省取系统时区，推断失败时须显式指定
        #[arg(long)]
        timezone: Option<String>,
    },
    Status,
    Refresh {
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        full: bool,
    },
    Audit {
        #[arg(long)]
        source: Option<String>,
    },
    Backup {
        #[arg(long)]
        output: PathBuf,
    },
    Restore {
        #[arg(long)]
        input: PathBuf,
        #[command(flatten)]
        confirm: Confirm,
    },
    Forget {
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        all: bool,
        #[arg(long)]
        identity: Option<String>,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[command(flatten)]
        confirm: Confirm,
    },
    Timezone {
        #[arg(long)]
        set: String,
        #[arg(long, required = true)]
        yes: bool,
    },
    Pause {
        #[arg(long)]
        source: String,
    },
    Resume {
        #[arg(long)]
        source: String,
    },
    Unsuppress {
        #[arg(long)]
        rule_id: i64,
        #[command(flatten)]
        confirm: Confirm,
    },
    Check,
}
#[derive(Args)]
pub struct Confirm {
    #[arg(long, conflicts_with = "apply")]
    dry_run: bool,
    #[arg(long, requires = "preview_token")]
    apply: bool,
    #[arg(long, requires = "apply")]
    preview_token: Option<String>,
}
impl Confirm {
    fn token(&self) -> Option<&str> {
        if self.apply {
            self.preview_token.as_deref()
        } else {
            None
        }
    }
}
#[derive(Args, Default)]
pub struct ReportArgs {
    // Accepted only to return the same explicit diagnostic as the shortcut.
    // main rejects this before any report I/O; sampling is not supported.
    #[arg(long, hide = true)]
    pub max_files: Option<usize>,
    #[arg(long,conflicts_with_all=["days","query_file"])]
    pub from: Option<String>,
    #[arg(long,conflicts_with_all=["days","query_file"])]
    pub to: Option<String>,
    #[arg(long, conflicts_with = "query_file")]
    pub days: Option<usize>,
    #[arg(long, conflicts_with = "query_file")]
    pub as_of: Option<String>,
    #[arg(long,action=clap::ArgAction::Append,conflicts_with="query_file")]
    pub source: Vec<String>,
    #[arg(long,action=clap::ArgAction::Append,conflicts_with="query_file")]
    pub model: Vec<String>,
    #[arg(long)]
    pub query_file: Option<PathBuf>,
}
pub fn fail(error: impl std::fmt::Display, code: i32) -> ! {
    eprintln!("{error}");
    std::process::exit(code)
}
pub fn roots() -> Vec<PathBuf> {
    usagebook_core::registered_sources()
        .into_iter()
        .find(|s| s.id() == "claude")
        .map(|s| s.discover_roots())
        .unwrap_or_default()
}
pub fn run(command: LedgerCommand) {
    if let Err(e) = execute(command) {
        fail(&e, e.exit_code())
    }
}
fn execute(command: LedgerCommand) -> usagebook_core::ledger::Result<()> {
    let path = db::default_path();
    let value = match command {
        LedgerCommand::Init { timezone } => {
            let timezone = timezone.or_else(db::host_timezone_hint);
            let Some(timezone) = timezone else {
                return Err(LedgerError::InvalidInput(
                    "无法确定系统时区；请用 --timezone <IANA 名> 显式指定（如 Asia/Shanghai）"
                        .into(),
                ));
            };
            let l = Ledger::init(&path, &timezone)?;
            json!({"initialized":true,"path":path,"timezone":timezone,"stamp":l.stamp()?,
                "collection_started":false,"next_step":"启动 serve 开始采集；若服务已在运行，请先重启。"})
        }
        LedgerCommand::Status => {
            let l = Ledger::open(&path, false)?;
            QueryService::new(l.connection(), None)
                .status()
                .map_err(|e| LedgerError::InvalidInput(e.to_string()))?
        }
        LedgerCommand::Audit { source } => {
            return execute(LedgerCommand::Refresh { source, full: true })
        }
        LedgerCommand::Refresh { source, full } => {
            if source.as_deref().is_some_and(|s| s != "claude") {
                return Err(LedgerError::SourceNotProtected);
            }
            let mut l = Ledger::open(&path, true)?;
            let summary = l.refresh_claude(&roots(), full)?;
            l.publish_pending()?;
            if !roots().is_empty() {
                l.activate_claude_ledger()?;
            }
            if let Err(e) = l.optimize_if_due() {
                tracing::warn!("查询统计维护暂未完成，将在空闲时重试: {e}");
            }
            let incomplete = summary.errors > 0 || summary.pending > 0;
            let value = json!(summary);
            println!("{}", value);
            if incomplete {
                std::process::exit(5)
            }
            return Ok(());
        }
        LedgerCommand::Backup { output } => {
            Maintenance::open(&path)?.backup(&output)?;
            json!({"backup":output,"complete":true})
        }
        LedgerCommand::Restore { input, confirm } => {
            json!(Maintenance::open(&path)?.restore(&input, confirm.token())?)
        }
        LedgerCommand::Forget {
            source,
            all,
            identity,
            from,
            to,
            confirm,
        } => {
            let l = Ledger::open(&path, false)?;
            let tz = l.stamp()?.report_timezone;
            drop(l);
            let (start, end) = date_range(&tz, from.as_deref(), to.as_deref())?;
            json!(Maintenance::open(&path)?.forget(
                &ForgetScope {
                    source,
                    identity,
                    all,
                    start_ms: start,
                    end_ms: end
                },
                confirm.token()
            )?)
        }
        LedgerCommand::Timezone { set, yes: _ } => {
            Maintenance::open(&path)?.set_timezone(&set)?;
            json!({"timezone":set})
        }
        LedgerCommand::Pause { source } => {
            Maintenance::open(&path)?.pause(&source, true)?;
            json!({"source":source,"paused":true})
        }
        LedgerCommand::Resume { source } => {
            Maintenance::open(&path)?.pause(&source, false)?;
            json!({"source":source,"paused":false})
        }
        LedgerCommand::Unsuppress { rule_id, confirm } => {
            json!(Maintenance::open(&path)?.unsuppress(rule_id, confirm.token())?)
        }
        LedgerCommand::Check => {
            Maintenance::open(&path)?.check()?;
            json!({"integrity":"ok","foreign_keys":"ok"})
        }
    };
    println!("{}", serde_json::to_string_pretty(&value).unwrap());
    Ok(())
}
fn date_range(
    tz: &str,
    from: Option<&str>,
    to: Option<&str>,
) -> usagebook_core::ledger::Result<(Option<i64>, Option<i64>)> {
    use usagebook_core::ledger::metrics::{date_boundary, timezone};
    match (from, to) {
        (None, None) => Ok((None, None)),
        (Some(a), Some(b)) => {
            let a = chrono::NaiveDate::parse_from_str(a, "%Y-%m-%d")
                .map_err(|_| LedgerError::InvalidInput("invalid from date".into()))?;
            let b = chrono::NaiveDate::parse_from_str(b, "%Y-%m-%d")
                .map_err(|_| LedgerError::InvalidInput("invalid to date".into()))?
                .succ_opt()
                .ok_or(LedgerError::NumericOverflow)?;
            let zone = timezone(tz)?;
            let a = date_boundary(zone, a)?;
            let b = date_boundary(zone, b)?;
            if b <= a {
                return Err(LedgerError::InvalidInput("reversed dates".into()));
            }
            Ok((Some(a), Some(b)))
        }
        _ => Err(LedgerError::InvalidInput(
            "from and to must be given together".into(),
        )),
    }
}

pub fn load_legacy(conn: &rusqlite::Connection) -> QueryResult<LegacySnapshot> {
    let mut snapshot = LegacySnapshot::new(new_epoch(), QueryService::routing_hash(conn)?);
    for adapter in usagebook_core::registered_sources() {
        let source = adapter.id();
        let protected: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM source_roots WHERE report_source=?1 AND route='ledger')",
            [source],
            |r| r.get(0),
        )?;
        if protected {
            continue;
        }
        if source == "ccswitch-rollup" {
            let p = usagebook_core::common::home().join(".cc-switch/cc-switch.db");
            snapshot.observe_input_containers(source, vec![p.clone()]);
            if p.exists() {
                if snapshot.load_ccswitch_at(&p).is_err() {
                    snapshot.sources.insert(
                        source.into(),
                        LegacySourceStatus {
                            last_scanned_ms: None,
                            stale: true,
                            error: Some("SOURCE_READ_FAILED".into()),
                            unaccounted_rows: 0,
                            unallocated_sessions: 0,
                        },
                    );
                }
            } else {
                snapshot.sources.insert(
                    source.into(),
                    LegacySourceStatus {
                        last_scanned_ms: Some(now_ms()),
                        stale: false,
                        error: None,
                        unaccounted_rows: 0,
                        unallocated_sessions: 0,
                    },
                );
            }
            continue;
        }
        let _ = usagebook_core::common::take_walk_error();
        let roots = adapter.discover_roots();
        snapshot.observe_input_containers(source, adapter.input_containers(&roots));
        let (records, stats) = usagebook_core::common::scan_source_cached(adapter.as_ref(), &roots);
        let normalization = snapshot.push_records_with_stats(source, records, &stats, now_ms());
        if let Err(error) = &normalization {
            // push_records only validates this adapter's records and commits
            // atomically. Keep DB/routing failures outside this source boundary.
            // Its fixed validation messages contain no model names or source text.
            tracing::warn!(source, code = %error.code, reason = %error.message, "来源归一化失败");
        }
        if normalization.is_err() || stats.failed || stats.lines_bad_json > 0 {
            snapshot.sources.insert(
                source.into(),
                LegacySourceStatus {
                    last_scanned_ms: None,
                    stale: true,
                    error: Some("SOURCE_READ_FAILED".into()),
                    unaccounted_rows: 0,
                    unallocated_sessions: 0,
                },
            );
        }
    }
    Ok(snapshot)
}
pub fn report(args: ReportArgs, json_output: bool) {
    if let Err((message, code)) = report_inner(args, json_output) {
        fail(message, code)
    }
}
fn query_exit(e: usagebook_core::ledger::query_types::QueryError) -> (String, i32) {
    let code = match e.code.as_str() {
        "INVALID_QUERY" | "BODY_TOO_LARGE" | "QUERY_LIMIT" => 2,
        "BUSY" => 4,
        _ => 3,
    };
    let message = if matches!(e.code.as_str(), "CURSOR_STALE" | "TIMEZONE_CHANGED") {
        format!("{e}; data changed, rerun the report from the first page")
    } else {
        e.to_string()
    };
    (message, code)
}
fn print_report_page(
    response: &QueryResponse,
    json_output: bool,
    shown_warnings: &mut std::collections::BTreeSet<String>,
) {
    if json_output {
        println!("{}", serde_json::to_string(response).unwrap());
    } else {
        for warning in &response.warnings {
            if shown_warnings.insert(warning.clone()) {
                eprintln!("{warning}");
            }
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&response.payload).unwrap()
        );
    }
}
fn report_inner(args: ReportArgs, json_output: bool) -> std::result::Result<(), (String, i32)> {
    // Reject typos before opening the ledger or touching any source, including all + typo.
    for id in &args.source {
        if id != "all"
            && !usagebook_core::SOURCE_CATALOG
                .iter()
                .any(|source| source.id == id)
        {
            let allowed = usagebook_core::SOURCE_CATALOG
                .iter()
                .map(|source| source.id)
                .collect::<Vec<_>>()
                .join(", ");
            return Err((format!("Unknown source '{id}'. Allowed: all, {allowed}"), 2));
        }
    }
    let path = db::default_path();
    let ledger = if db::exists_or_recoverable(&path) {
        Some(Ledger::open(&path, false).map_err(|e| (e.to_string(), e.exit_code()))?)
    } else {
        None
    };
    let transient = if ledger.is_none() {
        let tz = db::host_timezone_hint().unwrap_or_else(|| {
            eprintln!(
                "LEDGER_TIMEZONE_HINT_FAILED：未能确定系统时区，未初始化报表临时按 UTC 折算日界。"
            );
            "UTC".to_string()
        });
        Some(db::live_connection(&tz).map_err(|e| (e.to_string(), e.exit_code()))?)
    } else {
        None
    };
    let conn = ledger
        .as_ref()
        .map(Ledger::connection)
        .unwrap_or_else(|| transient.as_ref().unwrap());
    if ledger.is_none() {
        eprintln!(
            "LEDGER_NOT_INITIALIZED：当前扫描未受历史保护；运行 usagebook ledger init 初始化。"
        );
    }
    let snapshot = load_legacy(conn).map_err(|e| (e.to_string(), 3))?;
    let cache = ModelCatalogCache::default();
    let service = QueryService::new(conn, Some(&snapshot)).with_catalog_cache(&cache);
    if let Some(path) = args.query_file {
        let bytes = read_query_file(&path).map_err(|e| (e.to_string(), 2))?;
        let request = QueryRequest::from_json(&bytes).map_err(|e| (e.to_string(), 2))?;
        let response = service.query(request).map_err(query_exit)?;
        println!("{}", serde_json::to_string(&response).unwrap());
        return if response.source_coverage == "partial" {
            Err(("PARTIAL_COVERAGE".into(), 5))
        } else {
            Ok(())
        };
    }
    let timezone: String = conn
        .query_row("SELECT report_timezone FROM ledger_meta", [], |r| r.get(0))
        .map_err(|e| (e.to_string(), 3))?;
    let asof = args
        .as_of
        .as_deref()
        .map(chrono::DateTime::parse_from_rfc3339)
        .transpose()
        .map_err(|_| ("invalid as-of".into(), 2))?
        .map(|d| d.timestamp_millis());
    // A rolling window and its cutoff must use the same clock reading. Other
    // windows adopt the first query's resolved cutoff below.
    let asof = asof.or_else(|| args.days.map(|_| now_ms()));
    let window = if let Some(days) = args.days {
        let ms = i64::try_from(days)
            .ok()
            .and_then(|v| v.checked_mul(86_400_000))
            .filter(|n| *n > 0)
            .ok_or(("invalid days".into(), 2))?;
        let end = asof.unwrap_or_else(now_ms);
        Window::Instant {
            start_ms: end
                .checked_sub(ms)
                .ok_or(("days overflow".into(), 2))?
                .to_string(),
            end_ms_exclusive: end
                .checked_add(1)
                .ok_or(("date overflow".into(), 2))?
                .to_string(),
        }
    } else {
        let (a, b) = date_range(&timezone, args.from.as_deref(), args.to.as_deref())
            .map_err(|e| (e.to_string(), 2))?;
        match (a, b) {
            (Some(a), Some(b)) => Window::Instant {
                start_ms: a.to_string(),
                end_ms_exclusive: b.to_string(),
            },
            _ => Window::All,
        }
    };
    let sources = if args.source.is_empty() || args.source.iter().any(|s| s == "all") {
        Selector::All
    } else {
        Selector::Include { ids: args.source }
    };
    let models = if args.model.is_empty() {
        Selector::All
    } else {
        Selector::Include { ids: args.model }
    };
    let mut query = Query {
        window,
        report_timezone: timezone,
        as_of_ms: asof.map(|v| v.to_string()),
        sources: sources.clone(),
        models: models.clone(),
        metric: Metric::Total,
        group_by: GroupBy::Source,
    };
    let mut partial = false;
    let mut shown_warnings = std::collections::BTreeSet::new();
    let mut report_stamp: Option<Stamp> = None;
    for dimension in [Dimension::Day, Dimension::Source, Dimension::Model] {
        let mut page = Page::default();
        loop {
            let response = service
                .query(QueryRequest::Table {
                    query: query.clone(),
                    dimension,
                    scope: Scope::Filtered,
                    page: page.clone(),
                })
                .map_err(query_exit)?;
            check_report_stamp(&mut report_stamp, &response.stamp).map_err(query_exit)?;
            if query.as_of_ms.is_none() {
                query.as_of_ms = Some(response.as_of_ms.clone());
            }
            partial |= response.source_coverage == "partial";
            print_report_page(&response, json_output, &mut shown_warnings);
            let next = response
                .payload
                .pointer("/data/next_cursor")
                .and_then(|v| v.as_str());
            if let Some(next) = next {
                page.cursor = Some(next.into())
            } else {
                break;
            }
        }
    }
    let mut page = Page::default();
    loop {
        let response = service
            .query(QueryRequest::SessionTotals {
                sources: sources.clone(),
                models: models.clone(),
                page: page.clone(),
            })
            .map_err(query_exit)?;
        check_report_stamp(&mut report_stamp, &response.stamp).map_err(query_exit)?;
        partial |= response.source_coverage == "partial";
        print_report_page(&response, json_output, &mut shown_warnings);
        if let Some(next) = response.payload.get("next_cursor").and_then(|v| v.as_str()) {
            page.cursor = Some(next.into())
        } else {
            break;
        }
    }
    if partial {
        Err((
            "PARTIAL_COVERAGE：已显示可读历史，部分来源存在缺口。".into(),
            5,
        ))
    } else {
        Ok(())
    }
}
fn check_report_stamp(expected: &mut Option<Stamp>, actual: &Stamp) -> QueryResult<()> {
    if let Some(expected) = expected {
        if expected != actual {
            return Err(QueryError::new(
                "CURSOR_STALE",
                "report snapshot changed between pages",
            ));
        }
    } else {
        *expected = Some(actual.clone());
    }
    Ok(())
}
fn read_query_file(path: &Path) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take((MAX_BODY + 1) as u64)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}
