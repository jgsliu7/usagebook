//! Fresh metadata for dashboard navigation and visible persistence warnings.
//! Historical diagnostic counts remain in QueryService::status.
use super::{
    legacy::LegacySnapshot,
    query::QueryService,
    query_types::{QueryError, QueryResult, Stamp, Status},
};
use rusqlite::Connection;
use serde_json::json;
use std::{collections::BTreeMap, time::Instant};

struct DeadlineGuard<'a>(&'a Connection);
impl Drop for DeadlineGuard<'_> {
    fn drop(&mut self) {
        let _ = self.0.progress_handler(0, None::<fn() -> bool>);
    }
}

/// Every call reads a new SQLite snapshot, including runtime changes which do
/// not advance commit_seq. No source files or historical usage rows are loaded.
pub fn read(conn: &Connection, legacy: Option<&LegacySnapshot>) -> QueryResult<Status> {
    let started = Instant::now();
    conn.progress_handler(1000, Some(move || started.elapsed().as_secs_f64() > 5.0))?;
    let _deadline = DeadlineGuard(conn);
    let _snapshot = conn.unchecked_transaction()?;
    let mut stamp = conn.query_row(
        "SELECT ledger_uuid,epoch,commit_seq,report_timezone,derived_version FROM ledger_meta WHERE singleton=1",
        [],
        |row| Ok(Stamp {
            ledger_uuid: row.get(0)?, epoch: row.get(1)?,
            commit_seq: row.get::<_, i64>(2)?.to_string(),
            report_timezone: row.get(3)?, derived_version: row.get::<_, i64>(4)?.to_string(),
            caliber_version: 1, legacy_generation: None,
        }),
    )?;
    let _: chrono_tz::Tz = stamp
        .report_timezone
        .parse()
        .map_err(|_| QueryError::new("LEDGER_UNREADABLE", "invalid stored report timezone"))?;
    let routing_hash = QueryService::routing_hash(conn)?;
    let live = if let Some(legacy) = legacy {
        if !legacy.routing_hash.is_empty() && legacy.routing_hash != routing_hash {
            return Err(QueryError::new(
                "BUSY",
                "source routing changed during refresh",
            ));
        }
        Some(super::read_model::State::from_snapshot(legacy))
    } else {
        super::read_model::state(conn)?
    };
    stamp.legacy_generation = live.as_ref().map(|state| state.generation.clone());
    let routes: BTreeMap<String, String> = conn
        .prepare("SELECT report_source,MIN(route) FROM source_roots GROUP BY report_source")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let runtime = super::collection::runtime_status(conn)
        .map_err(|error| QueryError::new(error.code(), error.to_string()))?;
    let mut sources = Vec::new();
    for info in crate::SOURCE_CATALOG {
        let durable = routes.get(info.id).is_some_and(|route| route == "ledger");
        let mut source = if durable {
            let (roots, enabled, errors, unenumerated, error): (i64, i64, i64, i64, Option<String>) = conn.query_row(
                "SELECT COUNT(*),COALESCE(SUM(enabled),0),COALESCE(SUM(error_code IS NOT NULL),0),COALESCE(SUM(enumeration_done=0),0),MAX(error_code) FROM source_roots WHERE report_source=? AND collector_key<>'reported-history-v1'",
                [info.id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)),
            )?;
            let (isolated, incomplete, object_error): (i64, i64, Option<String>) = conn.query_row(
                "SELECT COALESCE(SUM(o.state IN('missing','error','replay_required')),0),COALESCE(SUM(o.target_done=0),0),MAX(o.error_code) FROM source_objects o JOIN source_roots r USING(root_id) WHERE r.report_source=?",
                [info.id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
            )?;
            let publishing: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM ingest_runs u JOIN source_roots r ON r.identity_family=u.identity_family AND r.namespace_key=u.namespace_key WHERE r.report_source=? AND u.state='publishing')",
                [info.id], |row| row.get(0),
            )?;
            let compat_pending: bool =
                conn.query_row(super::resolve::COMPAT_PENDING_SQL, [info.id], |row| {
                    row.get(0)
                })?;
            let state = if roots > 0 && enabled == 0 {
                "paused"
            } else if errors + isolated > 0 {
                "error"
            } else if unenumerated + incomplete > 0 || publishing || compat_pending {
                "collecting"
            } else {
                "ready"
            };
            json!({"id":info.id,"route":"ledger","state":state,"error":error.or(object_error),"isolated_objects":isolated.to_string()})
        } else {
            let state = live.as_ref().and_then(|live| live.sources.get(info.id));
            let name = if state.is_some_and(|s| s.error.as_deref() == Some("LOADING")) {
                "collecting"
            } else if state.is_some_and(|s| s.stale) {
                "stale"
            } else if state.is_some_and(|s| s.error.is_some()) {
                "error"
            } else if state.is_some() {
                "ready"
            } else {
                "collecting"
            };
            let mut source = json!({"id":info.id,"route":"live","state":name,"error":state.and_then(|s|s.error.clone()),"isolated_objects":"0"});
            if info.id == "claude" && legacy.is_none() {
                let (has_baseline, pending): (bool, i64) = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM legacy_capture_entries WHERE report_source='claude'), (SELECT COUNT(*) FROM claude_protection_pending WHERE reason<>'original_baseline' AND reason NOT GLOB 'approved_successor/*')",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                source["history_protection"] = json!(if has_baseline {
                    "source_reported_baseline"
                } else {
                    "unprotected"
                });
                source["pending_compatibility_observations"] = json!(pending.to_string());
            }
            if info.id == "goose" {
                let count = state.map_or(0, |s| s.unallocated_sessions);
                source["accounting"] = json!({
                    "state":if count > 0 { "partial" } else { "complete" },
                    "unallocated_sessions":count.to_string(),
                    "reason":if count > 0 { Some("SESSION_USAGE_UNALLOCATED") } else { None },
                });
            }
            source
        };
        let phases = &runtime["sources"][info.id];
        if let Some(error) = runtime["continuation"][info.id]["error"].as_str() {
            source["error"] = json!(error);
            source["state"] = json!("publish_error");
        } else if runtime["continuation"][info.id]["state"] == "recovery_unverified" {
            source["error"] = json!("RECOVERY_UNVERIFIED");
            source["state"] = json!("recovery_unverified");
        }
        for (stage, state) in [
            ("save", "save_error"),
            ("publish", "publish_error"),
            ("source_read", "read_error"),
        ] {
            if let Some(error) = phases[stage]["error"].as_str() {
                source["error"] = json!(error);
                source["state"] = json!(state);
                break;
            }
            if phases[stage]["state"] == "recovery_unverified" {
                source["error"] = json!("RECOVERY_UNVERIFIED");
                source["state"] = json!("recovery_unverified");
                break;
            }
        }
        sources.push(source);
    }
    Ok(json!({
        "stamp":stamp,
        "ready":routes.values().any(|route|route=="ledger") || live.as_ref().is_some_and(|live|live.sources.values().any(|source|source.last_scanned_ms.is_some())),
        "sources":sources,
    }))
}
