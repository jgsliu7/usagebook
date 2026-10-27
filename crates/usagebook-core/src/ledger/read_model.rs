//! Disposable, normalized compatibility facts used only by read queries.
//! Publication belongs to the caller's transaction; no source I/O occurs here.
use super::{
    legacy::{LegacyEntry, LegacySnapshot, LegacySourceStatus},
    query::{legacy_fact, Fact},
    query_types::{QueryError, QueryResult},
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(super) struct State {
    pub generation: String,
    pub sources: BTreeMap<String, LegacySourceStatus>,
}
impl State {
    pub fn from_snapshot(snapshot: &LegacySnapshot) -> Self {
        Self {
            generation: snapshot.generation.clone(),
            sources: snapshot.sources.clone(),
        }
    }
}
#[derive(Serialize, Deserialize)]
pub(super) struct LiveFact {
    pub fact: Fact,
    pub label: String,
    pub reason: Option<String>,
}
impl LiveFact {
    pub fn from_entry(entry: &LegacyEntry) -> QueryResult<Self> {
        let (label, reason) = match entry {
            LegacyEntry::Request { record, .. } => (record.model.clone(), None),
            LegacyEntry::SessionTotal { record, reason, .. } => {
                (record.model.clone(), reason.clone())
            }
            LegacyEntry::Day(day) => (day.model_display.clone(), None),
        };
        Ok(Self {
            fact: legacy_fact(entry)?,
            label,
            reason,
        })
    }
}
fn invalid() -> QueryError {
    QueryError::new("LEDGER_UNREADABLE", "invalid persisted query projection")
}
fn exists(conn: &Connection) -> QueryResult<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='query_live_state')",
        [],
        |r| r.get(0),
    )?)
}
/// Replace the compatibility read model atomically with its capture metadata.
/// The owning writer must include this call and its durable capture publication
/// in one transaction. Requiring a transaction prevents a half-written view.
pub fn replace(conn: &Connection, snapshot: &LegacySnapshot) -> QueryResult<()> {
    if conn.is_autocommit() {
        return Err(QueryError::invalid(
            "query projection publication requires a transaction",
        ));
    }
    super::legacy::CapturePrivacy::load(conn)?.validate(conn, snapshot)?;
    let (uuid, epoch): (String, String) = conn.query_row(
        "SELECT ledger_uuid,epoch FROM ledger_meta WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let unchanged:bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM query_live_state WHERE singleton=1 AND ledger_uuid=?1 AND epoch=?2 AND generation=?3)", params![uuid,epoch,snapshot.generation], |r|r.get(0))?;
    if !unchanged {
        conn.execute("DELETE FROM query_live_facts", [])?;
        let routes = {
            let mut query = conn
                .prepare("SELECT DISTINCT report_source FROM source_roots WHERE route='ledger'")?;
            let rows = query.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<rusqlite::Result<std::collections::BTreeSet<_>>>()?
        };
        let mut insert = conn.prepare("INSERT INTO query_live_facts(report_source,fact_kind,model_key,occurred_ms,payload_json) VALUES(?1,?2,?3,?4,?5)")?;
        for entry in &snapshot.entries {
            // Test the route before normalization: adopted histories may dwarf
            // the few remaining compatibility sources.
            if routes.contains(entry.report_source()) {
                continue;
            }
            let row = LiveFact::from_entry(entry)?;
            let payload = serde_json::to_string(&row).map_err(|_| invalid())?;
            insert.execute(params![
                row.fact.source,
                row.fact.kind,
                row.fact.model,
                row.fact.time,
                payload
            ])?;
        }
    }
    let sources = serde_json::to_string(&snapshot.sources).map_err(|_| invalid())?;
    conn.execute("INSERT INTO query_live_state VALUES(1,?1,?2,?3,?4,?5) ON CONFLICT(singleton) DO UPDATE SET ledger_uuid=excluded.ledger_uuid,epoch=excluded.epoch,generation=excluded.generation,routing_hash=excluded.routing_hash,sources_json=excluded.sources_json", params![uuid,epoch,snapshot.generation,snapshot.routing_hash,sources])?;
    Ok(())
}
pub(super) fn state(conn: &Connection) -> QueryResult<Option<State>> {
    if !exists(conn)? {
        return Ok(None);
    }
    let row: Option<(String,String)> = conn.query_row("SELECT s.generation,s.sources_json FROM query_live_state s JOIN ledger_meta m ON m.singleton=s.singleton AND m.ledger_uuid=s.ledger_uuid AND m.epoch=s.epoch", [], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
    row.map(|(generation, sources)| {
        Ok(State {
            generation,
            sources: serde_json::from_str(&sources).map_err(|_| invalid())?,
        })
    })
    .transpose()
}
pub(super) fn facts(conn: &Connection) -> QueryResult<Vec<LiveFact>> {
    if !exists(conn)? {
        return Ok(Vec::new());
    }
    // Current routing and epoch are read in the query's existing SQLite snapshot.
    // A route change needs no source rescan and cannot count both providers.
    let mut statement = conn.prepare("SELECT f.payload_json FROM query_live_facts f WHERE EXISTS(SELECT 1 FROM query_live_state s JOIN ledger_meta m ON m.singleton=s.singleton AND m.ledger_uuid=s.ledger_uuid AND m.epoch=s.epoch) AND NOT EXISTS(SELECT 1 FROM source_roots r WHERE r.report_source=f.report_source AND r.route='ledger') ORDER BY f.row_id")?;
    let mut rows = statement.query([])?;
    let mut facts = Vec::new();
    while let Some(row) = rows.next()? {
        let json: String = row.get(0)?;
        facts.push(serde_json::from_str(&json).map_err(|_| invalid())?);
    }
    Ok(facts)
}
