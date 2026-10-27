//! One-time, auditable reconstruction of old adopted day facts from retained
//! request evidence. Original amounts and original adoption-time members are
//! never changed. Dashboard reads only consume sealed revisions.
use super::{
    collection, db, ingest,
    legacy::{self, LegacyEntry, LegacySnapshot},
    time_allocation::{self, TimeMember},
    types::*,
    Ledger,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

const PROVENANCE: &str = "rebuilt-from-retained-source";
const MAX_BATCH: usize = 128;
const MAX_SOURCE_ROWS: usize = 100_000;
const COMPLETE_KEY: &str = "time-repair-complete-v1";
const PENDING: &str = "SELECT o.observation_id FROM usage_observations o JOIN adoption_items i ON i.selected_observation_id=o.observation_id JOIN adoption_policies p USING(policy_id) LEFT JOIN durable_time_allocations a ON a.parent_observation_id=o.observation_id LEFT JOIN durable_time_revisions r ON r.parent_observation_id=o.observation_id WHERE p.status='active' AND p.kind='legacy_snapshot' AND o.parser_version='reported-history-v1' AND o.fact_kind='day' AND a.parent_observation_id IS NULL AND r.parent_observation_id IS NULL";

/// Run inside the schema upgrade transaction, before any new capture can replace
/// old rows. It preserves normalized usage fields only, with per-source bounds.
pub(crate) fn preserve_upgrade_capture(c: &Connection) -> Result<()> {
    if c.is_autocommit() {
        return Err(LedgerError::CorruptLedger);
    }
    c.execute(&format!("INSERT INTO time_repair_saved_sources(report_source,ledger_uuid,metadata_json,entry_count) SELECT e.report_source,s.ledger_uuid,s.metadata_json,COUNT(*) FROM legacy_capture_entries e CROSS JOIN legacy_capture_state s JOIN ledger_meta m ON s.ledger_uuid=m.ledger_uuid AND s.epoch=m.epoch AND json_extract(s.metadata_json,'$.entry_count')=(SELECT COUNT(*) FROM legacy_capture_entries) WHERE e.report_source IN(SELECT report_source FROM usage_observations WHERE observation_id IN({PENDING})) AND coalesce(json_extract(s.metadata_json,'$.sources.'||e.report_source||'.stale'),1)=0 AND json_extract(s.metadata_json,'$.sources.'||e.report_source||'.error') IS NULL AND json_extract(s.metadata_json,'$.sources.'||e.report_source||'.last_scanned_ms') IS NOT NULL AND coalesce(json_extract(s.metadata_json,'$.sources.'||e.report_source||'.unaccounted_rows'),0)=0 AND coalesce(json_extract(s.metadata_json,'$.sources.'||e.report_source||'.unallocated_sessions'),0)=0 GROUP BY e.report_source HAVING COUNT(*)<=100000"),[])?;
    c.execute("INSERT INTO time_repair_saved_entries SELECT e.report_source,e.ordinal,e.entry_json FROM legacy_capture_entries e JOIN time_repair_saved_sources s USING(report_source)",[])?;
    c.execute("INSERT INTO time_repair_saved_projection SELECT s.report_source,r.root_id,r.row_key,root.identity_family,root.namespace_key,s.layout_json,r.projection_json,r.row_hash FROM source_projection_scopes s JOIN source_projection_rows r USING(root_id) JOIN source_roots root USING(root_id) JOIN time_repair_saved_sources kept ON kept.report_source=s.report_source WHERE s.report_source='zcode' AND s.complete=1 AND s.error_code IS NULL AND s.epoch=(SELECT epoch FROM ledger_meta) AND r.table_name='model_usage'",[])?;
    Ok(())
}

pub(crate) fn clear_staging(c: &Connection) -> Result<()> {
    c.execute("DELETE FROM time_repair_saved_sources", [])?;
    c.execute("DELETE FROM time_repair_attempts", [])?;
    Ok(())
}
pub(crate) fn clear_source_staging(c: &Connection, source: &str) -> Result<()> {
    c.execute(
        "DELETE FROM time_repair_saved_sources WHERE report_source=?1",
        [source],
    )?;
    c.execute("DELETE FROM time_repair_attempts WHERE parent_observation_id IN(SELECT observation_id FROM usage_observations WHERE report_source=?1)",[source])?;
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RevisionMember {
    member: TimeMember,
    content_hash: String,
}
#[derive(Serialize)]
struct Candidate {
    parent_id: i64,
    source: String,
    parent_hash: String,
    policy_hash: String,
    evidence: Value,
    members: Vec<RevisionMember>,
}
impl Candidate {
    fn manifest(&self) -> Result<String> {
        time_allocation::digest(&(
            "time-revision-v1",
            PROVENANCE,
            &self.parent_hash,
            &self.policy_hash,
            &self.evidence,
            &self.members,
        ))
    }
}
#[derive(Serialize)]
struct Skipped {
    parent_observation_id: i64,
    source: String,
    reason: String,
}
struct Batch {
    candidates: Vec<Candidate>,
    skipped: Vec<Skipped>,
    token: String,
    basis: BTreeMap<String, String>,
    has_more: bool,
}
fn reject(reason: &str) -> LedgerError {
    LedgerError::InvalidInput(reason.into())
}
fn reason(error: LedgerError) -> Result<String> {
    match error {
        LedgerError::InvalidInput(s) => Ok(s),
        LedgerError::IncompleteEvidence => Ok("INCOMPLETE_EVIDENCE".into()),
        LedgerError::RecordTooLarge => Ok("EVIDENCE_LIMIT_EXCEEDED".into()),
        LedgerError::NumericOverflow | LedgerError::InvalidUsageNumber => {
            Ok("INVALID_USAGE_NUMBER".into())
        }
        other => Err(other),
    }
}
fn hash_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn policy(c: &Connection, id: i64) -> Result<Value> {
    Ok(c.query_row("SELECT p.policy_id,p.consumer_tool,p.kind,p.start_ms,p.end_ms,p.native_timezone,p.manifest_hash,p.status FROM adoption_policies p JOIN adoption_items i USING(policy_id) WHERE i.selected_observation_id=?1",[id],|r|Ok(json!({"id":r.get::<_,String>(0)?,"tool":r.get::<_,String>(1)?,"kind":r.get::<_,String>(2)?,"start_ms":r.get::<_,Option<i64>>(3)?,"end_ms":r.get::<_,Option<i64>>(4)?,"timezone":r.get::<_,Option<String>>(5)?,"manifest":r.get::<_,String>(6)?,"status":r.get::<_,String>(7)?})))?)
}

/// Unknown identity/session deletion intent cannot safely be mapped through a
/// historical aggregate. Physical tombstones likewise take precedence over
/// retained captures, including when their row has already been removed.
fn controls(c: &Connection, p: &Observation) -> Result<String> {
    if ingest::exact_suppressed(c, p)? {
        return Err(reject("FORGOTTEN_RANGE"));
    }
    let uncertain:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM suppression_rules WHERE kind IN('identity','identity_link','session') AND (identity_family IS NULL OR identity_family=?1) AND (namespace_key IS NULL OR namespace_key=?2) AND (report_source IS NULL OR report_source=?3) AND (native_tool IS NULL OR native_tool=?4))",params![p.identity_family,p.namespace_key,p.report_source,p.native_tool],|r|r.get(0))?;
    if uncertain {
        return Err(reject("UNRESOLVED_DELETION_SCOPE"));
    }
    let tombstones:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM source_projection_tombstones t JOIN source_roots r USING(root_id) WHERE r.report_source=?1 AND r.identity_family=?2 AND r.namespace_key=?3)",params![p.report_source,p.identity_family,p.namespace_key],|r|r.get(0))?;
    if tombstones {
        return Err(reject("PHYSICAL_ROWS_FORGOTTEN"));
    }
    // Includes ranges, suppressed identities, namespace/channel changes, and
    // durable tombstones. This is captured at publication, never re-evaluated
    // as an excuse to replace a revision on a later dashboard read.
    source_control_basis(c, &p.report_source)
}

fn source_control_basis(c: &Connection, source: &str) -> Result<String> {
    let queries=[
        "SELECT root_id,identity_family,namespace_key,report_source,native_tool,identity_version,parser_version,route,enabled FROM source_roots WHERE report_source=?1 ORDER BY root_id",
        "SELECT * FROM suppression_rules WHERE (report_source IS NULL OR report_source=?1) AND (native_tool IS NULL OR native_tool IN(SELECT native_tool FROM source_roots WHERE report_source=?1)) AND (identity_family IS NULL OR identity_family IN(SELECT identity_family FROM source_roots WHERE report_source=?1)) AND (namespace_key IS NULL OR namespace_key IN(SELECT namespace_key FROM source_roots WHERE report_source=?1)) ORDER BY rule_id",
        "SELECT * FROM adoption_forget_ranges WHERE consumer_tool IN(SELECT native_tool FROM source_roots WHERE report_source=?1) ORDER BY consumer_tool,start_ms,end_ms,native_timezone,identity_family,namespace_key",
        "SELECT * FROM durable_coverage WHERE native_tool IN(SELECT native_tool FROM source_roots WHERE report_source=?1) ORDER BY native_tool,namespace_key,start_ms,end_ms",
        "SELECT * FROM durable_channel_choices WHERE identity_family IN(SELECT identity_family FROM source_roots WHERE report_source=?1) AND namespace_key IN(SELECT namespace_key FROM source_roots WHERE report_source=?1) ORDER BY identity_family,namespace_key,session_key",
        "SELECT t.* FROM source_projection_tombstones t JOIN source_roots r USING(root_id) WHERE r.report_source=?1 ORDER BY t.root_id,t.table_name,t.row_key_hash",
    ];
    let mut values = Vec::new();
    for sql in queries {
        let mut stmt = c.prepare(sql)?;
        let count = stmt.column_count();
        let mut rows = stmt.query([source])?;
        while let Some(row) = rows.next()? {
            let mut fields = Vec::with_capacity(count);
            for i in 0..count {
                fields.push(match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => Value::Null,
                    rusqlite::types::ValueRef::Integer(n) => json!(n),
                    rusqlite::types::ValueRef::Text(s) => {
                        json!(std::str::from_utf8(s).map_err(|_| LedgerError::CorruptLedger)?)
                    }
                    rusqlite::types::ValueRef::Blob(bytes) => json!(hash_bytes(bytes)),
                    _ => return Err(LedgerError::CorruptLedger),
                });
            }
            values.push(json!([sql, fields]));
        }
    }
    time_allocation::digest(&values)
}

struct Capture {
    snapshot: LegacySnapshot,
    hash: String,
    saved: bool,
    by_model: BTreeMap<String, Vec<(i64, usize)>>,
    zcode: Option<BTreeMap<String, ZcodeRow>>,
    identity_hashes: BTreeMap<String, String>,
}
fn capture(c: &Connection, p: &Observation, prefer_saved: bool) -> Result<Capture> {
    let source = p.report_source.as_str();
    let saved:Option<(String,i64)>=c.query_row("SELECT metadata_json,entry_count FROM time_repair_saved_sources WHERE report_source=?1 AND ledger_uuid=(SELECT ledger_uuid FROM ledger_meta)",[source],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let saved = if prefer_saved { saved } else { None };
    let is_saved = saved.is_some();
    let snapshot = if let Some((metadata, expected)) = saved {
        let mut snapshot: LegacySnapshot =
            serde_json::from_str(&metadata).map_err(|_| LedgerError::CorruptLedger)?;
        let mut stmt=c.prepare("SELECT entry_json FROM time_repair_saved_entries WHERE report_source=?1 ORDER BY ordinal")?;
        let mut rows = stmt.query([source])?;
        while let Some(row) = rows.next()? {
            snapshot.entries.push(
                serde_json::from_str(&row.get::<_, String>(0)?)
                    .map_err(|_| LedgerError::CorruptLedger)?,
            );
        }
        if snapshot.entries.len() != expected as usize {
            return Err(LedgerError::CorruptLedger);
        }
        snapshot
    } else {
        LegacySnapshot::restore_capture_source(c, Some(source))
            .map_err(|_| reject("INVALID_SAVED_CAPTURE"))?
            .ok_or(LedgerError::IncompleteEvidence)?
    };
    let status = snapshot
        .sources
        .get(source)
        .ok_or(LedgerError::IncompleteEvidence)?;
    if status.stale
        || status.error.is_some()
        || status.last_scanned_ms.is_none()
        || status.unaccounted_rows > 0
        || status.unallocated_sessions > 0
    {
        return Err(reject("SOURCE_CAPTURE_INCOMPLETE"));
    }
    if snapshot.entries.len() > MAX_SOURCE_ROWS {
        return Err(LedgerError::RecordTooLarge);
    }
    if !is_saved {
        let expected:i64=c.query_row("SELECT json_extract(metadata_json,'$.entry_count') FROM legacy_capture_state WHERE singleton=1",[],|r|r.get(0))?;
        let actual: i64 = c.query_row("SELECT COUNT(*) FROM legacy_capture_entries", [], |r| {
            r.get(0)
        })?;
        if expected != actual {
            return Err(reject("CAPTURE_COUNT_MISMATCH"));
        }
    }
    let hash = time_allocation::digest(&("retained-source-capture-v1", source, &snapshot.entries))?;
    let mut by_model = BTreeMap::<String, Vec<(i64, usize)>>::new();
    let mut identity_hashes = BTreeMap::new();
    for (index, entry) in snapshot.entries.iter().enumerate() {
        let LegacyEntry::Request {
            record,
            native_tool,
            namespace,
            ..
        } = entry
        else {
            return Err(reject("NON_REQUEST_SOURCE_CAPTURE"));
        };
        if record.source != source
            || native_tool != &p.native_tool
            || namespace.as_ref().is_some_and(|ns| ns != &p.namespace_key)
        {
            return Err(reject("SOURCE_DOMAIN_MISMATCH"));
        }
        if record.caliber != crate::Caliber::RequestDelta || record.ts_ms < 0 {
            return Err(reject("UNTRUSTWORTHY_REQUEST_TIME"));
        }
        if record.dedup_key.is_empty() {
            return Err(reject("MISSING_SOURCE_RECORD_KEY"));
        }
        if identity_hashes
            .insert(record.dedup_key.clone(), time_allocation::digest(record)?)
            .is_some()
        {
            return Err(reject("DUPLICATE_SOURCE_RECORD_KEY"));
        }
        by_model
            .entry(legacy::model_key(&record.model))
            .or_default()
            .push((record.ts_ms, index));
    }
    for entries in by_model.values_mut() {
        entries.sort_unstable();
    }
    let zcode = if source == "zcode" {
        Some(zcode_rows(c, p, is_saved)?)
    } else {
        None
    };
    Ok(Capture {
        snapshot,
        hash,
        saved: is_saved,
        by_model,
        zcode,
        identity_hashes,
    })
}

// ZCode's completed-only reader timestamps requests by started_at. A request
// completed after the original fixed cutoff was not part of that snapshot.
// Selection uses this explicit source contract, never an amount-driven subset.
struct ZcodeRow {
    fields: BTreeMap<String, Value>,
    hash: String,
}
fn zcode_rows(c: &Connection, p: &Observation, saved: bool) -> Result<BTreeMap<String, ZcodeRow>> {
    let epoch: String = c.query_row("SELECT epoch FROM ledger_meta", [], |r| r.get(0))?;
    let sql = if saved {
        "SELECT (SELECT epoch FROM ledger_meta),1,NULL,layout_json,projection_json,row_hash FROM time_repair_saved_projection WHERE report_source='zcode' AND identity_family=?1 AND namespace_key=?2 ORDER BY root_id,row_key LIMIT ?3"
    } else {
        "SELECT s.epoch,s.complete,s.error_code,s.layout_json,r.projection_json,r.row_hash FROM source_projection_scopes s JOIN source_roots root USING(root_id) JOIN source_projection_rows r USING(root_id) WHERE s.report_source='zcode' AND root.identity_family=?1 AND root.namespace_key=?2 AND r.table_name='model_usage' ORDER BY r.root_id,r.row_key LIMIT ?3"
    };
    let mut stmt = c.prepare(sql)?;
    let mut rows = stmt.query(params![
        p.identity_family,
        p.namespace_key,
        MAX_SOURCE_ROWS as i64 + 1
    ])?;
    let mut result = BTreeMap::new();
    while let Some(row) = rows.next()? {
        if row.get::<_, String>(0)? != epoch
            || row.get::<_, i64>(1)? != 1
            || row.get::<_, Option<String>>(2)?.is_some()
        {
            return Err(reject("SOURCE_PROJECTION_INCOMPLETE"));
        }
        let layout: Value = serde_json::from_str(&row.get::<_, String>(3)?)
            .map_err(|_| LedgerError::CorruptLedger)?;
        let columns = layout["model_usage"]
            .as_array()
            .ok_or(LedgerError::CorruptLedger)?;
        let cells: Vec<super::source_projection::SqlCell> =
            serde_json::from_str(&row.get::<_, String>(4)?)
                .map_err(|_| LedgerError::CorruptLedger)?;
        if columns.len() != cells.len() {
            return Err(LedgerError::CorruptLedger);
        }
        let mut fields = BTreeMap::new();
        for (column, cell) in columns.iter().zip(cells) {
            let value = match cell {
                super::source_projection::SqlCell::Null => Value::Null,
                super::source_projection::SqlCell::Integer(n) => json!(n),
                super::source_projection::SqlCell::Text(s) => json!(s),
                super::source_projection::SqlCell::RealBits(bits) => json!(f64::from_bits(bits)),
            };
            fields.insert(
                column["name"]
                    .as_str()
                    .ok_or(LedgerError::CorruptLedger)?
                    .to_owned(),
                value,
            );
        }
        let id = fields
            .get("id")
            .and_then(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .or_else(|| v.as_i64().map(|n| n.to_string()))
            })
            .ok_or(LedgerError::IncompleteEvidence)?;
        let hash = time_allocation::digest(&(
            "retained-zcode-fields-v1",
            &fields,
            row.get::<_, String>(5)?,
        ))?;
        if result.insert(id, ZcodeRow { fields, hash }).is_some() {
            return Err(reject("CONFLICTING_SOURCE_ROW_ID"));
        }
        if result.len() > MAX_SOURCE_ROWS {
            return Err(LedgerError::RecordTooLarge);
        }
    }
    Ok(result)
}
fn zcode_completed(row: &ZcodeRow, record: &crate::model::UsageRecord) -> Result<i64> {
    use crate::sources::zcode::{value_to_ms, value_to_string, value_to_u64};
    use rusqlite::types::Value as SqlValue;
    let cell = |name: &str| match row.fields.get(name) {
        Some(Value::String(s)) => SqlValue::Text(s.clone()),
        Some(Value::Number(n)) => n
            .as_i64()
            .map(SqlValue::Integer)
            .unwrap_or_else(|| SqlValue::Real(n.as_f64().unwrap_or(f64::NAN))),
        _ => SqlValue::Null,
    };
    let integer = |name: &str| {
        i64::try_from(value_to_u64(&cell(name))).map_err(|_| LedgerError::NumericOverflow)
    };
    let completed = value_to_ms(&cell("completed_at"));
    let started = value_to_ms(&cell("started_at"))
        .or_else(|| completed?.checked_sub(integer("duration_ms").ok()?));
    let input = integer("input_tokens")?;
    let output = integer("output_tokens")?;
    let read = integer("cache_read_input_tokens")?.min(input);
    let write = integer("cache_creation_input_tokens")?.min(input - read);
    let reasoning = integer("reasoning_tokens")?.min(output);
    let metrics = legacy::legacy_metrics(record).map_err(|_| LedgerError::InvalidUsageNumber)?;
    let status = row.fields.get("status").and_then(Value::as_str);
    if status.is_some_and(|status| status != "completed")
        || started.is_some_and(|started| started != record.ts_ms)
        || completed
            .zip(started)
            .is_some_and(|(completed, started)| completed < started)
        || value_to_string(&cell("model_id")).map(|s| legacy::model_key(&s))
            != Some(legacy::model_key(&record.model))
        || value_to_string(&cell("session_id")).as_deref() != Some(record.session_id.as_str())
        || metrics[..5]
            != [
                Some(input - read - write),
                Some(read),
                Some(write),
                Some(output - reasoning),
                Some(reasoning),
            ]
    {
        return Err(reject("SOURCE_PROJECTION_CONFLICT"));
    }
    if status != Some("completed") || started.is_none() {
        return Err(reject("SOURCE_PROJECTION_FIELDS_MISSING"));
    }
    completed.ok_or_else(|| reject("SOURCE_PROJECTION_FIELDS_MISSING"))
}

fn candidate(c: &Connection, id: i64, cap: &Capture) -> Result<Candidate> {
    let p = time_allocation::parent(c, id)?;
    let control = controls(c, &p)?;
    let (start, end) = p
        .day_start_ms
        .zip(p.day_end_ms)
        .ok_or(LedgerError::CorruptLedger)?;
    let timezone = p
        .native_timezone
        .as_deref()
        .ok_or(LedgerError::CorruptLedger)?;
    let policy = policy(c, id)?;
    if policy["kind"] != "legacy_snapshot"
        || policy["status"] != "active"
        || policy["tool"] != p.native_tool
        || policy["timezone"].as_str() != Some(timezone)
        || policy["start_ms"].as_i64().is_none_or(|v| v > start)
        || policy["end_ms"].as_i64().is_none_or(|v| v < end)
    {
        return Err(LedgerError::CorruptLedger);
    }
    let conflicting_domain:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM source_roots WHERE report_source=?1 AND (identity_family<>?2 OR namespace_key<>?3 OR native_tool<>?4))",params![p.report_source,p.identity_family,p.namespace_key,p.native_tool],|r|r.get(0))?;
    if conflicting_domain {
        return Err(reject("AMBIGUOUS_SOURCE_DOMAIN"));
    }
    let zcode = &cap.zcode;
    let mut members = Vec::new();
    let mut projected = Vec::new();
    let mut excluded = Vec::new();
    let mut completed_times = BTreeMap::new();
    let mut incomplete_projection = None;
    let entries = cap
        .by_model
        .get(&p.model_key)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let from = entries.partition_point(|(time, _)| *time < start);
    let until = entries.partition_point(|(time, _)| *time < end);
    for (_, index) in &entries[from..until] {
        let LegacyEntry::Request { record, .. } = &cap.snapshot.entries[*index] else {
            return Err(LedgerError::CorruptLedger);
        };
        let (_, _, day) = super::metrics::day_bounds(timezone, record.ts_ms)?;
        if p.native_date.as_deref() != Some(day.as_str()) {
            return Err(reject("ORIGINAL_CALENDAR_MISMATCH"));
        }
        let key = time_allocation::digest(&(
            "retained-source-identity-v1",
            &p.identity_family,
            &p.namespace_key,
            &p.report_source,
            &record.dedup_key,
        ))?;
        if let Some(zcode) = zcode {
            let source_id = record
                .dedup_key
                .strip_prefix("zcode-sqlite:")
                .ok_or_else(|| reject("SOURCE_RECORD_KEY_CONFLICT"))?;
            if let Some(row) = zcode.get(source_id) {
                match zcode_completed(row, record) {
                    Ok(completed) => {
                        projected.push(json!([key, row.hash, completed]));
                        completed_times.insert(key.clone(), completed);
                    }
                    Err(LedgerError::InvalidInput(why))
                        if why == "SOURCE_PROJECTION_FIELDS_MISSING" =>
                    {
                        incomplete_projection = Some(why);
                    }
                    Err(error) => return Err(error),
                }
            } else {
                incomplete_projection = Some("SOURCE_PROJECTION_MISSING".into());
            }
        }
        let metrics =
            legacy::legacy_metrics(record).map_err(|_| LedgerError::InvalidUsageNumber)?;
        if record.cost_usd.is_some_and(|n| !n.is_finite() || n < 0.0) {
            return Err(LedgerError::InvalidUsageNumber);
        }
        let cost = collection::legacy_cost(record.cost_usd);
        let member = TimeMember {
            member_key: key,
            occurred_ms: record.ts_ms,
            metrics,
            cost_units: cost.map(|c| c.0),
            cost_scale: cost.map(|c| c.1),
        };
        let content_hash = time_allocation::digest(&(
            "retained-source-content-v1",
            &p.report_source,
            &p.model_key,
            record,
        ))?;
        members.push(RevisionMember {
            member,
            content_hash,
        });
    }
    members.sort_by(|a, b| a.member.member_key.cmp(&b.member.member_key));
    if members.is_empty() {
        return Err(reject("NO_RETAINED_MEMBERS"));
    }
    let mut used_completion_boundary = false;
    let plain: Vec<_> = members.iter().map(|m| m.member.clone()).collect();
    let mut validation = time_allocation::validate_members(&p, &plain);
    if matches!(validation, Err(LedgerError::CorruptLedger)) && zcode.is_some() {
        if let Some(why) = incomplete_projection {
            return Err(reject(&why));
        }
        let cutoff = policy["end_ms"]
            .as_i64()
            .ok_or(LedgerError::CorruptLedger)?;
        // Completion is a deterministic source-specific selection contract,
        // only needed when the full retained set is not the adopted amount.
        members.retain(|item| {
            let keep = completed_times
                .get(&item.member.member_key)
                .is_some_and(|done| *done < cutoff);
            if !keep {
                excluded.push(item.member.member_key.clone());
            }
            keep
        });
        used_completion_boundary = true;
        let plain: Vec<_> = members.iter().map(|m| m.member.clone()).collect();
        validation = if plain.is_empty() {
            Err(LedgerError::CorruptLedger)
        } else {
            time_allocation::validate_members(&p, &plain)
        };
    }
    match validation {
        Ok(()) => {}
        Err(LedgerError::CorruptLedger) => return Err(reject("PARENT_METRICS_OR_COST_MISMATCH")),
        Err(e) => return Err(e),
    }
    let evidence = json!({"version":1,"quality":"reconstructed-source-report","source":p.report_source,"identity_family":p.identity_family,"namespace":p.namespace_key,"model":p.model_key,"timezone":timezone,"start_ms":start,"end_ms":end,"parent_identity":time_allocation::digest(&(&p.identity_family,&p.namespace_key,&p.candidate_key))?,"policy":policy,"source_capture_hash":cap.hash,"source_capture_count":cap.snapshot.entries.len(),"controls_hash":control,"selection_rule":if used_completion_boundary{"zcode-completed-before-cutoff-v1"}else{"complete-retained-request-interval-v1"},"source_projection_hash":time_allocation::digest(&projected)?,"excluded_after_completion_count":excluded.len(),"excluded_after_completion_hash":time_allocation::digest(&excluded)?,"normalization":"legacy-request-metrics-v1"});
    Ok(Candidate {
        parent_id: id,
        source: p.report_source,
        parent_hash: hash_bytes(&p.normalized_hash),
        policy_hash: time_allocation::digest(&policy)?,
        evidence,
        members,
    })
}

fn retained_members_unchanged(old: &Capture, current: &Capture, p: &Observation) -> Result<()> {
    let (start, end) = p
        .day_start_ms
        .zip(p.day_end_ms)
        .ok_or(LedgerError::CorruptLedger)?;
    let entries = old
        .by_model
        .get(&p.model_key)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let from = entries.partition_point(|(time, _)| *time < start);
    let until = entries.partition_point(|(time, _)| *time < end);
    for (_, index) in &entries[from..until] {
        let LegacyEntry::Request { record, .. } = &old.snapshot.entries[*index] else {
            return Err(LedgerError::CorruptLedger);
        };
        if current.identity_hashes.get(&record.dedup_key)
            != old.identity_hashes.get(&record.dedup_key)
        {
            return Err(reject("RETAINED_SOURCE_IDENTITY_CONFLICT"));
        }
    }
    // Also reject an old key moved into this bucket from another day/model.
    // Only genuinely new identities can supplement an incomplete saved set.
    let incoming = current
        .by_model
        .get(&p.model_key)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let from = incoming.partition_point(|(time, _)| *time < start);
    let until = incoming.partition_point(|(time, _)| *time < end);
    for (_, index) in &incoming[from..until] {
        let LegacyEntry::Request { record, .. } = &current.snapshot.entries[*index] else {
            return Err(LedgerError::CorruptLedger);
        };
        if let Some(previous) = old.identity_hashes.get(&record.dedup_key) {
            if current.identity_hashes.get(&record.dedup_key) != Some(previous) {
                return Err(reject("RETAINED_SOURCE_IDENTITY_CONFLICT"));
            }
        }
    }
    Ok(())
}

fn batch(c: &Connection, limit: usize, background: bool) -> Result<Batch> {
    if !(1..=MAX_BATCH).contains(&limit) {
        return Err(reject("time repair limit must be between 1 and 128"));
    }
    let (uuid, epoch): (String, String) =
        c.query_row("SELECT ledger_uuid,epoch FROM ledger_meta", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?;
    let generation: Option<String> = c
        .query_row(
            "SELECT generation FROM legacy_capture_state WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let metadata: Option<String> = c
        .query_row(
            "SELECT metadata_json FROM legacy_capture_state WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let metadata: Value = metadata
        .map(|s| serde_json::from_str(&s).map_err(|_| LedgerError::CorruptLedger))
        .transpose()?
        .unwrap_or(Value::Null);
    let sources:Vec<String>=c.prepare(&format!("SELECT DISTINCT report_source FROM usage_observations WHERE observation_id IN({PENDING}) ORDER BY report_source"))?.query_map([],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
    let mut basis = BTreeMap::new();
    for source in sources {
        let status = &metadata["sources"][&source];
        let source_state = json!([
            status["stale"],
            status["error"],
            status["unaccounted_rows"],
            status["unallocated_sessions"],
            status["last_scanned_ms"].is_number()
        ]);
        let source_hash = metadata["source_content_hashes"][&source]
            .as_str()
            .map(str::to_owned)
            .or_else(|| generation.clone());
        let projection = if source == "zcode" {
            let states:Vec<(String,String,i64,Option<String>,String)>=c.prepare("SELECT root_id,epoch,complete,error_code,capability_version FROM source_projection_scopes WHERE report_source='zcode' ORDER BY root_id")?.query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?.collect::<rusqlite::Result<_>>()?;
            let rows:Vec<(String,String,String)>=c.prepare("SELECT r.root_id,r.row_key,r.row_hash FROM source_projection_rows r JOIN source_projection_scopes s USING(root_id) WHERE s.report_source='zcode' AND r.table_name='model_usage' ORDER BY r.root_id,r.row_key LIMIT 100001")?.query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?.collect::<rusqlite::Result<_>>()?;
            time_allocation::digest(&(states, rows))?
        } else {
            String::new()
        };
        basis.insert(
            source.clone(),
            time_allocation::digest(&(
                "time-repair-attempt-v2",
                &epoch,
                source_hash,
                source_state,
                projection,
                source_control_basis(c, &source)?,
            ))?,
        );
    }
    // Only changed evidence for this source releases a failed attempt. Routine
    // scans and unrelated source captures do not force another reconstruction.
    let sql=format!("{PENDING} AND NOT EXISTS(SELECT 1 FROM time_repair_attempts t WHERE t.parent_observation_id=o.observation_id AND t.basis=json_extract(?2,'$.'||o.report_source)) ORDER BY o.observation_id LIMIT ?1");
    let ids: Vec<i64> = c
        .prepare(&sql)?
        .query_map(
            params![
                limit as i64 + 1,
                serde_json::to_string(&basis).map_err(|_| LedgerError::RecordTooLarge)?
            ],
            |r| r.get(0),
        )?
        .collect::<rusqlite::Result<_>>()?;
    let mut has_more = ids.len() > limit;
    let started = Instant::now();
    let mut member_count = 0;
    let mut candidates = Vec::new();
    let mut skipped = Vec::new();
    let mut captures = BTreeMap::<String, std::result::Result<Capture, String>>::new();
    let mut current_captures = BTreeMap::<String, std::result::Result<Capture, String>>::new();
    for id in ids.into_iter().take(limit) {
        if background
            && (!candidates.is_empty() || !skipped.is_empty())
            && started.elapsed() > Duration::from_secs(3)
        {
            has_more = true;
            break;
        }
        let source: String = c.query_row(
            "SELECT report_source FROM usage_observations WHERE observation_id=?1",
            [id],
            |r| r.get(0),
        )?;
        if !captures.contains_key(&source) {
            let saved = match capture(c, &time_allocation::parent(c, id)?, true) {
                Ok(cap) => Ok(cap),
                Err(e) => Err(reason(e)?),
            };
            captures.insert(source.clone(), saved);
        }
        let result: std::result::Result<Candidate, String> = match captures
            .get(&source)
            .ok_or(LedgerError::CorruptLedger)?
        {
            Ok(cap) => match candidate(c, id, cap) {
                Ok(candidate) => Ok(candidate),
                Err(error) => {
                    let failure = reason(error)?;
                    if cap.saved
                        && matches!(
                            failure.as_str(),
                            "PARENT_METRICS_OR_COST_MISMATCH"
                                | "NO_RETAINED_MEMBERS"
                                | "SOURCE_PROJECTION_MISSING"
                                | "SOURCE_PROJECTION_FIELDS_MISSING"
                        )
                    {
                        let p = time_allocation::parent(c, id)?;
                        if !current_captures.contains_key(&source) {
                            current_captures.insert(
                                source.clone(),
                                match capture(c, &p, false) {
                                    Ok(current) => Ok(current),
                                    Err(e) => Err(reason(e)?),
                                },
                            );
                        }
                        match current_captures
                            .get(&source)
                            .ok_or(LedgerError::CorruptLedger)?
                        {
                            Ok(current) => match retained_members_unchanged(cap, current, &p)
                                .and_then(|()| candidate(c, id, current))
                            {
                                Ok(mut candidate) => {
                                    candidate.evidence["upgrade_capture_hash"] = json!(cap.hash);
                                    candidate.evidence["upgrade_members_unchanged"] = json!(true);
                                    Ok(candidate)
                                }
                                Err(e) => Err(reason(e)?),
                            },
                            Err(why) => Err(why.clone()),
                        }
                    } else {
                        Err(failure)
                    }
                }
            },
            Err(why) => Err(why.clone()),
        };
        match result {
            Ok(candidate) => {
                if member_count + candidate.members.len() > 100_000 && !candidates.is_empty() {
                    has_more = true;
                    break;
                }
                member_count += candidate.members.len();
                candidates.push(candidate);
            }
            Err(why) => skipped.push(Skipped {
                parent_observation_id: id,
                source,
                reason: why,
            }),
        }
    }
    let token = time_allocation::digest(&(
        "time-repair-preview-v1",
        uuid,
        epoch,
        limit,
        &candidates,
        &skipped,
    ))?;
    Ok(Batch {
        candidates,
        skipped,
        token,
        basis,
        has_more,
    })
}
fn summarize(batch: &Batch, applied: bool) -> Value {
    json!({"version":1,"provenance":PROVENANCE,"preview_token":batch.token,"candidates":batch.candidates.len(),"repaired":if applied{batch.candidates.len()}else{0},"skipped":batch.skipped,"has_more":batch.has_more,"applied":applied,"members":batch.candidates.iter().map(|c|c.members.len()).sum::<usize>()})
}
fn publish(c: &Connection, batch: &Batch) -> Result<()> {
    for candidate in &batch.candidates {
        let mut insert=c.prepare("INSERT INTO durable_time_revision_members(parent_observation_id,member_key,content_hash,occurred_ms,metrics_json,cost_units,cost_scale) VALUES(?1,?2,?3,?4,?5,?6,?7)")?;
        for item in &candidate.members {
            let member = &item.member;
            insert.execute(params![
                candidate.parent_id,
                member.member_key,
                item.content_hash,
                member.occurred_ms,
                serde_json::to_string(&member.metrics).map_err(|_| LedgerError::RecordTooLarge)?,
                member.cost_units,
                member.cost_scale
            ])?;
        }
        c.execute(
            "INSERT INTO durable_time_revisions VALUES(?1,1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                candidate.parent_id,
                PROVENANCE,
                candidate.members.len() as i64,
                candidate.parent_hash,
                candidate.policy_hash,
                candidate.evidence.to_string(),
                candidate.manifest()?,
                now_ms()
            ],
        )?;
        c.execute(
            "DELETE FROM time_repair_attempts WHERE parent_observation_id=?1",
            [candidate.parent_id],
        )?;
    }
    for skipped in &batch.skipped {
        c.execute("INSERT INTO time_repair_attempts VALUES(?1,?2,?3,?4) ON CONFLICT(parent_observation_id) DO UPDATE SET basis=excluded.basis,attempted_ms=excluded.attempted_ms,reason=excluded.reason",params![skipped.parent_observation_id,batch.basis.get(&skipped.source).ok_or(LedgerError::CorruptLedger)?,now_ms(),skipped.reason])?;
    }
    // Successful sources no longer need disposable upgrade staging. The sealed
    // revision carries member/content fingerprints and all time/amount fields.
    if !batch.candidates.is_empty() {
        c.execute(&format!("DELETE FROM time_repair_saved_sources WHERE report_source NOT IN (SELECT report_source FROM usage_observations WHERE observation_id IN({PENDING}))"),[])?;
    }
    if !batch.candidates.is_empty() {
        db::bump(c)?;
    }
    if !batch.has_more {
        let pending: bool = c.query_row(&format!("SELECT EXISTS({PENDING})"), [], |r| r.get(0))?;
        if !pending {
            c.execute("INSERT INTO ledger_scheduler(key,value) SELECT ?1,json_array(epoch,(SELECT COALESCE(MAX(selected_observation_id),0) FROM adoption_items)) FROM ledger_meta WHERE true ON CONFLICT(key) DO UPDATE SET value=excluded.value",[COMPLETE_KEY])?;
        }
    }
    Ok(())
}
impl Ledger {
    /// Read a stable, bounded preview exclusively from this ledger's saved
    /// evidence. No source filesystem access or amount mutation occurs.
    pub fn preview_time_repair(&self, limit: usize) -> Result<Value> {
        let tx = self.conn.unchecked_transaction()?;
        let batch = batch(&tx, limit, false)?;
        Ok(summarize(&batch, false))
    }
    pub fn apply_time_repair(&mut self, preview_token: &str, limit: usize) -> Result<Value> {
        self.require_writer()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let batch = batch(&tx, limit, false)?;
        if batch.token != preview_token {
            return Err(LedgerError::StaleObservation);
        }
        publish(&tx, &batch)?;
        tx.commit()?;
        Ok(summarize(&batch, true))
    }
    /// Background upgrade repair is incremental and retry-bounded. Unchanged
    /// failed evidence is not reparsed every collector tick.
    pub fn repair_pending_time_allocations(&mut self, limit: usize) -> Result<Value> {
        self.require_writer()?;
        if !(1..=MAX_BATCH).contains(&limit) {
            return Err(reject("time repair limit must be between 1 and 128"));
        }
        // The common steady-state path does not obtain a SQLite writer lock or
        // walk old adopted history. The high-water mark is indexed by the PK.
        let complete:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM ledger_scheduler s CROSS JOIN ledger_meta m WHERE s.key=?1 AND json_valid(s.value) AND json_extract(s.value,'$[0]')=m.epoch AND json_extract(s.value,'$[1]')=(SELECT COALESCE(MAX(selected_observation_id),0) FROM adoption_items))",[COMPLETE_KEY],|r|r.get(0))?;
        if complete {
            return Ok(
                json!({"version":1,"provenance":PROVENANCE,"candidates":0,"repaired":0,"skipped":[],"has_more":false,"applied":false,"members":0}),
            );
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let batch = batch(&tx, limit, true)?;
        publish(&tx, &batch)?;
        tx.commit()?;
        Ok(summarize(&batch, true))
    }
}

/// SQLite-only sealed projection. Its capture can subsequently disappear;
/// reads never consult, rebuild, or change that mutable evidence.
pub(crate) fn load(c: &Connection, parent_id: i64) -> Result<Option<Vec<TimeMember>>> {
    let header:Option<(i64,String,i64,String,String,String,String)>=c.query_row("SELECT version,provenance,member_count,parent_hash,policy_hash,evidence_json,manifest_hash FROM durable_time_revisions WHERE parent_observation_id=?1",[parent_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional()?;
    let Some((version, provenance, count, parent_hash, policy_hash, evidence, manifest)) = header
    else {
        return Ok(None);
    };
    if version != 1
        || provenance != PROVENANCE
        || !(1..=time_allocation::MAX_MEMBERS as i64).contains(&count)
        || evidence.len() > 262144
    {
        return Err(LedgerError::CorruptLedger);
    }
    let p = time_allocation::parent(c, parent_id)?;
    let both: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM durable_time_allocations WHERE parent_observation_id=?1)",
        [parent_id],
        |r| r.get(0),
    )?;
    if both
        || parent_hash != hash_bytes(&p.normalized_hash)
        || policy_hash != time_allocation::digest(&policy(c, parent_id)?)?
    {
        return Err(LedgerError::CorruptLedger);
    }
    let evidence: Value =
        serde_json::from_str(&evidence).map_err(|_| LedgerError::CorruptLedger)?;
    let mut stmt=c.prepare("SELECT member_key,content_hash,occurred_ms,metrics_json,cost_units,cost_scale FROM durable_time_revision_members WHERE parent_observation_id=?1 ORDER BY member_key LIMIT ?2")?;
    let mut rows = stmt.query(params![parent_id, time_allocation::MAX_MEMBERS as i64 + 1])?;
    let mut members = Vec::with_capacity(count as usize);
    while let Some(r) = rows.next()? {
        let content_hash: String = r.get(1)?;
        let encoded: String = r.get(3)?;
        if content_hash.len() != 64
            || !content_hash.bytes().all(|b| b.is_ascii_hexdigit())
            || encoded.len() > 512
        {
            return Err(LedgerError::CorruptLedger);
        }
        members.push(RevisionMember {
            member: TimeMember {
                member_key: r.get(0)?,
                occurred_ms: r.get(2)?,
                metrics: serde_json::from_str(&encoded).map_err(|_| LedgerError::CorruptLedger)?,
                cost_units: r.get(4)?,
                cost_scale: r.get(5)?,
            },
            content_hash,
        });
    }
    let candidate = Candidate {
        parent_id,
        source: p.report_source.clone(),
        parent_hash,
        policy_hash,
        evidence,
        members,
    };
    if candidate.members.len() != count as usize || candidate.manifest()? != manifest {
        return Err(LedgerError::CorruptLedger);
    }
    let members: Vec<_> = candidate.members.into_iter().map(|m| m.member).collect();
    time_allocation::validate_members(&p, &members)?;
    Ok(Some(members))
}
pub(crate) fn check(c: &Connection) -> Result<()> {
    let invalid_staging:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM time_repair_saved_sources s WHERE s.entry_count<>(SELECT COUNT(*) FROM time_repair_saved_entries e WHERE e.report_source=s.report_source) OR s.ledger_uuid<>(SELECT ledger_uuid FROM ledger_meta) OR json_type(s.metadata_json,'$.entries')<>'array' OR json_array_length(s.metadata_json,'$.entries')<>0)",[],|r|r.get(0))?;
    if invalid_staging {
        return Err(LedgerError::CorruptLedger);
    }
    let mut stmt = c.prepare(
        "SELECT parent_observation_id FROM durable_time_revisions ORDER BY parent_observation_id",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        load(c, row.get(0)?)?.ok_or(LedgerError::CorruptLedger)?;
    }
    Ok(())
}
pub(crate) fn validate_restore(current: &Connection, staging: &Connection) -> Result<()> {
    let mut stmt=current.prepare("SELECT h.parent_observation_id,h.manifest_hash,o.identity_family,o.namespace_key,o.candidate_key,o.normalized_hash FROM durable_time_revisions h JOIN usage_observations o ON o.observation_id=h.parent_observation_id JOIN adoption_items i ON i.selected_observation_id=o.observation_id JOIN adoption_policies p USING(policy_id) WHERE p.status='active'")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        load(current, r.get(0)?)?.ok_or(LedgerError::CorruptLedger)?;
        let present:bool=staging.query_row("SELECT EXISTS(SELECT 1 FROM durable_time_revisions h JOIN usage_observations o ON o.observation_id=h.parent_observation_id JOIN adoption_items i ON i.selected_observation_id=o.observation_id JOIN adoption_policies p USING(policy_id) WHERE p.status='active' AND h.manifest_hash=?1 AND o.identity_family=?2 AND o.namespace_key=?3 AND o.candidate_key=?4 AND o.normalized_hash=?5)",params![r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,Vec<u8>>(5)?],|row|row.get(0))?;
        if !present {
            return Err(LedgerError::IncompleteEvidence);
        }
    }
    Ok(())
}
pub(crate) fn status(c: &Connection) -> Result<Value> {
    let repaired: i64 = c.query_row("SELECT COUNT(*) FROM durable_time_revisions", [], |r| {
        r.get(0)
    })?;
    let remaining: i64 = c.query_row(&format!("SELECT COUNT(*) FROM ({PENDING})"), [], |r| {
        r.get(0)
    })?;
    let skipped: i64 = c.query_row(
        &format!(
            "SELECT COUNT(*) FROM time_repair_attempts WHERE parent_observation_id IN ({PENDING})"
        ),
        [],
        |r| r.get(0),
    )?;
    let reasons:Vec<(String,i64)>=c.prepare(&format!("SELECT reason,COUNT(*) FROM time_repair_attempts WHERE parent_observation_id IN ({PENDING}) GROUP BY reason ORDER BY reason"))?.query_map([],|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
    Ok(
        json!({"version":1,"state":if remaining==0{"complete"}else if remaining==skipped{"waiting_evidence"}else{"pending"},"repaired":repaired,"remaining":remaining,"skipped":skipped,"reasons":reasons,"has_more":remaining>skipped}),
    )
}
