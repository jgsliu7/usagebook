//! Typed, process-local bridge for sources that have not entered the durable ledger.
//! Never serializes a synthetic midnight request for native daily evidence.
use super::query_types::{QueryError, QueryResult};
use crate::model::{Caliber, ScanStats, UsageRecord};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyDay {
    pub row_key: String,
    pub report_source: String,
    pub native_tool: String,
    pub namespace: Option<String>,
    pub model_key: String,
    pub model_display: String,
    pub native_date: String,
    pub timezone: Option<String>,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub projection: [Option<i64>; 10],
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LegacyEntry {
    Request {
        record: UsageRecord,
        native_tool: String,
        namespace: Option<String>,
        known_fields: u16,
    },
    Day(LegacyDay),
    SessionTotal {
        record: UsageRecord,
        known_fields: u16,
        /// Explicit nullable precision, including unknown request count/date/model.
        projection: Option<[Option<i64>; 10]>,
        reason: Option<String>,
    },
}
impl LegacyEntry {
    pub fn report_source(&self) -> &str {
        match self {
            Self::Request { record, .. } | Self::SessionTotal { record, .. } => record.source,
            Self::Day(day) => &day.report_source,
        }
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LegacySourceStatus {
    pub last_scanned_ms: Option<i64>,
    pub stale: bool,
    pub error: Option<String>,
    /// Diagnostic day rows only; never a request count or an amount to add.
    pub unaccounted_rows: usize,
    /// Readable historical evidence that cannot be allocated to request windows.
    pub unallocated_sessions: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacySnapshot {
    pub generation: String,
    pub routing_hash: String,
    pub entries: Vec<LegacyEntry>,
    pub sources: BTreeMap<String, LegacySourceStatus>,
    // Private availability evidence. Never exposed in query content/status.
    // A complete empty scan also remembers its inputs across process restarts.
    input_containers: BTreeMap<String, Vec<InputContainer>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct InputContainer {
    path: PathBuf,
    directory: bool,
}

/// Exact scopes supported by compatibility captures. Durable identity keys are
/// deliberately not inferred from legacy session IDs or model/time similarity.
type CapturePrivacyRule = (
    String,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
);
pub(crate) struct CapturePrivacy {
    rules: Vec<CapturePrivacyRule>,
    adoption_ranges: Vec<(String, i64, i64)>,
}
impl CapturePrivacy {
    pub(crate) fn load(conn: &rusqlite::Connection) -> QueryResult<Self> {
        let rules = conn.prepare("SELECT kind,report_source,native_tool,start_ms,end_ms FROM suppression_rules WHERE kind IN('source','window')")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let adoption_ranges = conn.prepare("SELECT consumer_tool,start_ms,end_ms FROM adoption_forget_ranges WHERE identity_family IS NULL")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(Self {
            rules,
            adoption_ranges,
        })
    }
    pub(crate) fn suppresses(
        &self,
        conn: &rusqlite::Connection,
        entry: &LegacyEntry,
    ) -> QueryResult<bool> {
        let (native, time, start, end) = match entry {
            LegacyEntry::Request {
                record,
                native_tool,
                ..
            } => (
                native_tool.as_str(),
                (record.ts_ms > 0).then_some(record.ts_ms),
                None,
                None,
            ),
            LegacyEntry::SessionTotal { record, .. } => (
                record.source,
                (record.ts_ms > 0).then_some(record.ts_ms),
                None,
                None,
            ),
            LegacyEntry::Day(day) => (day.native_tool.as_str(), None, day.start_ms, day.end_ms),
        };
        let overlaps = |a: i64, b: i64| {
            time.is_some_and(|t| t >= a && t < b)
                || start.zip(end).is_some_and(|(x, y)| x < b && y > a)
        };
        let matching = |source: &Option<String>, tool: &Option<String>| {
            source.as_deref().is_none_or(|s| s == entry.report_source())
                && tool.as_deref().is_none_or(|t| t == native)
        };
        if self
            .rules
            .iter()
            .any(|(kind, source, tool, _, _)| kind == "source" && matching(source, tool))
        {
            return Ok(true);
        }
        let complete_day = |a: i64, b: i64| -> QueryResult<()> {
            if start.zip(end).is_some_and(|(x, y)| a > x || b < y) {
                return Err(QueryError::new("PRIVACY_SCOPE_INCOMPLETE", "saved daily summaries are indivisible; privacy controls cover only part of this native day"));
            }
            Ok(())
        };
        for (_, source, tool, a, b) in &self.rules {
            if !matching(source, tool) {
                continue;
            }
            if let Some((a, b)) = a.zip(*b).filter(|(a, b)| overlaps(*a, *b)) {
                complete_day(a, b)?;
                return Ok(true);
            }
        }
        for (tool, a, b) in &self.adoption_ranges {
            if tool == native && overlaps(*a, *b) {
                complete_day(*a, *b)?;
                return Ok(true);
            }
        }
        if let LegacyEntry::Day(day) = entry {
            if !self.adoption_ranges.is_empty() {
                return super::adoption::suppresses_unbound_day(
                    conn,
                    &day.native_tool,
                    &day.native_date,
                )
                .map_err(|e| {
                    QueryError::new(e.code(), "cannot apply saved native-day privacy controls")
                });
            }
        }
        Ok(false)
    }
    pub(crate) fn validate(
        &self,
        conn: &rusqlite::Connection,
        snapshot: &LegacySnapshot,
    ) -> QueryResult<()> {
        if self.rules.is_empty() && self.adoption_ranges.is_empty() {
            return Ok(());
        }
        for entry in &snapshot.entries {
            if self.suppresses(conn, entry)? {
                return Err(QueryError::new(
                    "PRIVACY_FILTER_REQUIRED",
                    "saved compatibility capture contains a forgotten source or time range",
                ));
            }
        }
        Ok(())
    }
}

// Compatibility rows predate native identity metadata. In particular, Claude's
// fallback key starts with the session, whereas the complete key starts with
// the message ID. Do not promote fallback identities to independent requests.
fn claude_identity(record: &UsageRecord) -> Option<(&str, &str)> {
    if record.session_id.is_empty()
        || record
            .dedup_key
            .starts_with(&format!("{}:", record.session_id))
    {
        return None;
    }
    let (message, request) = record.dedup_key.split_once(':')?;
    if message.is_empty() || request.is_empty() || request.contains(':') {
        return None;
    }
    Some((message, request))
}

fn claude_possible_message(record: &UsageRecord) -> Option<&str> {
    if let Some((message, _)) = claude_identity(record) {
        return Some(message);
    }
    let suffix = record
        .dedup_key
        .strip_prefix(&format!("{}:", record.session_id))?;
    if suffix.is_empty() || suffix.contains(':') {
        None
    } else {
        Some(suffix)
    }
}

// A fallback key is only usable after independently persisted native evidence
// proves one conflict-free identity and exactly the same numeric projection.
// Looking up a hash alone, matching totals, or assuming a session-local message
// ID is unique cannot establish this relationship.
fn proven_claude_fallback(conn: &rusqlite::Connection, entry: &LegacyEntry) -> QueryResult<bool> {
    use rusqlite::OptionalExtension;
    let LegacyEntry::Request {
        record,
        native_tool,
        namespace,
        known_fields,
    } = entry
    else {
        return Ok(false);
    };
    if record.session_id.is_empty()
        || native_tool != "claude"
        || namespace.is_some()
        || *known_fields != 0
        || record.cost_usd.is_some()
        || record.reasoning_output != 0
        || record.request_count != 1
    {
        return Ok(false);
    }
    let Some(message) = claude_possible_message(record) else {
        return Ok(false);
    };
    if claude_identity(record).is_some() {
        return Ok(false);
    }
    let numbers: Option<Vec<i64>> = [
        record.input_uncached,
        record.cache_read,
        record.cache_write,
        record.output,
        record.cache_write_1h,
    ]
    .into_iter()
    .map(|v| i64::try_from(v).ok())
    .collect();
    let Some(numbers) = numbers else {
        return Ok(false);
    };
    let identity = super::claude::identity_key("request/fallback", &[&record.session_id, message]);
    let id: Option<i64> = conn.query_row(
        "SELECT o.observation_id FROM usage_observations o
         JOIN usage_events e ON e.identity_family=o.identity_family AND e.namespace_key=o.namespace_key AND e.identity_key=o.identity_key AND e.selected_observation_id=o.observation_id
         WHERE o.identity_family='claude' AND o.namespace_key='unknown:claude' AND o.identity_key=?1
           AND o.report_source='claude' AND o.native_tool='claude' AND o.fact_kind='request' AND o.action='upsert'
           AND o.cost_state='unknown'
           AND o.native_session_id=?2 AND o.native_message_id=?3 AND o.native_request_id IS NULL
           AND e.state='active' AND e.has_conflict=0 AND o.eligibility='eligible'
           AND o.occurred_ms=?4 AND o.model_display=?5 AND o.input_uncached=?6 AND o.cache_read=?7
           AND o.cache_write=?8 AND o.output_total=?9 AND o.cache_write_1h=?10 AND o.request_count=1
           AND NOT EXISTS(SELECT 1 FROM identity_alias x WHERE x.identity_family='claude' AND x.namespace_key='unknown:claude' AND x.canonical_key=o.identity_key AND x.alias_key<>o.identity_key)
           AND NOT EXISTS(SELECT 1 FROM identity_alias x WHERE x.identity_family='claude' AND x.namespace_key='unknown:claude' AND x.alias_key=o.identity_key AND x.canonical_key<>o.identity_key)
           AND NOT EXISTS(SELECT 1 FROM usage_observations x WHERE x.identity_family='claude' AND x.namespace_key='unknown:claude' AND x.native_message_id=?3 AND (x.native_session_id IS NOT ?2 OR x.native_request_id IS NOT NULL OR x.identity_key IS NOT ?1))
         LIMIT 1",
        rusqlite::params![identity, record.session_id, message, record.ts_ms, record.model, numbers[0], numbers[1], numbers[2], numbers[3], numbers[4]], |r| r.get(0)).optional()?;
    let Some(id) = id else {
        return Ok(false);
    };
    let observation = conn.query_row(
        "SELECT * FROM usage_observations WHERE observation_id=?1",
        [id],
        super::resolve::load_observation,
    )?;
    let suppressed = super::ingest::exact_suppressed(conn, &observation)
        .map_err(|e| QueryError::new(e.code(), "cannot apply Claude identity deletion controls"))?;
    let linked: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM suppression_rules WHERE identity_family='claude' AND namespace_key='unknown:claude' AND ((kind='identity_link' AND link_version=1 AND link_key=?1) OR (kind='session' AND session_key=?2)))", rusqlite::params![super::claude::suppression_link(message), observation.session_key], |r| r.get(0))?;
    Ok(!suppressed && !linked)
}

/// Monotonic numbers alone do not prove that an already displayed request was
/// still streaming. Require native evidence for both versions and reject an
/// old value that has also been reported as terminal.
fn proven_claude_progress(
    conn: &rusqlite::Connection,
    old: &UsageRecord,
    new: &UsageRecord,
) -> QueryResult<bool> {
    let (message, request) = if let Some((message, request)) = claude_identity(new) {
        (message, Some(request))
    } else if let Some(message) = claude_possible_message(new) {
        (message, None)
    } else {
        return Ok(false);
    };
    if old.dedup_key != new.dedup_key
        || old.session_id != new.session_id
        || new.ts_ms < old.ts_ms
        || old.cost_usd.is_some()
        || new.cost_usd.is_some()
        || old.reasoning_output != 0
        || new.reasoning_output != 0
        || old.request_count != 1
        || new.request_count != 1
    {
        return Ok(false);
    }
    let identity = match request {
        Some(request) => super::claude::identity_key("request/full", &[message, request]),
        None => super::claude::identity_key("request/fallback", &[&new.session_id, message]),
    };
    let conflict: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM identity_alias WHERE identity_family='claude' AND namespace_key='unknown:claude' AND ((alias_key=?1 AND canonical_key<>?1) OR (canonical_key=?1 AND alias_key<>?1))) OR EXISTS(SELECT 1 FROM usage_observations o WHERE o.identity_family='claude' AND o.namespace_key='unknown:claude' AND o.native_message_id=?2 AND (o.native_session_id IS NOT ?3 OR o.native_request_id IS NOT ?4 OR o.identity_key IS NOT ?1)) OR EXISTS(SELECT 1 FROM usage_observations o WHERE o.identity_family='claude' AND o.namespace_key='unknown:claude' AND o.native_request_id=?4 AND o.native_message_id IS NOT ?2)", rusqlite::params![identity, message, new.session_id, request], |r| r.get(0))?;
    if conflict {
        return Ok(false);
    }
    let matches_version = |record: &UsageRecord,
                           eligibility: &str,
                           require_time: bool|
     -> QueryResult<bool> {
        let values: Option<Vec<i64>> = [
            record.input_uncached,
            record.cache_read,
            record.cache_write,
            record.output,
            record.cache_write_1h,
        ]
        .into_iter()
        .map(|v| i64::try_from(v).ok())
        .collect();
        let Some(values) = values else {
            return Ok(false);
        };
        Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM usage_observations o WHERE o.identity_family='claude' AND o.namespace_key='unknown:claude' AND o.identity_key=?1 AND o.report_source='claude' AND o.native_tool='claude' AND o.fact_kind='request' AND o.action='upsert' AND o.cost_state='unknown' AND o.native_session_id=?2 AND o.native_message_id=?3 AND o.native_request_id IS ?4 AND o.eligibility=?5 AND (?6=0 OR o.occurred_ms=?7) AND (?6<>0 OR ?5<>'pending_finality' OR o.occurred_ms<=?14) AND o.model_display=?8 AND o.input_uncached=?9 AND o.cache_read=?10 AND o.cache_write=?11 AND o.output_total=?12 AND (o.cache_write_1h=?13 OR (o.cache_write_1h IS NULL AND o.cache_write=0 AND ?13=0)) AND o.request_count=1)", rusqlite::params![identity,new.session_id,message,request,eligibility,require_time,record.ts_ms,record.model,values[0],values[1],values[2],values[3],values[4],new.ts_ms], |r|r.get(0))?)
    };
    // The display retains the first adoption time even after successive stream
    // updates. Later native versions therefore need not have that timestamp;
    // their full identity, model and token components must still match.
    if !matches_version(old, "pending_finality", false)? || matches_version(old, "eligible", false)?
    {
        return Ok(false);
    }
    let new_final = matches_version(new, "eligible", true)?;
    if !matches_version(new, "pending_finality", true)? && !new_final {
        return Ok(false);
    }
    // Any terminal version for this identity constrains the entire lifecycle,
    // not just versions equal to the old displayed value. A later pending 60
    // cannot supersede an already observed final 100.
    let final_exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM usage_observations WHERE identity_family='claude' AND namespace_key='unknown:claude' AND identity_key=?1 AND fact_kind='request' AND eligibility='eligible' AND action='upsert')", [&identity], |r| r.get(0))?;
    if final_exists {
        if !new_final {
            return Ok(false);
        }
        let values: Option<Vec<i64>> = [
            new.input_uncached,
            new.cache_read,
            new.cache_write,
            new.output,
            new.cache_write_1h,
        ]
        .into_iter()
        .map(|v| i64::try_from(v).ok())
        .collect();
        let Some(values) = values else {
            return Ok(false);
        };
        let conflicting_final: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM usage_observations o WHERE o.identity_family='claude' AND o.namespace_key='unknown:claude' AND o.identity_key=?1 AND o.fact_kind='request' AND o.eligibility='eligible' AND o.action='upsert' AND (o.model_display IS NOT ?2 OR o.input_uncached IS NOT ?3 OR o.cache_read IS NOT ?4 OR o.cache_write IS NOT ?5 OR o.output_total IS NOT ?6 OR NOT COALESCE(o.cache_write_1h IS ?7 OR (o.cache_write_1h IS NULL AND o.cache_write=0 AND ?7=0),0) OR o.request_count IS NOT 1 OR o.cost_state<>'unknown'))", rusqlite::params![identity,new.model,values[0],values[1],values[2],values[3],values[4]], |r|r.get(0))?;
        if conflicting_final {
            return Ok(false);
        }
    }
    let denied: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM suppression_rules WHERE identity_family='claude' AND namespace_key='unknown:claude' AND ((kind='identity' AND identity_key=?1) OR (kind='identity_link' AND link_version=1 AND link_key=?2) OR (kind='session' AND session_key=?3)))", rusqlite::params![identity,super::claude::suppression_link(message),super::claude::identity_key("session", &[&new.session_id])], |r| r.get(0))?;
    Ok(!denied)
}

fn claude_pending(
    conn: &rusqlite::Connection,
    entry: &LegacyEntry,
    reason: &str,
) -> QueryResult<()> {
    let encoded = serde_json::to_string(entry)
        .map_err(|_| QueryError::invalid("invalid Claude candidate"))?;
    let mut hash = Sha256::new();
    hash.update(b"usagebook/claude-pending/v1");
    hash.update((reason.len() as u64).to_be_bytes());
    hash.update(reason.as_bytes());
    hash.update((encoded.len() as u64).to_be_bytes());
    hash.update(encoded.as_bytes());
    let sql = "INSERT INTO claude_protection_pending(identity,entry_json,reason) VALUES(?1,?2,?3) ON CONFLICT(identity) DO NOTHING";
    conn.execute(
        sql,
        rusqlite::params![format!("{:x}", hash.finalize()), encoded, reason],
    )?;
    Ok(())
}

// One watermarked row per adopted request. Keep the displayed timestamp in
// the entry for privacy filtering; the actual source timestamp is in reason.
fn claude_approved_key(record: &UsageRecord) -> String {
    let mut hash = Sha256::new();
    hash.update(b"usagebook/claude-approved-successor/v1");
    for part in [
        record.source,
        record.session_id.as_str(),
        record.dedup_key.as_str(),
    ] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    format!("{:x}", hash.finalize())
}

fn claude_approved_time(
    conn: &rusqlite::Connection,
    old: &LegacyEntry,
) -> QueryResult<Option<i64>> {
    use rusqlite::OptionalExtension;
    let LegacyEntry::Request { record, .. } = old else {
        return Ok(None);
    };
    let saved: Option<(String, String)> = conn
        .query_row(
            "SELECT reason,entry_json FROM claude_protection_pending WHERE identity=?1",
            [claude_approved_key(record)],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((reason, saved)) = saved {
        let Some(raw_time) = reason
            .strip_prefix("approved_successor/")
            .and_then(|s| s.parse::<i64>().ok())
        else {
            return Ok(None);
        };
        let current = serde_json::to_string(old)
            .map_err(|_| QueryError::invalid("invalid Claude baseline"))?;
        if saved != current || raw_time < record.ts_ms {
            return Ok(None);
        }
        return Ok(Some(raw_time));
    }
    // Previous versions of this protocol retained old projections but did not
    // record the adopted source time. Do not infer their last watermark from
    // the displayed anchor or from a different native observation.
    let previous_progress: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM claude_protection_pending WHERE reason='original_baseline' AND json_extract(entry_json,'$.Request.record.session_id')=?1 AND json_extract(entry_json,'$.Request.record.dedup_key')=?2)", rusqlite::params![record.session_id,record.dedup_key], |r| r.get(0))?;
    Ok((!previous_progress).then_some(record.ts_ms))
}

fn claude_set_approved(
    conn: &rusqlite::Connection,
    successor: &LegacyEntry,
    raw_time: i64,
) -> QueryResult<()> {
    let LegacyEntry::Request { record, .. } = successor else {
        return Err(QueryError::invalid("invalid Claude successor"));
    };
    let encoded = serde_json::to_string(successor)
        .map_err(|_| QueryError::invalid("invalid Claude successor"))?;
    let changed = conn.execute("INSERT INTO claude_protection_pending(identity,entry_json,reason) VALUES(?1,?2,?3) ON CONFLICT(identity) DO UPDATE SET entry_json=excluded.entry_json,reason=excluded.reason WHERE claude_protection_pending.reason GLOB 'approved_successor/*'", rusqlite::params![claude_approved_key(record),encoded,format!("approved_successor/{raw_time}")])?;
    if changed != 1 {
        return Err(QueryError::new(
            "CLAUDE_EVIDENCE_CONFLICT",
            "cannot save Claude successor watermark",
        ));
    }
    Ok(())
}

// Deleting an intermediate observation installs identity/message controls.
// Compatibility values are indivisible even if their anchors lie elsewhere.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ClaudeDeletionMatch {
    None,
    Exact,
    Uncertain,
}

#[derive(Default)]
struct ClaudeDeletionControls {
    identities: std::collections::BTreeSet<String>,
    links: std::collections::BTreeSet<String>,
    sessions: std::collections::BTreeSet<String>,
}
impl ClaudeDeletionControls {
    fn load(conn: &rusqlite::Connection) -> QueryResult<Self> {
        let mut controls = Self::default();
        let mut query = conn.prepare("SELECT kind,identity_key,link_key,session_key FROM suppression_rules WHERE identity_family='claude' AND namespace_key='unknown:claude' AND kind IN('identity','identity_link','session')")?;
        let rows = query.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        })?;
        for row in rows {
            let (kind, identity, link, session) = row?;
            match kind.as_str() {
                "identity" => {
                    controls.identities.insert(identity.ok_or_else(|| {
                        QueryError::new("LEDGER_UNREADABLE", "invalid Claude deletion control")
                    })?);
                }
                "identity_link" => {
                    controls.links.insert(link.ok_or_else(|| {
                        QueryError::new("LEDGER_UNREADABLE", "invalid Claude deletion control")
                    })?);
                }
                "session" => {
                    controls.sessions.insert(session.ok_or_else(|| {
                        QueryError::new("LEDGER_UNREADABLE", "invalid Claude deletion control")
                    })?);
                }
                _ => unreachable!(),
            }
        }
        Ok(controls)
    }
    fn is_empty(&self) -> bool {
        self.identities.is_empty() && self.links.is_empty() && self.sessions.is_empty()
    }
    fn classify(&self, entry: &LegacyEntry) -> ClaudeDeletionMatch {
        if self.is_empty() {
            return ClaudeDeletionMatch::None;
        }
        let record = match entry {
            LegacyEntry::Request { record, .. } => record,
            LegacyEntry::SessionTotal { record, .. } => {
                return if !self.links.is_empty()
                    || !self.identities.is_empty()
                    || record.session_id.is_empty()
                    || self.sessions.contains(&super::claude::identity_key(
                        "session",
                        &[&record.session_id],
                    )) {
                    ClaudeDeletionMatch::Uncertain
                } else {
                    ClaudeDeletionMatch::None
                };
            }
            LegacyEntry::Day(_) => return ClaudeDeletionMatch::Uncertain,
        };
        let Some(message) = claude_possible_message(record) else {
            return if !self.links.is_empty()
                || !self.identities.is_empty()
                || record.session_id.is_empty()
                || self.sessions.contains(&super::claude::identity_key(
                    "session",
                    &[&record.session_id],
                )) {
                ClaudeDeletionMatch::Uncertain
            } else {
                ClaudeDeletionMatch::None
            };
        };
        let identity = if let Some((message, request)) = claude_identity(record) {
            super::claude::identity_key("request/full", &[message, request])
        } else if !record.session_id.is_empty() {
            super::claude::identity_key("request/fallback", &[&record.session_id, message])
        } else {
            return ClaudeDeletionMatch::Uncertain;
        };
        if self.identities.contains(&identity) {
            ClaudeDeletionMatch::Exact
        } else if self
            .links
            .contains(&super::claude::suppression_link(message))
        {
            ClaudeDeletionMatch::Uncertain
        } else {
            ClaudeDeletionMatch::None
        }
    }
}

/// Apply the same source/time deletion rules to retained unresolved candidates.
/// Caller must hold the maintenance transaction, after merging deletion rules.
pub(crate) fn scrub_claude_pending(conn: &rusqlite::Connection) -> QueryResult<usize> {
    let exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='claude_protection_pending')", [], |r| r.get(0))?;
    if !exists {
        return Ok(0);
    }
    let policy = CapturePrivacy::load(conn)?;
    let controls = ClaudeDeletionControls::load(conn)?;
    if !controls.is_empty() {
        // Also check first-time displayed rows, which may not have any
        // successor watermark or original-baseline evidence yet.
        let mut query = conn.prepare(
            "SELECT entry_json FROM legacy_capture_entries WHERE report_source='claude'",
        )?;
        let rows = query.query_map([], |r| r.get::<_, String>(0))?;
        for row in rows {
            let entry: LegacyEntry = serde_json::from_str(&row?)
                .map_err(|_| QueryError::new("LEDGER_UNREADABLE", "invalid Claude baseline"))?;
            if controls.classify(&entry) != ClaudeDeletionMatch::None
                && !policy.suppresses(conn, &entry)?
            {
                return Err(QueryError::new("PRIVACY_SCOPE_INCOMPLETE", "the selected interval includes native evidence for a displayed Claude request whose anchor is outside the interval; include the displayed anchor"));
            }
        }
    }
    let rows: Vec<(String, String, String)> = conn
        .prepare("SELECT identity,entry_json,reason FROM claude_protection_pending")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut removed = 0;
    for (identity, encoded, reason) in rows {
        let entry: LegacyEntry = serde_json::from_str(&encoded)
            .map_err(|_| QueryError::new("LEDGER_UNREADABLE", "invalid Claude candidate"))?;
        let anchor_suppressed = policy.suppresses(conn, &entry)?;
        let adopted = reason == "original_baseline" || reason.starts_with("approved_successor/");
        let identity_match = controls.classify(&entry);
        if !anchor_suppressed && identity_match != ClaudeDeletionMatch::None {
            if adopted || identity_match == ClaudeDeletionMatch::Uncertain {
                return Err(QueryError::new("PRIVACY_SCOPE_INCOMPLETE", "the selected native Claude identity overlaps retained compatibility evidence whose scope cannot be safely separated"));
            }
        }
        if let Some(raw) = reason.strip_prefix("approved_successor/") {
            let raw_time: i64 = raw.parse().map_err(|_| {
                QueryError::new("LEDGER_UNREADABLE", "invalid Claude successor watermark")
            })?;
            let mut source_entry = entry.clone();
            let LegacyEntry::Request { record, .. } = &mut source_entry else {
                return Err(QueryError::new(
                    "LEDGER_UNREADABLE",
                    "invalid Claude successor watermark",
                ));
            };
            record.ts_ms = raw_time;
            if policy.suppresses(conn, &source_entry)? && !anchor_suppressed {
                return Err(QueryError::new("PRIVACY_SCOPE_INCOMPLETE", "the selected interval contains an adopted Claude successor whose displayed anchor is outside the interval; include the displayed anchor"));
            }
        }
        if anchor_suppressed || identity_match == ClaudeDeletionMatch::Exact {
            let count = conn.execute(
                "DELETE FROM claude_protection_pending WHERE identity=?1",
                [identity],
            )?;
            if reason != "original_baseline" && !reason.starts_with("approved_successor/") {
                removed += count;
            }
        }
    }
    Ok(removed)
}

/// Keep the last committed Claude display as the baseline, never a fresh scan.
/// The capture, unresolved candidates and read model commit as one transaction.
fn protect_claude_capture(
    conn: &rusqlite::Connection,
    snapshot: &mut LegacySnapshot,
) -> QueryResult<()> {
    use rusqlite::OptionalExtension;
    use std::collections::{BTreeMap, BTreeSet};
    // Validate new evidence before looking at the old baseline; a stale scanner
    // must not republish anything covered by a concurrent forget operation.
    CapturePrivacy::load(conn)?.validate(conn, snapshot)?;
    let ledger_route: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM source_roots WHERE report_source='claude' AND route='ledger')",
        [],
        |r| r.get(0),
    )?;
    if ledger_route {
        return Ok(());
    }
    let identity_controls = ClaudeDeletionControls::load(conn)?;
    if !identity_controls.is_empty() {
        // Pending rows restored from a backup or observed by an older scanner
        // must obey the same identity closure before we process this capture.
        scrub_claude_pending(conn)?;
    }
    let previous = LegacySnapshot::restore_capture_source(conn, Some("claude"))?;
    // If capture metadata was removed by maintenance, never use that as proof
    // that a source/window-controlled history has never been displayed.
    let deletion_scope_unresolved: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM suppression_rules WHERE (kind='window' AND (report_source IS NULL OR report_source='claude') AND (native_tool IS NULL OR native_tool='claude')) OR (kind IN('identity','session','identity_link') AND identity_family='claude')) OR EXISTS(SELECT 1 FROM adoption_forget_ranges WHERE consumer_tool='claude')",
        [], |r| r.get(0))?;
    let mut previous = match previous {
        Some(previous) => previous,
        None if !deletion_scope_unresolved => return Ok(()),
        None => LegacySnapshot::new(super::types::new_epoch(), snapshot.routing_hash.clone()),
    };
    previous.filter_suppressed(conn)?;
    let previously_published = previous
        .sources
        .get("claude")
        .and_then(|status| status.last_scanned_ms)
        .is_some()
        || !previous.entries.is_empty();
    let first_complete = snapshot.sources.get("claude").is_some_and(|status| {
        status.last_scanned_ms.is_some() && !status.stale && status.error.is_none()
    });
    if !previously_published && first_complete && !deletion_scope_unresolved {
        return Ok(());
    }
    let before = snapshot.content_fingerprint();
    // A time-scoped deletion can remove the original timestamp of a streaming
    // request while a later observation has another timestamp. Compatibility
    // rows lack a durable per-member tombstone, so after such controls new
    // identities cannot be proved disjoint from deleted history automatically.
    let mut incoming = Vec::new();
    for entry in snapshot
        .entries
        .iter()
        .filter(|entry| entry.report_source() == "claude")
    {
        match identity_controls.classify(entry) {
            ClaudeDeletionMatch::None => incoming.push(entry.clone()),
            ClaudeDeletionMatch::Exact => { /* A forgotten identity may not re-enter pending. */ }
            ClaudeDeletionMatch::Uncertain => return Err(QueryError::new("PRIVACY_FILTER_REQUIRED", "Claude source evidence overlaps a forgotten identity and cannot be safely separated")),
        }
    }
    // Unresolved observations still constrain identity decisions. In
    // particular, an exact-key upgrade of a weak baseline must not later
    // permit a different message carrying the same request ID to slip in.
    let pending_rows: Vec<(String, String)> = conn
        .prepare("SELECT identity,entry_json FROM claude_protection_pending WHERE reason<>'original_baseline' AND reason NOT GLOB 'approved_successor/*' ORDER BY identity")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut all_candidates = incoming.clone();
    let mut pending_encoded = BTreeSet::new();
    for (_, row) in &pending_rows {
        pending_encoded.insert(row.clone());
        let entry: LegacyEntry = serde_json::from_str(row)
            .map_err(|_| QueryError::new("LEDGER_UNREADABLE", "invalid Claude candidate"))?;
        all_candidates.push(entry);
    }
    let mut candidate_messages: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut candidate_requests: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for entry in &all_candidates {
        if let LegacyEntry::Request { record, .. } = entry {
            if let Some(message) = claude_possible_message(record) {
                candidate_messages
                    .entry(message.to_owned())
                    .or_default()
                    .insert(record.dedup_key.clone());
            }
            if let Some((_, request)) = claude_identity(record) {
                candidate_requests
                    .entry(request.to_owned())
                    .or_default()
                    .insert(record.dedup_key.clone());
            }
        }
    }
    // Rotate over a bounded batch, so a permanently ambiguous early candidate
    // cannot starve later evidence. The cursor commits with the projection.
    let cursor: Option<String> = conn
        .query_row(
            "SELECT value_json FROM ledger_runtime_state WHERE state_key='claude-pending-cursor'",
            [],
            |r| r.get::<_, String>(0),
        )
        .optional()?
        .and_then(|value| serde_json::from_str(&value).ok());
    let start = cursor
        .as_ref()
        .map(|cursor| pending_rows.partition_point(|(id, _)| id <= cursor))
        .unwrap_or(0);
    let count = pending_rows.len().min(256);
    let mut candidates = incoming;
    let policy = CapturePrivacy::load(conn)?;
    let mut last_cursor = None;
    for offset in 0..count {
        let (identity, row) = &pending_rows[(start + offset) % pending_rows.len()];
        let entry: LegacyEntry = serde_json::from_str(row)
            .map_err(|_| QueryError::new("LEDGER_UNREADABLE", "invalid Claude candidate"))?;
        if !policy.suppresses(conn, &entry)? {
            candidates.push(entry);
        }
        last_cursor = Some(identity);
    }
    if let Some(cursor) = last_cursor {
        let encoded = serde_json::to_string(cursor)
            .map_err(|_| QueryError::invalid("invalid Claude cursor"))?;
        conn.execute("INSERT INTO ledger_runtime_state(state_key,value_json) VALUES('claude-pending-cursor',?1) ON CONFLICT(state_key) DO UPDATE SET value_json=excluded.value_json", [encoded])?;
    }
    let mut retained = previous.entries;
    let mut by_key: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut by_message: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut by_request: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut unknown_sessions = BTreeSet::new();
    let mut encoded_retained = BTreeSet::new();
    for (index, entry) in retained.iter().enumerate() {
        encoded_retained.insert(
            serde_json::to_string(entry)
                .map_err(|_| QueryError::invalid("invalid Claude baseline"))?,
        );
        if let LegacyEntry::Request { record, .. } = entry {
            by_key
                .entry(record.dedup_key.clone())
                .or_default()
                .push(index);
            if let Some(message) = claude_possible_message(record) {
                by_message
                    .entry(message.to_owned())
                    .or_default()
                    .push(index);
            } else {
                unknown_sessions.insert(record.session_id.clone());
            }
            if let Some((_, request)) = claude_identity(record) {
                by_request
                    .entry(request.to_owned())
                    .or_default()
                    .push(index);
            }
        }
    }
    let mut processed = BTreeSet::new();
    for entry in candidates {
        let candidate_json = serde_json::to_string(&entry)
            .map_err(|_| QueryError::invalid("invalid Claude candidate"))?;
        if !processed.insert(candidate_json.clone()) {
            continue;
        }
        let LegacyEntry::Request {
            record: candidate,
            native_tool,
            namespace,
            known_fields,
        } = &entry
        else {
            // Session and day summaries cannot safely be interpreted as requests.
            // Exact repeats need no extra copy; changed summaries await review.
            let encoded = serde_json::to_string(&entry)
                .map_err(|_| QueryError::invalid("invalid Claude candidate"))?;
            if !encoded_retained.contains(&encoded) {
                claude_pending(conn, &entry, "unresolved_summary")?;
            }
            continue;
        };
        let identity =
            claude_identity(candidate).map(|(message, request)| (message, Some(request)));
        let matching = by_key
            .get(&candidate.dedup_key)
            .cloned()
            .unwrap_or_default();
        let fallback = if identity.is_none() && !encoded_retained.contains(&candidate_json) {
            // A previously displayed fallback member can enter the existing
            // identity branch; a numeric change still requires independent
            // native lifecycle proof below. A new weak identity must pass the
            // stronger active/eligible proof before it may be appended.
            (matching.len() == 1
                && !candidate.session_id.is_empty()
                && claude_possible_message(candidate).is_some())
                || proven_claude_fallback(conn, &entry)?
        } else {
            false
        };
        let Some((message, request)) = identity
            .or_else(|| fallback.then(|| (claude_possible_message(candidate).unwrap(), None)))
        else {
            if !encoded_retained.contains(&candidate_json) {
                claude_pending(conn, &entry, "weak_identity")?;
            }
            continue;
        };
        if deletion_scope_unresolved && matching.is_empty() {
            claude_pending(conn, &entry, "deletion_scope_unresolved")?;
            continue;
        }
        let ambiguous = by_message
            .get(message)
            .is_some_and(|indices| indices.iter().any(|index| !matching.contains(index)))
            || request.is_some_and(|request| {
                by_request
                    .get(request)
                    .is_some_and(|indices| indices.iter().any(|index| !matching.contains(index)))
            })
            || candidate_messages
                .get(message)
                .is_some_and(|keys| keys.iter().any(|key| key != &candidate.dedup_key))
            || request.is_some_and(|request| {
                candidate_requests
                    .get(request)
                    .is_some_and(|keys| keys.iter().any(|key| key != &candidate.dedup_key))
            })
            || !unknown_sessions.is_empty();
        if ambiguous || matching.len() > 1 {
            claude_pending(conn, &entry, "identity_ambiguous")?;
            continue;
        }
        if let Some(&index) = matching.first() {
            let LegacyEntry::Request {
                record: old,
                native_tool: old_tool,
                namespace: old_namespace,
                known_fields: old_fields,
            } = &retained[index]
            else {
                unreachable!()
            };
            let comparable = old.source == candidate.source
                && old.session_id == candidate.session_id
                && old.agent == candidate.agent
                && old.model == candidate.model
                && old.caliber == candidate.caliber
                && old.request_count == candidate.request_count
                && old_tool == native_tool
                && old_namespace == namespace
                && old_fields == known_fields
                && old.cost_usd == candidate.cost_usd;
            let monotonic = candidate.input_uncached >= old.input_uncached
                && candidate.cache_read >= old.cache_read
                && candidate.cache_write >= old.cache_write
                && candidate.cache_write_1h >= old.cache_write_1h
                && candidate.output >= old.output
                && candidate.reasoning_output >= old.reasoning_output;
            if !comparable || !monotonic {
                claude_pending(conn, &entry, "conflicting_observation")?;
                continue;
            }
            let mut successor = entry.clone();
            if let LegacyEntry::Request { record, .. } = &mut successor {
                record.ts_ms = old.ts_ms;
            }
            let successor_json = serde_json::to_string(&successor)
                .map_err(|_| QueryError::invalid("invalid Claude successor"))?;
            let old_json = serde_json::to_string(&retained[index])
                .map_err(|_| QueryError::invalid("invalid Claude baseline"))?;
            if successor_json != old_json {
                let approved_time = claude_approved_time(conn, &retained[index])?;
                if *known_fields != 0
                    || approved_time.is_none_or(|time| candidate.ts_ms < time)
                    || !proven_claude_progress(conn, old, candidate)?
                {
                    claude_pending(conn, &entry, "progress_unproven")?;
                    continue;
                }
                claude_pending(conn, &retained[index], "original_baseline")?;
                claude_set_approved(conn, &successor, candidate.ts_ms)?;
            }
            encoded_retained.insert(successor_json);
            retained[index] = successor;
        } else {
            let index = retained.len();
            by_key
                .entry(candidate.dedup_key.clone())
                .or_default()
                .push(index);
            by_message
                .entry(message.to_owned())
                .or_default()
                .push(index);
            if let Some(request) = request {
                by_request
                    .entry(request.to_owned())
                    .or_default()
                    .push(index);
            }
            encoded_retained.insert(
                serde_json::to_string(&entry)
                    .map_err(|_| QueryError::invalid("invalid Claude candidate"))?,
            );
            retained.push(entry);
        }
        if pending_encoded.contains(&candidate_json) {
            conn.execute("DELETE FROM claude_protection_pending WHERE reason<>'original_baseline' AND reason NOT GLOB 'approved_successor/*' AND entry_json=?1", [candidate_json])?;
        }
    }
    snapshot
        .entries
        .retain(|entry| entry.report_source() != "claude");
    snapshot.entries.extend(retained);
    let evidence_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM claude_protection_pending WHERE reason NOT GLOB 'approved_successor/*'", [], |r| {
            r.get(0)
        })?;
    if evidence_count > 1_000_000 {
        return Err(QueryError::new(
            "CLAUDE_EVIDENCE_LIMIT",
            "Claude unresolved evidence limit reached; previous published history retained",
        ));
    }
    if before != snapshot.content_fingerprint() {
        snapshot.generation = super::types::new_epoch();
    }
    Ok(())
}

impl super::Ledger {
    /// Compatibility publication and its query projection share one commit.
    pub fn publish_compatibility_capture(&mut self, snapshot: &LegacySnapshot) -> QueryResult<()> {
        use rusqlite::params;
        self.require_writer()
            .map_err(|_| QueryError::invalid("writer required"))?;
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let previous = LegacySnapshot::restore_capture(&tx)?;
        let mut protected = snapshot.clone();
        protect_claude_capture(&tx, &mut protected)?;
        // Scanner generations describe raw input, not the protected result.
        // Keep the published cursor stable when a scan, status update or
        // pending-only retry leaves all visible facts and routing unchanged.
        if let Some(old) = &previous {
            if old.projection_fingerprint() == protected.projection_fingerprint() {
                protected.generation = old.generation.clone();
            } else {
                protected.generation = super::types::new_epoch();
            }
        } else {
            protected.generation = super::types::new_epoch();
        }
        let projection_unchanged: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM query_live_state s JOIN ledger_meta m ON m.singleton=s.singleton AND m.ledger_uuid=s.ledger_uuid AND m.epoch=s.epoch WHERE s.generation=?1 AND s.routing_hash=?2)",params![protected.generation,protected.routing_hash],|r|r.get(0))?;
        protected.persist(&tx)?;
        super::read_model::replace(&tx, &protected)?;
        if !projection_unchanged {
            super::db::bump(&tx).map_err(|e| QueryError::new(e.code(), "capture commit failed"))?;
        }
        tx.commit()?;
        Ok(())
    }
}
impl LegacySnapshot {
    /// Publish a restartable capture inside the owning writer transaction.
    /// Unchanged content updates only capture metadata, not all historical rows.
    pub fn persist(&self, conn: &rusqlite::Connection) -> QueryResult<()> {
        use rusqlite::{params, OptionalExtension};
        if conn.is_autocommit() {
            return Err(QueryError::invalid(
                "capture publication requires a transaction",
            ));
        }
        // Check before the same-generation shortcut: a stale caller must not
        // republish entries forbidden by controls added in this same epoch.
        CapturePrivacy::load(conn)?.validate(conn, self)?;
        let (uuid, epoch): (String, String) =
            conn.query_row("SELECT ledger_uuid,epoch FROM ledger_meta", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?;
        let previous: Option<(bool, String)> = conn.query_row(
            "SELECT generation=?1 AND ledger_uuid=?2 AND epoch=?3,metadata_json FROM legacy_capture_state WHERE singleton=1",
            params![self.generation, uuid, epoch], |r| Ok((r.get(0)?, r.get(1)?))).optional()?;
        let unchanged = previous.as_ref().is_some_and(|(unchanged, _)| *unchanged);
        let previous_hashes = previous
            .as_ref()
            .filter(|(unchanged, _)| *unchanged)
            .and_then(|(_, encoded)| serde_json::from_str::<serde_json::Value>(encoded).ok())
            .and_then(|metadata| {
                (metadata["source_content_hash_version"] == 1)
                    .then(|| metadata.get("source_content_hashes").cloned())
                    .flatten()
            })
            .and_then(|hashes| serde_json::from_value::<BTreeMap<String, String>>(hashes).ok())
            .filter(|hashes| {
                self.sources
                    .keys()
                    .all(|source| hashes.contains_key(source))
            });
        let source_content_hashes = if let Some(hashes) = previous_hashes {
            hashes
        } else {
            // Calculate source-specific retry dependencies while serializing
            // the rows we already publish. A new unrelated source must not
            // repeatedly activate every old failed repair bucket.
            let new_hash = || {
                let mut hash = Sha256::new();
                hash.update(b"usagebook/legacy-source-content/v1");
                hash
            };
            let mut hashes: BTreeMap<String, Sha256> = self
                .sources
                .keys()
                .map(|source| (source.clone(), new_hash()))
                .collect();
            if !unchanged {
                conn.execute("DELETE FROM legacy_capture_entries", [])?;
            }
            let mut insert = if unchanged {
                None
            } else {
                Some(conn.prepare("INSERT INTO legacy_capture_entries(ordinal,report_source,entry_json) VALUES(?1,?2,?3)")?)
            };
            for (i, entry) in self.entries.iter().enumerate() {
                let encoded = serde_json::to_string(entry)
                    .map_err(|_| QueryError::invalid("invalid capture entry"))?;
                let hash = hashes
                    .entry(entry.report_source().to_owned())
                    .or_insert_with(new_hash);
                hash.update((encoded.len() as u64).to_be_bytes());
                hash.update(encoded.as_bytes());
                if let Some(insert) = insert.as_mut() {
                    insert.execute(params![i as i64, entry.report_source(), encoded])?;
                }
            }
            hashes
                .into_iter()
                .map(|(source, hash)| (source, format!("{:x}", hash.finalize())))
                .collect()
        };
        let metadata = serde_json::json!({
            "generation":self.generation, "routing_hash":self.routing_hash,
            "sources":self.sources, "input_containers":self.input_containers,
            "entries":[], "entry_count":self.entries.len(),
            "source_content_hash_version":1,
            "source_content_hashes":source_content_hashes
        });
        conn.execute("INSERT INTO legacy_capture_state VALUES(1,?1,?2,?3,?4) ON CONFLICT(singleton) DO UPDATE SET ledger_uuid=excluded.ledger_uuid,epoch=excluded.epoch,generation=excluded.generation,metadata_json=excluded.metadata_json",
            params![uuid,epoch,self.generation,metadata.to_string()])?;
        Ok(())
    }

    /// Apply only explicit source/time/native-day scopes. Preserve every field
    /// of retained records; unknown time is not guessed into a dated deletion.
    pub fn filter_suppressed(&mut self, conn: &rusqlite::Connection) -> QueryResult<usize> {
        let policy = CapturePrivacy::load(conn)?;
        if policy.rules.is_empty() && policy.adoption_ranges.is_empty() {
            return Ok(0);
        }
        let keep = self
            .entries
            .iter()
            .map(|entry| policy.suppresses(conn, entry).map(|v| !v))
            .collect::<QueryResult<Vec<_>>>()?;
        let removed = keep.iter().filter(|keep| !**keep).count();
        if removed == 0 {
            return Ok(0);
        }
        let mut flags = keep.into_iter();
        self.entries.retain(|_| flags.next().unwrap());
        // Diagnostic row counts describe the retained snapshot, while the scan
        // success timestamp still describes the completed source read.
        for (source, status) in &mut self.sources {
            status.unaccounted_rows = self
                .entries
                .iter()
                .filter(
                    |entry| matches!(entry, LegacyEntry::Day(day) if &day.report_source == source),
                )
                .count();
            status.unallocated_sessions = self.entries.iter().filter(|entry| matches!(entry, LegacyEntry::SessionTotal { record, .. } if record.source == "goose" && record.source == source)).count();
        }
        self.generation = super::types::new_epoch();
        Ok(removed)
    }

    /// Read saved normalized fields only. Never discovers or reads source files.
    pub fn restore_capture(conn: &rusqlite::Connection) -> QueryResult<Option<Self>> {
        Self::restore_capture_source(conn, None)
    }
    pub fn restore_capture_source(
        conn: &rusqlite::Connection,
        source: Option<&str>,
    ) -> QueryResult<Option<Self>> {
        use rusqlite::OptionalExtension;
        // One SQLite snapshot for both the capture pointer and its rows.
        let _transaction = if conn.is_autocommit() {
            Some(conn.unchecked_transaction()?)
        } else {
            None
        };
        let exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='legacy_capture_state' AND type='table')", [], |r|r.get(0))?;
        if !exists {
            return Ok(None);
        }
        let metadata: Option<String> = conn.query_row(
            "SELECT s.metadata_json FROM legacy_capture_state s JOIN ledger_meta m ON m.singleton=s.singleton AND m.ledger_uuid=s.ledger_uuid AND m.epoch=s.epoch", [], |r|r.get(0)).optional()?;
        let Some(metadata) = metadata else {
            return Ok(None);
        };
        let invalid =
            || QueryError::new("LEDGER_UNREADABLE", "invalid saved compatibility capture");
        let value: serde_json::Value = serde_json::from_str(&metadata).map_err(|_| invalid())?;
        let expected = value["entry_count"].as_u64().ok_or_else(invalid)?;
        let mut snapshot: Self = serde_json::from_value(value).map_err(|_| invalid())?;
        let mut statement = conn.prepare("SELECT entry_json,entry_json -> '$.Request.record.cost_usd',entry_json -> '$.SessionTotal.record.cost_usd' FROM legacy_capture_entries WHERE (?1 IS NULL OR report_source=?1) ORDER BY ordinal")?;
        let mut rows = statement.query([source])?;
        while let Some(row) = rows.next()? {
            let encoded: String = row.get(0)?;
            let mut entry: LegacyEntry = serde_json::from_str(&encoded).map_err(|_| invalid())?;
            // The saved decimal literal is authoritative. Rust's float parser
            // round-trips the original f64 spelling; the JSON fast numeric path
            // can differ by one ULP. Do not rewrite historical cost on restart.
            let cost = match &mut entry {
                LegacyEntry::Request { record, .. } => {
                    Some((record, row.get::<_, Option<String>>(1)?))
                }
                LegacyEntry::SessionTotal { record, .. } => {
                    Some((record, row.get::<_, Option<String>>(2)?))
                }
                LegacyEntry::Day(_) => None,
            };
            if let Some((record, literal)) = cost {
                record.cost_usd = literal
                    .filter(|value| value != "null")
                    .map(|value| value.parse::<f64>().map_err(|_| invalid()))
                    .transpose()?;
            }
            snapshot.entries.push(entry);
        }
        if source.is_none() && snapshot.entries.len() as u64 != expected {
            return Err(invalid());
        }
        if let Some(source) = source {
            if snapshot
                .entries
                .iter()
                .any(|entry| entry.report_source() != source)
            {
                return Err(invalid());
            }
            snapshot.sources.retain(|id, _| id == source);
            snapshot.input_containers.retain(|id, _| id == source);
        }
        Ok(Some(snapshot))
    }

    pub fn new(generation: String, routing_hash: String) -> Self {
        Self {
            generation,
            routing_hash,
            entries: Vec::new(),
            sources: BTreeMap::new(),
            input_containers: BTreeMap::new(),
        }
    }
    /// Capture before scanning, so disappearance during a read cannot erase the
    /// fact that this scan used a container. Uninstalled optional inputs are absent.
    pub fn observe_input_containers(&mut self, source: &str, paths: Vec<PathBuf>) {
        let inputs = paths
            .into_iter()
            .filter_map(|path| {
                let metadata = std::fs::metadata(&path).ok()?;
                (metadata.is_dir() || metadata.is_file()).then_some(InputContainer {
                    path,
                    directory: metadata.is_dir(),
                })
            })
            .collect();
        self.input_containers.insert(source.into(), inputs);
    }
    /// Only a previous complete success establishes an input availability promise.
    pub fn has_missing_input_containers(&self, source: &str) -> bool {
        self.sources
            .get(source)
            .is_some_and(|status| status.last_scanned_ms.is_some())
            && self.input_containers.get(source).is_some_and(|inputs| {
                inputs.iter().any(|input| {
                    std::fs::metadata(&input.path).map_or(true, |metadata| {
                        if input.directory {
                            !metadata.is_dir()
                        } else {
                            !metadata.is_file()
                        }
                    })
                })
            })
    }
    /// A failed attempt must not replace the last complete input list with an
    /// empty discovery result; otherwise a second failure could look successful.
    pub fn retain_input_containers(&mut self, source: &str, previous: &Self) {
        match previous.input_containers.get(source) {
            Some(inputs) => {
                self.input_containers.insert(source.into(), inputs.clone());
            }
            None => {
                self.input_containers.remove(source);
            }
        }
    }
    /// Query content, independent of scan time and adapter iteration order.
    /// Only the publisher uses this digest; cursors retain random generations so
    /// an A -> B -> A transition cannot revive an old cursor.
    pub fn content_fingerprint(&self) -> [u8; 32] {
        self.fingerprint(true)
    }
    fn projection_fingerprint(&self) -> [u8; 32] {
        self.fingerprint(false)
    }
    fn fingerprint(&self, include_status: bool) -> [u8; 32] {
        fn key(entry: &LegacyEntry) -> (u8, &str, &str, &str) {
            match entry {
                LegacyEntry::Request {
                    record, namespace, ..
                } => (
                    0,
                    record.source,
                    namespace.as_deref().unwrap_or(""),
                    &record.dedup_key,
                ),
                LegacyEntry::Day(day) => (1, &day.report_source, &day.native_date, &day.row_key),
                LegacyEntry::SessionTotal { record, .. } => {
                    (2, record.source, "", &record.dedup_key)
                }
            }
        }
        let mut entries: Vec<_> = self.entries.iter().collect();
        // Stable sort preserves same-identity order: diagnostic/session pages
        // select the last matching row. Do not deduplicate repeated records.
        entries.sort_by(|a, b| key(a).cmp(&key(b)));
        let sources: Vec<_> = self
            .sources
            .iter()
            .map(|(id, s)| {
                (
                    id,
                    s.stale,
                    &s.error,
                    s.last_scanned_ms.is_some(),
                    s.unaccounted_rows,
                    s.unallocated_sessions,
                )
            })
            .collect();
        let mut hash = Sha256::new();
        hash.update(b"usagebook/legacy-content/v1");
        hash.update((self.routing_hash.len() as u64).to_be_bytes());
        hash.update(self.routing_hash.as_bytes());
        hash.update((entries.len() as u64).to_be_bytes());
        for entry in entries {
            // JSON maps non-finite floats to null; retain their bits as well.
            let cost_bits = match entry {
                LegacyEntry::Request { record, .. } | LegacyEntry::SessionTotal { record, .. } => {
                    record.cost_usd.map(f64::to_bits)
                }
                LegacyEntry::Day(_) => None,
            };
            let bytes = serde_json::to_vec(&(entry, cost_bits))
                .expect("typed legacy entry is serializable");
            hash.update((bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        }
        if include_status {
            hash.update(serde_json::to_vec(&sources).expect("typed source status is serializable"));
        }
        hash.finalize().into()
    }
    pub fn push_records(
        &mut self,
        source: &str,
        records: Vec<UsageRecord>,
        scanned_ms: i64,
    ) -> QueryResult<()> {
        self.push_records_with_stats(source, records, &ScanStats::default(), scanned_ms)
    }
    pub fn push_records_with_stats(
        &mut self,
        source: &str,
        records: Vec<UsageRecord>,
        stats: &ScanStats,
        scanned_ms: i64,
    ) -> QueryResult<()> {
        // A source is a single publication unit. A valid prefix must not survive
        // a later normalization failure, nor may failure advance its success time.
        let mut accepted = Vec::with_capacity(records.len());
        for record in records {
            if record.source == "ccswitch-rollup" {
                return Err(QueryError::invalid(
                    "cc-switch daily rows must enter LegacyEntry::Day",
                ));
            }
            if record.model.len() > 1024 {
                return Err(QueryError::limit("legacy model exceeds 1024 bytes"));
            }
            let precision = stats
                .session_total_precision
                .get(&record.dedup_key)
                .filter(|_| record.caliber == Caliber::SessionTotal);
            let fields = if let Some(precision) = precision {
                precision.metrics
            } else {
                legacy_metrics(&record)?
            };
            // Checking each supplied bucket alone misses an overflowing derived
            // total (or an impossible subset). Query output uses this evaluator too.
            super::metrics::evaluate(&fields)
                .map_err(|e| QueryError::new(e.code(), e.to_string()))?;
            accepted.push(match record.caliber {
                Caliber::RequestDelta => LegacyEntry::Request {
                    native_tool: source.into(),
                    namespace: None,
                    record,
                    known_fields: 0,
                },
                Caliber::SessionTotal => LegacyEntry::SessionTotal {
                    record,
                    known_fields: if precision.is_some() { 1023 } else { 0 },
                    projection: precision.map(|p| p.metrics),
                    reason: precision.map(|p| p.reason.clone()),
                },
            });
        }
        let unallocated_sessions = accepted
            .iter()
            .filter(|entry| {
                matches!(entry,
            LegacyEntry::SessionTotal { record, .. } if record.source == "goose")
            })
            .count();
        self.entries.extend(accepted);
        self.sources.insert(
            source.into(),
            LegacySourceStatus {
                last_scanned_ms: Some(scanned_ms),
                stale: false,
                error: None,
                unaccounted_rows: 0,
                unallocated_sessions,
            },
        );
        Ok(())
    }
    /// Reads only native rollup columns. The upstream schema supplies no trustworthy account or
    /// timezone, so those remain unknown and these rows stay in the diagnostic day view.
    pub fn load_ccswitch_at(&mut self, path: &Path) -> QueryResult<()> {
        let conn = rusqlite::Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let mut st=conn.prepare("SELECT date, app_type, model, SUM(input_tokens), SUM(cache_read_tokens), SUM(cache_creation_tokens), SUM(output_tokens), SUM(request_count) FROM usage_daily_rollups GROUP BY date,app_type,model ORDER BY date,app_type,model")?;
        let mut rows = st.query([])?;
        // Publish a complete read only. A later invalid row must not leak a
        // partial replacement or a success count into the caller's snapshot.
        let mut accepted = Vec::new();
        while let Some(r) = rows.next()? {
            let date: String = r.get(0)?;
            let app: String = r.get(1)?;
            let raw_model: Option<String> = r.get(2)?;
            let model_kind = if raw_model.is_some() { "text" } else { "null" };
            let model = raw_model.unwrap_or_default();
            if !matches!(app.as_str(), "claude" | "codex" | "opencode" | "pi") {
                continue;
            }
            if model.len() > 1024 {
                return Err(QueryError::limit("daily model exceeds 1024 bytes"));
            }
            let mut p = [None; 10];
            for (col, index) in [(3, 0), (4, 1), (5, 2), (6, 6), (7, 9)] {
                let v: Option<i64> = r.get(col)?;
                if v.is_some_and(|n| n < 0) {
                    return Err(QueryError::invalid("negative daily usage"));
                }
                p[index] = v;
            }
            // Query output uses the same constraints, including derived totals.
            // Reject the entire read before replacing any usable old evidence.
            super::metrics::evaluate(&p).map_err(|e| QueryError::new(e.code(), e.to_string()))?;
            let key = tuple_hash(&[
                "usagebook/legacy-day/v1",
                "ccswitch-rollup",
                "unknown",
                &date,
                &app,
                // Match the raw SQL grouping, including NULL versus empty text.
                // model_key remains a normalized filter, not evidence identity.
                model_kind,
                &model,
            ]);
            accepted.push(LegacyEntry::Day(LegacyDay {
                row_key: key,
                report_source: "ccswitch-rollup".into(),
                native_tool: app,
                namespace: None,
                model_key: model_key(&model),
                model_display: model,
                native_date: date,
                timezone: None,
                start_ms: None,
                end_ms: None,
                projection: p,
            }));
        }
        let unaccounted_rows = accepted.len();
        self.entries.extend(accepted);
        self.sources.insert(
            "ccswitch-rollup".into(),
            LegacySourceStatus {
                last_scanned_ms: Some(chrono::Utc::now().timestamp_millis()),
                stale: false,
                error: None,
                unaccounted_rows,
                unallocated_sessions: 0,
            },
        );
        Ok(())
    }
}
pub(crate) fn model_key(name: &str) -> String {
    if name.is_empty() {
        String::new()
    } else {
        format!("n:{}", name.to_lowercase())
    }
}
pub(crate) fn legacy_metrics(r: &UsageRecord) -> QueryResult<[Option<i64>; 10]> {
    let mut m = [None; 10];
    for (i, v) in [
        (0, r.input_uncached),
        (1, r.cache_read),
        (2, r.cache_write),
        (3, r.output),
        (4, r.reasoning_output),
        (8, r.cache_write_1h),
        (9, u64::from(r.request_count)),
    ] {
        m[i] = Some(i64::try_from(v).map_err(|_| {
            QueryError::new("NUMERIC_OVERFLOW", "legacy usage exceeds signed 64-bit")
        })?);
    }
    Ok(m)
}
pub(crate) fn tuple_hash(parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    for p in parts {
        hash.update((p.len() as u64).to_be_bytes());
        hash.update(p.as_bytes());
    }
    format!("{:x}", hash.finalize())
}
