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

impl super::Ledger {
    /// Compatibility publication and its query projection share one commit.
    pub fn publish_compatibility_capture(&mut self, snapshot: &LegacySnapshot) -> QueryResult<()> {
        self.require_writer()
            .map_err(|_| QueryError::invalid("writer required"))?;
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        snapshot.persist(&tx)?;
        super::read_model::replace(&tx, snapshot)?;
        super::db::bump(&tx).map_err(|e| QueryError::new(e.code(), "capture commit failed"))?;
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
        hash.update(serde_json::to_vec(&sources).expect("typed source status is serializable"));
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
