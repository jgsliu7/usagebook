//! Typed, process-local bridge for sources that have not entered the durable ledger.
//! Never serializes a synthetic midnight request for native daily evidence.
use super::query_types::{QueryError, QueryResult};
use crate::model::{Caliber, UsageRecord};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize)]
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
#[derive(Debug, Clone, Serialize)]
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
#[derive(Debug, Clone, Default)]
pub struct LegacySourceStatus {
    pub last_scanned_ms: Option<i64>,
    pub stale: bool,
    pub error: Option<String>,
    /// Diagnostic day rows only; never a request count or an amount to add.
    pub unaccounted_rows: usize,
}
#[derive(Debug, Clone)]
pub struct LegacySnapshot {
    pub generation: String,
    pub routing_hash: String,
    pub entries: Vec<LegacyEntry>,
    pub sources: BTreeMap<String, LegacySourceStatus>,
    // Process-local availability evidence only: not query content, persisted
    // state, or public status. An empty complete scan still remembers its inputs.
    input_containers: BTreeMap<String, Vec<InputContainer>>,
}
#[derive(Debug, Clone)]
struct InputContainer {
    path: PathBuf,
    directory: bool,
}
impl LegacySnapshot {
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
            let fields = legacy_metrics(&record)?;
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
                    known_fields: 0,
                },
            });
        }
        self.entries.extend(accepted);
        self.sources.insert(
            source.into(),
            LegacySourceStatus {
                last_scanned_ms: Some(scanned_ms),
                stale: false,
                error: None,
                unaccounted_rows: 0,
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
