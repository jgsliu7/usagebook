//! Bounded, read-only acquisition of CC Switch usage evidence.
//!
//! This adapter deliberately does not grant accounting eligibility. The external database
//! contains both proxy usage and mirrors of native sessions; an upstream row ID proves a row,
//! not a new billable request. Account coverage and historical day timezone require a separate
//! persisted decision. Configuration, credentials, prompts and error messages are never read.
use super::types::{Metrics, Observation};
use super::{LedgerError, Result};
use rusqlite::{Connection, OpenFlags, Row};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const PARSER_VERSION: &str = "ccswitch-ledger-v1";
pub const COLLECTOR_KEY: &str = "ccswitch-sqlite-v1";
pub const CAPABILITY_VERSION: &str = "ccswitch-values-snapshot-v2";
pub const NAMESPACE: &str = "unknown:ccswitch";
pub const DEFAULT_BATCH_ROWS: usize = 512;
const MAX_BATCH_ROWS: usize = 2048;
const MAX_TEXT_BYTES: usize = 1024;
const MAX_PATH_CONFIG_BYTES: u64 = 64 * 1024;

/// Only these fields can reach the durable evidence table. None contains conversation text.
pub const PROJECTION_FIELDS: &[&str] = &[
    "channel",
    "schema_version",
    "date",
    "app_type",
    "provider_id",
    "model",
    "request_model",
    "pricing_model",
    "request_id",
    "session_id",
    "data_source",
    "created_at",
    "status_code",
    "is_streaming",
    "request_count",
    "success_count",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_creation_tokens",
    "input_token_semantics",
    "total_cost_usd",
    "normalization_version",
    "normalization_reason",
    "row_key",
];

#[derive(Clone, Debug)]
pub struct UsageEvidence {
    pub row_key: String,
    pub fingerprint: String,
    pub observation: Observation,
    pub projection: Value,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReadProgress {
    pub day_rows: u64,
    pub request_rows: u64,
    pub rejected_rows: u64,
    pub missing_rows: u64,
    pub complete: bool,
}

#[derive(Clone, Debug, Default)]
pub struct EvidenceBatch {
    pub entries: Vec<UsageEvidence>,
    pub range_key: String,
    pub scanned_rows: u64,
    pub missing_keys: Vec<String>,
    pub progress: ReadProgress,
}

/// A finite initial target: SQLite pins the snapshot at open, so concurrent appends cannot
/// prevent completion. Every batch is bounded and can be committed before reading the next.
/// If the process stops, restart from the beginning; canonical keys and fingerprints make
/// replay idempotent. A new completed pass reads *all* old days again, including late changes.
/// Direct readers hold a snapshot only while in use. The scheduled Collector persists its
/// value snapshot and drops every source transaction before returning, including on pause.
/// No write is ever issued to the source.
pub struct EvidenceReader {
    conn: Connection,
    day_columns: BTreeSet<String>,
    request_columns: BTreeSet<String>,
    schema_version: i64,
    channel: u8,
    cursor: String,
    progress: ReadProgress,
    namespace: String,
    day_keys: Vec<String>,
    request_keys: Vec<String>,
    key_index: usize,
}

impl EvidenceReader {
    pub fn open(path: &Path, namespace: &str) -> Result<Self> {
        Self::open_mode(path, namespace, true)
    }
    fn open_mode(path: &Path, namespace: &str, manifest: bool) -> Result<Self> {
        if namespace.is_empty() || namespace.len() > MAX_TEXT_BYTES {
            return Err(LedgerError::InvalidInput(
                "invalid CC usage namespace".into(),
            ));
        }
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(Duration::from_millis(250))?;
        if !manifest {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            conn.progress_handler(1000, Some(move || std::time::Instant::now() >= deadline))?;
        }
        conn.execute_batch("PRAGMA query_only=ON; BEGIN DEFERRED;")?;
        // Read schema inside the transaction: it pins the WAL snapshot before either channel.
        let day_columns = columns(&conn, "usage_daily_rollups")?;
        let request_columns = columns(&conn, "proxy_request_logs")?;
        if day_columns.is_empty() && request_columns.is_empty() {
            return Err(LedgerError::InvalidInput(
                "no supported CC usage tables".into(),
            ));
        }
        require_columns(&day_columns, &["date", "app_type", "provider_id", "model"])?;
        require_columns(
            &request_columns,
            &["request_id", "app_type", "provider_id", "model"],
        )?;
        // A keyset cursor is safe only when the upstream request key is unique. Do not
        // silently skip duplicate keys in an unrecognized/rebuilt table shape.
        if !request_columns.is_empty() {
            let mut info = conn.prepare("PRAGMA table_info(proxy_request_logs)")?;
            let primary: Vec<String> = info
                .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, i64>(5)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?
                .into_iter()
                .filter(|(_, pk)| *pk > 0)
                .map(|(name, _)| name)
                .collect();
            if primary != ["request_id"] {
                return Err(LedgerError::UnsupportedVersion);
            }
        }
        let rejected_keys = if request_columns.is_empty() || !manifest {
            0
        } else {
            u64::try_from(conn.query_row("SELECT COUNT(*) FROM proxy_request_logs WHERE typeof(request_id)<>'text' OR length(request_id)=0",[],|r|r.get::<_,i64>(0))?)
                .map_err(|_|LedgerError::NumericOverflow)?
        };
        let schema_version = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        let day_keys = if manifest {
            read_keys(
                &conn,
                "usage_daily_rollups",
                &day_columns,
                &daily_key(&day_columns),
            )?
        } else {
            Vec::new()
        };
        let request_keys = if manifest {
            read_keys(&conn, "proxy_request_logs", &request_columns, "request_id")?
        } else {
            Vec::new()
        };
        Ok(Self {
            conn,
            day_columns,
            request_columns,
            schema_version,
            channel: 0,
            cursor: String::new(),
            progress: ReadProgress {
                rejected_rows: rejected_keys,
                ..ReadProgress::default()
            },
            namespace: namespace.into(),
            day_keys,
            request_keys,
            key_index: 0,
        })
    }

    pub fn progress(&self) -> &ReadProgress {
        &self.progress
    }

    pub fn read_batch(&mut self, limit: usize) -> Result<EvidenceBatch> {
        if limit == 0 || limit > MAX_BATCH_ROWS {
            return Err(LedgerError::InvalidInput(
                "CC batch size must be 1..2048".into(),
            ));
        }
        let start = format!("{}:{}", self.channel, self.cursor);
        let mut entries = Vec::with_capacity(limit);
        let mut missing_keys = Vec::new();
        let mut consumed = 0;
        while consumed < limit && self.channel < 2 {
            let (table, cols, key, manifest) = if self.channel == 0 {
                (
                    "usage_daily_rollups",
                    &self.day_columns,
                    daily_key(&self.day_columns),
                    &self.day_keys,
                )
            } else {
                (
                    "proxy_request_logs",
                    &self.request_columns,
                    "request_id".to_string(),
                    &self.request_keys,
                )
            };
            if self.key_index >= manifest.len() {
                self.channel += 1;
                self.cursor.clear();
                self.key_index = 0;
                continue;
            }
            let end = (self.key_index + limit - consumed).min(manifest.len());
            let wanted = &manifest[self.key_index..end];
            let fields = select_fields(cols, self.channel);
            let placeholders = vec!["?"; wanted.len()].join(",");
            let sql=format!("SELECT {key} AS usage_key,{fields} FROM {table} WHERE {key} COLLATE BINARY IN ({placeholders}) ORDER BY {key} COLLATE BINARY");
            let mut statement = self.conn.prepare(&sql)?;
            let mut rows = statement.query(rusqlite::params_from_iter(wanted.iter()))?;
            let mut found = BTreeSet::new();
            while let Some(row) = rows.next()? {
                let row_key: String = row.get(0)?;
                if !found.insert(row_key.clone()) {
                    return Err(LedgerError::UnsupportedVersion);
                }
                match parse_row(
                    row,
                    self.channel,
                    self.schema_version,
                    &self.namespace,
                    &row_key,
                ) {
                    Ok(evidence) => entries.push(evidence),
                    Err(
                        LedgerError::InvalidInput(_)
                        | LedgerError::InvalidUsageNumber
                        | LedgerError::RecordTooLarge
                        | LedgerError::MetricConflict
                        | LedgerError::NumericOverflow
                        | LedgerError::Sql(_),
                    ) => self.progress.rejected_rows += 1,
                    Err(error) => return Err(error),
                }
            }
            for wanted_key in wanted {
                if !found.contains(wanted_key) {
                    missing_keys.push(format!(
                        "{}:{}",
                        if self.channel == 0 { "day" } else { "request" },
                        digest(wanted_key.as_bytes())
                    ));
                    self.progress.missing_rows += 1;
                }
            }
            let count = wanted.len();
            consumed += count;
            if self.channel == 0 {
                self.progress.day_rows += count as u64;
            } else {
                self.progress.request_rows += count as u64;
            }
            self.cursor = wanted.last().cloned().unwrap_or_default();
            self.key_index = end;
        }
        // A full final batch can certify the finite key range immediately.
        if self.channel == 1 && self.key_index >= self.request_keys.len() {
            self.channel = 2;
        }
        if self.channel == 0
            && self.key_index >= self.day_keys.len()
            && self.request_keys.is_empty()
        {
            self.channel = 2;
        }
        self.progress.complete = self.channel >= 2;
        Ok(EvidenceBatch {
            entries,
            range_key: digest(format!("{start}:{}:{}", self.channel, self.cursor).as_bytes()),
            scanned_rows: consumed as u64,
            missing_keys,
            progress: self.progress.clone(),
        })
    }
}

/// A small fresh-work lane during a long fixed historical pass. This is supplemental:
/// old-date arrivals and modified old rows are still covered by complete reconciliation.
/// It does not advance or claim completion of the initial historical manifest.
fn read_recent_requests(
    ledger: &Ledger,
    root: &DurableRoot,
    path: &Path,
    namespace: &str,
    limit: usize,
) -> Result<Option<(EvidenceBatch, super::local_adapters::SqliteIncrementalBatch)>> {
    let reader = EvidenceReader::open_mode(path, namespace, false)?;
    if reader.request_columns.is_empty() {
        return Ok(None);
    }
    let Some(cursor) = super::local_adapters::sqlite_incremental_batch(
        ledger,
        root,
        path,
        &reader.conn,
        "proxy_request_logs",
        "request_id",
        "typeof(request_id)='text' AND length(request_id)>0",
        limit,
    )?
    else {
        return Ok(None);
    };
    drop(reader);
    let reader = EvidenceReader::open_mode(path, namespace, false)?;
    let fields = select_fields(&reader.request_columns, 1);
    let sql = format!(
        "SELECT request_id AS usage_key,{fields} FROM proxy_request_logs \
        WHERE request_id IN ({}) ORDER BY request_id",
        vec!["?"; cursor.keys.len()].join(",")
    );
    let mut statement = reader.conn.prepare(&sql)?;
    let mut rows = statement.query(rusqlite::params_from_iter(cursor.keys.iter()))?;
    let mut batch = EvidenceBatch {
        range_key: "recent-requests-v1".into(),
        ..EvidenceBatch::default()
    };
    while let Some(row) = rows.next()? {
        let key: String = row.get(0)?;
        batch.scanned_rows += 1;
        match parse_row(row, 1, reader.schema_version, namespace, &key) {
            Ok(entry) => batch.entries.push(entry),
            Err(
                LedgerError::InvalidInput(_)
                | LedgerError::InvalidUsageNumber
                | LedgerError::RecordTooLarge
                | LedgerError::MetricConflict
                | LedgerError::NumericOverflow
                | LedgerError::Sql(_),
            ) => batch.progress.rejected_rows += 1,
            Err(error) => return Err(error),
        }
    }
    Ok(Some((batch, cursor)))
}

fn read_keys(
    conn: &Connection,
    table: &str,
    cols: &BTreeSet<String>,
    key: &str,
) -> Result<Vec<String>> {
    if cols.is_empty() {
        return Ok(Vec::new());
    }
    let predicate = if table == "proxy_request_logs" {
        " WHERE typeof(request_id)='text' AND length(request_id)>0"
    } else {
        ""
    };
    let mut statement = conn.prepare(&format!(
        "SELECT {key} FROM {table}{predicate} ORDER BY {key} COLLATE BINARY"
    ))?;
    let keys: Vec<String> = statement
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    if keys.windows(2).any(|w| w[0] == w[1]) {
        return Err(LedgerError::UnsupportedVersion);
    }
    Ok(keys)
}

fn columns(conn: &Connection, table: &str) -> Result<BTreeSet<String>> {
    // Table names are private constants, not caller input.
    let mut query = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let result: BTreeSet<String> = query
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<std::result::Result<_, _>>()?;
    Ok(result
        .into_iter()
        .filter(|name| PROJECTION_FIELDS.contains(&name.as_str()))
        .collect())
}
fn require_columns(columns: &BTreeSet<String>, required: &[&str]) -> Result<()> {
    if !columns.is_empty() && required.iter().any(|c| !columns.contains(*c)) {
        return Err(LedgerError::UnsupportedVersion);
    }
    Ok(())
}
fn optional_column(cols: &BTreeSet<String>, name: &str) -> String {
    if cols.contains(name) {
        name.into()
    } else {
        "NULL".into()
    }
}
fn daily_key(cols: &BTreeSet<String>) -> String {
    format!(
        "json_array(date,app_type,provider_id,model,{},{})",
        optional_column(cols, "request_model"),
        optional_column(cols, "pricing_model")
    )
}
fn select_fields(cols: &BTreeSet<String>, channel: u8) -> String {
    let names = [
        if channel == 0 { "date" } else { "request_id" },
        "app_type",
        "provider_id",
        "model",
        "request_model",
        "pricing_model",
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_creation_tokens",
        "input_token_semantics",
        "request_count",
        "success_count",
        "total_cost_usd",
        "session_id",
        "data_source",
        "created_at",
        "status_code",
        "is_streaming",
    ];
    names
        .iter()
        .map(|name| optional_column(cols, name))
        .collect::<Vec<_>>()
        .join(",")
}
fn text(row: &Row<'_>, i: usize) -> Result<Option<String>> {
    let value: Option<String> = row.get(i)?;
    if value
        .as_ref()
        .is_some_and(|v| v.len() > MAX_TEXT_BYTES || v.contains('\0'))
    {
        return Err(LedgerError::RecordTooLarge);
    }
    Ok(value)
}
fn number(row: &Row<'_>, i: usize) -> Result<Option<i64>> {
    let value: Option<i64> = row.get(i)?;
    if value.is_some_and(|v| v < 0) {
        return Err(LedgerError::InvalidUsageNumber);
    }
    Ok(value)
}
fn insert<T: Serialize>(object: &mut Map<String, Value>, name: &str, value: T) -> Result<()> {
    object.insert(
        name.into(),
        serde_json::to_value(value)
            .map_err(|_| LedgerError::InvalidInput("cannot encode CC evidence".into()))?,
    );
    Ok(())
}
fn digest(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The upstream versioned normalization, kept per-row. Unknown semantics are never guessed.
/// A legacy inclusive cache greater than input keeps the upstream fresh interpretation, but
/// carries a reason so a future admission decision can refuse that ambiguity.
pub fn normalize_metrics(
    app: &str,
    semantics: Option<i64>,
    input: Option<i64>,
    cache_read: Option<i64>,
    cache_write: Option<i64>,
    output: Option<i64>,
    requests: Option<i64>,
) -> Result<(Metrics, Option<&'static str>)> {
    let mut out = [None; 10];
    if [input, cache_read, cache_write, output, requests]
        .into_iter()
        .flatten()
        .any(|v| v < 0)
    {
        return Err(LedgerError::InvalidUsageNumber);
    }
    let inclusive = matches!(app, "codex" | "gemini" | "grokbuild");
    let semantics = semantics.unwrap_or(0);
    let mut reason = None;
    out[0] = if !(0..=2).contains(&semantics) {
        reason = Some("CC_INPUT_SEMANTICS_UNKNOWN");
        None
    } else if semantics == 2 || !inclusive {
        input
    } else {
        let cached = if semantics == 0 {
            cache_read
        } else {
            match (cache_read, cache_write) {
                (Some(a), Some(b)) => Some(a.checked_add(b).ok_or(LedgerError::NumericOverflow)?),
                _ => None,
            }
        };
        match (input, cached) {
            (Some(i), Some(c)) if i >= c => Some(i - c),
            (Some(i), Some(_)) => {
                reason = Some("CC_LEGACY_INPUT_AMBIGUOUS");
                Some(i)
            }
            _ => {
                reason = Some("CC_INPUT_COMPONENT_UNKNOWN");
                None
            }
        }
    };
    out[1] = cache_read;
    out[2] = cache_write;
    out[6] = output;
    out[9] = requests;
    super::metrics::evaluate(&out)?;
    Ok((out, reason))
}

fn project_row(
    row: &Row<'_>,
    channel: u8,
    schema_version: i64,
    _namespace: &str,
    key: &str,
) -> Result<Value> {
    let first =
        text(row, 1)?.ok_or_else(|| LedgerError::InvalidInput("missing CC usage key".into()))?;
    let app = text(row, 2)?
        .filter(|v| !v.is_empty())
        .ok_or_else(|| LedgerError::InvalidInput("missing CC app type".into()))?;
    let provider = text(row, 3)?;
    let model = text(row, 4)?;
    let request_model = text(row, 5)?;
    let pricing_model = text(row, 6)?;
    let input = number(row, 7)?;
    let output = number(row, 8)?;
    let cache_read = number(row, 9)?;
    let cache_write = number(row, 10)?;
    let semantics = number(row, 11)?;
    let requests = if channel == 0 {
        number(row, 12)?
    } else {
        Some(1)
    };
    let success = number(row, 13)?;
    if let (Some(n), Some(ok)) = (requests, success) {
        if ok > n {
            return Err(LedgerError::InvalidUsageNumber);
        }
    }
    let cost = text(row, 14)?.filter(|s| valid_decimal(s));
    let session = text(row, 15)?;
    let data_source = text(row, 16)?;
    let created_at = number(row, 17)?;
    let status = number(row, 18)?;
    let streaming = number(row, 19)?;
    if channel == 0
        && !chrono::NaiveDate::parse_from_str(&first, "%Y-%m-%d")
            .is_ok_and(|date| date.format("%Y-%m-%d").to_string() == first)
    {
        return Err(LedgerError::InvalidInput("invalid CC usage day".into()));
    }
    if first.is_empty() {
        return Err(LedgerError::InvalidInput("empty CC request key".into()));
    }
    let (_, reason) = normalize_metrics(
        &app,
        semantics,
        input,
        cache_read,
        cache_write,
        output,
        requests,
    )?;
    let kind = if channel == 0 { "day" } else { "request" };
    let row_key = format!("{kind}:{}", digest(key.as_bytes()));
    let mut projection = Map::new();
    insert(&mut projection, "channel", kind)?;
    insert(&mut projection, "schema_version", schema_version)?;
    insert(&mut projection, "row_key", &row_key)?;
    insert(
        &mut projection,
        if channel == 0 { "date" } else { "request_id" },
        &first,
    )?;
    insert(&mut projection, "app_type", &app)?;
    insert(&mut projection, "provider_id", provider)?;
    insert(&mut projection, "model", &model)?;
    insert(&mut projection, "request_model", request_model)?;
    insert(&mut projection, "pricing_model", pricing_model)?;
    insert(&mut projection, "input_tokens", input)?;
    insert(&mut projection, "output_tokens", output)?;
    insert(&mut projection, "cache_read_tokens", cache_read)?;
    insert(&mut projection, "cache_creation_tokens", cache_write)?;
    insert(&mut projection, "input_token_semantics", semantics)?;
    insert(&mut projection, "request_count", requests)?;
    insert(&mut projection, "success_count", success)?;
    insert(&mut projection, "total_cost_usd", cost)?;
    insert(&mut projection, "normalization_version", 1)?;
    insert(&mut projection, "normalization_reason", reason)?;
    if channel != 0 {
        insert(&mut projection, "session_id", &session)?;
        insert(&mut projection, "data_source", data_source)?;
        insert(&mut projection, "created_at", created_at)?;
        insert(&mut projection, "status_code", status)?;
        insert(&mut projection, "is_streaming", streaming)?;
    }
    let projection = Value::Object(projection);
    Ok(projection)
}
fn parse_row(
    row: &Row<'_>,
    channel: u8,
    schema_version: i64,
    namespace: &str,
    key: &str,
) -> Result<UsageEvidence> {
    evidence_from_projection(
        &project_row(row, channel, schema_version, namespace, key)?,
        namespace,
    )
}
/// Reconstruct only the canonical, scalar usage projection saved by this adapter.
/// This does not grant admission, finality, account ownership, or a historical timezone.
pub fn evidence_from_projection(value: &Value, namespace: &str) -> Result<UsageEvidence> {
    let p = value
        .as_object()
        .ok_or_else(|| LedgerError::InvalidInput("invalid CC projection".into()))?;
    if namespace.is_empty()
        || namespace.len() > MAX_TEXT_BYTES
        || p.keys().any(|k| !PROJECTION_FIELDS.contains(&k.as_str()))
    {
        return Err(LedgerError::InvalidInput(
            "invalid CC projection fields".into(),
        ));
    }
    for v in p.values() {
        if !(v.is_null() || v.is_string() || v.as_i64().is_some())
            || v.as_str()
                .is_some_and(|s| s.len() > MAX_TEXT_BYTES || s.contains('\0'))
        {
            return Err(LedgerError::InvalidInput(
                "invalid CC projection scalar".into(),
            ));
        }
    }
    let s = |key: &str| -> Result<Option<&str>> {
        match p.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(v)) => Ok(Some(v)),
            _ => Err(LedgerError::InvalidInput("invalid CC text field".into())),
        }
    };
    let n = |key: &str| -> Result<Option<i64>> {
        match p.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => v
                .as_i64()
                .filter(|v| *v >= 0)
                .map(Some)
                .ok_or(LedgerError::InvalidUsageNumber),
        }
    };
    let kind = s("channel")?
        .filter(|v| matches!(*v, "day" | "request"))
        .ok_or(LedgerError::UnsupportedVersion)?;
    let app = s("app_type")?
        .filter(|s| !s.is_empty())
        .ok_or(LedgerError::UnsupportedVersion)?;
    if n("normalization_version")? != Some(1) || n("schema_version")?.is_none() {
        return Err(LedgerError::UnsupportedVersion);
    }
    let first = s(if kind == "day" { "date" } else { "request_id" })?
        .filter(|s| !s.is_empty())
        .ok_or(LedgerError::UnsupportedVersion)?;
    if kind == "day" && !valid_date(first) {
        return Err(LedgerError::InvalidInput("invalid CC date".into()));
    }
    let raw_key = if kind == "day" {
        serde_json::to_string(&serde_json::json!([
            first,
            app,
            s("provider_id")?,
            s("model")?,
            s("request_model")?,
            s("pricing_model")?
        ]))
        .map_err(|_| LedgerError::RecordTooLarge)?
    } else {
        first.into()
    };
    let row_key = format!("{kind}:{}", digest(raw_key.as_bytes()));
    if s("row_key")? != Some(row_key.as_str()) {
        return Err(LedgerError::HashCollision);
    }
    let requests = n("request_count")?;
    if (kind == "request" && requests != Some(1))
        || matches!((requests,n("success_count")?), (Some(a),Some(b)) if b>a)
    {
        return Err(LedgerError::InvalidUsageNumber);
    }
    let (metrics, reason) = normalize_metrics(
        app,
        n("input_token_semantics")?,
        n("input_tokens")?,
        n("cache_read_tokens")?,
        n("cache_creation_tokens")?,
        n("output_tokens")?,
        requests,
    )?;
    if s("normalization_reason")? != reason {
        return Err(LedgerError::HashCollision);
    }
    for key in ["created_at", "status_code", "is_streaming"] {
        n(key)?;
    }
    if s("total_cost_usd")?.is_some_and(|v| !valid_decimal(v)) {
        return Err(LedgerError::InvalidInput("invalid CC cost".into()));
    }
    let bytes = serde_json::to_vec(value).map_err(|_| LedgerError::RecordTooLarge)?;
    if bytes.len() > 16384 {
        return Err(LedgerError::RecordTooLarge);
    }
    let fingerprint = digest(&bytes);
    let model = s("model")?.map(str::to_owned);
    let mut observation = Observation {
        identity_family: "ccswitch".into(),
        namespace_key: namespace.into(),
        identity_key: None,
        candidate_key: format!("{row_key}:{fingerprint}"),
        record_key: row_key.clone(),
        parser_version: PARSER_VERSION.into(),
        semantics_version: 1,
        observed_ms: super::types::now_ms(),
        eligibility: if reason.is_some() {
            "inconsistent"
        } else {
            "identity_unknown"
        }
        .into(),
        fact_kind: kind.into(),
        report_source: "ccswitch-rollup".into(),
        native_tool: if matches!(app, "claude" | "codex" | "opencode" | "pi") {
            app
        } else {
            "unknown"
        }
        .into(),
        model_key: super::legacy::model_key(model.as_deref().unwrap_or("")),
        model_display: model,
        native_session_id: s("session_id")?.map(str::to_owned),
        metrics,
        native_request_id: (kind == "request").then(|| first.into()),
        native_date: (kind == "day").then(|| first.into()),
        occurred_ms: if kind == "request" {
            n("created_at")?
                .map(|v| v.checked_mul(1000).ok_or(LedgerError::NumericOverflow))
                .transpose()?
        } else {
            None
        },
        ..Observation::default()
    };
    observation.normalized_hash = super::claude::normalized_hash(&observation)?;
    Ok(UsageEvidence {
        row_key,
        fingerprint,
        observation,
        projection: value.clone(),
    })
}
fn valid_date(s: &str) -> bool {
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .is_ok_and(|d| d.format("%Y-%m-%d").to_string() == s)
}

#[derive(Clone, Debug, Serialize)]
pub struct ReportedCandidate {
    pub consumer_tool: String,
    pub grain: String,
    pub canonical_identity: Option<String>,
    pub native_session_id: Option<String>,
    pub native_message_id: Option<String>,
    pub session_key: Option<String>,
    pub native_date: Option<String>,
    pub occurred_ms: Option<i64>,
    pub metrics: Metrics,
    pub model_key: String,
    pub model_display: Option<String>,
    pub value_basis: String,
    pub finality: String,
    pub time_basis: String,
}
/// Narrow source-report admission evidence. Schema recognition certifies the field
/// interpretation, not the producer version of every historical row or native finality.
pub fn reported_candidate(p: &Value) -> Result<Option<ReportedCandidate>> {
    let e = evidence_from_projection(p, NAMESPACE)?;
    let o = e.observation;
    if p["schema_version"].as_i64() != Some(18)
        || !p["input_token_semantics"]
            .as_i64()
            .is_some_and(|n| (0..=2).contains(&n))
        || !p["normalization_reason"].is_null()
        || [0, 1, 2, 6, 9].iter().any(|i| o.metrics[*i].is_none())
    {
        return Ok(None);
    }
    let mut out = ReportedCandidate {
        consumer_tool: o.native_tool.clone(),
        grain: o.fact_kind.clone(),
        canonical_identity: None,
        native_session_id: o.native_session_id.clone(),
        native_message_id: None,
        session_key: None,
        native_date: o.native_date.clone(),
        occurred_ms: o.occurred_ms,
        metrics: o.metrics,
        model_key: o.model_key,
        model_display: o.model_display,
        value_basis: "ccswitch_schema18_source_reported".into(),
        finality: "not_proven".into(),
        time_basis: "source_timestamp".into(),
    };
    match (o.native_tool.as_str(), o.fact_kind.as_str()) {
        ("opencode", "request") if p["data_source"].as_str() == Some("opencode_session") => {
            let Some(session) = p["session_id"]
                .as_str()
                .filter(|s| !s.is_empty() && !s.contains(':'))
            else {
                return Ok(None);
            };
            let Some(rest) = p["request_id"]
                .as_str()
                .and_then(|s| s.strip_prefix("opencode_session:"))
            else {
                return Ok(None);
            };
            let Some((native_session, message)) = rest.split_once(':') else {
                return Ok(None);
            };
            if native_session != session
                || message.is_empty()
                || message.contains(':')
                || o.occurred_ms.is_none()
            {
                return Ok(None);
            };
            out.native_message_id = Some(message.into());
            out.canonical_identity = Some(super::types::identity_key(
                "opencode",
                "unknown:opencode",
                "message",
                &[session, message],
            ));
            out.session_key = Some(super::types::identity_key(
                "opencode",
                "unknown:opencode",
                "session",
                &[session],
            ));
        }
        ("pi", "day") => {
            if p["success_count"].as_i64().is_none()
                || p["input_token_semantics"].as_i64() != Some(2)
            {
                return Ok(None);
            };
            out.time_basis = "historical_local_date_timezone_required".into();
            out.finality = "source_day_report".into();
        }
        _ => return Ok(None),
    }
    Ok(Some(out))
}
/// A native identity probe is used solely for deletion closure, never to grant eligibility.
pub(crate) fn reported_identity_probe(p: &Value) -> Result<Option<Observation>> {
    let Some(c) = reported_candidate(p)? else {
        return Ok(None);
    };
    let Some(identity) = c.canonical_identity else {
        return Ok(None);
    };
    let mut o = evidence_from_projection(p, NAMESPACE)?.observation;
    o.identity_family = c.consumer_tool.clone();
    o.report_source = c.consumer_tool.clone();
    o.namespace_key = format!("unknown:{}", c.consumer_tool);
    o.identity_key = Some(identity);
    o.native_message_id = c.native_message_id;
    o.session_key = c.session_key;
    Ok(Some(o))
}
fn valid_decimal(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 80
        && s.bytes().all(|b| b.is_ascii_digit() || b == b'.')
        && s.bytes().filter(|b| *b == b'.').count() <= 1
        && s.bytes().any(|b| b.is_ascii_digit())
}

/// Explicit locations win. Otherwise mirror the official override key, with a bounded read
/// of that path-only file. Never probe arbitrary directories or load settings/provider data.
pub fn discover_databases(explicit: &[PathBuf]) -> Vec<PathBuf> {
    if !explicit.is_empty() {
        return unique_existing(explicit.iter().map(|path| database_at(path)).collect());
    }
    let home = crate::common::home();
    let config = official_path_config(&home);
    let override_db = config.as_deref().and_then(|p| override_database(p, &home));
    let mut candidates = Vec::new();
    if let Some(p) = override_db {
        candidates.push(p);
    }
    candidates.push(home.join(".cc-switch").join("cc-switch.db"));
    unique_existing(candidates)
}
fn database_at(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.join("cc-switch.db")
    } else {
        path.to_path_buf()
    }
}
fn unique_existing(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = BTreeSet::new();
    paths
        .into_iter()
        .filter(|p| p.is_file())
        .filter_map(|p| std::fs::canonicalize(p).ok())
        .filter(|p| seen.insert(p.clone()))
        .collect()
}
fn official_path_config(home: &Path) -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        Some(home.join("Library/Application Support/com.ccswitch.desktop/app_paths.json"))
    }
    #[cfg(target_os = "windows")]
    {
        std::env::var_os("APPDATA")
            .map(|p| PathBuf::from(p).join("com.ccswitch.desktop/app_paths.json"))
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        Some(
            std::env::var_os("XDG_DATA_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".local/share"))
                .join("com.ccswitch.desktop/app_paths.json"),
        )
    }
}
pub fn override_database(config: &Path, home: &Path) -> Option<PathBuf> {
    if std::fs::metadata(config).ok()?.len() > MAX_PATH_CONFIG_BYTES {
        return None;
    }
    let data: Value = serde_json::from_slice(&std::fs::read(config).ok()?).ok()?;
    let value = data.get("app_config_dir_override")?.as_str()?.trim();
    if value.is_empty() || value.len() > 4096 || value.contains('\0') {
        return None;
    }
    let path = if value == "~" {
        home.to_path_buf()
    } else if let Some(rest) = value
        .strip_prefix("~/")
        .or_else(|| value.strip_prefix("~\\"))
    {
        home.join(rest)
    } else {
        PathBuf::from(value)
    };
    if !path.is_absolute() {
        return None;
    }
    let db = path.join("cc-switch.db");
    db.is_file().then_some(db)
}

use super::durable::{AdapterSpec, DurableEvidence, DurableRoot, ObjectSpec};
use super::Ledger;

pub fn adapter_spec() -> AdapterSpec {
    AdapterSpec {
        identity_family: "ccswitch",
        collector_key: COLLECTOR_KEY,
        report_source: "ccswitch-rollup",
        native_tool: "ccswitch",
        parser_version: PARSER_VERSION,
        capability_version: CAPABILITY_VERSION,
        identity_version: 1,
        semantics_version: 1,
    }
}

#[derive(Default)]
pub struct Collector {
    next_root: usize,
    completed_cycle_roots: BTreeSet<String>,
}
#[derive(Debug, Default, Serialize)]
pub struct PollSummary {
    pub discovered_databases: usize,
    pub read_rows: u64,
    pub inserted: u64,
    pub duplicates: u64,
    pub suppressed: u64,
    pub rejected_rows: u64,
    pub missing_rows: u64,
    pub completed_passes: u64,
    pub active_passes: usize,
    pub failed_databases: usize,
}
#[derive(Clone, Serialize, Deserialize)]
struct ValueBlock {
    format: u32,
    fields: Vec<String>,
    /// Null is a forgotten row: its finite target position remains, its values do not.
    rows: Vec<Option<Vec<Value>>>,
    gap_count: u64,
}
impl ValueBlock {
    fn projection(&self, row: &[Value]) -> Result<Value> {
        if self.format != 2
            || self.fields.len() != row.len()
            || self
                .fields
                .iter()
                .any(|k| !PROJECTION_FIELDS.contains(&k.as_str()))
            || self.fields.iter().collect::<BTreeSet<_>>().len() != self.fields.len()
        {
            return Err(LedgerError::UnsupportedVersion);
        };
        Ok(Value::Object(
            self.fields
                .iter()
                .cloned()
                .zip(row.iter().cloned())
                .collect(),
        ))
    }
}
fn decode_block(bytes: &[u8]) -> Result<ValueBlock> {
    if bytes.len() > 65536 {
        return Err(LedgerError::RecordTooLarge);
    };
    let b: ValueBlock = serde_json::from_slice(bytes).map_err(|_| LedgerError::CorruptLedger)?;
    if b.format != 2 || b.rows.len() > MAX_BATCH_ROWS {
        return Err(LedgerError::UnsupportedVersion);
    };
    Ok(b)
}
pub(crate) fn validate_snapshot_checkpoint(bytes: &[u8]) -> Result<i64> {
    let b = decode_block(bytes)?;
    for row in b.rows.iter().flatten() {
        b.projection(row)?;
    }
    i64::try_from(b.rows.len()).map_err(|_| LedgerError::NumericOverflow)
}
pub(crate) fn snapshot_gap_count(bytes: &[u8]) -> Result<u64> {
    Ok(decode_block(bytes)?.gap_count)
}
pub(crate) fn has_privacy_controls(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM suppression_rules) OR EXISTS(SELECT 1 FROM adoption_forget_ranges)",[],|r|r.get(0))?)
}
pub(crate) fn snapshot_members(bytes: &[u8]) -> Result<(u64, Vec<UsageEvidence>)> {
    let b = decode_block(bytes)?;
    Ok((
        b.gap_count,
        b.rows
            .iter()
            .flatten()
            .map(|r| evidence_from_projection(&b.projection(r)?, NAMESPACE))
            .collect::<Result<_>>()?,
    ))
}
/// Call inside the forget transaction after its rules have been inserted. A block
/// may contain unrelated unread rows, so erase only selected rows and keep offsets.
pub(crate) fn scrub_snapshot_checkpoints(
    conn: &Connection,
    predicate: impl FnMut(&Observation) -> Result<bool>,
) -> Result<u64> {
    scrub_snapshot_blocks(conn, None, predicate)
}
pub(crate) fn scrub_snapshot_object(
    conn: &Connection,
    object_id: i64,
    predicate: impl FnMut(&Observation) -> Result<bool>,
) -> Result<u64> {
    scrub_snapshot_blocks(conn, Some(object_id), predicate)
}
fn scrub_snapshot_blocks(
    conn: &Connection,
    object_id: Option<i64>,
    mut predicate: impl FnMut(&Observation) -> Result<bool>,
) -> Result<u64> {
    let objects:Vec<(i64,String,Vec<u8>)>=conn.prepare("SELECT object_id,content_generation,checkpoint FROM source_objects WHERE object_key LIKE 'cc-values-v2:%' AND checkpoint IS NOT NULL AND (?1 IS NULL OR object_id=?1)")?
        .query_map([object_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?.collect::<rusqlite::Result<_>>()?;
    let mut removed = 0;
    for (object_id, generation, bytes) in objects {
        if bytes == b"sealed-v2-complete" {
            continue;
        }
        let mut b = decode_block(&bytes)?;
        let mut dirty = false;
        for i in 0..b.rows.len() {
            let Some(row) = b.rows[i].as_ref() else {
                continue;
            };
            let p = b.projection(row)?;
            let mut e = evidence_from_projection(&p, NAMESPACE)?;
            e.observation.object_id = object_id;
            let probe = reported_identity_probe(&p)?;
            let matched = predicate(&e.observation)?
                || probe
                    .as_ref()
                    .map(&mut predicate)
                    .transpose()?
                    .unwrap_or(false)
                || super::adoption::suppresses_projection(conn, &p)?;
            if matched {
                super::maintenance::add_incoming_controls(conn, &e.observation)?;
                if let Some(probe) = &probe {
                    super::maintenance::add_incoming_controls(conn, probe)?;
                }
                b.rows[i] = None;
                dirty = true;
                removed += 1;
                let count = conn.execute(
                    "DELETE FROM durable_snapshot_members WHERE snapshot_id=?1 AND row_key=?2",
                    rusqlite::params![generation, e.row_key],
                )?;
                conn.execute(
                    "UPDATE durable_snapshots SET gap_count=gap_count+?2 WHERE snapshot_id=?1",
                    rusqlite::params![generation, count as i64],
                )?;
            }
        }
        if dirty {
            conn.execute("UPDATE source_objects SET checkpoint=?2,object_revision=object_revision+1 WHERE object_id=?1",rusqlite::params![object_id,serde_json::to_vec(&b).map_err(|_|LedgerError::RecordTooLarge)?])?;
        }
    }
    Ok(removed)
}
#[derive(Clone, Debug, Serialize)]
pub struct SealedDayMembers {
    pub snapshot_id: String,
    pub observation_ids: Vec<i64>,
}
/// Exact membership in a single sealed *source report*. This proves completeness
/// of those source rows, not that source days and request details are disjoint.
pub fn sealed_day_members(
    conn: &Connection,
    root_id: &str,
    tool: &str,
    from: &str,
    to_exclusive: &str,
) -> Result<SealedDayMembers> {
    use rusqlite::OptionalExtension;
    if tool != "pi" || !valid_date(from) || !valid_date(to_exclusive) || from >= to_exclusive {
        return Err(LedgerError::InvalidInput("invalid source day scope".into()));
    };
    let snapshot:Option<(String,i64)>=conn.query_row("SELECT snapshot_id,gap_count FROM durable_snapshots WHERE root_id=?1 AND state='sealed' ORDER BY sealed_ms DESC,rowid DESC LIMIT 1",[root_id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let Some((snapshot_id, gap)) = snapshot else {
        return Err(LedgerError::InvalidInput(
            "sealed source snapshot required".into(),
        ));
    };
    if gap != 0 {
        return Err(LedgerError::InvalidInput(
            "source snapshot has evidence gaps".into(),
        ));
    };
    let rows:Vec<Option<i64>>=conn.prepare("SELECT observation_id FROM durable_snapshot_members WHERE snapshot_id=?1 AND consumer_tool=?2 AND grain='day' AND native_date>=?3 AND native_date<?4 ORDER BY native_date,row_key")?
        .query_map(rusqlite::params![snapshot_id,tool,from,to_exclusive],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
    if rows.is_empty() || rows.iter().any(Option::is_none) {
        return Err(LedgerError::InvalidInput(
            "source day snapshot is incomplete or forgotten".into(),
        ));
    };
    Ok(SealedDayMembers {
        snapshot_id,
        observation_ids: rows.into_iter().flatten().collect(),
    })
}
impl Collector {
    /// Start another explicit audit cycle. A caller draining other adapters
    /// must not recapture a completed CC root on every publication slice.
    pub fn begin_audit_cycle(&mut self) {
        self.completed_cycle_roots.clear();
    }

    /// Drain at most one sealed value snapshot per root in this audit cycle.
    /// Newly discovered roots, unfinished sealed work and the recent-request
    /// lane still run after a root's historical pass has completed.
    pub fn poll_cycle(
        &mut self,
        ledger: &mut Ledger,
        paths: &[PathBuf],
        row_budget: usize,
    ) -> Result<PollSummary> {
        self.poll_mode(ledger, paths, row_budget, true)
    }

    /// Each poll closes every external read transaction before returning. A sealed value
    /// snapshot survives source mutation, removal, restart, and pause. A failed unsealed
    /// attempt owns no targets. Later complete passes also revisit old dates.
    pub fn poll(
        &mut self,
        ledger: &mut Ledger,
        paths: &[PathBuf],
        row_budget: usize,
    ) -> Result<PollSummary> {
        self.poll_mode(ledger, paths, row_budget, false)
    }

    fn poll_mode(
        &mut self,
        ledger: &mut Ledger,
        paths: &[PathBuf],
        row_budget: usize,
        finite_cycle: bool,
    ) -> Result<PollSummary> {
        if row_budget == 0 || row_budget > 65_536 {
            return Err(LedgerError::InvalidInput(
                "CC poll budget must be 1..65536".into(),
            ));
        }
        let discovered = discover_databases(paths);
        let mut summary = PollSummary {
            discovered_databases: discovered.len(),
            ..PollSummary::default()
        };
        let mut active = discovered;
        // Previously discovered missing roots still have a durable initial target to finish.
        let known:Vec<PathBuf>=ledger.connection().prepare("SELECT locator FROM source_roots WHERE identity_family='ccswitch' AND report_source='ccswitch-rollup'")?
            .query_map([],|r|r.get::<_,String>(0).map(PathBuf::from))?.collect::<rusqlite::Result<_>>()?;
        for path in known {
            if !active.contains(&path) {
                active.push(path);
            }
        }
        if active.is_empty() {
            return Ok(summary);
        }
        let first = self.next_root % active.len();
        let mut remaining = row_budget;
        for step in 0..active.len() {
            if remaining == 0 {
                break;
            }
            let path = &active[(first + step) % active.len()];
            let root = ledger.register_durable_root(&adapter_spec(), NAMESPACE, path)?;
            if !root.enabled {
                continue;
            }
            ledger.retire_cc_key_manifests(&root)?;
            let retry: Option<i64> = ledger.connection().query_row(
                "SELECT retry_after_ms FROM source_roots WHERE root_id=?1",
                [&root.root_id],
                |r| r.get(0),
            )?;
            if retry.is_some_and(|due| due > super::types::now_ms())
                && pending_count(ledger, &root)? == 0
            {
                // Keep the saved source cursor and error visible without reopening
                // a locked or unhealthy upstream database on every scheduler tick.
                summary.failed_databases += 1;
                continue;
            }
            let slice = (remaining / (active.len() - step)).max(1);
            let allow_stage = !finite_cycle
                || (!self.completed_cycle_roots.contains(&root.root_id)
                    && super::collection::history_audit_due(ledger.connection(), &root)?);
            match poll_root(ledger, &root, path, slice, allow_stage) {
                Ok(done) => {
                    if finite_cycle && done.completed_passes > 0 {
                        self.completed_cycle_roots.insert(root.root_id.clone());
                    }
                    ledger.connection().execute("UPDATE source_roots SET error_code=NULL,failure_count=0,retry_after_ms=NULL WHERE root_id=?1",[&root.root_id])?;
                    remaining = remaining.saturating_sub(done.read_rows as usize);
                    summary.read_rows += done.read_rows;
                    summary.inserted += done.inserted;
                    summary.duplicates += done.duplicates;
                    summary.suppressed += done.suppressed;
                    summary.rejected_rows += done.rejected_rows;
                    summary.missing_rows += done.missing_rows;
                    summary.completed_passes += done.completed_passes;
                    summary.active_passes += done.active_passes;
                    summary.failed_databases += done.failed_databases;
                }
                Err(error)
                    if matches!(
                        error,
                        LedgerError::Busy
                            | LedgerError::Io(_)
                            | LedgerError::PermissionDenied
                            | LedgerError::CorruptLedger
                            | LedgerError::UnsupportedVersion
                            | LedgerError::RecordTooLarge
                            | LedgerError::Sql(_)
                    ) =>
                {
                    // An unavailable external root cannot stop another root or other adapters.
                    // Keep its persisted cursor untouched. Do not expose source row contents.
                    summary.failed_databases += 1;
                    ledger.connection().execute("UPDATE source_roots SET error_code=?2,failure_count=failure_count+1,retry_after_ms=?3 WHERE root_id=?1",rusqlite::params![root.root_id,error.code(),super::types::now_ms()+60_000])?;
                }
                Err(error) => return Err(error),
            }
        }
        self.next_root = (first + 1) % active.len();
        Ok(summary)
    }
}
fn pending_count(ledger: &Ledger, root: &DurableRoot) -> Result<i64> {
    Ok(ledger.connection().query_row("SELECT COUNT(*) FROM source_objects WHERE root_id=?1 AND object_key LIKE 'cc-values-v2:%' AND target_done=0",[&root.root_id],|r|r.get(0))?)
}
fn poll_root(
    ledger: &mut Ledger,
    root: &DurableRoot,
    path: &Path,
    budget: usize,
    allow_stage: bool,
) -> Result<PollSummary> {
    let mut result = PollSummary::default();
    let pending = pending_count(ledger, root)?;
    let historical_pass = pending > 0 || allow_stage;
    // The optional fresh lane cannot prevent recovery from sealed values.
    let fresh_budget = if budget < 4 && !allow_stage && pending == 0 {
        budget
    } else {
        (budget / 4).min(256)
    };
    if fresh_budget > 0 && path.is_file() {
        match read_recent_requests(ledger, root, path, NAMESPACE, fresh_budget) {
            Ok(Some((recent, cursor))) => {
                let saved = save_recent(ledger, root, path, &recent)?;
                result.inserted += saved.inserted;
                result.duplicates += saved.duplicates;
                result.suppressed += saved.suppressed;
                result.read_rows += recent.scanned_rows;
                result.rejected_rows += recent.progress.rejected_rows;
                result.active_passes += usize::from(cursor.has_more);
                cursor.commit(ledger)?;
            }
            Ok(None) => {}
            Err(_) => result.failed_databases += 1,
        }
    }
    if pending == 0 && allow_stage {
        if path.is_file() {
            stage_values(ledger, root, path)?;
        } else {
            return Err(LedgerError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "CC source unavailable for a new value snapshot",
            )));
        }
    }
    let mut left = budget.saturating_sub(result.read_rows as usize);
    for mut object in ledger.next_durable_objects(root, 1024)? {
        if left == 0 {
            break;
        };
        let Some(bytes) = object.checkpoint.as_deref() else {
            continue;
        };
        let Ok(block) = decode_block(bytes) else {
            continue;
        };
        let start =
            usize::try_from(object.saved_offset).map_err(|_| LedgerError::NumericOverflow)?;
        if start > block.rows.len() {
            return Err(LedgerError::CorruptLedger);
        };
        let end = (start + left).min(block.rows.len());
        let mut evidence = Vec::new();
        for row in block.rows[start..end].iter().flatten() {
            let e = evidence_from_projection(&block.projection(row)?, &root.namespace)?;
            evidence.push(DurableEvidence {
                observation: e.observation,
                projection: Some(e.projection),
            });
        }
        let checkpoint = bytes.to_vec();
        let saved = ledger.save_durable_evidence_batch_with_gap(
            &mut object,
            &evidence,
            end as i64,
            &checkpoint,
            end == block.rows.len(),
            (block.gap_count > 0).then_some("CC_HISTORICAL_EVIDENCE_GAP"),
        )?;
        result.inserted += saved.inserted;
        result.duplicates += saved.duplicates;
        result.suppressed += saved.suppressed;
        result.read_rows += (end - start) as u64;
        if start == 0 {
            result.rejected_rows += block.gap_count;
        }
        left = left.saturating_sub(end - start);
    }
    if pending_count(ledger, root)? == 0 {
        result.completed_passes = u64::from(historical_pass);
        if historical_pass && result.rejected_rows == 0 && result.failed_databases == 0 {
            let gaps:bool=ledger.connection().query_row("SELECT EXISTS(SELECT 1 FROM source_objects WHERE root_id=?1 AND object_key LIKE 'cc-values-v2:%' AND error_code IS NOT NULL)",[&root.root_id],|r|r.get(0))?;
            if !gaps {
                super::collection::complete_history_audit(ledger.connection(), root)?;
            }
        }
    } else {
        result.active_passes = 1;
    }
    Ok(result)
}
fn stage_values(ledger: &mut Ledger, root: &DurableRoot, path: &Path) -> Result<()> {
    let generation = ledger.begin_durable_snapshot(root)?;
    let captured = capture_values(path, &root.namespace);
    let result = match captured {
        Ok((objects, fingerprint)) => {
            ledger.seal_durable_snapshot(root, &generation, &objects, &fingerprint)
        }
        Err(e) => Err(e),
    };
    if let Err(e) = &result {
        ledger.abort_durable_snapshot(&generation, e.code())?;
    }
    result
}
fn capture_values(
    path: &Path,
    namespace: &str,
) -> Result<(Vec<super::durable::SnapshotObject>, String)> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let reader = EvidenceReader::open_mode(path, namespace, false)?;
    reader
        .conn
        .progress_handler(1000, Some(move || std::time::Instant::now() >= deadline))?;
    let mut blocks = Vec::new();
    let mut total_bytes = 0usize;
    let mut rejected = if reader.request_columns.is_empty() {
        0
    } else {
        u64::try_from(reader.conn.query_row("SELECT COUNT(*) FROM proxy_request_logs WHERE typeof(request_id)<>'text' OR length(request_id)=0",[],|r|r.get::<_,i64>(0))?).map_err(|_|LedgerError::NumericOverflow)?
    };
    let mut seal_hash = Sha256::new();
    let flush = |b: &mut ValueBlock,
                 blocks: &mut Vec<super::durable::SnapshotObject>,
                 total: &mut usize,
                 hash: &mut Sha256|
     -> Result<()> {
        if b.rows.is_empty() && b.gap_count == 0 {
            return Ok(());
        };
        let bytes = serde_json::to_vec(b).map_err(|_| LedgerError::RecordTooLarge)?;
        *total = total
            .checked_add(bytes.len())
            .ok_or(LedgerError::NumericOverflow)?;
        if *total > 64 * 1024 * 1024 || bytes.len() > 65536 {
            return Err(LedgerError::RecordTooLarge);
        };
        hash.update(&bytes);
        blocks.push(super::durable::SnapshotObject {
            object_key: format!("cc-values-v2:{:08}", blocks.len()),
            row_count: b.rows.len() as i64,
            checkpoint: bytes,
        });
        b.rows.clear();
        b.gap_count = 0;
        Ok(())
    };
    for (channel, table, cols, key) in [
        (
            0,
            "usage_daily_rollups",
            &reader.day_columns,
            daily_key(&reader.day_columns),
        ),
        (
            1,
            "proxy_request_logs",
            &reader.request_columns,
            "request_id".into(),
        ),
    ] {
        if cols.is_empty() {
            continue;
        };
        let predicate = if channel == 1 {
            " WHERE typeof(request_id)='text' AND length(request_id)>0"
        } else {
            ""
        };
        let mut query = reader.conn.prepare(&format!(
            "SELECT {key},{} FROM {table}{predicate} ORDER BY {key} COLLATE BINARY",
            select_fields(cols, channel)
        ))?;
        let mut rows = query.query([])?;
        let mut prior = None;
        let mut block = ValueBlock {
            format: 2,
            fields: Vec::new(),
            rows: Vec::new(),
            gap_count: 0,
        };
        let mut block_bytes = 0;
        while let Some(row) = rows.next()? {
            if std::time::Instant::now() >= deadline {
                return Err(LedgerError::Busy);
            };
            let key: String = row.get(0)?;
            if prior.as_ref() == Some(&key) {
                return Err(LedgerError::UnsupportedVersion);
            };
            prior = Some(key.clone());
            let projection = match project_row(row, channel, reader.schema_version, namespace, &key)
            {
                Ok(v) => v,
                Err(
                    LedgerError::InvalidInput(_)
                    | LedgerError::InvalidUsageNumber
                    | LedgerError::RecordTooLarge
                    | LedgerError::MetricConflict
                    | LedgerError::NumericOverflow
                    | LedgerError::Sql(_),
                ) => {
                    rejected += 1;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let p = projection.as_object().ok_or(LedgerError::CorruptLedger)?;
            if block.fields.is_empty() {
                block.fields = p.keys().cloned().collect();
            }
            let values: Vec<Value> = block.fields.iter().map(|key| p[key].clone()).collect();
            let size = serde_json::to_vec(&values)
                .map_err(|_| LedgerError::RecordTooLarge)?
                .len()
                + 1;
            if !block.rows.is_empty()
                && (block_bytes + size > 55 * 1024 || block.rows.len() >= DEFAULT_BATCH_ROWS)
            {
                flush(&mut block, &mut blocks, &mut total_bytes, &mut seal_hash)?;
                block_bytes = 0;
            }
            block.rows.push(Some(values));
            block_bytes += size;
        }
        flush(&mut block, &mut blocks, &mut total_bytes, &mut seal_hash)?;
    }
    if rejected > 0 {
        let mut gap = ValueBlock {
            format: 2,
            fields: Vec::new(),
            rows: Vec::new(),
            gap_count: rejected,
        };
        flush(&mut gap, &mut blocks, &mut total_bytes, &mut seal_hash)?;
    }
    drop(reader); // the finite read snapshot is released before any target is persisted
    Ok((blocks, format!("{:x}", seal_hash.finalize())))
}
fn save_recent(
    ledger: &mut Ledger,
    root: &DurableRoot,
    path: &Path,
    batch: &EvidenceBatch,
) -> Result<super::durable::SaveSummary> {
    if batch.entries.is_empty() {
        return Ok(super::durable::SaveSummary::default());
    }
    let mut fingerprints = Vec::new();
    for e in &batch.entries {
        fingerprints.extend_from_slice(e.fingerprint.as_bytes());
    }
    let generation = digest(&fingerprints);
    let mut object = ledger.discover_durable_object(
        root,
        ObjectSpec {
            object_key: "cc-recent-v1".into(),
            locator: format!("{}#recent-requests", path.display()),
            content_generation: generation,
            byte_length: batch.scanned_rows as i64,
            mtime_ns: None,
        },
    )?;
    if object.complete {
        return Ok(super::durable::SaveSummary {
            duplicates: batch.entries.len() as u64,
            ..super::durable::SaveSummary::default()
        });
    }
    let evidence: Vec<_> = batch
        .entries
        .iter()
        .map(|e| DurableEvidence {
            observation: e.observation.clone(),
            projection: Some(e.projection.clone()),
        })
        .collect();
    ledger.save_durable_evidence_batch(
        &mut object,
        &evidence,
        batch.scanned_rows as i64,
        b"recent-v1",
        true,
    )
}
