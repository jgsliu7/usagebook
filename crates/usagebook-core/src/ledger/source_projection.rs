//! Derived, body-free SQLite input for the existing source readers. The ledger
//! remains the authority for adopted history; these rows only replace repeated
//! reads of upstream databases during background report construction.
use super::{durable::DurableRoot, types::now_ms, LedgerError, Result};
use rusqlite::{params, types::Value as SqlValue, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub(crate) enum SqlCell {
    Null,
    Integer(i64),
    // Preserve the exact IEEE value without a JSON decimal round-trip.
    RealBits(u64),
    Text(String),
}
impl SqlCell {
    pub fn from_sql(value: SqlValue) -> Result<Self> {
        Ok(match value {
            SqlValue::Null => Self::Null,
            SqlValue::Integer(n) => Self::Integer(n),
            SqlValue::Real(n) if n.is_finite() => Self::RealBits(n.to_bits()),
            SqlValue::Text(s) if s.len() <= 65536 => Self::Text(s),
            _ => return Err(LedgerError::RecordTooLarge),
        })
    }
    fn sql(self) -> Result<SqlValue> {
        Ok(match self {
            Self::Null => SqlValue::Null,
            Self::Integer(n) => SqlValue::Integer(n),
            Self::RealBits(bits) if f64::from_bits(bits).is_finite() => {
                SqlValue::Real(f64::from_bits(bits))
            }
            Self::Text(s) if s.len() <= 65536 => SqlValue::Text(s),
            _ => return Err(LedgerError::CorruptLedger),
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Column {
    pub name: String,
    pub declared_type: String,
}
type Layout = BTreeMap<String, Vec<Column>>;
pub(crate) struct SavedDatabase {
    pub original_path: PathBuf,
    pub reader_domain: Option<String>,
    pub connection: Connection,
}
fn quoted(value: &str) -> Result<String> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_')
    {
        return Err(LedgerError::CorruptLedger);
    }
    Ok(format!("\"{value}\""))
}
fn epoch(conn: &Connection) -> Result<String> {
    Ok(
        conn.query_row("SELECT epoch FROM ledger_meta WHERE singleton=1", [], |r| {
            r.get(0)
        })?,
    )
}
pub(crate) fn begin(conn: &Connection, root: &DurableRoot) -> Result<()> {
    let epoch = epoch(conn)?;
    let old: Option<(String, String)> = conn
        .query_row(
            "SELECT epoch,capability_version FROM source_projection_scopes WHERE root_id=?1",
            [&root.root_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if old
        .as_ref()
        .is_some_and(|old| old.0 != epoch || old.1 != root.spec.capability_version)
    {
        conn.execute(
            "DELETE FROM source_projection_scopes WHERE root_id=?1",
            [&root.root_id],
        )?;
    }
    let locator: String = conn.query_row(
        "SELECT locator FROM source_roots WHERE root_id=?1",
        [&root.root_id],
        |r| r.get(0),
    )?;
    let path = std::path::Path::new(&locator);
    let reader_domain = reader_domain(path, root.spec.report_source);
    conn.execute("INSERT INTO source_projection_scopes(root_id,epoch,capability_version,report_source,layout_json,complete,observed_ms,reader_domain) VALUES(?1,?2,?3,?4,'{}',0,?5,?6) ON CONFLICT(root_id) DO UPDATE SET complete=0,error_code=NULL,observed_ms=excluded.observed_ms,reader_domain=excluded.reader_domain",params![root.root_id,epoch,root.spec.capability_version,root.spec.report_source,now_ms(),reader_domain])?;

    Ok(())
}
pub(crate) fn put_rows(
    conn: &Connection,
    root: &DurableRoot,
    table: &str,
    columns: &[Column],
    rows: &[(String, Vec<SqlCell>)],
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    let conn = &*tx;
    quoted(table)?;
    for column in columns {
        quoted(&column.name)?;
    }
    let stored:Option<(String,String,String)> = conn.query_row("SELECT epoch,capability_version,layout_json FROM source_projection_scopes WHERE root_id=?1",[&root.root_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let Some((stored_epoch, capability, json)) = stored else {
        return Err(LedgerError::IncompleteEvidence);
    };
    if stored_epoch != epoch(conn)? || capability != root.spec.capability_version {
        return Err(LedgerError::IncompleteEvidence);
    }
    let mut layout: Layout = serde_json::from_str(&json).map_err(|_| LedgerError::CorruptLedger)?;
    if let Some(previous) = layout.get(table) {
        if serde_json::to_string(previous).ok() != serde_json::to_string(columns).ok() {
            conn.execute(
                "DELETE FROM source_projection_rows WHERE root_id=?1 AND table_name=?2",
                params![root.root_id, table],
            )?;
            conn.execute(
                "UPDATE source_projection_scopes SET complete=0 WHERE root_id=?1",
                [&root.root_id],
            )?;
            super::collection::invalidate_history_audit(conn, root)?;
        }
    }
    layout.insert(table.into(), columns.to_vec());
    conn.execute(
        "UPDATE source_projection_scopes SET layout_json=?2,observed_ms=?3 WHERE root_id=?1",
        params![
            root.root_id,
            serde_json::to_string(&layout).map_err(|_| LedgerError::RecordTooLarge)?,
            now_ms()
        ],
    )?;
    for (key, values) in rows {
        if row_suppressed(conn, &root.root_id, table, key)? {
            continue;
        }
        if values.len() != columns.len() || key.len() > 4096 {
            return Err(LedgerError::RecordTooLarge);
        }
        let text = serde_json::to_string(values).map_err(|_| LedgerError::RecordTooLarge)?;
        if text.len() > 262144 {
            return Err(LedgerError::RecordTooLarge);
        }
        use sha2::Digest;
        let hash = format!("{:x}", sha2::Sha256::digest(text.as_bytes()));
        conn.execute("INSERT INTO source_projection_rows(root_id,table_name,row_key,projection_json,row_hash,observed_ms) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(root_id,table_name,row_key) DO UPDATE SET projection_json=excluded.projection_json,row_hash=excluded.row_hash,observed_ms=excluded.observed_ms WHERE row_hash<>excluded.row_hash",params![root.root_id,table,key,text,hash,now_ms()])?;
    }
    tx.commit()?;
    Ok(())
}
pub(crate) fn complete(conn: &Connection, root: &DurableRoot) -> Result<()> {
    conn.execute("UPDATE source_projection_scopes SET complete=1,error_code=NULL,observed_ms=?2 WHERE root_id=?1 AND epoch=?3 AND capability_version=?4",params![root.root_id,now_ms(),epoch(conn)?,root.spec.capability_version])?;
    Ok(())
}
pub(crate) fn saved_databases(conn: &Connection, source: &str) -> Result<Vec<SavedDatabase>> {
    let epoch = epoch(conn)?;
    let mut statement = conn.prepare("SELECT r.root_id,r.locator,p.epoch,p.layout_json,p.complete,p.error_code,p.reader_domain,r.enabled FROM source_roots r LEFT JOIN source_projection_scopes p USING(root_id) WHERE r.report_source=?1 AND r.collector_key IN('local-sqlite-history-v1','additional-native-v1') AND (r.collector_key='local-sqlite-history-v1' OR p.root_id IS NOT NULL OR r.locator LIKE '%.db') ORDER BY r.root_id")?;
    let records = statement
        .query_map([source], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<i64>>(4)?,
                r.get::<_, Option<String>>(5)?,
                r.get::<_, Option<String>>(6)?,
                r.get::<_, bool>(7)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut result = Vec::new();
    for (root_id, path, stored_epoch, json, complete, error, reader_domain, enabled) in records {
        let more:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM ledger_runtime_state WHERE state_key LIKE 'sqlite-incremental/'||?1||'/%' AND json_extract(value_json,'$.has_more')=1)",[&root_id],|r|r.get(0))?;
        if more {
            // The previous full scope may still be complete while the recent
            // lane has overwritten only its first page. Never seal that mixed
            // view as a new complete report/cutoff.
            return Err(LedgerError::IncompleteEvidence);
        }
        if stored_epoch.as_deref() != Some(&epoch) || complete != Some(1) || error.is_some() {
            if !enabled {
                continue;
            }
            return Err(LedgerError::IncompleteEvidence);
        }
        let layout: Layout =
            serde_json::from_str(json.as_deref().ok_or(LedgerError::IncompleteEvidence)?)
                .map_err(|_| LedgerError::CorruptLedger)?;
        let mut database = Connection::open_in_memory()?;
        let tx = database.transaction()?;
        for (table, columns) in layout {
            let defs = columns
                .iter()
                .map(|column| {
                    Ok(format!(
                        "{} {}",
                        quoted(&column.name)?,
                        match column.declared_type.to_ascii_uppercase().as_str() {
                            "INTEGER" | "INT" => "INTEGER",
                            "REAL" | "FLOAT" | "DOUBLE" => "REAL",
                            "TEXT" | "VARCHAR" => "TEXT",
                            _ => "",
                        }
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            tx.execute_batch(&format!(
                "CREATE TABLE {} ({})",
                quoted(&table)?,
                defs.join(",")
            ))?;
            let mut rows=conn.prepare("SELECT projection_json,row_key FROM source_projection_rows WHERE root_id=?1 AND table_name=?2 ORDER BY row_key")?;
            let mut cursor = rows.query(params![root_id, table])?;
            let sql = format!(
                "INSERT INTO {} VALUES({})",
                quoted(&table)?,
                vec!["?"; columns.len()].join(",")
            );
            let mut insert = tx.prepare(&sql)?;
            while let Some(row) = cursor.next()? {
                if row_suppressed(conn, &root_id, &table, &row.get::<_, String>(1)?)? {
                    continue;
                }
                let values: Vec<SqlCell> = serde_json::from_str(&row.get::<_, String>(0)?)
                    .map_err(|_| LedgerError::CorruptLedger)?;
                if values.len() != columns.len() {
                    return Err(LedgerError::CorruptLedger);
                }
                insert.execute(rusqlite::params_from_iter(
                    values
                        .into_iter()
                        .map(SqlCell::sql)
                        .collect::<Result<Vec<_>>>()?,
                ))?;
            }
            drop(insert);
            drop(cursor);
            drop(rows);
            for key in ["id", "session_id"] {
                if columns.iter().any(|c| c.name == key) {
                    tx.execute_batch(&format!(
                        "CREATE INDEX {} ON {}({})",
                        quoted(&format!("idx_{table}_{key}"))?,
                        quoted(&table)?,
                        quoted(key)?
                    ))?;
                }
            }
        }
        tx.commit()?;
        database.execute_batch("PRAGMA query_only=ON")?;
        result.push(SavedDatabase {
            original_path: path.into(),
            reader_domain,
            connection: database,
        });
    }
    Ok(result)
}

fn reader_domain(path: &std::path::Path, source: &str) -> Option<String> {
    let stamp = crate::common::file_stamp(path)?;
    let identity = stamp.identity_key();
    let parts = identity.split('/').take(2).collect::<Vec<_>>();
    if source == "hermes" {
        return Some(if parts.len() == 2 && parts.iter().all(|s| !s.is_empty()) {
            format!("file:{}/{}", parts[0], parts[1])
        } else {
            format!("path:{}", path.display())
        });
    }
    if source == "opencode" {
        let physical = if parts.len() == 2 && parts.iter().all(|s| !s.is_empty()) {
            format!("file:{}:{}", parts[0], parts[1])
        } else {
            format!("path:{path:?}")
        };
        use sha2::Digest;
        let mut hash = sha2::Sha256::new();
        hash.update(path.parent()?.as_os_str().as_encoded_bytes());
        hash.update([0]);
        hash.update(physical.as_bytes());
        return Some(format!("{:x}", hash.finalize()));
    }
    None
}
pub(crate) fn ensure(conn: &Connection, root: &DurableRoot) -> Result<()> {
    let valid:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM source_projection_scopes WHERE root_id=?1 AND epoch=?2 AND capability_version=?3)",params![root.root_id,epoch(conn)?,root.spec.capability_version],|r|r.get(0))?;
    if !valid {
        begin(conn, root)?;
        super::collection::invalidate_history_audit(conn, root)?;
    }
    Ok(())
}

fn retained_columns(table: &str) -> Result<&'static [&'static str]> {
    Ok(match table {
        "model_usage" => &[
            "id",
            "logical_request_id",
            "attempt_index",
            "session_id",
            "model_id",
            "status",
            "started_at",
            "completed_at",
            "duration_ms",
            "finish_reason",
            "input_tokens",
            "output_tokens",
            "reasoning_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
            "computed_total_tokens",
        ],
        "local_runtime_token_usage" => &[
            "id",
            "session_id",
            "agent_name",
            "model",
            "turn_id",
            "ts",
            "input_tokens",
            "output_tokens",
            "reasoning_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "cost_usd",
        ],
        "local_runtime_sessions" => &[
            "session_id",
            "record_json",
            "workspace_dir",
            "project_workspace_dir",
        ],
        "sessions" => &[
            "id",
            "model",
            "started_at",
            "ended_at",
            "parent_session_id",
            "input_tokens",
            "output_tokens",
            "reasoning_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "api_call_count",
            "actual_cost_usd",
            "estimated_cost_usd",
            "source",
            "model_config",
            "end_reason",
        ],
        "chat_message" => &[
            "id",
            "gmt_create",
            "token_info",
            "model_info",
            "session_id",
            "related_session_id",
            "role",
        ],
        "message" => &["id", "session_id", "data"],
        "session_message" => &["id", "session_id", "data", "type", "seq", "time_created"],
        "session_v2" => &["id", "fork_session_id", "fork_boundary"],
        "session" => &[
            "id",
            "model",
            "cost",
            "tokens_input",
            "tokens_output",
            "tokens_reasoning",
            "tokens_cache_read",
            "tokens_cache_write",
            "time_created",
        ],
        _ => return Err(LedgerError::UnsupportedVersion),
    })
}
// Deserialize only selected object paths. IgnoredAny skips body strings/arrays
// without allocating them. Number's original decimal spelling is preserved.
fn project_json(text: &str, paths: &[&str]) -> Result<String> {
    project_json_reader(text.as_bytes(), paths)
}
pub(crate) fn project_json_reader(reader: impl std::io::Read, paths: &[&str]) -> Result<String> {
    use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
    #[derive(Default)]
    struct Node {
        children: BTreeMap<String, Node>,
    }
    struct Seed<'a>(&'a Node);
    impl<'de> DeserializeSeed<'de> for Seed<'_> {
        type Value = serde_json::Value;
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            d: D,
        ) -> std::result::Result<Self::Value, D::Error> {
            // Leaf paths authorize scalar values, never an arbitrary subtree.
            // Keep an unexpected container's type (so the original reader still
            // rejects/ignores it), but stream past all unapproved descendants.
            struct V<'a>(&'a Node);
            impl<'de> Visitor<'de> for V<'_> {
                type Value = serde_json::Value;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("projected JSON value")
                }
                fn visit_map<M: MapAccess<'de>>(
                    self,
                    mut m: M,
                ) -> std::result::Result<Self::Value, M::Error> {
                    let mut out = serde_json::Map::new();
                    while let Some(key) = m.next_key::<String>()? {
                        if key == "$serde_json::private::Number" {
                            let text = m.next_value::<String>()?;
                            return text
                                .parse::<serde_json::Number>()
                                .map(serde_json::Value::Number)
                                .map_err(serde::de::Error::custom);
                        }
                        if let Some(node) = self.0.children.get(&key) {
                            out.insert(key, m.next_value_seed(Seed(node))?);
                        } else {
                            m.next_value::<IgnoredAny>()?;
                        }
                    }
                    Ok(serde_json::Value::Object(out))
                }
                fn visit_seq<A: SeqAccess<'de>>(
                    self,
                    mut a: A,
                ) -> std::result::Result<Self::Value, A::Error> {
                    while a.next_element::<IgnoredAny>()?.is_some() {}
                    Ok(serde_json::Value::Array(Vec::new()))
                }
                fn visit_f64<E: serde::de::Error>(
                    self,
                    v: f64,
                ) -> std::result::Result<Self::Value, E> {
                    serde_json::Number::from_f64(v)
                        .map(serde_json::Value::Number)
                        .ok_or_else(|| E::custom("nonfinite JSON number"))
                }
                fn visit_none<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
                    Ok(serde_json::Value::Null)
                }
                fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
                    Ok(serde_json::Value::Null)
                }
                fn visit_str<E: serde::de::Error>(
                    self,
                    v: &str,
                ) -> std::result::Result<Self::Value, E> {
                    if v.len() > 65536 {
                        return Err(E::custom("source projection value limit exceeded"));
                    }
                    Ok(serde_json::Value::String(v.into()))
                }
                fn visit_bool<E: serde::de::Error>(
                    self,
                    v: bool,
                ) -> std::result::Result<Self::Value, E> {
                    Ok(v.into())
                }
                fn visit_i64<E: serde::de::Error>(
                    self,
                    v: i64,
                ) -> std::result::Result<Self::Value, E> {
                    Ok(v.into())
                }
                fn visit_u64<E: serde::de::Error>(
                    self,
                    v: u64,
                ) -> std::result::Result<Self::Value, E> {
                    Ok(v.into())
                }
            }
            d.deserialize_any(V(self.0))
        }
    }
    let mut tree = Node::default();
    for path in paths {
        let mut n = &mut tree;
        for key in path.split('.') {
            n = n.children.entry(key.into()).or_default();
        }
    }
    let classify = |error: serde_json::Error| {
        if error.is_io()
            || error
                .to_string()
                .contains("source projection value limit exceeded")
        {
            LedgerError::RecordTooLarge
        } else {
            LedgerError::IncompleteEvidence
        }
    };
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let value = Seed(&tree)
        .deserialize(&mut deserializer)
        .map_err(classify)?;
    deserializer.end().map_err(classify)?;
    let out = serde_json::to_string(&value).map_err(|_| LedgerError::RecordTooLarge)?;
    if out.len() > 65536 {
        return Err(LedgerError::RecordTooLarge);
    }
    Ok(out)
}
fn json_paths(table: &str, column: &str) -> Option<&'static [&'static str]> {
    Some(match (table, column) {
        ("message" | "session_message", "data") => &[
            "role",
            "type",
            "sessionID",
            "modelID",
            "modelId",
            "model.id",
            "providerID",
            "tokens.input",
            "tokens.output",
            "tokens.reasoning",
            "tokens.total",
            "tokens.cache.read",
            "tokens.cache.write",
            "time.created",
            "time.completed",
            "cost",
            "finish",
        ],
        ("chat_message", "token_info") => &["prompt_tokens", "completion_tokens", "cached_tokens"],
        ("chat_message", "model_info") => &["model_key", "model"],
        ("sessions", "model_config") => &["_branched_from", "_delegate_from", "_reset_from"],
        ("local_runtime_sessions", "record_json") => &["workspaceDir"],
        ("session", "model") => &["id"],
        _ => return None,
    })
}
fn sanitize_column(table: &str, column: &str, value: SqlValue) -> Result<SqlCell> {
    let Some(paths) = json_paths(table, column) else {
        return SqlCell::from_sql(value);
    };
    match value {
        SqlValue::Text(s) => {
            // A scalar model is also a supported old-reader shape.
            if table == "session" && !s.trim_start().starts_with('{') {
                return SqlCell::from_sql(SqlValue::Text(s));
            }
            let text = match project_json(&s, paths) {
                Ok(text) => text,
                Err(LedgerError::IncompleteEvidence) => "!invalid-json".into(),
                Err(e) => return Err(e),
            };
            SqlCell::from_sql(SqlValue::Text(text))
        }
        other => SqlCell::from_sql(other),
    }
}

// TEXT columns can be read with SQLite's incremental BLOB API. It keeps the
// source transaction stable without allocating an entire transcript in Rust.
// The original text limit and projected-field limit are separate contracts.
fn project_large_json(
    upstream: &Connection,
    table: &str,
    column: &str,
    keycol: &str,
    key: &str,
) -> Result<String> {
    use std::io::{BufReader, Read};
    const MAX_SOURCE_BYTES: i64 = 256 * 1024 * 1024;
    struct Deadline<R> {
        inner: R,
        until: std::time::Instant,
    }
    impl<R: Read> Read for Deadline<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if std::time::Instant::now() > self.until {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "source projection deadline",
                ));
            }
            self.inner.read(buf)
        }
    }
    let paths = json_paths(table, column).ok_or(LedgerError::RecordTooLarge)?;
    let shadowed = upstream
        .prepare(&format!("PRAGMA table_info({})", quoted(table)?))?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|name| name.eq_ignore_ascii_case("_rowid_"));
    if shadowed {
        return Err(LedgerError::UnsupportedVersion);
    }
    let (rowid, kind, length): (i64, String, i64) = upstream.query_row(
        &format!(
            "SELECT _rowid_,typeof({}),length(CAST({} AS BLOB)) FROM {} WHERE {}=?1",
            quoted(column)?,
            quoted(column)?,
            quoted(table)?,
            quoted(keycol)?
        ),
        [key],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    if kind != "text" || length > MAX_SOURCE_BYTES {
        return Err(LedgerError::RecordTooLarge);
    }
    let blob = upstream.blob_open("main", table, column, rowid, true)?;
    if i64::try_from(blob.len()).map_err(|_| LedgerError::RecordTooLarge)? != length {
        return Err(LedgerError::StaleObservation);
    }
    let reader = BufReader::with_capacity(
        64 * 1024,
        Deadline {
            inner: blob,
            until: std::time::Instant::now() + std::time::Duration::from_secs(5),
        },
    );
    match project_json_reader(reader, paths) {
        Err(LedgerError::IncompleteEvidence) => Ok("!invalid-json".into()),
        result => result,
    }
}
/// Capture only a bounded selected key page. The upstream transaction is the
/// adapter's existing snapshot; the persisted table is independent of immutable
/// evidence versions and stores the latest source-reader input per physical row.
pub(crate) fn capture_rows(
    conn: &Connection,
    root: &DurableRoot,
    upstream: &Connection,
    table: &str,
    keys: &[String],
    evidence: &[super::durable::DurableEvidence],
) -> Result<()> {
    ensure(conn, root)?;
    let allowed = retained_columns(table)?;
    let mut info = upstream.prepare(&format!("PRAGMA table_info({})", quoted(table)?))?;
    let columns = info
        .query_map([], |r| {
            Ok(Column {
                name: r.get(1)?,
                declared_type: r.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .filter(|c| allowed.contains(&c.name.as_str()))
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Ok(());
    }
    let keycol = if table == "local_runtime_sessions" {
        "session_id"
    } else {
        "id"
    };
    let list = columns
        .iter()
        .map(|c| {
            let name = quoted(&c.name)?;
            // Per-value bound; oversized records become an explicit gap rather than
            // allocating an unbounded transcript while constructing the projection.
            Ok(format!(
                "CASE WHEN length(CAST({name} AS BLOB))>16777216 THEN zeroblob(1) ELSE {name} END"
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join(",");
    let mut q = upstream.prepare(&format!(
        "SELECT {list} FROM {} WHERE {}=?1",
        quoted(table)?,
        quoted(keycol)?
    ))?;
    let mut retained = Vec::new();
    for key in keys {
        if row_suppressed(conn, &root.root_id, table, key)? {
            continue;
        }
        let values = q
            .query_row([key], |r| {
                (0..columns.len())
                    .map(|i| r.get::<_, SqlValue>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .optional()?;
        let Some(mut values) = values else {
            continue;
        };
        for (column, value) in columns.iter().zip(&mut values) {
            if matches!(value, SqlValue::Blob(_)) && json_paths(table, &column.name).is_some() {
                *value = SqlValue::Text(project_large_json(
                    upstream,
                    table,
                    &column.name,
                    keycol,
                    key,
                )?);
            }
        }
        let mut probe = evidence
            .iter()
            .find(|e| {
                e.observation.native_message_id.as_deref() == Some(key.as_str())
                    || e.projection
                        .as_ref()
                        .and_then(|p| p.get("source_record_id"))
                        .and_then(|v| v.as_str())
                        == Some(key.as_str())
            })
            .map(|e| e.observation.clone())
            .unwrap_or_else(|| super::types::Observation {
                report_source: root.spec.report_source.into(),
                native_tool: root.spec.native_tool.into(),
                identity_family: root.spec.identity_family.into(),
                namespace_key: root.namespace.clone(),
                ..Default::default()
            });
        update_probe_time(&mut probe, &columns, &values)?;
        if super::durable::suppressed(conn, &probe)? {
            suppress_row(conn, &root.root_id, table, key)?;
            continue;
        }
        let projected = columns
            .iter()
            .zip(values)
            .map(|(c, v)| sanitize_column(table, &c.name, v))
            .collect::<Result<Vec<_>>>()?;
        retained.push((key.clone(), projected));
    }
    put_rows(conn, root, table, &columns, &retained)
}

/// Saved-database compatibility readers are background-only. The dashboard uses
/// the query read model; this dispatcher never opens an upstream SQLite DB.
pub fn scan_saved_database(
    source: &str,
    conn: &Connection,
) -> Option<(Vec<crate::model::UsageRecord>, crate::model::ScanStats)> {
    use crate::model::UsageSource;
    Some(match source {
        "zcode" => crate::sources::zcode::scan_saved(conn),
        "hermes" => crate::sources::hermes::scan_saved(conn),
        "minimax" => crate::sources::minimax::scan_saved(conn),
        "kilo" => crate::sources::kilo::scan_saved(conn),
        "qoder" => {
            let roots = crate::sources::qoder::QoderSource
                .discover_roots()
                .into_iter()
                .filter(|p| p.is_dir())
                .collect::<Vec<_>>();
            crate::sources::qoder::scan_saved(conn, &roots)
        }
        "opencode" => {
            let roots = crate::sources::opencode::OpencodeSource.discover_roots();
            crate::sources::opencode::scan_saved(conn, &roots)
        }
        _ => return None,
    })
}

fn update_probe_time(
    probe: &mut super::types::Observation,
    columns: &[Column],
    values: &[SqlValue],
) -> Result<()> {
    for (c, v) in columns.iter().zip(values) {
        if matches!(
            c.name.as_str(),
            "ts" | "started_at" | "gmt_create" | "time_created"
        ) {
            let n = match v {
                SqlValue::Integer(n) => Some(*n),
                SqlValue::Real(n) => Some(*n as i64),
                SqlValue::Text(s) => s.parse().ok().or_else(|| crate::common::iso_to_ms(s)),
                _ => None,
            };
            if let Some(n) = n.filter(|n| *n > 0) {
                probe.occurred_ms = Some(crate::common::secs_or_ms(n));
            }
        }
        if c.name == "data" {
            if let SqlValue::Text(s) = v {
                let projected = project_json(s, &["time.created", "time.completed"])
                    .unwrap_or_else(|_| "null".into());
                let v: serde_json::Value = serde_json::from_str(&projected)
                    .map_err(|_| LedgerError::IncompleteEvidence)?;
                if let Some(n) = v
                    .pointer("/time/completed")
                    .or_else(|| v.pointer("/time/created"))
                    .and_then(|v| v.as_i64())
                {
                    probe.occurred_ms = Some(crate::common::secs_or_ms(n));
                }
            }
        }
    }
    Ok(())
}
fn row_key_hash(root: &str, table: &str, key: &str) -> String {
    super::types::identity_key("source-projection", root, table, &[key])
}
pub(crate) fn row_suppressed(
    conn: &Connection,
    root: &str,
    table: &str,
    key: &str,
) -> Result<bool> {
    Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM source_projection_tombstones WHERE root_id=?1 AND table_name=?2 AND row_key_hash=?3)",params![root,table,row_key_hash(root,table,key)],|r|r.get(0))?)
}
fn suppress_row(conn: &Connection, root: &str, table: &str, key: &str) -> Result<u64> {
    let added = conn.execute(
        "INSERT OR IGNORE INTO source_projection_tombstones VALUES(?1,?2,?3)",
        params![root, table, row_key_hash(root, table, key)],
    )?;
    conn.execute(
        "DELETE FROM source_projection_rows WHERE root_id=?1 AND table_name=?2 AND row_key=?3",
        params![root, table, key],
    )?;
    Ok(added as u64)
}
/// Call in the maintenance transaction before deleting observations or clearing
/// derived scopes. Uses the already expanded TEMP delete_ids set, plus exact
/// source/window controls for captured rows not yet saved as observations.
/// Preview rollback therefore rolls back both payload erasure and tombstones.
pub(crate) fn retain_suppressed_row_keys(conn: &Connection) -> Result<u64> {
    let mut count = 0;
    let selected=conn.prepare("SELECT f.root_id,o.native_message_id,d.projection_json FROM delete_ids x JOIN usage_observations o ON o.observation_id=x.id JOIN source_objects f USING(object_id) LEFT JOIN durable_evidence d USING(observation_id)")?.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,Option<String>>(2)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    for (root, message, projection) in selected {
        let Some(p) = projection.and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        else {
            continue;
        };
        let Some(table) = p.get("channel").and_then(|v| v.as_str()) else {
            continue;
        };
        if retained_columns(table).is_err() {
            continue;
        }
        let key = p
            .get("source_record_id")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .or(message);
        if let Some(key) = key {
            count += suppress_row(conn, &root, table, &key)?;
        }
    }
    let mut st=conn.prepare("SELECT p.root_id,p.table_name,p.row_key,p.projection_json,s.layout_json,r.report_source,r.native_tool,r.identity_family,r.namespace_key FROM source_projection_rows p JOIN source_projection_scopes s USING(root_id) JOIN source_roots r USING(root_id)")?;
    let mut rows = st.query([])?;
    let mut erase = Vec::new();
    while let Some(row) = rows.next()? {
        let root: String = row.get(0)?;
        let table: String = row.get(1)?;
        let key: String = row.get(2)?;
        let layout: Layout = serde_json::from_str(&row.get::<_, String>(4)?)
            .map_err(|_| LedgerError::CorruptLedger)?;
        let columns = layout.get(&table).ok_or(LedgerError::CorruptLedger)?;
        let cells: Vec<SqlCell> = serde_json::from_str(&row.get::<_, String>(3)?)
            .map_err(|_| LedgerError::CorruptLedger)?;
        let values = cells
            .into_iter()
            .map(SqlCell::sql)
            .collect::<Result<Vec<_>>>()?;
        let mut probe = super::types::Observation {
            report_source: row.get(5)?,
            native_tool: row.get(6)?,
            identity_family: row.get(7)?,
            namespace_key: row.get(8)?,
            ..Default::default()
        };
        update_probe_time(&mut probe, columns, &values)?;
        if row_suppressed(conn, &root, &table, &key)? || super::durable::suppressed(conn, &probe)? {
            erase.push((root, table, key));
        }
    }
    drop(rows);
    drop(st);
    for (root, table, key) in erase {
        count += suppress_row(conn, &root, &table, &key)?;
    }
    Ok(count)
}

pub(crate) fn tombstone_digest(conn: &Connection) -> Result<String> {
    use sha2::Digest;
    let mut hash = sha2::Sha256::new();
    let exists:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='source_projection_tombstones')",[],|r|r.get(0))?;
    if exists {
        let mut st=conn.prepare("SELECT root_id,table_name,row_key_hash FROM source_projection_tombstones ORDER BY root_id,table_name,row_key_hash")?;
        let mut rows = st.query([])?;
        while let Some(row) = rows.next()? {
            for i in 0..3 {
                let value: String = row.get(i)?;
                hash.update((value.len() as u64).to_le_bytes());
                hash.update(value.as_bytes());
            }
        }
    }
    Ok(format!("{:x}", hash.finalize()))
}
pub(crate) fn merge_tombstones(current: &Connection, target: &Connection) -> Result<()> {
    let exists:bool=current.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='source_projection_tombstones')",[],|r|r.get(0))?;
    if !exists {
        return Ok(());
    }
    target.execute_batch("CREATE TABLE IF NOT EXISTS source_projection_tombstones(root_id TEXT NOT NULL,table_name TEXT NOT NULL,row_key_hash TEXT NOT NULL,PRIMARY KEY(root_id,table_name,row_key_hash)) WITHOUT ROWID")?;
    let mut st = current
        .prepare("SELECT root_id,table_name,row_key_hash FROM source_projection_tombstones")?;
    let mut rows = st.query([])?;
    let mut insert =
        target.prepare("INSERT OR IGNORE INTO source_projection_tombstones VALUES(?1,?2,?3)")?;
    while let Some(row) = rows.next()? {
        insert.execute(params![
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?
        ])?;
    }
    Ok(())
}
/// Incomplete first capture is a backlog state, not an upstream read failure.
/// A real source/object error still reports failure through collection status.
pub fn saved_database_pending(conn: &Connection, source: &str) -> Result<bool> {
    Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM source_roots r LEFT JOIN source_projection_scopes p USING(root_id) WHERE r.report_source=?1 AND r.enabled=1 AND r.collector_key IN('local-sqlite-history-v1','additional-native-v1') AND (r.collector_key='local-sqlite-history-v1' OR p.root_id IS NOT NULL OR r.locator LIKE '%.db') AND (p.root_id IS NULL OR p.complete=0 OR p.epoch<>(SELECT epoch FROM ledger_meta WHERE singleton=1) OR EXISTS(SELECT 1 FROM ledger_runtime_state s WHERE s.state_key LIKE 'sqlite-incremental/'||r.root_id||'/%' AND json_extract(s.value_json,'$.has_more')=1)) AND r.error_code IS NULL AND p.error_code IS NULL AND NOT EXISTS(SELECT 1 FROM source_objects o WHERE o.root_id=r.root_id AND o.error_code IS NOT NULL))",[source],|r|r.get(0))?)
}
