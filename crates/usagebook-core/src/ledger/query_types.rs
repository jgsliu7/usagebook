//! Versioned, strict query input. Integers on the wire are decimal strings.
use serde::{Deserialize, Serialize};
use std::fmt;

pub type QueryResult<T> = Result<T, QueryError>;
#[derive(Debug, Clone, Serialize)]
pub struct QueryError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}
impl QueryError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: matches!(code, "BUSY" | "QUERY_TIMEOUT" | "CURSOR_STALE"),
        }
    }
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new("INVALID_QUERY", message)
    }
    pub fn limit(message: impl Into<String>) -> Self {
        Self::new("QUERY_LIMIT", message)
    }
    pub fn http_status(&self) -> u16 {
        match self.code.as_str() {
            "INVALID_QUERY" => 400,
            "BODY_TOO_LARGE" => 413,
            "QUERY_LIMIT" | "NUMERIC_OVERFLOW" => 422,
            "CURSOR_STALE" | "TIMEZONE_CHANGED" => 409,
            "BUSY" => 429,
            _ => 503,
        }
    }
}
impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for QueryError {}
impl From<rusqlite::Error> for QueryError {
    fn from(e: rusqlite::Error) -> Self {
        let s = e.to_string();
        Self::new(
            if s.contains("interrupted") {
                "QUERY_TIMEOUT"
            } else if s.contains("overflow") {
                "NUMERIC_OVERFLOW"
            } else {
                "LEDGER_UNREADABLE"
            },
            s,
        )
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum Selector {
    All,
    Include { ids: Vec<String> },
    Exclude { ids: Vec<String> },
}
impl<'de> Deserialize<'de> for Selector {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            mode: String,
            #[serde(default, deserialize_with = "selected_ids")]
            ids: Option<Vec<String>>,
        }
        fn selected_ids<'de, D: serde::Deserializer<'de>>(
            d: D,
        ) -> Result<Option<Vec<String>>, D::Error> {
            struct Limited;
            impl<'de> serde::de::Visitor<'de> for Limited {
                type Value = Vec<String>;
                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str("at most 10000 selected IDs")
                }
                fn visit_seq<A: serde::de::SeqAccess<'de>>(
                    self,
                    mut seq: A,
                ) -> Result<Self::Value, A::Error> {
                    let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(10000));
                    while let Some(v) = seq.next_element::<String>()? {
                        if out.len() == 10000 || v.len() > 1400 {
                            return Err(serde::de::Error::custom(
                                "QUERY_LIMIT: selector exceeds limits",
                            ));
                        }
                        out.push(v);
                    }
                    Ok(out)
                }
            }
            d.deserialize_seq(Limited).map(Some)
        }
        let r = Raw::deserialize(d)?;
        match (r.mode.as_str(), r.ids) {
            ("all", None) => Ok(Self::All),
            ("include", Some(ids)) => Ok(Self::Include { ids }),
            ("exclude", Some(ids)) => Ok(Self::Exclude { ids }),
            _ => Err(serde::de::Error::custom("invalid selector fields")),
        }
    }
}
impl Default for Selector {
    fn default() -> Self {
        Self::All
    }
}
impl Selector {
    pub fn allows(&self, id: &str) -> bool {
        match self {
            Self::All => true,
            Self::Include { ids } => ids.iter().any(|v| v == id),
            Self::Exclude { ids } => !ids.iter().any(|v| v == id),
        }
    }
    pub fn ids(&self) -> &[String] {
        match self {
            Self::All => &[],
            Self::Include { ids } | Self::Exclude { ids } => ids,
        }
    }
    pub fn normalize(&mut self, max: usize, max_bytes: usize) -> QueryResult<()> {
        match self {
            Self::All => {}
            Self::Include { ids } | Self::Exclude { ids } => {
                if ids.len() > max {
                    return Err(QueryError::limit("too many selected IDs"));
                }
                if ids.iter().any(|v| v.len() > max_bytes) {
                    return Err(QueryError::limit("selected ID too long"));
                }
                ids.sort();
                ids.dedup();
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Window {
    All,
    Calendar {
        start_date: String,
        end_date_exclusive: String,
    },
    Instant {
        start_ms: String,
        end_ms_exclusive: String,
    },
}
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    Total,
    InputUncached,
    CacheRead,
    CacheWrite,
    Output,
    OutputCombined,
    ReasoningOutput,
}
impl Metric {
    pub fn index(self) -> usize {
        match self {
            Self::Total => 7,
            Self::InputUncached => 0,
            Self::CacheRead => 1,
            Self::CacheWrite => 2,
            Self::Output => 3,
            Self::OutputCombined => 6,
            Self::ReasoningOutput => 4,
        }
    }
}
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GroupBy {
    Source,
    Model,
    None,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub window: Window,
    pub report_timezone: String,
    pub as_of_ms: Option<String>,
    pub sources: Selector,
    pub models: Selector,
    pub metric: Metric,
    pub group_by: GroupBy,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Comparison {
    YesterdaySameLocalTime,
    Explicit { window: Window },
}
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrendUnit {
    Week,
    Month,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trend {
    pub unit: TrendUnit,
    pub periods: u16,
    pub anchor_date: String,
}
fn default_limit() -> usize {
    100
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Page {
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
}
impl Default for Page {
    fn default() -> Self {
        Self {
            limit: 100,
            cursor: None,
        }
    }
}
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Dimension {
    Source,
    Model,
    Day,
}
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Facet,
    Filtered,
}
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SeriesUnit {
    Auto,
    Day,
    Week,
    Month,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryRequest {
    Totals {
        query: Query,
    },
    Dashboard {
        query: Query,
        series_unit: SeriesUnit,
        max_points: usize,
        table_limit: usize,
        comparison: Option<Comparison>,
        trend: Option<Trend>,
    },
    Table {
        query: Query,
        dimension: Dimension,
        scope: Scope,
        page: Page,
    },
    Day {
        query: Query,
        date: String,
    },
    SourceDays {
        source: String,
        models: Selector,
        start_date: String,
        end_date_exclusive: String,
        page: Page,
    },
    SessionTotals {
        sources: Selector,
        models: Selector,
        page: Page,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogRequest {
    pub dimension: Dimension,
    pub search: String,
    pub page: Page,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Stamp {
    pub ledger_uuid: String,
    pub epoch: String,
    pub commit_seq: String,
    pub report_timezone: String,
    pub derived_version: String,
    pub caliber_version: u32,
    pub legacy_generation: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryResponse {
    pub api_version: u32,
    pub stamp: Stamp,
    pub as_of_ms: String,
    pub query_key: String,
    pub payload: serde_json::Value,
    pub source_coverage: String,
    pub warnings: Vec<String>,
}
pub type CatalogPage = serde_json::Value;
pub type Status = serde_json::Value;

pub const MAX_BODY: usize = 16 * 1024 * 1024;
impl QueryRequest {
    pub fn from_json(body: &[u8]) -> QueryResult<Self> {
        decode(body)
    }
}
impl CatalogRequest {
    pub fn from_json(body: &[u8]) -> QueryResult<Self> {
        decode(body)
    }
}
fn decode<T: serde::de::DeserializeOwned>(body: &[u8]) -> QueryResult<T> {
    if body.len() > MAX_BODY {
        return Err(QueryError::new(
            "BODY_TOO_LARGE",
            "JSON body exceeds 16 MiB",
        ));
    }
    let shape: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| QueryError::invalid(e.to_string()))?;
    fn check_unit(v: &serde_json::Value) -> QueryResult<()> {
        match v {
            serde_json::Value::Object(map) => {
                if matches!(
                    map.get("kind").and_then(|v| v.as_str()),
                    Some("all" | "yesterday_same_local_time")
                ) && map.len() != 1
                {
                    return Err(QueryError::invalid("unexpected fields in unit variant"));
                }
                for value in map.values() {
                    check_unit(value)?;
                }
            }
            serde_json::Value::Array(a) => {
                for v in a {
                    check_unit(v)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    check_unit(&shape)?;
    drop(shape);
    serde_json::from_slice(body).map_err(|e| {
        if e.to_string().contains("QUERY_LIMIT:") {
            QueryError::limit(e.to_string())
        } else {
            QueryError::invalid(e.to_string())
        }
    })
}
