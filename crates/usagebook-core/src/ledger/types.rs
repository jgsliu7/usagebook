//! Versioned, bounded projections: no conversation text enters the ledger.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub type Result<T> = std::result::Result<T, LedgerError>;
#[derive(Debug)]
pub enum LedgerError {
    Busy,
    StaleObservation,
    IdentityMappingRequired,
    IdentityLinkNeedsReview,
    RestoreIdentityNeedsReview,
    RouteInconsistent,
    CheckpointReplayRequired,
    IncompleteEvidence,
    UnsupportedVersion,
    UpgradeRequiresDrain,
    MetricConflict,
    NumericOverflow,
    HashCollision,
    InvalidUsageNumber,
    RecordTooLarge,
    DiskFull,
    PermissionDenied,
    CorruptLedger,
    Cancelled,
    NotInitialized,
    AlreadyInitialized,
    SourceNotProtected,
    StaleCursor,
    InvalidInput(String),
    Sql(rusqlite::Error),
    Io(std::io::Error),
}
impl LedgerError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Busy => "BUSY",
            Self::StaleObservation => "STALE_OBSERVATION",
            Self::IdentityMappingRequired => "IDENTITY_MAPPING_REQUIRED",
            Self::IdentityLinkNeedsReview => "IDENTITY_LINK_NEEDS_REVIEW",
            Self::RestoreIdentityNeedsReview => "RESTORE_IDENTITY_NEEDS_REVIEW",
            Self::RouteInconsistent => "ROUTE_INCONSISTENT",
            Self::CheckpointReplayRequired => "CHECKPOINT_REPLAY_REQUIRED",
            Self::IncompleteEvidence => "INCOMPLETE_EVIDENCE",
            Self::UnsupportedVersion => "UNSUPPORTED_VERSION",
            Self::UpgradeRequiresDrain => "UPGRADE_REQUIRES_DRAIN",
            Self::MetricConflict => "METRIC_CONFLICT",
            Self::NumericOverflow => "NUMERIC_OVERFLOW",
            Self::HashCollision => "HASH_COLLISION",
            Self::InvalidUsageNumber => "INVALID_USAGE_NUMBER",
            Self::RecordTooLarge => "RECORD_TOO_LARGE",
            Self::DiskFull => "DISK_FULL",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::CorruptLedger => "CORRUPT_LEDGER",
            Self::Cancelled => "CANCELLED",
            Self::NotInitialized => "NOT_INITIALIZED",
            Self::AlreadyInitialized => "ALREADY_INITIALIZED",
            Self::SourceNotProtected => "SOURCE_NOT_PROTECTED",
            Self::StaleCursor => "STALE_CURSOR",
            Self::InvalidInput(_) => "INVALID_INPUT",
            Self::Sql(_) => "STORAGE_ERROR",
            Self::Io(_) => "IO_ERROR",
        }
    }
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Busy => 4,
            Self::Cancelled => 130,
            Self::InvalidInput(_) | Self::RecordTooLarge => 2,
            Self::IncompleteEvidence
            | Self::IdentityLinkNeedsReview
            | Self::RestoreIdentityNeedsReview
            | Self::IdentityMappingRequired
            | Self::SourceNotProtected => 5,
            _ => 3,
        }
    }
}
impl std::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidInput(s) => write!(f, "{}: {}", self.code(), s),
            _ => f.write_str(self.code()),
        }
    }
}
impl std::error::Error for LedgerError {}
impl From<rusqlite::Error> for LedgerError {
    fn from(e: rusqlite::Error) -> Self {
        use rusqlite::ErrorCode;
        match e.sqlite_error_code() {
            Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => Self::Busy,
            Some(ErrorCode::DiskFull) => Self::DiskFull,
            Some(ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase) => Self::CorruptLedger,
            Some(ErrorCode::PermissionDenied | ErrorCode::ReadOnly) => Self::PermissionDenied,
            Some(ErrorCode::OperationInterrupted) => Self::Cancelled,
            _ => Self::Sql(e),
        }
    }
}
impl From<std::io::Error> for LedgerError {
    fn from(e: std::io::Error) -> Self {
        #[cfg(windows)]
        if matches!(e.raw_os_error(), Some(32 | 33)) {
            return Self::Busy;
        }
        match e.kind() {
            std::io::ErrorKind::WouldBlock => Self::Busy,
            std::io::ErrorKind::PermissionDenied => Self::PermissionDenied,
            _ => Self::Io(e),
        }
    }
}

pub const METRIC_NAMES: [&str; 10] = [
    "input_uncached",
    "cache_read",
    "cache_write",
    "output_normal",
    "output_reasoning",
    "input_total",
    "output_total",
    "total_tokens",
    "cache_write_1h",
    "request_count",
];
pub type Metrics = [Option<i64>; 10];
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueState {
    Exact,
    Partial,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricValue {
    pub state: ValueState,
    pub known_value: Option<i64>,
}
impl MetricValue {
    pub fn exact(v: i64) -> Self {
        Self {
            state: ValueState::Exact,
            known_value: Some(v),
        }
    }
    pub fn unknown() -> Self {
        Self {
            state: ValueState::Unknown,
            known_value: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stamp {
    pub ledger_uuid: String,
    pub epoch: String,
    pub commit_seq: String,
    pub report_timezone: String,
    pub derived_version: String,
    pub caliber_version: u32,
    pub legacy_generation: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Observation {
    pub observation_id: i64,
    pub identity_family: String,
    pub namespace_key: String,
    pub identity_key: Option<String>,
    pub candidate_key: String,
    pub normalized_hash: Vec<u8>,
    pub object_id: i64,
    pub content_generation: String,
    pub record_key: String,
    pub parser_version: String,
    pub identity_version: i64,
    pub semantics_version: i64,
    pub observed_ms: i64,
    pub eligibility: String,
    pub action: String,
    pub revision_authority: Option<String>,
    pub revision_no: Option<i64>,
    pub fact_kind: String,
    pub report_source: String,
    pub native_tool: String,
    pub model_key: String,
    pub model_display: Option<String>,
    pub session_key: Option<String>,
    pub native_session_id: Option<String>,
    pub native_message_id: Option<String>,
    pub native_request_id: Option<String>,
    pub occurred_ms: Option<i64>,
    pub native_date: Option<String>,
    pub native_timezone: Option<String>,
    pub day_start_ms: Option<i64>,
    pub day_end_ms: Option<i64>,
    pub metrics: Metrics,
    pub cost_state: String,
    pub cost_units: Option<i64>,
    pub cost_scale: Option<i64>,
    pub cost_currency: Option<String>,
}
impl Default for Observation {
    fn default() -> Self {
        Self {
            observation_id: 0,
            identity_family: "claude".into(),
            namespace_key: "unknown:claude".into(),
            identity_key: None,
            candidate_key: String::new(),
            normalized_hash: Vec::new(),
            object_id: 0,
            content_generation: String::new(),
            record_key: String::new(),
            parser_version: "claude-ledger-v1".into(),
            identity_version: 1,
            semantics_version: 1,
            observed_ms: 0,
            eligibility: "eligible".into(),
            action: "upsert".into(),
            revision_authority: None,
            revision_no: None,
            fact_kind: "request".into(),
            report_source: "claude".into(),
            native_tool: "claude".into(),
            model_key: String::new(),
            model_display: None,
            session_key: None,
            native_session_id: None,
            native_message_id: None,
            native_request_id: None,
            occurred_ms: None,
            native_date: None,
            native_timezone: None,
            day_start_ms: None,
            day_end_ms: None,
            metrics: [None; 10],
            cost_state: "unknown".into(),
            cost_units: None,
            cost_scale: None,
            cost_currency: None,
        }
    }
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
/// Named process-stop points exist only in the opt-in regression build.
/// The parent test waits for the marker, kills this process, then checks recovery.
#[cfg(feature = "regression-hooks")]
pub(crate) fn regression_stop(name: &str) {
    if std::env::var("USAGEBOOK_LEDGER_STOP_AT").as_deref() != Ok(name) {
        return;
    }
    let Some(marker) = std::env::var_os("USAGEBOOK_LEDGER_STOP_FILE") else {
        return;
    };
    std::fs::write(marker, name).expect("write regression stop marker");
    loop {
        std::thread::park_timeout(std::time::Duration::from_secs(1));
    }
}
#[cfg(not(feature = "regression-hooks"))]
#[inline]
pub(crate) fn regression_stop(_: &str) {}

pub fn new_epoch() -> String {
    uuid::Uuid::new_v4().to_string()
}
pub fn hash_parts(domain: &str, parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in std::iter::once(domain).chain(parts.iter().copied()) {
        h.update((p.len() as u32).to_be_bytes());
        h.update(p.as_bytes());
    }
    format!("{:x}", h.finalize())
}
pub fn identity_key(family: &str, namespace: &str, role: &str, parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in ["usagebook/identity", family, namespace] {
        h.update((p.len() as u32).to_be_bytes());
        h.update(p.as_bytes());
    }
    h.update(1_u32.to_be_bytes());
    for p in std::iter::once(role).chain(parts.iter().copied()) {
        h.update((p.len() as u32).to_be_bytes());
        h.update(p.as_bytes());
    }
    format!("{:x}", h.finalize())
}
pub fn identity_link(family: &str, namespace: &str, message: &str) -> String {
    {
        let mut h = Sha256::new();
        h.update(b"usagebook/suppression-link");
        h.update(1_u32.to_be_bytes());
        for p in [family, namespace, "claude/request/message", message] {
            h.update((p.len() as u32).to_be_bytes());
            h.update(p.as_bytes());
        }
        format!("{:x}", h.finalize())
    }
}
pub fn model_key(name: &str) -> String {
    if name.is_empty() {
        String::new()
    } else {
        format!("n:{}", name.to_lowercase())
    }
}
