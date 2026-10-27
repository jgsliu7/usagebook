//! Durable usage evidence. Never recovered by replacing a corrupt file with an empty database.
pub mod additional_adapters;
pub mod adoption;
mod adoption_legacy;
pub mod ccswitch;
pub mod claude;
pub mod collection;
pub(crate) mod coverage;
pub mod db;
pub mod document_adapters;
pub mod dsh_evidence;
pub mod durable;
pub mod ingest;
pub mod legacy;
pub mod local_adapters;
pub(crate) mod source_projection;
pub use source_projection::{saved_database_pending, scan_saved_database};
pub mod maintenance;
pub mod metrics;
pub mod native_adapters;
pub mod query;
pub mod query_types;
pub mod read_model;
pub mod reconciled_adapters;
pub mod resolve;
pub(crate) mod time_allocation;
pub mod time_repair;
pub mod types;
pub use db::Ledger;
pub use types::{LedgerError, Result};

/// Explicitly admitted durable adapters, independent of discovery or readiness.
pub const DURABLE_SUPPORTED_SOURCES: &[&str] = &[
    "claude",
    "codex",
    "kimi",
    "kimi-work",
    "qwen",
    "ccswitch-rollup",
    "zcode",
    "pi",
    "omp",
    "opencode",
    "kilo",
    "openclaw",
    "copilot",
];
/// Includes formats whose white-listed evidence is saved pending a proved
/// identity/finality contract. Their live totals are not promoted automatically.
pub const DURABLE_EVIDENCE_SOURCES: &[&str] = &[
    "claude",
    "codex",
    "kimi",
    "kimi-work",
    "qwen",
    "ccswitch-rollup",
    "zcode",
    "minimax",
    "hermes",
    "pi",
    "omp",
    "opencode",
    "kilo",
    "openclaw",
    "copilot",
    "goose",
    "workbuddy",
    "codebuddy",
    "codebuddy-ide",
    "cline",
    "roocode",
    "grok",
    "codebuff",
    "droid",
    "qoder",
    "amp",
    "gemini",
    "dsh",
];
