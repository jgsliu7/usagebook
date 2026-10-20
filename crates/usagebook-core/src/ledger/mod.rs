//! Durable usage evidence. Never recovered by replacing a corrupt file with an empty database.
pub mod claude;
pub mod db;
pub mod ingest;
pub mod legacy;
pub mod maintenance;
pub mod metrics;
pub mod query;
pub mod query_types;
pub mod resolve;
pub mod types;
pub use db::Ledger;
pub use types::{LedgerError, Result};
