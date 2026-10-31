//! Optional query acceleration. Missing indexes never change the read contract.
use super::types::{LedgerError, Result};
use rusqlite::{Connection, OptionalExtension};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

const INDEX_NAME: &str = "events_active_conflicts_v1";
const INDEX_SQL: &str = "CREATE INDEX events_active_conflicts_v1 ON usage_events(report_source) WHERE state='active' AND has_conflict=1";
pub(crate) const RETRY_DELAY: Duration = Duration::from_secs(5 * 60);
pub(crate) const OPEN_BUDGET: Duration = Duration::from_secs(5);
pub(crate) const IDLE_BUDGET: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InstallOutcome {
    Ready,
    Deferred,
    DefinitionConflict,
}

enum Definition {
    Missing,
    Ready,
    Conflict,
}

// Accept formatting of our generated SQL, not arbitrary equivalent SQL. In
// particular, preserve case and whitespace inside SQL string literals.
fn compact_sql(sql: &str) -> String {
    let sql = sql.trim().trim_end_matches(';');
    let mut quoted = false;
    sql.chars()
        .filter(|ch| {
            if *ch == '\'' {
                quoted = !quoted;
            }
            quoted || !ch.is_ascii_whitespace()
        })
        .collect()
}

fn definition(c: &Connection) -> Result<Definition> {
    let object: Option<(String, String, Option<String>)> = c
        .query_row(
            "SELECT type,tbl_name,sql FROM main.sqlite_schema WHERE name=?1 COLLATE NOCASE",
            [INDEX_NAME],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((kind, table, sql)) = object else {
        return Ok(Definition::Missing);
    };
    if kind != "index"
        || table != "usage_events"
        || sql.is_none_or(|sql| compact_sql(&sql) != compact_sql(INDEX_SQL))
    {
        return Ok(Definition::Conflict);
    }
    let indexes = c
        .prepare("PRAGMA main.index_list('usage_events')")?
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(1)?,
                r.get::<_, bool>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, bool>(4)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !indexes.iter().any(|(name, unique, origin, partial)| {
        name == INDEX_NAME && !unique && origin == "c" && *partial
    }) {
        return Ok(Definition::Conflict);
    }
    let keys = c
        .prepare("PRAGMA main.index_xinfo('events_active_conflicts_v1')")?
        .query_map([], |r| {
            Ok((
                r.get::<_, Option<String>>(2)?,
                r.get::<_, bool>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, bool>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let keys = keys
        .iter()
        .filter(|(_, _, _, key)| *key)
        .collect::<Vec<_>>();
    if keys.len() != 1
        || keys[0].0.as_deref() != Some("report_source")
        || keys[0].1
        || keys[0].2.as_deref() != Some("BINARY")
    {
        return Ok(Definition::Conflict);
    }
    Ok(Definition::Ready)
}

/// Only an idle owner of the writer/maintenance lease, or a newly created
/// private in-memory database, may call this. It owns no caller transaction.
pub(crate) fn install(c: &Connection, budget: Duration) -> Result<InstallOutcome> {
    if !c.is_autocommit() {
        return Err(LedgerError::InvalidInput(
            "query index maintenance requires an idle writer".into(),
        ));
    }
    #[cfg(feature = "regression-hooks")]
    let (budget, failure) = regression::take(budget);
    let saved_busy: u32 = c.query_row("PRAGMA busy_timeout", [], |r| r.get(0))?;
    c.busy_timeout(Duration::from_millis(100))?;
    let started = Instant::now();
    let budget_expired = Arc::new(AtomicBool::new(false));
    let expired = Arc::clone(&budget_expired);
    if let Err(error) = c.progress_handler(
        1000,
        Some(move || {
            let stop = started.elapsed() >= budget;
            if stop {
                expired.store(true, Ordering::Relaxed);
            }
            stop
        }),
    ) {
        c.busy_timeout(Duration::from_millis(u64::from(saved_busy)))?;
        return Err(error.into());
    }
    let mut owns_transaction = false;
    let operation = (|| -> Result<InstallOutcome> {
        match definition(c)? {
            Definition::Ready => return Ok(InstallOutcome::Ready),
            Definition::Conflict => return Ok(InstallOutcome::DefinitionConflict),
            Definition::Missing => {}
        }
        c.execute_batch("BEGIN IMMEDIATE")?;
        owns_transaction = true;
        // The lease serializes application writers; recheck also guards a
        // separate SQLite client that did not acquire that lease.
        let outcome = match definition(c)? {
            Definition::Ready => InstallOutcome::Ready,
            Definition::Conflict => InstallOutcome::DefinitionConflict,
            Definition::Missing => {
                c.execute_batch(INDEX_SQL)?;
                if !matches!(definition(c)?, Definition::Ready) {
                    return Err(LedgerError::InvalidInput(
                        "query index definition changed during installation".into(),
                    ));
                }
                InstallOutcome::Ready
            }
        };
        c.execute_batch("COMMIT")?;
        Ok(outcome)
    })();

    // Disable our deadline before rollback: a budget that interrupted CREATE
    // INDEX must not also interrupt its cleanup. Try every restoration even if
    // an earlier cleanup step failed; never reuse a connection after any error.
    let clear = c.progress_handler(0, None::<fn() -> bool>);
    let rollback = if owns_transaction && !c.is_autocommit() {
        c.execute_batch("ROLLBACK")
    } else {
        Ok(())
    };
    let busy = c.busy_timeout(Duration::from_millis(u64::from(saved_busy)));
    clear?;
    rollback?;
    busy?;
    #[cfg(feature = "regression-hooks")]
    if failure.is_some() {
        return Err(LedgerError::InvalidInput(
            "injected query index cleanup failure".into(),
        ));
    }
    if !c.is_autocommit() {
        return Err(LedgerError::InvalidInput(
            "query index maintenance did not restore autocommit".into(),
        ));
    }

    match operation {
        Ok(outcome) => {
            if outcome == InstallOutcome::DefinitionConflict {
                tracing::warn!(
                    index = INDEX_NAME,
                    code = "QUERY_INDEX_DEFINITION_CONFLICT",
                    "Optional query index was not installed; existing object retained"
                );
            }
            Ok(outcome)
        }
        Err(error)
            if matches!(error, LedgerError::Busy | LedgerError::DiskFull)
                || matches!(error, LedgerError::Cancelled)
                    && budget_expired.load(Ordering::Relaxed) =>
        {
            tracing::warn!(
                index = INDEX_NAME,
                code = error.code(),
                "Optional query index installation deferred"
            );
            Ok(InstallOutcome::Deferred)
        }
        Err(error) => Err(error),
    }
}

#[cfg(feature = "regression-hooks")]
pub use regression::{
    fail_cleanup as regression_fail_cleanup, next_budget as regression_next_budget,
    QueryIndexCleanupFailure,
};

#[cfg(feature = "regression-hooks")]
mod regression {
    use std::cell::Cell;
    use std::time::Duration;

    #[derive(Clone, Copy, Debug)]
    pub enum QueryIndexCleanupFailure {
        Rollback,
        ClearProgress,
        RestoreBusy,
    }
    thread_local! {
        static BUDGET: Cell<Option<Duration>> = const { Cell::new(None) };
        static FAILURE: Cell<Option<QueryIndexCleanupFailure>> = const { Cell::new(None) };
    }
    pub fn next_budget(budget: Duration) {
        BUDGET.set(Some(budget));
    }
    pub fn fail_cleanup(failure: QueryIndexCleanupFailure) {
        FAILURE.set(Some(failure));
    }
    pub(super) fn take(budget: Duration) -> (Duration, Option<QueryIndexCleanupFailure>) {
        (BUDGET.take().unwrap_or(budget), FAILURE.take())
    }
}
