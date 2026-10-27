//! Persistent ownership of admitted daily evidence. A later request is evidence
//! to reconcile, not permission to add to, or remove, a published daily total.
use super::{resolve::load_observation, types::*};
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) fn owns_day(
    conn: &Connection,
    family: &str,
    namespace: &str,
    identity: &str,
) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM durable_coverage WHERE identity_family=?1 AND namespace_key=?2 AND identity_key=?3)",
        params![family, namespace, identity], |r| r.get(0),
    )?)
}

/// Called inside the publisher's transaction, before replacing its unit. Old
/// published facts remain frozen. New selections must respect earlier owners.
pub(crate) fn guard_selections(conn: &Connection, family: &str, namespace: &str) -> Result<()> {
    let mut statement = conn.prepare(
        "SELECT d.identity_key,o.* FROM ledger_unit_decisions d JOIN usage_observations o ON o.observation_id=d.selected_id WHERE d.state='active' AND o.fact_kind IN('request','day') ORDER BY d.identity_key",
    )?;
    // Read the canonical keys separately; Observation has an explicit column
    // contract and must not be decoded from a shifted SELECT projection.
    let keys = statement
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    for key in keys {
        let old: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM usage_events WHERE identity_family=?1 AND namespace_key=?2 AND identity_key=?3 AND state='active')",
            params![family, namespace, key], |r| r.get(0),
        )?;
        let o = conn.query_row(
            "SELECT o.* FROM ledger_unit_decisions d JOIN usage_observations o ON o.observation_id=d.selected_id WHERE d.identity_key=?1",
            [&key], load_observation,
        )?;
        // An explicitly adopted manifest owns its whole selected interval. Its
        // separate provider rows are additive within that one manifest.
        let adopted:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM adoption_items i JOIN adoption_policies p USING(policy_id) WHERE i.selected_observation_id=?1 AND p.status='active')",[o.observation_id],|r|r.get(0))?;
        if adopted {
            continue;
        }
        if !old && (overlaps(conn, &o, &key)? || channel_conflict(conn, &o)?) {
            conn.execute(
                "UPDATE ledger_unit_decisions SET selected_id=NULL,state='unresolved',has_conflict=1 WHERE identity_key=?1",
                [&key],
            )?;
            continue;
        }
        remember_channel(conn, &o)?;
        if o.fact_kind == "day" {
            let (Some(start), Some(end)) = (o.day_start_ms, o.day_end_ms) else {
                return Err(LedgerError::IncompleteEvidence);
            };
            conn.execute(
                "INSERT OR IGNORE INTO durable_coverage(identity_family,namespace_key,identity_key,native_tool,model_key,start_ms,end_ms,first_observation_id,rule_version,created_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'day-owner-v1',?9)",
                params![family, namespace, key, o.native_tool, o.model_key, start, end, o.observation_id, now_ms()],
            )?;
        }
    }
    Ok(())
}

fn channel(conn: &Connection, o: &Observation) -> Result<Option<String>> {
    if o.identity_family != "qwen" || o.session_key.is_none() {
        return Ok(None);
    }
    Ok(conn.query_row(
        "SELECT json_extract(projection_json,'$.channel') FROM durable_evidence WHERE observation_id=?1",
        [o.observation_id], |r| r.get(0),
    ).optional()?.flatten())
}
fn channel_conflict(conn: &Connection, o: &Observation) -> Result<bool> {
    let Some(channel) = channel(conn, o)? else {
        return Ok(false);
    };
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM durable_channel_choices WHERE identity_family=?1 AND namespace_key=?2 AND session_key=?3 AND channel<>?4) OR EXISTS(SELECT 1 FROM usage_events e JOIN durable_evidence d ON d.observation_id=e.selected_observation_id WHERE e.identity_family=?1 AND e.namespace_key=?2 AND e.session_key=?3 AND e.state='active' AND json_extract(d.projection_json,'$.channel')<>?4)",
        params![o.identity_family, o.namespace_key, o.session_key, channel], |r| r.get(0),
    )?)
}
fn remember_channel(conn: &Connection, o: &Observation) -> Result<()> {
    if let Some(channel) = channel(conn, o)? {
        // Channel ownership survives deletion of the original observation.
        // Existing requests also establish the choice when rebuilding old data.
        conn.execute(
            "INSERT OR IGNORE INTO durable_channel_choices(identity_family,namespace_key,session_key,channel,first_observation_id,created_ms) SELECT e.identity_family,e.namespace_key,e.session_key,json_extract(d.projection_json,'$.channel'),e.selected_observation_id,?4 FROM usage_events e JOIN durable_evidence d ON d.observation_id=e.selected_observation_id WHERE e.identity_family=?1 AND e.namespace_key=?2 AND e.session_key=?3 AND e.state='active' AND json_extract(d.projection_json,'$.channel') IS NOT NULL ORDER BY e.selected_observation_id LIMIT 1",
            params![o.identity_family, o.namespace_key, o.session_key, now_ms()],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO durable_channel_choices(identity_family,namespace_key,session_key,channel,first_observation_id,created_ms) VALUES(?1,?2,?3,?4,?5,?6)",
            params![o.identity_family, o.namespace_key, o.session_key, channel, o.observation_id, now_ms()],
        )?;
    }
    Ok(())
}

fn overlaps(conn: &Connection, o: &Observation, canonical: &str) -> Result<bool> {
    if o.fact_kind == "request" {
        let Some(time) = o.occurred_ms else {
            return Ok(true);
        };
        return Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM durable_coverage WHERE native_tool=?1 AND namespace_key=?2 AND start_ms<=?3 AND end_ms>?3 AND (model_key='' OR ?4='' OR model_key=?4))",
            params![o.native_tool, o.namespace_key, time, o.model_key], |r| r.get(0),
        )?);
    }
    let (Some(start), Some(end)) = (o.day_start_ms, o.day_end_ms) else {
        return Ok(true);
    };
    // No external day adapter may turn an unbound account into a proved domain.
    if o.report_source == "ccswitch-rollup"
        && (o.namespace_key.is_empty() || o.namespace_key.starts_with("unknown:"))
    {
        return Ok(true);
    }
    let requests: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM usage_events WHERE state='active' AND fact_kind='request' AND native_tool=?1 AND namespace_key=?2 AND occurred_ms>=?3 AND occurred_ms<?4 AND (model_key='' OR ?5='' OR model_key=?5))",
        params![o.native_tool, o.namespace_key, start, end, o.model_key], |r| r.get(0),
    )?;
    let days: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM durable_coverage WHERE native_tool=?1 AND namespace_key=?2 AND start_ms<?4 AND end_ms>?3 AND (model_key='' OR ?5='' OR model_key=?5) AND NOT(identity_family=?6 AND identity_key=?7))",
        params![o.native_tool, o.namespace_key, start, end, o.model_key, o.identity_family, canonical], |r| r.get(0),
    )?;
    Ok(requests || days)
}
