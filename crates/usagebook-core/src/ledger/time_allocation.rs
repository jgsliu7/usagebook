//! Immutable time evidence subordinate to an adopted amount. Querying a member
//! set never admits another usage event or changes its parent's numeric value.
use super::{metrics, resolve, types::*};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const MAX_MEMBERS: usize = 100_000;
const PROVENANCE: &str = "captured-with-adoption";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TimeMember {
    /// Hash of the source record identity and its occurrence. Duplicate source
    /// contributions are retained, not silently deduplicated during adoption.
    pub(crate) member_key: String,
    pub(crate) occurred_ms: i64,
    pub(crate) metrics: Metrics,
    pub(crate) cost_units: Option<i64>,
    pub(crate) cost_scale: Option<i64>,
}

pub(crate) fn digest(value: &impl Serialize) -> Result<String> {
    // A large member set must not allocate another full JSON copy merely to
    // validate its hash on a dashboard read.
    struct HashWriter(Sha256);
    impl std::io::Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = HashWriter(Sha256::new());
    serde_json::to_writer(&mut writer, value).map_err(|_| LedgerError::RecordTooLarge)?;
    Ok(format!("{:x}", writer.0.finalize()))
}

pub(crate) fn parent(c: &Connection, id: i64) -> Result<Observation> {
    c.query_row("SELECT o.* FROM usage_observations o JOIN adoption_items i ON i.selected_observation_id=o.observation_id JOIN adoption_policies p USING(policy_id) WHERE o.observation_id=?1 AND p.status='active' AND p.kind='legacy_snapshot' AND o.parser_version='reported-history-v1' AND o.fact_kind='day'", [id], resolve::load_observation)
        .optional()?.ok_or(LedgerError::CorruptLedger)
}

fn add_cost(total: Option<(i64, i64)>, value: Option<(i64, i64)>) -> Result<Option<(i64, i64)>> {
    let Some((b, sb)) = value else {
        return Ok(total);
    };
    if b < 0 || !(0..=18).contains(&sb) {
        return Err(LedgerError::CorruptLedger);
    }
    let (a, sa) = total.unwrap_or((0, 0));
    let mut scale = sa.max(sb);
    let a = (a as i128)
        .checked_mul(10_i128.pow((scale - sa) as u32))
        .ok_or(LedgerError::NumericOverflow)?;
    let b = (b as i128)
        .checked_mul(10_i128.pow((scale - sb) as u32))
        .ok_or(LedgerError::NumericOverflow)?;
    let mut amount = a.checked_add(b).ok_or(LedgerError::NumericOverflow)?;
    while scale > 0 && amount % 10 == 0 {
        amount /= 10;
        scale -= 1;
    }
    Ok(Some((
        i64::try_from(amount).map_err(|_| LedgerError::NumericOverflow)?,
        scale,
    )))
}

/// Members must be in canonical key order. All ten raw metric fields (including
/// unknowns) and the known decimal cost subtotal must conserve the parent.
pub(crate) fn validate_members(p: &Observation, members: &[TimeMember]) -> Result<()> {
    if members.is_empty() || members.len() > MAX_MEMBERS {
        return Err(LedgerError::RecordTooLarge);
    }
    let (Some(start), Some(end)) = (p.day_start_ms, p.day_end_ms) else {
        return Err(LedgerError::CorruptLedger);
    };
    let mut sum: Metrics = [Some(0); 10];
    let mut cost = None;
    let mut previous: Option<&str> = None;
    for member in members {
        if member.member_key.len() != 64
            || !member.member_key.bytes().all(|b| b.is_ascii_hexdigit())
            || previous.is_some_and(|key| key >= member.member_key.as_str())
            || member.occurred_ms < start
            || member.occurred_ms >= end
            || member.cost_units.is_some() != member.cost_scale.is_some()
        {
            return Err(LedgerError::CorruptLedger);
        }
        previous = Some(&member.member_key);
        metrics::evaluate(&member.metrics)?;
        for (a, b) in sum.iter_mut().zip(member.metrics) {
            *a = match (*a, b) {
                (Some(a), Some(b)) => Some(a.checked_add(b).ok_or(LedgerError::NumericOverflow)?),
                _ => None,
            };
        }
        cost = add_cost(cost, member.cost_units.zip(member.cost_scale))?;
    }
    let expected_cost = if p.cost_state == "known" && p.cost_currency.as_deref() == Some("USD") {
        add_cost(None, p.cost_units.zip(p.cost_scale))?
    } else if p.cost_state == "unknown" {
        None
    } else {
        return Err(LedgerError::CorruptLedger);
    };
    if sum != p.metrics || cost != expected_cost {
        return Err(LedgerError::CorruptLedger);
    }
    Ok(())
}

fn manifest(p: &Observation, members: &[TimeMember]) -> Result<String> {
    digest(&(
        "adopted-time-allocation-v1",
        PROVENANCE,
        &p.normalized_hash,
        members,
    ))
}

/// Called only while adopting the original capture. This API intentionally has
/// no caller-supplied provenance override for reconstructing old distributions.
pub(crate) fn save(c: &Connection, parent_id: i64, members: &[TimeMember]) -> Result<()> {
    if c.is_autocommit() {
        return Err(LedgerError::InvalidInput(
            "time allocation requires adoption transaction".into(),
        ));
    }
    let p = parent(c, parent_id)?;
    validate_members(&p, members)?;
    let hash = manifest(&p, members)?;
    if let Some(existing) = load(c, parent_id)? {
        return if existing == members {
            Ok(())
        } else {
            Err(LedgerError::StaleObservation)
        };
    }
    let mut insert=c.prepare("INSERT INTO durable_time_members(parent_observation_id,member_key,occurred_ms,metrics_json,cost_units,cost_scale) VALUES(?1,?2,?3,?4,?5,?6)")?;
    for member in members {
        insert.execute(params![
            parent_id,
            member.member_key,
            member.occurred_ms,
            serde_json::to_string(&member.metrics).map_err(|_| LedgerError::RecordTooLarge)?,
            member.cost_units,
            member.cost_scale
        ])?;
    }
    c.execute("INSERT INTO durable_time_allocations(parent_observation_id,version,provenance,complete,member_count,manifest_hash,created_ms) VALUES(?1,1,?2,1,?3,?4,?5)",params![parent_id,PROVENANCE,members.len() as i64,hash,now_ms()])?;
    Ok(())
}

/// A bounded SQLite-only lookup: one header/parent read and one ordered member
/// read, with no per-member queries, source files, or mutable capture fallback.
pub(crate) fn load(c: &Connection, parent_id: i64) -> Result<Option<Vec<TimeMember>>> {
    let header: Option<(i64,String,i64,i64,String)> = c.query_row("SELECT version,provenance,complete,member_count,manifest_hash FROM durable_time_allocations WHERE parent_observation_id=?1",[parent_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
    let Some((version, provenance, complete, count, hash)) = header else {
        return Ok(None);
    };
    if version != 1
        || provenance != PROVENANCE
        || complete != 1
        || !(1..=MAX_MEMBERS as i64).contains(&count)
    {
        return Err(LedgerError::CorruptLedger);
    }
    let p = parent(c, parent_id)?;
    let mut stmt=c.prepare("SELECT member_key,occurred_ms,metrics_json,cost_units,cost_scale FROM durable_time_members WHERE parent_observation_id=?1 ORDER BY member_key LIMIT ?2")?;
    let mut rows = stmt.query(params![parent_id, MAX_MEMBERS as i64 + 1])?;
    let mut members = Vec::with_capacity(count as usize);
    while let Some(r) = rows.next()? {
        let encoded: String = r.get(2)?;
        if encoded.len() > 512 {
            return Err(LedgerError::CorruptLedger);
        }
        members.push(TimeMember {
            member_key: r.get(0)?,
            occurred_ms: r.get(1)?,
            metrics: serde_json::from_str(&encoded).map_err(|_| LedgerError::CorruptLedger)?,
            cost_units: r.get(3)?,
            cost_scale: r.get(4)?,
        });
        if members.len() > MAX_MEMBERS {
            return Err(LedgerError::CorruptLedger);
        }
    }
    if members.len() != count as usize {
        return Err(LedgerError::CorruptLedger);
    }
    validate_members(&p, &members)?;
    if manifest(&p, &members)? != hash {
        return Err(LedgerError::CorruptLedger);
    }
    Ok(Some(members))
}

pub(crate) fn check(c: &Connection) -> Result<()> {
    let mut stmt = c.prepare(
        "SELECT parent_observation_id FROM durable_time_allocations ORDER BY parent_observation_id",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        load(c, row.get(0)?)?.ok_or(LedgerError::CorruptLedger)?;
    }
    Ok(())
}

/// Restoring a backup cannot silently discard a current immutable distribution.
/// Match parent identities/hashes instead of local row ids, just like adoption.
pub(crate) fn validate_restore(current: &Connection, staging: &Connection) -> Result<()> {
    let mut stmt=current.prepare("SELECT h.parent_observation_id,h.manifest_hash,o.identity_family,o.namespace_key,o.candidate_key,o.normalized_hash FROM durable_time_allocations h JOIN usage_observations o ON o.observation_id=h.parent_observation_id JOIN adoption_items i ON i.selected_observation_id=o.observation_id JOIN adoption_policies p USING(policy_id) WHERE p.status='active'")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let id: i64 = r.get(0)?;
        load(current, id)?.ok_or(LedgerError::CorruptLedger)?;
        let present:bool=staging.query_row("SELECT EXISTS(SELECT 1 FROM durable_time_allocations h JOIN usage_observations o ON o.observation_id=h.parent_observation_id JOIN adoption_items i ON i.selected_observation_id=o.observation_id JOIN adoption_policies p USING(policy_id) WHERE p.status='active' AND h.manifest_hash=?1 AND o.identity_family=?2 AND o.namespace_key=?3 AND o.candidate_key=?4 AND o.normalized_hash=?5)",params![r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,Vec<u8>>(5)?],|row|row.get(0))?;
        if !present {
            return Err(LedgerError::IncompleteEvidence);
        }
    }
    Ok(())
}
