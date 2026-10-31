//! Proven local adoption edges. These records are never inferred from equal
//! amounts and are published atomically with the local route and adopted facts.
use super::{sync_identity, sync_projection, sync_state, time_allocation, types::*, Result};
use base64::Engine;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct InputProof {
    pub identity_hash: String,
    pub source_record_hash: String,
    pub semantic_hash: String,
}
pub(crate) fn input_identity(
    family: &str,
    namespace: &str,
    key: &str,
    occurrence: usize,
) -> Result<String> {
    time_allocation::digest(&(
        "sync-legacy-input-v1",
        family,
        namespace,
        "request",
        key,
        occurrence,
    ))
}
type AdoptedBinding = (
    sync_state::Binding,
    sync_projection::Identity,
    BTreeMap<(String, String), NativeBucket>,
);
pub(crate) struct Prepared {
    bindings: Vec<AdoptedBinding>,
    version: i64,
}
#[derive(Default)]
struct NativeBucket {
    start_ms: i64,
    end_ms: i64,
    retire: Vec<(String, String)>,
    evidence: Vec<Value>,
}
pub(crate) struct Bucket<'a> {
    pub observation_id: i64,
    pub parent_key: &'a str,
    pub native_date: &'a str,
    pub model: &'a str,
    pub start_ms: i64,
    pub end_ms: i64,
    pub members: &'a [time_allocation::TimeMember],
    pub proofs: &'a BTreeMap<String, InputProof>,
}
fn blocked(code: &str) -> LedgerError {
    LedgerError::InvalidInput(code.into())
}

/// Runs inside the adoption's IMMEDIATE transaction, before any local mutation.
/// Only the current binding can receive new cloud work. Prior accounts and
/// replaced devices retain frozen cloud history without blocking local adoption.
pub(crate) fn prepare(
    c: &Connection,
    source: &str,
    active: &[Observation],
    timezone: &str,
) -> Result<Prepared> {
    let aborted: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM sync_scopes s JOIN sync_bindings b USING(binding_id) WHERE b.is_current=1 AND s.source=?1 AND s.reason='TRANSACTION_ABORTED')",
        [source],
        |r| r.get(0),
    )?;
    if aborted {
        return Err(blocked("TRANSACTION_ABORTED"));
    }
    let busy: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM sync_source_exports e JOIN sync_bindings b USING(binding_id) WHERE b.is_current=1 AND e.source=?1) OR EXISTS(SELECT 1 FROM sync_transactions t JOIN sync_bindings b USING(binding_id) WHERE b.is_current=1 AND t.source=?1 AND t.state NOT IN('acked','aborted')) OR EXISTS(SELECT 1 FROM sync_reservations r JOIN sync_contributions p USING(binding_id,contribution_id) JOIN sync_bindings b USING(binding_id) WHERE b.is_current=1 AND p.source=?1)", [source], |r| r.get(0))?;
    if busy {
        return Err(blocked("SYNC_SOURCE_PENDING"));
    }
    let mut q = c.prepare("SELECT binding_id,account_id,device_id,ledger_uuid,secret_check_hash,enabled,status,history_from_ms,restore_epoch,server_generation FROM sync_bindings b WHERE b.is_current=1 AND EXISTS(SELECT 1 FROM sync_lineages l WHERE l.binding_id=b.binding_id AND l.source=?1) ORDER BY binding_id")?;
    let rows = q
        .query_map([source], |r| {
            Ok(sync_state::Binding {
                binding_id: r.get(0)?,
                account_id: r.get(1)?,
                device_id: r.get(2)?,
                ledger_uuid: r.get(3)?,
                secret_check_hash: r.get(4)?,
                enabled: r.get(5)?,
                status: r.get(6)?,
                history_from_ms: r.get(7)?,
                restore_epoch: r.get(8)?,
                server_generation: r.get(9)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut bindings = Vec::new();
    for binding in rows {
        let database: String = c.query_row(
            "SELECT file FROM pragma_database_list WHERE name='main'",
            [],
            |r| r.get(0),
        )?;
        let parent = Path::new(&database)
            .parent()
            .ok_or_else(|| blocked("SYNC_ADOPTION_CREDENTIAL_REQUIRED"))?;
        let credential = sync_identity::read_private_json(
            &parent
                .join("cloud-credentials")
                .join(format!("{}.json", binding.binding_id)),
        )
        .map_err(|_| blocked("SYNC_ADOPTION_CREDENTIAL_REQUIRED"))?;
        let secret = base64::engine::general_purpose::STANDARD
            .decode(
                credential["device_secret"]
                    .as_str()
                    .ok_or_else(|| blocked("SYNC_ADOPTION_CREDENTIAL_REQUIRED"))?,
            )
            .map_err(|_| blocked("SYNC_ADOPTION_CREDENTIAL_REQUIRED"))?;
        if secret.len() != 32
            || sync_identity::hash(&secret) != binding.secret_check_hash
            || credential["binding_id"] != binding.binding_id
            || credential["account_id"] != binding.account_id
            || credential["device_id"] != binding.device_id
        {
            return Err(blocked("SYNC_ADOPTION_CREDENTIAL_REQUIRED"));
        }
        let identity = sync_projection::Identity {
            binding_id: binding.binding_id.clone(),
            account_id: binding.account_id.clone(),
            device_id: binding.device_id.clone(),
            ledger_uuid: binding.ledger_uuid.clone(),
            secret,
        };
        // Pin the exact selected observations before local retirement clears
        // usage_events. Canonical alias edges are identity evidence; amounts
        // are deliberately absent from this match.
        let active_by_id: BTreeMap<i64, &Observation> =
            active.iter().map(|o| (o.observation_id, o)).collect();
        let mut native = BTreeMap::<(String, String), NativeBucket>::new();
        let mut native_query=c.prepare("SELECT l.basis_json,p.contribution_id,p.confirmed_revision,p.header_json FROM sync_lineages l JOIN sync_contributions p USING(binding_id,contribution_id) WHERE l.binding_id=?1 AND l.source=?2 AND l.domain='ledger' AND p.presence<>'retired' AND p.confirmed_revision<>'0'")?;
        let mut native_rows = native_query.query(params![binding.binding_id, source])?;
        while let Some(row) = native_rows.next()? {
            let basis: Value = serde_json::from_str(&row.get::<_, String>(0)?)
                .map_err(|_| LedgerError::CorruptLedger)?;
            if basis["kind"] != "request" {
                continue;
            }
            let family = basis["family"].as_str().ok_or(LedgerError::CorruptLedger)?;
            let namespace = basis["namespace"]
                .as_str()
                .ok_or(LedgerError::CorruptLedger)?;
            let key = basis["key"].as_str().ok_or(LedgerError::CorruptLedger)?;
            let selected:Option<i64>=c.query_row("SELECT selected_observation_id FROM usage_events WHERE identity_family=?1 AND namespace_key=?2 AND report_source=?4 AND identity_key=COALESCE((SELECT canonical_key FROM identity_alias WHERE identity_family=?1 AND namespace_key=?2 AND alias_key=?3),?3) AND state='active'",params![family,namespace,key,source],|r|r.get(0)).optional()?;
            let Some(observation) = selected.and_then(|id| active_by_id.get(&id)) else {
                continue;
            };
            let header: Value = serde_json::from_str(&row.get::<_, String>(3)?)
                .map_err(|_| LedgerError::CorruptLedger)?;
            let at = observation
                .occurred_ms
                .ok_or(LedgerError::IncompleteEvidence)?;
            if header["time"]["at_ms"]
                .as_str()
                .and_then(|v| v.parse::<i64>().ok())
                != Some(at)
                || header["model"] != sync_projection::export_model_key(&observation.model_key)
            {
                return Err(blocked("SYNC_ADOPTION_IDENTITY_CONFLICT"));
            }
            let (start_ms, end_ms, date) = super::metrics::day_bounds(timezone, at)?;
            let bucket = native
                .entry((date, observation.model_key.clone()))
                .or_insert_with(|| NativeBucket {
                    start_ms,
                    end_ms,
                    ..Default::default()
                });
            let id: String = row.get(1)?;
            bucket.retire.push((id.clone(), row.get(2)?));
            bucket.evidence.push(json!({"id":id,"selected_observation_id":observation.observation_id,"identity_digest":time_allocation::digest(&(family,namespace,key))?}));
        }
        bindings.push((binding, identity, native));
    }
    Ok(Prepared {
        bindings,
        version: sync_state::version(c)?,
    })
}

/// Seals one mixed transaction per proven bucket. The outer adoption transaction
/// also owns route publication; a failure rolls back facts, route, and outbox.
pub(crate) fn publish(
    c: &Connection,
    prepared: Prepared,
    source: &str,
    family: &str,
    namespace: &str,
    policy: &str,
    buckets: &[Bucket<'_>],
) -> Result<()> {
    if prepared.bindings.is_empty() {
        return Ok(());
    }
    if buckets.iter().any(|b| {
        b.members.len() != b.proofs.len()
            || b.members
                .iter()
                .any(|m| !b.proofs.contains_key(&m.member_key))
    }) {
        return Err(blocked("SYNC_ADOPTION_IDENTITY_UNPROVEN"));
    }
    c.execute_batch("CREATE TEMP TABLE IF NOT EXISTS sync_adoption_members(member_key TEXT PRIMARY KEY,identity_hash TEXT NOT NULL,semantic_hash TEXT NOT NULL,parent_key TEXT NOT NULL,occurred_ms INTEGER NOT NULL,model TEXT NOT NULL) WITHOUT ROWID; DELETE FROM sync_adoption_members;")?;
    for bucket in buckets {
        for member in bucket.members {
            let proof = &bucket.proofs[&member.member_key];
            c.execute(
                "INSERT INTO sync_adoption_members VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    member.member_key,
                    proof.identity_hash,
                    proof.semantic_hash,
                    bucket.parent_key,
                    member.occurred_ms,
                    sync_projection::export_model_key(bucket.model)
                ],
            )?;
        }
    }
    let version = sync_state::version(c)?;
    for (binding, identity, mut native) in prepared.bindings {
        let mut retire = BTreeMap::<String, Vec<(String, String)>>::new();
        let mut q=c.prepare("SELECT l.basis_json,p.contribution_id,p.confirmed_revision,p.payload_hash,p.header_json FROM sync_lineages l JOIN sync_contributions p USING(binding_id,contribution_id) WHERE l.binding_id=?1 AND l.source=?2 AND l.domain='live' AND p.presence<>'retired' AND p.confirmed_revision<>'0' ORDER BY json_extract(l.basis_json,'$.key'),json_extract(l.basis_json,'$.occurrence'),p.contribution_id")?;
        let mut rows = q.query(params![binding.binding_id, source])?;
        let mut previous_key = String::new();
        let mut hashes = BTreeSet::new();
        let mut matched = false;
        while let Some(row) = rows.next()? {
            let basis: Value = serde_json::from_str(&row.get::<_, String>(0)?)
                .map_err(|_| LedgerError::CorruptLedger)?;
            let key = basis["key"].as_str().ok_or(LedgerError::CorruptLedger)?;
            if key != previous_key {
                if matched && hashes.len() > 1 {
                    return Err(blocked("SYNC_ADOPTION_IDENTITY_CONFLICT"));
                }
                hashes.clear();
                matched = false;
                previous_key = key.into();
            }
            hashes.insert(row.get::<_, String>(3)?);
            if basis["kind"] != "request" {
                continue;
            }
            let occurrence = basis["occurrence"]
                .as_u64()
                .ok_or(LedgerError::CorruptLedger)? as usize;
            let source_record = time_allocation::digest(&(
                "legacy-source-record-v1",
                family,
                namespace,
                source,
                key,
            ))?;
            let member_key =
                time_allocation::digest(&("legacy-time-member-v1", source_record, occurrence))?;
            let found:Option<(String,String,i64,String)>=c.query_row("SELECT identity_hash,parent_key,occurred_ms,model FROM sync_adoption_members WHERE member_key=?1",[&member_key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
            let Some((proof, parent, time, model)) = found else {
                continue;
            };
            matched = true;
            let proof_from_basis = input_identity(
                basis["family"].as_str().ok_or(LedgerError::CorruptLedger)?,
                basis["namespace"]
                    .as_str()
                    .ok_or(LedgerError::CorruptLedger)?,
                key,
                occurrence,
            )?;
            let header: Value = serde_json::from_str(&row.get::<_, String>(4)?)
                .map_err(|_| LedgerError::CorruptLedger)?;
            if proof != proof_from_basis
                || header["time"]["at_ms"]
                    .as_str()
                    .and_then(|v| v.parse::<i64>().ok())
                    != Some(time)
                || header["model"] != model
            {
                return Err(blocked("SYNC_ADOPTION_IDENTITY_CONFLICT"));
            }
            retire
                .entry(parent)
                .or_default()
                .push((row.get(1)?, row.get(2)?));
        }
        if matched && hashes.len() > 1 {
            return Err(blocked("SYNC_ADOPTION_IDENTITY_CONFLICT"));
        }
        drop(rows);
        drop(q);
        c.execute_batch("CREATE TEMP TABLE IF NOT EXISTS sync_export_facts(id TEXT PRIMARY KEY,local_key TEXT NOT NULL,basis TEXT NOT NULL,header TEXT NOT NULL,payload_hash TEXT,need_put INTEGER DEFAULT 0) WITHOUT ROWID; CREATE TEMP TABLE IF NOT EXISTS sync_export_parts(id TEXT NOT NULL,part_index INTEGER NOT NULL,entry TEXT NOT NULL,PRIMARY KEY(id,part_index)) WITHOUT ROWID; DELETE FROM sync_export_facts;DELETE FROM sync_export_parts;")?;
        let parents: BTreeSet<&str> = buckets.iter().map(|b| b.parent_key).collect();
        let mut selected = BTreeSet::new();
        sync_projection::snapshot(c, &identity, source, |row| {
            if let (Some(local), Some(basis)) = (&row.local_key, &row.basis) {
                let value: Value =
                    serde_json::from_str(basis).map_err(|_| LedgerError::CorruptLedger)?;
                if value["domain"] != "ledger"
                    || !parents.contains(value["key"].as_str().unwrap_or(""))
                {
                    return Ok(());
                }
                selected.insert(row.id.clone());
                c.execute(
                    "INSERT INTO sync_export_facts(id,local_key,basis,header) VALUES(?1,?2,?3,?4)",
                    params![row.id, local, basis, row.entry.to_string()],
                )?;
            } else if selected.contains(&row.id) {
                if let Some(hash) = row.payload_hash {
                    c.execute(
                        "UPDATE sync_export_facts SET payload_hash=?1 WHERE id=?2",
                        params![hash, row.id],
                    )?;
                } else {
                    c.execute(
                        "INSERT INTO sync_export_parts VALUES(?1,?2,?3)",
                        params![
                            row.id,
                            row.entry["part_index"].as_i64(),
                            row.entry.to_string()
                        ],
                    )?;
                }
            }
            Ok(())
        })?;
        let scope_cutoff: Option<i64> = c
            .query_row(
                "SELECT history_from_ms FROM sync_scopes WHERE binding_id=?1 AND source=?2",
                params![binding.binding_id, source],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        let cutoff = binding
            .history_from_ms
            .into_iter()
            .chain(scope_cutoff)
            .max();
        for bucket in buckets {
            if cutoff.is_some_and(|t| bucket.start_ms < t) {
                continue;
            }
            let parent_id: String = c
                .query_row(
                    "SELECT id FROM sync_export_facts WHERE json_extract(basis,'$.key')=?1",
                    [bucket.parent_key],
                    |r| r.get(0),
                )
                .optional()?
                .ok_or_else(|| blocked("SYNC_ADOPTION_PARENT_UNSELECTED"))?;
            let mut retired = retire.remove(bucket.parent_key).unwrap_or_default();
            let native_bucket = native
                .remove(&(bucket.native_date.to_owned(), bucket.model.to_owned()))
                .unwrap_or_default();
            retired.extend(native_bucket.retire);
            retired.sort();
            retired.dedup();
            let transaction = sync_state::seal_group(
                c,
                &binding,
                source,
                version,
                std::slice::from_ref(&parent_id),
                &retired,
                "adoption",
            )?;
            let evidence = json!({"version":1,"kind":"legacy-adoption-v1","policy_id":policy,"parent_ids":[parent_id],"retire":retired.iter().map(|(id,r)|json!({"id":id,"expected_revision":r})).collect::<Vec<_>>(),"parent_observation_id":bucket.observation_id,"native_edges":native_bucket.evidence,"member_count":bucket.members.len(),"member_digest":time_allocation::digest(&bucket.proofs)?,"source_family":family,"namespace":namespace,"bucket":{"native_date":bucket.native_date,"model":bucket.model,"start_ms":bucket.start_ms,"end_ms":bucket.end_ms}});
            c.execute("INSERT INTO sync_transition_groups VALUES(?1,?2,?3,'adoption',?4,?5,?6,?7,'published')",params![new_epoch(),binding.binding_id,source,evidence.to_string(),prepared.version,version,transaction])?;
        }
        // Explicit adoption replaces the complete selected interval. A native
        // date/model bucket absent from the frozen live input is a proven local
        // removal, so preserve that exact edge as a retire-only transaction.
        for ((date, model), mut bucket) in native {
            if cutoff.is_some_and(|t| bucket.start_ms < t) {
                continue;
            }
            bucket.retire.sort();
            bucket.retire.dedup();
            let transaction = sync_state::seal_group(
                c,
                &binding,
                source,
                version,
                &[],
                &bucket.retire,
                "adoption",
            )?;
            let evidence = json!({"version":1,"kind":"legacy-adoption-v1","policy_id":policy,"parent_ids":[],"retire":bucket.retire.iter().map(|(id,r)|json!({"id":id,"expected_revision":r})).collect::<Vec<_>>(),"parent_observation_id":null,"native_edges":bucket.evidence,"member_count":0,"member_digest":time_allocation::digest(&Vec::<InputProof>::new())?,"source_family":family,"namespace":namespace,"bucket":{"native_date":date,"model":model,"start_ms":bucket.start_ms,"end_ms":bucket.end_ms}});
            c.execute("INSERT INTO sync_transition_groups VALUES(?1,?2,?3,'adoption',?4,?5,?6,?7,'published')",params![new_epoch(),binding.binding_id,source,evidence.to_string(),prepared.version,version,transaction])?;
        }
        c.execute("INSERT INTO sync_scopes(binding_id,source,route,state) VALUES(?1,?2,'ledger','ready') ON CONFLICT(binding_id,source) DO UPDATE SET route='ledger',source_epoch=source_epoch+1,state='ready',reason=NULL",params![binding.binding_id,source])?;
    }
    Ok(())
}
