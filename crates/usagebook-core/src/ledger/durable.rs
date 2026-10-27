//! Shared, bounded acquisition for explicitly admitted source protocols.
//! Adapters own stable reads and native identity; this layer owns atomic evidence
//! and checkpoints. It never turns a saved candidate into an eligible fact.
use super::{db, types::*, Ledger};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::Serialize;
use serde_json::Value;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct AdapterSpec {
    pub identity_family: &'static str,
    pub collector_key: &'static str,
    pub report_source: &'static str,
    pub native_tool: &'static str,
    pub parser_version: &'static str,
    pub capability_version: &'static str,
    pub identity_version: i64,
    pub semantics_version: i64,
}

/// Kept explicit so an unsupported binary cannot silently reinterpret saved facts.
pub fn supported_parser(family: &str, parser: &str, identity_version: i64) -> bool {
    identity_version == 1
        && (matches!(
            (family, parser),
            ("claude", "claude-ledger-v1")
                | ("claude", "claude-ledger-v2")
                | ("codex", "codex-ledger-v1")
                | ("kimi", "kimi-ledger-v1")
                | ("kimi-work", "kimi-work-ledger-v1")
                | ("qwen", "qwen-ledger-v1")
                | ("ccswitch", "ccswitch-ledger-v1")
                | ("opencode", "reported-history-v1")
                | ("pi", "reported-history-v1")
                | ("opencode", "opencode-ledger-v1")
                | ("pi", "pi-ledger-v1")
                | ("omp", "omp-ledger-v1")
                | ("goose", "goose-ledger-v1")
                | ("openclaw", "openclaw-ledger-v1")
                | ("copilot", "copilot-ledger-v1")
                | ("zcode", "zcode-ledger-v1")
                | ("minimax", "minimax-ledger-v1")
                | ("kilo", "kilo-ledger-v1")
                | ("hermes", "hermes-ledger-v1")
                | ("qoder", "qoder-ledger-v1")
                | ("dsh", "dsh-ledger-v1")
        ) || (parser == "reported-history-v1"
            && !matches!(family, "ccswitch" | "ccswitch-rollup")
            && super::DURABLE_EVIDENCE_SOURCES.contains(&family))
            || (parser == super::document_adapters::PARSER
                && super::document_adapters::spec(family).is_some()))
}

#[derive(Clone, Debug)]
pub struct DurableRoot {
    pub root_id: String,
    pub import_id: i64,
    pub enabled: bool,
    pub namespace: String,
    pub spec: AdapterSpec,
}

#[derive(Clone, Debug)]
pub struct ObjectSpec {
    pub object_key: String,
    pub locator: String,
    /// A verified content epoch, not an inode or mtime alone. Without an append
    /// prefix proof, an adapter must use a new generation and replay from zero.
    pub content_generation: String,
    /// Position units are bytes for streams and one for an immutable row image.
    pub byte_length: i64,
    pub mtime_ns: Option<i64>,
}

/// Private, bounded value image, not a list of keys to reread from the source.
#[derive(Clone, Debug)]
pub struct SnapshotObject {
    pub object_key: String,
    pub checkpoint: Vec<u8>,
    pub row_count: i64,
}

#[derive(Clone, Debug)]
pub struct DurableObject {
    pub object_id: i64,
    pub root: DurableRoot,
    pub generation: String,
    pub revision: i64,
    pub saved_offset: i64,
    pub checkpoint: Option<Vec<u8>>,
    pub byte_length: i64,
    pub mtime_ns: Option<i64>,
    pub epoch: String,
    pub complete: bool,
}

#[derive(Clone, Debug)]
pub struct DurableEvidence {
    pub observation: Observation,
    /// Only adapter-specific, validated scalar usage/identity fields are accepted.
    pub projection: Option<Value>,
}

#[derive(Debug, Default, Serialize)]
pub struct SaveSummary {
    pub inserted: u64,
    pub duplicates: u64,
    pub suppressed: u64,
    pub pending: u64,
}

#[derive(Debug, Serialize)]
pub struct ImportStatus {
    pub import_id: i64,
    pub root_id: String,
    pub source: String,
    pub capability_version: String,
    pub enabled: bool,
    pub enumeration_done: bool,
    pub initial_pass_finished: bool,
    pub target_objects: i64,
    pub saved_objects: i64,
    pub gap_objects: i64,
    pub remaining_objects: i64,
    pub error_objects: i64,
    pub pending_candidates: i64,
    pub saved_observation_id: i64,
    pub published_observation_id: i64,
}

fn bounded_text(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.contains('\0')
}

// Resolve aliases in the still-existing prefix even after the source itself
// disappeared. Falling back to the raw spelling would create a second root
// for e.g. /var/... versus /private/var/... and strand the old import targets.
fn canonical_root_locator(path: &Path) -> PathBuf {
    let mut prefix = path.to_path_buf();
    let mut suffix = Vec::new();
    loop {
        if let Ok(mut canonical) = prefix.canonicalize() {
            for name in suffix.iter().rev() {
                canonical.push(name);
            }
            return canonical;
        }
        let Some(name) = prefix.file_name().map(|name| name.to_os_string()) else {
            return path.to_path_buf();
        };
        suffix.push(name);
        if !prefix.pop() {
            return path.to_path_buf();
        }
        if prefix.as_os_str().is_empty() {
            prefix.push(".");
        }
    }
}

fn validate_spec(spec: &AdapterSpec) -> Result<()> {
    if !supported_parser(
        spec.identity_family,
        spec.parser_version,
        spec.identity_version,
    ) || spec.semantics_version != 1
        || !bounded_text(spec.capability_version, 128)
        || !bounded_text(spec.collector_key, 128)
        || !bounded_text(spec.report_source, 128)
        || !bounded_text(spec.native_tool, 128)
    {
        return Err(LedgerError::UnsupportedVersion);
    }
    Ok(())
}

fn finish_targets(c: &Connection, import_id: i64) -> Result<()> {
    // A replay can fulfill the same object/generation in an earlier capability's
    // sealed manifest too. Complete every satisfied manifest for this root;
    // leaving the older job open would make source status permanently unfinished.
    c.execute(
        "UPDATE durable_imports SET completed_ms=COALESCE(completed_ms,?2)
         WHERE root_id=(SELECT root_id FROM durable_imports WHERE import_id=?1)
         AND enumeration_done=1
         AND NOT EXISTS(SELECT 1 FROM durable_import_targets t WHERE t.import_id=durable_imports.import_id AND t.state='pending')",
        params![import_id, now_ms()],
    )?;
    Ok(())
}

impl Ledger {
    /// Key-only CC manifests cannot be replayed as sealed values. Close their
    /// acquisition work explicitly even if the upstream file has disappeared;
    /// immutable observations already saved from them remain untouched.
    pub(crate) fn retire_cc_key_manifests(&mut self, root: &DurableRoot) -> Result<()> {
        self.require_writer()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute("UPDATE source_objects SET target_done=1,state='missing',error_code='CC_KEYS_REQUIRE_VALUE_RECAPTURE',checkpoint=NULL,checkpoint_format=NULL,saved_offset=NULL,object_revision=object_revision+1 WHERE root_id=?1 AND object_key LIKE 'cc-manifest-v1:%' AND (target_done=0 OR state<>'missing' OR checkpoint IS NOT NULL)",[&root.root_id])?;
        let targets = tx.execute("UPDATE durable_import_targets SET state='gap',error_code='CC_KEYS_REQUIRE_VALUE_RECAPTURE',finished_ms=?2 WHERE state='pending' AND object_id IN(SELECT object_id FROM source_objects WHERE root_id=?1 AND object_key LIKE 'cc-manifest-v1:%')",params![root.root_id,now_ms()])?;
        if changed > 0 || targets > 0 {
            finish_targets(&tx, root.import_id)?;
            db::bump(&tx)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Start a new bounded snapshot attempt. An interrupted source transaction
    /// cannot be resumed: retain its attempt record, but never merge its targets
    /// into a later snapshot. Only sealing below creates targets.
    pub fn begin_durable_snapshot(&mut self, root: &DurableRoot) -> Result<String> {
        self.require_writer()?;
        let id = uuid::Uuid::new_v4().to_string();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let enabled: bool = tx.query_row(
            "SELECT enabled FROM source_roots WHERE root_id=?1",
            [&root.root_id],
            |r| r.get(0),
        )?;
        if !enabled {
            return Err(LedgerError::Cancelled);
        }
        tx.execute("UPDATE durable_snapshots SET state='aborted',error_code='SNAPSHOT_INTERRUPTED' WHERE root_id=?1 AND state='building'", [&root.root_id])?;
        tx.execute("INSERT INTO durable_snapshots(snapshot_id,import_id,root_id,state,created_ms) VALUES(?1,?2,?3,'building',?4)", params![id,root.import_id,root.root_id,now_ms()])?;
        tx.commit()?;
        Ok(id)
    }

    pub fn abort_durable_snapshot(&mut self, snapshot: &str, code: &str) -> Result<()> {
        self.require_writer()?;
        if !bounded_text(code, 128) || !code.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(LedgerError::InvalidInput("invalid snapshot error".into()));
        }
        self.conn.execute("UPDATE durable_snapshots SET state='aborted',error_code=?2 WHERE snapshot_id=?1 AND state='building'",params![snapshot,code])?;
        Ok(())
    }

    /// Publish the complete value manifest and its initial target set together.
    /// No upstream handle is retained here. A failure at any chunk rolls back
    /// all objects/targets and leaves a visibly unsealed attempt for recovery.
    pub fn seal_durable_snapshot(
        &mut self,
        root: &DurableRoot,
        snapshot: &str,
        objects: &[SnapshotObject],
        fingerprint: &str,
    ) -> Result<()> {
        self.require_writer()?;
        if root.spec.identity_family != "ccswitch"
            || fingerprint.len() != 64
            || !fingerprint.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(LedgerError::InvalidInput("invalid CC snapshot".into()));
        }
        let mut row_count = 0i64;
        let mut gap_count = 0u64;
        let mut byte_count = 0usize;
        let mut keys = std::collections::BTreeSet::new();
        for object in objects {
            if !object.object_key.starts_with("cc-values-v2:")
                || !keys.insert(&object.object_key)
                || object.checkpoint.is_empty()
                || object.checkpoint.len() > 65536
                || object.row_count < 0
            {
                return Err(LedgerError::InvalidInput("invalid snapshot block".into()));
            }
            let count = super::ccswitch::validate_snapshot_checkpoint(&object.checkpoint)?;
            gap_count = gap_count
                .checked_add(super::ccswitch::snapshot_gap_count(&object.checkpoint)?)
                .ok_or(LedgerError::NumericOverflow)?;
            if count != object.row_count {
                return Err(LedgerError::InvalidInput(
                    "snapshot target differs from value count".into(),
                ));
            }
            row_count = row_count
                .checked_add(object.row_count)
                .ok_or(LedgerError::NumericOverflow)?;
            byte_count = byte_count
                .checked_add(object.checkpoint.len())
                .ok_or(LedgerError::NumericOverflow)?;
        }
        if byte_count > 64 * 1024 * 1024 {
            return Err(LedgerError::RecordTooLarge);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let valid:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM durable_snapshots s JOIN source_roots r USING(root_id) WHERE snapshot_id=?1 AND s.import_id=?2 AND s.root_id=?3 AND s.state='building' AND r.enabled=1 AND r.parser_version=?4)",params![snapshot,root.import_id,root.root_id,root.spec.parser_version],|r|r.get(0))?;
        if !valid {
            return Err(LedgerError::StaleObservation);
        }
        let unchanged:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM durable_snapshots s WHERE s.root_id=?1 AND s.import_id=?2 AND s.state='sealed' AND s.fingerprint=?3 AND s.gap_count=0 AND s.snapshot_id=(SELECT snapshot_id FROM durable_snapshots WHERE root_id=?1 AND state='sealed' ORDER BY sealed_ms DESC,rowid DESC LIMIT 1)) AND NOT EXISTS(SELECT 1 FROM source_objects WHERE root_id=?1 AND object_key LIKE 'cc-values-v2:%' AND target_done=0)",params![root.root_id,root.import_id,fingerprint],|r|r.get(0))?;
        if unchanged {
            tx.execute("UPDATE durable_snapshots SET state='aborted',error_code='UNCHANGED_SOURCE_SNAPSHOT' WHERE snapshot_id=?1",[snapshot])?;
            tx.commit()?;
            return Ok(());
        }
        // Old key-only manifests cannot certify that their former values were
        // captured. Keep the old import as a gap, rather than silently replace it.
        tx.execute("UPDATE durable_import_targets SET state='gap',error_code='CC_KEYS_REQUIRE_VALUE_RECAPTURE',finished_ms=?2 WHERE state='pending' AND object_id IN (SELECT object_id FROM source_objects WHERE root_id=?1 AND object_key LIKE 'cc-manifest-v1:%')",params![root.root_id,now_ms()])?;
        tx.execute("UPDATE source_objects SET target_done=1,state='missing',error_code='CC_KEYS_REQUIRE_VALUE_RECAPTURE',checkpoint=NULL,checkpoint_format=NULL,saved_offset=NULL WHERE root_id=?1 AND object_key LIKE 'cc-manifest-v1:%'",[&root.root_id])?;
        for object in objects {
            let locator = format!("cc-snapshot:{snapshot}:{}", object.object_key);
            tx.execute("INSERT INTO source_objects(root_id,object_key,locator,content_generation,byte_length,object_revision,state,parser_version,saved_offset,checkpoint_format,checkpoint,target_generation,target_end,target_done,last_observed_ms,audit_due_ms) VALUES(?1,?2,?3,?4,?5,0,'ready',?6,0,2,?7,?4,?5,0,?8,?8) ON CONFLICT(root_id,object_key) DO UPDATE SET locator=excluded.locator,content_generation=excluded.content_generation,byte_length=excluded.byte_length,object_revision=source_objects.object_revision+1,state='ready',parser_version=excluded.parser_version,saved_offset=0,checkpoint_format=2,checkpoint=excluded.checkpoint,target_generation=excluded.target_generation,target_end=excluded.target_end,target_done=0,last_observed_ms=excluded.last_observed_ms,error_code=NULL,retry_after_ms=NULL",params![root.root_id,object.object_key,locator,snapshot,object.row_count,root.spec.parser_version,object.checkpoint,now_ms()])?;
            let object_id: i64 = tx.query_row(
                "SELECT object_id FROM source_objects WHERE root_id=?1 AND object_key=?2",
                params![root.root_id, object.object_key],
                |r| r.get(0),
            )?;
            for member in super::ccswitch::snapshot_members(&object.checkpoint)?.1 {
                tx.execute("INSERT INTO durable_snapshot_members(snapshot_id,row_key,fingerprint,consumer_tool,grain,native_date) VALUES(?1,?2,?3,?4,?5,?6)",params![snapshot,member.row_key,member.fingerprint,member.observation.native_tool,member.observation.fact_kind,member.observation.native_date])?;
            }

            tx.execute("INSERT INTO durable_import_targets(import_id,object_id,target_generation,target_end) SELECT ?1,?2,?3,?4 WHERE EXISTS(SELECT 1 FROM durable_imports WHERE import_id=?1 AND enumeration_done=0) ON CONFLICT DO NOTHING",params![root.import_id,object_id,snapshot,object.row_count])?;
        }
        tx.execute("UPDATE durable_snapshots SET state='sealed',sealed_ms=?2,fingerprint=?3,row_count=?4,byte_count=?5,gap_count=?6 WHERE snapshot_id=?1",params![snapshot,now_ms(),fingerprint,row_count,byte_count as i64,gap_count as i64])?;
        tx.execute("UPDATE durable_imports SET enumeration_done=1,sealed_ms=COALESCE(sealed_ms,?2) WHERE import_id=?1",params![root.import_id,now_ms()])?;
        tx.execute(
            "UPDATE source_roots SET enumeration_done=1,last_enumerated_ms=?2 WHERE root_id=?1",
            params![root.root_id, now_ms()],
        )?;
        // Retain the selected source evidence itself. Old complete source-report
        // manifests are no longer offered for adoption; keep only members used
        // by an active adopted policy, not another full copy of all source IDs.
        tx.execute("DELETE FROM durable_snapshot_members WHERE snapshot_id IN(SELECT snapshot_id FROM durable_snapshots WHERE root_id=?1 AND state='sealed' AND snapshot_id<>?2) AND NOT EXISTS(SELECT 1 FROM adoption_items a JOIN adoption_policies p USING(policy_id) WHERE p.status='active' AND a.source_observation_id=durable_snapshot_members.observation_id)",params![root.root_id,snapshot])?;
        tx.execute("UPDATE durable_snapshots SET error_code='SNAPSHOT_SUPERSEDED',gap_count=MAX(gap_count,row_count-(SELECT COUNT(*) FROM durable_snapshot_members m WHERE m.snapshot_id=durable_snapshots.snapshot_id)) WHERE root_id=?1 AND state='sealed' AND snapshot_id<>?2",params![root.root_id,snapshot])?;
        finish_targets(&tx, root.import_id)?;
        db::bump(&tx)?;
        if super::ccswitch::has_privacy_controls(&tx)? {
            super::ccswitch::scrub_snapshot_checkpoints(&tx, |o| {
                super::ingest::exact_suppressed(&tx, o)
            })?;
        }
        regression_stop("cc_snapshot_before_seal_commit");
        tx.commit()?;
        regression_stop("cc_snapshot_after_seal_commit");
        Ok(())
    }

    /// Bridge the established Claude reader into the same first-import status.
    /// Call before and after acquisition. Its mutable per-poll target is copied
    /// only while the initial manifest is open, never on a subsequent append.
    pub fn track_claude_imports(&mut self) -> Result<()> {
        self.require_writer()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let roots: Vec<(String,String,i64,bool,bool)> = tx.prepare("SELECT root_id,parser_version,identity_version,enabled,enumeration_done FROM source_roots WHERE identity_family='claude'")?
            .query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?.collect::<rusqlite::Result<_>>()?;
        for (root, parser, identity, enabled, enumerated) in roots {
            if !supported_parser("claude", &parser, identity) {
                return Err(LedgerError::UnsupportedVersion);
            }
            let capability = format!("{parser}:all-project-history");
            tx.execute("INSERT INTO durable_imports(root_id,capability_version,parser_version,identity_version,semantics_version,created_ms) VALUES(?1,?2,?3,?4,1,?5) ON CONFLICT DO NOTHING",params![root,capability,parser,identity,now_ms()])?;
            let id:i64=tx.query_row("SELECT import_id FROM durable_imports WHERE root_id=?1 AND capability_version=?2 AND parser_version=?3 AND identity_version=?4 AND semantics_version=1",params![root,capability,parser,identity],|r|r.get(0))?;
            if !enabled {
                continue;
            }
            tx.execute("INSERT INTO durable_import_targets(import_id,object_id,target_generation,target_end) SELECT ?1,object_id,content_generation,byte_length FROM source_objects WHERE root_id=?2 AND state<>'missing' AND EXISTS(SELECT 1 FROM durable_imports WHERE import_id=?1 AND enumeration_done=0) ON CONFLICT DO NOTHING",params![id,root])?;
            tx.execute("UPDATE durable_import_targets SET state='saved',finished_ms=?2,error_code=NULL WHERE import_id=?1 AND state='pending' AND EXISTS(SELECT 1 FROM source_objects o WHERE o.object_id=durable_import_targets.object_id AND o.content_generation=durable_import_targets.target_generation AND o.saved_offset>=durable_import_targets.target_end)",params![id,now_ms()])?;
            tx.execute("UPDATE durable_import_targets SET state='gap',finished_ms=?2,error_code='SOURCE_CHANGED_BEFORE_SAVE' WHERE import_id=?1 AND state='pending' AND EXISTS(SELECT 1 FROM source_objects o WHERE o.object_id=durable_import_targets.object_id AND (o.content_generation<>durable_import_targets.target_generation OR o.state='missing'))",params![id,now_ms()])?;
            tx.execute("UPDATE durable_imports SET saved_observation_id=MAX(saved_observation_id,COALESCE((SELECT MAX(o.observation_id) FROM usage_observations o JOIN source_objects f USING(object_id) WHERE f.root_id=?2),0)),enumeration_done=MAX(enumeration_done,?3),sealed_ms=CASE WHEN ?3 THEN COALESCE(sealed_ms,?4) ELSE sealed_ms END WHERE import_id=?1",params![id,root,enumerated,now_ms()])?;
            finish_targets(&tx, id)?;
        }
        db::bump(&tx)?;
        tx.commit()?;
        Ok(())
    }

    pub fn register_durable_root(
        &mut self,
        spec: &AdapterSpec,
        namespace: &str,
        locator: &Path,
    ) -> Result<DurableRoot> {
        self.require_writer()?;
        validate_spec(spec)?;
        if !bounded_text(namespace, 512) {
            return Err(LedgerError::InvalidInput("invalid source namespace".into()));
        }
        let canonical = canonical_root_locator(locator);
        let locator = canonical
            .to_str()
            .filter(|p| bounded_text(p, 16_384))
            .ok_or_else(|| LedgerError::InvalidInput("invalid source locator".into()))?;
        // Locators identify acquisition roots only; they never create a new
        // consumption namespace. Native identities deduplicate root aliases.
        let suggested_root_id = hash_parts(
            "usagebook/durable-root/v1",
            &[spec.identity_family, namespace, spec.collector_key, locator],
        );
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing_roots:Vec<(String,String)>=tx.prepare("SELECT root_id,locator FROM source_roots WHERE identity_family=?1 AND namespace_key=?2 AND collector_key=?3 ORDER BY root_id")?
            .query_map(params![spec.identity_family,namespace,spec.collector_key],|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
        let root_id = existing_roots
            .into_iter()
            .find(|(_, previous)| {
                previous == locator || canonical_root_locator(Path::new(previous)) == canonical
            })
            .map(|(id, _)| id)
            .unwrap_or(suggested_root_id);
        let routes: Vec<String> = tx
            .prepare("SELECT DISTINCT route FROM source_roots WHERE report_source=?1")?
            .query_map([spec.report_source], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        if routes.len() > 1 {
            return Err(LedgerError::RouteInconsistent);
        }
        let route = routes.first().map(String::as_str).unwrap_or("live");
        let enabled: bool = tx.query_row(
            "SELECT COALESCE(MIN(enabled),1) FROM source_roots WHERE report_source=?1 AND collector_key<>'reported-history-v1'",
            [spec.report_source],
            |r| r.get(0),
        )?;
        let inserted = tx.execute(
            "INSERT INTO source_roots(root_id,identity_family,namespace_key,collector_key,report_source,native_tool,locator,enabled,identity_version,parser_version,route)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11) ON CONFLICT DO NOTHING",
            params![root_id,spec.identity_family,namespace,spec.collector_key,spec.report_source,spec.native_tool,locator,enabled,spec.identity_version,spec.parser_version,route])?;
        let (old_parser, old_identity, root_enabled): (String, i64, bool) = tx.query_row(
            "SELECT parser_version,identity_version,enabled FROM source_roots WHERE root_id=?1",
            [&root_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if old_identity != spec.identity_version {
            return Err(LedgerError::IdentityMappingRequired);
        }
        if old_parser != spec.parser_version {
            tx.execute(
                "UPDATE source_roots SET parser_version=?2,enumeration_done=0 WHERE root_id=?1",
                params![root_id, spec.parser_version],
            )?;
            tx.execute("UPDATE source_objects SET parser_version=?2,state='replay_required',saved_offset=NULL,checkpoint=NULL,checkpoint_format=NULL,target_done=0,object_revision=object_revision+1 WHERE root_id=?1",params![root_id,spec.parser_version])?;
        }
        let new_import = tx.execute("INSERT INTO durable_imports(root_id,capability_version,parser_version,identity_version,semantics_version,created_ms) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT DO NOTHING",params![root_id,spec.capability_version,spec.parser_version,spec.identity_version,spec.semantics_version,now_ms()])?;
        if new_import > 0 && inserted == 0 && old_parser == spec.parser_version {
            // A discovery/format capability upgrade must revisit old objects,
            // even if its parser version intentionally stays compatible.
            tx.execute("UPDATE source_objects SET state='replay_required',saved_offset=NULL,checkpoint=NULL,checkpoint_format=NULL,target_done=0,object_revision=object_revision+1 WHERE root_id=?1", [&root_id])?;
        }
        let import_id = tx.query_row("SELECT import_id FROM durable_imports WHERE root_id=?1 AND capability_version=?2 AND parser_version=?3 AND identity_version=?4 AND semantics_version=?5",params![root_id,spec.capability_version,spec.parser_version,spec.identity_version,spec.semantics_version], |r| r.get(0))?;
        if inserted > 0 || new_import > 0 || old_parser != spec.parser_version {
            db::bump(&tx)?;
        }
        tx.commit()?;
        Ok(DurableRoot {
            root_id,
            import_id,
            enabled: root_enabled,
            namespace: namespace.to_owned(),
            spec: spec.clone(),
        })
    }

    pub fn discover_durable_object(
        &mut self,
        root: &DurableRoot,
        spec: ObjectSpec,
    ) -> Result<DurableObject> {
        self.discover_durable_object_inner(root, spec, None)
    }

    /// Stage a bounded, versioned list of native usage keys atomically with its
    /// first target. The adapter may resume that exact list after source pruning.
    pub fn stage_durable_manifest(
        &mut self,
        root: &DurableRoot,
        spec: ObjectSpec,
        checkpoint: &[u8],
    ) -> Result<DurableObject> {
        if checkpoint.is_empty() || checkpoint.len() > 65536 {
            return Err(LedgerError::RecordTooLarge);
        }
        self.discover_durable_object_inner(root, spec, Some(checkpoint))
    }

    fn discover_durable_object_inner(
        &mut self,
        root: &DurableRoot,
        spec: ObjectSpec,
        initial_checkpoint: Option<&[u8]>,
    ) -> Result<DurableObject> {
        self.require_writer()?;
        validate_spec(&root.spec)?;
        if spec.byte_length < 0
            || !bounded_text(&spec.object_key, 16_384)
            || !bounded_text(&spec.locator, 16_384)
            || !bounded_text(&spec.content_generation, 512)
        {
            return Err(LedgerError::InvalidInput("invalid durable object".into()));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let enabled: bool = tx.query_row("SELECT enabled FROM source_roots WHERE root_id=?1 AND parser_version=?2 AND identity_version=?3",params![root.root_id,root.spec.parser_version,root.spec.identity_version], |r| r.get(0))?;
        if !enabled {
            return Err(LedgerError::Cancelled);
        }
        let old: Option<(i64,String,i64,String)> = tx.query_row("SELECT object_id,content_generation,byte_length,state FROM source_objects WHERE root_id=?1 AND object_key=?2",params![root.root_id,spec.object_key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        let object_id = if let Some((id, generation, length, state)) = old {
            if generation != spec.content_generation || length > spec.byte_length {
                tx.execute("UPDATE durable_import_targets SET state='gap',error_code='SOURCE_CHANGED_BEFORE_SAVE',finished_ms=?2 WHERE object_id=?1 AND state='pending'",params![id,now_ms()])?;
                tx.execute("UPDATE source_objects SET locator=?2,content_generation=?3,byte_length=?4,mtime_ns=?5,object_revision=object_revision+1,state='ready',parser_version=?6,saved_offset=0,checkpoint_format=1,checkpoint=X'',target_generation=?3,target_end=?4,target_done=0,last_observed_ms=?7,error_code=NULL,retry_after_ms=NULL,quick_fingerprint=NULL WHERE object_id=?1",params![id,spec.locator,spec.content_generation,spec.byte_length,spec.mtime_ns,root.spec.parser_version,now_ms()])?;
            } else if length != spec.byte_length || state != "ready" {
                let replay = state == "replay_required";
                tx.execute("UPDATE source_objects SET locator=?2,byte_length=?3,mtime_ns=?4,object_revision=object_revision+1,state='ready',saved_offset=CASE WHEN ?5 THEN 0 ELSE saved_offset END,checkpoint_format=1,checkpoint=CASE WHEN ?5 THEN X'' ELSE checkpoint END,target_generation=content_generation,target_end=?3,target_done=CASE WHEN ?5 OR byte_length<>?3 THEN 0 ELSE target_done END,last_observed_ms=?6,error_code=NULL,retry_after_ms=NULL WHERE object_id=?1",params![id,spec.locator,spec.byte_length,spec.mtime_ns,replay,now_ms()])?;
            }
            id
        } else {
            tx.execute("INSERT INTO source_objects(root_id,object_key,locator,content_generation,byte_length,mtime_ns,object_revision,state,parser_version,saved_offset,checkpoint_format,checkpoint,target_generation,target_end,last_observed_ms,audit_due_ms) VALUES(?1,?2,?3,?4,?5,?6,0,'ready',?7,0,1,X'',?4,?5,?8,?8)",params![root.root_id,spec.object_key,spec.locator,spec.content_generation,spec.byte_length,spec.mtime_ns,root.spec.parser_version,now_ms()])?;
            tx.last_insert_rowid()
        };
        // Recent lanes are supplemental checkpoints, never members of the
        // fixed historical target manifest (they now run before enumeration).
        if !matches!(
            spec.object_key.as_str(),
            "cc-recent-v1" | "local-recent-v1" | "native-db-recent-v2"
        ) {
            tx.execute("INSERT INTO durable_import_targets(import_id,object_id,target_generation,target_end,state,finished_ms) SELECT ?1,?2,?3,?4,CASE WHEN target_done=1 THEN 'saved' ELSE 'pending' END,CASE WHEN target_done=1 THEN ?5 ELSE NULL END FROM source_objects WHERE object_id=?2 AND EXISTS(SELECT 1 FROM durable_imports WHERE import_id=?1 AND enumeration_done=0) ON CONFLICT DO NOTHING",params![root.import_id,object_id,spec.content_generation,spec.byte_length,now_ms()])?;
        }
        if let Some(checkpoint) = initial_checkpoint {
            tx.execute("UPDATE source_objects SET checkpoint=?2 WHERE object_id=?1 AND saved_offset=0 AND checkpoint=X''",params![object_id,checkpoint])?;
        }
        let result = load_object(&tx, root, object_id)?;
        finish_targets(&tx, root.import_id)?;
        db::bump(&tx)?;
        tx.commit()?;
        Ok(result)
    }

    /// Call only after all entries in the declared discovery scope were visited.
    /// Partial enumeration must remain retryable and cannot certify absence.
    pub fn finish_durable_enumeration(&mut self, root: &DurableRoot) -> Result<()> {
        self.require_writer()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("UPDATE durable_imports SET enumeration_done=1,sealed_ms=COALESCE(sealed_ms,?2) WHERE import_id=?1 AND EXISTS(SELECT 1 FROM source_roots WHERE root_id=?3 AND enabled=1)",params![root.import_id,now_ms(),root.root_id])?;
        tx.execute("UPDATE source_roots SET enumeration_done=1,last_enumerated_ms=?2,error_code=NULL WHERE root_id=?1 AND enabled=1",params![root.root_id,now_ms()])?;
        finish_targets(&tx, root.import_id)?;
        db::bump(&tx)?;
        tx.commit()?;
        Ok(())
    }

    pub fn save_durable_batch(
        &mut self,
        object: &mut DurableObject,
        observations: &[Observation],
        next_offset: i64,
        checkpoint: &[u8],
        complete: bool,
    ) -> Result<SaveSummary> {
        self.save_durable_refs(
            object,
            observations.iter().map(|o| (o, None)),
            next_offset,
            checkpoint,
            complete,
            None,
        )
    }

    pub fn save_durable_evidence_batch(
        &mut self,
        object: &mut DurableObject,
        evidence: &[DurableEvidence],
        next_offset: i64,
        checkpoint: &[u8],
        complete: bool,
    ) -> Result<SaveSummary> {
        self.save_durable_refs(
            object,
            evidence
                .iter()
                .map(|e| (&e.observation, e.projection.as_ref())),
            next_offset,
            checkpoint,
            complete,
            None,
        )
    }

    /// The caller carries a cumulative gap flag in its checkpoint; a later
    /// healthy chunk alone cannot establish that an earlier missing key returned.
    pub fn save_durable_evidence_batch_with_gap(
        &mut self,
        object: &mut DurableObject,
        evidence: &[DurableEvidence],
        next_offset: i64,
        checkpoint: &[u8],
        complete: bool,
        gap_code: Option<&str>,
    ) -> Result<SaveSummary> {
        self.save_durable_refs(
            object,
            evidence
                .iter()
                .map(|e| (&e.observation, e.projection.as_ref())),
            next_offset,
            checkpoint,
            complete,
            gap_code,
        )
    }

    fn save_durable_refs<'a>(
        &mut self,
        object: &mut DurableObject,
        observations: impl Iterator<Item = (&'a Observation, Option<&'a Value>)>,
        next_offset: i64,
        checkpoint: &[u8],
        complete: bool,
        gap_code: Option<&str>,
    ) -> Result<SaveSummary> {
        self.require_writer()?;
        if gap_code.is_some_and(|code| {
            !bounded_text(code, 128)
                || !code.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        }) {
            return Err(LedgerError::InvalidInput("invalid diagnostic code".into()));
        }
        if next_offset < object.saved_offset
            || next_offset > object.byte_length
            || checkpoint.len() > 65536
            || (complete && next_offset != object.byte_length)
        {
            return Err(LedgerError::InvalidInput(
                "invalid durable checkpoint".into(),
            ));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let valid: bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM source_objects o JOIN source_roots r USING(root_id) JOIN ledger_meta m ON m.singleton=1 WHERE o.object_id=?1 AND o.object_revision=?2 AND o.content_generation=?3 AND o.saved_offset=?4 AND o.byte_length=?5 AND r.enabled=1 AND r.parser_version=?6 AND r.identity_version=?7 AND m.epoch=?8)",params![object.object_id,object.revision,object.generation,object.saved_offset,object.byte_length,object.root.spec.parser_version,object.root.spec.identity_version,object.epoch],|r|r.get(0))?;
        if !valid {
            return Err(LedgerError::StaleObservation);
        }
        let mut summary = SaveSummary::default();
        let mut highwater = 0;
        let has_controls = super::ccswitch::has_privacy_controls(&tx)?;
        for (incoming, projection) in observations {
            let mut o = incoming.clone();
            validate_observation(object, &o)?;
            o.object_id = object.object_id;
            o.content_generation = object.generation.clone();
            if o.observed_ms == 0 {
                o.observed_ms = now_ms();
            }
            // Acquisition fields do not affect canonical content identity.
            o.normalized_hash = super::claude::normalized_hash(&o)?;
            let raw = projection
                .map(|p| validate_projection(object.root.spec.parser_version, p))
                .transpose()?;
            let cc_suppressed = if has_controls && object.root.spec.identity_family == "ccswitch" {
                if let Some(p) = projection {
                    let probe = super::ccswitch::reported_identity_probe(p)?;
                    probe
                        .as_ref()
                        .map(|p| super::ingest::exact_suppressed(&tx, p))
                        .transpose()?
                        .unwrap_or(false)
                        || super::adoption::suppresses_projection(&tx, p)?
                } else {
                    false
                }
            } else {
                false
            };
            if cc_suppressed || (has_controls && suppressed(&tx, &o)?) {
                super::maintenance::add_incoming_controls(&tx, &o)?;
                summary.suppressed += 1;
                continue;
            }
            if super::ingest::insert_observation(&tx, &o)? {
                summary.inserted += 1;
            } else {
                summary.duplicates += 1;
            }
            if o.eligibility != "eligible" && o.eligibility != "identity_only" {
                summary.pending += 1;
            }
            let id:i64=tx.query_row("SELECT observation_id FROM usage_observations WHERE identity_family=?1 AND namespace_key=?2 AND candidate_key=?3 AND normalized_hash=?4",params![o.identity_family,o.namespace_key,o.candidate_key,o.normalized_hash],|r|r.get(0))?;
            highwater = highwater.max(id);
            if object.root.spec.identity_family == "ccswitch" {
                if let Some(p) = projection {
                    let member =
                        super::ccswitch::evidence_from_projection(p, &object.root.namespace)?;
                    tx.execute("UPDATE durable_snapshot_members SET observation_id=?4 WHERE snapshot_id=?1 AND row_key=?2 AND fingerprint=?3",params![object.generation,member.row_key,member.fingerprint,id])?;
                }
            }

            if let Some(raw) = raw {
                let prior:Option<(String,String)>=tx.query_row("SELECT projection_format,projection_json FROM durable_evidence WHERE observation_id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
                if let Some((format, prior)) = prior {
                    if format != object.root.spec.parser_version || prior != raw {
                        return Err(LedgerError::HashCollision);
                    }
                } else {
                    tx.execute(
                        "INSERT INTO durable_evidence VALUES(?1,?2,?3)",
                        params![id, object.root.spec.parser_version, raw],
                    )?;
                }
            }
        }
        if summary.suppressed > 0 {
            super::maintenance::finish_incoming_suppression(&tx)?;
        }
        let mut revision=tx.query_row("UPDATE source_objects SET saved_offset=?2,checkpoint_format=CASE WHEN object_key LIKE 'cc-values-v2:%' THEN 2 ELSE 1 END,checkpoint=?3,target_done=?4,object_revision=object_revision+1,state='ready',error_code=NULL,failure_count=0,retry_after_ms=NULL,audit_due_ms=?5 WHERE object_id=?1 RETURNING object_revision",params![object.object_id,next_offset,checkpoint,complete,now_ms()],|r|r.get(0))?;
        tx.execute("UPDATE durable_import_targets SET state='saved',finished_ms=?4,error_code=NULL WHERE object_id=?1 AND target_generation=?2 AND target_end<=?3 AND state IN('pending','gap')",params![object.object_id,object.generation,next_offset,now_ms()])?;
        tx.execute("UPDATE durable_imports SET saved_observation_id=MAX(saved_observation_id,?2) WHERE import_id=?1",params![object.root.import_id,highwater])?;
        if let Some(code) = gap_code {
            tx.execute("UPDATE durable_import_targets SET state='gap',error_code=?3,finished_ms=?4 WHERE object_id=?1 AND target_generation=?2 AND target_end<=?5",params![object.object_id,object.generation,code,now_ms(),next_offset])?;
            tx.execute("UPDATE source_objects SET state=CASE WHEN ?4 THEN 'error' ELSE 'ready' END,error_code=?2,retry_after_ms=CASE WHEN ?4 THEN ?3 ELSE NULL END WHERE object_id=?1",params![object.object_id,code,now_ms()+60_000,complete])?;
        }
        let mut final_checkpoint = Some(checkpoint.to_vec());
        if object.root.spec.identity_family == "ccswitch" {
            if has_controls {
                super::ccswitch::scrub_snapshot_object(&tx, object.object_id, |o| {
                    super::ingest::exact_suppressed(&tx, o)
                })?;
            }
            if complete {
                tx.execute("UPDATE source_objects SET checkpoint=X'7365616c65642d76322d636f6d706c657465' WHERE object_id=?1 AND object_key LIKE 'cc-values-v2:%'",[object.object_id])?;
            }
            (revision, final_checkpoint) = tx.query_row(
                "SELECT object_revision,checkpoint FROM source_objects WHERE object_id=?1",
                [object.object_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
        }
        finish_targets(&tx, object.root.import_id)?;
        super::collection::record_acquisition_progress(
            &tx,
            object.root.spec.report_source,
            &object.root.root_id,
        )?;
        db::bump(&tx)?;
        regression_stop("durable_before_commit");
        tx.commit()?;
        regression_stop("durable_after_commit");
        object.saved_offset = next_offset;
        object.checkpoint = final_checkpoint;
        object.revision = revision;
        object.complete = complete;
        Ok(summary)
    }

    pub fn fail_durable_object(&mut self, object: &DurableObject, code: &str) -> Result<()> {
        self.require_writer()?;
        if !bounded_text(code, 128) || !code.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(LedgerError::InvalidInput("invalid diagnostic code".into()));
        }
        self.conn.execute("UPDATE source_objects SET state='error',error_code=?2,failure_count=failure_count+1,retry_after_ms=?3 WHERE object_id=?1 AND object_revision=?4 AND content_generation=?5",params![object.object_id,code,now_ms()+60_000,object.revision,object.generation])?;
        Ok(())
    }

    /// A verified missing member of a sealed source snapshot is a historical
    /// gap, not successful import. The same generation can later fill the gap.
    pub fn mark_durable_gap(&mut self, object: &DurableObject, code: &str) -> Result<()> {
        self.require_writer()?;
        if !bounded_text(code, 128) || !code.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(LedgerError::InvalidInput("invalid diagnostic code".into()));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("UPDATE source_objects SET state='error',error_code=?2,failure_count=failure_count+1,retry_after_ms=?3 WHERE object_id=?1 AND object_revision=?4 AND content_generation=?5",params![object.object_id,code,now_ms()+60_000,object.revision,object.generation])?;
        tx.execute("UPDATE durable_import_targets SET state='gap',error_code=?3,finished_ms=?4 WHERE object_id=?1 AND target_generation=?2 AND state='pending'",params![object.object_id,object.generation,code,now_ms()])?;
        finish_targets(&tx, object.root.import_id)?;
        db::bump(&tx)?;
        tx.commit()?;
        Ok(())
    }

    pub fn mark_durable_object_missing(&mut self, object: &DurableObject) -> Result<()> {
        self.mark_durable_gap(object, "SOURCE_DISAPPEARED_BEFORE_SAVE")?;
        self.conn.execute("UPDATE source_objects SET state='missing' WHERE object_id=?1 AND object_revision=?2 AND content_generation=?3",params![object.object_id,object.revision,object.generation])?;
        Ok(())
    }

    /// Only call after a complete directory enumeration, never after a failed
    /// or truncated walk. Missing source material never removes observations.
    pub fn mark_durable_missing(
        &mut self,
        root: &DurableRoot,
        seen_object_keys: &[String],
    ) -> Result<()> {
        self.require_writer()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch("CREATE TEMP TABLE IF NOT EXISTS durable_seen_keys(object_key TEXT PRIMARY KEY); DELETE FROM durable_seen_keys;")?;
        for key in seen_object_keys {
            tx.execute("INSERT OR IGNORE INTO durable_seen_keys VALUES(?1)", [key])?;
        }
        tx.execute("UPDATE source_objects SET state='missing',error_code=CASE WHEN target_done=0 THEN 'SOURCE_DISAPPEARED_BEFORE_SAVE' ELSE NULL END WHERE root_id=?1 AND object_key NOT IN(SELECT object_key FROM durable_seen_keys)",[&root.root_id])?;
        tx.execute("UPDATE durable_import_targets SET state='gap',error_code='SOURCE_DISAPPEARED_BEFORE_SAVE',finished_ms=?2 WHERE state='pending' AND object_id IN(SELECT object_id FROM source_objects WHERE root_id=?1 AND state='missing')",params![root.root_id,now_ms()])?;
        finish_targets(&tx, root.import_id)?;
        db::bump(&tx)?;
        tx.commit()?;
        Ok(())
    }

    pub fn fail_durable_enumeration(&mut self, root: &DurableRoot, code: &str) -> Result<()> {
        self.require_writer()?;
        if !bounded_text(code, 128) || !code.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(LedgerError::InvalidInput("invalid diagnostic code".into()));
        }
        self.conn.execute("UPDATE source_roots SET enumeration_done=0,error_code=?2,failure_count=failure_count+1,retry_after_ms=?3 WHERE root_id=?1",params![root.root_id,code,now_ms()+60_000])?;
        db::bump(&self.conn)?;
        Ok(())
    }

    /// A bounded work queue alternates recently changed objects with the oldest
    /// unprocessed objects. A large initial import cannot monopolize each slice.
    pub fn next_durable_objects(
        &mut self,
        root: &DurableRoot,
        limit: usize,
    ) -> Result<Vec<DurableObject>> {
        let limit = limit.min(1024);
        if limit == 0 || !root.enabled {
            return Ok(Vec::new());
        }
        self.require_writer()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut turn: i64 = tx.query_row(
            "SELECT next_queue FROM durable_imports WHERE import_id=?1",
            [root.import_id],
            |r| r.get(0),
        )?;
        let query = |order: &str| -> Result<Vec<i64>> {
            let sql=format!("SELECT o.object_id FROM source_objects o JOIN source_roots r USING(root_id) WHERE o.root_id=?1 AND r.enabled=1 AND o.target_done=0 AND o.state<>'missing' AND (o.retry_after_ms IS NULL OR o.retry_after_ms<=?2) ORDER BY {order} LIMIT ?3");
            Ok(tx
                .prepare(&sql)?
                .query_map(params![root.root_id, now_ms(), limit as i64], |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?)
        };
        let fresh = query("o.last_observed_ms DESC,o.object_id DESC")?;
        let oldest = query("o.audit_due_ms,o.object_id")?;
        let mut ids = Vec::new();
        let mut positions = [0usize; 2];
        while ids.len() < limit {
            let lists = [&fresh, &oldest];
            let selected = turn as usize;
            while positions[selected] < lists[selected].len()
                && ids.contains(&lists[selected][positions[selected]])
            {
                positions[selected] += 1;
            }
            if positions[selected] < lists[selected].len() {
                ids.push(lists[selected][positions[selected]]);
                positions[selected] += 1;
            } else if positions[1 - selected] >= lists[1 - selected].len() {
                break;
            }
            turn = 1 - turn;
        }
        tx.execute(
            "UPDATE durable_imports SET next_queue=?2 WHERE import_id=?1",
            params![root.import_id, turn],
        )?;
        tx.commit()?;
        ids.into_iter()
            .map(|id| load_object(&self.conn, root, id))
            .collect()
    }

    pub fn status_durable_imports(&self) -> Result<Vec<ImportStatus>> {
        import_status(&self.conn)
    }
}

pub fn import_status(conn: &Connection) -> Result<Vec<ImportStatus>> {
    Ok(conn.prepare("SELECT i.import_id,i.root_id,r.report_source,i.capability_version,r.enabled,i.enumeration_done,i.completed_ms IS NOT NULL,(SELECT COUNT(*) FROM durable_import_targets t WHERE t.import_id=i.import_id),(SELECT COUNT(*) FROM durable_import_targets t WHERE t.import_id=i.import_id AND t.state='saved'),(SELECT COUNT(*) FROM durable_import_targets t WHERE t.import_id=i.import_id AND t.state='gap'),i.saved_observation_id,COALESCE((SELECT MAX(u.upper_observation_id) FROM ingest_runs u WHERE u.identity_family=r.identity_family AND u.namespace_key=r.namespace_key AND u.state='done'),0),(SELECT COUNT(*) FROM durable_import_targets t WHERE t.import_id=i.import_id AND t.state='pending'),(SELECT COUNT(*) FROM source_objects f WHERE f.root_id=r.root_id AND f.state IN('missing','error')),(SELECT COUNT(*) FROM usage_observations o JOIN source_objects f USING(object_id) WHERE f.root_id=r.root_id AND o.eligibility NOT IN('eligible','identity_only')) FROM durable_imports i JOIN source_roots r USING(root_id) ORDER BY i.import_id")?.query_map([],|r|Ok(ImportStatus {import_id:r.get(0)?,root_id:r.get(1)?,source:r.get(2)?,capability_version:r.get(3)?,enabled:r.get(4)?,enumeration_done:r.get(5)?,initial_pass_finished:r.get(6)?,target_objects:r.get(7)?,saved_objects:r.get(8)?,gap_objects:r.get(9)?,saved_observation_id:r.get(10)?,published_observation_id:r.get(11)?,remaining_objects:r.get(12)?,error_objects:r.get(13)?,pending_candidates:r.get(14)?}))?.collect::<rusqlite::Result<_>>()?)
}

fn load_object(c: &Connection, root: &DurableRoot, id: i64) -> Result<DurableObject> {
    Ok(c.query_row("SELECT o.content_generation,o.object_revision,COALESCE(o.saved_offset,0),o.checkpoint,o.byte_length,o.mtime_ns,m.epoch,o.target_done FROM source_objects o JOIN ledger_meta m ON m.singleton=1 WHERE o.object_id=?1 AND o.root_id=?2",params![id,root.root_id],|r|Ok(DurableObject {object_id:id,root:root.clone(),generation:r.get(0)?,revision:r.get(1)?,saved_offset:r.get(2)?,checkpoint:r.get(3)?,byte_length:r.get(4)?,mtime_ns:r.get(5)?,epoch:r.get(6)?,complete:r.get(7)?}))?)
}

fn validate_observation(object: &DurableObject, o: &Observation) -> Result<()> {
    let spec = &object.root.spec;
    if o.identity_family != spec.identity_family
        || o.namespace_key != object.root.namespace
        || o.parser_version != spec.parser_version
        || o.identity_version != spec.identity_version
        || o.semantics_version != spec.semantics_version
    {
        return Err(LedgerError::UnsupportedVersion);
    }
    if o.report_source != spec.report_source
        || (o.native_tool != spec.native_tool
            && (spec.identity_family != "ccswitch"
                || !matches!(
                    o.native_tool.as_str(),
                    "claude" | "codex" | "opencode" | "pi" | "unknown"
                )))
    {
        return Err(LedgerError::InvalidInput(
            "observation source differs from adapter".into(),
        ));
    }
    if !bounded_text(&o.candidate_key, 1024)
        || !bounded_text(&o.record_key, 4096)
        || o.model_key.len() > 4096
        || o.model_display.as_ref().is_some_and(|s| s.len() > 4096)
        || [
            &o.native_session_id,
            &o.native_message_id,
            &o.native_request_id,
            &o.session_key,
        ]
        .iter()
        .any(|s| s.as_ref().is_some_and(|s| s.len() > 4096))
        || o.identity_key.as_ref().is_some_and(|k| {
            k.len() != 64
                || !k
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        })
    {
        return Err(LedgerError::InvalidInput(
            "unbounded or malformed observation identity".into(),
        ));
    }
    if o.action != "upsert" || o.revision_authority.is_some() || o.revision_no.is_some() {
        return Err(LedgerError::InvalidInput(
            "adapter cannot revise published facts".into(),
        ));
    }
    Ok(())
}

pub(crate) fn suppressed(c: &Connection, o: &Observation) -> Result<bool> {
    if super::ingest::exact_suppressed(c, o)? {
        return Ok(true);
    }
    // Coarse or unknown-time evidence must not evade an explicit forgotten
    // native coverage. Retain no raw row that could resurrect that range.
    Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM suppression_rules s WHERE (?7='day' AND s.kind='coverage_family' AND s.native_tool=?1 AND s.namespace_key=?2) OR (?7='day' AND s.kind='coverage_interval' AND s.native_tool=?1 AND s.namespace_key=?2 AND (?3 IS NULL OR (?3<s.end_ms AND ?4>s.start_ms))) OR (s.kind='identity_link' AND s.identity_family=?5 AND s.namespace_key=?2 AND s.link_key=?6))",params![o.native_tool,o.namespace_key,o.day_start_ms.or(o.occurred_ms),o.day_end_ms.or(o.occurred_ms.and_then(|v|v.checked_add(1))),o.identity_family,o.native_message_id.as_ref().map(|m|identity_link(&o.identity_family,&o.namespace_key,m)),o.fact_kind],|r|r.get(0))?)
}

fn validate_projection(parser: &str, value: &Value) -> Result<String> {
    // The concrete adapters declare narrow scalar projections. Nested arbitrary
    // JSON, message content and configuration are never accepted by this API.
    let object = value
        .as_object()
        .ok_or_else(|| LedgerError::InvalidInput("usage projection must be an object".into()))?;
    let allowed: &[&str] = match parser {
        super::document_adapters::PARSER => super::document_adapters::projection_fields(),
        "ccswitch-ledger-v1" => &[
            "channel",
            "normalization_version",
            "normalization_reason",
            "is_streaming",
            "kind",
            "date",
            "app_type",
            "provider_id",
            "model",
            "request_model",
            "pricing_model",
            "request_count",
            "success_count",
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_creation_tokens",
            "total_cost_usd",
            "avg_latency_ms",
            "input_token_semantics",
            "request_id",
            "session_id",
            "data_source",
            "created_at",
            "status_code",
            "streaming",
            "latency_ms",
            "schema_version",
            "projection_version",
            "row_key",
            "fingerprint",
            "pending_reason",
        ],
        "codex-ledger-v1"
        | "kimi-ledger-v1"
        | "kimi-work-ledger-v1"
        | "qwen-ledger-v1"
        | "opencode-ledger-v1"
        | "pi-ledger-v1"
        | "omp-ledger-v1"
        | "goose-ledger-v1"
        | "openclaw-ledger-v1"
        | "copilot-ledger-v1"
        | "zcode-ledger-v1"
        | "minimax-ledger-v1"
        | "kilo-ledger-v1"
        | "hermes-ledger-v1"
        | "qoder-ledger-v1"
        | "dsh-ledger-v1" => &[
            "channel",
            "pending_reason",
            "protocol_version",
            "step_uuid",
            "turn_id",
            "source_session_id",
            "source_agent_id",
            "source_record_id",
            "source_entry_id",
            "source_parent_session",
            "upstream_finish",
            "raw_usage_hash",
            "raw_input_tokens",
            "raw_output_tokens",
            "raw_cache_read_tokens",
            "raw_cache_write_tokens",
            "raw_reasoning_tokens",
            "raw_total_tokens",
            "status",
            "source_sequence",
            "source_fork_parent",
            "source_fork_boundary",
            "payload_schema",
            "native_stop_reason",
            "source_usage_scope",
            "projection_version",
        ],
        _ => {
            return Err(LedgerError::InvalidInput(
                "adapter has no supplemental projection contract".into(),
            ))
        }
    };
    if object.iter().any(|(k, v)| {
        !allowed.contains(&k.as_str())
            || matches!(v, Value::Array(_) | Value::Object(_))
            || v.as_str().is_some_and(|s| {
                s.len()
                    > if parser == super::document_adapters::PARSER && k == "raw_usage" {
                        12_000
                    } else {
                        4096
                    }
            })
    }) {
        return Err(LedgerError::InvalidInput(
            "field is outside usage projection allowlist".into(),
        ));
    }
    if parser == super::document_adapters::PARSER
        && !object
            .get("raw_usage")
            .is_some_and(super::document_adapters::validate_raw_usage)
    {
        return Err(LedgerError::InvalidInput(
            "raw usage is outside numeric projection contract".into(),
        ));
    }
    let text = serde_json::to_string(value)
        .map_err(|_| LedgerError::InvalidInput("invalid usage projection".into()))?;
    if text.len() > 16_384 {
        return Err(LedgerError::RecordTooLarge);
    }
    Ok(text)
}
