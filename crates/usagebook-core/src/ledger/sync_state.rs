//! Single-writer durable sync state. HTTP is deliberately absent from this module.
use super::{sync_identity as identity, sync_projection::ProjectedRow, types::*, Ledger, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
#[derive(Clone, Serialize, Deserialize)]
pub struct Binding {
    pub binding_id: String,
    pub account_id: String,
    pub device_id: String,
    pub ledger_uuid: String,
    pub secret_check_hash: String,
    pub enabled: bool,
    pub status: String,
    pub history_from_ms: Option<i64>,
    pub restore_epoch: Option<String>,
    pub server_generation: String,
}
pub fn binding(c: &Connection) -> Result<Option<Binding>> {
    Ok(c.query_row("SELECT binding_id,account_id,device_id,ledger_uuid,secret_check_hash,enabled,status,history_from_ms,restore_epoch,server_generation FROM sync_bindings WHERE is_current=1",[],|r|Ok(Binding{binding_id:r.get(0)?,account_id:r.get(1)?,device_id:r.get(2)?,ledger_uuid:r.get(3)?,secret_check_hash:r.get(4)?,enabled:r.get(5)?,status:r.get(6)?,history_from_ms:r.get(7)?,restore_epoch:r.get(8)?,server_generation:r.get(9)?})).optional()?)
}
pub fn status(c: &Connection) -> Result<Value> {
    let Some(b) = binding(c)? else {
        return Ok(
            json!({"enabled":false,"state":"unbound","sources":[],"queue_count":0,"last_ack_ms":null,"control_seq":"0"}),
        );
    };
    let (ack, err): (Option<i64>, Option<String>) = c.query_row(
        "SELECT last_ack_ms,last_error_code FROM sync_bindings WHERE binding_id=?1",
        [&b.binding_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let queue:i64=c.query_row("SELECT COUNT(*) FROM sync_transactions WHERE binding_id=?1 AND state NOT IN('acked','aborted')",[&b.binding_id],|r|r.get(0))?;
    // Match next_upload's ordering: a permanently blocked head reserves the
    // device even after off/on clears the binding's transient error fields.
    let head: Option<(String, Option<String>)> = c.query_row(
        "SELECT state,last_error_code FROM sync_transactions WHERE binding_id=?1 AND state NOT IN('acked','aborted') ORDER BY rowid LIMIT 1",
        [&b.binding_id], |r| Ok((r.get(0)?, r.get(1)?)),
    ).optional()?;
    let blocked_error = head.as_ref().and_then(|(state, code)| {
        (state == "blocked").then(|| code.as_deref().unwrap_or("SYNC_TRANSACTION_BLOCKED"))
    });
    let seq: i64 = c.query_row("SELECT control_seq FROM sync_meta", [], |r| r.get(0))?;
    let mut sources = Vec::new();
    let mut q=c.prepare("SELECT source,state,reason,(SELECT COUNT(*) FROM sync_contributions c WHERE c.binding_id=s.binding_id AND c.source=s.source AND c.presence='absent'),(SELECT COUNT(*) FROM sync_coverage_gaps g WHERE g.binding_id=s.binding_id AND g.source=s.source) FROM sync_scopes s WHERE binding_id=?1 ORDER BY source")?;
    for row in q.query_map([&b.binding_id],|r|Ok(json!({"source":r.get::<_,String>(0)?,"state":r.get::<_,String>(1)?,"error_code":r.get::<_,Option<String>>(2)?,"absent_count":r.get::<_,i64>(3)?,"gap_count":r.get::<_,i64>(4)?})))?{sources.push(row?);}
    // A receipt from another source must not hide a retained export failure.
    // Binding-wide authentication/restore/storage/backoff states retain priority.
    let source_error = sources.iter().find_map(|s| s["error_code"].as_str());
    let state = if !b.enabled {
        "off"
    } else if (blocked_error.is_some() || source_error.is_some())
        && matches!(b.status.as_str(), "ready" | "sending")
    {
        "conflict"
    } else {
        &b.status
    };
    Ok(
        json!({"enabled":b.enabled,"state":state,"account_id":b.account_id,"device_id":b.device_id,"sources":sources,"queue_count":queue,"last_ack_ms":ack.map(|n|n.to_string()),"error_code":err.as_deref().or(blocked_error).or(source_error),"control_seq":seq.to_string()}),
    )
}
pub fn version(c: &Connection) -> Result<i64> {
    Ok(
        c.query_row("SELECT version FROM sync_projection_clock", [], |r| {
            r.get(0)
        })?,
    )
}
pub fn sources(c: &Connection) -> Result<Vec<String>> {
    let mut q=c.prepare("SELECT DISTINCT report_source FROM source_roots UNION SELECT DISTINCT report_source FROM query_live_facts ORDER BY 1")?;
    let rows = q
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}
pub fn source_export_ready(c: &Connection, source: &str) -> Result<bool> {
    Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM sync_scopes s JOIN sync_bindings b USING(binding_id) WHERE b.is_current=1 AND s.source=?1 AND s.state='ready' AND s.reason IS NULL)",[source],|r|r.get(0))?)
}
pub fn error(code: &str) -> LedgerError {
    LedgerError::InvalidInput(code.to_string())
}
impl Ledger {
    pub fn sync_bind(
        &mut self,
        id: &str,
        account: &str,
        device: &str,
        secret: &[u8],
        cutoff: Option<i64>,
    ) -> Result<()> {
        self.require_writer()?;
        let stamp = self.stamp()?;
        let prior:Option<(String,String)>=self.conn.query_row("SELECT binding_id,secret_check_hash FROM sync_bindings WHERE account_id=?1 AND device_id=?2",params![account,device],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        if prior.is_some_and(|(binding, hash)| binding != id || hash != identity::hash(secret)) {
            return Err(error("SYNC_RESTORE_REQUIRED"));
        }
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            "UPDATE sync_bindings SET enabled=0,is_current=0,status=CASE WHEN status='restore_required' THEN status ELSE 'off' END",
            [],
        )?;
        tx.execute("INSERT INTO sync_bindings(binding_id,account_id,device_id,ledger_uuid,secret_check_hash,enabled,is_current,status,history_from_ms,created_ms) VALUES(?1,?2,?3,?4,?5,0,1,'off',?6,?7) ON CONFLICT(account_id,device_id) DO UPDATE SET enabled=0,is_current=1,status=CASE WHEN sync_bindings.status='restore_required' THEN sync_bindings.status ELSE 'off' END",params![id,account,device,stamp.ledger_uuid,identity::hash(secret),cutoff,now_ms()])?;
        tx.commit()?;
        Ok(())
    }
    pub fn sync_set_enabled(&mut self, enabled: bool) -> Result<Value> {
        self.require_writer()?;
        let b = binding(&self.conn)?.ok_or_else(|| error("SYNC_NOT_BOUND"))?;
        if enabled && b.status == "restore_required" {
            return Err(error("SYNC_RESTORE_REQUIRED"));
        }
        let tx = self.conn.unchecked_transaction()?;
        tx.execute("UPDATE sync_meta SET control_seq=control_seq+1", [])?;
        tx.execute("UPDATE sync_bindings SET enabled=?1,status=CASE WHEN status='restore_required' THEN status ELSE ?2 END,last_error_code=CASE WHEN status='restore_required' THEN last_error_code ELSE NULL END WHERE binding_id=?3",params![enabled,if enabled{"ready"}else{"off"},b.binding_id])?;
        tx.execute("DELETE FROM sync_source_exports", [])?;
        tx.commit()?;
        status(&self.conn)
    }
    pub fn sync_record_error(&mut self, state: &str, code: &str) -> Result<()> {
        self.require_writer()?;
        self.conn.execute(
            "UPDATE sync_bindings SET status=CASE WHEN status='restore_required' THEN status ELSE ?1 END,last_error_code=CASE WHEN status='restore_required' AND ?1<>'restore_required' THEN last_error_code ELSE ?2 END WHERE is_current=1",
            params![state, code],
        )?;
        Ok(())
    }
    /// Only call after the full explicit reconciliation completed successfully.
    pub fn sync_finish_reconcile(&mut self) -> Result<()> {
        self.require_writer()?;
        self.conn.execute("UPDATE sync_bindings SET enabled=0,status='off',last_error_code=NULL WHERE is_current=1",[])?;
        Ok(())
    }
    pub fn sync_set_server(
        &mut self,
        epoch: &str,
        generation: &str,
        allow_change: bool,
    ) -> Result<()> {
        self.require_writer()?;
        let b = binding(&self.conn)?.ok_or_else(|| error("SYNC_NOT_BOUND"))?;
        if b.restore_epoch.as_deref().is_some_and(|old| old != epoch) && !allow_change {
            self.sync_record_error("restore_required", "RESTORE_EPOCH_CHANGED")?;
            return Err(error("RESTORE_EPOCH_CHANGED"));
        }
        self.conn.execute(
            "UPDATE sync_bindings SET restore_epoch=?1,server_generation=?2 WHERE binding_id=?3",
            params![epoch, generation, b.binding_id],
        )?;
        Ok(())
    }
    pub fn sync_recover_exports(&mut self) -> Result<()> {
        self.require_writer()?;
        self.conn.execute("DELETE FROM sync_source_exports", [])?;
        self.conn
            .execute("DELETE FROM sync_transactions WHERE state='staging'", [])?;
        Ok(())
    }
    /// Export failures belong to their source and remain retryable. Do not stop
    /// independent sources or overwrite the binding's authentication/restore guard.
    pub fn sync_record_source_error(&mut self, source: &str, code: &str) -> Result<()> {
        self.require_writer()?;
        let b = binding(&self.conn)?.ok_or_else(|| error("SYNC_NOT_BOUND"))?;
        self.conn.execute("INSERT INTO sync_scopes(binding_id,source,route,state,reason) VALUES(?1,?2,CASE WHEN EXISTS(SELECT 1 FROM source_roots WHERE report_source=?2 AND route='ledger') THEN 'ledger' ELSE 'live' END,'conflict',?3) ON CONFLICT(binding_id,source) DO UPDATE SET state='conflict',reason=excluded.reason",params![b.binding_id,source,code])?;
        Ok(())
    }
    pub fn sync_begin_export(&mut self, source: &str) -> Result<String> {
        self.require_writer()?;
        let b = binding(&self.conn)?.ok_or_else(|| error("SYNC_NOT_BOUND"))?;
        if !b.enabled {
            return Err(error("SYNC_DISABLED"));
        }
        if !crate::registered_sources().iter().any(|s| s.id() == source) {
            return Err(error("SOURCE_PROTOCOL_UNSUPPORTED"));
        }
        let aborted:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM sync_scopes WHERE binding_id=?1 AND source=?2 AND reason='TRANSACTION_ABORTED')",params![b.binding_id,source],|r|r.get(0))?;
        if aborted {
            return Err(error("TRANSACTION_ABORTED"));
        }
        let busy:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM sync_transactions WHERE binding_id=?1 AND source=?2 AND state NOT IN('acked','aborted'))",params![b.binding_id,source],|r|r.get(0))?;
        if busy {
            return Err(error("SYNC_SOURCE_PENDING"));
        }
        let route: &str = if self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM source_roots WHERE report_source=?1 AND route='ledger')",
            [source],
            |r| r.get::<_, bool>(0),
        )? {
            "ledger"
        } else {
            "live"
        };
        let prior: Option<String> = self
            .conn
            .query_row(
                "SELECT route FROM sync_scopes WHERE binding_id=?1 AND source=?2",
                params![b.binding_id, source],
                |r| r.get(0),
            )
            .optional()?;
        if prior.as_deref().is_some_and(|r| r != route) {
            return Err(error("SYNC_ROUTE_BRIDGE_REQUIRED"));
        }
        let export = new_epoch();
        let tx = self.conn.unchecked_transaction()?;
        tx.execute("INSERT OR IGNORE INTO sync_scopes(binding_id,source,route,state) VALUES(?1,?2,?3,'ready')",params![b.binding_id,source,route])?;
        tx.execute("INSERT INTO sync_source_exports SELECT ?1,?2,?3,(SELECT version FROM sync_projection_clock),source_epoch,(SELECT control_seq FROM sync_meta),?4,'reading' FROM sync_scopes WHERE binding_id=?1 AND source=?2",params![b.binding_id,source,export,now_ms()])?;
        tx.execute_batch("CREATE TEMP TABLE IF NOT EXISTS sync_export_facts(id TEXT PRIMARY KEY,local_key TEXT NOT NULL,basis TEXT NOT NULL,header TEXT NOT NULL,payload_hash TEXT,need_put INTEGER DEFAULT 0) WITHOUT ROWID; CREATE TEMP TABLE IF NOT EXISTS sync_export_parts(id TEXT NOT NULL,part_index INTEGER NOT NULL,entry TEXT NOT NULL,PRIMARY KEY(id,part_index)) WITHOUT ROWID; DELETE FROM sync_export_facts;DELETE FROM sync_export_parts;")?;
        tx.commit()?;
        Ok(export)
    }
    pub fn sync_accept_page(&mut self, export: &str, page: &[ProjectedRow]) -> Result<()> {
        self.require_writer()?;
        let active:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM sync_source_exports e JOIN sync_bindings b USING(binding_id) WHERE export_id=?1 AND b.enabled=1 AND e.control_seq=(SELECT control_seq FROM sync_meta))",[export],|r|r.get(0))?;
        if !active {
            return Err(error("SYNC_EXPORT_CANCELLED"));
        }
        let tx = self.conn.unchecked_transaction()?;
        for row in page {
            if let Some(local) = &row.local_key {
                tx.execute(
                    "INSERT INTO sync_export_facts(id,local_key,basis,header) VALUES(?1,?2,?3,?4)",
                    params![row.id, local, row.basis, row.entry.to_string()],
                )?;
            } else if let Some(hash) = &row.payload_hash {
                tx.execute(
                    "UPDATE sync_export_facts SET payload_hash=?1 WHERE id=?2",
                    params![hash, row.id],
                )?;
            } else {
                tx.execute(
                    "INSERT INTO sync_export_parts VALUES(?1,?2,?3)",
                    params![
                        row.id,
                        row.entry["part_index"].as_i64(),
                        row.entry.to_string()
                    ],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub fn sync_cancel_export(&mut self, export: &str, code: Option<&str>) -> Result<()> {
        self.require_writer()?;
        if let Some(code) = code {
            self.conn.execute("UPDATE sync_scopes SET state='conflict',reason=?1 WHERE (binding_id,source) IN(SELECT binding_id,source FROM sync_source_exports WHERE export_id=?2)",params![code,export])?;
        }
        self.conn.execute(
            "DELETE FROM sync_source_exports WHERE export_id=?1",
            [export],
        )?;
        Ok(())
    }
    pub fn sync_finish_export(&mut self, export: &str) -> Result<usize> {
        self.require_writer()?;
        let b = binding(&self.conn)?.ok_or_else(|| error("SYNC_NOT_BOUND"))?;
        let row:Option<(String,String,i64,Option<i64>)>=self.conn.query_row("SELECT e.source,s.route,e.projection_version,s.history_from_ms FROM sync_source_exports e JOIN sync_scopes s USING(binding_id,source) JOIN sync_bindings b USING(binding_id) WHERE export_id=?1 AND b.enabled=1 AND e.control_seq=(SELECT control_seq FROM sync_meta) AND e.source_epoch=s.source_epoch",[export],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        let (source, route, version, scope_cutoff) =
            row.ok_or_else(|| error("SYNC_EXPORT_CANCELLED"))?;
        let cutoff = b.history_from_ms.into_iter().chain(scope_cutoff).max();
        // Ignore pre-cutoff facts for new upload, but retain their identity baseline.
        let missing:i64=self.conn.query_row("SELECT COUNT(*) FROM sync_live_baseline b WHERE b.binding_id=?1 AND b.source=?2 AND NOT EXISTS(SELECT 1 FROM sync_export_facts f WHERE f.local_key=b.local_key)",params![b.binding_id,source],|r|r.get(0))?;
        let new:i64=self.conn.query_row("SELECT COUNT(*) FROM sync_export_facts f WHERE NOT EXISTS(SELECT 1 FROM sync_live_baseline b WHERE b.binding_id=?1 AND b.source=?2 AND b.local_key=f.local_key)",params![b.binding_id,source],|r|r.get(0))?;
        let changed:i64=self.conn.query_row("SELECT COUNT(*) FROM sync_export_facts f JOIN sync_live_baseline b ON b.local_key=f.local_key WHERE b.binding_id=?1 AND b.source=?2 AND b.payload_hash<>f.payload_hash",params![b.binding_id,source],|r|r.get(0))?;
        if route == "live" && (changed > 0 || missing > 0 && new > 0) {
            self.conn.execute("UPDATE sync_scopes SET state='conflict',reason=?3 WHERE binding_id=?1 AND source=?2",params![b.binding_id,source,if changed>0{"VALUE_CONFLICT"}else{"LIVE_IDENTITY_CONFLICT"}])?;
            self.sync_cancel_export(export, None)?;
            return Ok(0);
        }
        let mut q=self.conn.prepare("SELECT f.id,f.header,f.payload_hash,c.header_json,c.payload_hash,c.confirmed_revision FROM sync_export_facts f LEFT JOIN sync_contributions c ON c.binding_id=?1 AND c.contribution_id=f.id ORDER BY f.id")?;
        let mut rows = q.query([&b.binding_id])?;
        let mut changed_any = false;
        while let Some(r) = rows.next()? {
            let id: String = r.get(0)?;
            let new_header: String = r.get(1)?;
            let new_hash: String = r.get(2)?;
            let old_header: Option<String> = r.get(3)?;
            let old_hash: Option<String> = r.get(4)?;
            let mut header: Value =
                serde_json::from_str(&new_header).map_err(|_| LedgerError::CorruptLedger)?;
            let at = header["time"]["at_ms"]
                .as_str()
                .or(header["time"]["start_ms"].as_str())
                .and_then(|v| v.parse::<i64>().ok())
                .ok_or_else(|| error("SYNC_TIME_INVALID"))?;
            if cutoff.is_some_and(|t| at < t) {
                continue;
            }
            if old_hash.as_deref() == Some(&new_hash) {
                continue;
            }
            if let Some(old) = old_header {
                let mut old: Value =
                    serde_json::from_str(&old).map_err(|_| LedgerError::CorruptLedger)?;
                for name in [
                    "type",
                    "id",
                    "expected_revision",
                    "allocation_metrics",
                    "part_count",
                ] {
                    old.as_object_mut().unwrap().remove(name);
                }
                let mut compare = header.clone();
                for name in [
                    "type",
                    "id",
                    "expected_revision",
                    "allocation_metrics",
                    "part_count",
                ] {
                    compare.as_object_mut().unwrap().remove(name);
                }
                if old != compare {
                    self.conn.execute("UPDATE sync_scopes SET state='conflict',reason='VALUE_CONFLICT' WHERE binding_id=?1 AND source=?2",params![b.binding_id,source])?;
                    changed_any = true;
                    continue;
                }
            }
            header["expected_revision"] =
                json!(r.get::<_, Option<String>>(5)?.unwrap_or_else(|| "0".into()));
            self.conn.execute(
                "UPDATE sync_export_facts SET header=?1,need_put=1 WHERE id=?2",
                params![header.to_string(), id],
            )?;
        }
        drop(rows);
        drop(q);
        let mut ids = self
            .conn
            .prepare("SELECT id FROM sync_export_facts WHERE need_put=1 ORDER BY id")?;
        let mut cursor = ids.query([])?;
        let mut batch = Vec::new();
        let mut parts = 0usize;
        let mut count = 0;
        // The first independently committed batch must carry the complete,
        // already validated source baseline. Otherwise a crash between batches
        // leaves uploadable history without the missing+new identity guard.
        let mut baseline_sealed = route != "live";
        while let Some(row) = cursor.next()? {
            let id: String = row.get(0)?;
            let n: i64 = self.conn.query_row(
                "SELECT json_extract(header,'$.part_count') FROM sync_export_facts WHERE id=?1",
                [&id],
                |r| r.get(0),
            )?;
            if !batch.is_empty() && (parts + n as usize > 100000 || batch.len() >= 512) {
                count +=
                    seal_export_batch(&self.conn, &b, &source, version, &batch, !baseline_sealed)?;
                baseline_sealed = true;
                batch.clear();
                parts = 0;
            }
            batch.push(id);
            parts += n as usize;
        }
        if !batch.is_empty() {
            count += seal_export_batch(&self.conn, &b, &source, version, &batch, !baseline_sealed)?;
        }
        drop(cursor);
        drop(ids);
        let tx = self.conn.unchecked_transaction()?;
        tx.execute("UPDATE sync_contributions SET presence='absent' WHERE binding_id=?1 AND source=?2 AND presence='present' AND NOT EXISTS(SELECT 1 FROM sync_export_facts f WHERE f.id=contribution_id)",params![b.binding_id,source])?;
        tx.execute("UPDATE sync_contributions SET presence='present' WHERE binding_id=?1 AND source=?2 AND presence='absent' AND EXISTS(SELECT 1 FROM sync_export_facts f WHERE f.id=contribution_id)",params![b.binding_id,source])?;
        if route == "live" {
            tx.execute("INSERT OR IGNORE INTO sync_live_baseline SELECT ?1,?2,local_key,payload_hash,id FROM sync_export_facts WHERE payload_hash IS NOT NULL",params![b.binding_id,source])?;
        }
        if !changed_any {
            tx.execute("UPDATE sync_scopes SET state='ready',reason=NULL WHERE binding_id=?1 AND source=?2",params![b.binding_id,source])?;
        }
        tx.execute(
            "DELETE FROM sync_source_exports WHERE export_id=?1",
            [export],
        )?;
        tx.commit()?;
        Ok(count)
    }
    pub fn sync_apply_receipt(&mut self, txid: &str, receipt: &Value) -> Result<()> {
        self.require_writer()?;
        let tx = self.conn.unchecked_transaction()?;
        let (binding, hash, state): (String, String, String) = tx.query_row(
            "SELECT binding_id,manifest_hash,state FROM sync_transactions WHERE transaction_id=?1",
            [txid],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if state == "acked" {
            return Ok(());
        }
        if receipt["transaction_id"] != txid
            || receipt["accepted"] != true
            || receipt["manifest_hash"] != hash
        {
            return Err(error("SYNC_RECEIPT_INVALID"));
        }
        let mut q = tx
            .prepare("SELECT body FROM sync_chunks WHERE transaction_id=?1 ORDER BY chunk_index")?;
        for row in q.query_map([txid], |r| r.get::<_, Vec<u8>>(0))? {
            let chunk: Value =
                serde_json::from_slice(&row?).map_err(|_| LedgerError::CorruptLedger)?;
            for entry in chunk["entries"]
                .as_array()
                .ok_or(LedgerError::CorruptLedger)?
            {
                if entry["type"] == "put" {
                    let id = entry["id"].as_str().ok_or(LedgerError::CorruptLedger)?;
                    let rev = entry["expected_revision"]
                        .as_str()
                        .and_then(|s| s.parse::<i64>().ok())
                        .and_then(|r| r.checked_add(1))
                        .ok_or(LedgerError::NumericOverflow)?;
                    let hash:String=tx.query_row("SELECT payload_hash FROM sync_transaction_items WHERE transaction_id=?1 AND contribution_id=?2",params![txid,id],|r|r.get(0))?;
                    tx.execute("UPDATE sync_contributions SET header_json=?1,payload_hash=?2,confirmed_hash=?2,confirmed_revision=?3,part_count=?4,presence='present' WHERE binding_id=?5 AND contribution_id=?6",params![entry.to_string(),hash,rev.to_string(),entry["part_count"].as_i64(),binding,id])?;
                } else if entry["type"] == "retire" {
                    tx.execute("UPDATE sync_contributions SET presence='retired' WHERE binding_id=?1 AND contribution_id=?2",params![binding,entry["id"].as_str()])?;
                }
            }
        }
        drop(q);
        tx.execute("UPDATE sync_transactions SET state='acked',receipt_json=?1,acked_ms=?2,last_error_code=NULL WHERE transaction_id=?3",params![receipt.to_string(),now_ms(),txid])?;
        tx.execute(
            "DELETE FROM sync_reservations WHERE transaction_id=?1",
            [txid],
        )?;
        tx.execute(
            "UPDATE sync_transition_groups SET state='acked' WHERE transaction_id=?1",
            [txid],
        )?;
        tx.execute("UPDATE sync_bindings SET last_ack_ms=?1,server_generation=?2,status=CASE WHEN status='restore_required' THEN status WHEN enabled=1 THEN 'ready' ELSE 'off' END,last_error_code=CASE WHEN status='restore_required' THEN last_error_code ELSE NULL END WHERE binding_id=?3",params![now_ms(),receipt["change_generation"].as_str(),binding])?;
        tx.commit()?;
        Ok(())
    }
    pub fn sync_attempt_failed(
        &mut self,
        txid: &str,
        code: &str,
        retry_ms: Option<i64>,
    ) -> Result<()> {
        self.require_writer()?;
        self.conn.execute("UPDATE sync_transactions SET state=?1,last_error_code=?2,retry_count=retry_count+1,next_attempt_ms=?3 WHERE transaction_id=?4 AND state<>'acked'",params![if retry_ms.is_some(){"commit_unknown"}else{"blocked"},code,retry_ms.unwrap_or(0),txid])?;
        self.sync_record_error(
            if code == "UNAUTHORIZED" {
                "reauth"
            } else if retry_ms.is_some() {
                "backoff"
            } else {
                "conflict"
            },
            code,
        )
    }
    pub fn sync_continue_source(&mut self, source: &str, cutoff: i64) -> Result<()> {
        self.require_writer()?;
        let b = binding(&self.conn)?.ok_or_else(|| error("SYNC_NOT_BOUND"))?;
        let pending:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM sync_transactions WHERE binding_id=?1 AND source=?2 AND state NOT IN('acked','aborted'))",params![b.binding_id,source],|r|r.get(0))?;
        if pending {
            return Err(error("SYNC_UNRESOLVED_TRANSACTION"));
        }
        let tx = self.conn.unchecked_transaction()?;
        if tx.execute("UPDATE sync_scopes SET history_from_ms=?1,source_epoch=source_epoch+1,state='continued',reason=NULL WHERE binding_id=?2 AND source=?3",params![cutoff,b.binding_id,source])?==0{return Err(error("SYNC_SOURCE_NOT_FOUND"));}
        tx.execute(
            "DELETE FROM sync_live_baseline WHERE binding_id=?1 AND source=?2",
            params![b.binding_id, source],
        )?;
        // Old identities remain. The next complete export accepts the new baseline,
        // while the persisted cutoff excludes all pre-repair facts from new upload.
        tx.execute("INSERT INTO sync_coverage_gaps VALUES(?1,?2,?3,NULL,?4,'explicit_continue_from_now',?5)",params![new_epoch(),b.binding_id,source,cutoff,now_ms()])?;
        tx.commit()?;
        Ok(())
    }
}
/// Commit the full live identity guard together with the first sendable batch.
/// Later batches remain independent so a large export need not retain all of
/// its transport bytes in one write transaction. The final export transaction
/// still accepts baselines for snapshots that do not produce any upload.
fn seal_export_batch(
    c: &Connection,
    b: &Binding,
    source: &str,
    version: i64,
    ids: &[String],
    preserve_live_baseline: bool,
) -> Result<usize> {
    if !preserve_live_baseline {
        return seal_batch(c, b, source, version, ids);
    }
    c.execute_batch("SAVEPOINT sync_export_baseline;")?;
    let result = (|| {
        c.execute("INSERT OR IGNORE INTO sync_live_baseline SELECT ?1,?2,local_key,payload_hash,id FROM sync_export_facts WHERE payload_hash IS NOT NULL",params![b.binding_id,source])?;
        seal_batch(c, b, source, version, ids)
    })();
    match result {
        Ok(count) => {
            c.execute_batch("RELEASE sync_export_baseline;")?;
            Ok(count)
        }
        Err(error) => {
            let _ =
                c.execute_batch("ROLLBACK TO sync_export_baseline; RELEASE sync_export_baseline;");
            Err(error)
        }
    }
}
struct ChunkSink<'a> {
    c: &'a Connection,
    txid: &'a str,
    entries: Vec<Value>,
    bytes: usize,
    count: usize,
    digest: Sha256,
}
impl<'a> ChunkSink<'a> {
    fn new(c: &'a Connection, txid: &'a str) -> Self {
        Self {
            c,
            txid,
            entries: Vec::new(),
            bytes: 14,
            count: 0,
            digest: Sha256::new(),
        }
    }
    fn push(&mut self, entry: Value) -> Result<()> {
        let bytes = entry.to_string().len() + 1;
        if bytes + 14 > 1048576 {
            return Err(error("SYNC_ENTRY_TOO_LARGE"));
        }
        if self.entries.len() >= 512 || self.bytes + bytes > 1048576 {
            self.flush()?;
        }
        self.entries.push(entry);
        self.bytes += bytes;
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        if self.count >= 1024 {
            return Err(error("SYNC_TRANSACTION_TOO_LARGE"));
        }
        let body = serde_json::to_vec(&json!({"entries":self.entries}))
            .map_err(|_| LedgerError::CorruptLedger)?;
        let hash = Sha256::digest(&body);
        self.digest.update(hash);
        self.c.execute(
            "INSERT INTO sync_chunks VALUES(?1,?2,?3,?4,0)",
            params![self.txid, self.count as i64, body, format!("{hash:x}")],
        )?;
        self.entries.clear();
        self.bytes = 14;
        self.count += 1;
        Ok(())
    }
}
/// Caller fills the connection-local staging tables from a complete projection.
/// SAVEPOINT supports both ordinary exports and a proven adoption's final txn.
pub(crate) fn seal_batch(
    c: &Connection,
    b: &Binding,
    source: &str,
    version: i64,
    ids: &[String],
) -> Result<usize> {
    seal_group(c, b, source, version, ids, &[], "put").map(|_| ids.len())
}
pub(crate) fn seal_group(
    c: &Connection,
    b: &Binding,
    source: &str,
    version: i64,
    ids: &[String],
    retire: &[(String, String)],
    kind: &str,
) -> Result<String> {
    if ids.len() + retire.len() > 100001 {
        return Err(error("SYNC_TRANSACTION_TOO_LARGE"));
    }
    c.execute_batch("SAVEPOINT sync_seal;")?;
    let result = (|| -> Result<String> {
        let txid = new_epoch();
        let mut sink = ChunkSink::new(c, &txid);
        let mut parts = 0usize;
        c.execute("INSERT INTO sync_transactions(transaction_id,binding_id,source,kind,state,projection_version,created_ms) VALUES(?1,?2,?3,?6,'staging',?4,?5)",params![txid,b.binding_id,source,version,now_ms(),kind])?;
        for (id, revision) in retire {
            c.execute(
                "INSERT INTO sync_transaction_items VALUES(?1,?2,'retire',?3,NULL)",
                params![txid, id, revision],
            )?;
            c.execute(
                "INSERT INTO sync_reservations VALUES(?1,?2,?3)",
                params![b.binding_id, id, txid],
            )?;
            sink.push(json!({"type":"retire","id":id,"expected_revision":revision}))?;
        }
        for id in ids {
            let (local, basis, header, hash): (String, String, String, String) = c.query_row(
                "SELECT local_key,basis,header,payload_hash FROM sync_export_facts WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
            let h: Value = serde_json::from_str(&header).map_err(|_| LedgerError::CorruptLedger)?;
            c.execute("INSERT OR IGNORE INTO sync_contributions VALUES(?1,?2,?3,?4,?5,?6,'0',NULL,'present',NULL,?7)",params![b.binding_id,id,source,hash,header,h["part_count"].as_i64(),version])?;
            c.execute(
                "INSERT INTO sync_transaction_items VALUES(?1,?2,'put',?3,?4)",
                params![txid, id, h["expected_revision"].as_str(), hash],
            )?;
            c.execute(
                "INSERT INTO sync_reservations VALUES(?1,?2,?3)",
                params![b.binding_id, id, txid],
            )?;
            let basis_value: Value =
                serde_json::from_str(&basis).map_err(|_| LedgerError::CorruptLedger)?;
            c.execute(
                "INSERT OR IGNORE INTO sync_lineages VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    b.binding_id,
                    local,
                    id,
                    source,
                    basis_value["domain"].as_str(),
                    basis
                ],
            )?;
            sink.push(h)?;
            let mut q =
                c.prepare("SELECT entry FROM sync_export_parts WHERE id=?1 ORDER BY part_index")?;
            for row in q.query_map([id], |r| r.get::<_, String>(0))? {
                let part: Value =
                    serde_json::from_str(&row?).map_err(|_| LedgerError::CorruptLedger)?;
                c.execute(
                    "INSERT OR IGNORE INTO sync_parts VALUES(?1,?2,?3,?4,?5,?6)",
                    params![
                        b.binding_id,
                        id,
                        hash,
                        part["part_index"].as_i64(),
                        part["at_ms"].as_str().and_then(|s| s.parse::<i64>().ok()),
                        part["metrics"].to_string()
                    ],
                )?;
                sink.push(part)?;
                parts += 1;
                if parts > 100000 {
                    return Err(error("SYNC_TRANSACTION_TOO_LARGE"));
                }
            }
        }
        sink.flush()?;
        let manifest=json!({"protocol_version":1,"transaction_id":txid,"chunk_count":sink.count,"operation_count":ids.len()+retire.len(),"part_count":parts,"content_hash":format!("{:x}",sink.digest.finalize())}).to_string();
        c.execute("UPDATE sync_transactions SET state='queued',manifest_json=?1,manifest_hash=?2 WHERE transaction_id=?3",params![manifest,identity::hash(manifest.as_bytes()),txid])?;
        Ok(txid)
    })();
    match result {
        Ok(n) => {
            c.execute_batch("RELEASE sync_seal;")?;
            Ok(n)
        }
        Err(e) => {
            let _ = c.execute_batch("ROLLBACK TO sync_seal; RELEASE sync_seal;");
            Err(e)
        }
    }
}
#[derive(Clone)]
pub struct Upload {
    pub transaction_id: String,
    pub manifest: String,
    pub retry_count: i64,
}
pub fn next_upload(c: &Connection) -> Result<Option<Upload>> {
    // A device has one remote open transaction. Backoff or an unresolved blocked
    // first group must not let a later source/group open a second transaction.
    Ok(c.query_row("SELECT transaction_id,manifest_json,retry_count FROM (SELECT t.* FROM sync_transactions t JOIN sync_bindings b USING(binding_id) WHERE b.is_current=1 AND b.enabled=1 AND b.status NOT IN('reauth','restore_required') AND t.state NOT IN('acked','aborted') ORDER BY t.rowid LIMIT 1) oldest WHERE state IN('queued','uploading','commit_unknown') AND next_attempt_ms<=?1",[now_ms()],|r|Ok(Upload{transaction_id:r.get(0)?,manifest:r.get(1)?,retry_count:r.get(2)?})).optional()?)
}
pub fn chunk(c: &Connection, txid: &str, index: i64) -> Result<Vec<u8>> {
    Ok(c.query_row(
        "SELECT body FROM sync_chunks WHERE transaction_id=?1 AND chunk_index=?2",
        params![txid, index],
        |r| r.get(0),
    )?)
}
impl Ledger {
    pub fn sync_abort_confirmed(&mut self, id: &str) -> Result<()> {
        self.require_writer()?;
        let tx = self.conn.unchecked_transaction()?;
        tx.execute("UPDATE sync_transactions SET state='aborted',last_error_code='TRANSACTION_ABORTED' WHERE transaction_id=?1 AND state<>'acked'",[id])?;
        tx.execute(
            "DELETE FROM sync_reservations WHERE transaction_id=?1",
            [id],
        )?;
        tx.execute("UPDATE sync_scopes SET state='conflict',reason='TRANSACTION_ABORTED' WHERE (binding_id,source) IN(SELECT binding_id,source FROM sync_transactions WHERE transaction_id=?1)",[id])?;
        tx.execute("UPDATE sync_transition_groups SET state='cancelled' WHERE transaction_id=?1 AND state<>'acked'",[id])?;
        tx.commit()?;
        Ok(())
    }
    /// Requeue retained CONFIRMED bytes only after a stable remote inventory proved
    /// this ID absent. No local usage observation is created by cloud repair.
    pub fn sync_requeue_missing(&mut self, id: &str) -> Result<String> {
        self.require_writer()?;
        let b = binding(&self.conn)?.ok_or_else(|| error("SYNC_NOT_BOUND"))?;
        let (source,header,hash):(String,String,String)=self.conn.query_row("SELECT source,header_json,confirmed_hash FROM sync_contributions WHERE binding_id=?1 AND contribution_id=?2 AND confirmed_revision<>'0' AND presence<>'retired'",params![b.binding_id,id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
        let (local,basis):(String,String)=self.conn.query_row("SELECT local_key,basis_json FROM sync_lineages WHERE binding_id=?1 AND contribution_id=?2 LIMIT 1",params![b.binding_id,id],|r|Ok((r.get(0)?,r.get(1)?)))?;
        self.conn.execute_batch("CREATE TEMP TABLE IF NOT EXISTS sync_export_facts(id TEXT PRIMARY KEY,local_key TEXT NOT NULL,basis TEXT NOT NULL,header TEXT NOT NULL,payload_hash TEXT,need_put INTEGER DEFAULT 0) WITHOUT ROWID; CREATE TEMP TABLE IF NOT EXISTS sync_export_parts(id TEXT NOT NULL,part_index INTEGER NOT NULL,entry TEXT NOT NULL,PRIMARY KEY(id,part_index)) WITHOUT ROWID; DELETE FROM sync_export_facts;DELETE FROM sync_export_parts;")?;
        let mut h: Value = serde_json::from_str(&header).map_err(|_| LedgerError::CorruptLedger)?;
        h["expected_revision"] = json!("0");
        self.conn.execute(
            "INSERT INTO sync_export_facts VALUES(?1,?2,?3,?4,?5,1)",
            params![id, local, basis, h.to_string(), hash],
        )?;
        let mut q=self.conn.prepare("SELECT part_index,at_ms,metrics_json FROM sync_parts WHERE binding_id=?1 AND contribution_id=?2 AND projection_hash=?3 ORDER BY part_index")?;
        let mut count = 0;
        for row in q.query_map(params![b.binding_id, id, hash], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
            ))
        })? {
            let (index, at, metrics) = row?;
            let metrics: Value =
                serde_json::from_str(&metrics).map_err(|_| LedgerError::CorruptLedger)?;
            self.conn.execute("INSERT INTO sync_export_parts VALUES(?1,?2,?3)",params![id,index,json!({"type":"part","id":id,"part_index":index,"at_ms":at.to_string(),"metrics":metrics}).to_string()])?;
            count += 1;
        }
        if h["part_count"].as_i64() != Some(count) {
            return Err(error("RECONCILE_PARTS_MISSING"));
        }
        drop(q);
        seal_batch(
            &self.conn,
            &b,
            &source,
            version(&self.conn)?,
            &[id.to_owned()],
        )?;
        Ok(self.conn.query_row("SELECT transaction_id FROM sync_reservations WHERE binding_id=?1 AND contribution_id=?2",params![b.binding_id,id],|r|r.get(0))?)
    }
}
impl Ledger {
    pub fn sync_cleanup(&mut self) -> Result<()> {
        self.require_writer()?;
        let tx = self.conn.unchecked_transaction()?;
        // Receipt and transition evidence are permanent. Only old acknowledged
        // transport bytes are disposable; unconfirmed work is never aged out.
        tx.execute("DELETE FROM sync_chunks WHERE transaction_id IN(SELECT transaction_id FROM sync_transactions WHERE state='acked' AND acked_ms<?1 AND transaction_id IN(SELECT transaction_id FROM sync_chunks) LIMIT 100)",[now_ms()-7*86400000])?;
        tx.execute("DELETE FROM sync_transaction_items WHERE transaction_id IN(SELECT transaction_id FROM sync_transactions WHERE state='acked' AND kind IN('put','allocation') AND acked_ms<?1 AND NOT EXISTS(SELECT 1 FROM sync_chunks WHERE transaction_id=sync_transactions.transaction_id) LIMIT 100)",[now_ms()-7*86400000])?;
        tx.execute("DELETE FROM sync_parts WHERE (binding_id,contribution_id,projection_hash,part_index) IN(SELECT p.binding_id,p.contribution_id,p.projection_hash,p.part_index FROM sync_parts p JOIN sync_contributions c USING(binding_id,contribution_id) WHERE p.projection_hash<>c.payload_hash AND NOT EXISTS(SELECT 1 FROM sync_transaction_items i JOIN sync_transactions t USING(transaction_id) WHERE t.binding_id=p.binding_id AND i.contribution_id=p.contribution_id AND i.payload_hash=p.projection_hash AND (t.state NOT IN('acked','aborted') OR t.kind IN('adoption','route_bridge'))) LIMIT 512)",[])?;
        tx.commit()?;
        Ok(())
    }
}

/// A source continuation is explicit permission to leave its older cloud history
/// unchanged. It is never permission to infer a source for an unknown remote ID.
pub fn reconcile_continued_ids(c: &Connection) -> Result<i64> {
    c.execute_batch("CREATE TEMP TABLE IF NOT EXISTS sync_restore_ignored(id TEXT PRIMARY KEY) WITHOUT ROWID; DELETE FROM sync_restore_ignored;")?;
    c.execute("INSERT INTO sync_restore_ignored SELECT contribution_id FROM sync_contributions c JOIN sync_scopes s USING(binding_id,source) JOIN sync_bindings b USING(binding_id) WHERE b.is_current=1 AND s.history_from_ms IS NOT NULL AND CAST(COALESCE(json_extract(c.header_json,'$.time.at_ms'),json_extract(c.header_json,'$.time.start_ms')) AS INTEGER)<s.history_from_ms",[])?;
    Ok(
        c.query_row("SELECT count(*) FROM sync_restore_ignored", [], |r| {
            r.get(0)
        })?,
    )
}
pub fn transition_was_continued(c: &Connection, group: &str) -> Result<bool> {
    Ok(c.query_row("SELECT NOT EXISTS(SELECT 1 FROM sync_transition_groups g JOIN sync_transaction_items i USING(transaction_id) WHERE g.transition_id=?1 AND NOT EXISTS(SELECT 1 FROM sync_restore_ignored ignored WHERE ignored.id=i.contribution_id))",[group],|r|r.get(0))?)
}

#[derive(Clone, Serialize)]
pub struct RestoreTransition {
    pub transition_id: String,
    pub parent_id: Option<String>,
    pub retire: Vec<(String, String)>,
}
/// The inventory table is populated by an authenticated stable-generation repair
/// read. Only retained, exact transition evidence can explain an old active ID.
pub fn restore_transitions(c: &Connection) -> Result<Vec<RestoreTransition>> {
    let Some(b) = binding(c)? else {
        return Ok(Vec::new());
    };
    let mut q=c.prepare("SELECT transition_id,transaction_id,evidence_json FROM sync_transition_groups WHERE binding_id=?1 AND state='acked' ORDER BY rowid")?;
    let mut plans = Vec::new();
    let mut covered = std::collections::BTreeSet::new();
    for row in q.query_map([&b.binding_id], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
        ))
    })? {
        let (group, txid, evidence) = row?;
        let e: Value = serde_json::from_str(&evidence).map_err(|_| LedgerError::CorruptLedger)?;
        if e["version"] != 1 || e["kind"] != "legacy-adoption-v1" {
            continue;
        }
        let Some(parents) = e["parent_ids"].as_array().filter(|a| a.len() <= 1) else {
            continue;
        };
        let parent = parents.first().and_then(Value::as_str);
        if parents.len() == 1 && parent.is_none() {
            continue;
        }
        let put_count:i64=c.query_row("SELECT count(*) FROM sync_transaction_items WHERE transaction_id=?1 AND operation='put'",[&txid],|r|r.get(0))?;
        if put_count as usize != parents.len() {
            continue;
        }
        if let Some(parent) = parent {
            let parent_hash:Option<String>=c.query_row("SELECT confirmed_hash FROM sync_contributions WHERE binding_id=?1 AND contribution_id=?2 AND confirmed_revision<>'0' AND presence<>'retired'",params![b.binding_id,parent],|r|r.get(0)).optional()?;
            let Some(parent_hash) = parent_hash else {
                continue;
            };
            let remote: Option<(String, bool)> = c
                .query_row(
                    "SELECT payload_hash,active FROM sync_inventory WHERE id=?1",
                    [parent],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if remote
                .as_ref()
                .is_some_and(|(hash, active)| hash != &parent_hash || !*active)
            {
                continue;
            }
            let proved:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM sync_transaction_items WHERE transaction_id=?1 AND operation='put' AND contribution_id=?2)",params![txid,parent],|r|r.get(0))?;
            if !proved {
                continue;
            }
        }
        let Some(items) = e["retire"].as_array() else {
            continue;
        };
        let mut retire = Vec::new();
        let mut valid = true;
        let count:i64=c.query_row("SELECT count(*) FROM sync_transaction_items WHERE transaction_id=?1 AND operation='retire'",[&txid],|r|r.get(0))?;
        if count as usize != items.len() {
            continue;
        }
        for item in items {
            let Some(id) = item["id"].as_str() else {
                valid = false;
                break;
            };
            let Some(expected) = item["expected_revision"].as_str() else {
                valid = false;
                break;
            };
            let proved:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM sync_transaction_items WHERE transaction_id=?1 AND contribution_id=?2 AND operation='retire' AND expected_revision=?3)",params![txid,id,expected],|r|r.get(0))?;
            if !proved {
                valid = false;
                break;
            }
            let remote: Option<(String, String, bool)> = c
                .query_row(
                    "SELECT revision,payload_hash,active FROM sync_inventory WHERE id=?1",
                    [id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            if let Some((revision, hash, true)) = remote {
                let old:Option<String>=c.query_row("SELECT confirmed_hash FROM sync_contributions WHERE binding_id=?1 AND contribution_id=?2",params![b.binding_id,id],|r|r.get(0)).optional()?;
                if old.as_deref() != Some(&hash) {
                    valid = false;
                    break;
                }
                retire.push((id.to_owned(), revision));
            }
        }
        if valid && !retire.is_empty() && !retire.iter().any(|(id, _)| covered.contains(id)) {
            covered.extend(retire.iter().map(|(id, _)| id.clone()));
            plans.push(RestoreTransition {
                transition_id: group,
                parent_id: parent.map(str::to_owned),
                retire,
            });
        }
    }
    Ok(plans)
}
impl Ledger {
    pub fn sync_requeue_transition(&mut self, plan: &RestoreTransition) -> Result<String> {
        self.require_writer()?;
        let b = binding(&self.conn)?.ok_or_else(|| error("SYNC_NOT_BOUND"))?;
        let tx = self.conn.unchecked_transaction()?;
        if let Some(parent) = &plan.parent_id {
            prepare_retained_projection(&tx, &b, parent)?;
            let revision: Option<String> = tx
                .query_row(
                    "SELECT revision FROM sync_inventory WHERE id=?1",
                    [parent],
                    |r| r.get(0),
                )
                .optional()?;
            tx.execute("UPDATE sync_export_facts SET header=json_set(header,'$.expected_revision',?1) WHERE id=?2",params![revision.unwrap_or_else(||"0".into()),parent])?;
        }
        let (source, evidence): (String, String) = tx.query_row(
            "SELECT source,evidence_json FROM sync_transition_groups WHERE transition_id=?1",
            [&plan.transition_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let mut evidence: Value =
            serde_json::from_str(&evidence).map_err(|_| LedgerError::CorruptLedger)?;
        evidence["retire"] = json!(plan
            .retire
            .iter()
            .map(|(id, revision)| json!({"id":id,"expected_revision":revision}))
            .collect::<Vec<_>>());
        let parents: Vec<String> = plan.parent_id.iter().cloned().collect();
        let txid = seal_group(
            &tx,
            &b,
            &source,
            version(&tx)?,
            &parents,
            &plan.retire,
            "adoption",
        )?;
        tx.execute("INSERT INTO sync_transition_groups SELECT ?1,binding_id,source,kind,?5,?2,?2,?3,'published' FROM sync_transition_groups WHERE transition_id=?4",params![new_epoch(),version(&tx)?,txid,plan.transition_id,evidence.to_string()])?;
        tx.commit()?;
        Ok(txid)
    }
}
fn prepare_retained_projection(c: &Connection, b: &Binding, id: &str) -> Result<()> {
    let (header,hash):(String,String)=c.query_row("SELECT header_json,confirmed_hash FROM sync_contributions WHERE binding_id=?1 AND contribution_id=?2 AND confirmed_revision<>'0' AND presence<>'retired'",params![b.binding_id,id],|r|Ok((r.get(0)?,r.get(1)?)))?;
    let (local,basis):(String,String)=c.query_row("SELECT local_key,basis_json FROM sync_lineages WHERE binding_id=?1 AND contribution_id=?2 LIMIT 1",params![b.binding_id,id],|r|Ok((r.get(0)?,r.get(1)?)))?;
    c.execute_batch("CREATE TEMP TABLE IF NOT EXISTS sync_export_facts(id TEXT PRIMARY KEY,local_key TEXT NOT NULL,basis TEXT NOT NULL,header TEXT NOT NULL,payload_hash TEXT,need_put INTEGER DEFAULT 0) WITHOUT ROWID; CREATE TEMP TABLE IF NOT EXISTS sync_export_parts(id TEXT NOT NULL,part_index INTEGER NOT NULL,entry TEXT NOT NULL,PRIMARY KEY(id,part_index)) WITHOUT ROWID; DELETE FROM sync_export_facts;DELETE FROM sync_export_parts;")?;
    let mut h: Value = serde_json::from_str(&header).map_err(|_| LedgerError::CorruptLedger)?;
    h["expected_revision"] = json!("0");
    c.execute(
        "INSERT INTO sync_export_facts VALUES(?1,?2,?3,?4,?5,1)",
        params![id, local, basis, h.to_string(), hash],
    )?;
    let mut q=c.prepare("SELECT part_index,at_ms,metrics_json FROM sync_parts WHERE binding_id=?1 AND contribution_id=?2 AND projection_hash=?3 ORDER BY part_index")?;
    let mut count = 0;
    for row in q.query_map(params![b.binding_id, id, hash], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, String>(2)?,
        ))
    })? {
        let (index, at, metrics) = row?;
        let metrics: Value =
            serde_json::from_str(&metrics).map_err(|_| LedgerError::CorruptLedger)?;
        c.execute("INSERT INTO sync_export_parts VALUES(?1,?2,?3)",params![id,index,json!({"type":"part","id":id,"part_index":index,"at_ms":at.to_string(),"metrics":metrics}).to_string()])?;
        count += 1;
    }
    if h["part_count"].as_i64() != Some(count) {
        return Err(error("RECONCILE_PARTS_MISSING"));
    }
    Ok(())
}
