-- Cloud-sync local state v1; installed atomically by ledger schema v9.
-- Add in ledger schema v8 -> v9, inside the existing database writer transaction.
-- Keep all rows independent of usage_observations: local forget MUST NOT cascade here.
PRAGMA foreign_keys = ON;
CREATE TABLE sync_meta (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 schema_version INTEGER NOT NULL CHECK(schema_version=1),
 control_seq INTEGER NOT NULL DEFAULT 0 CHECK(control_seq>=0)
) STRICT;
INSERT INTO sync_meta VALUES(1,1,0);
CREATE TABLE sync_bindings (
 binding_id TEXT PRIMARY KEY,
 account_id TEXT NOT NULL,
 device_id TEXT NOT NULL,
 ledger_uuid TEXT NOT NULL,
 secret_check_hash TEXT NOT NULL CHECK(length(secret_check_hash)=64),
 enabled INTEGER NOT NULL DEFAULT 0 CHECK(enabled IN(0,1)),
 is_current INTEGER NOT NULL DEFAULT 1 CHECK(is_current IN(0,1)),
 status TEXT NOT NULL CHECK(status IN('off','ready','sending','backoff','reauth','conflict','storage_error','restore_required')),
 history_from_ms INTEGER,
 restore_epoch TEXT,
 server_generation TEXT NOT NULL DEFAULT '0',
 last_ack_ms INTEGER,
 last_error_code TEXT,
 created_ms INTEGER NOT NULL,
 UNIQUE(account_id,device_id)
) STRICT;
CREATE UNIQUE INDEX sync_one_current_binding ON sync_bindings(is_current) WHERE is_current=1;
CREATE TABLE sync_scopes (
 binding_id TEXT NOT NULL REFERENCES sync_bindings(binding_id),
 source TEXT NOT NULL,
 route TEXT NOT NULL CHECK(route IN('live','ledger')),
 source_epoch INTEGER NOT NULL DEFAULT 1 CHECK(source_epoch>0),
 history_from_ms INTEGER,
 baseline_generation TEXT,
 baseline_hash TEXT,
 state TEXT NOT NULL CHECK(state IN('ready','conflict','continued')),
 reason TEXT,
 PRIMARY KEY(binding_id,source)
) STRICT, WITHOUT ROWID;
-- Stable local keys are keyed digests; raw source IDs/paths are never network identifiers.
CREATE TABLE sync_lineages (
 binding_id TEXT NOT NULL REFERENCES sync_bindings(binding_id),
 local_key TEXT NOT NULL CHECK(length(local_key)=64),
 contribution_id TEXT NOT NULL CHECK(length(contribution_id)=64),
 source TEXT NOT NULL,
 domain TEXT NOT NULL CHECK(domain IN('ledger','live','transition')),
 basis_json TEXT NOT NULL CHECK(json_valid(basis_json)),
 PRIMARY KEY(binding_id,local_key)
) STRICT, WITHOUT ROWID;
CREATE INDEX sync_lineage_contribution ON sync_lineages(binding_id,contribution_id);
-- A retained cloud-facing projection, not a second source of local dashboard amounts.
-- For confirmed_revision>0 header/parts/hash are the latest CONFIRMED projection.
-- Pending headers live in immutable outbox chunks until apply_receipt.
-- Parts may be prepared under a new projection_hash; the contribution hash selects
-- the confirmed version. apply_receipt changes the pointer, never rewrites 100k parts.
-- For confirmed_revision=0 these fields hold the first queued projection.
CREATE TABLE sync_contributions (
 binding_id TEXT NOT NULL REFERENCES sync_bindings(binding_id),
 contribution_id TEXT NOT NULL CHECK(length(contribution_id)=64),
 source TEXT NOT NULL,
 payload_hash TEXT NOT NULL CHECK(length(payload_hash)=64),
 header_json TEXT NOT NULL CHECK(json_valid(header_json)),
 part_count INTEGER NOT NULL CHECK(part_count BETWEEN 0 AND 100000),
 confirmed_revision TEXT NOT NULL DEFAULT '0',
 confirmed_hash TEXT,
 presence TEXT NOT NULL CHECK(presence IN('present','absent','retired')),
 conflict_code TEXT,
 observed_projection_version INTEGER NOT NULL CHECK(observed_projection_version>=0),
 PRIMARY KEY(binding_id,contribution_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX sync_contribution_source ON sync_contributions(binding_id,source,presence);
CREATE TABLE sync_parts (
 binding_id TEXT NOT NULL,
 contribution_id TEXT NOT NULL,
 projection_hash TEXT NOT NULL CHECK(length(projection_hash)=64),
 part_index INTEGER NOT NULL CHECK(part_index>=0 AND part_index<100000),
 at_ms INTEGER NOT NULL,
 metrics_json TEXT NOT NULL CHECK(json_valid(metrics_json)),
 PRIMARY KEY(binding_id,contribution_id,projection_hash,part_index),
 FOREIGN KEY(binding_id,contribution_id) REFERENCES sync_contributions(binding_id,contribution_id)
) STRICT, WITHOUT ROWID;
-- Last accepted live publication. Absence is not a remote delete instruction.
CREATE TABLE sync_live_baseline (
 binding_id TEXT NOT NULL,
 source TEXT NOT NULL,
 local_key TEXT NOT NULL CHECK(length(local_key)=64),
 payload_hash TEXT NOT NULL CHECK(length(payload_hash)=64),
 contribution_id TEXT NOT NULL CHECK(length(contribution_id)=64),
 PRIMARY KEY(binding_id,source,local_key),
 FOREIGN KEY(binding_id,source) REFERENCES sync_scopes(binding_id,source)
) STRICT, WITHOUT ROWID;
CREATE TABLE sync_projection_clock (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 version INTEGER NOT NULL CHECK(version>=0)
) STRICT;
INSERT INTO sync_projection_clock VALUES(1,0);
-- staging means constructed locally but not yet sealed/authorized by a publication.
CREATE TABLE sync_transactions (
 transaction_id TEXT PRIMARY KEY,
 binding_id TEXT NOT NULL REFERENCES sync_bindings(binding_id),
 source TEXT,
 kind TEXT NOT NULL CHECK(kind IN('put','allocation','adoption','route_bridge')),
 state TEXT NOT NULL CHECK(state IN('staging','queued','uploading','commit_unknown','acked','blocked','aborted')),
 manifest_json TEXT CHECK(manifest_json IS NULL OR json_valid(manifest_json)),
 manifest_hash TEXT,
 projection_version INTEGER NOT NULL CHECK(projection_version>=0),
 retry_count INTEGER NOT NULL DEFAULT 0 CHECK(retry_count>=0),
 next_attempt_ms INTEGER NOT NULL DEFAULT 0,
 receipt_json TEXT CHECK(receipt_json IS NULL OR json_valid(receipt_json)),
 created_ms INTEGER NOT NULL,
 acked_ms INTEGER,
 last_error_code TEXT
) STRICT;
CREATE INDEX sync_outbox_due ON sync_transactions(binding_id,state,next_attempt_ms,created_ms);
CREATE TABLE sync_chunks (
 transaction_id TEXT NOT NULL REFERENCES sync_transactions(transaction_id) ON DELETE CASCADE,
 chunk_index INTEGER NOT NULL CHECK(chunk_index BETWEEN 0 AND 1023),
 body BLOB NOT NULL CHECK(length(body)<=1048576),
 body_hash TEXT NOT NULL CHECK(length(body_hash)=64),
 uploaded INTEGER NOT NULL DEFAULT 0 CHECK(uploaded IN(0,1)),
 PRIMARY KEY(transaction_id,chunk_index)
) STRICT, WITHOUT ROWID;
CREATE TABLE sync_transaction_items (
 transaction_id TEXT NOT NULL REFERENCES sync_transactions(transaction_id) ON DELETE CASCADE,
 contribution_id TEXT NOT NULL CHECK(length(contribution_id)=64),
 operation TEXT NOT NULL CHECK(operation IN('put','retire')),
 expected_revision TEXT NOT NULL,
 payload_hash TEXT,
 PRIMARY KEY(transaction_id,contribution_id)
) STRICT, WITHOUT ROWID;
-- One unresolved transaction per contribution. Release reservations only after ack or
-- after server proves transaction aborted (or a local-only staging never sent).
-- aborted is terminal and never auto-reopened; a published local transition stays
-- source-conflicted until explicit repair even after its reservation is released.
CREATE TABLE sync_reservations (
 binding_id TEXT NOT NULL,
 contribution_id TEXT NOT NULL,
 transaction_id TEXT NOT NULL REFERENCES sync_transactions(transaction_id),
 PRIMARY KEY(binding_id,contribution_id)
) STRICT, WITHOUT ROWID;
CREATE TABLE sync_transition_groups (
 transition_id TEXT PRIMARY KEY,
 binding_id TEXT NOT NULL REFERENCES sync_bindings(binding_id),
 source TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN('adoption','route_bridge')),
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 prepared_projection_version INTEGER NOT NULL,
 published_projection_version INTEGER,
 transaction_id TEXT NOT NULL REFERENCES sync_transactions(transaction_id),
 state TEXT NOT NULL CHECK(state IN('prepared','published','acked','cancelled'))
) STRICT;
CREATE TABLE sync_coverage_gaps (
 gap_id TEXT PRIMARY KEY,
 binding_id TEXT NOT NULL REFERENCES sync_bindings(binding_id),
 source TEXT NOT NULL,
 start_ms INTEGER,
 end_ms INTEGER NOT NULL,
 reason TEXT NOT NULL,
 recorded_ms INTEGER NOT NULL,
 CHECK(start_ms IS NULL OR start_ms<end_ms)
) STRICT;
-- Pins source identity while a WAL read snapshot is streamed. Ordinary publication
-- remains permitted; route/adoption/alias changes wait. Forget cancels the export first.
CREATE TABLE sync_source_exports (
 binding_id TEXT NOT NULL,
 source TEXT NOT NULL,
 export_id TEXT NOT NULL UNIQUE,
 projection_version INTEGER NOT NULL CHECK(projection_version>=0),
 source_epoch INTEGER NOT NULL CHECK(source_epoch>0),
 control_seq INTEGER NOT NULL CHECK(control_seq>=0),
 started_ms INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN('reading','sealing','cancelled')),
 PRIMARY KEY(binding_id,source),
 FOREIGN KEY(binding_id,source) REFERENCES sync_scopes(binding_id,source)
) STRICT, WITHOUT ROWID;
