-- A later reconstruction is intentionally distinct from original adoption-time
-- evidence. It allocates an existing amount and cannot create consumption.
CREATE TABLE durable_time_revisions (
 parent_observation_id INTEGER PRIMARY KEY REFERENCES usage_observations(observation_id) ON DELETE CASCADE,
 version INTEGER NOT NULL CHECK(version=1),
 provenance TEXT NOT NULL CHECK(provenance='rebuilt-from-retained-source'),
 member_count INTEGER NOT NULL CHECK(member_count BETWEEN 1 AND 100000),
 parent_hash TEXT NOT NULL CHECK(length(parent_hash)=64),
 policy_hash TEXT NOT NULL CHECK(length(policy_hash)=64),
 evidence_json TEXT NOT NULL CHECK(length(CAST(evidence_json AS BLOB))<=262144 AND json_valid(evidence_json)),
 manifest_hash TEXT NOT NULL CHECK(length(manifest_hash)=64),
 created_ms INTEGER NOT NULL
) STRICT;
CREATE TABLE durable_time_revision_members (
 parent_observation_id INTEGER NOT NULL REFERENCES durable_time_revisions(parent_observation_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
 member_key TEXT NOT NULL CHECK(length(member_key)=64),
 content_hash TEXT NOT NULL CHECK(length(content_hash)=64),
 occurred_ms INTEGER NOT NULL CHECK(occurred_ms>=0),
 metrics_json TEXT NOT NULL CHECK(length(CAST(metrics_json AS BLOB))<=512 AND json_valid(metrics_json) AND json_array_length(metrics_json)=10),
 cost_units INTEGER, cost_scale INTEGER,
 PRIMARY KEY(parent_observation_id,member_key),
 CHECK((cost_units IS NULL AND cost_scale IS NULL) OR
       (cost_units IS NOT NULL AND cost_units>=0 AND cost_scale IS NOT NULL AND cost_scale BETWEEN 0 AND 18))
) STRICT, WITHOUT ROWID;
CREATE TABLE time_repair_attempts (
 parent_observation_id INTEGER PRIMARY KEY REFERENCES usage_observations(observation_id) ON DELETE CASCADE,
 basis TEXT NOT NULL, attempted_ms INTEGER NOT NULL, reason TEXT NOT NULL
) STRICT;
-- Upgrade staging preserves the earliest retained compatibility evidence before
-- a new collector publication can replace it. These are not additive facts.
CREATE TABLE time_repair_saved_sources (
 report_source TEXT PRIMARY KEY, ledger_uuid TEXT NOT NULL,
 metadata_json TEXT NOT NULL CHECK(json_valid(metadata_json)),
 entry_count INTEGER NOT NULL CHECK(entry_count BETWEEN 0 AND 100000)
) STRICT;
CREATE TABLE time_repair_saved_entries (
 report_source TEXT NOT NULL REFERENCES time_repair_saved_sources(report_source) ON DELETE CASCADE,
 ordinal INTEGER NOT NULL, entry_json TEXT NOT NULL CHECK(json_valid(entry_json)),
 PRIMARY KEY(report_source,ordinal)
) STRICT, WITHOUT ROWID;
CREATE TABLE time_repair_saved_projection (
 report_source TEXT NOT NULL REFERENCES time_repair_saved_sources(report_source) ON DELETE CASCADE,
 root_id TEXT NOT NULL, row_key TEXT NOT NULL,
 identity_family TEXT NOT NULL, namespace_key TEXT NOT NULL,
 layout_json TEXT NOT NULL CHECK(json_valid(layout_json)),
 projection_json TEXT NOT NULL CHECK(json_valid(projection_json)),
 row_hash TEXT NOT NULL,
 PRIMARY KEY(root_id,row_key)
) STRICT, WITHOUT ROWID;
CREATE TRIGGER time_revision_immutable BEFORE UPDATE ON durable_time_revisions
 BEGIN SELECT RAISE(ABORT,'immutable time revision'); END;
CREATE TRIGGER time_revision_member_immutable BEFORE UPDATE ON durable_time_revision_members
 BEGIN SELECT RAISE(ABORT,'immutable time revision member'); END;
CREATE TRIGGER time_revision_member_sealed BEFORE INSERT ON durable_time_revision_members
 WHEN EXISTS(SELECT 1 FROM durable_time_revisions WHERE parent_observation_id=NEW.parent_observation_id)
 BEGIN SELECT RAISE(ABORT,'sealed time revision'); END;
CREATE TRIGGER time_revision_no_detach BEFORE DELETE ON durable_time_revisions
 WHEN EXISTS(SELECT 1 FROM usage_observations WHERE observation_id=OLD.parent_observation_id)
 BEGIN SELECT RAISE(ABORT,'time revision requires parent deletion'); END;
CREATE TRIGGER time_revision_member_no_detach BEFORE DELETE ON durable_time_revision_members
 WHEN EXISTS(SELECT 1 FROM usage_observations WHERE observation_id=OLD.parent_observation_id)
 BEGIN SELECT RAISE(ABORT,'time revision member requires parent deletion'); END;
CREATE TRIGGER time_revision_exclusive BEFORE INSERT ON durable_time_revisions
 WHEN EXISTS(SELECT 1 FROM durable_time_allocations WHERE parent_observation_id=NEW.parent_observation_id)
 BEGIN SELECT RAISE(ABORT,'original time allocation already exists'); END;
CREATE TRIGGER time_allocation_exclusive BEFORE INSERT ON durable_time_allocations
 WHEN EXISTS(SELECT 1 FROM durable_time_revisions WHERE parent_observation_id=NEW.parent_observation_id)
 BEGIN SELECT RAISE(ABORT,'time revision already exists'); END;
