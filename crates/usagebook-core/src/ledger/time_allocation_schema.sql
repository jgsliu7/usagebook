-- An adopted amount remains owned by its original observation. These immutable
-- members preserve only its captured time distribution; they are never additive
-- usage events and are deleted only together with that parent observation.
CREATE TABLE durable_time_allocations (
 parent_observation_id INTEGER PRIMARY KEY REFERENCES usage_observations(observation_id) ON DELETE CASCADE,
 version INTEGER NOT NULL CHECK(version=1),
 provenance TEXT NOT NULL CHECK(provenance='captured-with-adoption'),
 complete INTEGER NOT NULL CHECK(complete=1),
 member_count INTEGER NOT NULL CHECK(member_count BETWEEN 1 AND 100000),
 manifest_hash TEXT NOT NULL CHECK(length(manifest_hash)=64),
 created_ms INTEGER NOT NULL
) STRICT;
CREATE TABLE durable_time_members (
 parent_observation_id INTEGER NOT NULL REFERENCES durable_time_allocations(parent_observation_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
 member_key TEXT NOT NULL CHECK(length(member_key)=64),
 occurred_ms INTEGER NOT NULL CHECK(occurred_ms>=0),
 metrics_json TEXT NOT NULL CHECK(length(CAST(metrics_json AS BLOB))<=512 AND json_valid(metrics_json) AND json_array_length(metrics_json)=10),
 cost_units INTEGER, cost_scale INTEGER,
 PRIMARY KEY(parent_observation_id,member_key),
 CHECK((cost_units IS NULL AND cost_scale IS NULL) OR
       (cost_units IS NOT NULL AND cost_units>=0 AND cost_scale IS NOT NULL AND cost_scale BETWEEN 0 AND 18))
) STRICT, WITHOUT ROWID;
CREATE TRIGGER time_allocation_immutable BEFORE UPDATE ON durable_time_allocations
 BEGIN SELECT RAISE(ABORT,'immutable time allocation'); END;
CREATE TRIGGER time_member_immutable BEFORE UPDATE ON durable_time_members
 BEGIN SELECT RAISE(ABORT,'immutable time member'); END;
-- The complete header is written last in the same transaction. No later writer
-- may append to the captured member set.
CREATE TRIGGER time_member_sealed BEFORE INSERT ON durable_time_members
 WHEN EXISTS(SELECT 1 FROM durable_time_allocations WHERE parent_observation_id=NEW.parent_observation_id)
 BEGIN SELECT RAISE(ABORT,'sealed time allocation'); END;
CREATE TRIGGER time_allocation_no_detach BEFORE DELETE ON durable_time_allocations
 WHEN EXISTS(SELECT 1 FROM usage_observations WHERE observation_id=OLD.parent_observation_id)
 BEGIN SELECT RAISE(ABORT,'time allocation requires parent deletion'); END;
CREATE TRIGGER time_member_no_detach BEFORE DELETE ON durable_time_members
 WHEN EXISTS(SELECT 1 FROM usage_observations WHERE observation_id=OLD.parent_observation_id)
 BEGIN SELECT RAISE(ABORT,'time member requires parent deletion'); END;
