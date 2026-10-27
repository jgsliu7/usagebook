-- Value snapshots are sealed atomically with their finite import targets.
-- A building attempt never owns targets and is abandoned after interruption.
CREATE TABLE durable_snapshots (
 snapshot_id TEXT PRIMARY KEY,
 import_id INTEGER NOT NULL REFERENCES durable_imports(import_id),
 root_id TEXT NOT NULL REFERENCES source_roots(root_id),
 state TEXT NOT NULL CHECK(state IN('building','sealed','aborted')),
 created_ms INTEGER NOT NULL,
 sealed_ms INTEGER,
 fingerprint TEXT,
 error_code TEXT,
 row_count INTEGER NOT NULL DEFAULT 0 CHECK(row_count>=0),
 gap_count INTEGER NOT NULL DEFAULT 0 CHECK(gap_count>=0),
 byte_count INTEGER NOT NULL DEFAULT 0 CHECK(byte_count>=0),
 CHECK((state='sealed' AND sealed_ms IS NOT NULL AND fingerprint IS NOT NULL)
       OR (state<>'sealed' AND sealed_ms IS NULL))
) STRICT;
CREATE INDEX durable_snapshots_root ON durable_snapshots(root_id,state,created_ms);

CREATE TABLE durable_snapshot_members (
 snapshot_id TEXT NOT NULL REFERENCES durable_snapshots(snapshot_id) ON DELETE CASCADE,
 row_key TEXT NOT NULL,
 fingerprint TEXT NOT NULL,
 consumer_tool TEXT NOT NULL,
 grain TEXT NOT NULL CHECK(grain IN('day','request')),
 native_date TEXT,
 observation_id INTEGER REFERENCES usage_observations(observation_id) ON DELETE SET NULL,
 PRIMARY KEY(snapshot_id,row_key)
) STRICT;
CREATE INDEX durable_snapshot_members_days ON durable_snapshot_members(snapshot_id,consumer_tool,grain,native_date);
CREATE INDEX durable_snapshot_members_observation ON durable_snapshot_members(observation_id);
