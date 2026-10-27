-- Explicit source-reported selection uses the same observations and events as
-- automatic requests. These tables retain decisions, never a second token sum.
CREATE TABLE adoption_policies (
 policy_id TEXT PRIMARY KEY,
 consumer_tool TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN('request_set','day_range','legacy_snapshot')),
 start_ms INTEGER, end_ms INTEGER, native_timezone TEXT,
 manifest_hash TEXT NOT NULL,
 created_ms INTEGER NOT NULL,
 status TEXT NOT NULL CHECK(status IN('active','forgotten')),
 CHECK((start_ms IS NULL AND end_ms IS NULL) OR (start_ms IS NOT NULL AND end_ms IS NOT NULL AND end_ms>start_ms))
) STRICT;
CREATE INDEX adoption_policy_range ON adoption_policies(consumer_tool,status,start_ms,end_ms);
CREATE TABLE adoption_items (
 selected_observation_id INTEGER PRIMARY KEY REFERENCES usage_observations(observation_id) ON DELETE CASCADE,
 source_observation_id INTEGER NOT NULL REFERENCES usage_observations(observation_id) ON DELETE CASCADE,
 policy_id TEXT NOT NULL REFERENCES adoption_policies(policy_id),
 UNIQUE(source_observation_id,policy_id)
) STRICT;
CREATE INDEX adoption_item_origin ON adoption_items(source_observation_id);
CREATE TABLE ledger_scheduler (
 key TEXT PRIMARY KEY,
 value TEXT NOT NULL
) STRICT;

-- Calendar-bearing deletion intent survives both source cleanup and restore.
CREATE TABLE adoption_forget_ranges (
 consumer_tool TEXT NOT NULL,
 start_ms INTEGER NOT NULL,
 end_ms INTEGER NOT NULL CHECK(end_ms>start_ms),
 native_timezone TEXT NOT NULL,
 identity_family TEXT, namespace_key TEXT,
 CHECK((identity_family IS NULL AND namespace_key IS NULL) OR
       (identity_family IS NOT NULL AND namespace_key IS NOT NULL AND length(identity_family)>0 AND length(namespace_key)>0))
) STRICT;
CREATE UNIQUE INDEX adoption_forget_range_scope ON adoption_forget_ranges(consumer_tool,start_ms,end_ms,native_timezone,COALESCE(identity_family,''),COALESCE(namespace_key,''));
