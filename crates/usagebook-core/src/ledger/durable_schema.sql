-- Acquisition state is separate from immutable usage evidence and publication.
CREATE TABLE durable_imports (
 import_id INTEGER PRIMARY KEY AUTOINCREMENT,
 root_id TEXT NOT NULL REFERENCES source_roots(root_id),
 capability_version TEXT NOT NULL,
 parser_version TEXT NOT NULL,
 identity_version INTEGER NOT NULL CHECK(identity_version>0),
 semantics_version INTEGER NOT NULL CHECK(semantics_version>0),
 enumeration_done INTEGER NOT NULL DEFAULT 0 CHECK(enumeration_done IN(0,1)),
 created_ms INTEGER NOT NULL, sealed_ms INTEGER, completed_ms INTEGER,
 next_queue INTEGER NOT NULL DEFAULT 0 CHECK(next_queue IN(0,1)),
 saved_observation_id INTEGER NOT NULL DEFAULT 0 CHECK(saved_observation_id>=0),
 UNIQUE(root_id,capability_version,parser_version,identity_version,semantics_version),
 CHECK((enumeration_done=0 AND sealed_ms IS NULL) OR (enumeration_done=1 AND sealed_ms IS NOT NULL))
) STRICT;
CREATE TABLE durable_import_targets (
 import_id INTEGER NOT NULL REFERENCES durable_imports(import_id),
 object_id INTEGER NOT NULL REFERENCES source_objects(object_id),
 target_generation TEXT NOT NULL,
 target_end INTEGER NOT NULL CHECK(target_end>=0),
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN('pending','saved','gap')),
 error_code TEXT, finished_ms INTEGER,
 PRIMARY KEY(import_id,object_id),
 CHECK((state='pending' AND finished_ms IS NULL) OR (state<>'pending' AND finished_ms IS NOT NULL))
) STRICT;
CREATE INDEX durable_targets_object ON durable_import_targets(object_id,state,import_id);
CREATE TABLE durable_evidence (
 observation_id INTEGER PRIMARY KEY REFERENCES usage_observations(observation_id) ON DELETE CASCADE,
 projection_format TEXT NOT NULL,
 projection_json TEXT NOT NULL CHECK(length(CAST(projection_json AS BLOB))<=16384 AND json_valid(projection_json))
) STRICT;
CREATE TRIGGER durable_evidence_immutable BEFORE UPDATE ON durable_evidence
BEGIN SELECT RAISE(ABORT,'durable evidence is immutable'); END;
