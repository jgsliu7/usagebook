-- A published daily contribution owns its proved interval even if its source
-- disappears. This is selection evidence, never an extra token balance.
CREATE TABLE durable_coverage (
 identity_family TEXT NOT NULL,
 namespace_key TEXT NOT NULL,
 identity_key TEXT NOT NULL,
 native_tool TEXT NOT NULL,
 model_key TEXT NOT NULL,
 start_ms INTEGER NOT NULL,
 end_ms INTEGER NOT NULL CHECK(end_ms>start_ms),
 -- An opaque origin reference, not an FK: forgetting its support must not
 -- release the interval to another channel or resurrect forgotten usage.
 first_observation_id INTEGER NOT NULL,
 rule_version TEXT NOT NULL CHECK(rule_version='day-owner-v1'),
 created_ms INTEGER NOT NULL,
 PRIMARY KEY(identity_family,namespace_key,identity_key)
) STRICT;
CREATE INDEX durable_coverage_interval ON durable_coverage(native_tool,namespace_key,start_ms,end_ms);
CREATE TRIGGER durable_coverage_immutable BEFORE UPDATE ON durable_coverage
 BEGIN SELECT RAISE(ABORT,'immutable coverage decision'); END;

CREATE TABLE durable_channel_choices (
 identity_family TEXT NOT NULL,
 namespace_key TEXT NOT NULL,
 session_key TEXT NOT NULL,
 channel TEXT NOT NULL,
 first_observation_id INTEGER NOT NULL,
 created_ms INTEGER NOT NULL,
 PRIMARY KEY(identity_family,namespace_key,session_key)
) STRICT;
CREATE TRIGGER durable_channel_choices_immutable BEFORE UPDATE ON durable_channel_choices
 BEGIN SELECT RAISE(ABORT,'immutable channel decision'); END;
