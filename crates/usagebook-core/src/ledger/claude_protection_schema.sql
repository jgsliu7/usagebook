-- Unresolved and superseded Claude source-report observations remain durable.
-- Rows contain only whitelisted usage fields, never conversation text.
CREATE TABLE IF NOT EXISTS claude_protection_pending (
 identity TEXT PRIMARY KEY,
 entry_json TEXT NOT NULL,
 reason TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS claude_protection_pending_reason
 ON claude_protection_pending(reason);
-- Identify pre-watermark adopted histories without scanning all evidence.
CREATE INDEX IF NOT EXISTS claude_original_baseline_identity
 ON claude_protection_pending(json_extract(entry_json, '$.Request.record.session_id'), json_extract(entry_json, '$.Request.record.dedup_key'))
 WHERE reason = 'original_baseline';
CREATE INDEX IF NOT EXISTS observations_claude_request_identity
 ON usage_observations(identity_family, namespace_key, native_request_id, native_message_id, native_session_id)
 WHERE identity_family = 'claude' AND native_request_id IS NOT NULL;
