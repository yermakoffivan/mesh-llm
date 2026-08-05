//! Forward-only schema migrations for log_store.

use rusqlite::Connection;

/// Current schema version (incremented with each migration).
pub const CURRENT_VERSION: u32 = 10;

const MIGRATIONS_V1: &str = r#"
CREATE TABLE IF NOT EXISTS summaries (
    request_id   TEXT PRIMARY KEY,
    state        TEXT    NOT NULL DEFAULT 'active',
    created_at   TEXT    NOT NULL,
    terminal_at  TEXT,
    route        TEXT,
    model        TEXT,
    provider     TEXT,
    engine       TEXT,
    status_code  INTEGER,
    error_msg    TEXT,
    tenant_id    TEXT,
    account_id   TEXT,
    user_id      TEXT
);

CREATE INDEX IF NOT EXISTS idx_summaries_created ON summaries (created_at DESC, request_id DESC);
CREATE INDEX IF NOT EXISTS idx_summaries_state ON summaries (state);

CREATE TABLE IF NOT EXISTS lifecycle_events (
    event_id     TEXT PRIMARY KEY,
    request_id   TEXT    NOT NULL REFERENCES summaries(request_id) ON DELETE CASCADE,
    occurred_at  TEXT    NOT NULL,
    payload_json TEXT    NOT NULL DEFAULT '{}',

    UNIQUE(request_id, event_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_terminal_event_one_per_request
ON lifecycle_events (request_id)
WHERE payload_json LIKE '%"type":"completed"%'
   OR payload_json LIKE '%"type":"failed"%'
   OR payload_json LIKE '%"type":"rejected"%'
   OR payload_json LIKE '%"type":"cancelled"%';

CREATE INDEX IF NOT EXISTS idx_lifecycle_events_occurred ON lifecycle_events (occurred_at DESC, event_id DESC);
CREATE INDEX IF NOT EXISTS idx_lifecycle_events_request ON lifecycle_events (request_id);

CREATE TABLE IF NOT EXISTS artifact_pointers (
    artifact_id  TEXT PRIMARY KEY,
    request_id   TEXT    NOT NULL REFERENCES summaries(request_id) ON DELETE CASCADE,
    occurred_at  TEXT    NOT NULL,
    kind         TEXT    NOT NULL,
    metadata_json TEXT,

    UNIQUE(request_id, artifact_id)
);

CREATE INDEX IF NOT EXISTS idx_artifact_pointers_occurred ON artifact_pointers (occurred_at DESC, artifact_id DESC);

CREATE TABLE IF NOT EXISTS proxy_records (
    attempt_id   TEXT PRIMARY KEY,
    request_id   TEXT    NOT NULL REFERENCES summaries(request_id) ON DELETE CASCADE,
    occurred_at  TEXT    NOT NULL,
    target       TEXT    NOT NULL,
    provider     TEXT,
    engine       TEXT,
    started_at   TEXT,
    completed_at TEXT,
    status_code  INTEGER,
    error_msg    TEXT,

    UNIQUE(request_id, attempt_id)
);

CREATE INDEX IF NOT EXISTS idx_proxy_records_occurred ON proxy_records (occurred_at DESC, attempt_id DESC);

CREATE TABLE IF NOT EXISTS audit_entries (
    entry_id     TEXT PRIMARY KEY,
    request_id   TEXT REFERENCES summaries(request_id) ON DELETE SET NULL,
    occurred_at  TEXT NOT NULL,
    actor        TEXT NOT NULL,
    action       TEXT NOT NULL,
    detail_json  TEXT,

    UNIQUE(request_id, entry_id)
);

CREATE INDEX IF NOT EXISTS idx_audit_entries_occurred ON audit_entries (occurred_at DESC, entry_id DESC);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    delivery_id   TEXT PRIMARY KEY,
    request_id    TEXT REFERENCES summaries(request_id) ON DELETE SET NULL,
    occurred_at   TEXT    NOT NULL,
    target_url    TEXT    NOT NULL,
    attempt_number INTEGER NOT NULL,
    status_code   INTEGER,
    response_body TEXT,
    error_msg     TEXT,

    UNIQUE(request_id, delivery_id)
);

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_occurred ON webhook_deliveries (occurred_at DESC, delivery_id DESC);

CREATE TABLE IF NOT EXISTS cleanup_runs (
    run_id        TEXT PRIMARY KEY,
    occurred_at   TEXT    NOT NULL,
    policy_name   TEXT    NOT NULL,
    cutoff_before TEXT    NOT NULL,
    deleted_count INTEGER NOT NULL DEFAULT 0,
    duration_ms   INTEGER
);

CREATE INDEX IF NOT EXISTS idx_cleanup_runs_occurred ON cleanup_runs (occurred_at DESC, run_id DESC);
"#;

const MIGRATIONS_V2: &str = r#"
ALTER TABLE artifact_pointers ADD COLUMN media_kind TEXT;
ALTER TABLE artifact_pointers ADD COLUMN checksum TEXT;
ALTER TABLE artifact_pointers ADD COLUMN bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE artifact_pointers ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE artifact_pointers ADD COLUMN redacted INTEGER NOT NULL DEFAULT 0;
ALTER TABLE artifact_pointers ADD COLUMN truncated INTEGER NOT NULL DEFAULT 0;
ALTER TABLE artifact_pointers ADD COLUMN stored_at TEXT;
ALTER TABLE artifact_pointers ADD COLUMN missing INTEGER NOT NULL DEFAULT 0;
ALTER TABLE artifact_pointers ADD COLUMN corrupt INTEGER NOT NULL DEFAULT 0;
"#;

const MIGRATIONS_V3: &str = r#"
DROP INDEX IF EXISTS idx_terminal_event_one_per_request;
CREATE UNIQUE INDEX idx_terminal_event_one_per_request
ON lifecycle_events (request_id)
WHERE payload_json LIKE '%"type":"completed"%'
   OR payload_json LIKE '%"type":"failed"%'
   OR payload_json LIKE '%"type":"rejected"%'
   OR payload_json LIKE '%"type":"cancelled"%'
   OR payload_json LIKE '%"type":"dropped"%';
"#;

const MIGRATIONS_V4: &str = r#"
CREATE TABLE IF NOT EXISTS maintenance_operations (
    operation_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    cutoff_before TEXT NOT NULL,
    request_limit INTEGER NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL,
    planned_requests INTEGER NOT NULL,
    planned_events INTEGER NOT NULL,
    planned_artifacts INTEGER NOT NULL,
    planned_proxy_records INTEGER NOT NULL,
    planned_database_rows INTEGER NOT NULL,
    executed_requests INTEGER NOT NULL DEFAULT 0,
    executed_events INTEGER NOT NULL DEFAULT 0,
    executed_artifacts INTEGER NOT NULL DEFAULT 0,
    executed_proxy_records INTEGER NOT NULL DEFAULT 0,
    executed_database_rows INTEGER NOT NULL DEFAULT 0,
    has_more INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS maintenance_operation_targets (
    operation_id TEXT NOT NULL REFERENCES maintenance_operations(operation_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    PRIMARY KEY (operation_id, request_id),
    UNIQUE (operation_id, ordinal)
);

CREATE INDEX IF NOT EXISTS idx_maintenance_operation_targets_operation
ON maintenance_operation_targets (operation_id, ordinal);
"#;

const MIGRATIONS_V5: &str = r#"
ALTER TABLE maintenance_operations ADD COLUMN selection_fingerprint TEXT NOT NULL DEFAULT '';
"#;

const MIGRATIONS_V6: &str = r#"
ALTER TABLE maintenance_operations ADD COLUMN artifact_files_removed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE maintenance_operations ADD COLUMN artifact_files_failed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE maintenance_operations ADD COLUMN artifact_file_failure_class TEXT;
"#;

// Webhook delivery is a durable state machine, not a request-path side effect.
// The v1 columns are retained for forward-only compatibility but are scrubbed:
// an endpoint, response body, or raw transport error must never survive in a
// local logging record. New repository methods use only the v7 fields below.
const MIGRATIONS_V7: &str = r#"
ALTER TABLE webhook_deliveries ADD COLUMN state TEXT NOT NULL DEFAULT 'succeeded';
ALTER TABLE webhook_deliveries ADD COLUMN created_at TEXT NOT NULL DEFAULT '';
ALTER TABLE webhook_deliveries ADD COLUMN updated_at TEXT NOT NULL DEFAULT '';
ALTER TABLE webhook_deliveries ADD COLUMN next_attempt_at TEXT;
ALTER TABLE webhook_deliveries ADD COLUMN lease_expires_at TEXT;
ALTER TABLE webhook_deliveries ADD COLUMN claim_generation INTEGER NOT NULL DEFAULT 0;
ALTER TABLE webhook_deliveries ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 1;
ALTER TABLE webhook_deliveries ADD COLUMN last_error_code TEXT;

UPDATE webhook_deliveries
SET target_url = 'configured_webhook',
    response_body = NULL,
    error_msg = NULL,
    created_at = occurred_at,
    updated_at = occurred_at,
    state = CASE
        WHEN status_code BETWEEN 200 AND 299 THEN 'succeeded'
        ELSE 'dead_letter'
    END,
    max_attempts = CASE
        WHEN attempt_number > 0 THEN attempt_number
        ELSE 1
    END;

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_eligible
ON webhook_deliveries (state, next_attempt_at, lease_expires_at, created_at, delivery_id);
"#;

// Lifecycle terminal detection must not inspect serialized payload text. The
// typed columns preserve a queryable event classification while the payload
// remains available to the existing event-query API.
const MIGRATIONS_V8: &str = r#"
ALTER TABLE lifecycle_events ADD COLUMN event_type TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE lifecycle_events ADD COLUMN is_terminal INTEGER NOT NULL DEFAULT 0
    CHECK (is_terminal IN (0, 1));

UPDATE lifecycle_events
SET event_type = CASE
        WHEN json_valid(payload_json) THEN COALESCE(json_extract(payload_json, '$.type'), 'unknown')
        ELSE 'unknown'
    END,
    is_terminal = CASE
        WHEN json_valid(payload_json)
         AND json_extract(payload_json, '$.type') IN ('completed', 'failed', 'rejected', 'cancelled', 'dropped')
        THEN 1
        ELSE 0
    END;

DROP INDEX IF EXISTS idx_terminal_event_one_per_request;
CREATE UNIQUE INDEX idx_terminal_event_one_per_request
ON lifecycle_events (request_id)
WHERE is_terminal = 1;

CREATE INDEX IF NOT EXISTS idx_lifecycle_events_request_terminal
ON lifecycle_events (request_id, is_terminal);
"#;

const MIGRATIONS_V9: &str = r#"
ALTER TABLE maintenance_operations ADD COLUMN preview_audit_id TEXT;
ALTER TABLE maintenance_operations ADD COLUMN execution_audit_id TEXT;
"#;

const MIGRATIONS_V10: &str = r#"
ALTER TABLE maintenance_operations ADD COLUMN cleanup_filters_json TEXT NOT NULL DEFAULT '{}';
"#;

/// Apply all pending migrations, committing each schema step with its version
/// marker as one SQLite transaction.
pub fn apply_migrations(conn: &Connection) -> Result<(), rusqlite::Error> {
    let current_ver: i32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;

    if current_ver >= CURRENT_VERSION as i32 {
        return Ok(()); // already up-to-date
    }

    for (version, migration) in [
        (1, MIGRATIONS_V1),
        (2, MIGRATIONS_V2),
        (3, MIGRATIONS_V3),
        (4, MIGRATIONS_V4),
        (5, MIGRATIONS_V5),
        (6, MIGRATIONS_V6),
        (7, MIGRATIONS_V7),
        (8, MIGRATIONS_V8),
        (9, MIGRATIONS_V9),
        (10, MIGRATIONS_V10),
    ] {
        if current_ver < version {
            apply_migration_transactionally(conn, version, migration)?;
        }
    }

    Ok(())
}

fn apply_migration_transactionally(
    conn: &Connection,
    version: i32,
    migration: &str,
) -> Result<(), rusqlite::Error> {
    let transaction = conn.unchecked_transaction()?;
    transaction.execute_batch(migration)?;
    transaction.execute_batch(&format!("PRAGMA user_version = {version}"))?;
    transaction.commit()
}
