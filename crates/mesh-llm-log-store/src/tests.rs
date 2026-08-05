//! Acceptance tests for mesh-llm-log-store.
//! All tests use real temp SQLite files (no in-memory shortcut).

use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::cursor::{decode_cursor, encode_cursor};
use super::error::LogStoreError;
use super::migrations::{CURRENT_VERSION, apply_migrations};
use super::repositories::{
    WebhookDeliveryErrorCode, WebhookDeliveryInsertOutcome, WebhookDeliveryRecord,
    WebhookDeliveryState, WebhookRetryOutcome,
};
use super::store::{Clock as ClockTrait, LogStore};

/// Fixed clock returning deterministic ISO timestamps.
#[derive(Debug)]
struct TestClock {
    instant: AtomicU64,
}

impl Default for TestClock {
    fn default() -> Self {
        Self {
            instant: AtomicU64::new(0),
        }
    }
}

impl ClockTrait for TestClock {
    fn now(&self) -> String {
        let n = self.instant.fetch_add(1, Ordering::Relaxed);
        format!("2025-01-01T00:00:{:02}Z", n % 60)
    }
}

/// Open a fresh store backed by a temp directory. Directory is cleaned up on drop.
fn open_store() -> (LogStore, Arc<dyn ClockTrait>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let clock: Arc<dyn ClockTrait> = Arc::new(TestClock::default());
    let store = LogStore::open(tmp.path(), clock.clone()).expect("open log store");
    (store, clock, tmp)
}

// ════════════════════════════════════
//  MIGRATION TESTS
// ════════════════════════════════════

#[test]
fn fresh_db_migrates_to_latest() {
    let (store, _, _tmp) = open_store();
    assert_eq!(store.schema_version(), CURRENT_VERSION);
}

#[cfg(unix)]
#[test]
fn sqlite_root_database_and_sidecars_are_owner_private() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().expect("database root");
    let clock: Arc<dyn ClockTrait> = Arc::new(TestClock::default());
    let store = LogStore::open(root.path(), clock.clone()).expect("open store");
    store
        .insert_summary(
            "private-db",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .expect("write transaction");

    let database = store.db_path().to_path_buf();
    assert_eq!(
        std::fs::metadata(root.path()).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&database).unwrap().permissions().mode() & 0o777,
        0o600
    );
    for suffix in ["-wal", "-shm"] {
        let sidecar = std::path::PathBuf::from(format!("{}{}", database.display(), suffix));
        if sidecar.exists() {
            assert_eq!(
                std::fs::metadata(sidecar).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
}

#[cfg(unix)]
#[test]
fn sqlite_root_symlink_is_rejected_before_canonicalization() {
    use std::os::unix::fs::symlink;

    let parent = tempfile::tempdir().expect("parent root");
    let target = tempfile::tempdir().expect("target root");
    let link = parent.path().join("configured-log-root");
    symlink(target.path(), &link).expect("database root link");
    let clock: Arc<dyn ClockTrait> = Arc::new(TestClock::default());

    let error = match LogStore::open(&link, clock) {
        Ok(_) => panic!("symlinked root must fail closed"),
        Err(error) => error,
    };
    assert!(matches!(error, LogStoreError::PathUnsafe { .. }));
    assert!(!target.path().join("log_store.db").exists());
    assert!(!error.to_string().contains(&link.display().to_string()));
}

#[cfg(windows)]
#[test]
fn sqlite_root_database_and_sidecars_have_only_current_user_acl() {
    let root = tempfile::tempdir().expect("database root");
    let clock: Arc<dyn ClockTrait> = Arc::new(TestClock::default());
    let store = LogStore::open(root.path(), clock.clone()).expect("open store");
    store
        .insert_summary(
            "private-db",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .expect("write transaction");

    crate::artifact_privacy::verify_current_user_only_storage_path(root.path(), true)
        .expect("private root DACL");
    let database = store.db_path();
    crate::artifact_privacy::verify_current_user_only_storage_path(database, false)
        .expect("private database DACL");
    for suffix in ["-wal", "-shm"] {
        let sidecar = std::path::PathBuf::from(format!("{}{}", database.display(), suffix));
        if sidecar.exists() {
            crate::artifact_privacy::verify_current_user_only_storage_path(&sidecar, false)
                .expect("private SQLite sidecar DACL");
        }
    }
}

#[test]
fn reopen_preserves_data_and_skips_migrations() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let clock: Arc<dyn ClockTrait> = Arc::new(TestClock::default());

    // Insert data.
    let store1 = LogStore::open(tmp.path(), clock.clone()).expect("open v1");
    store1
        .insert_summary(
            "s-001",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();
    drop(store1);

    // Reopen at same path — migrations should be skipped, data preserved.
    let (store2, _, _tmp) = {
        let s = LogStore::reopen_at(tmp.path(), clock.clone()).expect("reopen");
        (s, clock.clone(), tmp)
    };

    assert_eq!(store2.schema_version(), CURRENT_VERSION);
    let row = store2
        .get_summary("s-001")
        .unwrap()
        .expect("summary exists after reopen");
    assert_eq!(row.request_id, "s-001");
}

#[test]
fn migrations_are_idempotent_on_reopen() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let clock: Arc<dyn ClockTrait> = Arc::new(TestClock::default());

    for _ in 0..3 {
        let s = LogStore::open(tmp.path(), clock.clone()).expect("open");
        assert_eq!(s.schema_version(), CURRENT_VERSION);
        drop(s);
    }
}

// ════════════════════════════════════
//  TERMINAL EVENT TRANSACTION TESTS
// ════════════════════════════════════

#[test]
fn terminal_event_and_summary_are_one_transaction() {
    let (store, clock, _tmp) = open_store();

    store
        .insert_summary(
            "txn-s1",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();

    let payload = r#"{"type":"completed","status_code":200}"#;
    store
        .write_terminal_event("txn-s1", "evt-1", payload, "completed", &clock.now())
        .expect("write terminal succeeds");

    let row = store
        .get_summary("txn-s1")
        .unwrap()
        .expect("summary exists");
    assert_eq!(row.state, "completed");
}

#[test]
fn duplicate_terminal_write_returns_typed_conflict() {
    let (store, clock, _tmp) = open_store();

    store
        .insert_summary(
            "dup-s1",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();

    let payload1 = r#"{"type":"completed","status_code":200}"#;
    store
        .write_terminal_event("dup-s1", "evt-1", payload1, "completed", &clock.now())
        .expect("first write succeeds");

    let err = store
        .write_terminal_event(
            "dup-s1",
            "evt-2",
            r#"{"type":"failed","error":"boom"}"#,
            "failed",
            &clock.now(),
        )
        .unwrap_err();
    assert!(matches!(err, LogStoreError::DuplicateTerminalEvent { .. }));
}

#[test]
fn duplicate_terminal_error_keeps_payload_out_of_the_error() {
    let (store, clock, _tmp) = open_store();
    let secret = "supersecret-terminal-payload";
    store
        .insert_summary(
            "duplicate-safe",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .expect("insert summary");
    store
        .write_terminal_event(
            "duplicate-safe",
            "first-terminal",
            r#"{"type":"completed"}"#,
            "completed",
            &clock.now(),
        )
        .expect("first terminal");

    let error = store
        .write_terminal_event(
            "duplicate-safe",
            "second-terminal",
            &format!(r#"{{"type":"failed","error":"{secret}"}}"#),
            "failed",
            &clock.now(),
        )
        .expect_err("duplicate terminal should fail");

    assert!(matches!(
        error,
        LogStoreError::DuplicateTerminalEvent { ref event_type, .. } if event_type == "failed"
    ));
    assert!(!error.to_string().contains(secret));
}

#[test]
fn terminal_detection_uses_the_typed_top_level_event_type() {
    let (store, clock, _tmp) = open_store();
    store
        .insert_summary(
            "typed-terminal",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .expect("insert summary");

    store
        .insert_lifecycle_event(
            "typed-terminal",
            "nested-terminal-text",
            r#"{"type":"admitted","context":{"type":"completed"}}"#,
            &clock.now(),
        )
        .expect("nested terminal text is not terminal");
    assert!(
        !store
            .has_terminal_event("typed-terminal")
            .expect("terminal state")
    );

    store
        .insert_lifecycle_event(
            "typed-terminal",
            "actual-terminal",
            r#"{"type":"completed"}"#,
            &clock.now(),
        )
        .expect("actual terminal event");
    assert!(
        store
            .has_terminal_event("typed-terminal")
            .expect("terminal state")
    );
}

#[test]
fn v8_migration_backfills_typed_terminal_columns_and_index() {
    let connection = rusqlite::Connection::open_in_memory().expect("open legacy database");
    connection
        .execute_batch(
            r#"
            CREATE TABLE lifecycle_events (
                event_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE UNIQUE INDEX idx_terminal_event_one_per_request
            ON lifecycle_events (request_id)
            WHERE payload_json LIKE '%"type":"completed"%';
            INSERT INTO lifecycle_events VALUES
                ('legacy-terminal', 'request-terminal', '2025-01-01T00:00:00Z', '{"type":"completed"}'),
                ('legacy-active', 'request-active', '2025-01-01T00:00:01Z', '{"type":"admitted"}');
            PRAGMA user_version = 7;
            "#,
        )
        .expect("seed v7 lifecycle table");
    seed_v7_maintenance_schema(&connection);

    apply_migrations(&connection).expect("migrate v7 lifecycle table");
    let terminal: (String, i64) = connection
        .query_row(
            "SELECT event_type, is_terminal FROM lifecycle_events WHERE event_id = 'legacy-terminal'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read terminal backfill");
    let active: (String, i64) = connection
        .query_row(
            "SELECT event_type, is_terminal FROM lifecycle_events WHERE event_id = 'legacy-active'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read active backfill");

    assert_eq!(terminal, ("completed".to_string(), 1));
    assert_eq!(active, ("admitted".to_string(), 0));
    assert!(connection
        .execute(
            "INSERT INTO lifecycle_events \
             (event_id, request_id, occurred_at, payload_json, event_type, is_terminal) \
             VALUES ('duplicate-terminal', 'request-terminal', '2025-01-01T00:00:02Z', '{}', 'failed', 1)",
            [],
        )
        .is_err());
    assert_eq!(
        connection
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
            .expect("read migrated schema version"),
        CURRENT_VERSION as i32
    );
    let maintenance_columns: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('maintenance_operations') \
             WHERE name IN ( \
                'selection_fingerprint', \
                'artifact_files_removed', \
                'artifact_files_failed', \
                'artifact_file_failure_class', \
                'preview_audit_id', \
                'execution_audit_id', \
                'cleanup_filters_json' \
             )",
            [],
            |row| row.get(0),
        )
        .expect("inspect migrated maintenance schema");
    assert_eq!(maintenance_columns, 7);
}

#[test]
fn failed_v8_migration_rolls_back_terminal_columns_and_schema_version() {
    let connection = rusqlite::Connection::open_in_memory().expect("open legacy database");
    connection
        .execute_batch(
            r#"
            CREATE TABLE lifecycle_events (
                event_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}'
            );
            INSERT INTO lifecycle_events VALUES
                ('first-terminal', 'request-terminal', '2025-01-01T00:00:00Z', '{"type":"completed"}'),
                ('second-terminal', 'request-terminal', '2025-01-01T00:00:01Z', '{"type":"failed"}');
            PRAGMA user_version = 7;
            "#,
        )
        .expect("seed v7 lifecycle table with terminal conflict");
    seed_v7_maintenance_schema(&connection);

    assert!(apply_migrations(&connection).is_err());
    let version: i32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("read schema version after failed migration");
    assert_eq!(version, 7);
    let has_terminal_columns: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('lifecycle_events') \
             WHERE name IN ('event_type', 'is_terminal')",
            [],
            |row| row.get(0),
        )
        .expect("inspect rolled back lifecycle table");
    assert_eq!(has_terminal_columns, 0);
}

/// A database marked v7 has the maintenance schema introduced in v4 and
/// extended in v5 and v6. Lifecycle-only fixtures still need that historical
/// prerequisite before they exercise later migrations.
fn seed_v7_maintenance_schema(connection: &rusqlite::Connection) {
    connection
        .execute_batch(
            r#"
            CREATE TABLE maintenance_operations (
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
                completed_at TEXT,
                selection_fingerprint TEXT NOT NULL DEFAULT '',
                artifact_files_removed INTEGER NOT NULL DEFAULT 0,
                artifact_files_failed INTEGER NOT NULL DEFAULT 0,
                artifact_file_failure_class TEXT
            );

            CREATE TABLE maintenance_operation_targets (
                operation_id TEXT NOT NULL REFERENCES maintenance_operations(operation_id) ON DELETE CASCADE,
                ordinal INTEGER NOT NULL,
                request_id TEXT NOT NULL,
                PRIMARY KEY (operation_id, request_id),
                UNIQUE (operation_id, ordinal)
            );

            CREATE INDEX idx_maintenance_operation_targets_operation
            ON maintenance_operation_targets (operation_id, ordinal);
            "#,
        )
        .expect("seed v7 maintenance schema");
}

#[test]
fn duplicate_non_terminal_same_type_events_allowed() {
    let (store, clock, _tmp) = open_store();

    store
        .insert_summary(
            "multi-s1",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();

    // Insert two non-terminal events with different event_ids — both should succeed.
    let payload = r#"{"type":"admitted","model":"llama3"}"#;
    store
        .insert_lifecycle_event("multi-s1", "evt-started-1", payload, &clock.now())
        .expect("first started succeeds");
    store
        .insert_lifecycle_event("multi-s1", "evt-stream-2", payload, &clock.now())
        .expect("second admitted also succeeds — no unique(summary,event_type) constraint");

    // Verify both events exist.
    let count = store.count_table("lifecycle_events").unwrap();
    assert_eq!(count, 2);
}

// ════════════════════════════════════
//  CURSOR PAGINATION TESTS
// ════════════════════════════════════

#[test]
fn cursor_pages_no_overlap_or_omission() {
    let (store, _clock, _tmp) = open_store();

    // Insert with non-unique timestamps so pagination works correctly.
    // Unique sequential timestamps cause gaps: cursor at T3 skips T4 (which is >T3 and <T5).
    for i in 0..7u32 {
        let ts = if i % 2 == 0 {
            "2025-01-01T00:00:10Z"
        } else {
            "2025-01-01T00:00:20Z"
        };
        store
            .insert_summary(
                &format!("page-{:04}", i),
                None,
                None,
                None,
                None,
                ts,
                None,
                None,
                None,
            )
            .unwrap();
    }

    let page_size = 3;
    let mut all_ids: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let page = store.list_summaries(page_size, cursor.as_deref()).unwrap();
        assert!(page.items.len() <= page_size);
        all_ids.extend(page.items.iter().map(|r| r.request_id.clone()));
        if let Some(c) = page.next_cursor {
            cursor = Some(c);
        } else {
            break;
        }
    }

    // All 7 IDs present, no duplicates.
    assert_eq!(all_ids.len(), 7, "expected all 7 summaries");
    let mut sorted = all_ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 7, "no duplicate IDs across pages");

    // Verify expected IDs.
    for i in 0..7u32 {
        assert!(all_ids.iter().any(|id| id == &format!("page-{:04}", i)));
    }
}

#[test]
fn opaque_keyset_cursor_respects_boundaries_and_limits_for_summaries_and_events() {
    let (store, _clock, _tmp) = open_store();
    for (request_id, created_at) in [
        ("page-a", "2025-01-01T00:00:01Z"),
        ("page-b", "2025-01-01T00:00:02Z"),
        ("page-c", "2025-01-01T00:00:03Z"),
    ] {
        store
            .insert_summary(
                request_id, None, None, None, None, created_at, None, None, None,
            )
            .expect("insert summary");
    }
    for (event_id, occurred_at) in [
        ("event-a", "2025-01-01T00:00:01Z"),
        ("event-b", "2025-01-01T00:00:02Z"),
        ("event-c", "2025-01-01T00:00:03Z"),
    ] {
        store
            .insert_lifecycle_event("page-a", event_id, r#"{"type":"admitted"}"#, occurred_at)
            .expect("insert lifecycle event");
    }

    let first_summaries = store.list_summaries(1, None).expect("first summaries page");
    assert_eq!(first_summaries.items[0].request_id, "page-c");
    let second_summaries = store
        .list_summaries(1, first_summaries.next_cursor.as_deref())
        .expect("second summaries page");
    assert_eq!(second_summaries.items[0].request_id, "page-b");

    let first_events = store
        .list_lifecycle_events(1, None)
        .expect("first lifecycle page");
    assert_eq!(first_events.items[0].event_id, "event-c");
    let second_events = store
        .list_lifecycle_events(1, first_events.next_cursor.as_deref())
        .expect("second lifecycle page");
    assert_eq!(second_events.items[0].event_id, "event-b");

    assert!(
        store
            .list_summaries(0, None)
            .expect("zero summary limit")
            .items
            .is_empty()
    );
    assert!(
        store
            .list_lifecycle_events(0, None)
            .expect("zero lifecycle limit")
            .items
            .is_empty()
    );
    assert!(matches!(
        store.list_summaries(1, Some("not-an-opaque-cursor")),
        Err(LogStoreError::CursorMalformed(_))
    ));
}

#[test]
fn cursor_pages_no_gap_after_reopen() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let clock: Arc<dyn ClockTrait> = Arc::new(TestClock::default());

    // Insert data and get first page.
    let store1 = LogStore::open(tmp.path(), clock.clone()).unwrap();
    for i in 0..5u32 {
        let ts = if i % 2 == 0 {
            "2025-01-01T00:00:10Z"
        } else {
            "2025-01-01T00:00:20Z"
        };
        store1
            .insert_summary(
                &format!("reopen-{:04}", i),
                None,
                None,
                None,
                None,
                ts,
                None,
                None,
                None,
            )
            .unwrap();
    }

    let first_page = store1.list_summaries(2, None).unwrap();
    assert_eq!(first_page.items.len(), 2);
    let cursor_str = first_page.next_cursor.expect("has next cursor");
    drop(store1);

    // Reopen and fetch page 2 using the same cursor.
    let store2 = LogStore::reopen_at(tmp.path(), clock.clone()).unwrap();
    let second_page = store2.list_summaries(2, Some(&cursor_str)).unwrap();

    // No overlap with first page.
    assert!(
        second_page.items.iter().all(|r| {
            !first_page
                .items
                .iter()
                .any(|f| f.request_id == r.request_id)
        }),
        "page 2 should not contain items from page 1"
    );

    let total: Vec<String> = first_page
        .items
        .into_iter()
        .chain(second_page.items)
        .map(|r| r.request_id)
        .collect();
    assert_eq!(total.len(), 4, "should see 4 items across two pages");
}

#[test]
fn cursor_same_timestamp_no_overlap_or_omission() {
    let (store, _, _tmp) = open_store();

    // Insert all rows with the same created_at — tiebreak by request_id.
    for i in 0..5u32 {
        store
            .conn()
            .execute(
                "INSERT INTO summaries (request_id, state, created_at) VALUES (?, 'active', ?)",
                rusqlite::params![format!("same-ts-{:04}", i), "2025-06-15T12:00:00Z"],
            )
            .unwrap();
    }

    let page_size = 3;
    let mut all_ids: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let page = store.list_summaries(page_size, cursor.as_deref()).unwrap();
        assert!(page.items.len() <= page_size);
        all_ids.extend(page.items.iter().map(|r| r.request_id.clone()));
        if let Some(c) = page.next_cursor {
            cursor = Some(c);
        } else {
            break;
        }
    }

    assert_eq!(
        all_ids.len(),
        5,
        "expected all 5 summaries with same timestamp"
    );

    // Verify ordering: DESC on (created_at, request_id), so highest ID first.
    let mut sorted = all_ids.clone();
    sorted.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(
        sorted[0], "same-ts-0004",
        "DESC order means highest ID first"
    );
}

#[test]
fn cursor_encode_decode_roundtrip() {
    let ts = "2025-06-15T12:34:56Z";
    let id = "abc-def-123";
    let encoded = encode_cursor(ts, id);

    assert!(!encoded.is_empty());

    let (dec_ts, dec_id) = decode_cursor(&encoded).expect("decode valid cursor");
    assert_eq!(dec_ts, ts);
    assert_eq!(dec_id, id);
}

#[test]
fn cursor_decode_malformed_returns_error() {
    // Empty string.
    let err = decode_cursor("").unwrap_err();
    match &err {
        LogStoreError::CursorMalformed(msg) => assert!(!msg.is_empty()),
        other => panic!("expected CursorMalformed, got: {:?}", other),
    }

    // Invalid base64 characters.
    let err = decode_cursor("v1:!!!invalid!!!").unwrap_err();
    match &err {
        LogStoreError::CursorMalformed(_) => {} // expected
        other => panic!("expected CursorMalformed, got: {:?}", other),
    }
}

#[test]
fn cursor_decode_unknown_version_returns_error() {
    let err = decode_cursor("v99:dGVzdA==").unwrap_err();
    match &err {
        LogStoreError::CursorMalformed(msg) => assert!(msg.contains("unknown cursor version")),
        other => panic!("expected CursorMalformed, got: {:?}", other),
    }
}

// ════════════════════════════════════
//  CASCADE CLEANUP TESTS
// ════════════════════════════════════

#[test]
fn cascade_cleanup_removes_by_cutoff() {
    let (store, _, _tmp) = open_store();

    // Create summaries + events for months Jan(1)..May(5).
    for i in 0..5u32 {
        let month = 1 + i;
        store
            .conn()
            .execute(
                "INSERT INTO summaries (request_id, state, created_at, terminal_at)\n\
                 VALUES (?, 'completed', ?, ?)",
                rusqlite::params![
                    format!("cleanup-summ-{:04}", i),
                    format!("2025-{:02}-15T00:00:00Z", month),
                    format!("2025-{:02}-15T00:00:00Z", month)
                ],
            )
            .unwrap();

        store.conn().execute(
            "INSERT INTO lifecycle_events (event_id, request_id, occurred_at, payload_json) VALUES (?, ?, ?, ?)",
            rusqlite::params![format!("ev-{:04}", i), format!("cleanup-summ-{:04}", i),
                format!("2025-{:02}-15T00:00:00Z", month), r#"{"type":"admitted"}"#],
        ).unwrap();

        store.conn().execute(
            "INSERT INTO artifact_pointers (artifact_id, request_id, occurred_at, kind) VALUES (?, ?, ?, 'log')",
            rusqlite::params![format!("art-{:04}", i), format!("cleanup-summ-{:04}", i),
                format!("2025-{:02}-15T00:00:00Z", month)],
        ).unwrap();

        store.conn().execute(
            "INSERT INTO proxy_records (attempt_id, request_id, occurred_at, target) VALUES (?, ?, ?, 'http://example.com')",
            rusqlite::params![format!("proxy-{:04}", i), format!("cleanup-summ-{:04}", i),
                format!("2025-{:02}-15T00:00:00Z", month)],
        ).unwrap();

        // Audit entries with request_id reference (SET NULL on summary delete).
        store.conn().execute(
            "INSERT INTO audit_entries (entry_id, request_id, occurred_at, actor, action) VALUES (?, ?, ?, 'system', 'create')",
            rusqlite::params![format!("audit-{:04}", i), format!("cleanup-summ-{:04}", i),
                format!("2025-{:02}-15T00:00:00Z", month)],
        ).unwrap();

        // Webhook deliveries with request_id reference (SET NULL on summary delete).
        store.conn().execute(
            "INSERT INTO webhook_deliveries (delivery_id, request_id, occurred_at, target_url, attempt_number) VALUES (?, ?, ?, 'https://hooks.example', 1)",
            rusqlite::params![format!("wh-{:04}", i), format!("cleanup-summ-{:04}", i),
                format!("2025-{:02}-15T00:00:00Z", month)],
        ).unwrap();
    }

    // Cleanup everything before March (Jan and Feb entries — indices 0,1).
    store
        .cascade_cleanup_before("2025-03-01T00:00:00Z")
        .unwrap();

    let ev_count = store.count_table("lifecycle_events").unwrap();
    assert_eq!(ev_count, 3, "only Mar/Apr/May events remain");

    let art_count = store.count_table("artifact_pointers").unwrap();
    assert_eq!(art_count, 3, "only Mar/Apr/May artifacts remain");

    let proxy_count = store.count_table("proxy_records").unwrap();
    assert_eq!(proxy_count, 3, "only Mar/Apr/May proxy records remain");

    // Terminal Jan/Feb summaries cascade with their owned detail; Mar/Apr/May survive.
    let summ_count = store.count_table("summaries").unwrap();
    assert_eq!(summ_count, 3, "only Mar/Apr/May summaries remain");

    let audit_count = store.count_table("audit_entries").unwrap();
    assert_eq!(audit_count, 3, "old audit rows follow their TTL policy");

    let wh_count = store.count_table("webhook_deliveries").unwrap();
    assert_eq!(wh_count, 3, "old webhook rows follow their TTL policy");

    // Verify Mar/Apr/May audit/webhook rows still reference their summaries.
    let non_null_audits: i64 = store.conn().query_row(
        "SELECT COUNT(*) FROM audit_entries WHERE occurred_at >= '2025-03-01T00:00:00Z' AND request_id IS NOT NULL",
        [], |row| row.get::<_, i64>(0),
    ).unwrap();
    assert_eq!(non_null_audits, 3);
}

// ════════════════════════════════════
//  FOREIGN KEY ENFORCEMENT TESTS
// ════════════════════════════════════

#[test]
fn foreign_keys_enforced() {
    let (store, _, _tmp) = open_store();

    // Attempt to insert a lifecycle_event for a nonexistent request_id.
    let result = store.insert_lifecycle_event(
        "nonexistent-request",
        "evt-orph",
        r#"{"type":"admitted"}"#,
        "2025-01-01T00:00:00Z",
    );

    // Should fail with a SQLite constraint error (FK violation).
    assert!(
        result.is_err(),
        "orphan insert should fail — foreign_keys=ON"
    );
}

// ════════════════════════════════════
//  EMPTY / SINGLE ITEM PAGINATION TESTS
// ════════════════════════════════════

#[test]
fn empty_table_pagination() {
    let (store, _, _tmp) = open_store();

    let page = store.list_summaries(10, None).unwrap();
    assert!(page.items.is_empty());
    assert!(page.next_cursor.is_none());

    // Also test lifecycle events and artifacts.
    let ev_page = store.list_lifecycle_events(10, None).unwrap();
    assert!(ev_page.items.is_empty());

    let art_page = store.list_artifact_pointers(10, None).unwrap();
    assert!(art_page.items.is_empty());
}

#[test]
fn single_item_pagination() {
    let (store, clock, _tmp) = open_store();

    store
        .insert_summary(
            "only-one",
            Some("llama3"),
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();

    let page = store.list_summaries(10, None).unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].request_id, "only-one");
    assert!(page.next_cursor.is_none());
}

// ════════════════════════════════════
//  SUMMARY STATUS COUNTS TEST
// ════════════════════════════════════

#[test]
fn summary_status_counts() {
    let (store, clock, _tmp) = open_store();

    store
        .insert_summary(
            "s-active-1",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();

    // Insert + terminal update.
    store
        .insert_summary(
            "s-completed-1",
            None,
            Some("route-a"),
            Some("provider-x"),
            Some("engine-y"),
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();
    let payload = r#"{"type":"completed","status_code":200}"#;
    store
        .write_terminal_event(
            "s-completed-1",
            "evt-c1",
            payload,
            "completed",
            &clock.now(),
        )
        .unwrap();

    // Failed terminal.
    store
        .insert_summary(
            "s-failed-1",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();
    let failed_payload = r#"{"type":"failed","error":"timeout"}"#;
    store
        .write_terminal_event(
            "s-failed-1",
            "evt-f1",
            failed_payload,
            "failed",
            &clock.now(),
        )
        .unwrap();

    let counts = store.count_summaries_by_status().unwrap();
    assert_eq!(counts.len(), 3); // active, completed, failed states

    // Verify specific counts.
    for (state, count) in &counts {
        match state.as_str() {
            "active" => assert_eq!(*count, 1),
            "completed" => assert_eq!(*count, 1),
            "failed" => assert_eq!(*count, 1),
            _ => panic!("unexpected state: {}", state),
        }
    }
}

// ════════════════════════════════════
//  HAPPY PATH INSERT + COUNT TESTS
// ════════════════════════════════════

#[test]
fn artifact_insert_and_count() {
    let (store, clock, _tmp) = open_store();

    store
        .insert_summary(
            "req-1",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();
    store
        .insert_artifact_pointer(
            "art-1",
            "req-1",
            &clock.now(),
            "log",
            Some(r#"{"size": 42}"#),
        )
        .expect("insert artifact");

    assert_eq!(store.count_table("artifact_pointers").unwrap(), 1);

    // Duplicate PK should fail with AlreadyExists.
    let err = store
        .insert_artifact_pointer("art-1", "req-1", &clock.now(), "log", None)
        .unwrap_err();
    assert!(matches!(err, LogStoreError::AlreadyExists { .. }));
}

#[test]
fn proxy_record_insert_and_count() {
    let (store, clock, _tmp) = open_store();

    store
        .insert_summary(
            "req-1",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();
    store
        .insert_proxy_record(
            "att-1",
            "req-1",
            &clock.now(),
            "http://target.api",
            Some("provider-x"),
            Some("engine-y"),
            Some(&clock.now()),
            Some(&clock.now()),
            Some(200),
            None,
        )
        .expect("insert proxy record");

    assert_eq!(store.count_table("proxy_records").unwrap(), 1);

    // Duplicate PK fails.
    let err = store
        .insert_proxy_record(
            "att-1",
            "req-1",
            &clock.now(),
            "http://other.api",
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
    assert!(matches!(err, LogStoreError::AlreadyExists { .. }));
}

#[test]
fn audit_entry_insert_and_count() {
    let (store, clock, _tmp) = open_store();

    // With request_id.
    store
        .insert_summary(
            "req-1",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();
    store
        .insert_audit_entry(
            "aud-1",
            Some("req-1"),
            &clock.now(),
            "user-alice",
            "model_added",
            Some(r#"{"model":"llama3"}"#),
        )
        .expect("insert audit with request_id");

    // Without request_id (standalone).
    store
        .insert_audit_entry("aud-2", None, &clock.now(), "system", "startup", None)
        .expect("insert audit without request_id");

    assert_eq!(store.count_table("audit_entries").unwrap(), 2);

    // Duplicate PK fails.
    let err = store
        .insert_audit_entry(
            "aud-1",
            Some("req-1"),
            &clock.now(),
            "user-bob",
            "other_action",
            None,
        )
        .unwrap_err();
    assert!(matches!(err, LogStoreError::AlreadyExists { .. }));

    // UNIQUE(request_id, entry_id) — same request_id + different entry_id should work.
    store
        .insert_audit_entry(
            "aud-3",
            Some("req-1"),
            &clock.now(),
            "user-carol",
            "action_3",
            None,
        )
        .expect("different entry_id with same request_id is fine");

    // UNIQUE(request_id, entry_id) — different request + different entry should work.
    store
        .insert_audit_entry(
            "aud-5",
            Some("req-1"),
            &clock.now(),
            "user-carol",
            "action_3",
            None,
        )
        .expect("different entry and request is fine");

    // Different entry_id always works (entry_id is PK).
    store
        .insert_audit_entry(
            "aud-4",
            Some("req-1"),
            &clock.now(),
            "user-dave",
            "action_4",
            None,
        )
        .expect("another unique entry_id with same request_id is fine");

    assert_eq!(store.count_table("audit_entries").unwrap(), 5);
}

#[test]
fn webhook_delivery_insert_and_count() {
    let (store, clock, _tmp) = open_store();

    store
        .insert_summary(
            "req-1",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();
    store
        .insert_webhook_delivery("wh-1", Some("req-1"), &clock.now(), 1, Some(200))
        .expect("insert webhook delivery");

    assert_eq!(store.count_table("webhook_deliveries").unwrap(), 1);

    // Duplicate PK fails.
    let err = store
        .insert_webhook_delivery("wh-1", Some("req-1"), &clock.now(), 2, None)
        .unwrap_err();
    assert!(matches!(err, LogStoreError::AlreadyExists { .. }));

    // Without request_id.
    store
        .insert_webhook_delivery("wh-2", None, &clock.now(), 1, Some(500))
        .expect("insert webhook without request_id");

    assert_eq!(store.count_table("webhook_deliveries").unwrap(), 2);
}

#[test]
fn webhook_delivery_state_machine_is_idempotent_fenced_and_restart_safe() {
    let (store, clock, _tmp) = open_store();
    let created_at = "2026-08-04T12:00:00Z";
    seed_terminal_webhook_request(&store, created_at);
    assert_enqueue_requires_terminal_event(&store, created_at);
    enqueue_webhook_delivery_idempotently(&store, created_at);
    let first = claim_initial_webhook_delivery(&store);
    let second = retry_and_claim_webhook_delivery(&store, first.claim_generation);
    let manual = dead_letter_and_claim_manual_retry(&store, second.claim_generation);
    complete_webhook_delivery_with_fencing(
        &store,
        manual.claim_generation,
        second.claim_generation,
    );
    assert_webhook_delivery_is_private_and_restart_safe(&store, clock);
}

fn seed_terminal_webhook_request(store: &LogStore, created_at: &str) {
    store
        .insert_summary(
            "request-terminal",
            None,
            None,
            None,
            None,
            created_at,
            None,
            None,
            None,
        )
        .expect("insert terminal summary owner");
}

fn assert_enqueue_requires_terminal_event(store: &LogStore, created_at: &str) {
    assert!(matches!(
        store.enqueue_webhook_delivery("before-terminal", "request-terminal", created_at, 2),
        Err(LogStoreError::InvalidQuery(message)) if message.contains("durable terminal event")
    ));
    store
        .write_terminal_event(
            "request-terminal",
            "event-terminal",
            r#"{"type":"completed"}"#,
            "completed",
            created_at,
        )
        .expect("commit terminal before webhook enqueue");
}

fn enqueue_webhook_delivery_idempotently(store: &LogStore, created_at: &str) {
    let created = store
        .enqueue_webhook_delivery("delivery-terminal", "request-terminal", created_at, 2)
        .expect("enqueue terminal webhook");
    assert!(matches!(
        created,
        WebhookDeliveryInsertOutcome::Created(WebhookDeliveryRecord {
            state: WebhookDeliveryState::Pending,
            attempt_number: 0,
            max_attempts: 2,
            ..
        })
    ));
    assert!(matches!(
        store
            .enqueue_webhook_delivery("delivery-terminal", "request-terminal", created_at, 2)
            .expect("idempotent enqueue"),
        WebhookDeliveryInsertOutcome::Existing(_)
    ));
}

fn claim_initial_webhook_delivery(store: &LogStore) -> WebhookDeliveryRecord {
    let first = store
        .claim_next_webhook_delivery("2026-08-04T12:00:01Z", "2026-08-04T12:01:01Z")
        .expect("claim first attempt")
        .expect("pending delivery is claimable");
    assert_eq!(first.state, WebhookDeliveryState::InFlight);
    assert_eq!(first.attempt_number, 1);
    assert_eq!(first.claim_generation, 1);
    assert!(
        store
            .claim_next_webhook_delivery("2026-08-04T12:00:02Z", "2026-08-04T12:01:02Z")
            .expect("second claim")
            .is_none(),
        "an active lease excludes duplicate worker wakeups"
    );
    first
}

fn retry_and_claim_webhook_delivery(
    store: &LogStore,
    claim_generation: u64,
) -> WebhookDeliveryRecord {
    assert_eq!(
        store
            .retry_or_dead_letter_webhook_delivery(
                "delivery-terminal",
                claim_generation,
                "2026-08-04T12:00:03Z",
                "2026-08-04T12:00:10Z",
                WebhookDeliveryErrorCode::Timeout,
            )
            .expect("schedule retry"),
        Some(WebhookRetryOutcome::RetryScheduled)
    );
    assert!(
        store
            .claim_next_webhook_delivery("2026-08-04T12:00:09Z", "2026-08-04T12:01:09Z")
            .expect("claim before retry")
            .is_none()
    );

    let second = store
        .claim_next_webhook_delivery("2026-08-04T12:00:10Z", "2026-08-04T12:01:10Z")
        .expect("claim retry")
        .expect("retry is eligible");
    assert_eq!(second.attempt_number, 2);
    second
}

fn dead_letter_and_claim_manual_retry(
    store: &LogStore,
    claim_generation: u64,
) -> WebhookDeliveryRecord {
    assert_eq!(
        store
            .retry_or_dead_letter_webhook_delivery(
                "delivery-terminal",
                claim_generation,
                "2026-08-04T12:00:11Z",
                "2026-08-04T12:00:20Z",
                WebhookDeliveryErrorCode::Http5xx,
            )
            .expect("dead-letter exhausted delivery"),
        Some(WebhookRetryOutcome::DeadLettered)
    );
    assert!(
        store
            .manually_retry_webhook_delivery("delivery-terminal", "2026-08-04T12:00:12Z")
            .expect("manual retry dead letter")
    );

    let manual = store
        .claim_next_webhook_delivery("2026-08-04T12:00:12Z", "2026-08-04T12:01:12Z")
        .expect("claim manual retry")
        .expect("manual retry is eligible");
    assert_eq!(manual.state, WebhookDeliveryState::InFlight);
    assert_eq!(manual.attempt_number, 1);
    assert_eq!(manual.claim_generation, 3);
    manual
}

fn complete_webhook_delivery_with_fencing(
    store: &LogStore,
    winning_claim_generation: u64,
    stale_claim_generation: u64,
) {
    assert!(
        store
            .complete_webhook_delivery(
                "delivery-terminal",
                winning_claim_generation,
                "2026-08-04T12:00:13Z",
                204,
            )
            .expect("complete fenced delivery")
    );
    assert!(
        !store
            .complete_webhook_delivery(
                "delivery-terminal",
                stale_claim_generation,
                "2026-08-04T12:00:14Z",
                204,
            )
            .expect("stale completion is harmless"),
        "a displaced worker cannot overwrite the fenced completion"
    );
}

fn assert_webhook_delivery_is_private_and_restart_safe(
    store: &LogStore,
    clock: Arc<dyn ClockTrait>,
) {
    let record = store
        .webhook_delivery("delivery-terminal")
        .expect("load delivery")
        .expect("delivery persisted");
    assert_eq!(record.state, WebhookDeliveryState::Succeeded);
    assert_eq!(record.status_code, Some(204));
    assert_eq!(record.last_error_code, None);
    let (target, body, error): (String, Option<String>, Option<String>) = store
        .conn()
        .query_row(
            "SELECT target_url, response_body, error_msg FROM webhook_deliveries WHERE delivery_id = ?",
            ["delivery-terminal"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("inspect privacy-safe storage");
    assert_eq!(target, "configured_webhook");
    assert!(body.is_none());
    assert!(error.is_none());

    let reopened = store.reopen(clock).expect("reopen webhook database");
    assert_eq!(
        reopened
            .webhook_delivery("delivery-terminal")
            .expect("load after restart")
            .expect("record after restart")
            .state,
        WebhookDeliveryState::Succeeded
    );
}

#[test]
fn cleanup_run_insert_and_count() {
    let (store, clock, _tmp) = open_store();

    store
        .insert_cleanup_run(
            "cr-1",
            &clock.now(),
            "daily-cleanup",
            "2025-01-01T00:00:00Z",
            42,
            Some(150),
        )
        .expect("insert cleanup run");

    assert_eq!(store.count_table("cleanup_runs").unwrap(), 1);

    // Duplicate PK fails.
    let err = store
        .insert_cleanup_run(
            "cr-1",
            &clock.now(),
            "other-policy",
            "2025-02-01T00:00:00Z",
            10,
            None,
        )
        .unwrap_err();
    assert!(matches!(err, LogStoreError::AlreadyExists { .. }));
}

// ════════════════════════════════════
//  HAS_TERMINAL_EVENT TESTS
// ════════════════════════════════════

#[test]
fn has_terminal_event_detects_correctly() {
    let (store, clock, _tmp) = open_store();

    store
        .insert_summary(
            "term-s1",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();
    assert!(
        !store.has_terminal_event("term-s1").unwrap(),
        "no terminal yet"
    );

    let payload = r#"{"type":"completed","status_code":200}"#;
    store
        .insert_lifecycle_event("term-s1", "evt-term", payload, &clock.now())
        .unwrap();
    assert!(
        store.has_terminal_event("term-s1").unwrap(),
        "terminal exists now"
    );

    // Non-terminal events should not trigger has_terminal.
    let non_term_payload = r#"{"type":"admitted","model":"llama3"}"#;
    store
        .insert_lifecycle_event("term-s1", "evt-admit", non_term_payload, &clock.now())
        .unwrap();
    assert!(
        store.has_terminal_event("term-s1").unwrap(),
        "still has terminal despite new non-terminal event"
    );

    // New summary without any events.
    store
        .insert_summary(
            "term-s2",
            None,
            None,
            None,
            None,
            &clock.now(),
            None,
            None,
            None,
        )
        .unwrap();
    assert!(!store.has_terminal_event("term-s2").unwrap());
}

// ════════════════════════════════════
//  LIST EVENTS FOR SUMMARY TESTS
// ════════════════════════════════════

#[test]
fn list_events_for_summary_ordered_chronologically() {
    let (store, _, _tmp) = open_store();

    store
        .insert_summary(
            "req-1",
            None,
            None,
            None,
            None,
            "2025-01-01T00:00:00Z",
            None,
            None,
            None,
        )
        .unwrap();

    // Insert events in reverse chronological order.
    store.conn().execute(
        "INSERT INTO lifecycle_events (event_id, request_id, occurred_at, payload_json) VALUES (?, 'req-1', ?, ?)",
        rusqlite::params!["evt-c", "2025-03-01T00:00:00Z", r#"{"type":"completed"}"#],
    ).unwrap();
    store.conn().execute(
        "INSERT INTO lifecycle_events (event_id, request_id, occurred_at, payload_json) VALUES (?, 'req-1', ?, ?)",
        rusqlite::params!["evt-a", "2025-01-01T00:00:00Z", r#"{"type":"admitted"}"#],
    ).unwrap();
    store.conn().execute(
        "INSERT INTO lifecycle_events (event_id, request_id, occurred_at, payload_json) VALUES (?, 'req-1', ?, ?)",
        rusqlite::params!["evt-b", "2025-02-01T00:00:00Z", r#"{"type":"stream_started"}"#],
    ).unwrap();

    let events = store.list_events_for_summary("req-1").unwrap();
    assert_eq!(events.len(), 3);
    // Should be ordered ASC by occurred_at.
    assert_eq!(events[0].event_id, "evt-a");
    assert_eq!(events[1].event_id, "evt-b");
    assert_eq!(events[2].event_id, "evt-c");
}

#[test]
fn max_row_prune_is_deterministic_preserves_active_and_returns_owned_artifact_pointers() {
    let (store, _, _tmp) = open_store();
    for request_id in ["active", "tie-a", "tie-b", "newest"] {
        store
            .insert_summary(
                request_id,
                None,
                None,
                None,
                None,
                "2025-01-01T00:00:00Z",
                None,
                None,
                None,
            )
            .expect("insert summary");
    }
    for request_id in ["tie-a", "tie-b"] {
        store
            .write_terminal_event(
                request_id,
                &format!("event-{request_id}"),
                r#"{"type":"completed"}"#,
                "completed",
                "2025-02-01T00:00:00Z",
            )
            .expect("write terminal event");
    }
    store
        .write_terminal_event(
            "newest",
            "event-newest",
            r#"{"type":"completed"}"#,
            "completed",
            "2025-03-01T00:00:00Z",
        )
        .expect("write newest terminal event");
    store
        .insert_artifact_pointer(
            "artifact-tie-a",
            "tie-a",
            "2025-02-01T00:00:00Z",
            "request_body",
            None,
        )
        .expect("insert pointer");

    let (deleted, pointers) = store
        .cascade_prune_terminal_summaries_to_max_rows(2)
        .expect("prune terminal history");
    assert_eq!(deleted, 3, "summary, terminal event, and pointer");
    assert_eq!(
        pointers,
        vec![super::repositories::CascadeArtifactPointer {
            artifact_id: "artifact-tie-a".to_string(),
            request_id: "tie-a".to_string(),
        }]
    );
    assert!(store.get_summary("active").unwrap().is_some());
    assert!(store.get_summary("tie-a").unwrap().is_none());
    assert!(store.get_summary("tie-b").unwrap().is_some());
    assert!(store.get_summary("newest").unwrap().is_some());
    assert_eq!(store.count_table("artifact_pointers").unwrap(), 0);
}

#[test]
fn max_row_prune_is_idempotent_after_retention_is_satisfied() {
    let (store, _, _tmp) = open_store();
    store
        .insert_summary(
            "completed",
            None,
            None,
            None,
            None,
            "2025-01-01T00:00:00Z",
            None,
            None,
            None,
        )
        .expect("insert summary");
    store
        .write_terminal_event(
            "completed",
            "event-completed",
            r#"{"type":"completed"}"#,
            "completed",
            "2025-01-01T00:00:01Z",
        )
        .expect("write terminal event");

    assert_eq!(
        store
            .cascade_prune_terminal_summaries_to_max_rows(1)
            .expect("initial no-op"),
        (0, Vec::new())
    );
}

#[test]
fn max_row_prune_survives_store_restart_with_the_retained_summaries_intact() {
    let (store, clock, tmp) = open_store();
    for (request_id, occurred_at) in [
        ("oldest", "2025-01-01T00:00:01Z"),
        ("newer", "2025-01-01T00:00:02Z"),
    ] {
        store
            .insert_summary(
                request_id,
                None,
                None,
                None,
                None,
                occurred_at,
                None,
                None,
                None,
            )
            .expect("insert summary");
        store
            .write_terminal_event(
                request_id,
                &format!("event-{request_id}"),
                r#"{"type":"completed"}"#,
                "completed",
                occurred_at,
            )
            .expect("write terminal");
    }
    store
        .cascade_prune_terminal_summaries_to_max_rows(1)
        .expect("prune before restart");
    drop(store);

    let reopened = LogStore::reopen_at(tmp.path(), clock).expect("reopen store");
    assert!(reopened.get_summary("oldest").unwrap().is_none());
    assert!(reopened.get_summary("newer").unwrap().is_some());
    assert_eq!(
        reopened
            .cascade_prune_terminal_summaries_to_max_rows(1)
            .expect("idempotent after restart"),
        (0, Vec::new())
    );
}

#[test]
fn retention_policy_uses_summary_ownership_and_per_table_ttl_after_reopen() {
    let (store, clock, tmp) = open_store();
    let old = "2025-01-01T00:00:00Z";
    let fresh = "2025-03-01T00:00:00Z";
    let cutoff = "2025-02-01T00:00:00Z";

    for request_id in ["expired-terminal", "retained-terminal", "active"] {
        store
            .insert_summary(request_id, None, None, None, None, old, None, None, None)
            .expect("insert summary");
    }
    store
        .write_terminal_event(
            "expired-terminal",
            "expired-terminal-event",
            r#"{"type":"completed"}"#,
            "completed",
            old,
        )
        .expect("write expired terminal");
    store
        .write_terminal_event(
            "retained-terminal",
            "retained-terminal-event",
            r#"{"type":"completed"}"#,
            "completed",
            fresh,
        )
        .expect("write retained terminal");
    store
        .conn()
        .execute(
            "INSERT INTO lifecycle_events (event_id, request_id, occurred_at, payload_json)\n\
             VALUES ('retained-old-event', 'retained-terminal', ?1, '{\"type\":\"chunk\"}')",
            rusqlite::params![old],
        )
        .expect("insert retained old event");
    store
        .insert_proxy_record(
            "expired-proxy",
            "expired-terminal",
            old,
            "target",
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("insert expired proxy");
    store
        .insert_proxy_record(
            "retained-proxy",
            "retained-terminal",
            old,
            "target",
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("insert retained proxy");
    store
        .insert_artifact_pointer("expired-artifact", "expired-terminal", old, "request", None)
        .expect("insert expired pointer");
    store
        .insert_artifact_pointer("active-artifact", "active", old, "request", None)
        .expect("insert active pointer");
    store
        .insert_audit_entry("old-audit", None, old, "operator", "test", None)
        .expect("insert audit");
    store
        .insert_webhook_delivery("old-webhook", None, old, 1, None)
        .expect("insert webhook");
    store
        .insert_cleanup_run("old-cleanup", old, "test", cutoff, 0, None)
        .expect("insert cleanup run");

    let result = store
        .apply_retention_policy(cutoff, 100)
        .expect("apply retention");
    assert_eq!(result.ttl_deleted_count, 11);
    assert_eq!(result.max_rows_deleted_count, 0);
    assert_eq!(
        result.artifact_pointers,
        vec![super::repositories::CascadeArtifactPointer {
            artifact_id: "expired-artifact".to_string(),
            request_id: "expired-terminal".to_string(),
        }]
    );
    assert!(store.get_summary("expired-terminal").unwrap().is_none());
    assert!(store.get_summary("retained-terminal").unwrap().is_none());
    assert!(store.get_summary("active").unwrap().is_some());
    assert!(
        store
            .get_artifact_pointer("active-artifact")
            .unwrap()
            .is_some()
    );
    assert!(result.table_results.iter().all(|entry| {
        matches!(
            entry.table,
            super::repositories::RetentionTable::Summaries
                | super::repositories::RetentionTable::LifecycleEvents
                | super::repositories::RetentionTable::ArtifactPointers
                | super::repositories::RetentionTable::ProxyRecords
                | super::repositories::RetentionTable::AuditEntries
                | super::repositories::RetentionTable::WebhookDeliveries
                | super::repositories::RetentionTable::CleanupRuns
        )
    }));
    assert_eq!(store.count_table("proxy_records").unwrap(), 0);
    assert_eq!(store.count_table("audit_entries").unwrap(), 0);
    assert_eq!(store.count_table("webhook_deliveries").unwrap(), 0);
    assert_eq!(store.count_table("cleanup_runs").unwrap(), 0);

    drop(store);
    let reopened = LogStore::reopen_at(tmp.path(), clock).expect("reopen store");
    assert_eq!(
        reopened
            .apply_retention_policy(cutoff, 100)
            .expect("idempotent retention after reopen"),
        super::repositories::RetentionCleanupResult {
            ttl_deleted_count: 0,
            max_rows_deleted_count: 0,
            artifact_pointers: Vec::new(),
            table_results: super::repositories::RetentionTable::ALL
                .into_iter()
                .map(|table| super::repositories::RetentionTableResult {
                    table,
                    ttl_deleted_count: 0,
                    max_rows_deleted_count: 0,
                })
                .collect(),
        }
    );
}

#[test]
fn webhook_dead_letter_retention_uses_transition_time_and_preserves_generic_policy() {
    use super::repositories::{RetentionPolicy, RetentionTable};

    let (store, clock, tmp) = open_store();
    let generic_cutoff = "2025-01-01T00:00:00Z";
    let generic_cleanup_cutoff = "2025-03-01T00:00:00Z";
    let dead_letter_cutoff = "2025-04-01T00:00:00Z";
    let occurred_at = "2025-02-01T00:00:00Z";
    let insert = |delivery_id: &str, state: &str, updated_at: &str| {
        store
            .conn()
            .execute(
                r#"
                INSERT INTO webhook_deliveries
                    (delivery_id, request_id, occurred_at, target_url, attempt_number, response_body, error_msg,
                     state, created_at, updated_at, next_attempt_at, lease_expires_at, claim_generation, max_attempts, last_error_code)
                VALUES (?1, NULL, ?2, 'configured_webhook', 1, NULL, NULL, ?3, ?2, ?4, NULL, NULL, 0, 3, NULL)
                "#,
                rusqlite::params![delivery_id, occurred_at, state, updated_at],
            )
            .expect("insert webhook delivery");
    };
    insert("expired-dead-letter", "dead_letter", "2025-03-31T23:59:59Z");
    insert("fresh-dead-letter", "dead_letter", "2025-04-01T00:00:00Z");
    for (delivery_id, state) in [
        ("pending-delivery", "pending"),
        ("retry-delivery", "retry"),
        ("in-flight-delivery", "in_flight"),
        ("manual-retry-delivery", "manual_retry"),
        ("succeeded-delivery", "succeeded"),
    ] {
        insert(delivery_id, state, "2025-03-31T23:59:59Z");
    }

    let policy = RetentionPolicy::uniform(generic_cutoff, 100)
        .expect("generic retention policy")
        .with_webhook_dead_letter_cutoff(dead_letter_cutoff);
    let result = store
        .apply_retention_policy_map(&policy)
        .expect("dead-letter retention");
    assert_eq!(result.ttl_deleted_count, 1);
    assert_eq!(
        result
            .table_results
            .iter()
            .find(|entry| entry.table == RetentionTable::WebhookDeliveries)
            .expect("webhook table result")
            .ttl_deleted_count,
        1
    );
    assert!(
        store
            .webhook_delivery("expired-dead-letter")
            .expect("expired lookup")
            .is_none()
    );
    assert_eq!(
        store
            .webhook_delivery("fresh-dead-letter")
            .expect("fresh lookup")
            .expect("fresh dead letter retained")
            .state,
        WebhookDeliveryState::DeadLetter
    );
    for (delivery_id, expected_state) in [
        ("pending-delivery", WebhookDeliveryState::Pending),
        ("retry-delivery", WebhookDeliveryState::Retry),
        ("in-flight-delivery", WebhookDeliveryState::InFlight),
        ("manual-retry-delivery", WebhookDeliveryState::ManualRetry),
        ("succeeded-delivery", WebhookDeliveryState::Succeeded),
    ] {
        assert_eq!(
            store
                .webhook_delivery(delivery_id)
                .expect("non-dead-letter lookup")
                .expect("non-dead-letter retained")
                .state,
            expected_state,
            "{delivery_id} must not use the dead-letter window"
        );
    }

    drop(store);
    let reopened = LogStore::reopen_at(tmp.path(), clock).expect("reopen store");
    assert_eq!(
        reopened
            .apply_retention_policy_map(&policy)
            .expect("idempotent dead-letter retention")
            .ttl_deleted_count,
        0
    );
    let generic_result = reopened
        .apply_retention_policy_map(
            &RetentionPolicy::uniform(generic_cleanup_cutoff, 100)
                .expect("generic retention policy"),
        )
        .expect("generic webhook retention remains available");
    assert_eq!(generic_result.ttl_deleted_count, 6);
    assert_eq!(
        reopened
            .count_table("webhook_deliveries")
            .expect("count webhooks"),
        0
    );
}

#[test]
fn per_table_retention_caps_are_deterministic_owner_safe_and_restart_safe() {
    use super::repositories::{RetentionPolicy, RetentionTable, RetentionTablePolicy};

    let (store, clock, tmp) = open_store();
    let fresh = "2025-03-01T00:00:00Z";
    for request_id in ["active", "tie-a", "tie-b"] {
        store
            .insert_summary(request_id, None, None, None, None, fresh, None, None, None)
            .expect("insert summary");
    }
    for request_id in ["tie-a", "tie-b"] {
        store
            .write_terminal_event(
                request_id,
                &format!("terminal-{request_id}"),
                r#"{"type":"completed"}"#,
                "completed",
                fresh,
            )
            .expect("terminal event");
        store
            .insert_artifact_pointer(
                &format!("artifact-{request_id}"),
                request_id,
                fresh,
                "request",
                None,
            )
            .expect("owned pointer");
        store
            .insert_proxy_record(
                &format!("proxy-{request_id}"),
                request_id,
                fresh,
                "target",
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("owned proxy");
    }
    store
        .insert_artifact_pointer("artifact-active", "active", fresh, "request", None)
        .expect("active pointer");
    for index in 0..2 {
        let id = index.to_string();
        store
            .insert_audit_entry(
                &format!("audit-{id}"),
                None,
                fresh,
                "operator",
                "retention-test",
                None,
            )
            .expect("audit");
        store
            .insert_webhook_delivery(&format!("webhook-{id}"), None, fresh, 1, None)
            .expect("webhook");
        store
            .insert_cleanup_run(&format!("run-{id}"), fresh, "test", fresh, 0, None)
            .expect("cleanup receipt");
    }

    let table_policies = RetentionTable::ALL
        .into_iter()
        .map(|table| {
            (
                table,
                RetentionTablePolicy {
                    cutoff_occurred_at: "2025-01-01T00:00:00Z".to_string(),
                    max_rows: if table == RetentionTable::Summaries {
                        2
                    } else {
                        1
                    },
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let result = store
        .apply_retention_policy_map(&RetentionPolicy::new(table_policies).expect("complete map"))
        .expect("per-table retention");

    // Same-time owner selection is deterministic by ID: the pointer cap picks
    // tie-a first, while active rows and their artifact remain protected.
    assert!(store.get_summary("tie-a").unwrap().is_none());
    assert!(store.get_summary("active").unwrap().is_some());
    assert!(
        store
            .get_artifact_pointer("artifact-active")
            .unwrap()
            .is_some()
    );
    assert_eq!(store.count_table("audit_entries").unwrap(), 1);
    assert_eq!(store.count_table("webhook_deliveries").unwrap(), 1);
    assert_eq!(store.count_table("cleanup_runs").unwrap(), 1);
    assert!(
        result
            .table_results
            .iter()
            .all(|entry| RetentionTable::ALL.contains(&entry.table))
    );
    assert!(
        result
            .table_results
            .iter()
            .any(|entry| entry.table == RetentionTable::ArtifactPointers
                && entry.max_rows_deleted_count > 0)
    );

    drop(store);
    let reopened = LogStore::reopen_at(tmp.path(), clock).expect("reopen");
    assert!(reopened.get_summary("active").unwrap().is_some());
    assert!(
        reopened
            .get_artifact_pointer("artifact-active")
            .unwrap()
            .is_some()
    );
    assert!(reopened.count_table("audit_entries").unwrap() <= 1);
    assert!(reopened.count_table("webhook_deliveries").unwrap() <= 1);
    assert!(reopened.count_table("cleanup_runs").unwrap() <= 1);
}

#[test]
fn per_table_retention_rejects_missing_or_unbounded_table_policies() {
    use super::repositories::{RetentionPolicy, RetentionTable, RetentionTablePolicy};

    let missing = BTreeMap::new();
    assert!(RetentionPolicy::new(missing).is_err());
    let zero = RetentionTable::ALL
        .into_iter()
        .map(|table| {
            (
                table,
                RetentionTablePolicy {
                    cutoff_occurred_at: "2025-01-01T00:00:00Z".to_string(),
                    max_rows: if table == RetentionTable::AuditEntries {
                        0
                    } else {
                        1
                    },
                },
            )
        })
        .collect();
    assert!(RetentionPolicy::new(zero).is_err());
}
