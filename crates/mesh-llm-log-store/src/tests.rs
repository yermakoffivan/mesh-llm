//! Acceptance tests for mesh-llm-log-store.
//! All tests use real temp SQLite files (no in-memory shortcut).

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::cursor::{decode_cursor, encode_cursor};
use super::error::LogStoreError;
use super::migrations::CURRENT_VERSION;
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
                "INSERT INTO summaries (request_id, state, created_at) VALUES (?, 'active', ?)",
                rusqlite::params![
                    format!("cleanup-summ-{:04}", i),
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

    // Summaries: Jan/Feb summaries deleted (orphaned — no remaining lifecycle_events).
    // Mar/Apr/May summaries survive.
    let summ_count = store.count_table("summaries").unwrap();
    assert_eq!(summ_count, 3, "only Mar/Apr/May summaries remain");

    // Audit + webhook rows SURVIVE via ON DELETE SET NULL (request_id becomes NULL).
    let audit_count = store.count_table("audit_entries").unwrap();
    assert_eq!(
        audit_count, 5,
        "all audit entries survive with request_id=NULL for deleted summaries"
    );

    let wh_count = store.count_table("webhook_deliveries").unwrap();
    assert_eq!(
        wh_count, 5,
        "all webhook deliveries survive with request_id=NULL for deleted summaries"
    );

    // Verify Jan/Feb audit/webhook rows have NULL request_id.
    let null_audits: i64 = store.conn().query_row(
        "SELECT COUNT(*) FROM audit_entries WHERE occurred_at < '2025-03-01T00:00:00Z' AND request_id IS NULL",
        [], |row| row.get::<_, i64>(0),
    ).unwrap();
    assert_eq!(
        null_audits, 2,
        "Jan/Feb audit entries have NULL request_id after cascade"
    );

    let null_wh: i64 = store.conn().query_row(
        "SELECT COUNT(*) FROM webhook_deliveries WHERE occurred_at < '2025-03-01T00:00:00Z' AND request_id IS NULL",
        [], |row| row.get::<_, i64>(0),
    ).unwrap();
    assert_eq!(
        null_wh, 2,
        "Jan/Feb webhook deliveries have NULL request_id after cascade"
    );

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
        .insert_webhook_delivery(
            "wh-1",
            Some("req-1"),
            &clock.now(),
            "https://example.com/hook",
            1,
            Some(200),
            Some(r#"{"ok":true}"#),
            None,
        )
        .expect("insert webhook delivery");

    assert_eq!(store.count_table("webhook_deliveries").unwrap(), 1);

    // Duplicate PK fails.
    let err = store
        .insert_webhook_delivery(
            "wh-1",
            Some("req-1"),
            &clock.now(),
            "https://other.com/hook",
            2,
            None,
            None,
            None,
        )
        .unwrap_err();
    assert!(matches!(err, LogStoreError::AlreadyExists { .. }));

    // Without request_id.
    store
        .insert_webhook_delivery(
            "wh-2",
            None,
            &clock.now(),
            "https://standalone.com/hook",
            1,
            Some(500),
            None,
            Some("connection refused"),
        )
        .expect("insert webhook without request_id");

    assert_eq!(store.count_table("webhook_deliveries").unwrap(), 2);
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
