use std::sync::Arc;

use crate::{
    Clock, LogStore, LogStoreError, MAX_QUERY_LIMIT, PageQuery, ProxyQuery, QuerySort,
    RequestOutcome, RequestQuery, encode_cursor,
};

#[derive(Debug)]
struct FixedClock;

impl Clock for FixedClock {
    fn now(&self) -> String {
        "2026-08-03T00:00:00Z".to_string()
    }
}

fn open_store() -> (tempfile::TempDir, LogStore) {
    let root = tempfile::tempdir().expect("create query store root");
    let store = LogStore::open(root.path(), Arc::new(FixedClock)).expect("open query store");
    (root, store)
}

fn request_query() -> RequestQuery {
    RequestQuery {
        limit: 10,
        cursor: None,
        from: None,
        to: None,
        route: None,
        model: None,
        provider: None,
        engine: None,
        status_code: None,
        outcome: None,
        sort: QuerySort::Descending,
    }
}

#[test]
fn request_query_applies_all_filters_and_normalizes_time_bounds() {
    let (_root, store) = open_store();
    store
        .insert_summary(
            "matching-request",
            Some("model-a"),
            Some("chat"),
            Some("provider-a"),
            Some("engine-a"),
            "2026-08-03T00:00:05Z",
            None,
            None,
            None,
        )
        .expect("insert matching summary");
    store
        .write_terminal_event(
            "matching-request",
            "matching-event",
            r#"{"type":"completed","status_code":201}"#,
            "completed",
            "2026-08-03T00:00:06Z",
        )
        .expect("complete matching summary");
    store
        .conn()
        .execute(
            "UPDATE summaries SET status_code = 201 WHERE request_id = ?",
            ["matching-request"],
        )
        .expect("set status code");
    store
        .insert_summary(
            "non-matching-request",
            Some("model-b"),
            Some("completion"),
            Some("provider-b"),
            Some("engine-b"),
            "2026-08-03T00:00:05Z",
            None,
            None,
            None,
        )
        .expect("insert non-matching summary");

    let page = store
        .query_requests(&RequestQuery {
            limit: 10,
            cursor: None,
            from: Some("2026-08-02T20:00:00-04:00".to_string()),
            to: Some("2026-08-03T00:01:00Z".to_string()),
            route: Some("chat".to_string()),
            model: Some("model-a".to_string()),
            provider: Some("provider-a".to_string()),
            engine: Some("engine-a".to_string()),
            status_code: Some(201),
            outcome: Some(RequestOutcome::Completed),
            sort: QuerySort::Descending,
        })
        .expect("query requests");

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].request_id, "matching-request");
}

#[test]
fn durable_query_rejects_unbounded_or_invalid_inputs() {
    let (_root, store) = open_store();
    let mut query = request_query();
    query.limit = 0;
    assert!(matches!(
        store.query_requests(&query),
        Err(LogStoreError::InvalidQuery(_))
    ));

    query.limit = MAX_QUERY_LIMIT + 1;
    assert!(matches!(
        store.query_requests(&query),
        Err(LogStoreError::InvalidQuery(_))
    ));

    query.limit = 1;
    query.from = Some("not-a-time".to_string());
    assert!(matches!(
        store.query_requests(&query),
        Err(LogStoreError::InvalidQuery(_))
    ));

    query.from = Some("2026-08-03T00:01:00Z".to_string());
    query.to = Some("2026-08-03T00:00:00Z".to_string());
    assert!(matches!(
        store.query_requests(&query),
        Err(LogStoreError::InvalidQuery(_))
    ));
}

#[test]
fn forged_or_scope_mismatched_request_cursor_is_rejected() {
    let (_root, store) = open_store();
    store
        .insert_summary(
            "request-a",
            None,
            Some("chat"),
            None,
            None,
            "2026-08-03T00:00:05Z",
            None,
            None,
            None,
        )
        .expect("insert request");
    let mut query = request_query();
    query.limit = 1;
    query.cursor = Some(encode_cursor("2026-08-03T00:00:04Z", "request-a"));
    assert!(matches!(
        store.query_requests(&query),
        Err(LogStoreError::CursorInvalid)
    ));

    query.cursor = Some(encode_cursor("2026-08-03T00:00:05Z", "request-a"));
    query.route = Some("completion".to_string());
    assert!(matches!(
        store.query_requests(&query),
        Err(LogStoreError::CursorInvalid)
    ));
}

#[test]
fn related_records_are_typed_scoped_and_path_free() {
    let (_root, store) = open_store();
    for request_id in ["request-a", "request-b"] {
        store
            .insert_summary(
                request_id,
                None,
                None,
                None,
                None,
                "2026-08-03T00:00:00Z",
                None,
                None,
                None,
            )
            .expect("insert summary");
    }
    store
        .insert_lifecycle_event(
            "request-a",
            "event-a",
            r#"{"type":"stream_chunk","tokens":3}"#,
            "2026-08-03T00:00:01Z",
        )
        .expect("insert lifecycle event");
    store
        .insert_artifact_pointer(
            "artifact-a",
            "request-a",
            "2026-08-03T00:00:02Z",
            "response",
            None,
        )
        .expect("insert artifact pointer");
    store
        .update_artifact_pointer_storage("artifact-a", Some("text/plain"), "abc", 3, 1, true, false)
        .expect("store artifact metadata");
    store
        .update_artifact_pointer_missing("artifact-a")
        .expect("mark artifact missing");
    store
        .insert_proxy_record(
            "attempt-a",
            "request-a",
            "2026-08-03T00:00:03Z",
            "local-target",
            Some("provider-a"),
            Some("engine-a"),
            None,
            None,
            Some(200),
            None,
        )
        .expect("insert request-a proxy");
    store
        .insert_proxy_record(
            "attempt-b",
            "request-b",
            "2026-08-03T00:00:03Z",
            "other-target",
            None,
            None,
            None,
            None,
            Some(503),
            None,
        )
        .expect("insert request-b proxy");

    let page = PageQuery {
        limit: 10,
        cursor: None,
        sort: QuerySort::Ascending,
    };
    let events = store
        .query_events("request-a", &page)
        .expect("query events");
    let artifacts = store
        .query_artifacts("request-a", &page)
        .expect("query artifact metadata");
    let proxies = store
        .query_proxy_records(&ProxyQuery {
            page,
            request_id: Some("request-a".to_string()),
            provider: Some("provider-a".to_string()),
            engine: Some("engine-a".to_string()),
            status_code: Some(200),
        })
        .expect("query proxy records");

    assert_eq!(
        events.items[0].payload_json,
        r#"{"type":"stream_chunk","tokens":3}"#
    );
    assert!(artifacts.items[0].redacted);
    assert!(artifacts.items[0].missing);
    assert_eq!(artifacts.items[0].checksum.as_deref(), Some("abc"));
    assert_eq!(proxies.items.len(), 1);
    assert_eq!(proxies.items[0].attempt_id, "attempt-a");
}
