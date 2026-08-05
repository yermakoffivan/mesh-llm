use super::*;

fn push_sse_event(
    logging: &crate::logging::LoggingRuntimeState,
    channel: mesh_llm_events::logging::replay::ReplayChannel,
    request_id: mesh_llm_events::logging::identifiers::RequestId,
) -> String {
    use mesh_llm_events::logging::{envelope::CanonicalEnvelope, events::LifecycleEvent};

    logging
        .service_for_test()
        .expect("installed logging service")
        .enqueue_event(
            request_id,
            channel,
            serde_json::to_string(&LifecycleEvent::Admitted {
                model: Some("/private/operator/model.gguf?token=secret".into()),
                method: None,
            })
            .expect("serialize test lifecycle event"),
        )
        .expect("enqueue test lifecycle event");

    let record = logging
        .replay_bus()
        .expect("installed replay bus")
        .replay_window()
        .records
        .into_iter()
        .rev()
        .find(|record| {
            serde_json::from_str::<serde_json::Value>(&record.entry.payload)
                .ok()
                .and_then(|payload| payload.get("canonical_envelope").cloned())
                .and_then(|value| CanonicalEnvelope::from_json_str(&value.to_string()).ok())
                .is_some_and(|envelope| {
                    envelope.request_id == request_id && envelope.channel == channel
                })
        })
        .expect("enqueued test lifecycle event retained in replay window");
    format!(
        "id: v1:{}.{}.{}",
        record
            .cursor
            .sequence(mesh_llm_events::logging::replay::ReplayChannel::Requests),
        record
            .cursor
            .sequence(mesh_llm_events::logging::replay::ReplayChannel::Operations),
        record
            .cursor
            .sequence(mesh_llm_events::logging::replay::ReplayChannel::System),
    )
}

async fn install_sse_logging() -> tempfile::TempDir {
    let temporary_directory = tempfile::tempdir().unwrap();
    crate::initialize_logging_foundation(&mesh_llm_config::LoggingConfig {
        application_state_root: Some(temporary_directory.path().join("logging")),
        replay_capacity: 8,
        ..Default::default()
    })
    .await;
    temporary_directory
}

async fn disable_sse_logging() {
    crate::initialize_logging_foundation(&mesh_llm_config::LoggingConfig {
        enabled: false,
        ..Default::default()
    })
    .await;
}

fn cleanup_post(path: &str, body: &str) -> String {
    format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
}

fn delete_post(request_id: &str, body: &str) -> String {
    format!(
        "POST /api/logs/requests/{request_id}/delete HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
}

fn webhook_retry_post(delivery_id: &str, body: &str) -> String {
    format!(
        "POST /api/logs/webhooks/{delivery_id}/retry HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
}

fn seed_terminal_summary(store: &mesh_llm_log_store::LogStore, request_id: &str, created_at: &str) {
    store
        .insert_summary(
            request_id,
            Some("safe-model"),
            Some("management"),
            None,
            None,
            created_at,
            None,
            None,
            None,
        )
        .unwrap();
    store
        .conn()
        .execute(
            "UPDATE summaries SET state = 'completed' WHERE request_id = ?1",
            [request_id],
        )
        .unwrap();
}

#[tokio::test]
async fn log_routes_reject_hostile_host_and_origin_before_dispatch() {
    for header in [
        "Host: hostile.example\r\n",
        "Host: localhost\r\nOrigin: https://hostile.example\r\n",
    ] {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response = send_management_request(
            address,
            format!("GET /api/logs/requests HTTP/1.1\r\n{header}\r\n"),
        )
        .await;
        server.await.unwrap().unwrap();
        assert!(response.starts_with("HTTP/1.1 403 Forbidden"));
        assert!(!response.contains("logging service"));
    }
}

#[tokio::test]
async fn cleanup_routes_are_post_only_and_trusted_local_before_dispatch() {
    let operation_id = uuid::Uuid::new_v4();
    let body = serde_json::json!({
        "operationId": operation_id,
        "cutoffBefore": "2026-08-03T00:00:00Z",
        "requestLimit": 1,
        "reason": "operator cleanup",
    })
    .to_string();
    for path in ["/api/logs/cleanup/preview", "/api/logs/cleanup/run"] {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response = send_management_request(
            address,
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n"),
        )
        .await;
        server.await.unwrap().unwrap();
        assert!(
            response.starts_with("HTTP/1.1 405 Method Not Allowed"),
            "{path}"
        );
        assert_eq!(json_body(&response)["error"]["code"], "method_not_allowed");
    }

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let response = send_management_request(
        address,
        format!(
            "POST /api/logs/cleanup/preview HTTP/1.1\r\nHost: hostile.example\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .await;
    server.await.unwrap().unwrap();
    assert!(response.starts_with("HTTP/1.1 403 Forbidden"));
    assert!(!response.contains("logging service"));
}

#[tokio::test]
async fn delete_route_rejects_hostile_callers_and_wrong_methods_before_dispatch() {
    let request_id = "00000000-0000-4000-8000-000000000041";
    let body =
        r#"{"operationId":"00000000-0000-4000-8000-000000000042","reason":"operator delete"}"#;
    for header in [
        "Host: hostile.example\r\n",
        "Host: localhost\r\nOrigin: https://hostile.example\r\n",
    ] {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response = send_management_request(
            address,
            format!(
                "POST /api/logs/requests/{request_id}/delete HTTP/1.1\r\n{header}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
        .await;
        server.await.unwrap().unwrap();
        assert!(response.starts_with("HTTP/1.1 403 Forbidden"));
        assert!(!response.contains("logging service"));
    }

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let response = send_management_request(
        address,
        format!("GET /api/logs/requests/{request_id}/delete HTTP/1.1\r\nHost: localhost\r\n\r\n"),
    )
    .await;
    server.await.unwrap().unwrap();
    assert!(response.starts_with("HTTP/1.1 405 Method Not Allowed"));
    assert_eq!(json_body(&response)["error"]["code"], "method_not_allowed");
}

#[tokio::test]
async fn webhook_retry_route_rejects_hostile_callers_and_wrong_methods_before_dispatch() {
    let delivery_id = "webhook:00000000-0000-4000-8000-000000000080";
    let body = r#"{"reason":"operator webhook retry"}"#;
    for header in [
        "Host: hostile.example\r\n",
        "Host: localhost\r\nOrigin: https://hostile.example\r\n",
    ] {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response = send_management_request(
            address,
            format!(
                "POST /api/logs/webhooks/{delivery_id}/retry HTTP/1.1\r\n{header}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
        .await;
        server.await.unwrap().unwrap();
        assert!(response.starts_with("HTTP/1.1 403 Forbidden"));
        assert!(!response.contains("logging service"));
    }

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let response = send_management_request(
        address,
        format!("GET /api/logs/webhooks/{delivery_id}/retry HTTP/1.1\r\nHost: localhost\r\n\r\n"),
    )
    .await;
    server.await.unwrap().unwrap();
    assert!(response.starts_with("HTTP/1.1 405 Method Not Allowed"));
    assert_eq!(json_body(&response)["error"]["code"], "method_not_allowed");
}

#[tokio::test]
#[serial]
async fn webhook_retry_route_rejects_invalid_input_before_mutation_or_audit() {
    let temporary_directory = install_sse_logging().await;
    let logging = crate::logging_runtime_state().unwrap();
    let store = logging.store().unwrap();
    let delivery_id = "webhook:00000000-0000-4000-8000-000000000081";
    store
        .insert_webhook_delivery(delivery_id, None, "2026-08-04T00:00:00Z", 1, None)
        .unwrap();

    for request in [
        webhook_retry_post(&"x".repeat(129), r#"{"reason":"operator webhook retry"}"#),
        webhook_retry_post(delivery_id, r#"{}"#),
        webhook_retry_post(delivery_id, r#"{"reason":""}"#),
        webhook_retry_post(
            delivery_id,
            r#"{"reason":"operator webhook retry","extra":true}"#,
        ),
        format!(
            "POST /api/logs/webhooks/{delivery_id}/retry?unexpected=true HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 35\r\n\r\n{{\"reason\":\"operator webhook retry\"}}"
        ),
    ] {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response = send_management_request(address, request).await;
        server.await.unwrap().unwrap();
        assert!(
            response.starts_with("HTTP/1.1 400 Bad Request"),
            "{response}"
        );
    }
    assert_eq!(
        store.webhook_delivery(delivery_id).unwrap().unwrap().state,
        mesh_llm_log_store::WebhookDeliveryState::DeadLetter
    );
    let audit_count: i64 = store
        .conn()
        .query_row("SELECT COUNT(*) FROM audit_entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(audit_count, 0);

    disable_sse_logging().await;
    drop(temporary_directory);
}

#[tokio::test]
#[serial]
async fn webhook_retry_route_is_idempotent_audited_and_private() {
    let temporary_directory = install_sse_logging().await;
    let logging = crate::logging_runtime_state().unwrap();
    let store = logging.store().unwrap();
    let delivery_id = "webhook:00000000-0000-4000-8000-000000000082";
    let private_target = "https://hooks.example/private?credential=webhook-secret";
    let private_body = "webhook-private-response-body";
    store
        .insert_webhook_delivery(delivery_id, None, "2026-08-04T00:00:00Z", 1, None)
        .unwrap();
    store
        .conn()
        .execute(
            "UPDATE webhook_deliveries SET target_url = ?1, response_body = ?2, error_msg = ?3 WHERE delivery_id = ?4",
            [
                private_target,
                private_body,
                "credential=error-secret",
                delivery_id,
            ],
        )
        .unwrap();

    let body = r#"{"reason":"operator webhook retry"}"#;
    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let first_response =
        send_management_request(address, webhook_retry_post(delivery_id, body)).await;
    server.await.unwrap().unwrap();
    assert!(
        first_response.starts_with("HTTP/1.1 200 OK"),
        "{first_response}"
    );
    assert_eq!(
        json_body(&first_response),
        serde_json::json!({ "outcome": "scheduled" })
    );

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let replay_response =
        send_management_request(address, webhook_retry_post(delivery_id, body)).await;
    server.await.unwrap().unwrap();
    assert!(
        replay_response.starts_with("HTTP/1.1 200 OK"),
        "{replay_response}"
    );
    assert_eq!(
        json_body(&replay_response),
        serde_json::json!({ "outcome": "already_scheduled" })
    );

    let delivery = store.webhook_delivery(delivery_id).unwrap().unwrap();
    assert_eq!(
        delivery.state,
        mesh_llm_log_store::WebhookDeliveryState::ManualRetry
    );
    assert_eq!(delivery.attempt_number, 0);
    for value in [&first_response, &replay_response] {
        assert!(!value.contains(delivery_id));
        assert!(!value.contains(private_target));
        assert!(!value.contains(private_body));
        assert!(!value.contains("credential=error-secret"));
    }
    let audit_details = {
        let connection = store.conn();
        let mut statement = connection
            .prepare(
                "SELECT detail_json FROM audit_entries WHERE action = 'log_webhook_manual_retry' ORDER BY occurred_at, entry_id",
            )
            .unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    assert_eq!(audit_details.len(), 2);
    for detail in audit_details {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&detail).unwrap(),
            serde_json::json!({
                "actor": "trusted_local_operator",
                "source": "logs_api",
                "result": "succeeded",
                "reason": "operator webhook retry",
            })
        );
        assert!(!detail.contains(delivery_id));
        assert!(!detail.contains(private_target));
        assert!(!detail.contains(private_body));
        assert!(!detail.contains("credential=error-secret"));
    }

    disable_sse_logging().await;
    drop(temporary_directory);
}

#[tokio::test]
#[serial]
async fn webhook_retry_route_maps_typed_failures_and_audit_write_failures() {
    let temporary_directory = install_sse_logging().await;
    let logging = crate::logging_runtime_state().unwrap();
    let store = logging.store().unwrap();
    let succeeded_delivery = "webhook:00000000-0000-4000-8000-000000000083";
    let retry_delivery = "webhook:00000000-0000-4000-8000-000000000084";
    let body = r#"{"reason":"operator webhook retry"}"#;
    store
        .insert_webhook_delivery(
            succeeded_delivery,
            None,
            "2026-08-04T00:00:00Z",
            1,
            Some(204),
        )
        .unwrap();
    for delivery_id in [
        "webhook:00000000-0000-4000-8000-000000000085",
        succeeded_delivery,
    ] {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response =
            send_management_request(address, webhook_retry_post(delivery_id, body)).await;
        server.await.unwrap().unwrap();
        if delivery_id == succeeded_delivery {
            assert!(response.starts_with("HTTP/1.1 409 Conflict"), "{response}");
            assert_eq!(
                json_body(&response)["error"]["code"],
                "webhook_not_retryable"
            );
        } else {
            assert!(response.starts_with("HTTP/1.1 404 Not Found"), "{response}");
            assert_eq!(json_body(&response)["error"]["code"], "not_found");
        }
    }
    store
        .insert_webhook_delivery(retry_delivery, None, "2026-08-04T00:00:00Z", 1, None)
        .unwrap();
    store
        .conn()
        .execute_batch(
            "CREATE TRIGGER reject_webhook_retry_audit \
             BEFORE INSERT ON audit_entries \
             WHEN NEW.action = 'log_webhook_manual_retry' \
             BEGIN SELECT RAISE(ABORT, 'audit write rejected'); END;",
        )
        .unwrap();
    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let response = send_management_request(address, webhook_retry_post(retry_delivery, body)).await;
    server.await.unwrap().unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert_eq!(
        json_body(&response),
        serde_json::json!({ "outcome": "scheduled" })
    );
    assert_eq!(
        store
            .webhook_delivery(retry_delivery)
            .unwrap()
            .unwrap()
            .state,
        mesh_llm_log_store::WebhookDeliveryState::ManualRetry
    );
    let audit_count: i64 = store
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM audit_entries WHERE action = 'log_webhook_manual_retry'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(audit_count, 2, "failed audit must not recursively amplify");

    disable_sse_logging().await;
    drop(temporary_directory);
}

#[tokio::test]
#[serial]
async fn delete_route_rejects_invalid_identifiers_and_missing_reason_before_mutation() {
    let temporary_directory = install_sse_logging().await;
    let store = crate::logging_runtime_state().unwrap().store().unwrap();
    for (request_id, body) in [
        (
            "not-a-uuid",
            r#"{"operationId":"00000000-0000-4000-8000-000000000043","reason":"operator delete"}"#,
        ),
        (
            "00000000-0000-4000-8000-000000000044",
            r#"{"operationId":"00000000-0000-4000-8000-000000000045"}"#,
        ),
        (
            "00000000-0000-4000-8000-000000000046",
            r#"{"operationId":"00000000-0000-4000-8000-000000000047","reason":""}"#,
        ),
    ] {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response = send_management_request(address, delete_post(request_id, body)).await;
        server.await.unwrap().unwrap();
        assert!(
            response.starts_with("HTTP/1.1 400 Bad Request"),
            "{response}"
        );
    }
    let operation_count: i64 = store
        .conn()
        .query_row("SELECT COUNT(*) FROM maintenance_operations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(operation_count, 0);
    let summary_count: i64 = store
        .conn()
        .query_row("SELECT COUNT(*) FROM summaries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(summary_count, 0);

    disable_sse_logging().await;
    drop(temporary_directory);
}

#[tokio::test]
#[serial]
async fn delete_route_cascades_terminal_artifacts_and_replays_the_receipt() {
    let temporary_directory = tempfile::tempdir().unwrap();
    let mut config = mesh_llm_config::LoggingConfig {
        application_state_root: Some(temporary_directory.path().join("logging")),
        ..Default::default()
    };
    config.artifact.capture_mode = mesh_llm_config::CaptureMode::RedactedArtifacts;
    crate::initialize_logging_foundation(&config).await;
    let logging = crate::logging_runtime_state().unwrap();
    let store = logging.store().unwrap();
    let request_id = "00000000-0000-4000-8000-000000000051";
    let artifact_id = "00000000-0000-4000-8000-000000000052";
    seed_terminal_summary(&store, request_id, "2026-08-01T00:00:00Z");
    logging
        .write_artifact(crate::logging::ArtifactCaptureRequest {
            artifact_id,
            request_id,
            kind: "response",
            occurred_at: "2026-08-01T00:00:01Z",
            content: b"operator delete",
            media_kind: Some("text/plain"),
            version: 1,
            truncated: false,
            byte_limit: 1024,
            aggregate_limit: 1024,
        })
        .unwrap();
    let body =
        r#"{"operationId":"00000000-0000-4000-8000-000000000053","reason":"operator delete"}"#;
    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let first_response = send_management_request(address, delete_post(request_id, body)).await;
    server.await.unwrap().unwrap();
    assert!(first_response.starts_with("HTTP/1.1 200 OK"));
    let first = json_body(&first_response);
    assert_eq!(first["requestId"], request_id);
    assert_eq!(first["operationId"], "00000000-0000-4000-8000-000000000053");
    let audit_id = first["auditId"].as_str().expect("delete audit ID");
    assert_eq!(
        store
            .conn()
            .query_row(
                "SELECT action FROM audit_entries WHERE entry_id = ?1",
                [audit_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "log_delete_request"
    );
    assert_eq!(first["state"], "completed");
    assert_eq!(first["planned"]["requests"], 1);
    assert_eq!(first["executed"], first["planned"]);
    assert_eq!(
        first["artifactDeletion"],
        serde_json::json!({ "removed": 1, "failed": 0 })
    );
    assert!(!first_response.contains(&*temporary_directory.path().to_string_lossy()));

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let replay_response = send_management_request(address, delete_post(request_id, body)).await;
    server.await.unwrap().unwrap();
    assert!(replay_response.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(json_body(&replay_response), first);
    assert!(store.query_request(request_id).unwrap().is_none());
    assert!(store.query_artifact(artifact_id).unwrap().is_none());
    assert!(
        !temporary_directory
            .path()
            .join("logging")
            .join("artifacts")
            .join(request_id)
            .join(artifact_id)
            .exists()
    );
    let audit_count: i64 = store
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM audit_entries WHERE action = 'log_delete_request'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(audit_count, 1);

    disable_sse_logging().await;
}

#[tokio::test]
#[serial]
async fn delete_route_maps_missing_active_and_unavailable_to_typed_outcomes() {
    let temporary_directory = install_sse_logging().await;
    let logging = crate::logging_runtime_state().unwrap();
    let store = logging.store().unwrap();
    let missing_id = "00000000-0000-4000-8000-000000000061";
    let missing_body =
        r#"{"operationId":"00000000-0000-4000-8000-000000000062","reason":"operator delete"}"#;
    for _ in 0..2 {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response =
            send_management_request(address, delete_post(missing_id, missing_body)).await;
        server.await.unwrap().unwrap();
        assert!(response.starts_with("HTTP/1.1 404 Not Found"));
        assert_eq!(json_body(&response)["error"]["code"], "not_found");
    }
    let missing_operation_count: i64 = store
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM maintenance_operations WHERE operation_id = '00000000-0000-4000-8000-000000000062'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(missing_operation_count, 0);

    let active_id = "00000000-0000-4000-8000-000000000063";
    store
        .insert_summary(
            active_id,
            Some("safe-model"),
            Some("management"),
            None,
            None,
            "2026-08-01T00:00:00Z",
            None,
            None,
            None,
        )
        .unwrap();
    let active_body =
        r#"{"operationId":"00000000-0000-4000-8000-000000000064","reason":"operator delete"}"#;
    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let response = send_management_request(address, delete_post(active_id, active_body)).await;
    server.await.unwrap().unwrap();
    assert!(response.starts_with("HTTP/1.1 409 Conflict"));
    assert_eq!(json_body(&response)["error"]["code"], "request_active");
    assert!(store.query_request(active_id).unwrap().is_some());
    let active_operation_count: i64 = store
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM maintenance_operations WHERE operation_id = '00000000-0000-4000-8000-000000000064'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(active_operation_count, 0);

    disable_sse_logging().await;
    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let response = send_management_request(address, delete_post(active_id, active_body)).await;
    server.await.unwrap().unwrap();
    assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
    assert_eq!(json_body(&response)["error"]["code"], "logging_unavailable");
    drop(temporary_directory);
}

#[tokio::test]
#[serial]
async fn detail_and_artifact_reads_write_one_metadata_only_success_audit_each() {
    let temporary_directory = tempfile::tempdir().unwrap();
    let mut config = mesh_llm_config::LoggingConfig {
        application_state_root: Some(temporary_directory.path().join("logging")),
        ..Default::default()
    };
    config.artifact.capture_mode = mesh_llm_config::CaptureMode::RedactedArtifacts;
    crate::initialize_logging_foundation(&config).await;
    let logging = crate::logging_runtime_state().unwrap();
    let store = logging.store().unwrap();
    let request_id = "00000000-0000-4000-8000-000000000071";
    let artifact_id = "00000000-0000-4000-8000-000000000072";
    let artifact_body = b"detail-audit-artifact-body";
    seed_terminal_summary(&store, request_id, "2026-08-01T00:00:00Z");
    logging
        .write_artifact(crate::logging::ArtifactCaptureRequest {
            artifact_id,
            request_id,
            kind: "response",
            occurred_at: "2026-08-01T00:00:01Z",
            content: artifact_body,
            media_kind: Some("text/plain"),
            version: 1,
            truncated: false,
            byte_limit: 1024,
            aggregate_limit: 1024,
        })
        .unwrap();

    for path in [
        format!("/api/logs/requests/{request_id}"),
        format!("/api/logs/artifacts/{artifact_id}"),
    ] {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response = send_management_request(
            address,
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n"),
        )
        .await;
        server.await.unwrap().unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    }

    let audit_count: i64 = store
        .conn()
        .query_row("SELECT COUNT(*) FROM audit_entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        audit_count, 2,
        "one direct audit per read, without recursion"
    );
    for (action, reason) in [
        ("log_request_detail_read", "request detail read"),
        ("log_artifact_read", "artifact read"),
    ] {
        let detail: String = store
            .conn()
            .query_row(
                "SELECT detail_json FROM audit_entries WHERE action = ?1",
                [action],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&detail).unwrap(),
            serde_json::json!({
                "actor": "trusted_local_operator",
                "source": "logs_api",
                "result": "succeeded",
                "reason": reason,
            })
        );
        assert!(!detail.contains(request_id));
        assert!(!detail.contains(artifact_id));
        assert!(!detail.contains(&*String::from_utf8_lossy(artifact_body)));
        assert!(!detail.contains(&*temporary_directory.path().to_string_lossy()));
        assert!(!detail.contains("contentBase64"));
    }

    disable_sse_logging().await;
}

#[tokio::test]
#[serial]
async fn detail_and_artifact_missing_or_unavailable_reads_audit_failures() {
    let temporary_directory = install_sse_logging().await;
    let logging = crate::logging_runtime_state().unwrap();
    let store = logging.store().unwrap();
    let missing_request_id = "00000000-0000-4000-8000-000000000073";
    let missing_artifact_id = "00000000-0000-4000-8000-000000000074";
    let unavailable_request_id = "00000000-0000-4000-8000-000000000075";
    let unavailable_artifact_id = "00000000-0000-4000-8000-000000000076";
    seed_terminal_summary(&store, unavailable_request_id, "2026-08-01T00:00:00Z");
    store
        .insert_artifact_pointer(
            unavailable_artifact_id,
            unavailable_request_id,
            "2026-08-01T00:00:01Z",
            "response",
            None,
        )
        .unwrap();
    store
        .update_artifact_pointer_storage(
            unavailable_artifact_id,
            Some("text/plain"),
            "unavailable-checksum",
            1,
            1,
            false,
            false,
        )
        .unwrap();

    for (path, status, code) in [
        (
            format!("/api/logs/requests/{missing_request_id}"),
            "HTTP/1.1 404 Not Found",
            Some("not_found"),
        ),
        (
            format!("/api/logs/artifacts/{missing_artifact_id}"),
            "HTTP/1.1 404 Not Found",
            Some("not_found"),
        ),
        (
            format!("/api/logs/artifacts/{unavailable_artifact_id}"),
            "HTTP/1.1 200 OK",
            None,
        ),
    ] {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response = send_management_request(
            address,
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n"),
        )
        .await;
        server.await.unwrap().unwrap();
        assert!(response.starts_with(status), "{response}");
        if let Some(code) = code {
            assert_eq!(json_body(&response)["error"]["code"], code);
        } else {
            assert_eq!(json_body(&response)["contentState"], "unavailable");
        }
    }

    let detail_failures: i64 = store
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM audit_entries WHERE action = 'log_request_detail_read' AND detail_json LIKE '%\"result\":\"failed\"%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let artifact_failures: i64 = store
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM audit_entries WHERE action = 'log_artifact_read' AND detail_json LIKE '%\"result\":\"failed\"%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(detail_failures, 1);
    assert_eq!(artifact_failures, 2);

    disable_sse_logging().await;
    drop(temporary_directory);
}

#[tokio::test]
#[serial]
async fn detail_read_serves_normally_when_its_audit_write_fails_without_recursion() {
    let temporary_directory = install_sse_logging().await;
    let logging = crate::logging_runtime_state().unwrap();
    let store = logging.store().unwrap();
    let request_id = "00000000-0000-4000-8000-000000000077";
    seed_terminal_summary(&store, request_id, "2026-08-01T00:00:00Z");
    store
        .conn()
        .execute_batch(
            "CREATE TRIGGER reject_detail_read_audit \
             BEFORE INSERT ON audit_entries \
             WHEN NEW.action = 'log_request_detail_read' \
             BEGIN SELECT RAISE(ABORT, 'audit write rejected'); END;",
        )
        .unwrap();

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let response = send_management_request(
        address,
        format!("GET /api/logs/requests/{request_id} HTTP/1.1\r\nHost: localhost\r\n\r\n"),
    )
    .await;
    server.await.unwrap().unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    let audit_count: i64 = store
        .conn()
        .query_row("SELECT COUNT(*) FROM audit_entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(audit_count, 0, "failed audit must not recursively amplify");

    disable_sse_logging().await;
    drop(temporary_directory);
}

#[tokio::test]
#[serial]
async fn cleanup_preview_and_run_share_receipt_and_cascade_only_selected_artifacts() {
    let temporary_directory = tempfile::tempdir().unwrap();
    let mut config = mesh_llm_config::LoggingConfig {
        application_state_root: Some(temporary_directory.path().join("logging")),
        ..Default::default()
    };
    config.artifact.capture_mode = mesh_llm_config::CaptureMode::RedactedArtifacts;
    crate::initialize_logging_foundation(&config).await;
    let logging = crate::logging_runtime_state().unwrap();
    let store = logging.store().unwrap();
    let selected_request = "00000000-0000-4000-8000-000000000011";
    let retained_request = "00000000-0000-4000-8000-000000000012";
    let selected_artifact = "00000000-0000-4000-8000-000000000021";
    let retained_artifact = "00000000-0000-4000-8000-000000000022";
    seed_terminal_summary(&store, selected_request, "2026-08-01T00:00:00Z");
    seed_terminal_summary(&store, retained_request, "2026-08-02T00:00:00Z");
    store
        .conn()
        .execute(
            "UPDATE summaries SET route = 'cleanup-route', model = 'cleanup-model', provider = 'mesh', engine = 'skippy' WHERE request_id = ?1",
            [selected_request],
        )
        .unwrap();
    store
        .conn()
        .execute(
            "UPDATE summaries SET route = 'retained-route', model = 'retained-model', provider = 'other', engine = 'other' WHERE request_id = ?1",
            [retained_request],
        )
        .unwrap();
    for (request_id, artifact_id, occurred_at) in [
        (selected_request, selected_artifact, "2026-08-01T00:00:01Z"),
        (retained_request, retained_artifact, "2026-08-02T00:00:01Z"),
    ] {
        logging
            .write_artifact(crate::logging::ArtifactCaptureRequest {
                artifact_id,
                request_id,
                kind: "response",
                occurred_at,
                content: b"operator-safe cleanup",
                media_kind: Some("text/plain"),
                version: 1,
                truncated: false,
                byte_limit: 1024,
                aggregate_limit: 1024,
            })
            .unwrap();
    }

    let operation_id = uuid::Uuid::new_v4();
    let preview_body = serde_json::json!({
        "operationId": operation_id,
        "cutoffBefore": "2026-08-03T00:00:00Z",
        "requestLimit": 1,
        "source": "durable",
        "from": "2026-08-01T00:00:00Z",
        "to": "2026-08-02T00:00:00Z",
        "route": "cleanup-route",
        "model": "cleanup-model",
        "provider": "mesh",
        "engine": "skippy",
        "outcome": "completed",
        "reason": "operator cleanup",
    })
    .to_string();
    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let preview_response = send_management_request(
        address,
        cleanup_post("/api/logs/cleanup/preview", &preview_body),
    )
    .await;
    server.await.unwrap().unwrap();
    let preview = json_body(&preview_response);
    assert!(preview_response.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(preview["state"], "previewed");
    let preview_audit_id = preview["auditId"].as_str().expect("preview audit ID");
    assert_eq!(
        store
            .conn()
            .query_row(
                "SELECT action FROM audit_entries WHERE entry_id = ?1",
                [preview_audit_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "log_cleanup_preview"
    );
    assert_eq!(preview["planned"]["requests"], 1);
    assert_eq!(preview["planned"]["artifacts"], 1);
    assert_eq!(preview["hasMore"], false);
    assert_eq!(
        preview["scope"],
        serde_json::json!({
            "source": "durable",
            "cutoffBefore": "2026-08-03T00:00:00Z",
            "requestLimit": 1,
            "from": "2026-08-01T00:00:00Z",
            "to": "2026-08-02T00:00:00Z",
            "route": "cleanup-route",
            "model": "cleanup-model",
            "provider": "mesh",
            "engine": "skippy",
            "outcome": "completed",
        })
    );
    assert_eq!(
        preview["artifactDeletion"],
        serde_json::json!({ "removed": 0, "failed": 0 })
    );
    assert!(
        preview["selectionFingerprint"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert!(!preview_response.contains(&*temporary_directory.path().to_string_lossy()));

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let preview_replay = send_management_request(
        address,
        cleanup_post("/api/logs/cleanup/preview", &preview_body),
    )
    .await;
    server.await.unwrap().unwrap();
    assert_eq!(json_body(&preview_replay), preview);

    let run_body = serde_json::json!({
        "operationId": operation_id,
        "reason": "operator cleanup",
    })
    .to_string();
    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let run_response =
        send_management_request(address, cleanup_post("/api/logs/cleanup/run", &run_body)).await;
    server.await.unwrap().unwrap();
    let run = json_body(&run_response);
    assert!(run_response.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(run["state"], "completed");
    let run_audit_id = run["auditId"].as_str().expect("run audit ID");
    assert_ne!(run_audit_id, preview_audit_id);
    assert_eq!(
        store
            .conn()
            .query_row(
                "SELECT action FROM audit_entries WHERE entry_id = ?1",
                [run_audit_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "log_cleanup_execute"
    );
    assert_eq!(run["operationId"], operation_id.to_string());
    assert_eq!(run["selectionFingerprint"], preview["selectionFingerprint"]);
    assert_eq!(run["planned"], preview["planned"]);
    assert_eq!(run["executed"], preview["planned"]);
    assert_eq!(
        run["artifactDeletion"],
        serde_json::json!({ "removed": 1, "failed": 0 })
    );

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let replay_response =
        send_management_request(address, cleanup_post("/api/logs/cleanup/run", &run_body)).await;
    server.await.unwrap().unwrap();
    assert!(replay_response.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(json_body(&replay_response), run);
    assert!(store.query_request(selected_request).unwrap().is_none());
    assert!(store.query_artifact(selected_artifact).unwrap().is_none());
    assert!(store.query_request(retained_request).unwrap().is_some());
    assert!(store.query_artifact(retained_artifact).unwrap().is_some());
    let execute_audits: i64 = store
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM audit_entries WHERE action = 'log_cleanup_execute'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(execute_audits, 1);

    disable_sse_logging().await;
}

#[tokio::test]
#[serial]
async fn cleanup_rejects_invalid_scope_and_reason_before_db_and_maps_typed_errors() {
    let temporary_directory = install_sse_logging().await;
    let logging = crate::logging_runtime_state().unwrap();
    let store = logging.store().unwrap();
    for body in [
        r#"{"operationId":"00000000-0000-4000-8000-000000000031","cutoffBefore":"not-a-time","requestLimit":1,"reason":"operator cleanup"}"#,
        r#"{"operationId":"00000000-0000-4000-8000-000000000031","cutoffBefore":"2026-08-03T00:00:00Z","requestLimit":101,"reason":"operator cleanup"}"#,
        r#"{"operationId":"00000000-0000-4000-8000-000000000031","cutoffBefore":"2026-08-03T00:00:00Z","requestLimit":1,"reason":""}"#,
        r#"{"operationId":"00000000-0000-4000-8000-000000000031","cutoffBefore":"2026-08-03T00:00:00Z","requestLimit":1,"model":"/private/model?token=secret","reason":"operator cleanup"}"#,
    ] {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response =
            send_management_request(address, cleanup_post("/api/logs/cleanup/preview", body)).await;
        server.await.unwrap().unwrap();
        assert!(response.starts_with("HTTP/1.1 400 Bad Request"));
        assert!(json_body(&response)["error"]["code"].is_string());
        assert!(!response.contains("/private/model") && !response.contains("token=secret"));
    }
    let operation_count: i64 = store
        .conn()
        .query_row("SELECT COUNT(*) FROM maintenance_operations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(operation_count, 0);

    let unknown_run =
        r#"{"operationId":"00000000-0000-4000-8000-000000000032","reason":"operator cleanup"}"#;
    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let response =
        send_management_request(address, cleanup_post("/api/logs/cleanup/run", unknown_run)).await;
    server.await.unwrap().unwrap();
    assert!(response.starts_with("HTTP/1.1 404 Not Found"));
    assert_eq!(json_body(&response)["error"]["code"], "not_found");
    let failure_detail: String = store
        .conn()
        .query_row(
            "SELECT detail_json FROM audit_entries WHERE action = 'log_cleanup_run' ORDER BY occurred_at DESC, entry_id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(failure_detail.contains("failed"));
    assert!(!failure_detail.contains(&*temporary_directory.path().to_string_lossy()));

    let operation_id = "00000000-0000-4000-8000-000000000033";
    let first = format!(
        r#"{{"operationId":"{operation_id}","cutoffBefore":"2026-08-03T00:00:00Z","requestLimit":1,"model":"scope-a","reason":"operator cleanup"}}"#
    );
    let changed = format!(
        r#"{{"operationId":"{operation_id}","cutoffBefore":"2026-08-03T00:00:00Z","requestLimit":1,"model":"scope-b","reason":"operator cleanup"}}"#
    );
    for (body, expected) in [(&first, "200 OK"), (&changed, "409 Conflict")] {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response =
            send_management_request(address, cleanup_post("/api/logs/cleanup/preview", body)).await;
        server.await.unwrap().unwrap();
        assert!(
            response.starts_with(&format!("HTTP/1.1 {expected}")),
            "{response}"
        );
    }

    disable_sse_logging().await;
}

#[tokio::test]
#[serial]
async fn all_registered_log_reads_reach_the_log_dispatcher() {
    let request_id = "00000000-0000-4000-8000-000000000001";
    let paths = vec![
        "/api/logs/requests".to_string(),
        format!("/api/logs/requests/{request_id}"),
        format!("/api/logs/requests/{request_id}/events"),
        format!("/api/logs/requests/{request_id}/artifacts"),
        format!("/api/logs/artifacts/{request_id}"),
        "/api/logs/proxy".to_string(),
    ];
    for path in paths {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response = send_management_request(
            address,
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n"),
        )
        .await;
        server.await.unwrap().unwrap();
        assert!(
            response.starts_with("HTTP/1.1 200 OK")
                || response.starts_with("HTTP/1.1 404 Not Found")
                || response.starts_with("HTTP/1.1 503 Service Unavailable"),
            "{path}"
        );
        assert!(!response.contains(r#"{"error":"Not found"}"#));
    }
}

#[tokio::test]
#[serial]
async fn successful_log_response_redacts_path_shaped_metadata() {
    let temporary_directory = tempfile::tempdir().unwrap();
    crate::initialize_logging_foundation(&mesh_llm_config::LoggingConfig {
        application_state_root: Some(temporary_directory.path().join("logging")),
        ..Default::default()
    })
    .await;
    let logging = crate::logging_runtime_state().unwrap();
    let request_id = "00000000-0000-4000-8000-000000000001";
    logging
        .store()
        .unwrap()
        .insert_summary(
            request_id,
            Some("/Users/operator/private-model.gguf?token=secret"),
            Some("chat"),
            None,
            None,
            "2026-08-01T00:00:00Z",
            None,
            None,
            None,
        )
        .unwrap();

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let response = send_management_request(
        address,
        "GET /api/logs/requests HTTP/1.1\r\nHost: localhost\r\n\r\n".to_string(),
    )
    .await;
    server.await.unwrap().unwrap();

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    let body = json_body(&response);
    let item = body["items"]
        .as_array()
        .and_then(|items| items.iter().find(|item| item["requestId"] == request_id))
        .expect("redacted test request is listed");
    assert_eq!(item["model"], "[REDACTED]");
    assert!(!response.contains("/Users/operator") && !response.contains("token=secret"));

    crate::initialize_logging_foundation(&mesh_llm_config::LoggingConfig {
        enabled: false,
        ..Default::default()
    })
    .await;
}

#[tokio::test]
#[serial]
async fn export_is_deterministic_capped_metadata_only_and_audited() {
    let temporary_directory = tempfile::tempdir().unwrap();
    crate::initialize_logging_foundation(&mesh_llm_config::LoggingConfig {
        application_state_root: Some(temporary_directory.path().join("logging")),
        ..Default::default()
    })
    .await;
    let logging = crate::logging_runtime_state().unwrap();
    let store = logging.store().unwrap();
    let ids = [
        mesh_llm_events::logging::identifiers::RequestId::new(),
        mesh_llm_events::logging::identifiers::RequestId::new(),
        mesh_llm_events::logging::identifiers::RequestId::new(),
    ];
    for (request_id, occurred_at) in ids.iter().zip([
        "2026-08-01T00:00:00Z",
        "2026-08-03T00:00:00Z",
        "2026-08-02T00:00:00Z",
    ]) {
        let request_id = request_id.as_uuid().to_string();
        store
            .insert_summary(
                &request_id,
                Some("safe-model"),
                Some("management"),
                None,
                None,
                occurred_at,
                None,
                None,
                None,
            )
            .unwrap();
    }
    let event = mesh_llm_events::logging::envelope::CanonicalEnvelope::new(
        mesh_llm_events::logging::identifiers::EventId::new(),
        ids[1],
        mesh_llm_events::logging::replay::ReplayChannel::Requests,
        1,
        "2026-08-03T00:00:01Z".to_string(),
        mesh_llm_events::logging::events::LifecycleEvent::Admitted {
            model: Some("safe-model".to_string()),
            method: Some("POST".to_string()),
        },
    );
    store
        .insert_lifecycle_event(
            &ids[1].as_uuid().to_string(),
            &event.event_id.as_uuid().to_string(),
            &serde_json::to_string(&event).unwrap(),
            &event.occurred_at,
        )
        .unwrap();

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let body = r#"{"reason":"operator export"}"#;
    let response = send_management_request(
        address,
        format!(
            "POST /api/logs/requests/export?limit=2 HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .await;
    server.await.unwrap().unwrap();

    let export = json_body(&response);
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(export["items"].as_array().unwrap().len(), 2);
    assert_eq!(
        export["items"][0]["summary"]["requestId"],
        ids[1].as_uuid().to_string()
    );
    assert_eq!(export["items"][0]["events"].as_array().unwrap().len(), 1);
    assert_eq!(export["items"][0]["artifacts"].as_array().unwrap().len(), 0);
    assert_eq!(export["artifactContentIncluded"], false);
    assert!(export["nextCursor"].is_string());
    assert!(!response.contains("contentBase64"));

    let (action, detail): (String, String) = store
        .conn()
        .query_row(
            "SELECT action, detail_json FROM audit_entries ORDER BY occurred_at DESC, entry_id DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(action, "log_export");
    assert!(detail.contains("trusted_local_operator"));
    assert!(detail.contains("logs_api"));
    assert!(detail.contains("partial"));

    crate::initialize_logging_foundation(&mesh_llm_config::LoggingConfig {
        enabled: false,
        ..Default::default()
    })
    .await;
}

#[tokio::test]
#[serial]
async fn export_never_advances_a_request_cursor_past_partial_child_history() {
    let temporary_directory = tempfile::tempdir().unwrap();
    crate::initialize_logging_foundation(&mesh_llm_config::LoggingConfig {
        application_state_root: Some(temporary_directory.path().join("logging")),
        ..Default::default()
    })
    .await;
    let logging = crate::logging_runtime_state().unwrap();
    let store = logging.store().unwrap();
    let request_id = mesh_llm_events::logging::identifiers::RequestId::new();
    let request_id_text = request_id.as_uuid().to_string();
    store
        .insert_summary(
            &request_id_text,
            Some("safe-model"),
            Some("management"),
            None,
            None,
            "2026-08-03T00:00:00Z",
            None,
            None,
            None,
        )
        .unwrap();
    for sequence in 0..50 {
        let event = mesh_llm_events::logging::envelope::CanonicalEnvelope::new(
            mesh_llm_events::logging::identifiers::EventId::new(),
            request_id,
            mesh_llm_events::logging::replay::ReplayChannel::Requests,
            sequence,
            format!("2026-08-03T00:00:{sequence:02}Z"),
            mesh_llm_events::logging::events::LifecycleEvent::Admitted {
                model: Some("safe-model".to_string()),
                method: Some("POST".to_string()),
            },
        );
        store
            .insert_lifecycle_event(
                &request_id_text,
                &event.event_id.as_uuid().to_string(),
                &serde_json::to_string(&event).unwrap(),
                &event.occurred_at,
            )
            .unwrap();
    }

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let body = r#"{"reason":"operator export"}"#;
    let response = send_management_request(
        address,
        format!(
            "POST /api/logs/requests/export?limit=1 HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .await;
    server.await.unwrap().unwrap();

    let export = json_body(&response);
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(export["items"].as_array().unwrap().len(), 1);
    assert_eq!(export["items"][0]["events"].as_array().unwrap().len(), 49);
    assert_eq!(export["items"][0]["childIncomplete"], true);
    assert_eq!(export["retryRequired"], true);
    assert!(export["nextCursor"].is_null());

    crate::initialize_logging_foundation(&mesh_llm_config::LoggingConfig {
        enabled: false,
        ..Default::default()
    })
    .await;
}

#[tokio::test]
#[serial]
async fn export_rejects_missing_reason_and_artifact_opt_in_without_capture() {
    let temporary_directory = tempfile::tempdir().unwrap();
    crate::initialize_logging_foundation(&mesh_llm_config::LoggingConfig {
        application_state_root: Some(temporary_directory.path().join("logging")),
        ..Default::default()
    })
    .await;
    for body in [
        r#"{}"#,
        r#"{"reason":""}"#,
        r#"{"reason":"copy","includeArtifacts":true}"#,
    ] {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response = send_management_request(
            address,
            format!(
                "POST /api/logs/requests/export HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
        .await;
        server.await.unwrap().unwrap();
        assert!(
            response.starts_with("HTTP/1.1 400 Bad Request")
                || response.starts_with("HTTP/1.1 403 Forbidden"),
            "{response}"
        );
        assert!(json_body(&response)["error"]["code"].is_string());
    }
    crate::initialize_logging_foundation(&mesh_llm_config::LoggingConfig {
        enabled: false,
        ..Default::default()
    })
    .await;
}

#[tokio::test]
#[serial]
async fn export_includes_redacted_artifact_bytes_only_after_explicit_opt_in() {
    let temporary_directory = tempfile::tempdir().unwrap();
    let mut config = mesh_llm_config::LoggingConfig {
        application_state_root: Some(temporary_directory.path().join("logging")),
        ..Default::default()
    };
    config.artifact.capture_mode = mesh_llm_config::CaptureMode::RedactedArtifacts;
    crate::initialize_logging_foundation(&config).await;
    let logging = crate::logging_runtime_state().unwrap();
    let request_id = mesh_llm_events::logging::identifiers::RequestId::new()
        .as_uuid()
        .to_string();
    let artifact_id = uuid::Uuid::new_v4().to_string();
    logging
        .store()
        .unwrap()
        .insert_summary(
            &request_id,
            Some("safe-model"),
            Some("management"),
            None,
            None,
            "2026-08-03T00:00:00Z",
            None,
            None,
            None,
        )
        .unwrap();
    logging
        .write_artifact(crate::logging::ArtifactCaptureRequest {
            artifact_id: &artifact_id,
            request_id: &request_id,
            kind: "response",
            occurred_at: "2026-08-03T00:00:01Z",
            content: b"operator-safe export",
            media_kind: Some("text/plain"),
            version: 1,
            truncated: false,
            byte_limit: 1024,
            aggregate_limit: 1024,
        })
        .unwrap();

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let metadata_body = r#"{"reason":"operator metadata export"}"#;
    let metadata_response = send_management_request(
        address,
        format!(
            "POST /api/logs/requests/export HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{metadata_body}",
            metadata_body.len()
        ),
    )
    .await;
    server.await.unwrap().unwrap();
    let metadata_export = json_body(&metadata_response);
    assert!(metadata_response.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(metadata_export["artifactContentIncluded"], false);
    assert_eq!(
        metadata_export["items"][0]["artifacts"][0]["artifactId"],
        artifact_id
    );
    assert!(metadata_export["items"][0]["artifacts"][0]["contentBase64"].is_null());

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let body = r#"{"reason":"operator export","includeArtifacts":true}"#;
    let response = send_management_request(
        address,
        format!(
            "POST /api/logs/requests/export HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .await;
    server.await.unwrap().unwrap();

    let export = json_body(&response);
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(export["artifactContentIncluded"], true);
    assert_eq!(
        export["items"][0]["artifacts"][0]["artifactId"],
        artifact_id
    );
    assert!(export["items"][0]["artifacts"][0]["contentBase64"].is_string());

    crate::initialize_logging_foundation(&mesh_llm_config::LoggingConfig {
        enabled: false,
        ..Default::default()
    })
    .await;
}

#[tokio::test]
async fn log_routes_reject_methods_and_invalid_paths_with_bounded_json_errors() {
    let requests = [
        "POST /api/logs/requests HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n".to_string(),
        "GET /api/logs/artifacts/not-a-uuid/extra HTTP/1.1\r\nHost: localhost\r\n\r\n"
            .to_string(),
        "GET /api/logs/requests/00000000-0000-4000-8000-000000000001?limit=1 HTTP/1.1\r\nHost: localhost\r\n\r\n".to_string(),
    ];
    for request in requests {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response = send_management_request(address, request).await;
        server.await.unwrap().unwrap();
        assert!(
            response.starts_with("HTTP/1.1 405 Method Not Allowed")
                || response.starts_with("HTTP/1.1 404 Not Found")
                || response.starts_with("HTTP/1.1 400 Bad Request")
        );
        let body = json_body(&response);
        assert!(body["error"]["code"].is_string());
        assert!(response.len() < 1024);
        assert!(!response.contains("/Users/") && !response.contains("sqlite"));
    }
}

#[tokio::test]
async fn existing_runtime_event_routes_remain_sse_routes() {
    for path in ["/api/events", "/api/runtime/events"] {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let response = read_until_contains(
            &mut stream,
            b"Content-Type: text/event-stream",
            Duration::from_secs(2),
        )
        .await;
        assert!(String::from_utf8_lossy(&response).contains("Content-Type: text/event-stream"));
        drop(stream);
        server.abort();
    }
}

#[tokio::test]
#[serial]
async fn logs_events_sends_semantic_replay_and_fans_out_live_updates() {
    let _temporary_directory = install_sse_logging().await;
    let logging = crate::logging_runtime_state().unwrap();
    let request_id = mesh_llm_events::logging::identifiers::RequestId::new();
    let replay_id = push_sse_event(
        &logging,
        mesh_llm_events::logging::replay::ReplayChannel::Requests,
        request_id,
    );

    let state = build_test_mesh_api().await;
    let (first_addr, first_server) = spawn_management_test_server(state.clone()).await;
    let (second_addr, second_server) = spawn_management_test_server(state).await;
    let request = b"GET /api/logs/events?channel=requests HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n";
    let mut first = TcpStream::connect(first_addr).await.unwrap();
    let mut second = TcpStream::connect(second_addr).await.unwrap();
    first.write_all(request).await.unwrap();
    second.write_all(request).await.unwrap();

    for stream in [&mut first, &mut second] {
        let replay =
            read_until_contains(stream, replay_id.as_bytes(), Duration::from_secs(2)).await;
        let replay = String::from_utf8_lossy(&replay);
        assert!(replay.contains("HTTP/1.1 200 OK"));
        assert!(replay.contains("Content-Type: text/event-stream"));
        assert!(replay.contains("event: log_event"));
        assert!(replay.contains(&replay_id));
        assert!(!replay.contains("private/operator") && !replay.contains("token=secret"));
    }

    let live_id = push_sse_event(
        &logging,
        mesh_llm_events::logging::replay::ReplayChannel::Requests,
        request_id,
    );
    for stream in [&mut first, &mut second] {
        let live = read_until_contains(stream, live_id.as_bytes(), Duration::from_secs(2)).await;
        assert!(String::from_utf8_lossy(&live).contains("event: log_event"));
    }

    drop(first);
    drop(second);
    first_server.abort();
    second_server.abort();
    disable_sse_logging().await;
}

#[tokio::test]
#[serial]
async fn logs_events_subscribes_before_exposing_sse_headers() {
    let _temporary_directory = install_sse_logging().await;
    let logging = crate::logging_runtime_state().unwrap();
    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /api/logs/events?channel=requests HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n")
        .await
        .unwrap();

    let headers = read_until_contains(&mut stream, b"\r\n\r\n", Duration::from_secs(2)).await;
    assert!(String::from_utf8_lossy(&headers).contains("200 OK"));
    let live_id = push_sse_event(
        &logging,
        mesh_llm_events::logging::replay::ReplayChannel::Requests,
        mesh_llm_events::logging::identifiers::RequestId::new(),
    );
    let live = read_until_contains(&mut stream, live_id.as_bytes(), Duration::from_secs(2)).await;
    assert!(String::from_utf8_lossy(&live).contains("event: log_event"));

    drop(stream);
    server.abort();
    disable_sse_logging().await;
}

#[tokio::test]
#[serial]
async fn logs_events_merges_cursors_filters_and_reports_eviction_gaps() {
    let _temporary_directory = install_sse_logging().await;
    let logging = crate::logging_runtime_state().unwrap();
    let bus = logging.replay_bus().unwrap();
    bus.set_capacity(1_024);
    let wanted = mesh_llm_events::logging::identifiers::RequestId::new();
    let other = mesh_llm_events::logging::identifiers::RequestId::new();
    let initial_request_id = push_sse_event(
        &logging,
        mesh_llm_events::logging::replay::ReplayChannel::Requests,
        wanted,
    );
    push_sse_event(
        &logging,
        mesh_llm_events::logging::replay::ReplayChannel::Operations,
        other,
    );

    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state.clone()).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    let request = format!(
        "GET /api/logs/events?channel=requests&filter=request_id%3A{}&cursor=v1%3A0.0.0 HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\nLast-Event-ID: {}\r\n\r\n",
        wanted.as_uuid(),
        initial_request_id.trim_start_matches("id: "),
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let headers = read_until_contains(&mut stream, b"\r\n\r\n", Duration::from_secs(2)).await;
    assert!(String::from_utf8_lossy(&headers).contains("200 OK"));
    assert_no_stream_bytes_within(&mut stream, Duration::from_millis(100)).await;
    let live_id = push_sse_event(
        &logging,
        mesh_llm_events::logging::replay::ReplayChannel::Requests,
        wanted,
    );
    let live = read_until_contains(&mut stream, live_id.as_bytes(), Duration::from_secs(2)).await;
    assert!(String::from_utf8_lossy(&live).contains("event: log_event"));
    drop(stream);
    server.abort();

    bus.set_capacity(1);
    push_sse_event(
        &logging,
        mesh_llm_events::logging::replay::ReplayChannel::Requests,
        wanted,
    );
    let (address, server) = spawn_management_test_server(state).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /api/logs/events?channel=requests&cursor=v1%3A0.0.0 HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n")
        .await
        .unwrap();
    let gap = read_until_contains(&mut stream, b"event: replay_gap", Duration::from_secs(2)).await;
    let gap = String::from_utf8_lossy(&gap);
    assert!(gap.contains("/api/logs/requests"));
    assert!(!gap.contains("private/operator") && !gap.contains("token=secret"));
    drop(stream);
    server.abort();
    disable_sse_logging().await;
}

#[tokio::test]
async fn logs_events_rejects_invalid_raw_requests_before_sse_headers() {
    let oversized = std::iter::repeat_n("unknown=x", 33)
        .collect::<Vec<_>>()
        .join("&");
    let requests = [
        "GET /api/logs/events?channel=requests HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\n\r\n".to_string(),
        "GET /api/logs/events?channel=requests HTTP/1.1\r\nAccept: text/event-stream\r\n\r\n".to_string(),
        "POST /api/logs/events?channel=requests HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\nContent-Length: 0\r\n\r\n".to_string(),
        "GET /api/logs/events?channel=requests&filter=request_id%ZZ HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n".to_string(),
        format!("GET /api/logs/events?{oversized} HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n"),
        "GET /api/logs/events?channel=requests HTTP/1.1\r\nHost: hostile.example\r\nAccept: text/event-stream\r\n\r\n".to_string(),
        "GET /api/logs/events?channel=requests HTTP/1.1\r\nHost: localhost\r\nOrigin: https://hostile.example\r\nAccept: text/event-stream\r\n\r\n".to_string(),
    ];
    for request in requests {
        let state = build_test_mesh_api().await;
        let (address, server) = spawn_management_test_server(state).await;
        let response = send_management_request(address, request).await;
        server.await.unwrap().unwrap();
        assert!(
            response.starts_with("HTTP/1.1 400")
                || response.starts_with("HTTP/1.1 403")
                || response.starts_with("HTTP/1.1 405")
                || response.starts_with("HTTP/1.1 406"),
            "{response}"
        );
        assert!(!response.contains("Content-Type: text/event-stream"));
        assert!(response.len() < 1024);
        assert!(json_body(&response)["error"].is_object());
    }
}

#[tokio::test]
#[serial]
async fn logs_events_stops_after_a_disconnected_tcp_client() {
    let _temporary_directory = install_sse_logging().await;
    let logging = crate::logging_runtime_state().unwrap();
    let state = build_test_mesh_api().await;
    let (address, server) = spawn_management_test_server(state).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /api/logs/events?channel=requests HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n")
        .await
        .unwrap();
    let _headers = read_until_contains(&mut stream, b"\r\n\r\n", Duration::from_secs(2)).await;
    let request_id = mesh_llm_events::logging::identifiers::RequestId::new();
    drop(stream);
    push_sse_event(
        &logging,
        mesh_llm_events::logging::replay::ReplayChannel::Requests,
        request_id,
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
    push_sse_event(
        &logging,
        mesh_llm_events::logging::replay::ReplayChannel::Requests,
        request_id,
    );
    let completed = tokio::time::timeout(Duration::from_secs(2), server).await;
    assert!(
        completed.is_ok(),
        "SSE server did not release disconnected client"
    );
    assert!(completed.unwrap().unwrap().is_ok());
    disable_sse_logging().await;
}
