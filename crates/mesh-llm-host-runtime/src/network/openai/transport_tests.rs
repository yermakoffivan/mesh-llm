use super::*;
use crate::logging::{
    LoggingService, OpenAiLifecycleAttachment, PersistSink, RawMeshLifecycleOwners,
    RawMeshRequestLifecycle, RequestSummaryEntry,
};
use crate::network::target_health::TargetHealthOutcome;
use anyhow::Result;
use mesh_llm_events::logging::proxy::ProxyRecord;
use mesh_llm_events::logging::replay::ReplayChannel;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct TransportProxySink {
    proxy_records: Mutex<Vec<ProxyRecord>>,
    summaries: Mutex<Vec<RequestSummaryEntry>>,
    artifact_pointers: Mutex<Vec<(String, serde_json::Value)>>,
}

impl TransportProxySink {
    fn proxy_records(&self) -> Vec<ProxyRecord> {
        self.proxy_records
            .lock()
            .expect("transport proxy records lock")
            .clone()
    }

    fn summary_count(&self) -> usize {
        self.summaries
            .lock()
            .expect("transport summaries lock")
            .len()
    }

    fn artifact_pointers(&self) -> Vec<(String, serde_json::Value)> {
        self.artifact_pointers
            .lock()
            .expect("transport artifact pointers lock")
            .clone()
    }
}

#[async_trait::async_trait]
impl PersistSink for TransportProxySink {
    async fn persist_summary(&self, entry: RequestSummaryEntry) -> Result<(), String> {
        self.summaries
            .lock()
            .expect("transport summaries lock")
            .push(entry);
        Ok(())
    }

    async fn persist_event(
        &self,
        _request_id: String,
        _event_id: String,
        _channel: ReplayChannel,
        _sequence: u64,
        _occurred_at: String,
        _payload_json: String,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn persist_artifact_pointer(
        &self,
        request_id: String,
        artifact_data: serde_json::Value,
    ) -> Result<(), String> {
        self.artifact_pointers
            .lock()
            .expect("transport artifact pointers lock")
            .push((request_id, artifact_data));
        Ok(())
    }

    async fn persist_proxy_record(&self, proxy_json: String) -> Result<(), String> {
        self.proxy_records
            .lock()
            .expect("transport proxy records lock")
            .push(serde_json::from_str(&proxy_json).expect("bounded proxy record"));
        Ok(())
    }

    async fn persist_audit_entry(
        &self,
        _record: crate::logging::OperationalAuditRecord,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn persist_webhook_delivery(
        &self,
        _request_id: Option<String>,
        _status_code: u16,
        _error: Option<String>,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn persist_cleanup_run(&self, _deleted_count: u64) -> Result<(), String> {
        Ok(())
    }
}

fn recorded_lifecycle_events(
    service: &LoggingService,
) -> Vec<mesh_llm_events::logging::events::LifecycleEvent> {
    service
        .bus_ref()
        .replay_window()
        .records
        .into_iter()
        .filter_map(|record| {
            let envelope = serde_json::from_str::<serde_json::Value>(&record.entry.payload).ok()?;
            let payload = envelope.get("payload")?.as_str()?;
            serde_json::from_str(payload).ok()
        })
        .collect()
}

#[tokio::test]
async fn route_observer_fails_open_without_a_parent_or_proxy_record() {
    let sink = Arc::new(TransportProxySink::default());
    let service = LoggingService::new(
        Default::default(),
        Arc::clone(&sink) as Arc<dyn PersistSink>,
        Box::new(crate::logging::SystemClock),
    );
    let observer = OpenAiRouteObserver::default();
    let attempt = observer.start_proxy_attempt();
    assert!(attempt.is_none());
    finish_route_attempt(
        observer,
        attempt,
        "remote",
        ResponseAdapter::OpenAiResponsesJson,
        &RouteAttemptResult::ClientDisconnected,
    );
    assert_eq!(service.pump_sync().await, 0);
    assert!(sink.proxy_records().is_empty());
}

#[tokio::test]
async fn transport_attempt_records_reuse_lifecycle_ids_and_keep_one_parent_terminal() {
    let sink = Arc::new(TransportProxySink::default());
    let service = Arc::new(LoggingService::new(
        Default::default(),
        Arc::clone(&sink) as Arc<dyn PersistSink>,
        Box::new(crate::logging::SystemClock),
    ));
    let parent = RawMeshRequestLifecycle::register(
        Arc::clone(&service),
        Arc::new(RawMeshLifecycleOwners::default()),
        RequestId::new(),
    )
    .expect("transport test should claim one parent");
    let mut attachment = OpenAiLifecycleAttachment::new(Some(parent));
    let observer = attachment.route_observer();

    for (target, adapter, result) in [
        (
            "local",
            ResponseAdapter::OpenAiChatCompletionsJson,
            RouteAttemptResult::Delivered {
                status_code: 200,
                completion_tokens: None,
            },
        ),
        (
            "remote",
            ResponseAdapter::OpenAiResponsesStream,
            RouteAttemptResult::RetryableTimeout,
        ),
        (
            "remote",
            ResponseAdapter::OpenAiResponsesStream,
            RouteAttemptResult::Delivered {
                status_code: 502,
                completion_tokens: None,
            },
        ),
        (
            "external",
            ResponseAdapter::OpenAiChatCompletionsStream,
            RouteAttemptResult::RetryableUnavailable,
        ),
        (
            "none",
            ResponseAdapter::None,
            RouteAttemptResult::RetryableUnavailable,
        ),
    ] {
        let attempt = observer.start_proxy_attempt();
        finish_route_attempt(observer, attempt, target, adapter, &result);
    }
    attachment.terminal(crate::logging::TerminalOutcome::Completed);
    let lifecycle_attempt_ids: Vec<_> = recorded_lifecycle_events(&service)
        .into_iter()
        .filter_map(|event| match event {
            mesh_llm_events::logging::events::LifecycleEvent::AttemptStarted { attempt_id } => {
                attempt_id
            }
            _ => None,
        })
        .collect();
    assert_eq!(lifecycle_attempt_ids.len(), 5);
    assert!(
        lifecycle_attempt_ids
            .iter()
            .enumerate()
            .all(|(index, id)| !lifecycle_attempt_ids[..index].contains(id)),
        "each remote retry must retain a distinct lifecycle attempt ID"
    );

    let _ = service.pump_sync().await;
    let records = sink.proxy_records();
    assert_eq!(records.len(), 5, "one record per real transport attempt");
    assert_eq!(
        records
            .iter()
            .map(|record| record.attempt_id)
            .collect::<Vec<_>>(),
        lifecycle_attempt_ids
    );
    assert_eq!(sink.summary_count(), 1, "the parent owns the sole terminal");
    assert_eq!(
        records
            .iter()
            .map(|record| {
                (
                    record.target.as_str(),
                    record.provider.as_deref(),
                    record.engine.as_deref(),
                    record.status_code,
                    record.error.as_deref(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                "local",
                Some("local"),
                Some("chat_completion"),
                Some(200),
                None
            ),
            (
                "remote",
                Some("remote"),
                Some("responses_stream"),
                None,
                Some("timeout")
            ),
            (
                "remote",
                Some("remote"),
                Some("responses_stream"),
                Some(502),
                Some("upstream_status"),
            ),
            (
                "external",
                Some("external"),
                Some("chat_completion_stream"),
                None,
                Some("unavailable"),
            ),
            ("none", None, None, None, Some("unavailable")),
        ]
    );
    for record in records {
        let serialized = serde_json::to_string(&record).expect("serialize bounded record");
        for forbidden in [
            "9337",
            "peer-id",
            "https://plugin.example/private/path",
            "plugin-name",
            "request body",
            "prompt text",
            "completion text",
            "connection refused",
        ] {
            assert!(!serialized.contains(forbidden), "record leaked {forbidden}");
        }
        assert!(!record.started_at.is_empty());
        assert!(record.completed_at.is_some());
    }
}

#[tokio::test]
async fn retry_then_stream_cancellation_keeps_one_metadata_only_parent() {
    let sink = Arc::new(TransportProxySink::default());
    let service = Arc::new(LoggingService::new(
        Default::default(),
        Arc::clone(&sink) as Arc<dyn PersistSink>,
        Box::new(crate::logging::SystemClock),
    ));
    let request_id = RequestId::new();
    let parent = RawMeshRequestLifecycle::register(
        Arc::clone(&service),
        Arc::new(RawMeshLifecycleOwners::default()),
        request_id,
    )
    .expect("transport test should claim one parent");
    let mut attachment = OpenAiLifecycleAttachment::new(Some(parent));
    let observer = attachment.route_observer();

    let first_attempt = observer.start_proxy_attempt();
    finish_route_attempt(
        observer,
        first_attempt,
        "remote",
        ResponseAdapter::OpenAiResponsesStream,
        &RouteAttemptResult::RetryableTimeout,
    );
    let second_attempt = observer.start_proxy_attempt();
    observer.stream_started(Some("safe-model"));
    observer.stream_first_token();
    finish_route_attempt(
        observer,
        second_attempt,
        "remote",
        ResponseAdapter::OpenAiResponsesStream,
        &RouteAttemptResult::Delivered {
            status_code: 200,
            completion_tokens: None,
        },
    );
    observer.stream_cancelled();
    attachment.terminal(crate::logging::TerminalOutcome::Cancelled(Some(
        "client_disconnected".into(),
    )));
    attachment.terminal(crate::logging::TerminalOutcome::Completed);

    let events = recorded_lifecycle_events(&service);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::Cancelled { .. }
            ))
            .count(),
        1,
        "the ingress parent emits one terminal cancellation"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::Completed { .. }
            ))
            .count(),
        0,
        "a late terminal result cannot replace the client cancellation"
    );
    let attempt_events = events
        .iter()
        .filter_map(|event| match event {
            mesh_llm_events::logging::events::LifecycleEvent::AttemptStarted { attempt_id } => {
                Some(("started", *attempt_id))
            }
            mesh_llm_events::logging::events::LifecycleEvent::AttemptFailed {
                attempt_id, ..
            } => Some(("failed", *attempt_id)),
            mesh_llm_events::logging::events::LifecycleEvent::AttemptCompleted {
                attempt_id,
                ..
            } => Some(("completed", *attempt_id)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        attempt_events
            .iter()
            .map(|(kind, _)| *kind)
            .collect::<Vec<_>>(),
        ["started", "failed", "started", "completed"]
    );
    assert_eq!(attempt_events[0].1, attempt_events[1].1);
    assert_eq!(attempt_events[2].1, attempt_events[3].1);
    assert_ne!(attempt_events[0].1, attempt_events[2].1);

    let _ = service.pump_sync().await;
    let records = sink.proxy_records();
    assert_eq!(
        sink.summary_count(),
        1,
        "only the parent persists a summary"
    );
    assert!(
        sink.artifact_pointers().is_empty(),
        "metadata-only retry and stream lifecycle paths never persist raw body artifacts"
    );
    assert_eq!(records.len(), 2, "both transport attempts are durable");
    assert!(records.iter().all(|record| record.request_id == request_id));
    assert!(records.iter().all(|record| record.completed_at.is_some()));
    assert_eq!(records[0].error.as_deref(), Some("timeout"));
    assert_eq!(records[1].status_code, Some(200));

    for record in service.bus_ref().replay_window().records {
        for forbidden in ["request body", "prompt text", "completion text"] {
            assert!(
                !record.entry.payload.contains(forbidden),
                "metadata-only replay leaked {forbidden}"
            );
        }
    }
}

#[test]
fn local_inference_attempt_failure_stays_under_one_parent() {
    let service = Arc::new(LoggingService::new_disabled(Default::default()));
    let parent = RawMeshRequestLifecycle::register(
        Arc::clone(&service),
        Arc::new(RawMeshLifecycleOwners::default()),
        RequestId::new(),
    )
    .unwrap();
    let mut attachment = OpenAiLifecycleAttachment::new(Some(parent));
    let observer = attachment.route_observer();
    observer.route_selected(Some("safe-model"));

    let result = record_local_inference_attempt(observer, RouteAttemptResult::RetryableUnavailable);
    assert_eq!(result, RouteAttemptResult::RetryableUnavailable);

    // The ingress attachment remains the sole terminal owner even after
    // the local attempt has failed; a late terminal call is ignored.
    attachment.terminal(crate::logging::TerminalOutcome::Failed(
        "local_inference_unavailable".into(),
    ));
    attachment.terminal(crate::logging::TerminalOutcome::Completed);

    let events = recorded_lifecycle_events(&service);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::Admitted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::RouteSelected { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::AttemptStarted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::AttemptFailed { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::Failed { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::Completed { .. }
            ))
            .count(),
        0
    );

    let attempt_id = events.iter().find_map(|event| match event {
        mesh_llm_events::logging::events::LifecycleEvent::AttemptStarted { attempt_id } => {
            *attempt_id
        }
        _ => None,
    });
    match events.iter().find(|event| {
        matches!(
            event,
            mesh_llm_events::logging::events::LifecycleEvent::AttemptFailed { .. }
        )
    }) {
        Some(mesh_llm_events::logging::events::LifecycleEvent::AttemptFailed {
            attempt_id: failed_id,
            error,
        }) => {
            assert_eq!(*failed_id, attempt_id);
            assert_eq!(error.as_deref(), Some("retryable_unavailable"));
        }
        other => panic!("expected one local attempt failure, got {other:?}"),
    }
    for record in service.bus_ref().replay_window().records {
        assert!(!record.entry.payload.contains("body"));
        assert!(!record.entry.payload.contains("prompt"));
        assert!(!record.entry.payload.contains("completion"));
    }
}

#[test]
fn local_inference_attempt_success_stays_under_one_parent() {
    let service = Arc::new(LoggingService::new_disabled(Default::default()));
    let parent = RawMeshRequestLifecycle::register(
        Arc::clone(&service),
        Arc::new(RawMeshLifecycleOwners::default()),
        RequestId::new(),
    )
    .unwrap();
    let mut attachment = OpenAiLifecycleAttachment::new(Some(parent));
    let observer = attachment.route_observer();
    observer.route_selected(Some("safe-model"));

    let result = record_local_inference_attempt(
        observer,
        RouteAttemptResult::Delivered {
            status_code: 200,
            completion_tokens: None,
        },
    );
    assert_eq!(
        result,
        RouteAttemptResult::Delivered {
            status_code: 200,
            completion_tokens: None,
        }
    );

    // The ingress attachment remains the sole terminal owner after a
    // successful local attempt; a late failure call is ignored.
    attachment.terminal(crate::logging::TerminalOutcome::Completed);
    attachment.terminal(crate::logging::TerminalOutcome::Failed(
        "late_local_failure".into(),
    ));

    let events = recorded_lifecycle_events(&service);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::Admitted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::RouteSelected { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::AttemptStarted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::AttemptCompleted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::AttemptFailed { .. }
            ))
            .count(),
        0
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::Completed { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::Failed { .. }
            ))
            .count(),
        0
    );

    let attempt_id = events.iter().find_map(|event| match event {
        mesh_llm_events::logging::events::LifecycleEvent::AttemptStarted { attempt_id } => {
            *attempt_id
        }
        _ => None,
    });
    match events.iter().find(|event| {
        matches!(
            event,
            mesh_llm_events::logging::events::LifecycleEvent::AttemptCompleted { .. }
        )
    }) {
        Some(mesh_llm_events::logging::events::LifecycleEvent::AttemptCompleted {
            attempt_id: completed_id,
            status_code,
        }) => {
            assert_eq!(*completed_id, attempt_id);
            assert_eq!(*status_code, Some(200));
        }
        other => panic!("expected one local attempt completion, got {other:?}"),
    }
    for record in service.bus_ref().replay_window().records {
        assert!(!record.entry.payload.contains("body"));
        assert!(!record.entry.payload.contains("prompt"));
        assert!(!record.entry.payload.contains("completion"));
    }
}

#[test]
fn remote_transports_record_target_failover_and_retry_under_one_parent() {
    let service = Arc::new(LoggingService::new_disabled(Default::default()));
    let parent = RawMeshRequestLifecycle::register(
        Arc::clone(&service),
        Arc::new(RawMeshLifecycleOwners::default()),
        RequestId::new(),
    )
    .unwrap();
    let mut attachment = OpenAiLifecycleAttachment::new(Some(parent));
    let observer = attachment.route_observer();
    observer.route_selected(Some("safe-model"));

    // Deterministic target A failure followed by target B success.
    record_remote_transport_attempt(observer, RouteAttemptResult::RetryableUnavailable);
    record_remote_transport_attempt(
        observer,
        RouteAttemptResult::Delivered {
            status_code: 202,
            completion_tokens: None,
        },
    );

    // A fresh transport retry on the same remote target has its own
    // attempt ID rather than being hidden inside the retry loop.
    record_remote_transport_attempt(observer, RouteAttemptResult::RetryableTimeout);
    record_remote_transport_attempt(
        observer,
        RouteAttemptResult::Delivered {
            status_code: 200,
            completion_tokens: None,
        },
    );

    attachment.terminal(crate::logging::TerminalOutcome::Completed);
    let events = recorded_lifecycle_events(&service);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::Admitted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::Completed { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::AttemptStarted { .. }
            ))
            .count(),
        4
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::AttemptFailed { .. }
            ))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                mesh_llm_events::logging::events::LifecycleEvent::AttemptCompleted { .. }
            ))
            .count(),
        2
    );
    let attempt_ids: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            mesh_llm_events::logging::events::LifecycleEvent::AttemptStarted { attempt_id } => {
                *attempt_id
            }
            _ => None,
        })
        .collect();
    assert_eq!(attempt_ids.len(), 4);
    assert!(
        attempt_ids
            .iter()
            .enumerate()
            .all(|(index, attempt_id)| !attempt_ids[..index].contains(attempt_id))
    );
    let attempt_events: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            mesh_llm_events::logging::events::LifecycleEvent::AttemptStarted { attempt_id } => {
                Some(("started", *attempt_id))
            }
            mesh_llm_events::logging::events::LifecycleEvent::AttemptFailed {
                attempt_id, ..
            } => Some(("failed", *attempt_id)),
            mesh_llm_events::logging::events::LifecycleEvent::AttemptCompleted {
                attempt_id,
                ..
            } => Some(("completed", *attempt_id)),
            _ => None,
        })
        .collect();
    assert_eq!(
        attempt_events
            .iter()
            .map(|(kind, _)| *kind)
            .collect::<Vec<_>>(),
        [
            "started",
            "failed",
            "started",
            "completed",
            "started",
            "failed",
            "started",
            "completed",
        ]
    );
    for pair in attempt_events.chunks_exact(2) {
        assert_eq!(pair[0].1, pair[1].1);
    }
    for record in service.bus_ref().replay_window().records {
        assert!(!record.entry.payload.contains("body"));
        assert!(!record.entry.payload.contains("prompt"));
        assert!(!record.entry.payload.contains("completion"));
    }
}

fn test_peer_serving_model(peer_id: iroh::EndpointId, model: &str) -> mesh::PeerInfo {
    mesh::PeerInfo {
        id: peer_id,
        addr: iroh::EndpointAddr {
            id: peer_id,
            addrs: Default::default(),
        },
        mesh_id: None,
        mesh_policy_hash: None,
        genesis_policy: None,
        role: mesh::NodeRole::Host { http_port: 9337 },
        first_joined_mesh_ts: None,
        models: vec![model.to_string()],
        vram_bytes: 16 * 1024 * 1024 * 1024,
        rtt_ms: None,
        model_source: None,
        admitted: true,
        serving_models: vec![model.to_string()],
        hosted_models: vec![model.to_string()],
        hosted_models_known: true,
        available_models: vec![],
        requested_models: vec![],
        explicit_model_interests: vec![],
        last_seen: std::time::Instant::now(),
        last_mentioned: std::time::Instant::now(),
        version: None,
        gpu_name: None,
        hostname: None,
        is_soc: None,
        gpu_vram: None,
        gpu_reserved_bytes: None,
        gpu_mem_bandwidth_gbps: None,
        gpu_compute_tflops_fp32: None,
        gpu_compute_tflops_fp16: None,
        available_model_metadata: vec![],
        experts_summary: None,
        available_model_sizes: HashMap::new(),
        served_model_descriptors: vec![local_gguf_descriptor(model)],
        served_model_runtime: vec![],
        owner_attestation: None,
        release_attestation_summary: crate::ReleaseAttestationSummary::default(),
        artifact_transfer_supported: false,
        stage_protocol_generation_supported: false,
        stage_status_list_supported: false,
        advertised_model_throughput: vec![],
        display_rtt: None,
        selected_path: None,
        propagated_latency: None,
        owner_summary: crate::crypto::OwnershipSummary::default(),
        inference_admission_state: None,
    }
}

async fn test_node_with_remote_models(models: &[(&str, iroh::EndpointId)]) -> mesh::Node {
    let node = mesh::Node::new_for_tests(mesh::NodeRole::Client)
        .await
        .expect("test node should start");
    for (model, peer_id) in models {
        node.insert_test_peer(test_peer_serving_model(*peer_id, model))
            .await;
    }
    node
}
fn text_auto_request() -> BufferedHttpRequest {
    let body = serde_json::json!({
        "model": "auto",
        "messages": [{"role": "user", "content": "hello"}]
    });
    let body_bytes = serde_json::to_vec(&body).expect("request body should serialize");
    BufferedHttpRequest {
        raw: Vec::new(),
        method: "POST".to_string(),
        path: "/v1/chat/completions".to_string(),
        client_path: "/v1/chat/completions".to_string(),
        request_id: mesh_llm_events::logging::identifiers::RequestId::default(),
        body_json: Some(body),
        body_json_attempted: true,
        body_bytes: Some(body_bytes),
        body_len_bytes: 0,
        completion_tokens: None,
        model_name: Some("auto".to_string()),
        stream: None,
        request_object_request_ids: Vec::new(),
        response_adapter: ResponseAdapter::None,
    }
}
fn large_tokenize_request(model: &str) -> BufferedHttpRequest {
    BufferedHttpRequest {
        raw: b"exact tokenizer request bytes".to_vec(),
        method: "POST".to_string(),
        path: "/v1/tokenize".to_string(),
        client_path: "/v1/tokenize".to_string(),
        request_id: mesh_llm_events::logging::identifiers::RequestId::default(),
        body_json: None,
        body_json_attempted: false,
        body_bytes: None,
        body_len_bytes: 140_000,
        completion_tokens: None,
        model_name: Some(model.to_string()),
        stream: None,
        request_object_request_ids: Vec::new(),
        response_adapter: ResponseAdapter::None,
    }
}
fn local_gguf_descriptor(model_name: &str) -> mesh::ServedModelDescriptor {
    mesh::ServedModelDescriptor {
        identity: mesh::ServedModelIdentity {
            model_name: model_name.to_string(),
            source_kind: mesh::ModelSourceKind::LocalGguf,
            local_file_name: Some(format!("{model_name}.gguf")),
            ..Default::default()
        },
        ..Default::default()
    }
}
#[test]
fn test_remote_retry_policy_only_retries_uncommitted_failures() {
    assert!(should_retry_uncommitted_remote_attempt(
        RouteAttemptResult::RetryableUnavailable
    ));
    assert!(should_retry_uncommitted_remote_attempt(
        RouteAttemptResult::RetryableTimeout
    ));
    assert!(!should_retry_uncommitted_remote_attempt(
        RouteAttemptResult::RetryableContextOverflow
    ));
    assert!(!should_retry_uncommitted_remote_attempt(
        RouteAttemptResult::RetryableResponseQuality(ResponseQualityFailure::EmptyAssistantOutput)
    ));
    assert!(!should_retry_uncommitted_remote_attempt(
        RouteAttemptResult::ClientDisconnected
    ));
    assert!(!should_retry_uncommitted_remote_attempt(
        RouteAttemptResult::Delivered {
            status_code: 200,
            completion_tokens: None,
        }
    ));
}

#[tokio::test]
async fn remote_tokenizer_plan_routes_identity_model_without_context_rejection() -> Result<()> {
    let model = "acme/code-model:Q4_K_M";
    let peer_id = iroh::EndpointId::from(iroh::SecretKey::generate().public());
    let node = test_node_with_remote_models(&[(model, peer_id)]).await;
    let mut peer = test_peer_serving_model(peer_id, model);
    peer.served_model_runtime = vec![mesh::ModelRuntimeDescriptor {
        model_name: model.to_owned(),
        identity_hash: None,
        context_length: Some(32_768),
        ready: true,
    }];
    node.insert_test_peer(peer).await;
    let affinity = AffinityRouter::new();
    let mut request = large_tokenize_request(model);
    let raw_before_plan = request.raw.clone();

    let generation_budget =
        request_budget_tokens_from_parts(request.body_len_bytes, request.completion_tokens);
    assert!(generation_budget.is_some_and(|tokens| tokens > 32_768));
    assert!(
        order_remote_hosts_by_context(
            &node,
            model,
            generation_budget,
            std::slice::from_ref(&peer_id),
        )
        .await
        .is_empty(),
        "a generation budget would incorrectly reject the tokenizer target"
    );

    let plan = build_mesh_request_plan(&node, &mut request, false, &affinity)
        .await
        .map_err(|_| anyhow::anyhow!("tokenizer request plan should resolve"))?;

    assert_eq!(request_context_budget(&request), None);
    assert_eq!(plan.effective_model.as_deref(), Some(model));
    assert_eq!(plan.target_hosts, vec![peer_id]);
    assert_eq!(request.raw, raw_before_plan);
    assert!(request.body_json.is_none());
    assert!(!request.body_json_attempted);
    Ok(())
}

#[test]
fn tokenizer_effective_model_cannot_override_authoritative_identity() {
    let model = "acme/code-model:Q4_K_M";
    let mut request = large_tokenize_request(model);
    let raw_before = request.raw.clone();

    rewrite_effective_model(&mut request, Some("different/internal-model"));

    assert_eq!(request.model_name.as_deref(), Some(model));
    assert_eq!(request.raw, raw_before);
}

#[tokio::test]
async fn cached_auto_model_stays_sticky_when_no_ready_remote_model_exists() -> Result<()> {
    let cached_model = "cached-cooling-model-31B";
    let alternate_model = "alternate-cooling-model-31B";
    let cached_peer = iroh::EndpointId::from(iroh::SecretKey::generate().public());
    let alternate_peer = iroh::EndpointId::from(iroh::SecretKey::generate().public());
    let node = test_node_with_remote_models(&[
        (cached_model, cached_peer),
        (alternate_model, alternate_peer),
    ])
    .await;
    let affinity = AffinityRouter::new();
    let key = 0xA11CE;
    affinity.remember_auto_model(key, cached_model);
    affinity.record_target_outcome(
        Some(cached_model),
        &election::InferenceTarget::Remote(cached_peer),
        TargetHealthOutcome::Unavailable,
    );
    affinity.record_target_outcome(
        Some(alternate_model),
        &election::InferenceTarget::Remote(alternate_peer),
        TargetHealthOutcome::Unavailable,
    );
    let descriptors = vec![
        local_gguf_descriptor(cached_model),
        local_gguf_descriptor(alternate_model),
    ];
    let media = router::MediaRequirements::default();
    let caps = crate::models::ModelCapabilities::default();
    let available = vec![
        router::RoutingCandidate::unscored(cached_model, caps),
        router::RoutingCandidate::unscored(alternate_model, caps),
    ];
    let ready_models = auto_route::ready_remote_models(&node, None, &available, &affinity).await;
    assert!(ready_models.is_empty());

    let cached = lookup_cached_auto_model(
        &node,
        &descriptors,
        &affinity,
        Some(key),
        &media,
        &ready_models,
    )
    .await;

    assert_eq!(cached.as_deref(), Some(cached_model));
    assert_eq!(
        affinity.lookup_auto_model(key).as_deref(),
        Some(cached_model)
    );
    Ok(())
}

#[tokio::test]
async fn auto_model_cache_switches_when_ready_alternate_exists() -> Result<()> {
    let cached_model = "cached-cooling-model-31B";
    let alternate_model = "ready-alternate-model-31B";
    let cached_peer = iroh::EndpointId::from(iroh::SecretKey::generate().public());
    let alternate_peer = iroh::EndpointId::from(iroh::SecretKey::generate().public());
    let node = test_node_with_remote_models(&[
        (cached_model, cached_peer),
        (alternate_model, alternate_peer),
    ])
    .await;
    let affinity = AffinityRouter::new();
    let key = 0xB0B;
    affinity.remember_auto_model(key, cached_model);
    affinity.record_target_outcome(
        Some(cached_model),
        &election::InferenceTarget::Remote(cached_peer),
        TargetHealthOutcome::Unavailable,
    );
    let served = vec![cached_model.to_string(), alternate_model.to_string()];
    let descriptors = vec![
        local_gguf_descriptor(cached_model),
        local_gguf_descriptor(alternate_model),
    ];
    let mut request = text_auto_request();

    let resolved = resolve_auto_model_request(AutoModelRequestArgs {
        node: &node,
        request: &mut request,
        served: &served,
        descriptors: &descriptors,
        is_auto_request: true,
        auto_session_key: Some(key),
        required_tokens: None,
        affinity: &affinity,
    })
    .await;

    assert!(matches!(
        resolved,
        AutoModelResolution::Model(Some(model)) if model == alternate_model
    ));
    assert_eq!(
        affinity.lookup_auto_model(key).as_deref(),
        Some(alternate_model)
    );
    Ok(())
}
#[test]
fn test_capture_path_for_request_uses_client_path() {
    let request = BufferedHttpRequest {
        raw: Vec::new(),
        method: "POST".to_string(),
        path: "/v1/chat/completions?foo=1".to_string(),
        client_path: "/v1/responses?foo=1".to_string(),
        request_id: mesh_llm_events::logging::identifiers::RequestId::default(),
        body_json: None,
        body_json_attempted: false,
        body_bytes: None,
        body_len_bytes: 0,
        completion_tokens: None,
        stream: None,
        model_name: Some("qwen".to_string()),
        request_object_request_ids: Vec::new(),
        response_adapter: ResponseAdapter::OpenAiResponsesStream,
    };

    assert_eq!(capture_path_for_request(&request), "/v1/responses?foo=1");
}
