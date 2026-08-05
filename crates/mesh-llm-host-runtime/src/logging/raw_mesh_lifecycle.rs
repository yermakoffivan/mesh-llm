//! Metadata-only lifecycle ownership for raw mesh ingress requests.
//!
//! This owner is shared with the embedded OpenAI observer so a request that
//! entered through raw mesh routing cannot gain a competing frontend parent.
//! Direct loopback requests still belong to the frontend observer because they
//! never claim this raw-ingress ownership.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use mesh_llm_events::logging::{
    events::LifecycleEvent,
    identifiers::{AttemptId, RequestId},
    proxy::ProxyRecord,
    replay::ReplayChannel,
};

use super::{LifecycleGuard, LoggingService, RequestSummaryMetadata, TerminalOutcome};

const MAX_RAW_MESH_LIFECYCLE_OWNERS: usize = 1_024;
const MAX_STREAM_CHUNK_EVENTS: usize = 256;
const MAX_LOGGED_COMPLETION_TOKENS: u64 = u32::MAX as u64;

#[derive(Default)]
pub(crate) struct RawMeshLifecycleOwners {
    active: Mutex<HashMap<RequestId, RawMeshLifecycleEntry>>,
    remote_suppressions: Mutex<HashMap<RequestId, RemoteSuppressionEntry>>,
    next_token: AtomicU64,
}

struct RawMeshLifecycleEntry {
    guard: LifecycleGuard,
    token: u64,
    route_selected: bool,
    stream_started: bool,
    stream_chunks: usize,
    stream_completed: bool,
    stream_error: bool,
}

struct RemoteSuppressionEntry {
    token: u64,
    leases: u32,
}

pub(crate) struct RawMeshRequestLifecycle {
    service: Arc<LoggingService>,
    owners: Arc<RawMeshLifecycleOwners>,
    request_id: RequestId,
    token: u64,
    guard: LifecycleGuard,
}

/// One transport attempt that is already owned by a raw mesh request parent.
///
/// The attempt identifier is created by the canonical lifecycle recorder and
/// is reused for durable proxy metadata; this type cannot create a second
/// terminal owner.
pub(crate) struct RawMeshProxyAttempt {
    attempt_id: AttemptId,
    started_at: String,
}

/// Sanitized terminal metadata for one durable proxy attempt record.
pub(crate) struct ProxyAttemptFinish {
    pub(crate) target: &'static str,
    pub(crate) provider: Option<&'static str>,
    pub(crate) engine: Option<&'static str>,
    pub(crate) status_code: Option<u16>,
    pub(crate) lifecycle_error: Option<&'static str>,
    pub(crate) error: Option<&'static str>,
}

/// A fail-open, process-local marker for a trusted remote tunnel request.
///
/// Unlike a raw ingress lifecycle, this never registers a logging parent. It
/// only prevents the target's embedded frontend observer from registering a
/// duplicate parent while the authenticated tunnel relay is active.
pub(crate) struct RawMeshRemoteSuppressionLease {
    owners: Arc<RawMeshLifecycleOwners>,
    request_id: RequestId,
    token: u64,
}

impl RawMeshLifecycleOwners {
    pub(crate) fn is_claimed(&self, request_id: RequestId) -> bool {
        lock_recover(&self.active).contains_key(&request_id)
            || lock_recover(&self.remote_suppressions).contains_key(&request_id)
    }

    fn acquire_remote_suppression(&self, request_id: RequestId) -> Option<u64> {
        let mut suppressions = lock_recover(&self.remote_suppressions);
        if let Some(existing) = suppressions.get_mut(&request_id) {
            existing.leases = existing.leases.saturating_add(1);
            return Some(existing.token);
        }
        if suppressions.len() >= MAX_RAW_MESH_LIFECYCLE_OWNERS {
            return None;
        }

        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        suppressions.insert(request_id, RemoteSuppressionEntry { token, leases: 1 });
        Some(token)
    }

    fn release_remote_suppression(&self, request_id: RequestId, token: u64) {
        let mut suppressions = lock_recover(&self.remote_suppressions);
        let Some(entry) = suppressions.get_mut(&request_id) else {
            return;
        };
        if entry.token != token {
            return;
        }
        if entry.leases > 1 {
            entry.leases -= 1;
        } else {
            suppressions.remove(&request_id);
        }
    }

    fn claim(
        &self,
        service: &LoggingService,
        request_id: RequestId,
        metadata: RequestSummaryMetadata,
    ) -> Option<(LifecycleGuard, u64)> {
        let mut active = lock_recover(&self.active);
        if let Some(existing) = active.get(&request_id) {
            let claim = (existing.guard.clone(), existing.token);
            drop(active);
            service.merge_request_metadata(request_id, metadata);
            return Some(claim);
        }
        if active.len() >= MAX_RAW_MESH_LIFECYCLE_OWNERS {
            return None;
        }

        let (guard, _) = service.register_request_with_metadata(request_id, metadata);
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        active.insert(
            request_id,
            RawMeshLifecycleEntry {
                guard: guard.clone(),
                token,
                route_selected: false,
                stream_started: false,
                stream_chunks: 0,
                stream_completed: false,
                stream_error: false,
            },
        );
        Some((guard, token))
    }

    fn emit_route_selected(
        &self,
        service: &LoggingService,
        request_id: RequestId,
        token: u64,
        model: Option<&str>,
        provider: Option<&str>,
        engine: Option<&str>,
    ) {
        let should_emit = {
            let mut active = lock_recover(&self.active);
            let Some(entry) = active.get_mut(&request_id) else {
                return;
            };
            if entry.token != token || entry.route_selected {
                false
            } else {
                entry.route_selected = true;
                true
            }
        };
        if !should_emit {
            return;
        }

        let metadata = RequestSummaryMetadata::from_parts(None, model, provider, engine);
        service.merge_request_metadata(request_id, metadata.clone());
        let event = LifecycleEvent::RouteSelected {
            model: metadata.model().map(str::to_owned),
            provider: metadata.provider().map(str::to_owned),
            engine: metadata.engine().map(str::to_owned),
        };
        if let Ok(payload) = serde_json::to_string(&event) {
            let _ = service.enqueue_event(request_id, ReplayChannel::Operations, payload);
        }
    }

    fn emit_stream_started(
        &self,
        service: &LoggingService,
        request_id: RequestId,
        token: u64,
        model: Option<&str>,
    ) {
        let should_emit = {
            let mut active = lock_recover(&self.active);
            let Some(entry) = active.get_mut(&request_id) else {
                return;
            };
            if entry.token != token || entry.stream_started || entry.stream_completed {
                false
            } else {
                entry.stream_started = true;
                entry.stream_chunks = 0;
                entry.stream_completed = false;
                entry.stream_error = false;
                true
            }
        };
        if should_emit {
            enqueue_stream_event(
                service,
                request_id,
                LifecycleEvent::StreamStarted {
                    model: bounded_route_metadata(model),
                },
            );
        }
    }

    fn emit_stream_chunk(&self, service: &LoggingService, request_id: RequestId, token: u64) {
        let should_emit = {
            let mut active = lock_recover(&self.active);
            let Some(entry) = active.get_mut(&request_id) else {
                return;
            };
            if entry.token != token
                || !entry.stream_started
                || entry.stream_completed
                || entry.stream_error
                || entry.stream_chunks >= MAX_STREAM_CHUNK_EVENTS
            {
                false
            } else {
                entry.stream_chunks += 1;
                true
            }
        };
        if should_emit {
            enqueue_stream_event(
                service,
                request_id,
                LifecycleEvent::StreamChunk { tokens: None },
            );
        }
    }

    fn emit_stream_completed(
        &self,
        service: &LoggingService,
        request_id: RequestId,
        token: u64,
        completion_tokens: Option<u64>,
    ) {
        let should_emit = {
            let mut active = lock_recover(&self.active);
            let Some(entry) = active.get_mut(&request_id) else {
                return;
            };
            if entry.token != token
                || !entry.stream_started
                || entry.stream_completed
                || entry.stream_error
            {
                false
            } else {
                entry.stream_completed = true;
                true
            }
        };
        if should_emit {
            enqueue_stream_event(
                service,
                request_id,
                LifecycleEvent::StreamCompleted {
                    tokens: bounded_completion_tokens(completion_tokens),
                },
            );
        }
    }

    fn emit_stream_error(
        &self,
        service: &LoggingService,
        request_id: RequestId,
        token: u64,
        label: &'static str,
    ) {
        let should_emit = {
            let mut active = lock_recover(&self.active);
            let Some(entry) = active.get_mut(&request_id) else {
                return;
            };
            if entry.token != token || entry.stream_completed || entry.stream_error {
                false
            } else {
                entry.stream_error = true;
                entry.stream_started = false;
                true
            }
        };
        if should_emit {
            enqueue_stream_event(
                service,
                request_id,
                LifecycleEvent::StreamError {
                    error: Some(label.to_owned()),
                },
            );
        }
    }

    fn release(&self, request_id: RequestId, token: u64) {
        let mut active = lock_recover(&self.active);
        if active
            .get(&request_id)
            .is_some_and(|entry| entry.token == token)
        {
            active.remove(&request_id);
        }
    }
}

fn enqueue_stream_event(service: &LoggingService, request_id: RequestId, event: LifecycleEvent) {
    if let Ok(payload) = serde_json::to_string(&event) {
        let _ = service.enqueue_event(request_id, ReplayChannel::Operations, payload);
    }
}

impl RawMeshRequestLifecycle {
    pub(crate) fn register(
        service: Arc<LoggingService>,
        owners: Arc<RawMeshLifecycleOwners>,
        request_id: RequestId,
    ) -> Option<Self> {
        Self::register_with_metadata(
            service,
            owners,
            request_id,
            RequestSummaryMetadata::default(),
        )
    }

    /// Register a raw ingress parent with the metadata known before routing.
    pub(crate) fn register_with_metadata(
        service: Arc<LoggingService>,
        owners: Arc<RawMeshLifecycleOwners>,
        request_id: RequestId,
        metadata: RequestSummaryMetadata,
    ) -> Option<Self> {
        let (guard, token) = owners.claim(&service, request_id, metadata)?;
        Some(Self {
            service,
            owners,
            request_id,
            token,
            guard,
        })
    }

    pub(crate) fn route_selected(&self, model: Option<&str>) {
        self.route_selected_with_metadata(model, Some("mesh"), Some("raw_ingress"));
    }

    pub(crate) fn route_selected_with_metadata(
        &self,
        model: Option<&str>,
        provider: Option<&str>,
        engine: Option<&str>,
    ) {
        self.owners.emit_route_selected(
            &self.service,
            self.request_id,
            self.token,
            model,
            provider,
            engine,
        );
    }

    pub(crate) fn stream_started(&self, model: Option<&str>) {
        self.owners
            .emit_stream_started(&self.service, self.request_id, self.token, model);
    }

    /// Record the first produced stream chunk. The canonical event contract
    /// deliberately keeps this metadata-only and represents the first-token
    /// boundary as the first `stream_chunk` event.
    pub(crate) fn stream_first_token(&self) {
        self.owners
            .emit_stream_chunk(&self.service, self.request_id, self.token);
    }

    pub(crate) fn stream_chunk(&self) {
        self.owners
            .emit_stream_chunk(&self.service, self.request_id, self.token);
    }

    pub(crate) fn stream_completed(&self, completion_tokens: Option<u64>) {
        self.owners.emit_stream_completed(
            &self.service,
            self.request_id,
            self.token,
            completion_tokens,
        );
    }

    pub(crate) fn stream_error(&self, label: &'static str) {
        self.owners
            .emit_stream_error(&self.service, self.request_id, self.token, label);
    }

    pub(crate) fn stream_cancelled(&self) {
        self.stream_error("client_disconnected");
    }

    /// Start one bounded transport attempt beneath this raw mesh parent.
    pub(crate) fn start_attempt(&self) -> AttemptId {
        self.service.start_attempt(self.request_id, &self.guard)
    }

    /// Start one lifecycle attempt and retain only the bounded metadata needed
    /// to persist its transport result later.
    pub(crate) fn start_proxy_attempt(&self) -> RawMeshProxyAttempt {
        RawMeshProxyAttempt {
            attempt_id: self.start_attempt(),
            started_at: self.service.proxy_record_timestamp(),
        }
    }

    /// Complete one previously started raw mesh transport attempt.
    pub(crate) fn complete_attempt(&self, attempt_id: AttemptId, status_code: u16) {
        self.service
            .complete_attempt(self.request_id, attempt_id, Some(status_code));
    }

    /// Fail one previously started raw mesh transport attempt with a bounded
    /// static outcome label. The parent remains active for later targets.
    pub(crate) fn fail_attempt(&self, attempt_id: AttemptId, label: &'static str) {
        self.service
            .fail_attempt(self.request_id, attempt_id, label.to_owned());
    }

    /// Finish the existing lifecycle attempt and enqueue one metadata-only
    /// durable proxy record. Persistence is deliberately fail-open and this
    /// method never terminalizes the parent request.
    pub(crate) fn finish_proxy_attempt(
        &self,
        attempt: RawMeshProxyAttempt,
        finish: ProxyAttemptFinish,
    ) {
        let ProxyAttemptFinish {
            target,
            provider,
            engine,
            status_code,
            lifecycle_error,
            error,
        } = finish;
        let completed_at = self.service.proxy_record_timestamp();
        let mut record = ProxyRecord::new(
            attempt.attempt_id,
            self.request_id,
            target.to_owned(),
            attempt.started_at,
        );
        record.provider = provider.map(str::to_owned);
        record.engine = engine.map(str::to_owned);

        if let Some(status_code) = status_code {
            self.complete_attempt(attempt.attempt_id, status_code);
            record.complete(status_code, completed_at);
            record.error = error.map(str::to_owned);
        } else {
            let lifecycle_error = lifecycle_error.unwrap_or("retryable_unavailable");
            let error = error.unwrap_or("unavailable");
            self.fail_attempt(attempt.attempt_id, lifecycle_error);
            record.fail(error.to_owned(), completed_at);
        }

        let _ = self.service.enqueue_proxy_record(record);
    }

    pub(crate) fn terminal(&self, outcome: TerminalOutcome) {
        let _ = self
            .service
            .transition_terminal(self.request_id, &self.guard, outcome);
        self.owners.release(self.request_id, self.token);
    }
}

impl RawMeshRemoteSuppressionLease {
    pub(crate) fn acquire(
        owners: Arc<RawMeshLifecycleOwners>,
        request_id: RequestId,
    ) -> Option<Self> {
        let token = owners.acquire_remote_suppression(request_id)?;
        Some(Self {
            owners,
            request_id,
            token,
        })
    }
}

impl Drop for RawMeshRemoteSuppressionLease {
    fn drop(&mut self) {
        self.owners
            .release_remote_suppression(self.request_id, self.token);
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

const MAX_ROUTE_METADATA_CHARS: usize = 64;

fn bounded_route_metadata(value: Option<&str>) -> Option<String> {
    let value = value?;
    let (value, _) = super::policy::apply_redaction(value);
    let bounded: String = value
        .chars()
        .take(MAX_ROUTE_METADATA_CHARS)
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect();
    (!bounded.is_empty()).then_some(bounded)
}

fn bounded_completion_tokens(tokens: Option<u64>) -> Option<u64> {
    tokens.filter(|value| *value <= MAX_LOGGED_COMPLETION_TOKENS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::OpenAiLifecycleAttachment;

    fn recorded_events(service: &LoggingService) -> Vec<LifecycleEvent> {
        service
            .bus_ref()
            .replay_window()
            .records
            .into_iter()
            .filter_map(|record| {
                let envelope =
                    serde_json::from_str::<serde_json::Value>(&record.entry.payload).ok()?;
                let payload = envelope.get("payload")?.as_str()?;
                serde_json::from_str::<LifecycleEvent>(payload).ok()
            })
            .collect()
    }

    #[test]
    fn raw_mesh_lifecycle_orders_metadata_events_without_payloads() {
        let service = Arc::new(LoggingService::new_disabled(Default::default()));
        let owners = Arc::new(RawMeshLifecycleOwners::default());
        let request_id = RequestId::new();
        let lifecycle = RawMeshRequestLifecycle::register_with_metadata(
            service.clone(),
            owners,
            request_id,
            RequestSummaryMetadata::from_openai_ingress_path("/v1/chat/completions?ignored"),
        )
        .unwrap();

        lifecycle.route_selected(Some("safe-model"));
        lifecycle.terminal(TerminalOutcome::Completed);

        let events = recorded_events(&service);
        assert!(matches!(events[0], LifecycleEvent::Admitted { .. }));
        assert!(matches!(events[1], LifecycleEvent::RouteSelected { .. }));
        assert!(matches!(events[2], LifecycleEvent::Completed { .. }));
        let route_event = serde_json::to_value(&events[1]).unwrap();
        assert_eq!(route_event["model"], "safe-model");
        let summary = service
            .registry_ref()
            .get_recent(&request_id.as_uuid().to_string())
            .expect("terminal request summary");
        assert_eq!(summary.metadata.route(), Some("chat_completions"));
        assert_eq!(summary.metadata.model(), Some("safe-model"));
        assert_eq!(summary.metadata.provider(), Some("mesh"));
        assert_eq!(summary.metadata.engine(), Some("raw_ingress"));
        for payload_field in ["body", "headers", "prompt", "artifacts"] {
            assert!(route_event.get(payload_field).is_none());
        }
    }

    #[test]
    fn stream_phases_are_ordered_bounded_and_metadata_only() {
        let service = Arc::new(LoggingService::new_disabled(Default::default()));
        let owners = Arc::new(RawMeshLifecycleOwners::default());
        let request_id = RequestId::new();
        let parent =
            RawMeshRequestLifecycle::register(service.clone(), owners, request_id).unwrap();
        let mut attachment = OpenAiLifecycleAttachment::new(Some(parent));
        {
            let observer = attachment.route_observer();
            observer.route_selected(Some("safe-model"));
            observer.stream_started(Some("safe model/secret"));
            observer.stream_started(Some("secret-model-name"));
            observer.stream_first_token();
            for _ in 0..(MAX_STREAM_CHUNK_EVENTS + 4) {
                observer.stream_chunk();
            }
            observer.stream_completed(None);
            observer.stream_completed(None);
        }
        attachment.terminal(TerminalOutcome::Completed);
        {
            let observer = attachment.route_observer();
            observer.stream_started(Some("post_terminal_model"));
            observer.stream_chunk();
            observer.stream_completed(None);
            observer.stream_error("post_terminal_error");
        }
        attachment.terminal(TerminalOutcome::Failed("late_raw_failure".into()));

        let events = recorded_events(&service);
        assert!(matches!(events[0], LifecycleEvent::Admitted { .. }));
        assert!(matches!(events[1], LifecycleEvent::RouteSelected { .. }));
        assert!(matches!(events[2], LifecycleEvent::StreamStarted { .. }));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::StreamStarted { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::StreamChunk { .. }))
                .count(),
            MAX_STREAM_CHUNK_EVENTS
        );
        let completed_index = events
            .iter()
            .position(|event| matches!(event, LifecycleEvent::StreamCompleted { .. }))
            .expect("stream completion should be recorded");
        let terminal_index = events
            .iter()
            .position(|event| matches!(event, LifecycleEvent::Completed { .. }))
            .expect("request completion should be recorded");
        assert!(completed_index > 2);
        assert!(terminal_index > completed_index);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::StreamCompleted { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::Completed { .. }))
                .count(),
            1
        );

        let serialized = serde_json::to_string(&events).expect("phase events should serialize");
        for forbidden in [
            "raw_prompt",
            "secret_chunk",
            "authorization",
            "late_raw_failure",
            "safe model/secret",
            "secret-model-name",
            "post_terminal_model",
            "post_terminal_error",
        ] {
            assert!(
                !serialized.contains(forbidden),
                "found forbidden data: {forbidden}"
            );
        }
        assert!(events.iter().all(|event| {
            !matches!(
                event,
                LifecycleEvent::StreamChunk { tokens: Some(_) }
                    | LifecycleEvent::StreamCompleted { tokens: Some(_) }
            )
        }));
    }

    #[test]
    fn stream_completion_tokens_propagate_only_when_bounded_and_available() {
        let service = Arc::new(LoggingService::new_disabled(Default::default()));
        let owners = Arc::new(RawMeshLifecycleOwners::default());
        for completion_tokens in [Some(42), None, Some(u64::MAX)] {
            let request_id = RequestId::new();
            let parent =
                RawMeshRequestLifecycle::register(service.clone(), owners.clone(), request_id)
                    .unwrap();
            let mut attachment = OpenAiLifecycleAttachment::new(Some(parent));
            let observer = attachment.route_observer();
            observer.stream_started(None);
            observer.stream_first_token();
            observer.stream_completed(completion_tokens);
            attachment.terminal(TerminalOutcome::Completed);
        }

        let events = recorded_events(&service);
        let completion_tokens = events
            .iter()
            .filter_map(|event| match event {
                LifecycleEvent::StreamCompleted { tokens } => Some(*tokens),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(completion_tokens, [Some(42), None, None]);

        let serialized = serde_json::to_string(&events).expect("phase events should serialize");
        assert!(serialized.contains("\"tokens\":42"));
        assert!(!serialized.contains(&u64::MAX.to_string()));
        for forbidden in ["prompt", "completion_text", "usage", "authorization"] {
            assert!(!serialized.contains(forbidden));
        }
    }

    #[test]
    fn stream_cancellation_emits_one_bounded_error_before_terminal_cancel() {
        let service = Arc::new(LoggingService::new_disabled(Default::default()));
        let owners = Arc::new(RawMeshLifecycleOwners::default());
        let request_id = RequestId::new();
        let parent =
            RawMeshRequestLifecycle::register(service.clone(), owners, request_id).unwrap();
        let mut attachment = OpenAiLifecycleAttachment::new(Some(parent));
        {
            let observer = attachment.route_observer();
            observer.stream_started(None);
            observer.stream_first_token();
            observer.stream_cancelled();
            observer.stream_error("raw_upstream_error");
        }
        attachment.terminal(TerminalOutcome::Cancelled(Some(
            "client_disconnected".into(),
        )));
        {
            let observer = attachment.route_observer();
            observer.stream_error("post_terminal_error");
            observer.stream_completed(None);
        }
        attachment.terminal(TerminalOutcome::Completed);

        let events = recorded_events(&service);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::StreamError { .. }))
                .count(),
            1
        );
        assert!(matches!(
            events
                .iter()
                .find(|event| matches!(event, LifecycleEvent::StreamError { .. }))
                .expect("stream error"),
            LifecycleEvent::StreamError {
                error: Some(label)
            } if label == "client_disconnected"
        ));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::Cancelled { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::Completed { .. }))
                .count(),
            0
        );
        let serialized = serde_json::to_string(&events).expect("phase events should serialize");
        assert!(!serialized.contains("raw_upstream_error"));
        assert!(!serialized.contains("post_terminal_error"));
    }

    #[test]
    fn stream_retry_resets_phase_state_without_replacing_parent() {
        let service = Arc::new(LoggingService::new_disabled(Default::default()));
        let owners = Arc::new(RawMeshLifecycleOwners::default());
        let request_id = RequestId::new();
        let parent =
            RawMeshRequestLifecycle::register(service.clone(), owners, request_id).unwrap();
        let mut attachment = OpenAiLifecycleAttachment::new(Some(parent));
        let observer = attachment.route_observer();

        observer.stream_started(Some("target-a"));
        observer.stream_first_token();
        observer.stream_error("upstream_status");
        observer.stream_started(Some("target-b"));
        observer.stream_first_token();
        observer.stream_chunk();
        observer.stream_completed(None);
        attachment.terminal(TerminalOutcome::Completed);

        let events = recorded_events(&service);
        let phase_types = events
            .iter()
            .filter_map(|event| match event {
                LifecycleEvent::StreamStarted { .. } => Some("started"),
                LifecycleEvent::StreamChunk { .. } => Some("chunk"),
                LifecycleEvent::StreamError { .. } => Some("error"),
                LifecycleEvent::StreamCompleted { .. } => Some("completed"),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            phase_types,
            [
                "started",
                "chunk",
                "error",
                "started",
                "chunk",
                "chunk",
                "completed"
            ]
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::Completed { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::Admitted { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn raw_mesh_owner_reuses_one_guard_and_terminalizes_once() {
        let service = Arc::new(LoggingService::new_disabled(Default::default()));
        let owners = Arc::new(RawMeshLifecycleOwners::default());
        let request_id = RequestId::new();
        let first =
            RawMeshRequestLifecycle::register(service.clone(), owners.clone(), request_id).unwrap();
        let second =
            RawMeshRequestLifecycle::register(service.clone(), owners.clone(), request_id).unwrap();

        first.route_selected(None);
        second.route_selected(None);
        first.terminal(TerminalOutcome::Completed);
        second.terminal(TerminalOutcome::Failed("late".into()));

        let events = recorded_events(&service);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::Admitted { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::RouteSelected { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::Completed { .. }))
                .count(),
            1
        );
        assert!(!owners.is_claimed(request_id));
    }

    #[test]
    fn claimed_plan_failure_terminalizes_without_route_selected_event() {
        let service = Arc::new(LoggingService::new_disabled(Default::default()));
        let owners = Arc::new(RawMeshLifecycleOwners::default());
        let request_id = RequestId::new();
        let lifecycle =
            RawMeshRequestLifecycle::register(service.clone(), owners, request_id).unwrap();

        // The raw handler claims after parsing; a later planning failure must
        // still finish the admitted parent without fabricating route metadata.
        lifecycle.terminal(TerminalOutcome::Failed("no_hosts_available".into()));

        let events = recorded_events(&service);
        assert!(matches!(events[0], LifecycleEvent::Admitted { .. }));
        assert!(matches!(events[1], LifecycleEvent::Failed { .. }));
        assert!(
            events
                .iter()
                .all(|event| !matches!(event, LifecycleEvent::RouteSelected { .. }))
        );
    }

    #[test]
    fn bounded_owner_registry_drops_overflow_without_partial_lifecycle() {
        let service = Arc::new(LoggingService::new_disabled(Default::default()));
        let owners = Arc::new(RawMeshLifecycleOwners::default());
        let mut lifecycles = Vec::with_capacity(MAX_RAW_MESH_LIFECYCLE_OWNERS);
        for _ in 0..MAX_RAW_MESH_LIFECYCLE_OWNERS {
            lifecycles.push(
                RawMeshRequestLifecycle::register(
                    service.clone(),
                    owners.clone(),
                    RequestId::new(),
                )
                .unwrap(),
            );
        }

        let overflow_request_id = RequestId::new();
        assert!(RawMeshRequestLifecycle::register(
            service.clone(),
            owners.clone(),
            overflow_request_id,
        )
        .is_none());
        assert!(!owners.is_claimed(overflow_request_id));
        assert_eq!(
            recorded_events(&service)
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::Admitted { .. }))
                .count(),
            MAX_RAW_MESH_LIFECYCLE_OWNERS
        );

        for lifecycle in lifecycles {
            lifecycle.terminal(TerminalOutcome::Completed);
        }
    }

    #[test]
    fn stale_duplicate_handle_cannot_release_a_newer_owner() {
        let service = Arc::new(LoggingService::new_disabled(Default::default()));
        let owners = Arc::new(RawMeshLifecycleOwners::default());
        let request_id = RequestId::new();
        let first =
            RawMeshRequestLifecycle::register(service.clone(), owners.clone(), request_id).unwrap();
        let stale_duplicate =
            RawMeshRequestLifecycle::register(service.clone(), owners.clone(), request_id).unwrap();

        first.terminal(TerminalOutcome::Completed);
        let replacement =
            RawMeshRequestLifecycle::register(service, owners.clone(), request_id).unwrap();
        stale_duplicate.terminal(TerminalOutcome::Failed("late".into()));
        assert!(owners.is_claimed(request_id));

        replacement.terminal(TerminalOutcome::Completed);
        assert!(!owners.is_claimed(request_id));
    }

    #[test]
    fn remote_suppression_is_metadata_free_and_releases_after_the_last_lease() {
        let service = Arc::new(LoggingService::new_disabled(Default::default()));
        let owners = Arc::new(RawMeshLifecycleOwners::default());
        let request_id = RequestId::new();

        let first = RawMeshRemoteSuppressionLease::acquire(Arc::clone(&owners), request_id)
            .expect("first suppression lease should fit");
        let second = RawMeshRemoteSuppressionLease::acquire(Arc::clone(&owners), request_id)
            .expect("duplicate suppression lease should share the marker");

        assert!(owners.is_claimed(request_id));
        assert!(recorded_events(&service).is_empty());

        drop(first);
        assert!(owners.is_claimed(request_id));
        drop(second);
        assert!(!owners.is_claimed(request_id));
        assert!(recorded_events(&service).is_empty());
    }

    #[test]
    fn remote_suppression_cap_fails_open_without_registering_parents() {
        let service = Arc::new(LoggingService::new_disabled(Default::default()));
        let owners = Arc::new(RawMeshLifecycleOwners::default());
        let mut leases = Vec::with_capacity(MAX_RAW_MESH_LIFECYCLE_OWNERS);
        for _ in 0..MAX_RAW_MESH_LIFECYCLE_OWNERS {
            leases.push(
                RawMeshRemoteSuppressionLease::acquire(Arc::clone(&owners), RequestId::new())
                    .expect("bounded lease should fit"),
            );
        }

        let overflow_request_id = RequestId::new();
        assert!(
            RawMeshRemoteSuppressionLease::acquire(Arc::clone(&owners), overflow_request_id,)
                .is_none()
        );
        assert!(!owners.is_claimed(overflow_request_id));
        assert!(recorded_events(&service).is_empty());

        drop(leases);
    }
}
