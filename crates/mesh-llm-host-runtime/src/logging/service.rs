//! LoggingService facade owning bus + registry + lifecycle guard factory + persistence worker.
//!
//! The service coordinates all logging components and exposes a simple API for request-path callers.
//! Persistence work happens on a dedicated background task (spawned via `tokio::task::spawn_blocking` or its own tokio task) — the enqueue path never blocks.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mesh_llm_events::logging::envelope::CanonicalEnvelope;
use mesh_llm_events::logging::events::LifecycleEvent;
use mesh_llm_events::logging::identifiers::{AttemptId, EventId, RequestId};
use mesh_llm_events::logging::proxy::ProxyRecord;

use mesh_llm_events::logging::replay::{ReplayChannel, ReplaySequence};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::{AbortHandle, JoinHandle};

const PERSISTENCE_QUEUE_CAPACITY: usize = 64;
const PERSISTENCE_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub use super::bus::{BusEntry, ReplayBus};
use super::lifecycle::LifecycleRecorder;
pub use super::lifecycle::{DuplicateTerminalError, LifecycleGuard, TerminalOutcome};
use super::limits::{DynamicLoggingLimits, LoggingDynamicLimits};
use super::metrics::{
    LoggingArtifactCaptureStatus, LoggingCleanupOutcome, LoggingMetric, LoggingMetrics,
    LoggingMetricsSink, LoggingTerminalOutcome,
};
use super::output_projection::emit_accepted_canonical_event;
use super::registry::RequestSummaryEventSnapshots;
pub use super::registry::{RegistryConfig, RequestRegistry, RequestSummaryEntry};
use super::request_metadata::RequestSummaryMetadata;
pub use super::sequences::SequenceGenerators;
pub use super::writer::FailOpenWriter;

/// Trait for persistence sinks. The real LogStore implements this in a later todo (Todo 7+).
/// For now, tests provide a Vec-backed implementation.
#[async_trait::async_trait]
pub trait PersistSink: Send + Sync {
    /// Persist a request summary record.
    async fn persist_summary(&self, entry: RequestSummaryEntry) -> Result<(), String>;

    /// Persist a lifecycle event payload (JSON string).
    async fn persist_event(
        &self,
        request_id: String,
        event_id: String,
        channel: ReplayChannel,
        sequence: u64,
        occurred_at: String,
        payload_json: String,
    ) -> Result<(), String>;

    /// Persist an artifact pointer (metadata only; content handled by ArtifactFileStore).
    async fn persist_artifact_pointer(
        &self,
        request_id: String,
        artifact_data: serde_json::Value,
    ) -> Result<(), String>;

    /// Persist a proxy transport record.
    async fn persist_proxy_record(&self, proxy_json: String) -> Result<(), String>;

    /// Persist one typed static operational audit record.
    async fn persist_audit_entry(&self, record: OperationalAuditRecord) -> Result<(), String>;

    /// Persist a webhook delivery record.
    async fn persist_webhook_delivery(
        &self,
        request_id: Option<String>,
        status_code: u16,
        error: Option<String>,
    ) -> Result<(), String>;

    /// Persist a cleanup run summary.
    async fn persist_cleanup_run(&self, deleted_count: u64) -> Result<(), String>;
}

/// Clock provider for deterministic timestamps (injected by the service constructor).
pub trait Clock: Send + Sync {
    /// Return an ISO 8601 timestamp string. Tests inject a counter-based clock; production uses chrono::Utc.
    fn now(&self) -> String;
}

/// Production clock using system time.
#[derive(Clone, Debug)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> String {
        use chrono::{DateTime, Utc};
        let dt: DateTime<Utc> = Utc::now();
        format!("{}", dt.format("%Y-%m-%dT%H:%M:%S%.3fZ"))
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self
    }
}

/// Static, bounded operational audit data admitted by the logging service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationalAuditRecord {
    source: &'static str,
    code: &'static str,
    severity: Option<OperationalAuditSeverity>,
    detail_json: Option<String>,
    entry_id: Option<String>,
    occurred_at: Option<String>,
}

impl OperationalAuditRecord {
    pub const fn builder(
        source: &'static str,
        code: &'static str,
    ) -> OperationalAuditRecordBuilder {
        OperationalAuditRecordBuilder {
            source,
            code,
            severity: None,
        }
    }

    pub const fn source(&self) -> &'static str {
        self.source
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }

    pub const fn severity(&self) -> Option<OperationalAuditSeverity> {
        self.severity
    }

    fn with_internal_detail(mut self, detail_json: String) -> Self {
        self.detail_json = Some(detail_json);
        self
    }

    pub(crate) fn detail_json(&self) -> Option<&str> {
        self.detail_json.as_deref()
    }

    /// Attach a shared entry identity so the live bus frame and the durable
    /// persistence row carry the same `entry_id` and `occurred_at`.
    pub(crate) fn with_identity(mut self, entry_id: String, occurred_at: String) -> Self {
        self.entry_id = Some(entry_id);
        self.occurred_at = Some(occurred_at);
        self
    }

    /// Shared entry identifier, present when the record was produced through
    /// the live enqueue path. Absent for fallback audit records.
    pub(crate) fn entry_id(&self) -> Option<&str> {
        self.entry_id.as_deref()
    }

    /// Shared occurrence timestamp, present when the record was produced
    /// through the live enqueue path. Absent for fallback audit records.
    pub(crate) fn occurred_at(&self) -> Option<&str> {
        self.occurred_at.as_deref()
    }
}

/// Builder for static operational audit records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationalAuditRecordBuilder {
    source: &'static str,
    code: &'static str,
    severity: Option<OperationalAuditSeverity>,
}

impl OperationalAuditRecordBuilder {
    pub const fn severity(mut self, severity: OperationalAuditSeverity) -> Self {
        self.severity = Some(severity);
        self
    }

    pub const fn build(self) -> OperationalAuditRecord {
        OperationalAuditRecord {
            source: self.source,
            code: self.code,
            severity: self.severity,
            detail_json: None,
            entry_id: None,
            occurred_at: None,
        }
    }
}

/// Bounded severity vocabulary for operational audit records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationalAuditSeverity {
    Info,
    Warning,
    Error,
}

impl OperationalAuditSeverity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

/// Configuration for the logging service. Derived from [`mesh_llm_config::LoggingConfig`] but simplified for runtime use.
#[derive(Clone, Debug)]
pub struct ServiceConfig {
    /// Maximum number of entries in the replay bus before drop-oldest applies.
    pub queue_capacity: usize,
    /// Registry configuration (max_active, max_recent).
    pub registry_config: RegistryConfig,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 4096, // matches config defaults from Todo 2.
            registry_config: RegistryConfig::default(),
        }
    }
}

/// Internal message sent from the service to the persistence worker via mpsc channel.
#[derive(Debug)]
enum WorkerMessage {
    /// Persist one accepted entry through the service-owned delivery path.
    Persist(PersistenceEntry),
    /// Drain every preceding entry, acknowledge the drain, then exit. The
    /// control message is queued behind normal work so the acknowledgement is
    /// a precise durability boundary for the bounded worker channel.
    Shutdown(oneshot::Sender<()>),
}

/// One durable record accepted by the logging service. Proxy attempts are
/// observational records, so they do not enter the lifecycle replay bus.
#[derive(Debug)]
enum PersistenceEntry {
    Bus(BusEntry),
    Audit(OperationalAuditRecord),
    ProxyRecord(String),
}

/// The one owner of an accepted entry's persistence hand-off.
///
/// The replay bus intentionally remains independent of this state: replay is a
/// bounded read window, while this queue is a one-time delivery path. Keeping
/// them separate prevents a synchronous persistence pass from consuming replay
/// history or causing a second persistence attempt.
enum DeliveryMode {
    /// No worker is running. Entries are retained for an explicit
    /// [`LoggingService::pump_sync`] call or handed to the first worker.
    Manual {
        pending: VecDeque<PersistenceEntry>,
        capacity: usize,
    },
    /// A `pump_sync` task owns entries that were atomically removed from the
    /// manual queue. The completion signal lets shutdown wait or abort without
    /// allowing another pump to duplicate those entries.
    ManualPumping(Arc<ManualPumpCompletion>),
    /// A dedicated worker owns delivery through this bounded channel.
    Worker(mpsc::Sender<WorkerMessage>),
    /// Shutdown has frozen new persistence hand-offs while the worker drains
    /// entries already accepted before the transition.
    Stopping,
    /// The previous worker is joined. New events still reach replay, but their
    /// persistence hand-off is counted as unavailable until a later spawn.
    Stopped,
}

/// Path-free local state of the persistence delivery owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PersistenceWorkerState {
    NotStarted,
    Running,
    Stopping,
    Stopped,
}

impl PersistenceWorkerState {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        }
    }
}

struct ManualPumpCompletion {
    done: AtomicBool,
    notify: Notify,
}

impl ManualPumpCompletion {
    fn new() -> Self {
        Self {
            done: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn finish(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

/// Handle to the persistence worker task for controlled shutdown.
pub struct WorkerHandle {
    tx: mpsc::Sender<WorkerMessage>,
    task: JoinHandle<()>,
}

/// The LoggingService facade coordinating all logging components.
pub struct LoggingService {
    /// Bounded replay bus for nonblocking enqueue with drop-oldest overflow policy.
    bus: Arc<ReplayBus>,

    /// Process-local optional metrics adapter. Logging owns this closed
    /// vocabulary and never depends on an OTLP implementation.
    metrics: LoggingMetrics,

    /// Sequence generators per ReplayChannel (monotonic, shared across clones).
    sequences: SequenceGenerators,

    /// Active/recent request registry.
    registry: Arc<RequestRegistry>,

    /// Fail-open writer with recursion guard for error-audit fallback.
    writer: Arc<FailOpenWriter>,

    /// Persistence sink (LogStore in production; Vec-backed in tests).
    sink: Option<Arc<dyn PersistSink>>,

    /// Clock provider for deterministic timestamps.
    clock: Arc<dyn Clock>,

    /// Worker handle for controlled shutdown of the persistence task.
    worker_handle: Mutex<Option<WorkerHandle>>,

    /// Serializes the state transitions that publish or claim a delivery
    /// owner. It closes the worker-handle publication window between spawn and
    /// shutdown and also makes manual freeze/pump ownership atomic.
    transition_lock: Mutex<()>,

    /// A running manual pump's cancellation handle. It is installed before a
    /// shutdown can observe `ManualPumping`, so bounded shutdown can always
    /// stop a stalled pump without relying on caller cancellation.
    manual_abort_handle: Arc<Mutex<Option<AbortHandle>>>,

    /// One-time delivery state, kept separate from the replay window.
    delivery: Arc<Mutex<DeliveryMode>>,

    /// Whether spawn() has been called (prevents double-spawn).
    spawned: Arc<AtomicBool>,

    /// A retired runtime state must never be able to resurrect a persistence
    /// worker through an `Arc<LoggingService>` captured by an earlier runtime
    /// invocation. Retirement is permanent for this service instance; a new
    /// process-local runtime receives a new service.
    startable: AtomicBool,

    /// Accepted entries that could not be handed to the bounded persistence
    /// channel. This intentionally excludes replay-window evictions.
    persistence_queue_drops: Arc<AtomicU64>,

    /// Persistence attempts that reached a sink but the sink rejected.
    persistence_failures: Arc<AtomicU64>,

    /// Accepted persistence entries that remain owned by a manual queue or a
    /// worker. A timed-out shutdown moves this exact count into the bounded
    /// loss counter before aborting the worker.
    persistence_outstanding: Arc<AtomicU64>,

    /// Accepted entries lost only because a shutdown drain timed out. This is
    /// separate from ordinary queue saturation and sink failure accounting.
    persistence_shutdown_losses: Arc<AtomicU64>,

    /// Fixed upper bound for the worker drain/join phase. Tests inject zero to
    /// exercise the abort/accounting path without wall-clock sleeps.
    shutdown_drain_timeout: Duration,

    /// Weakly referenced by request guards so an explicit terminal transition
    /// and final-handle Drop share the same service delivery path.
    lifecycle_recorder: Arc<dyn LifecycleRecorder>,

    /// Service configuration for observability.
    #[allow(dead_code)]
    config: ServiceConfig,

    /// Coherent live values for the only dynamically applicable logging
    /// settings. The replay bus capacity is adjusted before this snapshot is
    /// published, so readers never see a live snapshot that the bus has not
    /// reached yet.
    dynamic_limits: DynamicLoggingLimits,
}

/// The service-owned terminal callback installed in request lifecycle guards.
/// It holds only service components, never a guard, so request ownership cannot
/// form a reference cycle with the logging runtime.
#[derive(Clone)]
struct EventDelivery {
    bus: Arc<ReplayBus>,
    registry: Arc<RequestRegistry>,
    metrics: LoggingMetrics,
    sequences: SequenceGenerators,
    sink_enabled: bool,
    clock: Arc<dyn Clock>,
    delivery: Arc<Mutex<DeliveryMode>>,
    persistence_queue_drops: Arc<AtomicU64>,
    persistence_outstanding: Arc<AtomicU64>,
}

impl EventDelivery {
    fn enqueue(&self, request_id: RequestId, channel: ReplayChannel, payload_json: String) {
        self.enqueue_with_summary_snapshots(request_id, channel, payload_json, None);
    }

    fn enqueue_with_summary_snapshots(
        &self,
        request_id: RequestId,
        channel: ReplayChannel,
        payload_json: String,
        summary_snapshots: Option<RequestSummaryEventSnapshots>,
    ) {
        let _ =
            enqueue_event_with_delivery(self, request_id, channel, payload_json, summary_snapshots);
    }

    fn enqueue_audit(&self, record: OperationalAuditRecord) {
        let entry_id = EventId::new().as_uuid().to_string();
        let occurred_at = self.clock.now();
        let record = record.with_identity(entry_id.clone(), occurred_at.clone());
        let mut payload = serde_json::json!({
            "kind": "audit",
            "entry_id": entry_id,
            "occurred_at": occurred_at,
            "source": record.source(),
            "code": record.code(),
        });
        if let Some(severity) = record.severity() {
            payload
                .as_object_mut()
                .expect("audit payload is always an object")
                .insert("severity".into(), serde_json::json!(severity.as_str()));
        }
        let entry = BusEntry {
            payload: payload.to_string(),
            channel_hint: 2,
        };
        let outcome = self
            .bus
            .push_audit_replay(entry.payload.clone(), entry.channel_hint);
        if self.sink_enabled && !matches!(outcome, super::bus::PushOutcome::Rejected) {
            offer_persistence_to(
                &self.delivery,
                &self.persistence_queue_drops,
                &self.persistence_outstanding,
                &self.metrics,
                PersistenceEntry::Audit(record),
            );
        }
    }
}

struct ServiceLifecycleRecorder {
    registry: Arc<RequestRegistry>,
    event_delivery: EventDelivery,
}

impl LifecycleRecorder for ServiceLifecycleRecorder {
    fn record_terminal(&self, request_id: RequestId, outcome: TerminalOutcome) {
        self.event_delivery
            .metrics
            .record(LoggingMetric::LifecycleTerminal {
                outcome: logging_terminal_outcome(&outcome),
            });
        let request_id_string = request_id.as_uuid().to_string();
        let summary_snapshots = self.registry.terminalize(
            &request_id_string,
            outcome.as_str(),
            self.event_delivery.clock.now(),
        );

        if let Ok(payload) = serde_json::to_string(&terminal_lifecycle_event(&outcome)) {
            self.event_delivery.enqueue_with_summary_snapshots(
                request_id,
                ReplayChannel::Requests,
                payload,
                summary_snapshots,
            );
        }
    }
}

fn logging_terminal_outcome(outcome: &TerminalOutcome) -> LoggingTerminalOutcome {
    match outcome {
        TerminalOutcome::Completed => LoggingTerminalOutcome::Completed,
        TerminalOutcome::Failed(_) => LoggingTerminalOutcome::Failed,
        TerminalOutcome::Rejected(_) => LoggingTerminalOutcome::Rejected,
        TerminalOutcome::Cancelled(_) => LoggingTerminalOutcome::Cancelled,
        TerminalOutcome::Dropped(_) => LoggingTerminalOutcome::Dropped,
    }
}

fn terminal_lifecycle_event(outcome: &TerminalOutcome) -> LifecycleEvent {
    match outcome {
        TerminalOutcome::Completed => LifecycleEvent::Completed {
            status_code: None,
            duration_ms: None,
        },
        TerminalOutcome::Failed(error) => LifecycleEvent::Failed {
            error: error.clone(),
        },
        TerminalOutcome::Rejected(reason) => LifecycleEvent::Rejected {
            reason: reason.clone(),
        },
        TerminalOutcome::Cancelled(reason) => LifecycleEvent::Cancelled {
            reason: reason.clone(),
        },
        TerminalOutcome::Dropped(reason) => LifecycleEvent::Dropped {
            reason: reason.clone(),
        },
    }
}

fn enqueue_event_with_delivery(
    event_delivery: &EventDelivery,
    request_id: RequestId,
    channel: ReplayChannel,
    payload_json: String,
    summary_snapshots: Option<RequestSummaryEventSnapshots>,
) -> EventId {
    let sequence = event_delivery.sequences.next(channel);
    let occurred_at = event_delivery.clock.now();
    let event_id = EventId::new();
    let canonical_envelope = serde_json::from_str::<LifecycleEvent>(&payload_json)
        .ok()
        .map(|event| {
            CanonicalEnvelope::new(
                event_id,
                request_id,
                channel,
                sequence,
                occurred_at.clone(),
                event,
            )
        });
    let mut entry = serde_json::json!({
        "request_id": request_id.as_uuid(),
        "channel": channel,
        "sequence": sequence,
        "occurred_at": occurred_at,
        "payload": payload_json,
    });
    if let Some(ref envelope) = canonical_envelope {
        let entry_object = entry
            .as_object_mut()
            .expect("logging bus entry is always a JSON object");
        entry_object.insert(
            "event_id".into(),
            serde_json::json!(envelope.event_id.as_uuid()),
        );
        entry_object.insert("canonical_envelope".into(), serde_json::json!(envelope));
    }
    let request_id_string = request_id.as_uuid().to_string();
    let summary_snapshots = summary_snapshots.or_else(|| {
        event_delivery
            .registry
            .get_active(&request_id_string)
            .or_else(|| event_delivery.registry.get_recent(&request_id_string))
            .map(|entry| RequestSummaryEventSnapshots::current(&entry))
    });
    if let Some(summary_snapshots) = summary_snapshots {
        entry
            .as_object_mut()
            .expect("logging bus entry is always a JSON object")
            .insert(
                "request_summary_snapshots".into(),
                serde_json::json!(summary_snapshots),
            );
    }
    let entry_payload = entry.to_string();
    let channel_hint = match channel {
        ReplayChannel::Requests => 0,
        ReplayChannel::Operations => 1,
        ReplayChannel::System => 2,
    };
    let entry = BusEntry {
        payload: entry_payload,
        channel_hint,
    };
    let outcome = event_delivery.bus.push_replay(
        entry.payload.clone(),
        entry.channel_hint,
        ReplaySequence::next(channel, sequence),
    );
    emit_accepted_canonical_event(outcome, canonical_envelope.as_ref());
    if event_delivery.sink_enabled && !matches!(outcome, super::bus::PushOutcome::Rejected) {
        offer_persistence_to(
            &event_delivery.delivery,
            &event_delivery.persistence_queue_drops,
            &event_delivery.persistence_outstanding,
            &event_delivery.metrics,
            PersistenceEntry::Bus(entry),
        );
    }
    event_id
}

fn offer_summary_persistence(
    delivery: &Mutex<DeliveryMode>,
    persistence_queue_drops: &AtomicU64,
    persistence_outstanding: &AtomicU64,
    metrics: &LoggingMetrics,
    summary: RequestSummaryEntry,
) {
    let payload = match serde_json::to_string(&serde_json::json!({
        "kind": "summary",
        "summary": summary,
    })) {
        Ok(payload) => payload,
        Err(_) => {
            record_persistence_queue_drop(persistence_queue_drops, metrics);
            return;
        }
    };
    offer_persistence_to(
        delivery,
        persistence_queue_drops,
        persistence_outstanding,
        metrics,
        PersistenceEntry::Bus(BusEntry {
            payload,
            channel_hint: 0,
        }),
    );
}

fn offer_persistence_to(
    delivery: &Mutex<DeliveryMode>,
    persistence_queue_drops: &AtomicU64,
    persistence_outstanding: &AtomicU64,
    metrics: &LoggingMetrics,
    entry: PersistenceEntry,
) {
    let mut delivery = match delivery.lock() {
        Ok(delivery) => delivery,
        Err(poisoned) => poisoned.into_inner(),
    };
    match &mut *delivery {
        DeliveryMode::Manual { pending, capacity } => {
            if pending.len() >= *capacity {
                pending.pop_front();
                record_persistence_queue_drop(persistence_queue_drops, metrics);
                decrement_outstanding(persistence_outstanding, metrics);
            }
            pending.push_back(entry);
            increment_outstanding(persistence_outstanding, metrics);
        }
        DeliveryMode::Worker(tx) => {
            if tx.try_send(WorkerMessage::Persist(entry)).is_ok() {
                increment_outstanding(persistence_outstanding, metrics);
            } else {
                record_persistence_queue_drop(persistence_queue_drops, metrics);
            }
        }
        DeliveryMode::ManualPumping(_) | DeliveryMode::Stopping | DeliveryMode::Stopped => {
            record_persistence_queue_drop(persistence_queue_drops, metrics);
        }
    }
}

fn record_persistence_queue_drop(counter: &AtomicU64, metrics: &LoggingMetrics) {
    counter.fetch_add(1, Ordering::Relaxed);
    metrics.record(LoggingMetric::PersistenceQueueDropped { count: 1 });
}

fn increment_persistence_failure(counter: &AtomicU64, metrics: &LoggingMetrics) {
    counter.fetch_add(1, Ordering::Relaxed);
    metrics.record(LoggingMetric::PersistenceFailure { count: 1 });
}

fn increment_outstanding(outstanding: &AtomicU64, metrics: &LoggingMetrics) {
    let current = outstanding
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    metrics.record(LoggingMetric::PersistenceOutstanding { current });
}

fn decrement_outstanding(outstanding: &AtomicU64, metrics: &LoggingMetrics) {
    let _ = outstanding.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        current.checked_sub(1)
    });
    metrics.record(LoggingMetric::PersistenceOutstanding {
        current: outstanding.load(Ordering::Acquire),
    });
}

const PERSISTENCE_FAILURE_AUDIT: &str = "logging persistence delivery failed";
const PROXY_TARGET_CLASSES: &[&str] = &["local", "remote", "external", "none"];
const PROXY_PROVIDER_LABELS: &[&str] = &[
    "openai_frontend",
    "management_api",
    "local",
    "remote",
    "external",
];
const PROXY_ENGINE_LABELS: &[&str] = &[
    "models",
    "chat_completion",
    "chat_completion_stream",
    "completion",
    "completion_stream",
    "responses",
    "responses_stream",
    "local",
    "remote",
    "external",
];
const PROXY_ERROR_LABELS: &[&str] = &[
    "timeout",
    "unavailable",
    "connection_failed",
    "client_disconnected",
    "cancelled",
    "rejected",
    "upstream_status",
];

pub(super) fn validate_proxy_record(record: &ProxyRecord) -> bool {
    is_allowed_proxy_label(PROXY_TARGET_CLASSES, &record.target)
        && record
            .provider
            .as_deref()
            .is_none_or(|value| is_allowed_proxy_label(PROXY_PROVIDER_LABELS, value))
        && record
            .engine
            .as_deref()
            .is_none_or(|value| is_allowed_proxy_label(PROXY_ENGINE_LABELS, value))
        && record
            .error
            .as_deref()
            .is_none_or(|value| is_allowed_proxy_label(PROXY_ERROR_LABELS, value))
}

fn sanitize_proxy_record(mut record: ProxyRecord) -> Option<ProxyRecord> {
    if !is_allowed_proxy_label(PROXY_TARGET_CLASSES, &record.target) {
        return None;
    }
    record.provider = record
        .provider
        .filter(|value| is_allowed_proxy_label(PROXY_PROVIDER_LABELS, value));
    record.engine = record
        .engine
        .filter(|value| is_allowed_proxy_label(PROXY_ENGINE_LABELS, value));
    record.error = record
        .error
        .filter(|value| is_allowed_proxy_label(PROXY_ERROR_LABELS, value));
    Some(record)
}

fn is_allowed_proxy_label(allowed: &[&str], value: &str) -> bool {
    allowed.contains(&value)
}

/// A fallback audit is itself a canonical System event. Its sink failure is
/// deliberately terminal for the fallback path; producing another fallback
/// would create an unbounded self-logging loop.
fn is_fallback_audit(entry: &PersistenceEntry) -> bool {
    let PersistenceEntry::Bus(entry) = entry else {
        return false;
    };
    serde_json::from_str::<serde_json::Value>(&entry.payload)
        .ok()
        .and_then(|record| record.get("canonical_envelope").cloned())
        .and_then(|envelope| CanonicalEnvelope::from_json_str(&envelope.to_string()).ok())
        .is_some_and(|envelope| matches!(envelope.event, LifecycleEvent::AuditError { .. }))
}

fn record_persistence_failure(
    writer: &FailOpenWriter,
    event_delivery: &EventDelivery,
    entry: &PersistenceEntry,
) {
    if is_fallback_audit(entry) {
        writer.record_fallback_suppressed();
        return;
    }

    let event_delivery = event_delivery.clone();
    let _ = writer.try_record_error(move || {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Ok(payload) = serde_json::to_string(&LifecycleEvent::AuditError {
                message: PERSISTENCE_FAILURE_AUDIT.into(),
            }) {
                event_delivery.enqueue(RequestId::new(), ReplayChannel::System, payload);
            }
        }));
    });
}

impl LoggingService {
    /// Return a logging-owned timestamp for bounded persistence metadata.
    ///
    /// Request-path callers reach this only through lifecycle owners, so an
    /// unowned or disabled observer cannot manufacture a durable record.
    pub(crate) fn proxy_record_timestamp(&self) -> String {
        self.clock.now()
    }

    /// Create a new logging service with the given sink and clock. In production, `sink` is the real LogStore; in tests, it's a Vec-backed mock.
    pub fn new(config: ServiceConfig, sink: Arc<dyn PersistSink>, clock: Box<dyn Clock>) -> Self {
        let dynamic_limits = LoggingDynamicLimits {
            retention_ttl_secs: mesh_llm_config::LoggingConfig::default().retention_ttl_secs,
            replay_capacity: config.queue_capacity,
        };
        Self::new_with_dynamic_limits(config, sink, clock, dynamic_limits)
    }

    /// Create a service with its replay bus and live limits initialized from
    /// validated host configuration.
    pub fn new_with_dynamic_limits(
        config: ServiceConfig,
        sink: Arc<dyn PersistSink>,
        clock: Box<dyn Clock>,
        dynamic_limits: LoggingDynamicLimits,
    ) -> Self {
        let bus = Arc::new(ReplayBus::new(dynamic_limits.replay_capacity));
        let metrics = bus.metrics();
        let sequences = SequenceGenerators::new();
        let registry = Arc::new(RequestRegistry::new(config.registry_config.clone()));
        let writer = Arc::new(FailOpenWriter::new());
        let clock: Arc<dyn Clock> = Arc::from(clock);
        let delivery = Arc::new(Mutex::new(DeliveryMode::Manual {
            pending: VecDeque::new(),
            capacity: PERSISTENCE_QUEUE_CAPACITY,
        }));
        let persistence_queue_drops = Arc::new(AtomicU64::new(0));
        let persistence_outstanding = Arc::new(AtomicU64::new(0));
        let lifecycle_recorder: Arc<dyn LifecycleRecorder> = Arc::new(ServiceLifecycleRecorder {
            registry: Arc::clone(&registry),
            event_delivery: EventDelivery {
                bus: Arc::clone(&bus),
                registry: Arc::clone(&registry),
                metrics: metrics.clone(),
                sequences: sequences.clone(),
                sink_enabled: true,
                clock: Arc::clone(&clock),
                delivery: Arc::clone(&delivery),
                persistence_queue_drops: Arc::clone(&persistence_queue_drops),
                persistence_outstanding: Arc::clone(&persistence_outstanding),
            },
        });

        Self {
            bus,
            metrics,
            sequences,
            registry,
            writer,
            sink: Some(sink),
            clock,
            worker_handle: Mutex::new(None),
            transition_lock: Mutex::new(()),
            manual_abort_handle: Arc::new(Mutex::new(None)),
            delivery,
            spawned: Arc::new(AtomicBool::new(false)),
            startable: AtomicBool::new(true),
            persistence_queue_drops,
            persistence_failures: Arc::new(AtomicU64::new(0)),
            persistence_outstanding,
            persistence_shutdown_losses: Arc::new(AtomicU64::new(0)),
            shutdown_drain_timeout: PERSISTENCE_SHUTDOWN_DRAIN_TIMEOUT,
            lifecycle_recorder,
            config,
            dynamic_limits: DynamicLoggingLimits::new(dynamic_limits),
        }
    }

    /// Create a service without any persistence sink (events are buffered but never persisted). Useful for testing or disabled logging.
    pub fn new_disabled(config: ServiceConfig) -> Self {
        let replay_capacity = config.queue_capacity;
        let bus = Arc::new(ReplayBus::new(config.queue_capacity));
        let metrics = bus.metrics();
        let sequences = SequenceGenerators::new();
        let registry = Arc::new(RequestRegistry::new(config.registry_config.clone()));
        let writer = Arc::new(FailOpenWriter::new());
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let delivery = Arc::new(Mutex::new(DeliveryMode::Manual {
            pending: VecDeque::new(),
            capacity: PERSISTENCE_QUEUE_CAPACITY,
        }));
        let persistence_queue_drops = Arc::new(AtomicU64::new(0));
        let persistence_outstanding = Arc::new(AtomicU64::new(0));
        let lifecycle_recorder: Arc<dyn LifecycleRecorder> = Arc::new(ServiceLifecycleRecorder {
            registry: Arc::clone(&registry),
            event_delivery: EventDelivery {
                bus: Arc::clone(&bus),
                registry: Arc::clone(&registry),
                metrics: metrics.clone(),
                sequences: sequences.clone(),
                sink_enabled: false,
                clock: Arc::clone(&clock),
                delivery: Arc::clone(&delivery),
                persistence_queue_drops: Arc::clone(&persistence_queue_drops),
                persistence_outstanding: Arc::clone(&persistence_outstanding),
            },
        });

        Self {
            bus,
            metrics,
            sequences,
            registry,
            writer,
            sink: None,
            clock,
            worker_handle: Mutex::new(None),
            transition_lock: Mutex::new(()),
            manual_abort_handle: Arc::new(Mutex::new(None)),
            delivery,
            spawned: Arc::new(AtomicBool::new(false)),
            startable: AtomicBool::new(true),
            persistence_queue_drops,
            persistence_failures: Arc::new(AtomicU64::new(0)),
            persistence_outstanding,
            persistence_shutdown_losses: Arc::new(AtomicU64::new(0)),
            shutdown_drain_timeout: PERSISTENCE_SHUTDOWN_DRAIN_TIMEOUT,
            lifecycle_recorder,
            config,
            dynamic_limits: DynamicLoggingLimits::new(LoggingDynamicLimits {
                retention_ttl_secs: mesh_llm_config::LoggingConfig::default().retention_ttl_secs,
                replay_capacity,
            }),
        }
    }

    /// Return the coherent dynamic limit pair currently applied to this
    /// running service. Retention scheduling consumes this later; Todo 6 does
    /// not start a cleanup worker.
    pub fn dynamic_limits(&self) -> LoggingDynamicLimits {
        self.dynamic_limits.snapshot()
    }

    /// Attach the optional process-local telemetry adapter. `None` keeps all
    /// logging metric emissions disabled; attaching the adapter publishes the
    /// current bounded persistence gauge without replaying historical data.
    pub(crate) fn set_metrics_sink(&self, sink: Option<Arc<dyn LoggingMetricsSink>>) {
        self.metrics.set_sink(sink);
        self.metrics.record(LoggingMetric::PersistenceOutstanding {
            current: self.persistence_outstanding.load(Ordering::Acquire),
        });
    }

    pub(crate) fn metrics(&self) -> LoggingMetrics {
        self.metrics.clone()
    }

    pub(crate) fn record_cleanup_outcome(&self, outcome: LoggingCleanupOutcome) {
        self.metrics.record(LoggingMetric::Cleanup { outcome });
    }

    pub(crate) fn record_artifact_capture_status(&self, status: LoggingArtifactCaptureStatus) {
        self.metrics
            .record(LoggingMetric::ArtifactCapture { status });
    }

    /// Apply both dynamically supported logging limits to this running
    /// service. Shrinking replay capacity evicts only the oldest buffered
    /// entries and accounts for each eviction. This is nonblocking except for
    /// the short in-memory mutexes guarding replay and the published snapshot.
    pub fn apply_dynamic_limits(&self, next: LoggingDynamicLimits) {
        let bus = Arc::clone(&self.bus);
        let _ = self.dynamic_limits.apply(next, move |capacity| {
            bus.set_capacity(capacity);
            Ok::<_, std::convert::Infallible>(())
        });
    }

    #[cfg(test)]
    pub(crate) fn poison_worker_handle_for_test(&self) {
        let _worker_handle = match self.worker_handle.lock() {
            Ok(worker_handle) => worker_handle,
            Err(poisoned) => poisoned.into_inner(),
        };
        panic!("poison worker handle lock for recovery coverage");
    }

    /// Start the persistence worker task. Entries accepted before startup are
    /// transferred from the bounded manual delivery queue. Idempotent: calling
    /// twice is a no-op (second call returns false). Returns true for a new
    /// worker.
    pub fn spawn(&self) -> bool {
        let _transition = self
            .transition_lock
            .lock()
            .expect("transition mutex poisoned");
        if !self.startable.load(Ordering::Acquire) {
            return false;
        }
        // Prevent double-spawn.
        if self
            .spawned
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }

        let sink_opt = self.sink.clone();
        let persistence_failures = Arc::clone(&self.persistence_failures);
        let persistence_outstanding = Arc::clone(&self.persistence_outstanding);
        let metrics = self.metrics.clone();
        let writer = Arc::clone(&self.writer);
        let failure_delivery = self.event_delivery();

        let (tx, mut rx) = mpsc::channel::<WorkerMessage>(PERSISTENCE_QUEUE_CAPACITY);

        // Switch delivery modes while holding one lock. An enqueue can therefore
        // hand an entry to either the manual queue or worker, never both.
        let pending = {
            let mut delivery = self.delivery.lock().expect("delivery mutex poisoned");
            match std::mem::replace(&mut *delivery, DeliveryMode::Worker(tx.clone())) {
                DeliveryMode::Manual { pending, .. } => pending,
                DeliveryMode::Stopped => VecDeque::new(),
                DeliveryMode::ManualPumping(_) | DeliveryMode::Stopping => {
                    *delivery = DeliveryMode::Stopping;
                    self.spawned.store(false, Ordering::Release);
                    return false;
                }
                DeliveryMode::Worker(existing_tx) => {
                    *delivery = DeliveryMode::Worker(existing_tx);
                    self.spawned.store(false, Ordering::Release);
                    return false;
                }
            }
        };

        for entry in pending {
            if tx.try_send(WorkerMessage::Persist(entry)).is_err() {
                record_persistence_queue_drop(&self.persistence_queue_drops, &self.metrics);
                decrement_outstanding(&self.persistence_outstanding, &self.metrics);
            }
        }

        let task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    WorkerMessage::Persist(entry) => {
                        // Parse the bus entry and persist via sink.
                        if let Some(sink) = &sink_opt {
                            // Best-effort: failures are absorbed by fail-open writer.
                            if Self::process_persistence_entry(sink.as_ref(), &entry)
                                .await
                                .is_err()
                            {
                                increment_persistence_failure(&persistence_failures, &metrics);
                                record_persistence_failure(
                                    writer.as_ref(),
                                    &failure_delivery,
                                    &entry,
                                );
                            }
                        }
                        decrement_outstanding(&persistence_outstanding, &metrics);
                    }
                    WorkerMessage::Shutdown(ack) => {
                        let _ = ack.send(());
                        break;
                    }
                }
            }
        });

        // Store both the control sender and join handle. Retaining the handle
        // makes shutdown an observable drain boundary rather than a detached
        // best-effort task drop.
        *self
            .worker_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(WorkerHandle { tx, task });

        true
    }

    async fn process_persistence_entry(
        sink: &dyn PersistSink,
        entry: &PersistenceEntry,
    ) -> Result<(), String> {
        match entry {
            PersistenceEntry::Bus(entry) => Self::process_bus_entry(sink, entry).await,
            PersistenceEntry::Audit(record) => sink.persist_audit_entry(record.clone()).await,
            PersistenceEntry::ProxyRecord(proxy_json) => {
                sink.persist_proxy_record(proxy_json.clone()).await
            }
        }
    }

    async fn process_bus_entry(sink: &dyn PersistSink, entry: &BusEntry) -> Result<(), String> {
        let record: serde_json::Value = serde_json::from_str(&entry.payload)
            .map_err(|e| format!("invalid bus entry JSON: {}", e))?;

        if record.get("kind").and_then(serde_json::Value::as_str) == Some("summary") {
            let summary = serde_json::from_value(
                record
                    .get("summary")
                    .cloned()
                    .ok_or_else(|| "summary bus record has no summary".to_string())?,
            )
            .map_err(|error| format!("invalid summary bus record: {error}"))?;
            return sink.persist_summary(summary).await;
        }

        if record.get("kind").and_then(serde_json::Value::as_str) == Some("audit") {
            return Err("audit bus record reached lifecycle persistence path".to_string());
        }

        if let Some(envelope_value) = record
            .get("canonical_envelope")
            .filter(|value| !value.is_null())
        {
            let envelope = CanonicalEnvelope::from_json_str(&envelope_value.to_string())
                .map_err(|error| format!("invalid canonical bus envelope: {error}"))?;
            if let LifecycleEvent::AuditError { .. } = envelope.event {
                return sink
                    .persist_audit_entry(
                        OperationalAuditRecord::builder("logging_service", "audit_error")
                            .severity(OperationalAuditSeverity::Error)
                            .build()
                            .with_internal_detail(serde_json::to_string(&envelope).map_err(
                                |error| format!("serialize canonical audit envelope: {error}"),
                            )?),
                    )
                    .await;
            }
            return sink
                .persist_event(
                    envelope.request_id.as_uuid().to_string(),
                    envelope.event_id.as_uuid().to_string(),
                    envelope.channel,
                    envelope.sequence,
                    envelope.occurred_at.clone(),
                    serde_json::to_string(&envelope)
                        .map_err(|error| format!("serialize canonical bus envelope: {error}"))?,
                )
                .await;
        }

        // Entries which are not canonical lifecycle records are operational
        // audit records. They stay out of the lifecycle repositories.
        sink.persist_audit_entry(
            OperationalAuditRecord::builder("logging_service", "uncategorized_bus_record")
                .severity(OperationalAuditSeverity::Info)
                .build()
                .with_internal_detail(entry.payload.clone()),
        )
        .await
    }

    /// Enqueue a lifecycle event for the given request. This is fail-open: if the bus is full, drop counters increment and Ok(()) returns — the caller should NOT block or retry. Returns `Ok(())` always (the writer absorbs failures).
    pub fn enqueue_event(
        &self,
        request_id: RequestId,
        channel: ReplayChannel,
        payload_json: String,
    ) -> Result<(), BusEnqueueError> {
        let event_delivery = self.event_delivery();
        let _ =
            enqueue_event_with_delivery(&event_delivery, request_id, channel, payload_json, None);
        Ok(())
    }

    /// Enqueue one bounded proxy attempt for durable persistence. It does not
    /// publish a lifecycle event or transition the parent request, so proxy
    /// observation cannot acquire duplicate terminal ownership.
    pub fn enqueue_proxy_record(&self, record: ProxyRecord) -> Result<(), BusEnqueueError> {
        if self.sink.is_none() {
            return Ok(());
        }

        let Some(record) = sanitize_proxy_record(record) else {
            record_persistence_queue_drop(&self.persistence_queue_drops, &self.metrics);
            return Ok(());
        };
        let proxy_json = match serde_json::to_string(&record) {
            Ok(proxy_json) => proxy_json,
            Err(_) => {
                record_persistence_queue_drop(&self.persistence_queue_drops, &self.metrics);
                return Ok(());
            }
        };
        {
            offer_persistence_to(
                &self.delivery,
                &self.persistence_queue_drops,
                &self.persistence_outstanding,
                &self.metrics,
                PersistenceEntry::ProxyRecord(proxy_json),
            );
        }
        Ok(())
    }

    fn event_delivery(&self) -> EventDelivery {
        EventDelivery {
            bus: Arc::clone(&self.bus),
            registry: Arc::clone(&self.registry),
            metrics: self.metrics.clone(),
            sequences: self.sequences.clone(),
            sink_enabled: self.sink.is_some(),
            clock: Arc::clone(&self.clock),
            delivery: Arc::clone(&self.delivery),
            persistence_queue_drops: Arc::clone(&self.persistence_queue_drops),
            persistence_outstanding: Arc::clone(&self.persistence_outstanding),
        }
    }

    /// Register a new request in the active registry and emit an admitted event on the Requests channel. Returns a LifecycleGuard for tracking terminal transitions.
    pub fn register_request(&self, request_id: RequestId) -> (LifecycleGuard, EventId) {
        self.register_request_with_metadata(request_id, RequestSummaryMetadata::default())
    }

    /// Register a request with metadata available at its trusted ingress boundary.
    pub(crate) fn register_request_with_metadata(
        &self,
        request_id: RequestId,
        metadata: RequestSummaryMetadata,
    ) -> (LifecycleGuard, EventId) {
        let guard =
            LifecycleGuard::for_request(request_id, Arc::downgrade(&self.lifecycle_recorder));

        // Register summary in active set.
        let summary = RequestSummaryEntry {
            request_id: request_id.as_uuid().to_string(),
            state: "active".into(),
            created_at: self.clock.now(),
            terminal_at: None,
            metadata,
        };
        self.registry.register_active(summary.clone());
        if self.sink.is_some() {
            offer_summary_persistence(
                &self.delivery,
                &self.persistence_queue_drops,
                &self.persistence_outstanding,
                &self.metrics,
                summary,
            );
        }

        // The summary must precede the admitted envelope on the single
        // persistence delivery path so the typed lifecycle row can satisfy
        // its SQLite summary foreign key. Returning this exact ID lets callers
        // correlate registration with the canonical envelope and durable row.
        let event_id = enqueue_event_with_delivery(
            &self.event_delivery(),
            request_id,
            ReplayChannel::Requests,
            serde_json::to_string(&LifecycleEvent::Admitted {
                model: None,
                method: None,
            })
            .expect("LifecycleEvent serialization is infallible"),
            None,
        );

        (guard, event_id)
    }

    /// Merge newly available truthful metadata into the request projection.
    /// Evicted or absent requests simply retain no update.
    pub(crate) fn merge_request_metadata(
        &self,
        request_id: RequestId,
        metadata: RequestSummaryMetadata,
    ) {
        let Some(summary) = self
            .registry
            .merge_metadata(&request_id.as_uuid().to_string(), metadata)
        else {
            return;
        };
        if self.sink.is_some() {
            offer_summary_persistence(
                &self.delivery,
                &self.persistence_queue_drops,
                &self.persistence_outstanding,
                &self.metrics,
                summary,
            );
        }
    }

    /// Record the beginning of a transport attempt under an existing request.
    /// The returned branded identifier is used by its completion or failure and
    /// never changes the parent request lifecycle.
    pub fn start_attempt(&self, request_id: RequestId, guard: &LifecycleGuard) -> AttemptId {
        let attempt_id = guard.record_attempt();
        self.enqueue_lifecycle_event(
            request_id,
            ReplayChannel::Operations,
            LifecycleEvent::AttemptStarted {
                attempt_id: Some(attempt_id),
            },
        );
        attempt_id
    }

    /// Record a successful transport attempt without terminalizing its parent request.
    pub fn complete_attempt(
        &self,
        request_id: RequestId,
        attempt_id: AttemptId,
        status_code: Option<u16>,
    ) {
        self.enqueue_lifecycle_event(
            request_id,
            ReplayChannel::Operations,
            LifecycleEvent::AttemptCompleted {
                attempt_id: Some(attempt_id),
                status_code,
            },
        );
    }

    /// Record a failed transport attempt without terminalizing its parent request.
    pub fn fail_attempt(&self, request_id: RequestId, attempt_id: AttemptId, error: String) {
        self.enqueue_lifecycle_event(
            request_id,
            ReplayChannel::Operations,
            LifecycleEvent::AttemptFailed {
                attempt_id: Some(attempt_id),
                error: Some(error),
            },
        );
    }

    /// Transition a request to a terminal outcome. Moves the summary from active → recent in the registry and emits a terminal lifecycle event on the bus. Returns `Err(DuplicateTerminalError)` if already terminated (idempotent rejection).
    pub fn transition_terminal(
        &self,
        request_id: RequestId,
        guard: &LifecycleGuard,
        outcome: TerminalOutcome,
    ) -> Result<(), DuplicateTerminalError> {
        let _ = request_id;
        guard.terminate(outcome)
    }

    fn enqueue_lifecycle_event(
        &self,
        request_id: RequestId,
        channel: ReplayChannel,
        event: LifecycleEvent,
    ) {
        if let Ok(payload) = serde_json::to_string(&event) {
            let _ = self.enqueue_event(request_id, channel, payload);
        }
    }

    /// Write an error audit entry using the fail-open writer's recursion guard. Returns `true` if written, `false` if blocked by recursion detection (caller should proceed silently). Never panics.
    pub fn write_error_audit(&self, message: String) -> bool {
        // Best-effort audit write — fail-open. Use the same canonical System
        // event path as every other record so replay and persistence cannot
        // diverge. The recursion guard prevents self-logging loops.
        let event_delivery = self.event_delivery();

        self.writer.try_record_error(move || {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if let Ok(payload) = serde_json::to_string(&LifecycleEvent::AuditError { message })
                {
                    event_delivery.enqueue(RequestId::new(), ReplayChannel::System, payload);
                }
            }));
        })
    }

    /// Write one bounded operational audit record through the same replay and
    /// persistence hand-off as lifecycle records.
    pub fn write_operational_audit(&self, record: OperationalAuditRecord) -> bool {
        let event_delivery = self.event_delivery();
        self.writer.try_record_error(move || {
            event_delivery.enqueue_audit(record);
        })
    }

    /// Explicit no-worker pump for deterministic tests. It never drains the
    /// replay bus, and it is a no-op while a worker owns delivery. Returns the
    /// number of entries offered to the sink.
    #[allow(dead_code)]
    pub async fn pump_sync(&self) -> usize {
        let (task, completion) = {
            let _transition = self
                .transition_lock
                .lock()
                .expect("transition mutex poisoned");
            let mut abort = self
                .manual_abort_handle
                .lock()
                .expect("manual abort mutex poisoned");
            let mut delivery = self.delivery.lock().expect("delivery mutex poisoned");
            let entries = match &mut *delivery {
                DeliveryMode::Manual { pending, .. } => std::mem::take(pending),
                DeliveryMode::ManualPumping(_)
                | DeliveryMode::Worker(_)
                | DeliveryMode::Stopping
                | DeliveryMode::Stopped => return 0,
            };
            let Some(sink) = self.sink.clone() else {
                return 0;
            };
            if entries.is_empty() {
                return 0;
            }
            let completion = Arc::new(ManualPumpCompletion::new());
            *delivery = DeliveryMode::ManualPumping(Arc::clone(&completion));
            let delivery_state = Arc::clone(&self.delivery);
            let abort_state = Arc::clone(&self.manual_abort_handle);
            let failures = Arc::clone(&self.persistence_failures);
            let outstanding = Arc::clone(&self.persistence_outstanding);
            let metrics = self.metrics.clone();
            let writer = Arc::clone(&self.writer);
            let failure_delivery = self.event_delivery();
            let task_completion = Arc::clone(&completion);
            let task = tokio::spawn(async move {
                let count = entries.len();
                for entry in entries {
                    if Self::process_persistence_entry(sink.as_ref(), &entry)
                        .await
                        .is_err()
                    {
                        increment_persistence_failure(&failures, &metrics);
                        record_persistence_failure(writer.as_ref(), &failure_delivery, &entry);
                    }
                    decrement_outstanding(&outstanding, &metrics);
                }
                task_completion.finish();
                let mut abort = abort_state.lock().expect("manual abort mutex poisoned");
                *abort = None;
                let mut delivery = delivery_state.lock().expect("delivery mutex poisoned");
                if matches!(&*delivery, DeliveryMode::ManualPumping(_)) {
                    *delivery = DeliveryMode::Manual {
                        pending: VecDeque::new(),
                        capacity: PERSISTENCE_QUEUE_CAPACITY,
                    };
                }
                count
            });
            *abort = Some(task.abort_handle());
            (task, completion)
        };
        match task.await {
            Ok(count) => count,
            Err(_) => {
                completion.finish();
                0
            }
        }
    }

    /// Get total rejected entries and writer write drops. Replay-window evictions
    /// are tracked separately by the bus and are not rejected new events.
    #[allow(dead_code)]
    pub fn total_drops(&self) -> u64 {
        self.bus.drops.load(Ordering::Relaxed) + self.writer.write_drops.load(Ordering::Relaxed)
    }

    /// Number of accepted entries dropped because the dedicated persistence
    /// hand-off was full or unavailable. Replay evictions are excluded.
    #[allow(dead_code)]
    pub fn persistence_queue_drops(&self) -> u64 {
        self.persistence_queue_drops.load(Ordering::Relaxed)
    }

    /// Number of persistence attempts rejected by the sink. These failures do
    /// not alter request serving or the replay window.
    #[allow(dead_code)]
    pub fn persistence_failures(&self) -> u64 {
        self.persistence_failures.load(Ordering::Relaxed)
    }

    /// Number of accepted persistence entries abandoned only after a bounded
    /// shutdown drain timed out. Queue saturation and sink failures are
    /// reported separately.
    #[allow(dead_code)]
    pub fn persistence_shutdown_losses(&self) -> u64 {
        self.persistence_shutdown_losses.load(Ordering::Relaxed)
    }

    /// Number of one-time persistence entries currently owned by the manual
    /// queue or worker. This is intended for local health/tests only.
    #[allow(dead_code)]
    pub fn persistence_outstanding(&self) -> u64 {
        self.persistence_outstanding.load(Ordering::Relaxed)
    }

    /// Snapshot the delivery owner's state for trusted-local diagnostics.
    #[allow(dead_code)]
    pub(crate) fn persistence_worker_state(&self) -> PersistenceWorkerState {
        let delivery = self.delivery.lock().expect("delivery mutex poisoned");
        match &*delivery {
            DeliveryMode::Manual { .. } => PersistenceWorkerState::NotStarted,
            DeliveryMode::ManualPumping(_) | DeliveryMode::Worker(_) => {
                PersistenceWorkerState::Running
            }
            DeliveryMode::Stopping => PersistenceWorkerState::Stopping,
            DeliveryMode::Stopped => PersistenceWorkerState::Stopped,
        }
    }

    /// Get the bus for direct access (tests).
    #[allow(dead_code)]
    pub fn bus_ref(&self) -> Arc<ReplayBus> {
        Arc::clone(&self.bus)
    }

    /// Get the registry for direct access (tests).
    #[allow(dead_code)]
    pub fn registry_ref(&self) -> Arc<RequestRegistry> {
        Arc::clone(&self.registry)
    }

    /// Get sequence generators reference.
    #[allow(dead_code)]
    pub fn sequences_ref(&self) -> &SequenceGenerators {
        &self.sequences
    }

    /// Stop accepting persistence hand-offs, drain the worker within a fixed
    /// bound, and join it. Replay remains readable throughout. If the worker
    /// is stalled beyond the bound, it is aborted and every still-owned entry
    /// is recorded as a bounded shutdown loss; serving remains fail-open.
    ///
    /// A completed shutdown leaves the service in a stopped state. Calling
    /// [`Self::spawn`] later starts a fresh worker safely; a second shutdown is
    /// a no-op.
    #[allow(dead_code)]
    pub async fn shutdown(&self) -> bool {
        enum ShutdownOwner {
            Worker(WorkerHandle),
            Manual(VecDeque<PersistenceEntry>),
            ManualPump(Arc<ManualPumpCompletion>, AbortHandle),
            Unavailable,
        }

        // Freeze hand-off and claim exactly one delivery owner under the same
        // transition lock used by spawn/pump. No accepted entry can move into
        // a new owner after this boundary.
        let owner = {
            let _transition = self
                .transition_lock
                .lock()
                .expect("transition mutex poisoned");
            let mut delivery = self.delivery.lock().expect("delivery mutex poisoned");
            let previous = std::mem::replace(&mut *delivery, DeliveryMode::Stopping);
            match previous {
                DeliveryMode::Stopped | DeliveryMode::Stopping => {
                    *delivery = previous;
                    return false;
                }
                DeliveryMode::Worker(_) => {
                    self.spawned.store(false, Ordering::Release);
                    let handle = self
                        .worker_handle
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take();
                    match handle {
                        Some(handle) => ShutdownOwner::Worker(handle),
                        None => ShutdownOwner::Unavailable,
                    }
                }
                DeliveryMode::Manual { pending, .. } => ShutdownOwner::Manual(pending),
                DeliveryMode::ManualPumping(completion) => {
                    let abort = self
                        .manual_abort_handle
                        .lock()
                        .expect("manual abort mutex poisoned")
                        .take();
                    match abort {
                        Some(abort) => ShutdownOwner::ManualPump(completion, abort),
                        None => ShutdownOwner::Manual(VecDeque::new()),
                    }
                }
            }
        };

        let drained = match owner {
            ShutdownOwner::Worker(WorkerHandle { tx, mut task }) => {
                let (drained_tx, drained_rx) = oneshot::channel();
                let result = tokio::time::timeout(self.shutdown_drain_timeout, async {
                    tx.send(WorkerMessage::Shutdown(drained_tx))
                        .await
                        .map_err(|_| ())?;
                    drained_rx.await.map_err(|_| ())?;
                    (&mut task).await.map_err(|_| ())
                })
                .await;
                if result.is_err() || !matches!(result, Ok(Ok(()))) {
                    if !task.is_finished() {
                        task.abort();
                        let _ = task.await;
                    }
                    false
                } else {
                    true
                }
            }
            ShutdownOwner::Manual(entries) => match self.sink.clone() {
                None => true,
                Some(sink) => {
                    let result = tokio::time::timeout(self.shutdown_drain_timeout, async {
                        for entry in entries {
                            if Self::process_persistence_entry(sink.as_ref(), &entry)
                                .await
                                .is_err()
                            {
                                increment_persistence_failure(
                                    &self.persistence_failures,
                                    &self.metrics,
                                );
                                record_persistence_failure(
                                    self.writer.as_ref(),
                                    &self.event_delivery(),
                                    &entry,
                                );
                            }
                            decrement_outstanding(&self.persistence_outstanding, &self.metrics);
                        }
                    })
                    .await;
                    result.is_ok()
                }
            },
            ShutdownOwner::ManualPump(completion, abort) => {
                let result =
                    tokio::time::timeout(self.shutdown_drain_timeout, completion.wait()).await;
                if result.is_err() {
                    abort.abort();
                    false
                } else {
                    true
                }
            }
            ShutdownOwner::Unavailable => false,
        };

        if !drained {
            // Once a bounded owner is cancelled, no later decrement may
            // underflow this total: delivery uses saturating decrement above.
            let lost = self.persistence_outstanding.swap(0, Ordering::AcqRel);
            self.persistence_shutdown_losses
                .fetch_add(lost, Ordering::Relaxed);
            if lost != 0 {
                self.metrics
                    .record(LoggingMetric::PersistenceShutdownLoss { count: lost });
            }
            self.metrics
                .record(LoggingMetric::PersistenceOutstanding { current: 0 });
        }
        self.set_delivery_stopped();
        true
    }

    fn set_delivery_stopping(&self) {
        let mut delivery = self.delivery.lock().expect("delivery mutex poisoned");
        *delivery = DeliveryMode::Stopping;
    }

    fn set_delivery_stopped(&self) {
        let mut delivery = self.delivery.lock().expect("delivery mutex poisoned");
        *delivery = DeliveryMode::Stopped;
    }

    /// Permanently prevent this concrete service from starting another worker.
    ///
    /// The owning [`LoggingRuntimeState`](super::runtime_state::LoggingRuntimeState)
    /// calls this before it begins asynchronous cleanup. This narrow atomic
    /// boundary makes an already-captured service handle safe while replacement
    /// releases global locks and awaits bounded worker shutdown.
    pub(crate) fn retire(&self) {
        self.startable.store(false, Ordering::Release);
    }

    pub(crate) fn is_startable(&self) -> bool {
        self.startable.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn with_shutdown_drain_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_drain_timeout = timeout;
        self
    }

    /// Check if the service is currently spawned and running. For observability / tests.
    #[allow(dead_code)]
    pub fn is_spawned(&self) -> bool {
        self.spawned.load(Ordering::Acquire)
    }

    /// Clone writer for external observation of drop counters.
    #[allow(dead_code)]
    pub fn writer_ref(&self) -> Arc<FailOpenWriter> {
        Arc::clone(&self.writer)
    }

    #[cfg(test)]
    pub(crate) fn worker_handle_lock_for_test(
        &self,
    ) -> std::sync::MutexGuard<'_, Option<WorkerHandle>> {
        self.worker_handle
            .lock()
            .expect("worker handle lock starts healthy")
    }
}

/// Error type returned when bus enqueue fails (shouldn't happen with drop-oldest, but kept for API completeness).
#[derive(Clone, Debug)]
pub enum BusEnqueueError {
    /// The sink is unavailable and the error-audit fallback also failed.
    SinkUnavailable(String),
}

impl std::fmt::Display for BusEnqueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SinkUnavailable(msg) => write!(f, "sink unavailable: {}", msg),
        }
    }
}

impl std::error::Error for BusEnqueueError {}
