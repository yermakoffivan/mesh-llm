//! Host-owned durable logging state.
//!
//! This is intentionally a narrow startup boundary. It opens the durable
//! metadata store and the independently fail-open artifact capture facade,
//! but it does not start workers or instrument request producers.

use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mesh_llm_events::logging::identifiers::EventId;
use mesh_llm_log_store::{
    ARTIFACT_CAPTURE_DISABLED_PRIVACY_UNAVAILABLE, ArtifactCaptureDisabledReason,
    ArtifactCaptureOutcome, ArtifactContent, ArtifactRecord, ArtifactRedactor, Clock as StoreClock,
    EventRecord, FailOpenArtifactCapture, LogStore, LogStoreError, PageQuery, ProxyQuery,
    ProxyRecord, QueryPage, RealClock, RequestQuery, RequestRecord,
};

use super::cleanup::{CleanupOutcome, CleanupWorker, CleanupWorkerState, CleanupWorkerStatus};
use super::foundation::LoggingFoundation;
use super::openai_lifecycle::{OpenAiLifecycleAttachment, OpenAiLifecycleLoggingAdapter};
use super::operator_audit::OperatorAuditWriter;
use super::policy::redact_artifact_bytes;
use super::writer::FailOpenWriter;
use super::{
    ActiveRequestSnapshot, LogStoreSink, LoggingArtifactCaptureStatus, LoggingDynamicLimits,
    LoggingMetricsSink, LoggingService, ManagementRequestLifecycle, RandomWebhookJitter,
    RawMeshLifecycleOwners, RawMeshRemoteSuppressionLease, RawMeshRequestLifecycle,
    RequestSummaryMetadata, ReqwestWebhookTransport, ServiceConfig, SystemClock,
    SystemWebhookWorkerClock, WebhookDeliveryScheduler, WebhookDeliveryWorker,
};

const HEALTH_AUDIT_ACTOR: &str = "logging-runtime";

/// Internal capability state for local logging storage.
///
/// The only artifact-degradation value exposed from this state is the stable,
/// path-free circuit-breaker code. Errors and filesystem locations remain
/// private to the storage implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LoggingRuntimeHealth {
    pub metadata_available: bool,
    pub artifact_capture_available: bool,
    pub artifact_capture_degradation: Option<&'static str>,
}

/// Path-free runtime logging status for trusted-local API consumers.
///
/// This deliberately exposes only fixed state labels, counters, and the
/// stable artifact circuit-breaker code. Storage paths and backend errors stay
/// inside the logging implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LoggingRuntimeStatus {
    pub(crate) metadata_available: bool,
    pub(crate) artifact_capture_available: bool,
    pub(crate) artifact_capture_degradation: Option<&'static str>,
    pub(crate) persistence_worker_state: &'static str,
    pub(crate) persistence_queue_drops: u64,
    pub(crate) persistence_failures: u64,
    pub(crate) persistence_shutdown_losses: u64,
    pub(crate) persistence_outstanding: u64,
    pub(crate) cleanup_worker_state: &'static str,
    pub(crate) cleanup_shutdown_timeouts: u64,
    pub(crate) cleanup_last_outcome: Option<&'static str>,
    pub(crate) cleanup_last_deleted_count: Option<u64>,
}

impl LoggingRuntimeHealth {
    fn unavailable() -> Self {
        Self {
            metadata_available: false,
            artifact_capture_available: false,
            artifact_capture_degradation: None,
        }
    }
}

/// A fully specified artifact write owned by the logging runtime.
///
/// Keeping this request at the boundary prevents callers from reaching into
/// the fail-open capture object and forgetting to publish its one-shot health
/// marker after a privacy failure.
pub struct ArtifactCaptureRequest<'a> {
    pub artifact_id: &'a str,
    pub request_id: &'a str,
    pub kind: &'a str,
    pub occurred_at: &'a str,
    pub content: &'a [u8],
    pub media_kind: Option<&'a str>,
    pub version: u32,
    pub truncated: bool,
    pub byte_limit: usize,
    pub aggregate_limit: usize,
}

/// The process-local durable logging resources created from a foundation.
pub struct LoggingRuntimeState {
    store: Option<Arc<LogStore>>,
    service: Option<Arc<LoggingService>>,
    raw_mesh_owners: Arc<RawMeshLifecycleOwners>,
    artifact_capture: Option<Arc<FailOpenArtifactCapture>>,
    artifact_export_enabled: bool,
    export_limit_bytes: usize,
    health: Mutex<LoggingRuntimeHealth>,
    health_audit_writer: FailOpenWriter,
    operator_audit_writer: Arc<OperatorAuditWriter>,
    cleanup_worker: Mutex<Option<CleanupWorker>>,
    cleanup_status: Arc<Mutex<CleanupWorkerStatus>>,
    webhook_delivery_worker: Mutex<Option<WebhookDeliveryScheduler>>,
    webhook_config: Option<mesh_llm_config::LoggingWebhookConfig>,
    /// Serializes the synchronous start/retire boundary. The asynchronous
    /// cleanup and drain paths deliberately run after this short lock is
    /// released, so no ordinary lock is held across an await.
    activation_lock: Mutex<()>,
    /// Once a process-local state is replaced, any captured `Arc` must be
    /// unable to create another worker.
    retired: AtomicBool,
    cleanup_cadence: Duration,
    retention_max_rows: u64,
    webhook_dead_letter_retention_secs: u64,
    #[cfg(test)]
    cleanup_install_hook: Mutex<Option<Arc<CleanupInstallHook>>>,
    #[cfg(test)]
    cleanup_candidate_count: AtomicUsize,
}

/// Narrow, path-free query and read facade for trusted-local log routes.
///
/// It intentionally owns the active snapshot source, SQLite query substrate,
/// and confined artifact reader together so API code cannot reach into old
/// `ArtifactFileStore` ownership or discover local filesystem paths.
#[derive(Clone)]
pub(crate) struct LoggingQueryFacade {
    store: Arc<LogStore>,
    service: Arc<LoggingService>,
    artifact_capture: Option<Arc<FailOpenArtifactCapture>>,
    artifact_export_enabled: bool,
    export_limit_bytes: usize,
    operator_audit_writer: Arc<OperatorAuditWriter>,
}

impl LoggingQueryFacade {
    pub(crate) fn snapshot_active(&self) -> ActiveRequestSnapshot {
        self.service.registry_ref().snapshot_active()
    }

    pub(crate) fn request(&self, request_id: &str) -> Result<Option<RequestRecord>, LogStoreError> {
        self.store.query_request(request_id)
    }

    pub(crate) fn requests(
        &self,
        query: &RequestQuery,
    ) -> Result<QueryPage<RequestRecord>, LogStoreError> {
        self.store.query_requests(query)
    }

    pub(crate) fn events(
        &self,
        request_id: &str,
        query: &PageQuery,
    ) -> Result<QueryPage<EventRecord>, LogStoreError> {
        self.store.query_events(request_id, query)
    }

    pub(crate) fn artifacts(
        &self,
        request_id: &str,
        query: &PageQuery,
    ) -> Result<QueryPage<ArtifactRecord>, LogStoreError> {
        self.store.query_artifacts(request_id, query)
    }

    pub(crate) fn artifact(
        &self,
        artifact_id: &str,
    ) -> Result<Option<ArtifactRecord>, LogStoreError> {
        self.store.query_artifact(artifact_id)
    }

    pub(crate) fn proxy_records(
        &self,
        query: &ProxyQuery,
    ) -> Result<QueryPage<ProxyRecord>, LogStoreError> {
        self.store.query_proxy_records(query)
    }

    /// Read content only through the fail-open capture owner. The route core
    /// verifies `ArtifactRecord::redacted` before calling this method.
    pub(crate) fn read_artifact(
        &self,
        artifact_id: &str,
    ) -> Result<ArtifactContent, LogStoreError> {
        let Some(capture) = &self.artifact_capture else {
            return Err(LogStoreError::ArtifactMissing {
                artifact_id: artifact_id.to_string(),
            });
        };
        capture.read_artifact(artifact_id)
    }

    /// Whether a trusted-local export may include redacted artifact bytes.
    /// Metadata queries do not need this opt-in because they never read files.
    pub(crate) const fn artifact_export_enabled(&self) -> bool {
        self.artifact_export_enabled
    }

    /// The configured upper bound for one operator export response.
    pub(crate) const fn export_limit_bytes(&self) -> usize {
        self.export_limit_bytes
    }

    /// Persist one operator action without routing its failure through the
    /// logging service. The shared recursion guard keeps an audit-store error
    /// from recursively creating audit records of its own.
    pub(crate) fn write_operator_audit(
        &self,
        action: &'static str,
        reason: String,
        result: &'static str,
    ) -> Result<(), LogStoreError> {
        self.operator_audit_writer
            .write(Arc::clone(&self.store), action, reason, result)
    }

    pub(crate) fn preview_cleanup(
        &self,
        request: &mesh_llm_log_store::CleanupPreviewRequest,
        control: &dyn mesh_llm_log_store::MaintenanceExecutionControl,
    ) -> Result<mesh_llm_log_store::MaintenanceReceipt, LogStoreError> {
        self.store.preview_cleanup(request, control)
    }

    pub(crate) fn execute_cleanup(
        &self,
        operation_id: mesh_llm_log_store::MaintenanceOperationId,
        reason: &mesh_llm_log_store::MaintenanceReason,
        control: &dyn mesh_llm_log_store::MaintenanceExecutionControl,
    ) -> Result<mesh_llm_log_store::MaintenanceReceipt, LogStoreError> {
        let Some(capture) = &self.artifact_capture else {
            return Err(LogStoreError::MaintenanceExecutionCancelled);
        };
        capture.execute_cleanup(operation_id, reason, control)
    }

    /// Delete exactly one terminal durable request through the confined
    /// artifact owner. The API layer never receives filesystem paths.
    pub(crate) fn delete_request_cascade(
        &self,
        request: &mesh_llm_log_store::DeleteOneRequest,
        control: &dyn mesh_llm_log_store::MaintenanceExecutionControl,
    ) -> Result<mesh_llm_log_store::MaintenanceReceipt, LogStoreError> {
        let Some(capture) = &self.artifact_capture else {
            return Err(LogStoreError::MaintenanceExecutionCancelled);
        };
        capture.delete_request_cascade(request, control)
    }

    /// Load a matching delete-one receipt before checking its former request
    /// owner, which may have been removed by the original operation.
    pub(crate) fn delete_one_receipt(
        &self,
        request: &mesh_llm_log_store::DeleteOneRequest,
    ) -> Result<Option<mesh_llm_log_store::MaintenanceReceipt>, LogStoreError> {
        self.store.delete_one_receipt(request)
    }

    /// Transition one dead-letter delivery into the separately auditable
    /// manual-retry state. The route only receives the boolean transition
    /// outcome; endpoint and payload material remain private to the worker.
    pub(crate) fn manually_retry_webhook_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<bool, LogStoreError> {
        self.store
            .manually_retry_webhook_delivery(delivery_id, &self.store.now())
    }

    /// Load the state needed to make a manual-retry response idempotent. The
    /// API reduces this record to a fixed outcome label and never serializes
    /// its delivery metadata.
    pub(crate) fn webhook_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<Option<mesh_llm_log_store::WebhookDeliveryRecord>, LogStoreError> {
        self.store.webhook_delivery(delivery_id)
    }
}

/// Deterministic test-only pause after the scheduler is atomically published
/// and before its starter awaits readiness. Two barriers make replacement
/// cancel and join that installed scheduler without relying on wall-clock
/// scheduling.
#[cfg(test)]
struct CleanupInstallHook {
    candidate_created: tokio::sync::Barrier,
    resume_install: tokio::sync::Barrier,
}

#[cfg(test)]
impl CleanupInstallHook {
    fn new() -> Self {
        Self {
            candidate_created: tokio::sync::Barrier::new(2),
            resume_install: tokio::sync::Barrier::new(2),
        }
    }
}

impl LoggingRuntimeState {
    /// Open/migrate the local store and initialize independent artifact capture.
    ///
    /// A failure at either layer is fail-open for serving. In particular,
    /// artifact privacy failure never removes the already-open metadata store.
    pub fn initialize(
        foundation: &LoggingFoundation,
        config: &mesh_llm_config::LoggingConfig,
    ) -> Self {
        Self::initialize_with_capture_opener(foundation, config, |artifact_root, clock, store| {
            FailOpenArtifactCapture::open(
                artifact_root,
                clock,
                store,
                canonical_artifact_redactor(),
            )
        })
    }

    fn initialize_with_capture_opener<F>(
        foundation: &LoggingFoundation,
        config: &mesh_llm_config::LoggingConfig,
        open_capture: F,
    ) -> Self
    where
        F: FnOnce(
            PathBuf,
            Arc<dyn StoreClock>,
            Arc<LogStore>,
        ) -> Result<FailOpenArtifactCapture, LogStoreError>,
    {
        if !foundation.is_healthy() {
            tracing::warn!("Logging durable storage unavailable; continuing without logging");
            return Self::unavailable();
        }

        Self::initialize_healthy_foundation(foundation, config, open_capture)
    }

    fn initialize_healthy_foundation<F>(
        foundation: &LoggingFoundation,
        config: &mesh_llm_config::LoggingConfig,
        open_capture: F,
    ) -> Self
    where
        F: FnOnce(
            PathBuf,
            Arc<dyn StoreClock>,
            Arc<LogStore>,
        ) -> Result<FailOpenArtifactCapture, LogStoreError>,
    {
        let clock: Arc<dyn StoreClock> = Arc::new(RealClock);
        Self::initialize_healthy_foundation_with_clock(foundation, config, clock, open_capture)
    }

    fn initialize_healthy_foundation_with_clock<F>(
        foundation: &LoggingFoundation,
        config: &mesh_llm_config::LoggingConfig,
        clock: Arc<dyn StoreClock>,
        open_capture: F,
    ) -> Self
    where
        F: FnOnce(
            PathBuf,
            Arc<dyn StoreClock>,
            Arc<LogStore>,
        ) -> Result<FailOpenArtifactCapture, LogStoreError>,
    {
        let Some(store) = Self::open_metadata_store(foundation, &clock) else {
            return Self::unavailable();
        };
        let artifact_capture =
            Self::open_artifact_capture(foundation, clock, &store, open_capture).map(Arc::new);
        Self::from_open_store(store, artifact_capture, config)
    }

    fn open_metadata_store(
        foundation: &LoggingFoundation,
        clock: &Arc<dyn StoreClock>,
    ) -> Option<Arc<LogStore>> {
        match LogStore::open(foundation.store_dir(), Arc::clone(clock)) {
            Ok(store) => Some(Arc::new(store)),
            Err(_) => {
                tracing::warn!("Logging durable storage unavailable; continuing without logging");
                None
            }
        }
    }

    fn open_artifact_capture<F>(
        foundation: &LoggingFoundation,
        clock: Arc<dyn StoreClock>,
        store: &Arc<LogStore>,
        open_capture: F,
    ) -> Option<FailOpenArtifactCapture>
    where
        F: FnOnce(
            PathBuf,
            Arc<dyn StoreClock>,
            Arc<LogStore>,
        ) -> Result<FailOpenArtifactCapture, LogStoreError>,
    {
        match open_capture(
            foundation.artifact_dir().to_path_buf(),
            clock,
            Arc::clone(store),
        ) {
            Ok(capture) => Some(capture),
            Err(_) => {
                tracing::warn!(
                    "Logging artifact capture unavailable; metadata logging remains enabled"
                );
                None
            }
        }
    }

    fn from_open_store(
        store: Arc<LogStore>,
        artifact_capture: Option<Arc<FailOpenArtifactCapture>>,
        config: &mesh_llm_config::LoggingConfig,
    ) -> Self {
        let artifact_capture_available = artifact_capture
            .as_ref()
            .is_some_and(|capture| !capture.is_disabled());
        let persistence_sink = if config.webhook.enabled {
            LogStoreSink::with_terminal_webhook_enqueue(
                Arc::clone(&store),
                config.webhook.max_attempts,
            )
        } else {
            LogStoreSink::new(Arc::clone(&store))
        };
        let state = Self {
            service: Some(Arc::new(LoggingService::new_with_dynamic_limits(
                ServiceConfig {
                    queue_capacity: config.queue_capacity as usize,
                    ..ServiceConfig::default()
                },
                Arc::new(persistence_sink),
                Box::new(SystemClock),
                LoggingDynamicLimits::from_config(config),
            ))),
            raw_mesh_owners: Arc::new(RawMeshLifecycleOwners::default()),
            store: Some(store),
            artifact_capture,
            artifact_export_enabled: matches!(
                config.artifact.capture_mode,
                mesh_llm_config::CaptureMode::RedactedArtifacts
            ) && artifact_capture_available,
            export_limit_bytes: config.export_limit_bytes as usize,
            health: Mutex::new(LoggingRuntimeHealth {
                metadata_available: true,
                artifact_capture_available,
                artifact_capture_degradation: None,
            }),
            health_audit_writer: FailOpenWriter::new(),
            operator_audit_writer: Arc::new(OperatorAuditWriter::new()),
            cleanup_worker: Mutex::new(None),
            cleanup_status: Arc::new(Mutex::new(CleanupWorkerStatus::default())),
            webhook_delivery_worker: Mutex::new(None),
            webhook_config: config.webhook.enabled.then(|| config.webhook.clone()),
            activation_lock: Mutex::new(()),
            retired: AtomicBool::new(false),
            cleanup_cadence: Duration::from_secs(config.cleanup_cadence_secs),
            retention_max_rows: config.retention_max_rows,
            webhook_dead_letter_retention_secs: config.webhook.dead_letter_retention_secs,
            #[cfg(test)]
            cleanup_install_hook: Mutex::new(None),
            #[cfg(test)]
            cleanup_candidate_count: AtomicUsize::new(0),
        };
        state.consume_artifact_capture_health_marker();
        state
    }

    fn unavailable() -> Self {
        Self {
            store: None,
            service: None,
            raw_mesh_owners: Arc::new(RawMeshLifecycleOwners::default()),
            artifact_capture: None,
            artifact_export_enabled: false,
            export_limit_bytes: mesh_llm_config::LoggingConfig::default().export_limit_bytes
                as usize,
            health: Mutex::new(LoggingRuntimeHealth::unavailable()),
            health_audit_writer: FailOpenWriter::new(),
            operator_audit_writer: Arc::new(OperatorAuditWriter::new()),
            cleanup_worker: Mutex::new(None),
            cleanup_status: Arc::new(Mutex::new(CleanupWorkerStatus::default())),
            webhook_delivery_worker: Mutex::new(None),
            webhook_config: None,
            activation_lock: Mutex::new(()),
            retired: AtomicBool::new(false),
            cleanup_cadence: Duration::from_secs(
                mesh_llm_config::LoggingConfig::default().cleanup_cadence_secs,
            ),
            retention_max_rows: mesh_llm_config::LoggingConfig::default().retention_max_rows,
            webhook_dead_letter_retention_secs: mesh_llm_config::LoggingConfig::default()
                .webhook
                .dead_letter_retention_secs,
            #[cfg(test)]
            cleanup_install_hook: Mutex::new(None),
            #[cfg(test)]
            cleanup_candidate_count: AtomicUsize::new(0),
        }
    }

    /// Return the internal health/capability projection without filesystem details.
    pub fn health(&self) -> LoggingRuntimeHealth {
        *self
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Install the optional process-local telemetry adapter after runtime
    /// configuration has explicitly enabled telemetry. Logging remains fully
    /// independent of the adapter and unavailable state stays fail-open.
    pub(crate) fn set_metrics_sink(&self, sink: Option<Arc<dyn LoggingMetricsSink>>) {
        if self.retired.load(Ordering::Acquire) {
            return;
        }
        if let Some(service) = self.service.as_ref() {
            service.set_metrics_sink(sink);
        }
    }

    /// Return a fixed-label, path-free snapshot suitable for trusted-local
    /// management status. This is intentionally not a mesh capability.
    pub(crate) fn status(&self) -> LoggingRuntimeStatus {
        let health = self.health();
        let cleanup = *self
            .cleanup_status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (
            persistence_worker_state,
            persistence_queue_drops,
            persistence_failures,
            persistence_shutdown_losses,
            persistence_outstanding,
        ) = self
            .service
            .as_ref()
            .map_or(("unavailable", 0, 0, 0, 0), |service| {
                (
                    service.persistence_worker_state().label(),
                    service.persistence_queue_drops(),
                    service.persistence_failures(),
                    service.persistence_shutdown_losses(),
                    service.persistence_outstanding(),
                )
            });

        LoggingRuntimeStatus {
            metadata_available: health.metadata_available,
            artifact_capture_available: health.artifact_capture_available,
            artifact_capture_degradation: health.artifact_capture_degradation,
            persistence_worker_state,
            persistence_queue_drops,
            persistence_failures,
            persistence_shutdown_losses,
            persistence_outstanding,
            cleanup_worker_state: cleanup_worker_state_label(cleanup.state),
            cleanup_shutdown_timeouts: cleanup.shutdown_timeouts,
            cleanup_last_outcome: cleanup.last_outcome.map(|outcome| outcome.code()),
            cleanup_last_deleted_count: cleanup
                .last_outcome
                .and_then(|outcome| outcome.deleted_count()),
        }
    }

    /// Attempt an artifact write while publishing a one-time privacy failure.
    pub fn write_artifact(
        &self,
        request: ArtifactCaptureRequest<'_>,
    ) -> Result<ArtifactCaptureOutcome, LogStoreError> {
        // Redact before bytes leave the host-owned boundary. The capture store
        // applies the same mandatory redactor again as defense in depth.
        let redacted_content = redact_artifact_bytes(request.content);
        let outcome = match self.artifact_capture.as_ref() {
            Some(capture) => capture.write_artifact(
                request.artifact_id,
                request.request_id,
                request.kind,
                request.occurred_at,
                &redacted_content,
                request.media_kind,
                request.version,
                true,
                request.truncated,
                request.byte_limit,
                request.aggregate_limit,
            ),
            None => Ok(ArtifactCaptureOutcome::Disabled(
                ArtifactCaptureDisabledReason,
            )),
        };
        let status = match &outcome {
            Ok(ArtifactCaptureOutcome::Written(_)) => LoggingArtifactCaptureStatus::Written,
            Ok(ArtifactCaptureOutcome::Disabled(_)) => LoggingArtifactCaptureStatus::Disabled,
            Err(_) => LoggingArtifactCaptureStatus::Failed,
        };
        if let Some(service) = self.service.as_ref() {
            service.record_artifact_capture_status(status);
        }
        self.consume_artifact_capture_health_marker();
        outcome
    }

    /// Access to the typed store stays internal to host runtime ownership.
    pub(crate) fn store(&self) -> Option<Arc<LogStore>> {
        self.store.clone()
    }

    /// Create the only query/read handle used by trusted-local log API code.
    /// Disabled or degraded metadata storage yields no facade; artifact
    /// degradation alone still yields a facade for metadata-only queries.
    pub(crate) fn query_facade(&self) -> Option<LoggingQueryFacade> {
        Some(LoggingQueryFacade {
            store: self.store.clone()?,
            service: self.service.clone()?,
            artifact_capture: self.artifact_capture.clone(),
            artifact_export_enabled: self.artifact_export_enabled,
            export_limit_bytes: self.export_limit_bytes,
            operator_audit_writer: Arc::clone(&self.operator_audit_writer),
        })
    }

    /// Return the bounded semantic replay source without exposing the logging
    /// service's persistence or registry internals to the HTTP adapter.
    pub(crate) fn replay_bus(&self) -> Option<Arc<super::bus::ReplayBus>> {
        self.service.as_ref().map(|service| service.bus_ref())
    }

    /// Snapshot the host-owned OpenAI lifecycle observer for one frontend
    /// server instance. A disabled, unavailable, or retired runtime stays
    /// absent so request serving continues without logging.
    pub(crate) fn openai_lifecycle_observer(
        &self,
    ) -> Option<Arc<dyn openai_frontend::OpenAiLifecycleObserver>> {
        if self.retired.load(Ordering::Acquire) {
            return None;
        }
        let service = Arc::clone(self.service.as_ref()?);
        if !service.is_startable() {
            return None;
        }
        Some(Arc::new(OpenAiLifecycleLoggingAdapter::new(
            service,
            Arc::clone(&self.raw_mesh_owners),
        )))
    }

    /// Claim the metadata-only parent lifecycle for one raw mesh ingress
    /// request. The matching embedded frontend observer consults the same
    /// ownership registry and does not register a competing parent.
    pub(crate) fn register_raw_mesh_request(
        &self,
        request_id: mesh_llm_events::logging::identifiers::RequestId,
    ) -> Option<RawMeshRequestLifecycle> {
        self.register_raw_mesh_request_with_metadata(request_id, RequestSummaryMetadata::default())
    }

    fn register_raw_mesh_request_with_metadata(
        &self,
        request_id: mesh_llm_events::logging::identifiers::RequestId,
        metadata: RequestSummaryMetadata,
    ) -> Option<RawMeshRequestLifecycle> {
        if self.retired.load(Ordering::Acquire) {
            return None;
        }
        let service = Arc::clone(self.service.as_ref()?);
        if !service.is_startable() {
            return None;
        }
        RawMeshRequestLifecycle::register_with_metadata(
            service,
            Arc::clone(&self.raw_mesh_owners),
            request_id,
            metadata,
        )
    }

    /// Attach one parsed host OpenAI ingress to the canonical parent owner.
    ///
    /// The returned attachment remains usable when logging is unavailable; its
    /// route observer is then empty and all dispatch instrumentation fails open.
    pub(crate) fn openai_ingress_attachment(
        &self,
        request_id: mesh_llm_events::logging::identifiers::RequestId,
        metadata: RequestSummaryMetadata,
    ) -> OpenAiLifecycleAttachment {
        OpenAiLifecycleAttachment::new(
            self.register_raw_mesh_request_with_metadata(request_id, metadata),
        )
    }

    /// Suppress a duplicate embedded-frontend parent for one trusted remote
    /// HTTP tunnel. This is intentionally a lease only: it does not register
    /// any lifecycle events and failures stay fail-open for request serving.
    pub(crate) fn suppress_remote_tunneled_request(
        &self,
        request_id: mesh_llm_events::logging::identifiers::RequestId,
    ) -> Option<RawMeshRemoteSuppressionLease> {
        if self.retired.load(Ordering::Acquire) {
            return None;
        }
        let service = self.service.as_ref()?;
        if !service.is_startable() {
            return None;
        }
        RawMeshRemoteSuppressionLease::acquire(Arc::clone(&self.raw_mesh_owners), request_id)
    }

    /// Register one already-parsed management API request with bounded
    /// metadata. Disabled, unavailable, and retired logging stay fail-open.
    pub(crate) fn register_management_request(
        &self,
        request_id: mesh_llm_events::logging::identifiers::RequestId,
        method_route: &'static str,
    ) -> Option<ManagementRequestLifecycle> {
        if self.retired.load(Ordering::Acquire) {
            return None;
        }
        let service = Arc::clone(self.service.as_ref()?);
        if !service.is_startable() {
            return None;
        }
        Some(ManagementRequestLifecycle::register(
            service,
            request_id,
            method_route,
        ))
    }

    /// Record one static operational audit without exposing the service to
    /// producers. Disabled, unavailable, and retired logging remain fail-open
    /// for the calling runtime path.
    pub(crate) fn write_operational_audit(
        &self,
        level: &'static str,
        message: &'static str,
    ) -> bool {
        if self.retired.load(Ordering::Acquire) {
            return false;
        }
        self.service.as_ref().is_some_and(|service| {
            service.is_startable() && service.write_operational_audit(level, message)
        })
    }

    #[cfg(test)]
    pub(crate) fn service_for_test(&self) -> Option<Arc<LoggingService>> {
        self.service.clone()
    }

    /// Apply the only two logging settings whose schema permits live mutation.
    /// A disabled or fail-open runtime has no service, so callers can truthfully
    /// report the config as staged instead of claiming a live update.
    pub fn apply_dynamic_limits(
        &self,
        limits: LoggingDynamicLimits,
    ) -> Result<(), LoggingRuntimeApplyError> {
        let Some(service) = self.service.as_ref() else {
            return Err(LoggingRuntimeApplyError::Unavailable);
        };
        service.apply_dynamic_limits(limits);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn dynamic_limits(&self) -> Option<LoggingDynamicLimits> {
        self.service
            .as_ref()
            .map(|service| service.dynamic_limits())
    }

    /// Start the durable persistence worker after the runtime startup boundary
    /// has finished opening the confined store and artifact capture facade.
    ///
    /// `FailOpenArtifactCapture::open` runs its idempotent startup recovery
    /// before this state is constructed, so no producer can hand work to this
    /// service before recovery has completed. The underlying service start is
    /// idempotent, which keeps repeated embedded/runtime entrypoints from
    /// creating duplicate workers for one installed state.
    pub(crate) async fn start_persistence_worker(&self) -> Option<Arc<LoggingService>> {
        // Keep the synchronous activation boundary short. SQLite cleanup runs
        // on the blocking pool and startup must await its result before this
        // service is published as ready, but neither operation may hold this
        // mutex across an await.
        let service = {
            let _activation = self
                .activation_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.retired.load(Ordering::Acquire) {
                return None;
            }
            let service = Arc::clone(self.service.as_ref()?);
            let _ = service.spawn();
            if !service.is_spawned() {
                return None;
            }
            service
        };

        self.start_cleanup_worker(&service).await;
        self.start_webhook_delivery_worker();

        // Retirement can win while the bounded startup cleanup awaits.  Do
        // not hand a caller a service from a displaced runtime state.
        if self.retired.load(Ordering::Acquire) || !service.is_startable() {
            return None;
        }
        Some(service)
    }

    /// Start the opt-in durable-delivery scheduler only after the logging
    /// service has crossed its persistence startup boundary. Configuration or
    /// client construction failures remain local and fail open for serving.
    fn start_webhook_delivery_worker(&self) {
        let Some(config) = self.webhook_config.clone() else {
            return;
        };
        let Some(store) = self.store() else {
            return;
        };
        let Some(metrics) = self.service.as_ref().map(|service| service.metrics()) else {
            return;
        };
        let _activation = self
            .activation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.retired.load(Ordering::Acquire) {
            return;
        }
        let mut installed = self
            .webhook_delivery_worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if installed.is_some() {
            return;
        }
        let transport = match ReqwestWebhookTransport::new() {
            Ok(transport) => Arc::new(transport),
            Err(_) => {
                tracing::warn!(
                    "webhook delivery transport unavailable; continuing without dispatch"
                );
                return;
            }
        };
        let worker = match WebhookDeliveryWorker::from_config(
            store,
            &config,
            transport,
            Arc::new(SystemWebhookWorkerClock),
            Arc::new(RandomWebhookJitter),
        ) {
            Ok(worker) => worker.with_metrics(metrics),
            Err(_) => {
                tracing::warn!(
                    "webhook delivery configuration unavailable; continuing without dispatch"
                );
                return;
            }
        };
        *installed = Some(WebhookDeliveryScheduler::start(worker));
    }

    async fn start_cleanup_worker(&self, service: &Arc<LoggingService>) {
        let Some(store) = self.store() else {
            return;
        };

        let startup_waiter = self.create_and_publish_cleanup_worker(store, service);

        let Some(startup_waiter) = startup_waiter else {
            return;
        };
        // The test hook deliberately pauses only after the scheduler has been
        // published while holding the activation gate. This preserves an
        // observable retirement/cancellation boundary without reopening the
        // concurrent-start candidate race.
        self.pause_cleanup_installation_for_test().await;
        // Cleanup failure is deliberately fail-open: status/audit records the
        // result while the request-serving runtime still comes up.
        let _ = CleanupWorker::wait_for_startup_with(startup_waiter).await;
    }

    /// Return the canonical scheduler's readiness watch. Candidate creation
    /// and handle publication share the activation gate with retirement, so a
    /// concurrent caller can only observe the published worker; it can never
    /// spawn a losing task that races cleanup or overwrites shared status.
    fn create_and_publish_cleanup_worker(
        &self,
        store: Arc<LogStore>,
        service: &Arc<LoggingService>,
    ) -> Option<tokio::sync::watch::Receiver<Option<CleanupOutcome>>> {
        // This is the same gate retirement takes before releasing the state
        // for asynchronous shutdown. There is no await in this critical
        // section: task construction, the ownership check, and publication
        // form one atomic transition.
        let _activation = self
            .activation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.retired.load(Ordering::Acquire) || !service.is_startable() {
            return None;
        }

        let mut installed = self
            .cleanup_worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(worker) = installed.as_ref() {
            return Some(worker.startup_waiter());
        }

        #[cfg(test)]
        self.cleanup_candidate_count.fetch_add(1, Ordering::Relaxed);
        let candidate = CleanupWorker::start(
            store,
            self.artifact_capture.clone(),
            Arc::clone(service),
            self.retention_max_rows,
            self.webhook_dead_letter_retention_secs,
            self.cleanup_cadence,
            Arc::clone(&self.cleanup_status),
        );
        let startup_waiter = candidate.startup_waiter();
        *installed = Some(candidate);
        Some(startup_waiter)
    }

    /// Stop the scheduler before the service persistence worker. This ordering
    /// prevents a late cleanup audit from being offered after service shutdown.
    pub(crate) async fn shutdown_cleanup_worker(&self) {
        let worker = self
            .cleanup_worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker
            && !worker.shutdown().await
        {
            // A timed-out cleanup task retains its exclusive connection
            // and may still be unwinding after interruption. Keep the
            // owner installed so no replacement claims it stopped or
            // starts a concurrent scheduler on this runtime state.
            *self
                .cleanup_worker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker);
        }
    }

    /// Stop the terminal-delivery scheduler only after persistence has drained
    /// its final terminal records. The scheduler's own fixed join bound keeps
    /// shutdown finite; unfinished leased rows remain durable for restart
    /// recovery.
    pub(crate) async fn shutdown_webhook_delivery_worker(&self) {
        let worker = self
            .webhook_delivery_worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            worker.shutdown().await;
        }
    }

    /// Make this installed state permanently non-startable, then stop all of
    /// its background work in the required order. This is used only by the
    /// process-global replacement boundary; ordinary runtime shutdown may use
    /// the individual worker methods while leaving its state inspectable.
    /// Returns false if cleanup did not retire within its fixed bound. In that
    /// case callers must preserve this retired state rather than installing a
    /// replacement scheduler over a still-owned cleanup connection.
    pub(crate) async fn retire_and_shutdown(&self) -> bool {
        {
            let _activation = self
                .activation_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.retired.store(true, Ordering::Release);
            if let Some(service) = self.service.as_ref() {
                service.retire();
            }
        }

        // Cleanup may emit an audit entry, so it must be joined before the
        // persistence drain closes its delivery boundary.
        self.shutdown_cleanup_worker().await;
        if self.status().cleanup_worker_state == "timed_out" {
            self.shutdown_webhook_delivery_worker().await;
            return false;
        }
        if let Some(service) = self.service.as_ref() {
            let _ = service.shutdown().await;
        }
        self.shutdown_webhook_delivery_worker().await;
        true
    }

    #[cfg(test)]
    pub(crate) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn install_cleanup_publish_hook_for_test(&self) -> Arc<CleanupInstallHook> {
        let hook = Arc::new(CleanupInstallHook::new());
        *self
            .cleanup_install_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&hook));
        hook
    }

    #[cfg(test)]
    fn has_cleanup_worker_for_test(&self) -> bool {
        self.cleanup_worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    #[cfg(test)]
    pub(crate) fn has_webhook_delivery_worker_for_test(&self) -> bool {
        self.webhook_delivery_worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    #[cfg(test)]
    fn cleanup_candidate_count_for_test(&self) -> usize {
        self.cleanup_candidate_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    async fn pause_cleanup_installation_for_test(&self) {
        let hook = self
            .cleanup_install_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook.candidate_created.wait().await;
            hook.resume_install.wait().await;
        }
    }

    #[cfg(not(test))]
    async fn pause_cleanup_installation_for_test(&self) {}

    fn consume_artifact_capture_health_marker(&self) {
        let Some(capture) = self.artifact_capture.as_ref() else {
            return;
        };
        let Some(marker) = capture.take_health_marker() else {
            return;
        };
        let code = marker.reason().code();
        debug_assert_eq!(code, ARTIFACT_CAPTURE_DISABLED_PRIVACY_UNAVAILABLE);

        {
            let mut health = self
                .health
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            health.artifact_capture_available = false;
            health.artifact_capture_degradation = Some(code);
        }
        self.record_sanitized_health_audit(code);
    }

    fn record_sanitized_health_audit(&self, code: &'static str) {
        let Some(store) = self.store() else {
            return;
        };
        let action = code.to_string();
        let _ = self.health_audit_writer.try_record_error(move || {
            let entry_id = EventId::new().as_uuid().to_string();
            let occurred_at = store.now();
            let _ = store.insert_audit_entry(
                &entry_id,
                None,
                &occurred_at,
                HEALTH_AUDIT_ACTOR,
                &action,
                None,
            );
        });
    }

    #[cfg(test)]
    fn initialize_with_capture_opener_for_test<F>(
        foundation: &LoggingFoundation,
        config: &mesh_llm_config::LoggingConfig,
        open_capture: F,
    ) -> Self
    where
        F: FnOnce(
            PathBuf,
            Arc<dyn StoreClock>,
            Arc<LogStore>,
        ) -> Result<FailOpenArtifactCapture, LogStoreError>,
    {
        Self::initialize_with_capture_opener(foundation, config, open_capture)
    }

    #[cfg(test)]
    pub(crate) fn initialize_with_store_clock_for_test(
        foundation: &LoggingFoundation,
        config: &mesh_llm_config::LoggingConfig,
        clock: Arc<dyn StoreClock>,
    ) -> Self {
        if !foundation.is_healthy() {
            return Self::unavailable();
        }
        Self::initialize_healthy_foundation_with_clock(
            foundation,
            config,
            clock,
            |artifact_root, clock, store| {
                FailOpenArtifactCapture::open(
                    artifact_root,
                    clock,
                    store,
                    canonical_artifact_redactor(),
                )
            },
        )
    }
}

const fn cleanup_worker_state_label(state: CleanupWorkerState) -> &'static str {
    match state {
        CleanupWorkerState::NotStarted => "not_started",
        CleanupWorkerState::Running => "running",
        CleanupWorkerState::Stopping => "stopping",
        CleanupWorkerState::TimedOut => "timed_out",
        CleanupWorkerState::Stopped => "stopped",
    }
}

/// The path-free reason a live config apply could not reach a running service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoggingRuntimeApplyError {
    Unavailable,
}

impl std::fmt::Display for LoggingRuntimeApplyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("logging runtime is unavailable for live configuration apply")
    }
}

fn canonical_artifact_redactor() -> ArtifactRedactor {
    Arc::new(redact_artifact_bytes)
}

#[cfg(test)]
mod tests {
    use mesh_llm_log_store::ArtifactPrivacy;
    use std::path::Path;

    use super::*;

    #[derive(Default)]
    struct RejectPrivacy;

    impl ArtifactPrivacy for RejectPrivacy {
        fn prepare_directory(&self, _path: &Path) -> Result<(), LogStoreError> {
            Err(LogStoreError::PrivacyNotGuaranteed)
        }

        fn prepare_file(&self, _path: &Path) -> Result<(), LogStoreError> {
            Err(LogStoreError::PrivacyNotGuaranteed)
        }
    }

    #[derive(Default)]
    struct RejectArtifactFiles;

    impl ArtifactPrivacy for RejectArtifactFiles {
        fn prepare_directory(&self, _path: &Path) -> Result<(), LogStoreError> {
            Ok(())
        }

        fn prepare_file(&self, _path: &Path) -> Result<(), LogStoreError> {
            Err(LogStoreError::PrivacyNotGuaranteed)
        }
    }

    struct FixedStoreClock(&'static str);

    impl StoreClock for FixedStoreClock {
        fn now(&self) -> String {
            self.0.to_string()
        }
    }

    fn marker_audit_count(store: &LogStore) -> i64 {
        store
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM audit_entries WHERE action = ?",
                [ARTIFACT_CAPTURE_DISABLED_PRIVACY_UNAVAILABLE],
                |row| row.get(0),
            )
            .expect("count marker audits")
    }

    #[tokio::test]
    async fn disabled_webhook_config_starts_no_delivery_scheduler() {
        let root = tempfile::tempdir().expect("temporary logging root");
        let foundation = LoggingFoundation::init(true, Some(&root.path().to_path_buf()));
        let state = LoggingRuntimeState::initialize(&foundation, &Default::default());

        let service = state
            .start_persistence_worker()
            .await
            .expect("logging service starts");

        assert!(!state.has_webhook_delivery_worker_for_test());
        state.shutdown_cleanup_worker().await;
        assert!(service.shutdown().await);
    }

    #[tokio::test]
    async fn enabled_webhook_config_starts_and_retires_one_delivery_scheduler() {
        let root = tempfile::tempdir().expect("temporary logging root");
        let foundation = LoggingFoundation::init(true, Some(&root.path().to_path_buf()));
        let config = mesh_llm_config::LoggingConfig {
            webhook: mesh_llm_config::LoggingWebhookConfig {
                enabled: true,
                url: Some("http://127.0.0.1:9444/webhook".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let state = LoggingRuntimeState::initialize(&foundation, &config);

        state
            .start_persistence_worker()
            .await
            .expect("logging service starts");
        assert!(state.has_webhook_delivery_worker_for_test());

        assert!(state.retire_and_shutdown().await);
        assert!(!state.has_webhook_delivery_worker_for_test());
    }

    #[test]
    fn privacy_failure_disables_only_artifacts_and_records_one_sanitized_marker() {
        let root = tempfile::tempdir().expect("temporary logging root");
        let foundation = LoggingFoundation::init(true, Some(&root.path().to_path_buf()));
        let state = LoggingRuntimeState::initialize_with_capture_opener_for_test(
            &foundation,
            &mesh_llm_config::LoggingConfig::default(),
            |artifact_root, clock, store| {
                FailOpenArtifactCapture::open_with_privacy(
                    artifact_root,
                    clock,
                    store,
                    canonical_artifact_redactor(),
                    Arc::new(RejectPrivacy),
                )
            },
        );

        assert_eq!(
            state.health(),
            LoggingRuntimeHealth {
                metadata_available: true,
                artifact_capture_available: false,
                artifact_capture_degradation: Some(ARTIFACT_CAPTURE_DISABLED_PRIVACY_UNAVAILABLE,),
            }
        );
        let store = state.store().expect("metadata store remains available");
        assert_eq!(marker_audit_count(&store), 1);

        store
            .insert_summary(
                "metadata-request",
                None,
                None,
                None,
                None,
                &store.now(),
                None,
                None,
                None,
            )
            .expect("metadata summary insert");
        store
            .insert_audit_entry(
                "metadata-audit",
                Some("metadata-request"),
                &store.now(),
                "test",
                "metadata_still_available",
                None,
            )
            .expect("metadata audit insert");

        let outcome = state
            .write_artifact(ArtifactCaptureRequest {
                artifact_id: "artifact-after-disable",
                request_id: "metadata-request",
                kind: "request_body",
                occurred_at: &store.now(),
                content: b"redacted",
                media_kind: Some("text/plain"),
                version: 1,
                truncated: false,
                byte_limit: 4096,
                aggregate_limit: 8192,
            })
            .expect("disabled capture is fail-open");
        assert!(matches!(outcome, ArtifactCaptureOutcome::Disabled(_)));
        let repeated_outcome = state
            .write_artifact(ArtifactCaptureRequest {
                artifact_id: "artifact-after-disable-again",
                request_id: "metadata-request",
                kind: "request_body",
                occurred_at: &store.now(),
                content: b"redacted",
                media_kind: Some("text/plain"),
                version: 1,
                truncated: false,
                byte_limit: 4096,
                aggregate_limit: 8192,
            })
            .expect("repeated disabled capture is fail-open");
        assert!(matches!(
            repeated_outcome,
            ArtifactCaptureOutcome::Disabled(_)
        ));
        assert_eq!(marker_audit_count(&store), 1);
        assert!(store.get_summary("metadata-request").unwrap().is_some());
        let total_audits: i64 = store
            .conn()
            .query_row("SELECT COUNT(*) FROM audit_entries", [], |row| row.get(0))
            .expect("count all audits");
        assert_eq!(total_audits, 2);
    }

    #[test]
    fn write_time_privacy_failure_publishes_one_marker_and_keeps_metadata_available() {
        let root = tempfile::tempdir().expect("temporary logging root");
        let foundation = LoggingFoundation::init(true, Some(&root.path().to_path_buf()));
        let state = LoggingRuntimeState::initialize_with_capture_opener_for_test(
            &foundation,
            &mesh_llm_config::LoggingConfig::default(),
            |artifact_root, clock, store| {
                FailOpenArtifactCapture::open_with_privacy(
                    artifact_root,
                    clock,
                    store,
                    canonical_artifact_redactor(),
                    Arc::new(RejectArtifactFiles),
                )
            },
        );
        let store = state.store().expect("metadata store available");
        store
            .insert_summary(
                "write-time-request",
                None,
                None,
                None,
                None,
                &store.now(),
                None,
                None,
                None,
            )
            .expect("metadata summary insert");

        let occurred_at = store.now();
        let write = |artifact_id| ArtifactCaptureRequest {
            artifact_id,
            request_id: "write-time-request",
            kind: "request_body",
            occurred_at: &occurred_at,
            content: b"redacted",
            media_kind: Some("text/plain"),
            version: 1,
            truncated: false,
            byte_limit: 4096,
            aggregate_limit: 8192,
        };
        assert!(matches!(
            state.write_artifact(write("write-time-artifact")).unwrap(),
            ArtifactCaptureOutcome::Disabled(_)
        ));
        assert!(matches!(
            state
                .write_artifact(write("write-time-artifact-again"))
                .unwrap(),
            ArtifactCaptureOutcome::Disabled(_)
        ));

        assert_eq!(
            state.health().artifact_capture_degradation,
            Some(ARTIFACT_CAPTURE_DISABLED_PRIVACY_UNAVAILABLE)
        );
        assert!(state.health().metadata_available);
        assert_eq!(marker_audit_count(&store), 1);
    }

    #[test]
    fn raw_artifact_bytes_are_redacted_before_the_capture_boundary() {
        let root = tempfile::tempdir().expect("temporary logging root");
        let foundation = LoggingFoundation::init(true, Some(&root.path().to_path_buf()));
        let state = LoggingRuntimeState::initialize(&foundation, &Default::default());
        let store = state.store().expect("metadata store available");
        store
            .insert_summary(
                "raw-artifact-request",
                None,
                None,
                None,
                None,
                &store.now(),
                None,
                None,
                None,
            )
            .expect("metadata summary insert");

        let secret = b"Bearer super-secret-token-0123456789";
        let outcome = state
            .write_artifact(ArtifactCaptureRequest {
                artifact_id: "raw-artifact",
                request_id: "raw-artifact-request",
                kind: "request_body",
                occurred_at: &store.now(),
                content: secret,
                media_kind: Some("text/plain"),
                version: 1,
                truncated: false,
                byte_limit: 4096,
                aggregate_limit: 8192,
            })
            .expect("artifact write");
        assert!(matches!(
            outcome,
            ArtifactCaptureOutcome::Written(receipt) if receipt.redacted
        ));

        let stored = std::fs::read(
            foundation
                .artifact_dir()
                .join("raw-artifact-request")
                .join("raw-artifact"),
        )
        .expect("read stored artifact");
        assert!(!stored.windows(secret.len()).any(|window| window == secret));
        assert_eq!(stored, b"[REDACTED]");
        assert!(
            store
                .get_artifact_pointer("raw-artifact")
                .expect("artifact pointer")
                .expect("pointer present")
                .redacted
        );
    }

    #[test]
    fn unavailable_foundation_is_sanitized_and_fail_open() {
        let root = tempfile::tempdir().expect("temporary logging root");
        let foundation = LoggingFoundation::init(false, Some(&root.path().to_path_buf()));
        let state = LoggingRuntimeState::initialize(&foundation, &Default::default());

        assert_eq!(state.health(), LoggingRuntimeHealth::unavailable());
        assert!(state.store().is_none());
        assert_eq!(
            state.status(),
            LoggingRuntimeStatus {
                metadata_available: false,
                artifact_capture_available: false,
                artifact_capture_degradation: None,
                persistence_worker_state: "unavailable",
                persistence_queue_drops: 0,
                persistence_failures: 0,
                persistence_shutdown_losses: 0,
                persistence_outstanding: 0,
                cleanup_worker_state: "not_started",
                cleanup_shutdown_timeouts: 0,
                cleanup_last_outcome: None,
                cleanup_last_deleted_count: None,
            }
        );
        assert_eq!(
            state.apply_dynamic_limits(LoggingDynamicLimits {
                retention_ttl_secs: 7_200,
                replay_capacity: 256,
            }),
            Err(LoggingRuntimeApplyError::Unavailable)
        );
    }

    #[tokio::test]
    async fn awaited_runtime_startup_cleanup_uses_injected_store_time_before_ready() {
        let root = tempfile::tempdir().expect("temporary logging root");
        let foundation = LoggingFoundation::init(true, Some(&root.path().to_path_buf()));
        let config = mesh_llm_config::LoggingConfig {
            retention_ttl_secs: 3_600,
            cleanup_cadence_secs: 86_400,
            ..Default::default()
        };
        let state = LoggingRuntimeState::initialize_with_store_clock_for_test(
            &foundation,
            &config,
            Arc::new(FixedStoreClock("2026-08-03T12:00:00Z")),
        );
        let store = state.store().expect("metadata store available");
        store
            .insert_summary(
                "expired-before-startup",
                None,
                None,
                None,
                None,
                "2026-08-03T10:00:00Z",
                None,
                None,
                None,
            )
            .expect("insert stale summary");
        store
            .insert_summary(
                "retained-after-startup",
                None,
                None,
                None,
                None,
                "2026-08-03T11:30:01Z",
                None,
                None,
                None,
            )
            .expect("insert retained summary");
        for (request_id, terminal_at) in [
            ("expired-before-startup", "2026-08-03T10:00:00Z"),
            ("retained-after-startup", "2026-08-03T11:30:01Z"),
        ] {
            store
                .write_terminal_event(
                    request_id,
                    &format!("terminal-{request_id}"),
                    r#"{"type":"completed"}"#,
                    "completed",
                    terminal_at,
                )
                .expect("write deterministic terminal record");
        }

        let service = state
            .start_persistence_worker()
            .await
            .expect("service becomes ready after cleanup outcome");

        assert!(
            store
                .get_summary("expired-before-startup")
                .expect("load stale summary")
                .is_none(),
            "startup cleanup completed before the ready service was returned"
        );
        assert!(
            store
                .get_summary("retained-after-startup")
                .expect("load retained summary")
                .is_some()
        );
        assert_eq!(state.status().cleanup_last_outcome, Some("completed"));
        assert!(matches!(
            state.status().cleanup_last_deleted_count,
            Some(count) if count >= 1
        ));

        state.shutdown_cleanup_worker().await;
        assert!(service.shutdown().await);
    }

    #[tokio::test]
    async fn concurrent_starts_publish_one_cleanup_scheduler_and_truthful_status() {
        let root = tempfile::tempdir().expect("temporary logging root");
        let foundation = LoggingFoundation::init(true, Some(&root.path().to_path_buf()));
        let config = mesh_llm_config::LoggingConfig {
            cleanup_cadence_secs: 86_400,
            ..Default::default()
        };
        let state = Arc::new(LoggingRuntimeState::initialize(&foundation, &config));
        let barrier = Arc::new(tokio::sync::Barrier::new(3));

        let first_state = Arc::clone(&state);
        let first_barrier = Arc::clone(&barrier);
        let first = tokio::spawn(async move {
            first_barrier.wait().await;
            first_state.start_persistence_worker().await
        });
        let second_state = Arc::clone(&state);
        let second_barrier = Arc::clone(&barrier);
        let second = tokio::spawn(async move {
            second_barrier.wait().await;
            second_state.start_persistence_worker().await
        });

        // Release both calls from the same scheduling boundary. The
        // state-local candidate count is incremented at construction under
        // the activation gate, so it proves no losing task was ever spawned.
        barrier.wait().await;
        let first_service = first
            .await
            .expect("first start task joins")
            .expect("first start returns the ready service");
        let second_service = second
            .await
            .expect("second start task joins")
            .expect("second start returns the ready service");

        assert!(Arc::ptr_eq(&first_service, &second_service));
        assert_eq!(state.cleanup_candidate_count_for_test(), 1);
        assert!(state.has_cleanup_worker_for_test());
        assert_eq!(state.status().cleanup_worker_state, "running");
        assert_eq!(state.status().persistence_worker_state, "running");

        // Shutdown drains the one operational audit produced by the one
        // startup cleanup. A losing scheduler would produce a second audit or
        // leave a second task able to race a later cleanup pass.
        state.shutdown_cleanup_worker().await;
        assert!(first_service.shutdown().await);
        assert!(!state.has_cleanup_worker_for_test());
        assert_eq!(state.status().cleanup_worker_state, "stopped");
        assert_eq!(state.status().persistence_worker_state, "stopped");
        let cleanup_audits: i64 = state
            .store()
            .expect("metadata store available")
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM audit_entries WHERE action = 'logging_cleanup_completed'",
                [],
                |row| row.get(0),
            )
            .expect("count startup cleanup audits");
        assert_eq!(cleanup_audits, 1);
    }

    #[tokio::test]
    async fn retirement_after_cleanup_candidate_publication_leaves_no_worker_on_displaced_state() {
        let root = tempfile::tempdir().expect("temporary logging root");
        let foundation = LoggingFoundation::init(true, Some(&root.path().to_path_buf()));
        let config = mesh_llm_config::LoggingConfig {
            cleanup_cadence_secs: 86_400,
            ..Default::default()
        };
        let state = Arc::new(LoggingRuntimeState::initialize(&foundation, &config));
        let hook = state.install_cleanup_publish_hook_for_test();
        let displaced_service = Arc::clone(
            state
                .service
                .as_ref()
                .expect("healthy state owns a persistence service"),
        );

        let starting_state = Arc::clone(&state);
        let start = tokio::spawn(async move { starting_state.start_persistence_worker().await });

        // The candidate is already atomically published, but the starter has
        // not yet observed readiness. Retire the displaced state in this
        // exact window and prove retirement cancels and joins that task.
        hook.candidate_created.wait().await;
        state.retire_and_shutdown().await;
        hook.resume_install.wait().await;

        assert!(start.await.expect("start task joins").is_none());
        assert!(state.is_retired());
        assert!(
            !state.has_cleanup_worker_for_test(),
            "a retired state must not retain a cleanup task handle"
        );
        assert_eq!(state.status().cleanup_worker_state, "stopped");
        assert!(!displaced_service.is_startable());
        assert!(!displaced_service.is_spawned());
        assert_eq!(
            state.status().persistence_worker_state,
            "stopped",
            "replacement must have joined the displaced persistence worker"
        );
    }

    #[test]
    fn store_open_failure_is_fail_open_without_exposing_the_failed_path() {
        let root = tempfile::tempdir().expect("temporary logging root");
        let foundation = LoggingFoundation::init(true, Some(&root.path().to_path_buf()));
        std::fs::remove_dir_all(foundation.store_dir()).expect("remove store directory");
        std::fs::write(foundation.store_dir(), b"not a directory").expect("block store root");

        let state = LoggingRuntimeState::initialize(&foundation, &Default::default());

        assert_eq!(state.health(), LoggingRuntimeHealth::unavailable());
        assert!(state.store().is_none());
    }

    #[test]
    fn applies_retention_and_replay_limits_together_to_the_installed_service() {
        let root = tempfile::tempdir().expect("temporary logging root");
        let foundation = LoggingFoundation::init(true, Some(&root.path().to_path_buf()));
        let initial = mesh_llm_config::LoggingConfig {
            retention_ttl_secs: 3_600,
            replay_capacity: 4,
            ..Default::default()
        };
        let state = LoggingRuntimeState::initialize(&foundation, &initial);
        assert_eq!(
            state.dynamic_limits(),
            Some(LoggingDynamicLimits {
                retention_ttl_secs: 3_600,
                replay_capacity: 4,
            })
        );

        let next = LoggingDynamicLimits {
            retention_ttl_secs: 7_200,
            replay_capacity: 2,
        };
        state.apply_dynamic_limits(next).expect("healthy runtime");
        assert_eq!(state.dynamic_limits(), Some(next));
    }

    #[test]
    fn openai_lifecycle_observer_snapshot_is_absent_when_disabled_or_retired() {
        assert!(
            LoggingRuntimeState::unavailable()
                .openai_lifecycle_observer()
                .is_none()
        );

        let root = tempfile::tempdir().expect("temporary logging root");
        let foundation = LoggingFoundation::init(true, Some(&root.path().to_path_buf()));
        let state = LoggingRuntimeState::initialize(&foundation, &Default::default());
        assert!(state.openai_lifecycle_observer().is_some());

        state.retired.store(true, Ordering::Release);
        assert!(state.openai_lifecycle_observer().is_none());
    }
}
