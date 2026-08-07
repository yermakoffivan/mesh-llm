//! Bounded runtime-owned retention cleanup for durable logging.
//!
//! The worker applies both configured time-based and terminal-summary row-cap
//! retention. It never reinterprets presentation or queue limits as rows.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use mesh_llm_events::logging::identifiers::EventId;
use mesh_llm_log_store::{
    CascadeArtifactPointer, FailOpenArtifactCapture, LogStore, RetentionPolicy,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::{
    LoggingCleanupOutcome, LoggingService, OperationalAuditRecord, OperationalAuditSeverity,
};

const CLEANUP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const CLEANUP_COMPLETED_AUDIT: &str = "logging_cleanup_completed";
const CLEANUP_FAILED_AUDIT: &str = "logging_cleanup_failed";

/// Cancellation state for one scheduler.  Cleanup owns a dedicated SQLite
/// connection, so interrupting it cannot cancel request persistence work.
struct CleanupCancellation {
    cancelled: AtomicBool,
    store: Option<Arc<LogStore>>,
}

impl CleanupCancellation {
    fn new(store: Arc<LogStore>) -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            store: Some(store),
        }
    }

    fn unavailable() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            store: None,
        }
    }

    fn store(&self) -> Option<&Arc<LogStore>> {
        self.store.as_ref()
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(store) = self.store() {
            store.interrupt();
        }
    }
}

/// Result of one cleanup attempt, suitable for path-free runtime health.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupOutcome {
    Completed { deleted_count: u64 },
    SkippedUnavailable,
    Failed,
}

impl CleanupOutcome {
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Completed { .. } => "completed",
            Self::SkippedUnavailable => "skipped_unavailable",
            Self::Failed => "failed",
        }
    }

    pub(crate) const fn deleted_count(self) -> Option<u64> {
        match self {
            Self::Completed { deleted_count } => Some(deleted_count),
            Self::SkippedUnavailable | Self::Failed => None,
        }
    }
}

/// Local scheduler state kept separately from the task handle so shutdown
/// does not erase the last known result from the status projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CleanupWorkerStatus {
    pub(crate) state: CleanupWorkerState,
    pub(crate) last_outcome: Option<CleanupOutcome>,
    /// Counts worker shutdowns that reached the fixed join bound. This is an
    /// accounting signal, not a claim that a database mutation was lost.
    pub(crate) shutdown_timeouts: u64,
}

impl Default for CleanupWorkerStatus {
    fn default() -> Self {
        Self {
            state: CleanupWorkerState::NotStarted,
            last_outcome: None,
            shutdown_timeouts: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupWorkerState {
    NotStarted,
    Running,
    Stopping,
    TimedOut,
    Stopped,
}

/// One cancellable scheduler task per installed logging runtime state.
pub(crate) struct CleanupWorker {
    shutdown_tx: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
    startup_outcome: watch::Sender<Option<CleanupOutcome>>,
    status: Arc<Mutex<CleanupWorkerStatus>>,
    cancellation: Arc<CleanupCancellation>,
    shutdown_timeout: Duration,
    #[cfg(test)]
    stall_before_cleanup: Option<Arc<CleanupStallHook>>,
}

/// A test-only, pre-mutation cleanup seam. The worker cannot reach SQLite,
/// artifact deletion, or audit delivery until this hook is released.
#[cfg(test)]
struct CleanupStallHook {
    entered: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
    entered_notify: tokio::sync::Notify,
    release_notify: tokio::sync::Notify,
}

#[cfg(test)]
impl CleanupStallHook {
    fn new() -> Self {
        Self {
            entered: std::sync::atomic::AtomicBool::new(false),
            released: std::sync::atomic::AtomicBool::new(false),
            entered_notify: tokio::sync::Notify::new(),
            release_notify: tokio::sync::Notify::new(),
        }
    }

    async fn wait_until_entered(&self) {
        loop {
            let notified = self.entered_notify.notified();
            if self.entered.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    async fn wait_for_release(&self) {
        self.entered.store(true, Ordering::Release);
        self.entered_notify.notify_waiters();
        loop {
            let notified = self.release_notify.notified();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.release_notify.notify_waiters();
    }
}

impl CleanupWorker {
    pub(crate) fn start(
        store: Arc<LogStore>,
        artifact_capture: Option<Arc<FailOpenArtifactCapture>>,
        service: Arc<LoggingService>,
        retention_max_rows: u64,
        webhook_dead_letter_retention_secs: u64,
        cadence: Duration,
        status: Arc<Mutex<CleanupWorkerStatus>>,
    ) -> Self {
        Self::start_with_test_stall(
            store,
            artifact_capture,
            service,
            retention_max_rows,
            webhook_dead_letter_retention_secs,
            cadence,
            status,
            #[cfg(test)]
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_with_test_stall(
        store: Arc<LogStore>,
        artifact_capture: Option<Arc<FailOpenArtifactCapture>>,
        service: Arc<LoggingService>,
        retention_max_rows: u64,
        webhook_dead_letter_retention_secs: u64,
        cadence: Duration,
        status: Arc<Mutex<CleanupWorkerStatus>>,
        #[cfg(test)] stall_before_cleanup: Option<Arc<CleanupStallHook>>,
    ) -> Self {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        // A watch value lets every startup caller observe the same completed
        // catch-up instead of making readiness depend on which caller happens
        // to consume a one-shot receiver first.
        let (startup_outcome, _) = watch::channel(None);
        let task_startup_outcome = startup_outcome.clone();
        let task_status = Arc::clone(&status);
        // SQLite interrupts are connection-scoped. A worker-owned connection
        // makes cancellation precise even while request persistence is active.
        let cancellation = Arc::new(match store.reopen_for_background_worker() {
            Ok(cleanup_store) => CleanupCancellation::new(Arc::new(cleanup_store)),
            // Reusing the request-persistence connection would make a cleanup
            // interrupt unsafe. Keep serving and report a fail-open skipped
            // maintenance pass until a later runtime start can reopen it.
            Err(_) => CleanupCancellation::unavailable(),
        });
        let task_cancellation = Arc::clone(&cancellation);
        #[cfg(test)]
        let task_stall_before_cleanup = stall_before_cleanup.clone();
        let task = tokio::spawn(async move {
            update_cleanup_status(&task_status, CleanupWorkerState::Running, None);
            #[cfg(test)]
            if let Some(stall) = task_stall_before_cleanup {
                stall.wait_for_release().await;
            }
            if task_cancellation.is_cancelled() {
                update_cleanup_status(&task_status, CleanupWorkerState::Stopped, None);
                let outcome = CleanupOutcome::SkippedUnavailable;
                service.record_cleanup_outcome(logging_cleanup_outcome(outcome));
                let _ = task_startup_outcome.send(Some(outcome));
                return;
            }
            // A restart must not wait one full cadence before enforcing
            // retention.  This is deliberately serialized with later ticks
            // so a startup catch-up and timer tick never race SQLite/file
            // cleanup against each other.
            let startup = run_cleanup_once_with_cancellation(
                Arc::clone(&task_cancellation),
                artifact_capture.clone(),
                Arc::clone(&service),
                retention_max_rows,
                webhook_dead_letter_retention_secs,
            )
            .await;
            update_cleanup_status(&task_status, CleanupWorkerState::Running, Some(startup));
            let _ = task_startup_outcome.send(Some(startup));
            if task_cancellation.is_cancelled() {
                update_cleanup_status(&task_status, CleanupWorkerState::Stopped, None);
                return;
            }

            let mut ticker =
                tokio::time::interval_at(tokio::time::Instant::now() + cadence, cadence);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let outcome = run_cleanup_once_with_cancellation(
                            Arc::clone(&task_cancellation),
                            artifact_capture.clone(),
                            Arc::clone(&service),
                            retention_max_rows,
                            webhook_dead_letter_retention_secs,
                        ).await;
                        update_cleanup_status(&task_status, CleanupWorkerState::Running, Some(outcome));
                        if task_cancellation.is_cancelled() {
                            update_cleanup_status(&task_status, CleanupWorkerState::Stopped, None);
                            return;
                        }
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            update_cleanup_status(&task_status, CleanupWorkerState::Stopped, None);
                            return;
                        }
                    }
                }
            }
        });
        Self {
            shutdown_tx,
            task: Mutex::new(Some(task)),
            startup_outcome,
            status,
            cancellation,
            shutdown_timeout: CLEANUP_SHUTDOWN_TIMEOUT,
            #[cfg(test)]
            stall_before_cleanup,
        }
    }

    /// Wait until the startup retention pass has reached a terminal outcome.
    ///
    /// This is intentionally separate from `start`: the runtime releases its
    /// short activation lock before awaiting, so a store catch-up cannot hold
    /// a synchronous lock across an asynchronous database operation.
    pub(crate) async fn wait_for_startup(&self) -> CleanupOutcome {
        Self::wait_for_startup_with(self.startup_waiter()).await
    }

    pub(crate) fn startup_waiter(&self) -> watch::Receiver<Option<CleanupOutcome>> {
        self.startup_outcome.subscribe()
    }

    pub(crate) async fn wait_for_startup_with(
        mut receiver: watch::Receiver<Option<CleanupOutcome>>,
    ) -> CleanupOutcome {
        loop {
            if let Some(outcome) = *receiver.borrow() {
                return outcome;
            }
            // The sender lives for the worker. If it is gone unexpectedly,
            // the task could not complete startup; classify that fail-open.
            if receiver.changed().await.is_err() {
                return CleanupOutcome::Failed;
            }
        }
    }

    /// Signal and interrupt the worker-owned SQLite connection, then await a
    /// fixed join bound. A timeout retains the task handle and exposes
    /// `timed_out`; it never aborts only the async wrapper or claims stopped
    /// while the blocking phase might still own SQLite/files.
    pub(crate) async fn shutdown(&self) -> bool {
        let _ = self.shutdown_tx.send(true);
        self.cancellation.cancel();
        let Some(mut task) = self
            .task
            .lock()
            .expect("cleanup worker mutex poisoned")
            .take()
        else {
            return false;
        };
        let already_timed_out = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .state
            == CleanupWorkerState::TimedOut;
        if !already_timed_out {
            update_cleanup_status(&self.status, CleanupWorkerState::Stopping, None);
        }
        match tokio::time::timeout(self.shutdown_timeout, &mut task).await {
            Ok(Ok(())) => {
                update_cleanup_status(&self.status, CleanupWorkerState::Stopped, None);
                true
            }
            Ok(Err(_)) => {
                update_cleanup_status(&self.status, CleanupWorkerState::Stopped, None);
                false
            }
            Err(_) => {
                {
                    let mut status = self
                        .status
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if !already_timed_out {
                        status.shutdown_timeouts = status.shutdown_timeouts.saturating_add(1);
                    }
                    status.state = CleanupWorkerState::TimedOut;
                }
                *self.task.lock().expect("cleanup worker mutex poisoned") = Some(task);
                false
            }
        }
    }

    #[cfg(test)]
    fn with_shutdown_timeout_for_test(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }
}

impl Drop for CleanupWorker {
    fn drop(&mut self) {
        // A scheduler may be removed from its owner during retirement or if
        // its enclosing startup future is cancelled. Dropping that worker
        // must never claim it is stopped while a blocking operation lives.
        let _ = self.shutdown_tx.send(true);
        self.cancellation.cancel();
        if self
            .task
            .lock()
            .expect("cleanup worker mutex poisoned")
            .take()
            .is_some()
        {
            // The task retains cancellation and only publishes Stopped after
            // its blocking SQLite/file phases exit. Do not abort only the
            // async wrapper here.
        }
    }
}

fn update_cleanup_status(
    status: &Mutex<CleanupWorkerStatus>,
    state: CleanupWorkerState,
    outcome: Option<CleanupOutcome>,
) {
    let mut status = status
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    status.state = state;
    if let Some(outcome) = outcome {
        status.last_outcome = Some(outcome);
    }
}

/// Execute one retention attempt on Tokio's blocking pool. The cadence task
/// awaits this single attempt before considering another tick, so missed ticks
/// are skipped instead of creating concurrent SQLite/file cleanup workers.
pub(crate) async fn run_cleanup_once(
    store: Arc<LogStore>,
    artifact_capture: Option<Arc<FailOpenArtifactCapture>>,
    service: Arc<LoggingService>,
    retention_max_rows: u64,
    webhook_dead_letter_retention_secs: u64,
) -> CleanupOutcome {
    run_cleanup_once_with_cancellation(
        Arc::new(CleanupCancellation::new(store)),
        artifact_capture,
        service,
        retention_max_rows,
        webhook_dead_letter_retention_secs,
    )
    .await
}

async fn run_cleanup_once_with_cancellation(
    cancellation: Arc<CleanupCancellation>,
    artifact_capture: Option<Arc<FailOpenArtifactCapture>>,
    service: Arc<LoggingService>,
    retention_max_rows: u64,
    webhook_dead_letter_retention_secs: u64,
) -> CleanupOutcome {
    if cancellation.is_cancelled() {
        return report_cleanup_outcome(&service, CleanupOutcome::SkippedUnavailable);
    }
    if cancellation.store().is_none() {
        return report_cleanup_outcome(&service, CleanupOutcome::SkippedUnavailable);
    }
    let retention_ttl_secs = service.dynamic_limits().retention_ttl_secs;
    let task_cancellation = Arc::clone(&cancellation);
    let result = tokio::task::spawn_blocking(move || {
        execute_retention_cleanup(
            task_cancellation,
            artifact_capture,
            retention_ttl_secs,
            retention_max_rows,
            webhook_dead_letter_retention_secs,
        )
    })
    .await;

    if cancellation.is_cancelled() {
        return report_cleanup_outcome(&service, CleanupOutcome::SkippedUnavailable);
    }

    let outcome = match result {
        Ok(Ok(deleted_count)) => {
            service.write_operational_audit(
                OperationalAuditRecord::builder("logging_service", CLEANUP_COMPLETED_AUDIT)
                    .severity(OperationalAuditSeverity::Info)
                    .build(),
            );
            CleanupOutcome::Completed { deleted_count }
        }
        Ok(Err(_)) | Err(_) => {
            service.write_operational_audit(
                OperationalAuditRecord::builder("logging_service", CLEANUP_FAILED_AUDIT)
                    .severity(OperationalAuditSeverity::Error)
                    .build(),
            );
            CleanupOutcome::Failed
        }
    };
    report_cleanup_outcome(&service, outcome)
}

fn report_cleanup_outcome(service: &LoggingService, outcome: CleanupOutcome) -> CleanupOutcome {
    service.record_cleanup_outcome(logging_cleanup_outcome(outcome));
    outcome
}

fn logging_cleanup_outcome(outcome: CleanupOutcome) -> LoggingCleanupOutcome {
    match outcome {
        CleanupOutcome::Completed { .. } => LoggingCleanupOutcome::Completed,
        CleanupOutcome::SkippedUnavailable => LoggingCleanupOutcome::SkippedUnavailable,
        CleanupOutcome::Failed => LoggingCleanupOutcome::Failed,
    }
}

fn execute_retention_cleanup(
    cancellation: Arc<CleanupCancellation>,
    artifact_capture: Option<Arc<FailOpenArtifactCapture>>,
    retention_ttl_secs: u64,
    retention_max_rows: u64,
    webhook_dead_letter_retention_secs: u64,
) -> Result<u64, String> {
    check_cancelled(&cancellation)?;
    let store = cancellation
        .store()
        .ok_or_else(|| "logging cleanup store unavailable".to_string())?;
    let occurred_at = store.now();
    let cutoff_before = ttl_cutoff(&occurred_at, retention_ttl_secs)?;
    let webhook_dead_letter_cutoff = ttl_cutoff(&occurred_at, webhook_dead_letter_retention_secs)?;
    // Generic retention remains complete for every table. The configured
    // dead-letter window is an additional state-aware cutoff measured from
    // the durable dead-letter transition.
    let policy = RetentionPolicy::uniform(&cutoff_before, retention_max_rows)
        .map_err(|error| error.to_string())?
        .with_webhook_dead_letter_cutoff(webhook_dead_letter_cutoff);
    let result = store
        .apply_retention_policy_map(&policy)
        .map_err(|error| error.to_string())?;

    check_cancelled(&cancellation)?;
    delete_selected_artifact_files(artifact_capture.as_deref(), &result.artifact_pointers);
    for table in &result.table_results {
        check_cancelled(&cancellation)?;
        store
            .insert_cleanup_run(
                &EventId::new().as_uuid().to_string(),
                &occurred_at,
                &format!("ttl:{}", table.table.label()),
                &cutoff_before,
                table.ttl_deleted_count,
                None,
            )
            .map_err(|error| error.to_string())?;
        check_cancelled(&cancellation)?;
        store
            .insert_cleanup_run(
                &EventId::new().as_uuid().to_string(),
                &occurred_at,
                &format!("max_rows:{}", table.table.label()),
                &format!("max_rows:{retention_max_rows}"),
                table.max_rows_deleted_count,
                None,
            )
            .map_err(|error| error.to_string())?;
    }
    // Receipts themselves are subject to the same bounded policy.  A second
    // no-op-style pass after commit records them only while preserving the
    // configured cleanup_runs cap (including a deliberately tiny cap).
    let receipt_trim = store
        .apply_retention_policy_map(&policy)
        .map_err(|error| error.to_string())?;
    check_cancelled(&cancellation)?;
    delete_selected_artifact_files(artifact_capture.as_deref(), &receipt_trim.artifact_pointers);
    // Receipt trimming is bookkeeping, not an operator-visible deletion of
    // retained logging data.
    Ok((result.ttl_deleted_count + result.max_rows_deleted_count).max(0) as u64)
}

fn check_cancelled(cancellation: &CleanupCancellation) -> Result<(), String> {
    if cancellation.is_cancelled() {
        Err("logging cleanup cancelled".to_string())
    } else {
        Ok(())
    }
}

fn delete_selected_artifact_files(
    artifact_capture: Option<&FailOpenArtifactCapture>,
    pointers: &[CascadeArtifactPointer],
) {
    if let Some(artifact_capture) = artifact_capture {
        artifact_capture.delete_cascade_artifact_files(pointers);
    }
}

fn ttl_cutoff(occurred_at: &str, retention_ttl_secs: u64) -> Result<String, String> {
    let timestamp = DateTime::parse_from_rfc3339(occurred_at)
        .map_err(|_| "logging cleanup clock returned an invalid timestamp".to_string())?
        .with_timezone(&Utc);
    let retention = chrono::Duration::try_seconds(retention_ttl_secs as i64)
        .ok_or_else(|| "logging cleanup retention is out of range".to_string())?;
    Ok((timestamp - retention).to_rfc3339_opts(SecondsFormat::Secs, true))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use mesh_llm_log_store::{Clock as StoreClock, FailOpenArtifactCapture, LogStore};

    use super::*;
    use crate::logging::{LogStoreSink, LoggingDynamicLimits, ServiceConfig, SystemClock};

    struct FixedClock(&'static str);

    impl StoreClock for FixedClock {
        fn now(&self) -> String {
            self.0.to_string()
        }
    }

    fn setup(
        now: &'static str,
        retention_ttl_secs: u64,
        _retention_max_rows: u64,
    ) -> (
        tempfile::TempDir,
        Arc<LogStore>,
        Arc<FailOpenArtifactCapture>,
        Arc<LoggingService>,
    ) {
        let root = tempfile::tempdir().expect("temporary root");
        let clock: Arc<dyn StoreClock> = Arc::new(FixedClock(now));
        let store = Arc::new(
            LogStore::open(root.path().join("store"), Arc::clone(&clock)).expect("open store"),
        );
        let capture = Arc::new(
            FailOpenArtifactCapture::open(
                root.path().join("artifacts"),
                clock,
                Arc::clone(&store),
                Arc::new(|bytes| bytes.to_vec()),
            )
            .expect("open capture"),
        );
        let service = Arc::new(LoggingService::new_with_dynamic_limits(
            ServiceConfig::default(),
            Arc::new(LogStoreSink::new(Arc::clone(&store))),
            Box::new(SystemClock),
            LoggingDynamicLimits {
                retention_ttl_secs,
                replay_capacity: 8,
            },
        ));
        (root, store, capture, service)
    }

    fn insert_summary(store: &LogStore, request_id: &str, created_at: &str) {
        store
            .insert_summary(
                request_id, None, None, None, None, created_at, None, None, None,
            )
            .expect("insert summary");
    }

    fn insert_terminal_summary(store: &LogStore, request_id: &str, occurred_at: &str) {
        insert_summary(store, request_id, occurred_at);
        store
            .write_terminal_event(
                request_id,
                &format!("terminal-{request_id}"),
                r#"{"type":"completed"}"#,
                "completed",
                occurred_at,
            )
            .expect("write terminal event");
    }

    #[tokio::test]
    async fn ttl_cleanup_uses_live_retention_and_records_a_sanitized_audit() {
        let (_root, store, capture, service) = setup("2026-08-03T12:00:00Z", 172_800, 100);
        insert_terminal_summary(&store, "old", "2026-08-03T10:00:00Z");

        assert_eq!(
            run_cleanup_once(
                Arc::clone(&store),
                Some(capture),
                Arc::clone(&service),
                100,
                3_600
            )
            .await,
            CleanupOutcome::Completed { deleted_count: 0 }
        );
        assert!(store.get_summary("old").expect("get summary").is_some());

        service.apply_dynamic_limits(LoggingDynamicLimits {
            retention_ttl_secs: 3_600,
            replay_capacity: 8,
        });
        assert_eq!(
            run_cleanup_once(Arc::clone(&store), None, Arc::clone(&service), 100, 3_600).await,
            CleanupOutcome::Completed { deleted_count: 2 }
        );
        assert!(store.get_summary("old").expect("get summary").is_none());
        let cleanup_runs: i64 = store
            .conn()
            .query_row("SELECT COUNT(*) FROM cleanup_runs", [], |row| row.get(0))
            .expect("count runs");
        assert_eq!(
            cleanup_runs, 28,
            "two passes each record TTL and cap results for every durable table"
        );

        assert!(service.pump_sync().await >= 2);
        let audit_count: i64 = store
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM audit_entries WHERE action = 'logging_cleanup_completed'",
                [],
                |row| row.get(0),
            )
            .expect("read cleanup audits");
        assert_eq!(audit_count, 2);
    }

    #[tokio::test]
    async fn cleanup_failure_is_fail_open_and_records_a_bounded_error_audit() {
        let (_root, store, capture, service) = setup("not-a-timestamp", 3_600, 100);
        assert_eq!(
            run_cleanup_once(store, Some(capture), Arc::clone(&service), 100, 3_600).await,
            CleanupOutcome::Failed
        );
        assert_eq!(service.pump_sync().await, 1);
    }

    #[tokio::test]
    async fn ttl_cleanup_cascades_only_the_artifact_files_selected_by_the_store() {
        let (root, store, capture, service) = setup("2026-08-03T12:00:00Z", 3_600, 100);
        insert_terminal_summary(&store, "old-request", "2026-08-03T10:00:00Z");
        capture
            .write_artifact(
                "old-artifact",
                "old-request",
                "request_body",
                "2026-08-03T10:00:00Z",
                b"already-redacted",
                Some("text/plain"),
                1,
                true,
                false,
                4_096,
                8_192,
            )
            .expect("write artifact");

        assert_eq!(
            run_cleanup_once(store, Some(capture), service, 100, 3_600).await,
            CleanupOutcome::Completed { deleted_count: 3 }
        );
        assert!(
            !root
                .path()
                .join("artifacts/old-request/old-artifact")
                .exists()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_worker_shutdown_joins_without_waiting_for_its_cadence() {
        let (_root, store, capture, service) = setup("2026-08-03T12:00:00Z", 3_600, 100);
        let worker = CleanupWorker::start(
            store,
            Some(capture),
            service,
            100,
            3_600,
            Duration::from_secs(3_600),
            Arc::new(Mutex::new(CleanupWorkerStatus::default())),
        );
        assert!(worker.shutdown().await);
        assert!(!worker.shutdown().await);
    }

    #[tokio::test]
    async fn stalled_cleanup_shutdown_is_bounded_truthful_and_cannot_mutate_after_retirement() {
        let (root, store, capture, service) = setup("2026-08-03T12:00:00Z", 3_600, 100);
        insert_terminal_summary(&store, "expired-after-retire", "2026-08-03T10:00:00Z");
        capture
            .write_artifact(
                "retire-artifact",
                "expired-after-retire",
                "request_body",
                "2026-08-03T10:00:00Z",
                b"already-redacted",
                Some("text/plain"),
                1,
                true,
                false,
                4_096,
                8_192,
            )
            .expect("write artifact");
        let status = Arc::new(Mutex::new(CleanupWorkerStatus::default()));
        let stall = Arc::new(CleanupStallHook::new());
        let worker = CleanupWorker::start_with_test_stall(
            Arc::clone(&store),
            Some(capture),
            Arc::clone(&service),
            100,
            3_600,
            Duration::from_secs(3_600),
            Arc::clone(&status),
            Some(Arc::clone(&stall)),
        )
        .with_shutdown_timeout_for_test(Duration::ZERO);

        stall.wait_until_entered().await;
        assert!(
            !worker.shutdown().await,
            "the fixed bound must return even when cleanup is interrupt-resistant"
        );
        assert_eq!(
            *status.lock().expect("cleanup status"),
            CleanupWorkerStatus {
                state: CleanupWorkerState::TimedOut,
                last_outcome: None,
                shutdown_timeouts: 1,
            }
        );
        assert!(
            !worker.shutdown().await,
            "a still-owned worker remains unavailable to replacement"
        );
        assert_eq!(
            status.lock().expect("cleanup status").shutdown_timeouts,
            1,
            "repeated polling must not inflate the bounded timeout accounting"
        );
        assert!(
            store
                .get_summary("expired-after-retire")
                .expect("summary query")
                .is_some(),
            "the stalled pre-mutation phase may not delete durable rows"
        );
        assert!(
            root.path()
                .join("artifacts/expired-after-retire/retire-artifact")
                .exists(),
            "the stalled phase may not delete artifacts"
        );
        assert_eq!(
            service.pump_sync().await,
            0,
            "a retired cleanup may not enqueue a late audit"
        );

        // The retired task can now observe cancellation, exit without crossing
        // its mutation gate, and only then become truthfully stopped.
        stall.release();
        assert!(
            worker.shutdown().await,
            "released task joins after cancellation"
        );
        assert_eq!(
            *status.lock().expect("cleanup status"),
            CleanupWorkerStatus {
                state: CleanupWorkerState::Stopped,
                last_outcome: None,
                shutdown_timeouts: 1,
            }
        );
        assert!(store.get_summary("expired-after-retire").unwrap().is_some());
        assert!(
            root.path()
                .join("artifacts/expired-after-retire/retire-artifact")
                .exists()
        );
        assert_eq!(service.pump_sync().await, 0);
    }

    #[tokio::test]
    async fn shutdown_interrupts_locked_retention_without_late_file_or_audit_mutation() {
        let (root, store, capture, service) = setup("2026-08-03T12:00:00Z", 3_600, 100);
        insert_terminal_summary(&store, "expired-during-stop", "2026-08-03T10:00:00Z");
        capture
            .write_artifact(
                "expired-artifact",
                "expired-during-stop",
                "request_body",
                "2026-08-03T10:00:00Z",
                b"already-redacted",
                Some("text/plain"),
                1,
                true,
                false,
                4_096,
                8_192,
            )
            .expect("write artifact");

        // Open the cleanup-owned connection before locking the primary. This
        // focuses the test on a blocked retention transaction rather than a
        // connection-initialization lock race.
        let cleanup_store = Arc::new(
            store
                .reopen_for_background_worker()
                .expect("open dedicated cleanup store"),
        );
        let cancellation = Arc::new(CleanupCancellation::new(cleanup_store));

        // Hold the primary connection's write lease on a dedicated thread.
        // The async test never holds that synchronous guard across an await.
        let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let lock_store = Arc::clone(&store);
        let lock_holder = std::thread::spawn(move || {
            let primary = lock_store.conn();
            primary
                .execute_batch("BEGIN IMMEDIATE")
                .expect("hold deterministic sqlite write lock");
            locked_tx.send(()).expect("announce sqlite lock");
            release_rx.recv().expect("release sqlite lock");
            primary
                .execute_batch("ROLLBACK")
                .expect("release sqlite lock");
        });
        locked_rx.recv().expect("wait for sqlite lock");
        let cleanup = tokio::spawn(run_cleanup_once_with_cancellation(
            Arc::clone(&cancellation),
            Some(capture),
            Arc::clone(&service),
            100,
            3_600,
        ));
        // Give the spawned scheduler its deterministic executor turn; no wall
        // clock delay is used. The held write transaction makes its retention
        // path a SQLite busy wait until shutdown interrupts that connection.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        cancellation.cancel();
        let outcome = tokio::time::timeout(Duration::from_secs(1), cleanup)
            .await
            .expect("sqlite interrupt bounds shutdown");
        assert_eq!(
            outcome.expect("cleanup task joins"),
            CleanupOutcome::SkippedUnavailable
        );
        release_tx.send(()).expect("release sqlite lock");
        lock_holder.join().expect("sqlite lock holder joins");

        assert!(
            store
                .get_summary("expired-during-stop")
                .expect("summary query")
                .is_some(),
            "a retired worker may not commit a late retention delete"
        );
        assert!(
            root.path()
                .join("artifacts")
                .join("expired-during-stop")
                .join("expired-artifact")
                .exists()
        );
        assert_eq!(
            service.pump_sync().await,
            0,
            "no cleanup audit after cancellation"
        );
    }

    #[tokio::test]
    async fn cleanup_worker_runs_startup_catch_up_before_its_first_cadence() {
        let (_root, store, capture, service) = setup("2026-08-03T12:00:00Z", 3_600, 100);
        insert_terminal_summary(&store, "expired-before-restart", "2026-08-03T10:00:00Z");
        let status = Arc::new(Mutex::new(CleanupWorkerStatus::default()));

        let worker = CleanupWorker::start(
            Arc::clone(&store),
            Some(capture),
            service,
            100,
            3_600,
            Duration::from_secs(24 * 60 * 60),
            Arc::clone(&status),
        );

        assert_eq!(
            worker.wait_for_startup().await,
            CleanupOutcome::Completed { deleted_count: 2 }
        );
        assert!(
            store
                .get_summary("expired-before-restart")
                .expect("get summary")
                .is_none()
        );
        assert!(worker.shutdown().await);
        assert_eq!(
            *status.lock().expect("cleanup status mutex"),
            CleanupWorkerStatus {
                state: CleanupWorkerState::Stopped,
                last_outcome: Some(CleanupOutcome::Completed { deleted_count: 2 }),
                shutdown_timeouts: 0,
            }
        );
    }

    #[tokio::test]
    async fn cleanup_keeps_active_summary_and_its_old_artifact_reference_intact() {
        let (root, store, capture, service) = setup("2026-08-03T12:00:00Z", 3_600, 100);
        insert_summary(&store, "active-request", "2026-08-03T10:00:00Z");
        capture
            .write_artifact(
                "active-artifact",
                "active-request",
                "request_body",
                "2026-08-03T10:00:00Z",
                b"already-redacted",
                Some("text/plain"),
                1,
                true,
                false,
                4_096,
                8_192,
            )
            .expect("write artifact");

        assert_eq!(
            run_cleanup_once(Arc::clone(&store), Some(capture), service, 100, 3_600).await,
            CleanupOutcome::Completed { deleted_count: 0 }
        );
        assert!(store.get_summary("active-request").unwrap().is_some());
        assert!(
            store
                .get_artifact_pointer("active-artifact")
                .unwrap()
                .is_some()
        );
        assert!(
            root.path()
                .join("artifacts/active-request/active-artifact")
                .exists()
        );
    }

    #[tokio::test]
    async fn ttl_cleanup_prunes_standalone_audit_webhook_and_cleanup_receipts() {
        let (_root, store, capture, service) = setup("2026-08-03T12:00:00Z", 3_600, 100);
        let old = "2026-08-03T10:00:00Z";
        let current = "2026-08-03T12:00:00Z";
        store
            .insert_audit_entry("old-audit", None, old, "operator", "old_action", None)
            .unwrap();
        store
            .insert_webhook_delivery("old-webhook", None, old, 1, None)
            .unwrap();
        store
            .insert_cleanup_run("old-run", old, "old", old, 0, None)
            .unwrap();
        store
            .insert_audit_entry(
                "fresh-audit",
                None,
                current,
                "operator",
                "fresh_action",
                None,
            )
            .unwrap();

        assert_eq!(
            run_cleanup_once(Arc::clone(&store), Some(capture), service, 100, 3_600).await,
            CleanupOutcome::Completed { deleted_count: 3 }
        );
        assert_eq!(
            store
                .conn()
                .query_row("SELECT COUNT(*) FROM webhook_deliveries", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM audit_entries WHERE action = 'fresh_action'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn ttl_cutoff_is_stable_and_path_free() {
        assert_eq!(
            ttl_cutoff("2026-08-03T12:00:00.500Z", 3_600).expect("cutoff"),
            "2026-08-03T11:00:00Z"
        );
        assert!(
            ttl_cutoff("/private/secret", 3_600)
                .expect_err("invalid clock")
                .contains("invalid timestamp")
        );
    }

    #[tokio::test]
    async fn retention_pass_applies_max_rows_after_ttl_and_records_each_policy() {
        let (_root, store, capture, service) = setup("2026-08-03T12:00:00Z", 172_800, 2);
        for (request_id, terminal_at) in [
            ("oldest", "2026-08-03T10:00:00Z"),
            ("middle", "2026-08-03T11:00:00Z"),
            ("newest", "2026-08-03T11:30:00Z"),
        ] {
            insert_summary(&store, request_id, terminal_at);
            store
                .write_terminal_event(
                    request_id,
                    &format!("event-{request_id}"),
                    r#"{"type":"completed"}"#,
                    "completed",
                    terminal_at,
                )
                .expect("write terminal");
        }
        insert_summary(&store, "active", "2026-08-03T09:00:00Z");

        assert_eq!(
            run_cleanup_once(
                Arc::clone(&store),
                Some(capture),
                Arc::clone(&service),
                2,
                3_600
            )
            .await,
            CleanupOutcome::Completed { deleted_count: 2 }
        );
        assert!(store.get_summary("oldest").unwrap().is_none());
        assert!(store.get_summary("middle").unwrap().is_some());
        assert!(store.get_summary("newest").unwrap().is_some());
        assert!(store.get_summary("active").unwrap().is_some());
        let policies: Vec<String> = store
            .conn()
            .prepare("SELECT policy_name FROM cleanup_runs ORDER BY rowid ASC")
            .expect("prepare cleanup policies")
            .query_map([], |row| row.get(0))
            .expect("read cleanup policies")
            .collect::<Result<_, _>>()
            .expect("collect cleanup policies");
        assert_eq!(policies.len(), 2, "receipt cap applies after recording");
        assert!(
            policies
                .iter()
                .all(|policy| { policy.starts_with("ttl:") || policy.starts_with("max_rows:") })
        );
        assert_eq!(service.pump_sync().await, 1);
    }

    #[tokio::test]
    async fn invalid_max_row_policy_fails_open_and_records_a_bounded_audit() {
        let (_root, store, capture, service) = setup("2026-08-03T12:00:00Z", 3_600, 1);
        assert_eq!(
            run_cleanup_once(store, Some(capture), Arc::clone(&service), 0, 3_600).await,
            CleanupOutcome::Failed
        );
        assert_eq!(service.pump_sync().await, 1);
    }
}
