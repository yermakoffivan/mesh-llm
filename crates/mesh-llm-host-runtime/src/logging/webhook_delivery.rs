//! Bounded asynchronous delivery of already-durable terminal webhook records.
//!
//! This module intentionally has no startup loop or request-path hook. A later
//! runtime owner schedules [`WebhookDeliveryWorker::process_next`]; that one
//! step claims a record, performs one bounded HTTP attempt, and persists the
//! fenced terminal transition. The payload is built solely from the delivery
//! record, never by re-reading lifecycle payloads or artifacts.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use mesh_llm_config::LoggingWebhookConfig;
use mesh_llm_log_store::{
    LogStore, LogStoreError, WebhookDeliveryErrorCode, WebhookDeliveryRecord, WebhookRetryOutcome,
};
use rand::RngExt;
use serde::Serialize;
use thiserror::Error;
use url::Url;

use super::metrics::{
    LoggingMetric, LoggingMetrics, LoggingWebhookAttemptState, LoggingWebhookDeliveryOutcome,
};

const BASE_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);
const MAX_JITTER_MILLIS: u64 = 1_000;
const LEASE_SAFETY_MARGIN: Duration = Duration::from_secs(5);

/// A deliberately small terminal notification. In particular, it never
/// carries prompt/completion content, artifact metadata, a filesystem path,
/// endpoint URL, transport error, or lifecycle event JSON.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct WebhookTerminalPayload {
    request_id: String,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_code: Option<u16>,
}

impl WebhookTerminalPayload {
    fn from_record(record: &WebhookDeliveryRecord) -> Option<Self> {
        record.request_id.as_ref().map(|request_id| Self {
            request_id: request_id.clone(),
            // The store intentionally does not retain raw lifecycle payloads
            // for webhook dispatch. `terminal` is the only outcome value that
            // can be reconstructed without reopening that privacy boundary.
            outcome: "terminal",
            status_code: record.status_code,
        })
    }
}

/// Result returned by an injected transport. Transport errors intentionally
/// carry no endpoint, response body, or raw error text so they cannot be
/// accidentally persisted by the worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WebhookTransportError {
    Timeout,
    Transport,
}

/// HTTP transport seam for deterministic tests and alternate runtime clients.
#[async_trait]
pub(crate) trait WebhookTransport: Send + Sync {
    async fn post_terminal(
        &self,
        endpoint: &Url,
        payload: &WebhookTerminalPayload,
        timeout: Duration,
    ) -> Result<u16, WebhookTransportError>;
}

/// Default HTTP transport. The endpoint is constructed only from validated
/// logging configuration, and a timeout is set on every individual request.
pub(crate) struct ReqwestWebhookTransport {
    client: reqwest::Client,
}

impl ReqwestWebhookTransport {
    /// Disable redirects: only the explicitly configured endpoint is allowed
    /// to receive a terminal notification.
    pub(crate) fn new() -> Result<Self, reqwest::Error> {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map(|client| Self { client })
    }
}

#[async_trait]
impl WebhookTransport for ReqwestWebhookTransport {
    async fn post_terminal(
        &self,
        endpoint: &Url,
        payload: &WebhookTerminalPayload,
        timeout: Duration,
    ) -> Result<u16, WebhookTransportError> {
        let response = self
            .client
            .post(endpoint.clone())
            .timeout(timeout)
            .json(payload)
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    WebhookTransportError::Timeout
                } else {
                    WebhookTransportError::Transport
                }
            })?;
        Ok(response.status().as_u16())
    }
}

/// Timestamp source kept independent from the request logging clock so a
/// worker test can make retry and lease times exact without wall-clock sleeps.
pub(crate) trait WebhookWorkerClock: Send + Sync {
    fn now(&self) -> String;
}

/// Production timestamp source for webhook worker scheduling.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SystemWebhookWorkerClock;

impl WebhookWorkerClock for SystemWebhookWorkerClock {
    fn now(&self) -> String {
        Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
    }
}

/// Bounded random source used only to decorrelate retry wakeups. Tests inject
/// a fixed implementation, so retry scheduling remains deterministic there.
pub(crate) trait WebhookJitter: Send + Sync {
    fn millis_up_to(&self, inclusive_maximum: u64) -> u64;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RandomWebhookJitter;

impl WebhookJitter for RandomWebhookJitter {
    fn millis_up_to(&self, inclusive_maximum: u64) -> u64 {
        if inclusive_maximum == 0 {
            0
        } else {
            rand::rng().random_range(0..=inclusive_maximum)
        }
    }
}

/// Construction errors are deliberately static: configuration diagnostics own
/// user-facing detail, and worker errors must not expose an endpoint value.
#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum WebhookWorkerConfigError {
    #[error("webhook delivery is disabled")]
    Disabled,
    #[error("webhook delivery requires an endpoint")]
    MissingEndpoint,
    #[error("webhook delivery endpoint is invalid")]
    InvalidEndpoint,
    #[error("webhook delivery timeout is outside the supported range")]
    InvalidTimeout,
}

#[derive(Debug, Error)]
pub(crate) enum WebhookWorkerError {
    #[error("webhook delivery store operation failed")]
    Store,
    #[error("webhook delivery clock produced an invalid timestamp")]
    InvalidTimestamp,
    #[error("webhook delivery blocking worker failed")]
    BlockingWorker,
}

/// The outcome of one non-blocking worker step. A runtime loop can use this to
/// choose its next wakeup without the request-serving path awaiting delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WebhookWorkerOutcome {
    Idle,
    Delivered {
        delivery_id: String,
        status_code: u16,
    },
    RetryScheduled {
        delivery_id: String,
    },
    DeadLettered {
        delivery_id: String,
    },
    FencedOut {
        delivery_id: String,
    },
}

/// One-at-a-time executor for durable webhook deliveries. It does not spawn a
/// loop; callers must explicitly schedule each invocation on a worker owner.
pub(crate) struct WebhookDeliveryWorker {
    store: Arc<LogStore>,
    endpoint: Url,
    timeout: Duration,
    transport: Arc<dyn WebhookTransport>,
    clock: Arc<dyn WebhookWorkerClock>,
    jitter: Arc<dyn WebhookJitter>,
    metrics: LoggingMetrics,
}

impl WebhookDeliveryWorker {
    /// Build a worker from the already-loaded logging configuration, repeating
    /// endpoint safety checks as a defense-in-depth boundary before it reaches
    /// the HTTP client.
    pub(crate) fn from_config(
        store: Arc<LogStore>,
        config: &LoggingWebhookConfig,
        transport: Arc<dyn WebhookTransport>,
        clock: Arc<dyn WebhookWorkerClock>,
        jitter: Arc<dyn WebhookJitter>,
    ) -> Result<Self, WebhookWorkerConfigError> {
        if !config.enabled {
            return Err(WebhookWorkerConfigError::Disabled);
        }
        if !(1..=60).contains(&config.timeout_secs) {
            return Err(WebhookWorkerConfigError::InvalidTimeout);
        }
        let endpoint = config
            .url
            .as_deref()
            .map(str::trim)
            .filter(|endpoint| !endpoint.is_empty())
            .ok_or(WebhookWorkerConfigError::MissingEndpoint)
            .and_then(validate_endpoint)?;

        Ok(Self {
            store,
            endpoint,
            timeout: Duration::from_secs(config.timeout_secs),
            transport,
            clock,
            jitter,
            metrics: LoggingMetrics::default(),
        })
    }

    /// Attach the process-local metrics handle owned by `LoggingService`.
    /// The worker still performs no telemetry I/O and keeps delivery fail-open.
    pub(crate) fn with_metrics(mut self, metrics: LoggingMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Claim and process at most one eligible delivery. All SQLite operations
    /// use Tokio's blocking pool, while the HTTP request remains async, so this
    /// future cannot park a request-serving executor worker on SQLite or I/O.
    pub(crate) async fn process_next(&self) -> Result<WebhookWorkerOutcome, WebhookWorkerError> {
        let claimed_at = self.clock.now();
        let lease_expires_at = timestamp_after(&claimed_at, lease_duration(self.timeout))?;
        let claimed = self
            .run_store(move |store| {
                store.claim_next_webhook_delivery(&claimed_at, &lease_expires_at)
            })
            .await?;
        let Some(record) = claimed else {
            return Ok(WebhookWorkerOutcome::Idle);
        };
        self.metrics.record(LoggingMetric::WebhookAttempt {
            state: LoggingWebhookAttemptState::Claimed,
        });

        let outcome = if let Some(payload) = WebhookTerminalPayload::from_record(&record) {
            match self
                .transport
                .post_terminal(&self.endpoint, &payload, self.timeout)
                .await
            {
                Ok(status_code) if (200..=299).contains(&status_code) => {
                    self.record_success(&record, status_code).await?
                }
                Ok(status_code) => {
                    self.record_failure(&record, error_code_for_status(status_code))
                        .await?
                }
                Err(WebhookTransportError::Timeout) => {
                    self.record_failure(&record, WebhookDeliveryErrorCode::Timeout)
                        .await?
                }
                Err(WebhookTransportError::Transport) => {
                    self.record_failure(&record, WebhookDeliveryErrorCode::Transport)
                        .await?
                }
            }
        } else {
            self.record_failure(&record, WebhookDeliveryErrorCode::Configuration)
                .await?
        };
        self.record_delivery_outcome(&outcome);
        Ok(outcome)
    }

    fn record_delivery_outcome(&self, outcome: &WebhookWorkerOutcome) {
        let outcome = match outcome {
            WebhookWorkerOutcome::Idle => return,
            WebhookWorkerOutcome::Delivered { .. } => LoggingWebhookDeliveryOutcome::Delivered,
            WebhookWorkerOutcome::RetryScheduled { .. } => {
                LoggingWebhookDeliveryOutcome::RetryScheduled
            }
            WebhookWorkerOutcome::DeadLettered { .. } => {
                LoggingWebhookDeliveryOutcome::DeadLettered
            }
            WebhookWorkerOutcome::FencedOut { .. } => LoggingWebhookDeliveryOutcome::FencedOut,
        };
        self.metrics
            .record(LoggingMetric::WebhookDelivery { outcome });
    }

    async fn record_success(
        &self,
        record: &WebhookDeliveryRecord,
        status_code: u16,
    ) -> Result<WebhookWorkerOutcome, WebhookWorkerError> {
        let delivery_id = record.delivery_id.clone();
        let completed_at = self.clock.now();
        let claim_generation = record.claim_generation;
        let completed = self
            .run_store(move |store| {
                store.complete_webhook_delivery(
                    &delivery_id,
                    claim_generation,
                    &completed_at,
                    status_code,
                )
            })
            .await?;
        if completed {
            Ok(WebhookWorkerOutcome::Delivered {
                delivery_id: record.delivery_id.clone(),
                status_code,
            })
        } else {
            Ok(WebhookWorkerOutcome::FencedOut {
                delivery_id: record.delivery_id.clone(),
            })
        }
    }

    async fn record_failure(
        &self,
        record: &WebhookDeliveryRecord,
        error_code: WebhookDeliveryErrorCode,
    ) -> Result<WebhookWorkerOutcome, WebhookWorkerError> {
        let delivery_id = record.delivery_id.clone();
        let updated_at = self.clock.now();
        let retry_delay = retry_delay(record.attempt_number, self.jitter.as_ref());
        let next_attempt_at = timestamp_after(&updated_at, retry_delay)?;
        let claim_generation = record.claim_generation;
        let result = self
            .run_store(move |store| {
                store.retry_or_dead_letter_webhook_delivery(
                    &delivery_id,
                    claim_generation,
                    &updated_at,
                    &next_attempt_at,
                    error_code,
                )
            })
            .await?;
        match result {
            Some(WebhookRetryOutcome::RetryScheduled) => Ok(WebhookWorkerOutcome::RetryScheduled {
                delivery_id: record.delivery_id.clone(),
            }),
            Some(WebhookRetryOutcome::DeadLettered) => Ok(WebhookWorkerOutcome::DeadLettered {
                delivery_id: record.delivery_id.clone(),
            }),
            None => Ok(WebhookWorkerOutcome::FencedOut {
                delivery_id: record.delivery_id.clone(),
            }),
        }
    }

    async fn run_store<T>(
        &self,
        operation: impl FnOnce(&LogStore) -> Result<T, LogStoreError> + Send + 'static,
    ) -> Result<T, WebhookWorkerError>
    where
        T: Send + 'static,
    {
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || operation(&store))
            .await
            .map_err(|_| WebhookWorkerError::BlockingWorker)?
            .map_err(|_| WebhookWorkerError::Store)
    }
}

fn validate_endpoint(value: &str) -> Result<Url, WebhookWorkerConfigError> {
    let endpoint = Url::parse(value).map_err(|_| WebhookWorkerConfigError::InvalidEndpoint)?;
    let is_http = endpoint.scheme() == "http" || endpoint.scheme() == "https";
    if !is_http
        || endpoint.host().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(WebhookWorkerConfigError::InvalidEndpoint);
    }
    Ok(endpoint)
}

fn lease_duration(timeout: Duration) -> Duration {
    timeout
        .checked_add(LEASE_SAFETY_MARGIN)
        .unwrap_or(LEASE_SAFETY_MARGIN)
}

fn retry_delay(attempt_number: u32, jitter: &dyn WebhookJitter) -> Duration {
    let exponent = attempt_number.saturating_sub(1).min(6);
    let exponential = BASE_RETRY_DELAY.saturating_mul(1_u32 << exponent);
    let capped = exponential.min(MAX_RETRY_DELAY);
    let jitter_limit = capped
        .as_millis()
        .min(u128::from(MAX_JITTER_MILLIS))
        .try_into()
        .unwrap_or(MAX_JITTER_MILLIS)
        / 4;
    let remaining = MAX_RETRY_DELAY.saturating_sub(capped);
    let jitter = Duration::from_millis(
        jitter
            .millis_up_to(jitter_limit)
            .min(remaining.as_millis() as u64),
    );
    capped.checked_add(jitter).unwrap_or(MAX_RETRY_DELAY)
}

fn timestamp_after(value: &str, duration: Duration) -> Result<String, WebhookWorkerError> {
    let timestamp = DateTime::parse_from_rfc3339(value)
        .map_err(|_| WebhookWorkerError::InvalidTimestamp)?
        .with_timezone(&Utc);
    let milliseconds =
        i64::try_from(duration.as_millis()).map_err(|_| WebhookWorkerError::InvalidTimestamp)?;
    timestamp
        .checked_add_signed(ChronoDuration::milliseconds(milliseconds))
        .map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Millis, true))
        .ok_or(WebhookWorkerError::InvalidTimestamp)
}

fn error_code_for_status(status_code: u16) -> WebhookDeliveryErrorCode {
    if (400..=499).contains(&status_code) {
        WebhookDeliveryErrorCode::Http4xx
    } else {
        WebhookDeliveryErrorCode::Http5xx
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use mesh_llm_log_store::{Clock as StoreClock, WebhookDeliveryState};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{Notify, watch};

    use super::*;
    use crate::logging::operator_audit::OperatorAuditWriter;

    const NOW: &str = "2026-08-04T12:00:00Z";

    #[derive(Clone)]
    struct FixedClock;

    impl WebhookWorkerClock for FixedClock {
        fn now(&self) -> String {
            NOW.to_string()
        }
    }

    impl StoreClock for FixedClock {
        fn now(&self) -> String {
            NOW.to_string()
        }
    }

    struct FixedJitter(u64);

    impl WebhookJitter for FixedJitter {
        fn millis_up_to(&self, inclusive_maximum: u64) -> u64 {
            self.0.min(inclusive_maximum)
        }
    }

    #[derive(Clone)]
    struct AdjustableClock {
        value: Arc<Mutex<String>>,
    }

    impl AdjustableClock {
        fn new(value: &str) -> Self {
            Self {
                value: Arc::new(Mutex::new(value.to_owned())),
            }
        }

        fn set(&self, value: &str) {
            *self.value.lock().expect("clock lock") = value.to_owned();
        }
    }

    impl WebhookWorkerClock for AdjustableClock {
        fn now(&self) -> String {
            self.value.lock().expect("clock lock").clone()
        }
    }

    impl StoreClock for AdjustableClock {
        fn now(&self) -> String {
            WebhookWorkerClock::now(self)
        }
    }

    #[derive(Clone, Copy)]
    enum LocalHttpReply {
        Status(u16),
        Stall,
    }

    struct LocalFakeHttpServer {
        endpoint: String,
        requests: Arc<Mutex<Vec<String>>>,
        received: Arc<Notify>,
        shutdown_tx: watch::Sender<bool>,
        task: tokio::task::JoinHandle<()>,
    }

    impl LocalFakeHttpServer {
        async fn start(replies: impl IntoIterator<Item = LocalHttpReply>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind local fake webhook server");
            let endpoint = format!(
                "http://{}/webhook",
                listener.local_addr().expect("local server address")
            );
            let requests = Arc::new(Mutex::new(Vec::new()));
            let received = Arc::new(Notify::new());
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let task = tokio::spawn(run_local_fake_http_server(
                listener,
                replies.into_iter().collect(),
                Arc::clone(&requests),
                Arc::clone(&received),
                shutdown_rx,
            ));
            Self {
                endpoint,
                requests,
                received,
                shutdown_tx,
                task,
            }
        }

        async fn wait_for_requests(&self, expected: usize) {
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if self.requests.lock().expect("request lock").len() >= expected {
                        return;
                    }
                    self.received.notified().await;
                }
            })
            .await
            .expect("fake server received expected requests");
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().expect("request lock").clone()
        }

        async fn shutdown(self) {
            let _ = self.shutdown_tx.send(true);
            self.task.abort();
            let _ = self.task.await;
        }
    }

    async fn run_local_fake_http_server(
        listener: TcpListener,
        mut replies: VecDeque<LocalHttpReply>,
        requests: Arc<Mutex<Vec<String>>>,
        received: Arc<Notify>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        loop {
            let accepted = tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return;
                    }
                    continue;
                }
                accepted = listener.accept() => accepted,
            };
            let (mut stream, _) = accepted.expect("accept fake webhook request");
            let request = read_fake_http_request(&mut stream).await;
            requests.lock().expect("request lock").push(request);
            received.notify_one();
            match replies.pop_front().unwrap_or(LocalHttpReply::Status(500)) {
                LocalHttpReply::Status(status) => {
                    write_fake_http_response(&mut stream, status).await
                }
                LocalHttpReply::Stall => {
                    let _ = shutdown_rx.changed().await;
                    return;
                }
            }
        }
    }

    async fn read_fake_http_request(stream: &mut TcpStream) -> String {
        const MAX_REQUEST_BYTES: usize = 16 * 1024;
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = stream.read(&mut chunk).await.expect("read webhook request");
            assert!(read > 0, "webhook client closed before sending a request");
            bytes.extend_from_slice(&chunk[..read]);
            assert!(
                bytes.len() <= MAX_REQUEST_BYTES,
                "fake request exceeded bound"
            );
            let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let headers = std::str::from_utf8(&bytes[..header_end]).expect("request headers utf-8");
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.strip_prefix("content-length: ")
                        .or_else(|| line.strip_prefix("Content-Length: "))
                })
                .and_then(|value| value.parse::<usize>().ok())
                .expect("webhook request content length");
            if bytes.len() >= header_end + 4 + content_length {
                return String::from_utf8(bytes).expect("request utf-8");
            }
        }
    }

    async fn write_fake_http_response(stream: &mut TcpStream, status: u16) {
        stream
            .write_all(
                format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .expect("write fake webhook response");
    }

    #[derive(Clone, Copy)]
    enum FakeReply {
        Status(u16),
        Timeout,
    }

    struct FakeTransport {
        replies: Mutex<VecDeque<FakeReply>>,
        payloads: Mutex<Vec<WebhookTerminalPayload>>,
    }

    impl FakeTransport {
        fn new(replies: impl IntoIterator<Item = FakeReply>) -> Self {
            Self {
                replies: Mutex::new(replies.into_iter().collect()),
                payloads: Mutex::new(Vec::new()),
            }
        }

        fn payloads(&self) -> Vec<WebhookTerminalPayload> {
            self.payloads.lock().expect("payload lock").clone()
        }
    }

    #[async_trait]
    impl WebhookTransport for FakeTransport {
        async fn post_terminal(
            &self,
            _endpoint: &Url,
            payload: &WebhookTerminalPayload,
            _timeout: Duration,
        ) -> Result<u16, WebhookTransportError> {
            self.payloads
                .lock()
                .expect("payload lock")
                .push(payload.clone());
            match self.replies.lock().expect("reply lock").pop_front() {
                Some(FakeReply::Status(status)) => Ok(status),
                Some(FakeReply::Timeout) => Err(WebhookTransportError::Timeout),
                None => Err(WebhookTransportError::Transport),
            }
        }
    }

    struct GatedTransport {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl WebhookTransport for GatedTransport {
        async fn post_terminal(
            &self,
            _endpoint: &Url,
            _payload: &WebhookTerminalPayload,
            _timeout: Duration,
        ) -> Result<u16, WebhookTransportError> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(204)
        }
    }

    fn open_store() -> (Arc<LogStore>, tempfile::TempDir) {
        let root = tempfile::tempdir().expect("test root");
        let store = LogStore::open(root.path(), Arc::new(FixedClock)).expect("open store");
        (Arc::new(store), root)
    }

    fn seed_terminal_delivery(store: &LogStore, delivery_id: &str, max_attempts: u32) {
        let request_id = format!("request-{delivery_id}");
        store
            .insert_summary(&request_id, None, None, None, None, NOW, None, None, None)
            .expect("summary");
        store
            .write_terminal_event(
                &request_id,
                &format!("event-{delivery_id}"),
                r#"{"type":"completed","prompt":"prompt-secret","path":"/private/secret","artifact_url":"https://artifact.invalid/value"}"#,
                "completed",
                NOW,
            )
            .expect("terminal event");
        store
            .enqueue_webhook_delivery(delivery_id, &request_id, NOW, max_attempts)
            .expect("delivery");
    }

    fn worker(store: Arc<LogStore>, transport: Arc<dyn WebhookTransport>) -> WebhookDeliveryWorker {
        let config = LoggingWebhookConfig {
            enabled: true,
            url: Some("http://127.0.0.1:9444/webhook".to_string()),
            max_attempts: 3,
            timeout_secs: 1,
            dead_letter_retention_secs: 3_600,
        };
        WebhookDeliveryWorker::from_config(
            store,
            &config,
            transport,
            Arc::new(FixedClock),
            Arc::new(FixedJitter(250)),
        )
        .expect("worker config")
    }

    fn real_http_worker(
        store: Arc<LogStore>,
        endpoint: String,
        clock: AdjustableClock,
    ) -> WebhookDeliveryWorker {
        let transport: Arc<dyn WebhookTransport> =
            Arc::new(ReqwestWebhookTransport::new().expect("real webhook transport"));
        WebhookDeliveryWorker::from_config(
            store,
            &LoggingWebhookConfig {
                enabled: true,
                url: Some(endpoint),
                max_attempts: 3,
                timeout_secs: 1,
                dead_letter_retention_secs: 3_600,
            },
            transport,
            Arc::new(clock),
            Arc::new(FixedJitter(0)),
        )
        .expect("real worker config")
    }

    fn open_adjustable_store(clock: &AdjustableClock) -> (Arc<LogStore>, tempfile::TempDir) {
        let root = tempfile::tempdir().expect("test root");
        let store = LogStore::open(root.path(), Arc::new(clock.clone())).expect("open store");
        (Arc::new(store), root)
    }

    fn seed_terminal_delivery_with_private_event(
        store: &LogStore,
        delivery_id: &str,
        occurred_at: &str,
        max_attempts: u32,
    ) -> String {
        let request_id = format!("request-{delivery_id}");
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
            .expect("summary");
        store
            .write_terminal_event(
                &request_id,
                &format!("event-{delivery_id}"),
                r#"{"type":"completed","prompt":"prompt-secret","completion":"completion-secret","artifact":"/private/secret","credential":"webhook-secret"}"#,
                "completed",
                occurred_at,
            )
            .expect("terminal event");
        store
            .enqueue_webhook_delivery(delivery_id, &request_id, occurred_at, max_attempts)
            .expect("delivery");
        request_id
    }

    fn assert_private_delivery_storage(store: &LogStore, delivery_id: &str) {
        let (target_url, response_body, error_msg): (String, Option<String>, Option<String>) = store
            .conn()
            .query_row(
                "SELECT target_url, response_body, error_msg FROM webhook_deliveries WHERE delivery_id = ?1",
                [delivery_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("private delivery storage");
        for value in [target_url]
            .into_iter()
            .chain(response_body)
            .chain(error_msg)
        {
            for secret in [
                "prompt-secret",
                "completion-secret",
                "/private/secret",
                "webhook-secret",
                "127.0.0.1",
            ] {
                assert!(!value.contains(secret), "delivery storage leaked {secret}");
            }
        }
    }

    #[tokio::test]
    async fn real_http_worker_delivers_a_private_terminal_payload() {
        let clock = AdjustableClock::new(NOW);
        let (store, _root) = open_adjustable_store(&clock);
        let server = LocalFakeHttpServer::start([LocalHttpReply::Status(204)]).await;
        let delivery_id = "real-success";
        let request_id = seed_terminal_delivery_with_private_event(&store, delivery_id, NOW, 2);

        let outcome = real_http_worker(Arc::clone(&store), server.endpoint.clone(), clock)
            .process_next()
            .await
            .expect("successful local HTTP delivery");

        assert_eq!(
            outcome,
            WebhookWorkerOutcome::Delivered {
                delivery_id: delivery_id.to_owned(),
                status_code: 204,
            }
        );
        assert_eq!(
            store.webhook_delivery(delivery_id).unwrap().unwrap().state,
            WebhookDeliveryState::Succeeded
        );
        server.wait_for_requests(1).await;
        let request = server.requests().pop().expect("captured webhook request");
        assert!(request.starts_with("POST /webhook HTTP/1.1\r\n"));
        let body = request.split("\r\n\r\n").nth(1).expect("request body");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body).expect("terminal payload JSON"),
            serde_json::json!({ "request_id": request_id, "outcome": "terminal" })
        );
        for secret in [
            "prompt-secret",
            "completion-secret",
            "/private/secret",
            "webhook-secret",
            "credential",
        ] {
            assert!(!request.contains(secret), "webhook request leaked {secret}");
        }
        assert_private_delivery_storage(&store, delivery_id);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn real_http_worker_retries_5xx_and_dead_letters_at_the_attempt_bound() {
        let clock = AdjustableClock::new(NOW);
        let (store, _root) = open_adjustable_store(&clock);
        let server = LocalFakeHttpServer::start([
            LocalHttpReply::Status(503),
            LocalHttpReply::Status(204),
            LocalHttpReply::Status(502),
        ])
        .await;
        let retry_id = "real-retry";
        seed_terminal_delivery_with_private_event(&store, retry_id, NOW, 2);
        let worker = real_http_worker(Arc::clone(&store), server.endpoint.clone(), clock.clone());

        assert_eq!(
            worker.process_next().await.expect("5xx worker step"),
            WebhookWorkerOutcome::RetryScheduled {
                delivery_id: retry_id.to_owned(),
            }
        );
        let retry = store.webhook_delivery(retry_id).unwrap().unwrap();
        assert_eq!(retry.state, WebhookDeliveryState::Retry);
        assert_eq!(
            retry.last_error_code,
            Some(WebhookDeliveryErrorCode::Http5xx)
        );
        assert_eq!(
            retry.next_attempt_at.as_deref(),
            Some("2026-08-04T12:00:01.000Z")
        );

        clock.set("2026-08-04T12:00:01Z");
        assert_eq!(
            worker.process_next().await.expect("retry worker step"),
            WebhookWorkerOutcome::Delivered {
                delivery_id: retry_id.to_owned(),
                status_code: 204,
            }
        );
        assert_eq!(
            store.webhook_delivery(retry_id).unwrap().unwrap().state,
            WebhookDeliveryState::Succeeded
        );

        let dead_letter_id = "real-dead-letter";
        seed_terminal_delivery_with_private_event(
            &store,
            dead_letter_id,
            "2026-08-04T12:00:01Z",
            1,
        );
        assert_eq!(
            worker
                .process_next()
                .await
                .expect("dead letter worker step"),
            WebhookWorkerOutcome::DeadLettered {
                delivery_id: dead_letter_id.to_owned(),
            }
        );
        let dead_letter = store.webhook_delivery(dead_letter_id).unwrap().unwrap();
        assert_eq!(dead_letter.state, WebhookDeliveryState::DeadLetter);
        assert_eq!(dead_letter.attempt_number, 1);
        assert_eq!(
            dead_letter.last_error_code,
            Some(WebhookDeliveryErrorCode::Http5xx)
        );
        server.wait_for_requests(3).await;
        assert_private_delivery_storage(&store, retry_id);
        assert_private_delivery_storage(&store, dead_letter_id);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn real_http_timeout_keeps_terminal_persistence_off_the_delivery_path() {
        let clock = AdjustableClock::new(NOW);
        let (store, _root) = open_adjustable_store(&clock);
        let server = LocalFakeHttpServer::start([LocalHttpReply::Stall]).await;
        seed_terminal_delivery_with_private_event(&store, "real-timeout", NOW, 2);
        let worker = real_http_worker(Arc::clone(&store), server.endpoint.clone(), clock.clone());
        let worker_task = tokio::spawn(async move { worker.process_next().await });

        server.wait_for_requests(1).await;
        tokio::time::timeout(Duration::from_millis(250), {
            let store = Arc::clone(&store);
            tokio::task::spawn_blocking(move || {
                seed_terminal_delivery_with_private_event(
                    &store,
                    "terminal-while-http-stalls",
                    NOW,
                    1,
                )
            })
        })
        .await
        .expect("terminal persistence is not delayed by HTTP")
        .expect("terminal persistence task");

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), worker_task)
                .await
                .expect("HTTP timeout is bounded")
                .expect("worker task")
                .expect("timeout worker result"),
            WebhookWorkerOutcome::RetryScheduled {
                delivery_id: "real-timeout".to_owned(),
            }
        );
        let timeout_record = store.webhook_delivery("real-timeout").unwrap().unwrap();
        assert_eq!(timeout_record.state, WebhookDeliveryState::Retry);
        assert_eq!(
            timeout_record.last_error_code,
            Some(WebhookDeliveryErrorCode::Timeout)
        );
        assert!(
            store
                .webhook_delivery("terminal-while-http-stalls")
                .unwrap()
                .is_some(),
            "terminal persistence remains durable while HTTP is pending"
        );
        assert_private_delivery_storage(&store, "real-timeout");
        server.shutdown().await;
    }

    #[tokio::test]
    async fn real_http_worker_resumes_after_restart_and_completes_audited_manual_retry() {
        let clock = AdjustableClock::new(NOW);
        let root = tempfile::tempdir().expect("restart test root");
        let first_store =
            Arc::new(LogStore::open(root.path(), Arc::new(clock.clone())).expect("initial store"));
        seed_terminal_delivery_with_private_event(&first_store, "real-restart", NOW, 2);
        first_store
            .claim_next_webhook_delivery("2026-08-04T12:00:00Z", "2026-08-04T12:00:01Z")
            .expect("claim before restart")
            .expect("initial claim");
        drop(first_store);
        clock.set("2026-08-04T12:01:00Z");

        let store = Arc::new(
            LogStore::reopen_at(root.path(), Arc::new(clock.clone())).expect("reopened store"),
        );
        let server = LocalFakeHttpServer::start([
            LocalHttpReply::Status(204),
            LocalHttpReply::Status(503),
            LocalHttpReply::Status(204),
        ])
        .await;
        let worker = real_http_worker(Arc::clone(&store), server.endpoint.clone(), clock.clone());
        assert_eq!(
            worker.process_next().await.expect("restart worker step"),
            WebhookWorkerOutcome::Delivered {
                delivery_id: "real-restart".to_owned(),
                status_code: 204,
            }
        );

        seed_terminal_delivery_with_private_event(&store, "real-manual-retry", NOW, 1);
        assert_eq!(
            worker
                .process_next()
                .await
                .expect("manual retry initial step"),
            WebhookWorkerOutcome::DeadLettered {
                delivery_id: "real-manual-retry".to_owned(),
            }
        );
        assert!(
            store
                .manually_retry_webhook_delivery(
                    "real-manual-retry",
                    &WebhookWorkerClock::now(&clock),
                )
                .expect("manual retry transition")
        );
        OperatorAuditWriter::new()
            .write(
                Arc::clone(&store),
                "log_webhook_manual_retry",
                "operator webhook retry".to_owned(),
                "succeeded",
            )
            .expect("manual retry audit");
        assert_eq!(
            worker
                .process_next()
                .await
                .expect("manual retry worker step"),
            WebhookWorkerOutcome::Delivered {
                delivery_id: "real-manual-retry".to_owned(),
                status_code: 204,
            }
        );
        assert_eq!(
            store
                .webhook_delivery("real-manual-retry")
                .unwrap()
                .unwrap()
                .state,
            WebhookDeliveryState::Succeeded
        );
        let detail: String = store
            .conn()
            .query_row(
                "SELECT detail_json FROM audit_entries WHERE action = 'log_webhook_manual_retry'",
                [],
                |row| row.get(0),
            )
            .expect("manual retry audit detail");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&detail).expect("audit detail JSON"),
            serde_json::json!({
                "actor": "trusted_local_operator",
                "source": "logs_api",
                "result": "succeeded",
                "reason": "operator webhook retry",
            })
        );
        for secret in [
            "real-manual-retry",
            "prompt-secret",
            "completion-secret",
            "/private/secret",
            "webhook-secret",
            "127.0.0.1",
        ] {
            assert!(!detail.contains(secret), "audit leaked {secret}");
        }
        server.wait_for_requests(3).await;
        assert_private_delivery_storage(&store, "real-restart");
        assert_private_delivery_storage(&store, "real-manual-retry");
        server.shutdown().await;
    }

    #[tokio::test]
    async fn worker_delivers_a_redacted_terminal_payload_on_success() {
        let (store, _root) = open_store();
        seed_terminal_delivery(&store, "success", 2);
        let transport = Arc::new(FakeTransport::new([FakeReply::Status(204)]));

        let outcome = worker(Arc::clone(&store), transport.clone())
            .process_next()
            .await
            .expect("worker step");

        assert_eq!(
            outcome,
            WebhookWorkerOutcome::Delivered {
                delivery_id: "success".to_string(),
                status_code: 204,
            }
        );
        assert_eq!(
            store.webhook_delivery("success").unwrap().unwrap().state,
            WebhookDeliveryState::Succeeded
        );
        let payloads = transport.payloads();
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].request_id, "request-success");
        assert_eq!(payloads[0].outcome, "terminal");
        assert_eq!(payloads[0].status_code, None);
        let payload_json = serde_json::to_string(&payloads[0]).expect("payload json");
        for forbidden in [
            "prompt-secret",
            "/private/secret",
            "artifact.invalid",
            "webhook",
        ] {
            assert!(
                !payload_json.contains(forbidden),
                "payload leaked {forbidden}"
            );
        }
    }

    #[tokio::test]
    async fn worker_schedules_deterministic_retry_for_5xx() {
        let (store, _root) = open_store();
        seed_terminal_delivery(&store, "five-xx", 2);

        let outcome = worker(
            Arc::clone(&store),
            Arc::new(FakeTransport::new([FakeReply::Status(503)])),
        )
        .process_next()
        .await
        .expect("worker step");

        assert_eq!(
            outcome,
            WebhookWorkerOutcome::RetryScheduled {
                delivery_id: "five-xx".to_string(),
            }
        );
        let record = store.webhook_delivery("five-xx").unwrap().unwrap();
        assert_eq!(record.state, WebhookDeliveryState::Retry);
        assert_eq!(
            record.last_error_code,
            Some(WebhookDeliveryErrorCode::Http5xx)
        );
        assert_eq!(
            record.next_attempt_at.as_deref(),
            Some("2026-08-04T12:00:01.250Z")
        );
    }

    #[tokio::test]
    async fn worker_schedules_retry_for_timeout_without_persisting_error_text() {
        let (store, _root) = open_store();
        seed_terminal_delivery(&store, "timeout", 2);

        let outcome = worker(
            Arc::clone(&store),
            Arc::new(FakeTransport::new([FakeReply::Timeout])),
        )
        .process_next()
        .await
        .expect("worker step");

        assert_eq!(
            outcome,
            WebhookWorkerOutcome::RetryScheduled {
                delivery_id: "timeout".to_string(),
            }
        );
        let record = store.webhook_delivery("timeout").unwrap().unwrap();
        assert_eq!(
            record.last_error_code,
            Some(WebhookDeliveryErrorCode::Timeout)
        );
        let raw_error: Option<String> = store
            .conn()
            .query_row(
                "SELECT error_msg FROM webhook_deliveries WHERE delivery_id = 'timeout'",
                [],
                |row| row.get(0),
            )
            .expect("stored error");
        assert!(raw_error.is_none());
    }

    #[tokio::test]
    async fn worker_dead_letters_after_the_configured_attempt_bound() {
        let (store, _root) = open_store();
        seed_terminal_delivery(&store, "dead-letter", 1);

        let outcome = worker(
            Arc::clone(&store),
            Arc::new(FakeTransport::new([FakeReply::Status(503)])),
        )
        .process_next()
        .await
        .expect("worker step");

        assert_eq!(
            outcome,
            WebhookWorkerOutcome::DeadLettered {
                delivery_id: "dead-letter".to_string(),
            }
        );
        let record = store.webhook_delivery("dead-letter").unwrap().unwrap();
        assert_eq!(record.state, WebhookDeliveryState::DeadLetter);
        assert_eq!(record.attempt_number, 1);
    }

    #[tokio::test]
    async fn worker_transport_wait_does_not_block_the_serving_runtime() {
        let (store, _root) = open_store();
        seed_terminal_delivery(&store, "nonblocking", 1);
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let transport = Arc::new(GatedTransport {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        });
        let worker = worker(store, transport);
        let task = tokio::spawn(async move { worker.process_next().await });

        started.notified().await;
        let served = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let served_on_runtime = Arc::clone(&served);
        tokio::spawn(async move {
            served_on_runtime.store(true, std::sync::atomic::Ordering::Release);
        })
        .await
        .expect("serving task");
        assert!(served.load(std::sync::atomic::Ordering::Acquire));

        release.notify_one();
        assert!(matches!(
            task.await.expect("worker task").expect("worker result"),
            WebhookWorkerOutcome::Delivered { .. }
        ));
    }

    #[test]
    fn worker_rejects_endpoints_that_configuration_validation_forbids() {
        let (store, _root) = open_store();
        let config = LoggingWebhookConfig {
            enabled: true,
            url: Some("https://operator:secret@example.invalid/hook?token=secret".to_string()),
            max_attempts: 3,
            timeout_secs: 1,
            dead_letter_retention_secs: 3_600,
        };

        let result = WebhookDeliveryWorker::from_config(
            store,
            &config,
            Arc::new(FakeTransport::new([])),
            Arc::new(FixedClock),
            Arc::new(FixedJitter(0)),
        );
        let Err(error) = result else {
            panic!("unsafe endpoint must be rejected");
        };

        assert_eq!(error, WebhookWorkerConfigError::InvalidEndpoint);
        assert!(!error.to_string().contains("secret"));
    }
}
