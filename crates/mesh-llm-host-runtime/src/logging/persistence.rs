//! Durable `PersistSink` implementation backed by the typed SQLite repositories.

use std::sync::Arc;

use async_trait::async_trait;
use mesh_llm_events::logging::envelope::CanonicalEnvelope;
use mesh_llm_events::logging::events::LifecycleEvent;
use mesh_llm_events::logging::identifiers::EventId;
use mesh_llm_events::logging::replay::ReplayChannel;
use mesh_llm_log_store::{LogStore, LogStoreError};

use super::registry::RequestSummaryEntry;
use super::service::PersistSink;

/// Production persistence adapter for the logging service's typed LogStore.
///
/// It deliberately writes summaries and lifecycle envelopes through their
/// dedicated repositories. Operational audit records use their own sink method
/// and never share the lifecycle tables.
pub struct LogStoreSink {
    store: Arc<LogStore>,
    #[cfg(test)]
    before_blocking_operation: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl LogStoreSink {
    pub fn new(store: Arc<LogStore>) -> Self {
        Self {
            store,
            #[cfg(test)]
            before_blocking_operation: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_blocking_hook_for_test(
        store: Arc<LogStore>,
        before_blocking_operation: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            store,
            before_blocking_operation: Some(before_blocking_operation),
        }
    }

    fn map_error(error: LogStoreError) -> String {
        error.to_string()
    }

    /// Run the synchronous rusqlite repository operation on Tokio's bounded
    /// blocking pool. The logging service awaits these operations one at a
    /// time, so this is a serialized hand-off rather than per-entry task
    /// fan-out. In particular, SQLite's 30 second busy timeout can never park
    /// a shared async executor worker.
    async fn run_blocking<T, F>(&self, operation: F) -> Result<T, String>
    where
        T: Send + 'static,
        F: FnOnce(Arc<LogStore>) -> Result<T, LogStoreError> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        #[cfg(test)]
        let before_blocking_operation = self.before_blocking_operation.clone();
        tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(hook) = before_blocking_operation {
                hook();
            }
            operation(store)
        })
        .await
        .map_err(|error| format!("logging sqlite worker failed: {error}"))?
        .map_err(Self::map_error)
    }
}

#[async_trait]
impl PersistSink for LogStoreSink {
    async fn persist_summary(&self, entry: RequestSummaryEntry) -> Result<(), String> {
        self.run_blocking(move |store| {
            store.insert_summary(
                &entry.request_id,
                None,
                None,
                None,
                None,
                &entry.created_at,
                None,
                None,
                None,
            )
        })
        .await
    }

    async fn persist_event(
        &self,
        request_id: String,
        event_id: String,
        _channel: ReplayChannel,
        _sequence: u64,
        occurred_at: String,
        payload_json: String,
    ) -> Result<(), String> {
        let envelope = CanonicalEnvelope::from_json_str(&payload_json)
            .map_err(|error| format!("invalid canonical lifecycle envelope: {error}"))?;
        if envelope.request_id.as_uuid().to_string() != request_id
            || envelope.event_id.as_uuid().to_string() != event_id
            || envelope.occurred_at != occurred_at
        {
            return Err("canonical lifecycle envelope does not match persistence key".to_string());
        }

        match terminal_status(&envelope.event) {
            Some(status) => {
                self.run_blocking(move |store| {
                    store.write_terminal_event(
                        &request_id,
                        &event_id,
                        &payload_json,
                        status,
                        &occurred_at,
                    )
                })
                .await
            }
            None => {
                self.run_blocking(move |store| {
                    store.insert_lifecycle_event(
                        &request_id,
                        &event_id,
                        &payload_json,
                        &occurred_at,
                    )
                })
                .await
            }
        }
    }

    async fn persist_artifact_pointer(
        &self,
        _request_id: String,
        _artifact_data: serde_json::Value,
    ) -> Result<(), String> {
        Err("artifact persistence is not wired by the lifecycle service".to_string())
    }

    async fn persist_proxy_record(&self, _proxy_json: String) -> Result<(), String> {
        Err("proxy persistence is not wired by the lifecycle service".to_string())
    }

    async fn persist_audit_entry(&self, level: String, message: String) -> Result<(), String> {
        let entry_id = EventId::new().as_uuid().to_string();
        let occurred_at = self.store.now();
        self.run_blocking(move |store| {
            store.insert_audit_entry(
                &entry_id,
                None,
                &occurred_at,
                "logging-service",
                &level,
                Some(&message),
            )
        })
        .await
    }

    async fn persist_webhook_delivery(
        &self,
        _request_id: Option<String>,
        _status_code: u16,
        _error: Option<String>,
    ) -> Result<(), String> {
        Err("webhook persistence is not wired by the lifecycle service".to_string())
    }

    async fn persist_cleanup_run(&self, _deleted_count: u64) -> Result<(), String> {
        Err("cleanup persistence is not wired by the lifecycle service".to_string())
    }
}

fn terminal_status(event: &LifecycleEvent) -> Option<&'static str> {
    match event {
        LifecycleEvent::Completed { .. } => Some("completed"),
        LifecycleEvent::Failed { .. } => Some("failed"),
        LifecycleEvent::Rejected { .. } => Some("rejected"),
        LifecycleEvent::Cancelled { .. } => Some("cancelled"),
        LifecycleEvent::Dropped { .. } => Some("dropped"),
        _ => None,
    }
}
