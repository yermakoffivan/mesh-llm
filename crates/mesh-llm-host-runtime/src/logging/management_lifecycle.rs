//! Metadata-only lifecycle ownership for one management API request.

use std::sync::Arc;

use mesh_llm_events::logging::{
    events::LifecycleEvent, identifiers::RequestId, replay::ReplayChannel,
};

use super::{LifecycleGuard, LoggingService, RequestSummaryMetadata, TerminalOutcome};

pub(crate) struct ManagementRequestLifecycle {
    request_id: RequestId,
    guard: LifecycleGuard,
    service: Arc<LoggingService>,
}

impl ManagementRequestLifecycle {
    pub(crate) fn register(
        service: Arc<LoggingService>,
        request_id: RequestId,
        method_route: &'static str,
    ) -> Self {
        let metadata = RequestSummaryMetadata::from_parts(
            Some(method_route),
            None,
            Some("management_api"),
            Some(method_route),
        );
        let (guard, _) = service.register_request_with_metadata(request_id, metadata.clone());
        if let Ok(payload) = serde_json::to_string(&LifecycleEvent::RouteSelected {
            model: None,
            provider: metadata.provider().map(str::to_owned),
            engine: metadata.engine().map(str::to_owned),
        }) {
            let _ = service.enqueue_event(request_id, ReplayChannel::Operations, payload);
        }
        Self {
            request_id,
            guard,
            service,
        }
    }

    pub(crate) fn finish_status(&self, status: u16) {
        let outcome = if status < 400 {
            TerminalOutcome::Completed
        } else if status < 500 {
            TerminalOutcome::Rejected(Some("management_http_rejected".into()))
        } else {
            TerminalOutcome::Failed("management_http_failed".into())
        };
        let _ = self
            .service
            .transition_terminal(self.request_id, &self.guard, outcome);
    }

    pub(crate) fn fail_dispatch(&self) {
        let _ = self.service.transition_terminal(
            self.request_id,
            &self.guard,
            TerminalOutcome::Failed("management_dispatch_failed".into()),
        );
    }

    pub(crate) fn request_id(&self) -> RequestId {
        self.request_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn management_registration_captures_only_static_summary_metadata() {
        let service = Arc::new(LoggingService::new_disabled(Default::default()));
        let request_id = RequestId::new();
        let lifecycle = ManagementRequestLifecycle::register(
            Arc::clone(&service),
            request_id,
            "management_get_status",
        );

        lifecycle.finish_status(200);

        let summary = service
            .registry_ref()
            .get_recent(&request_id.as_uuid().to_string())
            .expect("terminal request summary");
        assert_eq!(summary.metadata.route(), Some("management_get_status"));
        assert!(summary.metadata.model().is_none());
        assert_eq!(summary.metadata.provider(), Some("management_api"));
        assert_eq!(summary.metadata.engine(), Some("management_get_status"));
    }
}
