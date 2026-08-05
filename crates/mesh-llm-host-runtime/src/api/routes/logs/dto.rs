use base64::Engine;
use mesh_llm_events::logging::envelope::CanonicalEnvelope;
use mesh_llm_events::logging::events::LifecycleEvent;
use mesh_llm_log_store::{
    ArtifactContent, ArtifactRecord, EventRecord, ProxyRecord, RequestRecord,
};
use serde::Serialize;

use super::LogsError;
use crate::logging::RequestSummaryEntry;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PageDto<T> {
    pub(crate) items: Vec<T>,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequestDto {
    request_id: String,
    outcome: String,
    created_at: String,
    terminal_at: Option<String>,
    route: Option<String>,
    model: Option<String>,
    provider: Option<String>,
    engine: Option<String>,
    status_code: Option<i64>,
    source: &'static str,
}

impl RequestDto {
    pub(super) fn durable(record: RequestRecord) -> Self {
        Self {
            request_id: record.request_id,
            outcome: record.outcome,
            created_at: record.created_at,
            terminal_at: record.terminal_at,
            route: record.route.as_deref().map(safe_metadata),
            model: record.model.as_deref().map(safe_metadata),
            provider: record.provider.as_deref().map(safe_metadata),
            engine: record.engine.as_deref().map(safe_metadata),
            status_code: record.status_code,
            source: "durable",
        }
    }

    pub(super) fn active(entry: RequestSummaryEntry, metadata: Option<RequestRecord>) -> Self {
        let summary_metadata = entry.metadata.clone();
        let metadata = metadata.map(|record| {
            (
                record.route.as_deref().map(safe_metadata),
                record.model.as_deref().map(safe_metadata),
                record.provider.as_deref().map(safe_metadata),
                record.engine.as_deref().map(safe_metadata),
                record.status_code,
            )
        });
        let (route, model, provider, engine, status_code) = metadata.unwrap_or_default();
        Self {
            request_id: entry.request_id,
            outcome: entry.state,
            created_at: entry.created_at,
            terminal_at: entry.terminal_at,
            route: summary_metadata.route().map(safe_metadata).or(route),
            model: summary_metadata.model().map(safe_metadata).or(model),
            provider: summary_metadata.provider().map(safe_metadata).or(provider),
            engine: summary_metadata.engine().map(safe_metadata).or(engine),
            status_code,
            source: "active",
        }
    }

    pub(super) fn request_id(&self) -> &str {
        &self.request_id
    }

    pub(super) fn created_at(&self) -> &str {
        &self.created_at
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EventDto {
    event_id: String,
    request_id: String,
    occurred_at: String,
    kind: &'static str,
    model: Option<String>,
    provider: Option<String>,
    engine: Option<String>,
    attempt_id: Option<String>,
    status_code: Option<u16>,
    duration_ms: Option<u64>,
    tokens: Option<u64>,
}

impl TryFrom<EventRecord> for EventDto {
    type Error = LogsError;

    fn try_from(record: EventRecord) -> Result<Self, Self::Error> {
        let envelope = CanonicalEnvelope::from_json_str(&record.payload_json)
            .map_err(|_| LogsError::StoreUnavailable)?;
        if envelope.event_id.as_uuid().to_string() != record.event_id
            || envelope.request_id.as_uuid().to_string() != record.request_id
            || envelope.occurred_at != record.occurred_at
        {
            return Err(LogsError::StoreUnavailable);
        }
        let mut dto = Self {
            event_id: record.event_id,
            request_id: record.request_id,
            occurred_at: record.occurred_at,
            kind: event_kind(&envelope.event),
            model: None,
            provider: None,
            engine: None,
            attempt_id: None,
            status_code: None,
            duration_ms: None,
            tokens: None,
        };
        match envelope.event {
            LifecycleEvent::Admitted { model, .. } | LifecycleEvent::StreamStarted { model } => {
                dto.model = model.as_deref().map(safe_metadata);
            }
            LifecycleEvent::RouteSelected {
                model,
                provider,
                engine,
            } => {
                dto.model = model.as_deref().map(safe_metadata);
                dto.provider = provider.as_deref().map(safe_metadata);
                dto.engine = engine.as_deref().map(safe_metadata);
            }
            LifecycleEvent::AttemptStarted { attempt_id }
            | LifecycleEvent::AttemptFailed { attempt_id, .. } => {
                dto.attempt_id = attempt_id.map(|id| id.as_uuid().to_string());
            }
            LifecycleEvent::AttemptCompleted {
                attempt_id,
                status_code,
            } => {
                dto.attempt_id = attempt_id.map(|id| id.as_uuid().to_string());
                dto.status_code = status_code;
            }
            LifecycleEvent::StreamChunk { tokens } | LifecycleEvent::StreamCompleted { tokens } => {
                dto.tokens = tokens;
            }
            LifecycleEvent::Completed {
                status_code,
                duration_ms,
            } => {
                dto.status_code = status_code;
                dto.duration_ms = duration_ms;
            }
            LifecycleEvent::StreamError { .. }
            | LifecycleEvent::AuditError { .. }
            | LifecycleEvent::Failed { .. }
            | LifecycleEvent::Rejected { .. }
            | LifecycleEvent::Cancelled { .. }
            | LifecycleEvent::Dropped { .. } => {}
        }
        Ok(dto)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ArtifactDto {
    artifact_id: String,
    request_id: String,
    occurred_at: String,
    kind: String,
    media_kind: Option<String>,
    checksum: Option<String>,
    bytes: i64,
    version: i32,
    redacted: bool,
    truncated: bool,
    content_state: &'static str,
    content_base64: Option<String>,
}

impl ArtifactDto {
    pub(super) fn metadata(record: ArtifactRecord) -> Self {
        Self::from_parts(record, None)
    }

    pub(super) fn content(record: ArtifactRecord, content: ArtifactContent) -> Self {
        Self::from_parts(
            record,
            Some(base64::engine::general_purpose::STANDARD.encode(content.bytes)),
        )
    }

    /// Whether this read returned redacted artifact bytes. The route audit
    /// records only this outcome classification, never this DTO or its body.
    pub(super) fn has_available_content(&self) -> bool {
        self.content_state == "available"
    }

    fn from_parts(record: ArtifactRecord, content_base64: Option<String>) -> Self {
        let content_state = artifact_state(&record);
        Self {
            artifact_id: record.artifact_id,
            request_id: record.request_id,
            occurred_at: record.occurred_at,
            kind: safe_metadata(&record.kind),
            media_kind: record.media_kind.as_deref().map(safe_metadata),
            checksum: record.checksum,
            bytes: record.bytes,
            version: record.version,
            redacted: record.redacted,
            truncated: record.truncated,
            content_state,
            content_base64,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProxyDto {
    attempt_id: String,
    request_id: String,
    occurred_at: String,
    target: String,
    provider: Option<String>,
    engine: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
    status_code: Option<i64>,
}

impl From<ProxyRecord> for ProxyDto {
    fn from(record: ProxyRecord) -> Self {
        Self {
            attempt_id: record.attempt_id,
            request_id: record.request_id,
            occurred_at: record.occurred_at,
            target: safe_target(&record.target),
            provider: record.provider.as_deref().map(safe_metadata),
            engine: record.engine.as_deref().map(safe_metadata),
            started_at: record.started_at,
            completed_at: record.completed_at,
            status_code: record.status_code,
        }
    }
}

pub(super) fn artifact_state(record: &ArtifactRecord) -> &'static str {
    if record.corrupt {
        "corrupt"
    } else if record.missing || (record.checksum.is_none() && record.bytes == 0) {
        "missing"
    } else if record.redacted {
        "available"
    } else {
        "unavailable"
    }
}

fn event_kind(event: &LifecycleEvent) -> &'static str {
    match event {
        LifecycleEvent::Admitted { .. } => "admitted",
        LifecycleEvent::RouteSelected { .. } => "route_selected",
        LifecycleEvent::AttemptStarted { .. } => "attempt_started",
        LifecycleEvent::AttemptCompleted { .. } => "attempt_completed",
        LifecycleEvent::AttemptFailed { .. } => "attempt_failed",
        LifecycleEvent::StreamStarted { .. } => "stream_started",
        LifecycleEvent::StreamChunk { .. } => "stream_chunk",
        LifecycleEvent::StreamCompleted { .. } => "stream_completed",
        LifecycleEvent::StreamError { .. } => "stream_error",
        LifecycleEvent::AuditError { .. } => "audit_error",
        LifecycleEvent::Completed { .. } => "completed",
        LifecycleEvent::Failed { .. } => "failed",
        LifecycleEvent::Rejected { .. } => "rejected",
        LifecycleEvent::Cancelled { .. } => "cancelled",
        LifecycleEvent::Dropped { .. } => "dropped",
    }
}

fn safe_target(target: &str) -> String {
    let Ok(url) = url::Url::parse(target) else {
        return "opaque".to_string();
    };
    let Some(host) = url.host_str() else {
        return "opaque".to_string();
    };
    match url.port() {
        Some(port) => format!("{}://{host}:{port}", url.scheme()),
        None => format!("{}://{host}", url.scheme()),
    }
}

fn safe_metadata(value: &str) -> String {
    let trimmed = value.trim();
    let path_shaped = trimmed.starts_with('/')
        || trimmed.starts_with("~/")
        || trimmed.as_bytes().get(1) == Some(&b':')
        || trimmed.contains('\\');
    if path_shaped || trimmed.contains('?') || trimmed.contains("://") {
        return "[REDACTED]".to_string();
    }
    crate::logging::policy::apply_redaction(trimmed).0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_and_proxy_dtos_do_not_leak_paths_or_credentials() {
        let dto = RequestDto::durable(RequestRecord {
            request_id: uuid::Uuid::new_v4().to_string(),
            outcome: "completed".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
            terminal_at: None,
            route: Some("/private/secret".into()),
            model: Some("Bearer secret-token".into()),
            provider: None,
            engine: None,
            status_code: None,
        });
        let json = serde_json::to_string(&dto).expect("serialize dto");
        assert!(!json.contains("secret-token"));
        assert!(!json.contains("/private/secret"));

        let proxy = ProxyDto::from(ProxyRecord {
            attempt_id: uuid::Uuid::new_v4().to_string(),
            request_id: uuid::Uuid::new_v4().to_string(),
            occurred_at: "2026-01-01T00:00:00Z".into(),
            target: "https://user:password@example.test/path?token=secret".into(),
            provider: None,
            engine: None,
            started_at: None,
            completed_at: None,
            status_code: None,
        });
        let json = serde_json::to_string(&proxy).expect("serialize proxy");
        assert!(json.contains("https://example.test"));
        assert!(!json.contains("password"));
        assert!(!json.contains("token=secret"));
    }
}
