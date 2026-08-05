use mesh_llm_events::logging::envelope::CanonicalEnvelope;
use mesh_llm_events::logging::events::LifecycleEvent;
use mesh_llm_events::logging::replay::ReplayChannel;
use serde::Serialize;

use super::query::Cursor;
use crate::logging::ReplayRecord;

pub(in crate::api::routes::logs) const MAX_FRAME_BYTES: usize = 16 * 1024;

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum ChannelName {
    Requests,
    Operations,
    System,
}

impl From<ReplayChannel> for ChannelName {
    fn from(channel: ReplayChannel) -> Self {
        match channel {
            ReplayChannel::Requests => Self::Requests,
            ReplayChannel::Operations => Self::Operations,
            ReplayChannel::System => Self::System,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PublicEvent {
    event_id: String,
    request_id: String,
    occurred_at: String,
    channel: ChannelName,
    sequence: u64,
    kind: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GapData {
    channel: ChannelName,
    from_sequence: u64,
    to_sequence: u64,
    recovery: RestRecovery,
}

impl GapData {
    pub(super) fn new(
        channel: ReplayChannel,
        from_sequence: u64,
        to_sequence: u64,
        recovery_cursor: Option<String>,
    ) -> Self {
        Self {
            channel: channel.into(),
            from_sequence,
            to_sequence,
            recovery: RestRecovery {
                endpoint: "/api/logs/requests",
                cursor: recovery_cursor,
            },
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RestRecovery {
    endpoint: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
}

/// Render one bounded privacy-safe lifecycle event. The bus's raw serialized
/// payload is never sent; only canonical identifiers, sequencing, timestamp,
/// and an exhaustive event-kind label cross the SSE boundary.
pub(super) fn event_frame(record: &ReplayRecord) -> Result<String, ()> {
    let envelope = envelope(record)?;
    let data = PublicEvent {
        event_id: envelope.event_id.as_uuid().to_string(),
        request_id: envelope.request_id.as_uuid().to_string(),
        occurred_at: envelope.occurred_at.clone(),
        channel: record.replay.channel.into(),
        sequence: record.replay.sequence,
        kind: event_kind(&envelope.event),
    };
    frame("log_event", &cursor_id(record.cursor), &data)
}

pub(super) fn gap_frame(cursor: Cursor, gap: &GapData) -> Result<String, ()> {
    frame("replay_gap", &cursor.event_id(), gap)
}

pub(super) fn error_frame(cursor: Cursor) -> String {
    frame(
        "stream_error",
        &cursor.event_id(),
        &serde_json::json!({"code":"invalid_event"}),
    )
    .expect("fixed stream error frame fits the SSE bound")
}

pub(in crate::api::routes::logs) fn heartbeat_frame() -> &'static str {
    ": keepalive\n\n"
}

fn envelope(record: &ReplayRecord) -> Result<CanonicalEnvelope, ()> {
    let parsed: serde_json::Value = serde_json::from_str(&record.entry.payload).map_err(|_| ())?;
    let envelope = parsed
        .get("canonical_envelope")
        .ok_or(())
        .and_then(|value| CanonicalEnvelope::from_json_str(&value.to_string()).map_err(|_| ()))?;
    if envelope.channel != record.replay.channel || envelope.sequence != record.replay.sequence {
        return Err(());
    }
    Ok(envelope)
}

fn cursor_id(cursor: crate::logging::ReplayCursor) -> String {
    Cursor::from_sequences(
        cursor.sequence(ReplayChannel::Requests),
        cursor.sequence(ReplayChannel::Operations),
        cursor.sequence(ReplayChannel::System),
    )
    .event_id()
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

fn frame<T: Serialize>(event: &str, id: &str, data: &T) -> Result<String, ()> {
    let data = serde_json::to_string(data).map_err(|_| ())?;
    let frame = format!("event: {event}\nid: {id}\ndata: {data}\n\n");
    (frame.len() <= MAX_FRAME_BYTES).then_some(frame).ok_or(())
}
