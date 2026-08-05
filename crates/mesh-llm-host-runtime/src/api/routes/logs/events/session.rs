use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use mesh_llm_events::logging::envelope::CanonicalEnvelope;
use mesh_llm_events::logging::replay::ReplayChannel;
use tokio::sync::mpsc;

use super::protocol::{GapData, error_frame, event_frame, gap_frame};
use super::query::{Cursor, Subscription};
use crate::logging::{ReplayBus, ReplayCursor, ReplayWindow, RequestSummaryEventSnapshots};

/// Build the deterministic initial replay in bus insertion order. The caller
/// supplies an opaque REST cursor created by the durable query layer; this
/// protocol never treats the in-memory window as recovery authority.
#[cfg(test)]
pub(in crate::api::routes::logs) fn replay_frames(
    bus: &ReplayBus,
    subscription: &Subscription,
    recovery_cursor: Option<String>,
) -> Vec<String> {
    replay_window_frames(bus.replay_window(), subscription, recovery_cursor)
}

/// Per-connection replay state. It advances over every selected-channel
/// record, including filtered or invalid records, so a live snapshot is never
/// re-emitted after an update notification.
pub(in crate::api::routes::logs) struct ReplaySession {
    subscription: Subscription,
    cursor: Cursor,
}

impl ReplaySession {
    pub(super) fn new(subscription: Subscription) -> Self {
        let cursor = subscription.cursor;
        Self {
            subscription,
            cursor,
        }
    }

    pub(super) fn next_frames(
        &mut self,
        bus: &ReplayBus,
        recovery_cursor: Option<String>,
    ) -> Vec<String> {
        let window = bus.replay_window();
        let subscription = Subscription {
            channels: self.subscription.channels.clone(),
            filters: self.subscription.filters.clone(),
            cursor: self.cursor,
        };
        bus.record_replay_gaps(replay_gap_count(&window, &subscription));
        let frames = replay_window_frames(window.clone(), &subscription, recovery_cursor);
        for record in window.records {
            if self.subscription.channels.contains(&record.replay.channel) {
                self.cursor
                    .advance(record.replay.channel, record.replay.sequence);
            }
        }
        frames
    }
}

fn replay_gap_count(window: &ReplayWindow, subscription: &Subscription) -> u64 {
    subscription
        .channels
        .iter()
        .filter(|channel| {
            subscription.cursor.sequence(**channel) < window.evicted_through.sequence(**channel)
        })
        .count() as u64
}

fn replay_window_frames(
    window: ReplayWindow,
    subscription: &Subscription,
    recovery_cursor: Option<String>,
) -> Vec<String> {
    let mut frames = gap_frames(&window, subscription, recovery_cursor);
    for record in window.records {
        if !subscription.channels.contains(&record.replay.channel)
            || record.replay.sequence <= subscription.cursor.sequence(record.replay.channel)
            || !matches_filter(&record, subscription)
        {
            continue;
        }
        match event_frame(&record) {
            Ok(frame) => frames.push(frame),
            Err(()) => frames.push(error_frame(cursor_from_replay(record.cursor))),
        }
    }
    frames
}

fn gap_frames(
    window: &ReplayWindow,
    subscription: &Subscription,
    recovery_cursor: Option<String>,
) -> Vec<String> {
    subscription
        .channels
        .iter()
        .filter_map(|channel| {
            let requested = subscription.cursor.sequence(*channel);
            let evicted_through = window.evicted_through.sequence(*channel);
            (requested < evicted_through).then(|| {
                let gap = GapData::new(
                    *channel,
                    requested.saturating_add(1),
                    evicted_through,
                    recovery_cursor.clone(),
                );
                gap_frame(cursor_from_replay(window.latest), &gap)
                    .expect("bounded replay-gap data fits the SSE frame cap")
            })
        })
        .collect()
}

fn matches_filter(record: &crate::logging::ReplayRecord, subscription: &Subscription) -> bool {
    let Ok(payload) = serde_json::from_str::<serde_json::Value>(&record.entry.payload) else {
        return false;
    };
    let Some(envelope) = payload
        .get("canonical_envelope")
        .and_then(|value| CanonicalEnvelope::from_json_str(&value.to_string()).ok())
    else {
        return false;
    };
    let summary_snapshots = payload
        .get("request_summary_snapshots")
        .cloned()
        .and_then(|value| serde_json::from_value::<RequestSummaryEventSnapshots>(value).ok());
    subscription
        .filters
        .matches(&envelope, summary_snapshots.as_ref())
}

fn cursor_from_replay(cursor: ReplayCursor) -> Cursor {
    Cursor::from_sequences(
        cursor.sequence(ReplayChannel::Requests),
        cursor.sequence(ReplayChannel::Operations),
        cursor.sequence(ReplayChannel::System),
    )
}

/// Bounded per-connection hand-off between a replay/live producer and the
/// socket writer. A full queue is a deliberate slow-consumer disconnect, not
/// an unbounded allocation or a blocked logging producer.
#[derive(Clone)]
pub(in crate::api::routes::logs) struct ConnectionQueue {
    sender: mpsc::Sender<String>,
    cancelled: Arc<AtomicBool>,
}

pub(super) struct ConnectionReceiver {
    receiver: mpsc::Receiver<String>,
    cancelled: Arc<AtomicBool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::api::routes::logs) enum QueueError {
    SlowConsumer,
    Cancelled,
}

impl ConnectionQueue {
    pub(super) fn new(capacity: usize) -> (Self, ConnectionReceiver) {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let cancelled = Arc::new(AtomicBool::new(false));
        (
            Self {
                sender,
                cancelled: Arc::clone(&cancelled),
            },
            ConnectionReceiver {
                receiver,
                cancelled,
            },
        )
    }

    #[cfg(test)]
    pub(super) fn try_send(&self, frame: String) -> Result<(), QueueError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(QueueError::Cancelled);
        }
        match self.sender.try_send(frame) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(QueueError::SlowConsumer),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(QueueError::Cancelled),
        }
    }

    pub(super) async fn send_with_timeout(
        &self,
        frame: String,
        timeout: std::time::Duration,
    ) -> Result<(), QueueError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(QueueError::Cancelled);
        }
        match tokio::time::timeout(timeout, self.sender.send(frame)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(QueueError::Cancelled),
            Err(_) => Err(QueueError::SlowConsumer),
        }
    }

    pub(super) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

impl ConnectionReceiver {
    pub(super) async fn recv(&mut self) -> Option<String> {
        if self.cancelled.load(Ordering::Acquire) {
            return None;
        }
        self.receiver.recv().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mesh_llm_events::logging::events::LifecycleEvent;
    use mesh_llm_events::logging::identifiers::{EventId, RequestId};
    use mesh_llm_events::logging::replay::ReplaySequence;

    use super::*;
    use crate::logging::{
        LoggingService, ReplayBus, RequestSummaryEntry, RequestSummaryEventSnapshots,
        RequestSummaryMetadata, TerminalOutcome,
    };

    fn summary(
        created_at: &str,
        state: &str,
        metadata: RequestSummaryMetadata,
    ) -> RequestSummaryEntry {
        RequestSummaryEntry {
            request_id: "test-request-summary".into(),
            state: state.into(),
            created_at: created_at.into(),
            terminal_at: None,
            metadata,
        }
    }

    fn current_snapshots(
        created_at: &str,
        state: &str,
        metadata: RequestSummaryMetadata,
    ) -> RequestSummaryEventSnapshots {
        let summary = summary(created_at, state, metadata);
        RequestSummaryEventSnapshots::current(&summary)
    }

    fn terminal_snapshots(
        created_at: &str,
        before_metadata: RequestSummaryMetadata,
        state: &str,
        after_metadata: RequestSummaryMetadata,
    ) -> RequestSummaryEventSnapshots {
        let before = summary(created_at, "active", before_metadata);
        let after = summary(created_at, state, after_metadata);
        RequestSummaryEventSnapshots::terminal(&before, &after)
    }

    fn entry(bus: &ReplayBus, channel: ReplayChannel, sequence: u64, request: RequestId) {
        let occurred_at = format!("2026-08-03T00:00:0{sequence}Z");
        entry_with_event(
            bus,
            channel,
            sequence,
            request,
            occurred_at,
            LifecycleEvent::Admitted {
                model: None,
                method: None,
            },
        );
    }

    fn entry_with_event(
        bus: &ReplayBus,
        channel: ReplayChannel,
        sequence: u64,
        request: RequestId,
        occurred_at: String,
        event: LifecycleEvent,
    ) {
        let summary_snapshots =
            current_snapshots(&occurred_at, "active", RequestSummaryMetadata::default());
        entry_with_event_and_snapshots(
            bus,
            channel,
            sequence,
            request,
            occurred_at,
            event,
            summary_snapshots,
        );
    }

    fn entry_with_event_and_snapshots(
        bus: &ReplayBus,
        channel: ReplayChannel,
        sequence: u64,
        request: RequestId,
        occurred_at: String,
        event: LifecycleEvent,
        summary_snapshots: RequestSummaryEventSnapshots,
    ) {
        let envelope = CanonicalEnvelope::new(
            EventId::new(),
            request,
            channel,
            sequence,
            occurred_at,
            event,
        );
        bus.push_replay(
            serde_json::json!({
                "canonical_envelope": envelope,
                "request_summary_snapshots": summary_snapshots,
                "not_public": "/private/operator/path?token=secret"
            })
            .to_string(),
            match channel {
                ReplayChannel::Requests => 0,
                ReplayChannel::Operations => 1,
                ReplayChannel::System => 2,
            },
            ReplaySequence::next(channel, sequence),
        );
    }

    fn subscription(channels: Vec<ReplayChannel>, cursor: Cursor) -> Subscription {
        Subscription {
            channels,
            filters: Default::default(),
            cursor,
        }
    }

    #[test]
    fn replay_is_ordered_monotonic_and_hides_raw_payload() {
        let bus = ReplayBus::new(4);
        entry(&bus, ReplayChannel::Requests, 1, RequestId::new());
        entry(&bus, ReplayChannel::Operations, 1, RequestId::new());
        entry(&bus, ReplayChannel::Requests, 2, RequestId::new());

        let frames = replay_frames(
            &bus,
            &subscription(
                vec![ReplayChannel::Requests, ReplayChannel::Operations],
                Cursor::default(),
            ),
            Some("durable-cursor".into()),
        );

        assert_eq!(frames.len(), 3);
        assert!(frames[0].contains("id: v1:1.0.0"));
        assert!(frames[1].contains("id: v1:1.1.0"));
        assert!(frames[2].contains("id: v1:2.1.0"));
        assert!(
            frames
                .iter()
                .all(|frame| !frame.contains("private/operator"))
        );
        assert!(frames.iter().all(|frame| !frame.contains("secret")));
        assert!(
            frames
                .iter()
                .all(|frame| !frame.contains("request_summary_snapshots"))
        );
    }

    #[test]
    fn last_event_id_and_explicit_cursor_deduplicate_replay() {
        let bus = ReplayBus::new(3);
        entry(&bus, ReplayChannel::Requests, 1, RequestId::new());
        entry(&bus, ReplayChannel::Requests, 2, RequestId::new());
        let raw = b"GET /api/logs/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\nLast-Event-ID: v1:1.0.0\r\n\r\n";
        let subscription = super::super::query::parse_subscription(
            "/api/logs/events?channel=requests&cursor=v1%3A0.0.0",
            raw,
        )
        .expect("reconnect subscription parses");

        let frames = replay_frames(&bus, &subscription, None);
        assert_eq!(frames.len(), 1);
        assert!(frames[0].contains("id: v1:2.0.0"));
    }

    #[test]
    fn evicted_cursor_emits_gap_with_rest_recovery_cursor() {
        let bus = ReplayBus::new(1);
        entry(&bus, ReplayChannel::Requests, 1, RequestId::new());
        entry(&bus, ReplayChannel::Requests, 2, RequestId::new());

        let frames = replay_frames(
            &bus,
            &subscription(vec![ReplayChannel::Requests], Cursor::default()),
            Some("opaque-rest-cursor".into()),
        );
        assert!(frames[0].contains("event: replay_gap"));
        assert!(frames[0].contains("opaque-rest-cursor"));
        assert!(frames[1].contains("id: v1:2.0.0"));
    }

    #[test]
    fn request_filter_selects_only_the_requested_lifecycle() {
        let bus = ReplayBus::new(3);
        let wanted = RequestId::new();
        entry(&bus, ReplayChannel::Requests, 1, wanted);
        entry(&bus, ReplayChannel::Requests, 2, RequestId::new());
        let mut subscription = subscription(vec![ReplayChannel::Requests], Cursor::default());
        subscription
            .filters
            .request_ids
            .insert(wanted.as_uuid().to_string());

        let frames = replay_frames(&bus, &subscription, None);
        assert_eq!(frames.len(), 1);
        assert!(frames[0].contains(&wanted.as_uuid().to_string()));
    }

    #[test]
    fn metadata_filters_match_summary_snapshots_not_event_fields() {
        let bus = ReplayBus::new(4);
        let wanted = RequestId::new();
        entry_with_event_and_snapshots(
            &bus,
            ReplayChannel::Requests,
            1,
            wanted,
            "2026-08-03T00:00:01Z".into(),
            LifecycleEvent::RouteSelected {
                model: Some("event-model-must-not-match".into()),
                provider: Some("event-provider-must-not-match".into()),
                engine: Some("event-engine-must-not-match".into()),
            },
            current_snapshots(
                "2026-08-03T00:00:01Z",
                "active",
                RequestSummaryMetadata::from_parts(
                    Some("chat_completions"),
                    Some("Qwen/Qwen3"),
                    Some("mesh"),
                    Some("skippy"),
                ),
            ),
        );
        entry_with_event_and_snapshots(
            &bus,
            ReplayChannel::Requests,
            2,
            RequestId::new(),
            "2026-08-03T00:00:02Z".into(),
            LifecycleEvent::RouteSelected {
                model: Some("Qwen/Qwen2.5".into()),
                provider: Some("mesh".into()),
                engine: Some("skippy".into()),
            },
            current_snapshots(
                "2026-08-03T00:00:02Z",
                "active",
                RequestSummaryMetadata::from_parts(
                    Some("completions"),
                    Some("Qwen/Qwen2.5"),
                    Some("mesh"),
                    Some("skippy"),
                ),
            ),
        );
        let subscription = super::super::query::parse_subscription(
            "/api/logs/events?channel=requests&route=chat_completions&filter=model%3AQwen%2FQwen3&provider=mesh&engine=skippy&from=2026-08-03T00%3A00%3A00Z&to=2026-08-03T00%3A00%3A01Z",
            b"GET /api/logs/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n",
        )
        .expect("ledger subscription parses");

        let frames = replay_frames(&bus, &subscription, None);
        assert_eq!(frames.len(), 1);
        assert!(frames[0].contains(&wanted.as_uuid().to_string()));
    }

    #[test]
    fn terminal_after_to_matches_created_within_range() {
        let bus = ReplayBus::new(3);
        let wanted = RequestId::new();
        entry_with_event_and_snapshots(
            &bus,
            ReplayChannel::Requests,
            1,
            wanted,
            "2026-08-03T00:00:05Z".into(),
            LifecycleEvent::Completed {
                status_code: Some(200),
                duration_ms: Some(4),
            },
            terminal_snapshots(
                "2026-08-03T00:00:01Z",
                RequestSummaryMetadata::default(),
                "completed",
                RequestSummaryMetadata::default(),
            ),
        );
        let subscription = super::super::query::parse_subscription(
            "/api/logs/events?channel=requests&from=2026-08-03T00%3A00%3A00Z&to=2026-08-03T00%3A00%3A02Z&outcome=completed",
            b"GET /api/logs/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n",
        )
        .expect("outcome subscription parses");

        let frames = replay_frames(&bus, &subscription, None);
        assert_eq!(frames.len(), 1);
        assert!(frames[0].contains("id: v1:1.0.0"));
    }

    #[test]
    fn event_inside_range_does_not_match_request_created_outside_range() {
        let bus = ReplayBus::new(3);
        entry_with_event_and_snapshots(
            &bus,
            ReplayChannel::Requests,
            1,
            RequestId::new(),
            "2026-08-03T00:00:01Z".into(),
            LifecycleEvent::RouteSelected {
                model: None,
                provider: None,
                engine: None,
            },
            current_snapshots(
                "2026-08-03T00:00:05Z",
                "active",
                RequestSummaryMetadata::default(),
            ),
        );
        let subscription = super::super::query::parse_subscription(
            "/api/logs/events?channel=requests&from=2026-08-03T00%3A00%3A00Z&to=2026-08-03T00%3A00%3A02Z",
            b"GET /api/logs/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n",
        )
        .expect("time subscription parses");

        assert!(replay_frames(&bus, &subscription, None).is_empty());
    }

    #[test]
    fn terminal_completion_notifies_active_and_completed_memberships() {
        let bus = ReplayBus::new(3);
        let wanted = RequestId::new();
        entry_with_event_and_snapshots(
            &bus,
            ReplayChannel::Requests,
            1,
            wanted,
            "2026-08-03T00:00:02Z".into(),
            LifecycleEvent::Completed {
                status_code: Some(200),
                duration_ms: Some(4),
            },
            terminal_snapshots(
                "2026-08-03T00:00:01Z",
                RequestSummaryMetadata::default(),
                "completed",
                RequestSummaryMetadata::default(),
            ),
        );

        for outcome in ["active", "completed"] {
            let subscription = super::super::query::parse_subscription(
                &format!("/api/logs/events?channel=requests&outcome={outcome}"),
                b"GET /api/logs/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n",
            )
            .expect("outcome subscription parses");
            let frames = replay_frames(&bus, &subscription, None);
            assert_eq!(frames.len(), 1, "{outcome} sees the terminal transition");
            assert!(frames[0].contains(&wanted.as_uuid().to_string()));
        }
    }

    #[test]
    fn failed_terminal_does_not_match_completed_membership() {
        let bus = ReplayBus::new(3);
        entry_with_event_and_snapshots(
            &bus,
            ReplayChannel::Requests,
            1,
            RequestId::new(),
            "2026-08-03T00:00:02Z".into(),
            LifecycleEvent::Failed {
                error: "bounded_failure".into(),
            },
            terminal_snapshots(
                "2026-08-03T00:00:01Z",
                RequestSummaryMetadata::default(),
                "failed",
                RequestSummaryMetadata::default(),
            ),
        );
        let completed = super::super::query::parse_subscription(
            "/api/logs/events?channel=requests&outcome=completed",
            b"GET /api/logs/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n",
        )
        .expect("completed subscription parses");
        let active = super::super::query::parse_subscription(
            "/api/logs/events?channel=requests&outcome=active",
            b"GET /api/logs/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n",
        )
        .expect("active subscription parses");

        assert!(replay_frames(&bus, &completed, None).is_empty());
        assert_eq!(replay_frames(&bus, &active, None).len(), 1);
    }

    #[test]
    fn terminal_replay_uses_enriched_summary_metadata_for_filters() {
        let service = Arc::new(LoggingService::new_disabled(Default::default()));
        let known = RequestId::new();
        let (known_guard, _) = service.register_request_with_metadata(
            known,
            RequestSummaryMetadata::from_parts(Some("chat_completions"), None, None, None),
        );
        service.merge_request_metadata(
            known,
            RequestSummaryMetadata::from_parts(
                None,
                Some("acme/model"),
                Some("mesh"),
                Some("raw_ingress"),
            ),
        );
        service
            .transition_terminal(known, &known_guard, TerminalOutcome::Completed)
            .expect("known request terminalizes");

        let absent = RequestId::new();
        let (absent_guard, _) = service.register_request(absent);
        service
            .transition_terminal(absent, &absent_guard, TerminalOutcome::Completed)
            .expect("metadata-absent request terminalizes");

        let subscription = super::super::query::parse_subscription(
            "/api/logs/events?channel=requests&route=chat_completions&model=acme%2Fmodel&provider=mesh&engine=raw_ingress&outcome=completed",
            b"GET /api/logs/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n",
        )
        .expect("metadata subscription parses");
        let bus = service.bus_ref();
        let frames = replay_frames(bus.as_ref(), &subscription, None);

        assert_eq!(frames.len(), 1);
        assert!(frames[0].contains(&known.as_uuid().to_string()));
        assert!(!frames[0].contains(&absent.as_uuid().to_string()));

        let active_subscription = super::super::query::parse_subscription(
            "/api/logs/events?channel=requests&route=chat_completions&model=acme%2Fmodel&provider=mesh&engine=raw_ingress&outcome=active",
            b"GET /api/logs/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n",
        )
        .expect("active metadata subscription parses");
        let active_frames = replay_frames(bus.as_ref(), &active_subscription, None);

        assert_eq!(active_frames.len(), 1);
        assert!(active_frames[0].contains(&known.as_uuid().to_string()));
        assert!(!active_frames[0].contains(&absent.as_uuid().to_string()));
    }

    #[test]
    fn heartbeat_is_an_sse_comment() {
        assert_eq!(super::super::heartbeat_frame(), ": keepalive\n\n");
    }

    #[tokio::test]
    async fn queue_bounds_slow_consumers_and_cancellation() {
        let (queue, mut receiver) = ConnectionQueue::new(1);
        queue.try_send("first".into()).expect("first fits");
        assert_eq!(
            queue.try_send("second".into()),
            Err(QueueError::SlowConsumer)
        );
        assert_eq!(receiver.recv().await.as_deref(), Some("first"));
        queue.cancel();
        assert_eq!(
            queue.try_send("after-cancel".into()),
            Err(QueueError::Cancelled)
        );
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn queue_times_out_a_slow_socket_writer_without_unbounded_growth() {
        let (queue, _receiver) = ConnectionQueue::new(1);
        queue.try_send("first".into()).unwrap();
        assert_eq!(
            queue
                .send_with_timeout("second".into(), std::time::Duration::from_millis(5))
                .await,
            Err(QueueError::SlowConsumer)
        );
    }
}
