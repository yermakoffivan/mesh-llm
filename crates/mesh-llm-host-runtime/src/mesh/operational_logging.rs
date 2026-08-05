//! Bounded, metadata-only operational audit vocabulary for mesh boundaries.
//!
//! These events deliberately accept no peer, endpoint, address, token, ALPN,
//! or error values. The service receives only the static level and code below.

#[cfg(test)]
use crate::logging::LoggingService;

const OPERATIONAL_AUDIT_INFO: &str = "info";
const OPERATIONAL_AUDIT_WARNING: &str = "warning";

/// Static outcomes that are safe to publish through the local operational log.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MeshOperationalEvent {
    QuicAlpnAccepted,
    QuicInboundFailed,
    ControlAlpnRejected,
    ControlConnectionAccepted,
    ControlConnectionFailed,
    GossipPeerPromoted,
    GossipPeerRejectedPolicy,
    GossipPeerRejectedVersion,
    GossipPeerRemoved,
    DiscoveryJoinSucceeded,
    DiscoveryJoinFailed,
}

impl MeshOperationalEvent {
    const fn level(self) -> &'static str {
        match self {
            Self::QuicAlpnAccepted
            | Self::ControlConnectionAccepted
            | Self::GossipPeerPromoted
            | Self::GossipPeerRemoved
            | Self::DiscoveryJoinSucceeded => OPERATIONAL_AUDIT_INFO,
            Self::QuicInboundFailed
            | Self::ControlAlpnRejected
            | Self::ControlConnectionFailed
            | Self::GossipPeerRejectedPolicy
            | Self::GossipPeerRejectedVersion
            | Self::DiscoveryJoinFailed => OPERATIONAL_AUDIT_WARNING,
        }
    }

    const fn code(self) -> &'static str {
        match self {
            Self::QuicAlpnAccepted => "mesh_quic_alpn_accepted",
            Self::QuicInboundFailed => "mesh_quic_inbound_failed",
            Self::ControlAlpnRejected => "mesh_control_alpn_rejected",
            Self::ControlConnectionAccepted => "mesh_control_connection_accepted",
            Self::ControlConnectionFailed => "mesh_control_connection_failed",
            Self::GossipPeerPromoted => "mesh_gossip_peer_promoted",
            Self::GossipPeerRejectedPolicy => "mesh_gossip_peer_rejected_policy",
            Self::GossipPeerRejectedVersion => "mesh_gossip_peer_rejected_version",
            Self::GossipPeerRemoved => "mesh_gossip_peer_removed",
            Self::DiscoveryJoinSucceeded => "mesh_discovery_join_succeeded",
            Self::DiscoveryJoinFailed => "mesh_discovery_join_failed",
        }
    }
}

/// Record one mesh boundary result through the process-local logging service.
/// Logging is optional and this intentionally never affects mesh serving.
pub(crate) fn record_mesh_operational_event(event: MeshOperationalEvent) {
    let Some(state) = crate::logging_runtime_state() else {
        return;
    };
    let _ = state.write_operational_audit(event.level(), event.code());
}

#[cfg(test)]
fn record_mesh_operational_event_with_service(
    service: &LoggingService,
    event: MeshOperationalEvent,
) {
    let _ = service.write_operational_audit(event.level(), event.code());
}

#[cfg(test)]
mod tests {
    use super::{MeshOperationalEvent, record_mesh_operational_event_with_service};
    use crate::logging::{LoggingService, ServiceConfig};

    fn recorded_audits(service: &LoggingService) -> Vec<serde_json::Value> {
        service
            .bus_ref()
            .drain()
            .into_iter()
            .map(|entry| serde_json::from_str(&entry.payload).expect("audit payload"))
            .collect()
    }

    #[test]
    fn mesh_boundary_outcomes_emit_exact_static_audits_without_raw_metadata() {
        let service = LoggingService::new_disabled(ServiceConfig::default());
        let events = [
            MeshOperationalEvent::QuicAlpnAccepted,
            MeshOperationalEvent::ControlAlpnRejected,
            MeshOperationalEvent::ControlConnectionFailed,
            MeshOperationalEvent::GossipPeerPromoted,
            MeshOperationalEvent::GossipPeerRejectedPolicy,
            MeshOperationalEvent::DiscoveryJoinFailed,
        ];

        for event in events {
            record_mesh_operational_event_with_service(&service, event);
        }

        let audits = recorded_audits(&service);
        assert_eq!(
            audits,
            vec![
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "mesh_quic_alpn_accepted",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "warning",
                    "message": "mesh_control_alpn_rejected",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "warning",
                    "message": "mesh_control_connection_failed",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "mesh_gossip_peer_promoted",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "warning",
                    "message": "mesh_gossip_peer_rejected_policy",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "warning",
                    "message": "mesh_discovery_join_failed",
                }),
            ]
        );

        let serialized = serde_json::to_string(&audits).expect("serialized audit payloads");
        for raw_value in [
            "node=untrusted-lab-host",
            "peer=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "token=mesh-secret-bootstrap-token",
        ] {
            assert!(
                !serialized.contains(raw_value),
                "raw mesh metadata must not enter the audit payload"
            );
        }
    }

    #[test]
    fn mesh_operational_vocabulary_is_bounded_and_identifier_free() {
        let events = [
            MeshOperationalEvent::QuicAlpnAccepted,
            MeshOperationalEvent::QuicInboundFailed,
            MeshOperationalEvent::ControlAlpnRejected,
            MeshOperationalEvent::ControlConnectionAccepted,
            MeshOperationalEvent::ControlConnectionFailed,
            MeshOperationalEvent::GossipPeerPromoted,
            MeshOperationalEvent::GossipPeerRejectedPolicy,
            MeshOperationalEvent::GossipPeerRejectedVersion,
            MeshOperationalEvent::GossipPeerRemoved,
            MeshOperationalEvent::DiscoveryJoinSucceeded,
            MeshOperationalEvent::DiscoveryJoinFailed,
        ];

        for event in events {
            let code = event.code();
            assert!(code.len() <= 48, "audit code must stay bounded: {code}");
            assert!(
                code.bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
                "audit code must be a static identifier: {code}"
            );
            assert!(matches!(event.level(), "info" | "warning"));
        }
    }
}
