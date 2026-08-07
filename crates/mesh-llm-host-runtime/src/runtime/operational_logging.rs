//! Bounded, metadata-only operational audit vocabulary for runtime, model,
//! configuration, discovery, and local-serving boundaries.
//!
//! These events deliberately accept no model reference, configuration value,
//! mesh identity, invite token, path, endpoint, secret, signal, error, or
//! process metadata. The logging service receives only the static level and
//! code below.

#[cfg(test)]
use crate::logging::LoggingService;
use crate::logging::{OperationalAuditRecord, OperationalAuditSeverity};
use mesh_llm_config::{ConfigDiagnostic, ConfigDiagnosticSeverity};

const OPERATIONAL_AUDIT_INFO: &str = "info";
const OPERATIONAL_AUDIT_WARNING: &str = "warning";

const OPERATIONAL_AUDIT_SOURCE: &str = "runtime";

fn operational_audit_record(code: &'static str, level: &'static str) -> OperationalAuditRecord {
    let severity = match level {
        OPERATIONAL_AUDIT_INFO => OperationalAuditSeverity::Info,
        OPERATIONAL_AUDIT_WARNING => OperationalAuditSeverity::Warning,
        _ => OperationalAuditSeverity::Error,
    };
    OperationalAuditRecord::builder(OPERATIONAL_AUDIT_SOURCE, code)
        .severity(severity)
        .build()
}

/// Static runtime and model lifecycle outcomes that are safe to publish locally.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeOperationalEvent {
    StartupStarted,
    StartupFailed,
    Ready,
    ShutdownStarted,
    ModelLoadStarted,
    ModelReady,
    ModelLoadFailed,
    ModelUnloaded,
}

/// Static native Skippy runtime transitions. These deliberately identify the
/// embedded native layer rather than re-emitting the host model lifecycle.
/// They carry no model reference, native detail, path, endpoint, or error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeSkippyOperationalEvent {
    RuntimeStartupStarted,
    RuntimeReady,
    RuntimeStartupFailed,
    RuntimeShutdownStarted,
    ModelOpenStarted,
    ModelOpenFinished,
    ModelOpenFailed,
}

impl NativeSkippyOperationalEvent {
    const fn level(self) -> &'static str {
        match self {
            Self::RuntimeStartupFailed | Self::ModelOpenFailed => OPERATIONAL_AUDIT_WARNING,
            Self::RuntimeStartupStarted
            | Self::RuntimeReady
            | Self::RuntimeShutdownStarted
            | Self::ModelOpenStarted
            | Self::ModelOpenFinished => OPERATIONAL_AUDIT_INFO,
        }
    }

    const fn code(self) -> &'static str {
        match self {
            Self::RuntimeStartupStarted => "skippy_native_runtime_startup_started",
            Self::RuntimeReady => "skippy_native_runtime_ready",
            Self::RuntimeStartupFailed => "skippy_native_runtime_startup_failed",
            Self::RuntimeShutdownStarted => "skippy_native_runtime_shutdown_started",
            Self::ModelOpenStarted => "skippy_native_model_open_started",
            Self::ModelOpenFinished => "skippy_native_model_open_finished",
            Self::ModelOpenFailed => "skippy_native_model_open_failed",
        }
    }
}

impl RuntimeOperationalEvent {
    const fn level(self) -> &'static str {
        match self {
            Self::StartupStarted
            | Self::Ready
            | Self::ShutdownStarted
            | Self::ModelLoadStarted
            | Self::ModelReady
            | Self::ModelUnloaded => OPERATIONAL_AUDIT_INFO,
            Self::StartupFailed | Self::ModelLoadFailed => OPERATIONAL_AUDIT_WARNING,
        }
    }

    const fn code(self) -> &'static str {
        match self {
            Self::StartupStarted => "runtime_startup_started",
            Self::StartupFailed => "runtime_startup_failed",
            Self::Ready => "runtime_ready",
            Self::ShutdownStarted => "runtime_shutdown_started",
            Self::ModelLoadStarted => "runtime_model_load_started",
            Self::ModelReady => "runtime_model_ready",
            Self::ModelLoadFailed => "runtime_model_load_failed",
            Self::ModelUnloaded => "runtime_model_unloaded",
        }
    }
}

/// Sanitized aggregate of configuration diagnostics. Only severity is
/// considered; diagnostic text, paths, sources, and codes never leave the
/// configuration boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConfigDiagnosticsOutcome {
    Clean,
    Info,
    Warning,
    Error,
}

impl ConfigDiagnosticsOutcome {
    pub(crate) fn from_diagnostics(diagnostics: &[ConfigDiagnostic]) -> Self {
        if diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity == ConfigDiagnosticSeverity::Error)
        {
            return Self::Error;
        }
        if diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity == ConfigDiagnosticSeverity::Warning)
        {
            return Self::Warning;
        }
        if diagnostics.is_empty() {
            Self::Clean
        } else {
            Self::Info
        }
    }

    const fn level(self) -> &'static str {
        match self {
            Self::Clean | Self::Info => OPERATIONAL_AUDIT_INFO,
            Self::Warning | Self::Error => OPERATIONAL_AUDIT_WARNING,
        }
    }

    const fn code(self) -> &'static str {
        match self {
            Self::Clean => "runtime_config_diagnostics_clean",
            Self::Info => "runtime_config_diagnostics_info",
            Self::Warning => "runtime_config_diagnostics_warning",
            Self::Error => "runtime_config_diagnostics_error",
        }
    }
}

/// Static configuration apply outcomes that are safe to publish locally.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConfigOperationalEvent {
    ApplyStarted,
    ApplyAccepted,
    ApplyRejected,
    Diagnostics(ConfigDiagnosticsOutcome),
}

impl ConfigOperationalEvent {
    const fn level(self) -> &'static str {
        match self {
            Self::ApplyStarted | Self::ApplyAccepted => OPERATIONAL_AUDIT_INFO,
            Self::ApplyRejected => OPERATIONAL_AUDIT_WARNING,
            Self::Diagnostics(outcome) => outcome.level(),
        }
    }

    const fn code(self) -> &'static str {
        match self {
            Self::ApplyStarted => "runtime_config_apply_started",
            Self::ApplyAccepted => "runtime_config_apply_accepted",
            Self::ApplyRejected => "runtime_config_apply_rejected",
            Self::Diagnostics(outcome) => outcome.code(),
        }
    }
}

/// Static discovery decisions and join outcomes that are safe to publish
/// locally. They deliberately do not distinguish discovery sources, meshes,
/// tokens, peers, or errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DiscoveryOperationalEvent {
    DecisionJoin,
    DecisionStartNew,
    JoinStarted,
    JoinSucceeded,
    JoinFailed,
    DiscoveryFailed,
}

impl DiscoveryOperationalEvent {
    const fn level(self) -> &'static str {
        match self {
            Self::DecisionJoin
            | Self::DecisionStartNew
            | Self::JoinStarted
            | Self::JoinSucceeded => OPERATIONAL_AUDIT_INFO,
            Self::JoinFailed | Self::DiscoveryFailed => OPERATIONAL_AUDIT_WARNING,
        }
    }

    const fn code(self) -> &'static str {
        match self {
            Self::DecisionJoin => "runtime_discovery_decision_join",
            Self::DecisionStartNew => "runtime_discovery_decision_start_new",
            Self::JoinStarted => "runtime_discovery_join_started",
            Self::JoinSucceeded => "runtime_discovery_join_succeeded",
            Self::JoinFailed => "runtime_discovery_join_failed",
            Self::DiscoveryFailed => "runtime_discovery_failed",
        }
    }
}

/// Static local-serving state transitions that are safe to publish locally.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocalServingOperationalEvent {
    TargetAdded,
    TargetRemoved,
    Ready,
    Unavailable,
}

impl LocalServingOperationalEvent {
    const fn level(self) -> &'static str {
        OPERATIONAL_AUDIT_INFO
    }

    const fn code(self) -> &'static str {
        match self {
            Self::TargetAdded => "runtime_local_target_added",
            Self::TargetRemoved => "runtime_local_target_removed",
            Self::Ready => "runtime_local_serving_ready",
            Self::Unavailable => "runtime_local_serving_unavailable",
        }
    }
}

/// Record one runtime lifecycle result through the process-local logging state.
/// Logging is optional and intentionally never affects startup, readiness, or
/// shutdown progress.
pub(crate) fn record_runtime_operational_event(event: RuntimeOperationalEvent) {
    let Some(state) = crate::logging_runtime_state() else {
        return;
    };
    let _ = state.write_operational_audit(operational_audit_record(event.code(), event.level()));
}

/// Record a native Skippy lifecycle transition through the same bounded,
/// fail-open operational audit seam as other runtime boundaries.
pub(crate) fn record_native_skippy_operational_event(event: NativeSkippyOperationalEvent) {
    let Some(state) = crate::logging_runtime_state() else {
        return;
    };
    let _ = state.write_operational_audit(operational_audit_record(event.code(), event.level()));
}

/// Record one configuration boundary result through the process-local logging
/// state. Logging is optional and intentionally never affects config apply
/// behavior.
pub(crate) fn record_config_operational_event(event: ConfigOperationalEvent) {
    let Some(state) = crate::logging_runtime_state() else {
        return;
    };
    let _ = state.write_operational_audit(operational_audit_record(event.code(), event.level()));
}

/// Record one discovery boundary result through the process-local logging
/// state. Logging is optional and intentionally never affects discovery or
/// joining behavior.
pub(crate) fn record_discovery_operational_event(event: DiscoveryOperationalEvent) {
    let Some(state) = crate::logging_runtime_state() else {
        return;
    };
    let _ = state.write_operational_audit(operational_audit_record(event.code(), event.level()));
}

/// Record one local-serving state transition through the process-local logging
/// state. Logging is optional and intentionally never affects routing or
/// readiness behavior.
pub(crate) fn record_local_serving_operational_event(event: LocalServingOperationalEvent) {
    let Some(state) = crate::logging_runtime_state() else {
        return;
    };
    let _ = state.write_operational_audit(operational_audit_record(event.code(), event.level()));
}

#[cfg(test)]
fn record_runtime_operational_event_with_service(
    service: &LoggingService,
    event: RuntimeOperationalEvent,
) {
    let _ = service.write_operational_audit(operational_audit_record(event.code(), event.level()));
}

#[cfg(test)]
fn record_native_skippy_operational_event_with_service(
    service: &LoggingService,
    event: NativeSkippyOperationalEvent,
) {
    let _ = service.write_operational_audit(operational_audit_record(event.code(), event.level()));
}

#[cfg(test)]
fn record_config_operational_event_with_service(
    service: &LoggingService,
    event: ConfigOperationalEvent,
) {
    let _ = service.write_operational_audit(operational_audit_record(event.code(), event.level()));
}

#[cfg(test)]
fn record_discovery_operational_event_with_service(
    service: &LoggingService,
    event: DiscoveryOperationalEvent,
) {
    let _ = service.write_operational_audit(operational_audit_record(event.code(), event.level()));
}

#[cfg(test)]
fn record_local_serving_operational_event_with_service(
    service: &LoggingService,
    event: LocalServingOperationalEvent,
) {
    let _ = service.write_operational_audit(operational_audit_record(event.code(), event.level()));
}

#[cfg(test)]
mod tests {
    use super::{
        ConfigDiagnosticsOutcome, ConfigOperationalEvent, DiscoveryOperationalEvent,
        LocalServingOperationalEvent, NativeSkippyOperationalEvent, RuntimeOperationalEvent,
        record_config_operational_event_with_service,
        record_discovery_operational_event_with_service,
        record_local_serving_operational_event_with_service,
        record_native_skippy_operational_event_with_service,
        record_runtime_operational_event_with_service,
    };
    use crate::logging::{LoggingService, ServiceConfig};
    use mesh_llm_config::{
        ConfigDiagnostic, ConfigDiagnosticCode, ConfigDiagnosticSeverity, ConfigDiagnosticSource,
    };

    fn recorded_audits(service: &LoggingService) -> Vec<serde_json::Value> {
        service
            .bus_ref()
            .drain()
            .into_iter()
            .map(|entry| {
                let audit: serde_json::Value =
                    serde_json::from_str(&entry.payload).expect("audit payload");
                serde_json::json!({
                    "kind": "audit",
                    "level": audit["severity"],
                    "message": audit["code"],
                })
            })
            .collect()
    }

    #[test]
    fn runtime_lifecycle_audits_are_ordered_and_static() {
        let service = LoggingService::new_disabled(ServiceConfig::default());
        let events = [
            RuntimeOperationalEvent::StartupStarted,
            RuntimeOperationalEvent::Ready,
            RuntimeOperationalEvent::ShutdownStarted,
        ];

        for event in events {
            record_runtime_operational_event_with_service(&service, event);
        }

        assert_eq!(
            recorded_audits(&service),
            vec![
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_startup_started",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_ready",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_shutdown_started",
                }),
            ]
        );
    }

    #[test]
    fn model_lifecycle_success_and_unload_audits_are_ordered_and_static() {
        let service = LoggingService::new_disabled(ServiceConfig::default());
        let events = [
            RuntimeOperationalEvent::ModelLoadStarted,
            RuntimeOperationalEvent::ModelReady,
            RuntimeOperationalEvent::ModelUnloaded,
        ];

        for event in events {
            record_runtime_operational_event_with_service(&service, event);
        }

        assert_eq!(
            recorded_audits(&service),
            vec![
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_model_load_started",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_model_ready",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_model_unloaded",
                }),
            ]
        );
    }

    #[test]
    fn native_skippy_lifecycle_audits_are_ordered_static_and_path_free() {
        let service = LoggingService::new_disabled(ServiceConfig::default());
        let events = [
            NativeSkippyOperationalEvent::RuntimeStartupStarted,
            NativeSkippyOperationalEvent::ModelOpenStarted,
            NativeSkippyOperationalEvent::ModelOpenFinished,
            NativeSkippyOperationalEvent::RuntimeReady,
            NativeSkippyOperationalEvent::RuntimeShutdownStarted,
        ];

        for event in events {
            record_native_skippy_operational_event_with_service(&service, event);
        }

        let audits = recorded_audits(&service);
        assert_eq!(
            audits,
            vec![
                serde_json::json!({ "kind": "audit", "level": "info", "message": "skippy_native_runtime_startup_started" }),
                serde_json::json!({ "kind": "audit", "level": "info", "message": "skippy_native_model_open_started" }),
                serde_json::json!({ "kind": "audit", "level": "info", "message": "skippy_native_model_open_finished" }),
                serde_json::json!({ "kind": "audit", "level": "info", "message": "skippy_native_runtime_ready" }),
                serde_json::json!({ "kind": "audit", "level": "info", "message": "skippy_native_runtime_shutdown_started" }),
            ]
        );

        let serialized = serde_json::to_string(&audits).expect("serialized native audits");
        for raw_value in [
            "/private/models/native-secret.gguf",
            "prompt=never-persist-this",
            "native detail: bearer private-token",
        ] {
            assert!(
                !serialized.contains(raw_value),
                "native operational audits must not include {raw_value}"
            );
        }
    }

    #[test]
    fn runtime_startup_failure_audit_excludes_runtime_metadata() {
        let service = LoggingService::new_disabled(ServiceConfig::default());
        record_runtime_operational_event_with_service(
            &service,
            RuntimeOperationalEvent::StartupFailed,
        );

        let audits = recorded_audits(&service);
        assert_eq!(
            audits,
            vec![serde_json::json!({
                "kind": "audit",
                "level": "warning",
                "message": "runtime_startup_failed",
            })]
        );

        let serialized = serde_json::to_string(&audits).expect("serialized audit payloads");
        for raw_value in [
            "/private/models/private-model.gguf",
            "model=private/model:secret",
            "SIGTERM pid=12345",
            "native load error: private detail",
        ] {
            assert!(
                !serialized.contains(raw_value),
                "runtime metadata must not enter the audit payload"
            );
        }
    }

    #[test]
    fn model_load_failure_audit_excludes_model_metadata() {
        let service = LoggingService::new_disabled(ServiceConfig::default());
        record_runtime_operational_event_with_service(
            &service,
            RuntimeOperationalEvent::ModelLoadFailed,
        );

        let audits = recorded_audits(&service);
        assert_eq!(
            audits,
            vec![serde_json::json!({
                "kind": "audit",
                "level": "warning",
                "message": "runtime_model_load_failed",
            })]
        );

        let serialized = serde_json::to_string(&audits).expect("serialized audit payloads");
        for raw_value in [
            "/private/models/private-model.gguf",
            "model=private/model:secret",
            "Private-Mistral-7B-Instruct-Q4_K_M",
            "runtime-12345",
            "SIGTERM pid=12345",
            "native load error: private detail",
        ] {
            assert!(
                !serialized.contains(raw_value),
                "model metadata must not enter the audit payload"
            );
        }
    }

    #[test]
    fn config_apply_outcomes_emit_ordered_static_audits_without_config_metadata() {
        let service = LoggingService::new_disabled(ServiceConfig::default());
        let diagnostic = ConfigDiagnostic::new(
            ConfigDiagnosticCode::InvalidValue,
            ConfigDiagnosticSeverity::Error,
            ConfigDiagnosticSource::Validation,
            "webhook.url=https://secret.example/hook?token=private-token",
        );
        let events = [
            ConfigOperationalEvent::ApplyStarted,
            ConfigOperationalEvent::Diagnostics(ConfigDiagnosticsOutcome::Clean),
            ConfigOperationalEvent::ApplyAccepted,
            ConfigOperationalEvent::ApplyStarted,
            ConfigOperationalEvent::Diagnostics(ConfigDiagnosticsOutcome::from_diagnostics(&[
                diagnostic,
            ])),
            ConfigOperationalEvent::ApplyRejected,
        ];

        for event in events {
            record_config_operational_event_with_service(&service, event);
        }

        let audits = recorded_audits(&service);
        assert_eq!(
            audits,
            vec![
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_config_apply_started",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_config_diagnostics_clean",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_config_apply_accepted",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_config_apply_started",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "warning",
                    "message": "runtime_config_diagnostics_error",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "warning",
                    "message": "runtime_config_apply_rejected",
                }),
            ]
        );

        let serialized = serde_json::to_string(&audits).expect("serialized audit payloads");
        for raw_value in [
            "webhook.url=https://secret.example/hook?token=private-token",
            "/private/mesh/config.toml",
            "Bearer private-token",
            "Private-Mistral-7B-Instruct-Q4_K_M",
            "invalid value: private detail",
        ] {
            assert!(
                !serialized.contains(raw_value),
                "config metadata must not enter the audit payload"
            );
        }
    }

    #[test]
    fn config_diagnostic_outcomes_are_severity_only() {
        let diagnostic = |severity| {
            ConfigDiagnostic::new(
                ConfigDiagnosticCode::InvalidValue,
                severity,
                ConfigDiagnosticSource::Validation,
                "private configuration detail",
            )
        };

        assert_eq!(
            ConfigDiagnosticsOutcome::from_diagnostics(&[]),
            ConfigDiagnosticsOutcome::Clean
        );
        assert_eq!(
            ConfigDiagnosticsOutcome::from_diagnostics(&[diagnostic(
                ConfigDiagnosticSeverity::Info
            )]),
            ConfigDiagnosticsOutcome::Info
        );
        assert_eq!(
            ConfigDiagnosticsOutcome::from_diagnostics(&[diagnostic(
                ConfigDiagnosticSeverity::Warning
            )]),
            ConfigDiagnosticsOutcome::Warning
        );
        assert_eq!(
            ConfigDiagnosticsOutcome::from_diagnostics(&[
                diagnostic(ConfigDiagnosticSeverity::Info),
                diagnostic(ConfigDiagnosticSeverity::Warning),
                diagnostic(ConfigDiagnosticSeverity::Error),
            ]),
            ConfigDiagnosticsOutcome::Error
        );
    }

    #[test]
    fn discovery_decisions_and_join_outcomes_are_ordered_and_metadata_free() {
        let service = LoggingService::new_disabled(ServiceConfig::default());
        let events = [
            DiscoveryOperationalEvent::DecisionJoin,
            DiscoveryOperationalEvent::JoinStarted,
            DiscoveryOperationalEvent::JoinSucceeded,
            DiscoveryOperationalEvent::DecisionJoin,
            DiscoveryOperationalEvent::JoinStarted,
            DiscoveryOperationalEvent::JoinFailed,
            DiscoveryOperationalEvent::DiscoveryFailed,
            DiscoveryOperationalEvent::DecisionStartNew,
        ];

        for event in events {
            record_discovery_operational_event_with_service(&service, event);
        }

        let audits = recorded_audits(&service);
        assert_eq!(
            audits,
            vec![
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_discovery_decision_join",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_discovery_join_started",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_discovery_join_succeeded",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_discovery_decision_join",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_discovery_join_started",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "warning",
                    "message": "runtime_discovery_join_failed",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "warning",
                    "message": "runtime_discovery_failed",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_discovery_decision_start_new",
                }),
            ]
        );

        let serialized = serde_json::to_string(&audits).expect("serialized audit payloads");
        for raw_value in [
            "private-lab-mesh",
            "mesh-secret-bootstrap-token",
            "wss://relay.private.example",
            "peer=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "join failed: private transport detail",
        ] {
            assert!(
                !serialized.contains(raw_value),
                "discovery metadata must not enter the audit payload"
            );
        }
    }

    #[test]
    fn local_serving_readiness_transitions_are_ordered_and_metadata_free() {
        let service = LoggingService::new_disabled(ServiceConfig::default());
        let events = [
            LocalServingOperationalEvent::TargetAdded,
            LocalServingOperationalEvent::Ready,
            LocalServingOperationalEvent::TargetRemoved,
            LocalServingOperationalEvent::Unavailable,
        ];

        for event in events {
            record_local_serving_operational_event_with_service(&service, event);
        }

        let audits = recorded_audits(&service);
        assert_eq!(
            audits,
            vec![
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_local_target_added",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_local_serving_ready",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_local_target_removed",
                }),
                serde_json::json!({
                    "kind": "audit",
                    "level": "info",
                    "message": "runtime_local_serving_unavailable",
                }),
            ]
        );

        let serialized = serde_json::to_string(&audits).expect("serialized audit payloads");
        for raw_value in [
            "Private-Mistral-7B-Instruct-Q4_K_M",
            "/private/models/private-model.gguf",
            "127.0.0.1:41731",
            "runtime-12345",
            "local serving error: private detail",
        ] {
            assert!(
                !serialized.contains(raw_value),
                "local-serving metadata must not enter the audit payload"
            );
        }
    }

    #[test]
    fn runtime_operational_vocabulary_is_bounded_and_identifier_only() {
        let events = [
            RuntimeOperationalEvent::StartupStarted,
            RuntimeOperationalEvent::StartupFailed,
            RuntimeOperationalEvent::Ready,
            RuntimeOperationalEvent::ShutdownStarted,
            RuntimeOperationalEvent::ModelLoadStarted,
            RuntimeOperationalEvent::ModelReady,
            RuntimeOperationalEvent::ModelLoadFailed,
            RuntimeOperationalEvent::ModelUnloaded,
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

    #[test]
    fn config_operational_vocabulary_is_bounded_and_identifier_only() {
        let events = [
            ConfigOperationalEvent::ApplyStarted,
            ConfigOperationalEvent::ApplyAccepted,
            ConfigOperationalEvent::ApplyRejected,
            ConfigOperationalEvent::Diagnostics(ConfigDiagnosticsOutcome::Clean),
            ConfigOperationalEvent::Diagnostics(ConfigDiagnosticsOutcome::Info),
            ConfigOperationalEvent::Diagnostics(ConfigDiagnosticsOutcome::Warning),
            ConfigOperationalEvent::Diagnostics(ConfigDiagnosticsOutcome::Error),
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

    #[test]
    fn discovery_and_local_serving_vocabularies_are_bounded_and_identifier_only() {
        let discovery_events = [
            DiscoveryOperationalEvent::DecisionJoin,
            DiscoveryOperationalEvent::DecisionStartNew,
            DiscoveryOperationalEvent::JoinStarted,
            DiscoveryOperationalEvent::JoinSucceeded,
            DiscoveryOperationalEvent::JoinFailed,
            DiscoveryOperationalEvent::DiscoveryFailed,
        ];
        let local_serving_events = [
            LocalServingOperationalEvent::TargetAdded,
            LocalServingOperationalEvent::TargetRemoved,
            LocalServingOperationalEvent::Ready,
            LocalServingOperationalEvent::Unavailable,
        ];

        for (level, code) in discovery_events
            .into_iter()
            .map(|event| (event.level(), event.code()))
            .chain(
                local_serving_events
                    .into_iter()
                    .map(|event| (event.level(), event.code())),
            )
        {
            assert!(code.len() <= 48, "audit code must stay bounded: {code}");
            assert!(
                code.bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
                "audit code must be a static identifier: {code}"
            );
            assert!(matches!(level, "info" | "warning"));
        }
    }
}
