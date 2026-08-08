#![forbid(unsafe_code)]

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::future::Future;
use std::io::{self, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, RwLock};
use tracing::debug;
use uuid::Uuid;

/// Audit log format
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, clap::ValueEnum)]
pub enum AuditLogFormat {
    #[default]
    Json,
    JsonLines,
}

/// Audit event severity level
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    PartialEq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    clap::ValueEnum,
)]
#[serde(rename_all = "snake_case")]
pub enum AuditLevel {
    #[default]
    Info,
    Warn,
    Error,
    Critical,
}

impl AuditLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuditLevel::Info => "info",
            AuditLevel::Warn => "warn",
            AuditLevel::Error => "error",
            AuditLevel::Critical => "critical",
        }
    }
}

/// Categories of audit events for filtering and routing
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditCategory {
    Authentication,
    Authorization,
    Configuration,
    MeshMembership,
    ModelAccess,
    AdminAction,
    System,
}

impl AuditCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuditCategory::Authentication => "authentication",
            AuditCategory::Authorization => "authorization",
            AuditCategory::Configuration => "configuration",
            AuditCategory::MeshMembership => "mesh_membership",
            AuditCategory::ModelAccess => "model_access",
            AuditCategory::AdminAction => "admin_action",
            AuditCategory::System => "system",
        }
    }
}

/// Outcome of an audited action
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Success,
    Failure,
    Denied,
    Error,
}

/// Structured audit event for security-relevant actions
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AuditEvent {
    /// Unique event identifier
    pub event_id: Uuid,
    /// Timestamp in RFC3339 format
    pub timestamp: DateTime<Utc>,
    /// Event category for filtering
    pub category: AuditCategory,
    /// Human-readable action description
    pub action: String,
    /// Resource being acted upon (model name, config path, peer ID, etc.)
    pub resource: Option<String>,
    /// Actor identity (owner ID, node ID, user, etc.)
    pub actor: Option<String>,
    /// Outcome of the action
    pub outcome: AuditOutcome,
    /// Severity level
    pub level: AuditLevel,
    /// Correlation ID for request tracing
    pub correlation_id: Option<Uuid>,
    /// Additional structured metadata
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Value>,
    /// Error details if outcome is Failure/Error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl AuditEvent {
    /// Create a new audit event with generated ID and current timestamp
    pub fn new(category: AuditCategory, action: impl Into<String>, outcome: AuditOutcome) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            category,
            action: action.into(),
            resource: None,
            actor: None,
            outcome,
            level: AuditLevel::Info,
            correlation_id: None,
            metadata: BTreeMap::new(),
            error: None,
        }
    }

    /// Set the resource being acted upon
    pub fn with_resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    /// Set the actor identity
    pub fn with_actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }

    /// Set the severity level
    pub fn with_level(mut self, level: AuditLevel) -> Self {
        self.level = level;
        self
    }

    /// Set correlation ID for request tracing
    pub fn with_correlation_id(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    /// Add metadata key-value pair
    pub fn with_metadata(mut self, key: impl Into<String>, value: Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Set error details
    pub fn with_error(mut self, error: impl Into<String>) -> Self {
        self.error = Some(error.into());
        self
    }

    /// Serialize to JSON string
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    /// Serialize to JSON Lines format (single line JSON)
    pub fn to_json_line(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }
}

/// Trait for audit log sinks - separate from OutputSink for TUI/logging
pub trait AuditSink: Send + Sync {
    /// Emit an audit event
    fn emit_audit(&self, event: &AuditEvent) -> io::Result<()>;

    /// Flush any buffered events
    fn flush(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>;

    /// Get the configured log format
    fn format(&self) -> AuditLogFormat {
        AuditLogFormat::JsonLines
    }

    /// Get the minimum level to log
    fn min_level(&self) -> AuditLevel {
        AuditLevel::Info
    }
}

type AuditSinkFuture<'a> = Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;

/// Global audit sink slot
static AUDIT_SINK: OnceLock<RwLock<Option<Arc<dyn AuditSink>>>> = OnceLock::new();

fn audit_sink_slot() -> &'static RwLock<Option<Arc<dyn AuditSink>>> {
    AUDIT_SINK.get_or_init(|| RwLock::new(None))
}

/// Set the global audit sink
pub fn set_audit_sink(sink: Arc<dyn AuditSink>) {
    if let Ok(mut slot) = audit_sink_slot().write() {
        *slot = Some(sink);
    }
}

/// Clear the global audit sink
pub fn clear_audit_sink() {
    if let Ok(mut slot) = audit_sink_slot().write() {
        *slot = None;
    }
}

/// Get the current audit sink
pub fn audit_sink() -> Option<Arc<dyn AuditSink>> {
    audit_sink_slot()
        .read()
        .ok()
        .and_then(|slot| slot.as_ref().cloned())
}

/// Emit an audit event to the global sink
pub fn emit_audit(event: AuditEvent) -> io::Result<()> {
    // Apply secret redaction before emission
    let redacted = redact_secrets(event);
    match audit_sink() {
        Some(sink) => {
            if redacted.level >= sink.min_level() {
                sink.emit_audit(&redacted)
            } else {
                Ok(())
            }
        }
        None => Ok(()),
    }
}

/// Flush the audit sink
pub async fn flush_audit() -> io::Result<()> {
    match audit_sink() {
        Some(sink) => sink.flush().await,
        None => Ok(()),
    }
}

/// Check if audit logging is enabled (sink configured)
pub fn audit_enabled() -> bool {
    audit_sink().is_some()
}

/// Secret patterns to redact from audit events
const SECRET_PATTERNS: &[&str] = &[
    "token",
    "password",
    "secret",
    "key",
    "credential",
    "auth",
    "bearer",
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
    "private_key",
    "certificate",
];

/// Redact sensitive fields from audit event metadata and error messages
fn redact_secrets(event: AuditEvent) -> AuditEvent {
    let mut redacted = event;

    // Redact metadata values
    for (key, value) in redacted.metadata.iter_mut() {
        let key_lower = key.to_lowercase();
        if SECRET_PATTERNS.iter().any(|p| key_lower.contains(p)) {
            *value = Value::String("[REDACTED]".to_string());
        } else if let Value::String(s) = value {
            // Check for bearer tokens in string values
            if s.starts_with("Bearer ") || s.starts_with("bearer ") {
                *value = Value::String("[REDACTED]".to_string());
            }
        }
    }

    // Redact error message if it contains secrets
    if let Some(error) = &redacted.error {
        let error_lower = error.to_lowercase();
        if SECRET_PATTERNS.iter().any(|p| error_lower.contains(p)) {
            redacted.error = Some("[REDACTED]".to_string());
        }
    }

    // Redact resource if it looks like a secret
    if let Some(resource) = &redacted.resource {
        let resource_lower = resource.to_lowercase();
        if SECRET_PATTERNS.iter().any(|p| resource_lower.contains(p)) {
            redacted.resource = Some("[REDACTED]".to_string());
        }
    }

    // Redact actor if it looks like a token
    if let Some(actor) = &redacted.actor {
        let actor_lower = actor.to_lowercase();
        if actor_lower.contains("token") || actor_lower.contains("key") || actor.len() > 64 {
            redacted.actor = Some("[REDACTED]".to_string());
        }
    }

    redacted
}

/// Configuration for file-based audit sink
#[derive(Clone, Debug)]
pub struct FileAuditSinkConfig {
    /// Path to audit log file
    pub path: PathBuf,
    /// Maximum file size before rotation (bytes)
    pub max_file_size: u64,
    /// Maximum number of rotated files to keep
    pub max_files: usize,
    /// Minimum audit level to log
    pub min_level: AuditLevel,
    /// Log format
    pub format: AuditLogFormat,
}

impl Default for FileAuditSinkConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("audit.log"),
            max_file_size: 100 * 1024 * 1024, // 100 MB
            max_files: 10,
            min_level: AuditLevel::Info,
            format: AuditLogFormat::JsonLines,
        }
    }
}

/// File-based audit sink with rotation
pub struct FileAuditSink {
    config: FileAuditSinkConfig,
    current_file: std::sync::Mutex<std::fs::File>,
    current_size: std::sync::Mutex<u64>,
}

impl FileAuditSink {
    /// Create a new file audit sink
    pub fn new(config: FileAuditSinkConfig) -> Result<Self> {
        // Ensure parent directory exists
        if let Some(parent) = config.path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Open or create the log file
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.path)?;

        let current_size = file.metadata()?.len();

        Ok(Self {
            config,
            current_file: std::sync::Mutex::new(file),
            current_size: std::sync::Mutex::new(current_size),
        })
    }

    /// Rotate log files if current file exceeds max size
    fn rotate_if_needed(&self) -> io::Result<()> {
        let size = self.current_size.lock().unwrap();
        if *size >= self.config.max_file_size {
            drop(size); // Release lock before rotation

            // Rotate files: audit.log.9 -> audit.log.10 (deleted), audit.log.8 -> audit.log.9, etc.
            for i in (1..self.config.max_files).rev() {
                let from = self.config.path.with_extension(format!("log.{}", i));
                let to = self.config.path.with_extension(format!("log.{}", i + 1));
                if from.exists() {
                    if i + 1 >= self.config.max_files {
                        let _ = std::fs::remove_file(&from);
                    } else {
                        let _ = std::fs::rename(&from, &to);
                    }
                }
            }

            // Rotate current file to .1
            let rotated = self.config.path.with_extension("log.1");
            let _ = std::fs::rename(&self.config.path, &rotated);

            // Create new file
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.config.path)?;

            *self.current_file.lock().unwrap() = file;
            *self.current_size.lock().unwrap() = 0;

            debug!("Rotated audit log file");
        }
        Ok(())
    }
}

impl AuditSink for FileAuditSink {
    fn emit_audit(&self, event: &AuditEvent) -> io::Result<()> {
        self.rotate_if_needed()?;

        let line = match self.config.format {
            AuditLogFormat::Json => event
                .to_json()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            AuditLogFormat::JsonLines => event
                .to_json_line()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        };

        let mut file = self.current_file.lock().unwrap();
        let mut size = self.current_size.lock().unwrap();

        writeln!(file, "{}", line)?;
        file.flush()?;

        *size += line.len() as u64 + 1; // +1 for newline

        Ok(())
    }

    fn flush(&self) -> AuditSinkFuture<'_> {
        Box::pin(async {
            let mut file = self.current_file.lock().unwrap();
            file.flush()
        })
    }

    fn format(&self) -> AuditLogFormat {
        self.config.format
    }

    fn min_level(&self) -> AuditLevel {
        self.config.min_level
    }
}

/// Helper to create audit events for common scenarios
pub mod audit_events {
    use super::*;

    /// Authentication attempt
    pub fn auth_attempt(actor: Option<String>, success: bool, method: &str) -> AuditEvent {
        AuditEvent::new(
            AuditCategory::Authentication,
            format!("auth_{}", if success { "success" } else { "failure" }),
            if success {
                AuditOutcome::Success
            } else {
                AuditOutcome::Failure
            },
        )
        .with_actor(actor.unwrap_or_else(|| "unknown".to_string()))
        .with_metadata("method".to_string(), Value::String(method.to_string()))
        .with_level(if success {
            AuditLevel::Info
        } else {
            AuditLevel::Warn
        })
    }

    /// Authorization check
    pub fn authz_check(
        actor: Option<String>,
        resource: &str,
        action: &str,
        allowed: bool,
    ) -> AuditEvent {
        AuditEvent::new(
            AuditCategory::Authorization,
            format!("authz_{}", if allowed { "allow" } else { "deny" }),
            if allowed {
                AuditOutcome::Success
            } else {
                AuditOutcome::Denied
            },
        )
        .with_actor(actor.unwrap_or_else(|| "unknown".to_string()))
        .with_resource(resource.to_string())
        .with_metadata("action".to_string(), Value::String(action.to_string()))
        .with_level(if allowed {
            AuditLevel::Info
        } else {
            AuditLevel::Warn
        })
    }

    /// Configuration change
    pub fn config_change(
        actor: Option<String>,
        config_path: &str,
        old_value: Option<Value>,
        new_value: Option<Value>,
    ) -> AuditEvent {
        let mut event = AuditEvent::new(
            AuditCategory::Configuration,
            "config_change".to_string(),
            AuditOutcome::Success,
        )
        .with_actor(actor.unwrap_or_else(|| "unknown".to_string()))
        .with_resource(config_path.to_string())
        .with_level(AuditLevel::Info);

        if let Some(v) = old_value {
            event = event.with_metadata("old_value".to_string(), v);
        }
        if let Some(v) = new_value {
            event = event.with_metadata("new_value".to_string(), v);
        }
        event
    }

    /// Mesh membership change (peer join/leave)
    pub fn mesh_membership(actor: Option<String>, peer_id: &str, joined: bool) -> AuditEvent {
        AuditEvent::new(
            AuditCategory::MeshMembership,
            if joined { "peer_joined" } else { "peer_left" },
            AuditOutcome::Success,
        )
        .with_actor(actor.unwrap_or_else(|| "system".to_string()))
        .with_resource(peer_id.to_string())
        .with_level(AuditLevel::Info)
    }

    /// Model access (load/unload/route)
    pub fn model_access(
        actor: Option<String>,
        model: &str,
        action: &str,
        success: bool,
    ) -> AuditEvent {
        AuditEvent::new(
            AuditCategory::ModelAccess,
            format!("model_{}", action),
            if success {
                AuditOutcome::Success
            } else {
                AuditOutcome::Failure
            },
        )
        .with_actor(actor.unwrap_or_else(|| "unknown".to_string()))
        .with_resource(model.to_string())
        .with_level(if success {
            AuditLevel::Info
        } else {
            AuditLevel::Warn
        })
    }

    /// Administrative action
    pub fn admin_action(
        actor: Option<String>,
        action: &str,
        resource: Option<&str>,
        success: bool,
        error: Option<String>,
    ) -> AuditEvent {
        let mut event = AuditEvent::new(
            AuditCategory::AdminAction,
            action.to_string(),
            if success {
                AuditOutcome::Success
            } else {
                AuditOutcome::Failure
            },
        )
        .with_actor(actor.unwrap_or_else(|| "unknown".to_string()))
        .with_level(if success {
            AuditLevel::Info
        } else {
            AuditLevel::Error
        });

        if let Some(r) = resource {
            event = event.with_resource(r.to_string());
        }
        if let Some(e) = error {
            event = event.with_error(e);
        }
        event
    }

    /// System event (startup, shutdown, error)
    pub fn system_event(action: &str, outcome: AuditOutcome, error: Option<String>) -> AuditEvent {
        let mut event = AuditEvent::new(AuditCategory::System, action.to_string(), outcome)
            .with_actor("system".to_string())
            .with_level(match outcome {
                AuditOutcome::Success => AuditLevel::Info,
                AuditOutcome::Failure | AuditOutcome::Error => AuditLevel::Error,
                AuditOutcome::Denied => AuditLevel::Warn,
            });

        if let Some(e) = error {
            event = event.with_error(e);
        }
        event
    }
}
