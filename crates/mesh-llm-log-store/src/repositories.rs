//! Typed repositories for log-store persistence operations.

use crate::cursor::{decode_cursor, encode_cursor};
use crate::error::LogStoreError;
use crate::store::LogStore;
use rusqlite::{OptionalExtension, Row, Transaction};
use std::collections::BTreeMap;

/// A single retention pass never deletes more than this many terminal
/// summaries. Further passes resume from the same deterministic oldest-first
/// ordering, keeping SQLite work and post-commit artifact file cleanup bounded.
const MAX_SUMMARIES_PER_CAP_PRUNE: i64 = 1_000;
const MAX_WEBHOOK_ATTEMPTS: u32 = 20;
const MAX_WEBHOOK_IDENTIFIER_BYTES: usize = 128;
const MAX_WEBHOOK_TIMESTAMP_BYTES: usize = 64;
const CONFIGURED_WEBHOOK_TARGET: &str = "configured_webhook";

// ─── Row types returned by queries ──────────────────────

#[derive(Debug, Clone)]
pub struct SummaryRow {
    pub request_id: String,
    pub state: String,
    pub created_at: String,
    #[allow(dead_code)]
    pub terminal_at: Option<String>,
    #[allow(dead_code)]
    pub route: Option<String>,
    #[allow(dead_code)]
    pub model: Option<String>,
    #[allow(dead_code)]
    pub provider: Option<String>,
    #[allow(dead_code)]
    pub engine: Option<String>,
    #[allow(dead_code)]
    pub status_code: Option<i64>,
    #[allow(dead_code)]
    pub error_msg: Option<String>,
    #[allow(dead_code)]
    pub tenant_id: Option<String>,
    #[allow(dead_code)]
    pub account_id: Option<String>,
    #[allow(dead_code)]
    pub user_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LifecycleEventRow {
    pub event_id: String,
    pub request_id: String,
    pub occurred_at: String,
}

/// Durable, privacy-safe state for one scoped terminal webhook delivery.
///
/// No endpoint, payload, response body, credential, or raw transport error is
/// retained here. The worker resolves the explicitly configured endpoint only
/// at send time and records a bounded error code after the attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookDeliveryState {
    Pending,
    InFlight,
    Succeeded,
    Retry,
    DeadLetter,
    ManualRetry,
}

impl WebhookDeliveryState {
    const fn code(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InFlight => "in_flight",
            Self::Succeeded => "succeeded",
            Self::Retry => "retry",
            Self::DeadLetter => "dead_letter",
            Self::ManualRetry => "manual_retry",
        }
    }

    fn parse(value: &str) -> Result<Self, LogStoreError> {
        match value {
            "pending" => Ok(Self::Pending),
            "in_flight" => Ok(Self::InFlight),
            "succeeded" => Ok(Self::Succeeded),
            "retry" => Ok(Self::Retry),
            "dead_letter" => Ok(Self::DeadLetter),
            "manual_retry" => Ok(Self::ManualRetry),
            _ => Err(LogStoreError::QueryFailed(
                "webhook delivery state is invalid".to_string(),
            )),
        }
    }
}

/// Sanitized, bounded classification of a failed webhook attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookDeliveryErrorCode {
    Timeout,
    Transport,
    Http4xx,
    Http5xx,
    Configuration,
}

impl WebhookDeliveryErrorCode {
    const fn code(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::Http4xx => "http_4xx",
            Self::Http5xx => "http_5xx",
            Self::Configuration => "configuration",
        }
    }

    fn parse(value: Option<String>) -> Result<Option<Self>, LogStoreError> {
        match value.as_deref() {
            None => Ok(None),
            Some("timeout") => Ok(Some(Self::Timeout)),
            Some("transport") => Ok(Some(Self::Transport)),
            Some("http_4xx") => Ok(Some(Self::Http4xx)),
            Some("http_5xx") => Ok(Some(Self::Http5xx)),
            Some("configuration") => Ok(Some(Self::Configuration)),
            Some(_) => Err(LogStoreError::QueryFailed(
                "webhook delivery error code is invalid".to_string(),
            )),
        }
    }
}

/// Persisted record returned to the asynchronous webhook worker. The claim
/// generation is a fencing value: only the worker that atomically incremented
/// it can complete or retry that in-flight attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookDeliveryRecord {
    pub delivery_id: String,
    pub request_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub state: WebhookDeliveryState,
    pub attempt_number: u32,
    pub max_attempts: u32,
    pub next_attempt_at: Option<String>,
    pub lease_expires_at: Option<String>,
    pub claim_generation: u64,
    pub status_code: Option<u16>,
    pub last_error_code: Option<WebhookDeliveryErrorCode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookDeliveryInsertOutcome {
    Created(WebhookDeliveryRecord),
    Existing(WebhookDeliveryRecord),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookRetryOutcome {
    RetryScheduled,
    DeadLettered,
}

/// A file artifact whose durable pointer was removed by cascade cleanup.
///
/// Keeping this ownership tuple in the transaction result means post-commit
/// file cleanup never has to rediscover a path by artifact filename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CascadeArtifactPointer {
    pub artifact_id: String,
    pub request_id: String,
}

/// Durable tables governed by the logging retention policy.  These stable,
/// path-free names are suitable for cleanup receipts and local health views.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RetentionTable {
    Summaries,
    LifecycleEvents,
    ArtifactPointers,
    ProxyRecords,
    AuditEntries,
    WebhookDeliveries,
    CleanupRuns,
}

impl RetentionTable {
    pub const ALL: [Self; 7] = [
        Self::Summaries,
        Self::LifecycleEvents,
        Self::ArtifactPointers,
        Self::ProxyRecords,
        Self::AuditEntries,
        Self::WebhookDeliveries,
        Self::CleanupRuns,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Summaries => "summaries",
            Self::LifecycleEvents => "lifecycle_events",
            Self::ArtifactPointers => "artifact_pointers",
            Self::ProxyRecords => "proxy_records",
            Self::AuditEntries => "audit_entries",
            Self::WebhookDeliveries => "webhook_deliveries",
            Self::CleanupRuns => "cleanup_runs",
        }
    }
}

/// Bounded retention settings for one durable table.  A cutoff is used rather
/// than a duration so the store remains deterministic under an injected clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionTablePolicy {
    pub cutoff_occurred_at: String,
    pub max_rows: u64,
}

/// Explicit policy map for every durable logging table.  The constructor
/// rejects missing tables, so adding a table cannot silently make it
/// unbounded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPolicy {
    table_policies: BTreeMap<RetentionTable, RetentionTablePolicy>,
    webhook_dead_letter_cutoff_at: Option<String>,
}

impl RetentionPolicy {
    pub fn new(
        table_policies: BTreeMap<RetentionTable, RetentionTablePolicy>,
    ) -> Result<Self, LogStoreError> {
        for table in RetentionTable::ALL {
            let Some(policy) = table_policies.get(&table) else {
                return Err(LogStoreError::QueryFailed(format!(
                    "logging retention policy is missing {}",
                    table.label()
                )));
            };
            if policy.max_rows == 0 {
                return Err(LogStoreError::QueryFailed(format!(
                    "logging retention max rows for {} must be at least one",
                    table.label()
                )));
            }
        }
        Ok(Self {
            table_policies,
            webhook_dead_letter_cutoff_at: None,
        })
    }

    /// Compatibility policy for the existing global config.  It deliberately
    /// expands that config into an explicit, complete map rather than allowing
    /// standalone audit, webhook, or cleanup tables to become unbounded.
    pub fn uniform(
        cutoff_occurred_at: impl Into<String>,
        max_rows: u64,
    ) -> Result<Self, LogStoreError> {
        let cutoff_occurred_at = cutoff_occurred_at.into();
        let table_policies = RetentionTable::ALL
            .into_iter()
            .map(|table| {
                (
                    table,
                    RetentionTablePolicy {
                        cutoff_occurred_at: cutoff_occurred_at.clone(),
                        max_rows,
                    },
                )
            })
            .collect();
        Self::new(table_policies)
    }

    pub fn table(&self, table: RetentionTable) -> &RetentionTablePolicy {
        // `new` proves complete coverage and this is private-state immutable.
        &self.table_policies[&table]
    }

    /// Add a dead-letter-only cutoff to generic webhook retention.
    ///
    /// `updated_at` is written by every transition into `dead_letter`, so it
    /// is the durable dead-letter transition timestamp. All other delivery
    /// states are unaffected by this additional cutoff.
    pub fn with_webhook_dead_letter_cutoff(mut self, cutoff_updated_at: impl Into<String>) -> Self {
        self.webhook_dead_letter_cutoff_at = Some(cutoff_updated_at.into());
        self
    }

    fn webhook_dead_letter_cutoff_at(&self) -> Option<&str> {
        self.webhook_dead_letter_cutoff_at.as_deref()
    }
}

/// Per-table deletion counts from one committed policy pass.  Names are
/// schema labels only; no filesystem location or artifact path is exposed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionTableResult {
    pub table: RetentionTable,
    pub ttl_deleted_count: i64,
    pub max_rows_deleted_count: i64,
}

/// The committed outcome of one bounded retention pass.
///
/// Artifact pointers are returned only when their owning terminal summary was
/// selected and deleted in the same SQLite transaction.  Callers must delete
/// those files after this transaction commits; a timestamp on an artifact
/// alone is never authority to remove its file while its request still exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionCleanupResult {
    pub ttl_deleted_count: i64,
    pub max_rows_deleted_count: i64,
    pub artifact_pointers: Vec<CascadeArtifactPointer>,
    pub table_results: Vec<RetentionTableResult>,
}

/// Paginated query result with an optional cursor for the next page.
#[derive(Debug)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

/// Static SQL shape for a descending opaque `(occurred_at, id)` keyset page.
///
/// The table and column names are private constants supplied by repository
/// methods, while the cursor values are always bound parameters.
struct OpaqueKeysetPage {
    table: &'static str,
    columns: &'static str,
    timestamp_column: &'static str,
    id_column: &'static str,
}

fn list_opaque_keyset_page<T>(
    connection: &rusqlite::Connection,
    page: OpaqueKeysetPage,
    limit: usize,
    after_cursor: Option<&str>,
    map: impl Fn(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    cursor_fields: impl Fn(&T) -> (&str, &str),
) -> Result<Page<T>, LogStoreError> {
    let (cursor_predicate, values) = if let Some(cursor) = after_cursor {
        let (timestamp, id) = decode_cursor(cursor)?;
        (
            format!(
                " WHERE ({}, {}) < (?, ?)",
                page.timestamp_column, page.id_column
            ),
            vec![timestamp, id],
        )
    } else {
        (String::new(), Vec::new())
    };
    let sql = format!(
        "SELECT {} FROM {}{} ORDER BY {} DESC, {} DESC LIMIT {}",
        page.columns, page.table, cursor_predicate, page.timestamp_column, page.id_column, limit,
    );
    let mut statement = connection.prepare(&sql).map_err(LogStoreError::Sqlite)?;
    let items = statement
        .query_map(rusqlite::params_from_iter(values.iter()), map)
        .map_err(LogStoreError::Sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| LogStoreError::QueryFailed(error.to_string()))?;
    let Some(last) = items.last() else {
        return Ok(Page {
            items,
            next_cursor: None,
        });
    };
    let (timestamp, id) = cursor_fields(last);
    let probe_sql = format!(
        "SELECT EXISTS(SELECT 1 FROM {} WHERE ({}, {}) < (?, ?) LIMIT 1)",
        page.table, page.timestamp_column, page.id_column,
    );
    let has_more = connection
        .query_row(&probe_sql, rusqlite::params![timestamp, id], |row| {
            row.get::<_, i32>(0)
        })
        .map(|value| value != 0)
        .map_err(LogStoreError::Sqlite)?;
    let next_cursor = has_more.then(|| encode_cursor(timestamp, id));

    Ok(Page { items, next_cursor })
}

// ─── Internal helpers ──────────────

pub(crate) fn is_unique_constraint_error(e: &rusqlite::Error) -> bool {
    if let rusqlite::Error::SqliteFailure(err, _) = e {
        err.code == rusqlite::ErrorCode::ConstraintViolation
    } else {
        false
    }
}

fn validate_webhook_identifier(value: &str, field: &'static str) -> Result<(), LogStoreError> {
    if value.is_empty() || value.len() > MAX_WEBHOOK_IDENTIFIER_BYTES {
        return Err(LogStoreError::InvalidQuery(format!(
            "webhook {field} must be between 1 and {MAX_WEBHOOK_IDENTIFIER_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_webhook_timestamp(value: &str, field: &'static str) -> Result<(), LogStoreError> {
    if value.is_empty() || value.len() > MAX_WEBHOOK_TIMESTAMP_BYTES {
        return Err(LogStoreError::InvalidQuery(format!(
            "webhook {field} must be between 1 and {MAX_WEBHOOK_TIMESTAMP_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_webhook_max_attempts(max_attempts: u32) -> Result<(), LogStoreError> {
    if !(1..=MAX_WEBHOOK_ATTEMPTS).contains(&max_attempts) {
        return Err(LogStoreError::InvalidQuery(format!(
            "webhook max_attempts must be between 1 and {MAX_WEBHOOK_ATTEMPTS}"
        )));
    }
    Ok(())
}

fn webhook_record_from_row(row: &Row<'_>) -> rusqlite::Result<WebhookDeliveryRecord> {
    let state: String = row.get("state")?;
    let last_error_code: Option<String> = row.get("last_error_code")?;
    let status_code: Option<i64> = row.get("status_code")?;
    let attempt_number: i64 = row.get("attempt_number")?;
    let max_attempts: i64 = row.get("max_attempts")?;
    let claim_generation: i64 = row.get("claim_generation")?;
    let state = WebhookDeliveryState::parse(&state).map_err(to_sqlite_conversion_error)?;
    let last_error_code =
        WebhookDeliveryErrorCode::parse(last_error_code).map_err(to_sqlite_conversion_error)?;
    let status_code = status_code
        .map(|status_code| {
            u16::try_from(status_code).map_err(|_| {
                to_sqlite_conversion_error(LogStoreError::QueryFailed(
                    "webhook status code is invalid".to_string(),
                ))
            })
        })
        .transpose()?;
    let attempt_number = u32::try_from(attempt_number).map_err(|_| {
        to_sqlite_conversion_error(LogStoreError::QueryFailed(
            "webhook attempt number is invalid".to_string(),
        ))
    })?;
    let max_attempts = u32::try_from(max_attempts).map_err(|_| {
        to_sqlite_conversion_error(LogStoreError::QueryFailed(
            "webhook max attempts is invalid".to_string(),
        ))
    })?;
    let claim_generation = u64::try_from(claim_generation).map_err(|_| {
        to_sqlite_conversion_error(LogStoreError::QueryFailed(
            "webhook claim generation is invalid".to_string(),
        ))
    })?;

    Ok(WebhookDeliveryRecord {
        delivery_id: row.get("delivery_id")?,
        request_id: row.get("request_id")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
        state,
        attempt_number,
        max_attempts,
        next_attempt_at: row.get("next_attempt_at")?,
        lease_expires_at: row.get("lease_expires_at")?,
        claim_generation,
        status_code,
        last_error_code,
    })
}

fn to_sqlite_conversion_error(error: LogStoreError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn select_webhook_delivery(
    connection: &rusqlite::Connection,
    delivery_id: &str,
) -> Result<Option<WebhookDeliveryRecord>, LogStoreError> {
    connection
        .query_row(
            "SELECT delivery_id, request_id, created_at, updated_at, state, attempt_number, \
                max_attempts, next_attempt_at, lease_expires_at, claim_generation, status_code, last_error_code \
             FROM webhook_deliveries WHERE delivery_id = ?",
            [delivery_id],
            webhook_record_from_row,
        )
        .optional()
        .map_err(|error| LogStoreError::QueryFailed(error.to_string()))
}

/// Closed event classification stored alongside the serialized payload.
///
/// The payload remains the compatibility surface for event readers, but
/// terminal semantics are deliberately derived once and stored as typed data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleEventType {
    Admitted,
    RouteSelected,
    AttemptStarted,
    AttemptCompleted,
    AttemptFailed,
    StreamStarted,
    StreamChunk,
    StreamCompleted,
    StreamError,
    AuditError,
    Completed,
    Failed,
    Rejected,
    Cancelled,
    Dropped,
    Unknown,
}

impl LifecycleEventType {
    fn from_payload(payload_json: &str) -> Self {
        serde_json::from_str::<serde_json::Value>(payload_json)
            .ok()
            .and_then(|value| {
                value
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .map(Self::from_code)
            })
            .unwrap_or(Self::Unknown)
    }

    fn from_code(code: &str) -> Self {
        match code {
            "admitted" => Self::Admitted,
            "route_selected" => Self::RouteSelected,
            "attempt_started" => Self::AttemptStarted,
            "attempt_completed" => Self::AttemptCompleted,
            "attempt_failed" => Self::AttemptFailed,
            "stream_started" => Self::StreamStarted,
            "stream_chunk" => Self::StreamChunk,
            "stream_completed" => Self::StreamCompleted,
            "stream_error" => Self::StreamError,
            "audit_error" => Self::AuditError,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "rejected" => Self::Rejected,
            "cancelled" => Self::Cancelled,
            "dropped" => Self::Dropped,
            _ => Self::Unknown,
        }
    }

    const fn code(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::RouteSelected => "route_selected",
            Self::AttemptStarted => "attempt_started",
            Self::AttemptCompleted => "attempt_completed",
            Self::AttemptFailed => "attempt_failed",
            Self::StreamStarted => "stream_started",
            Self::StreamChunk => "stream_chunk",
            Self::StreamCompleted => "stream_completed",
            Self::StreamError => "stream_error",
            Self::AuditError => "audit_error",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Rejected => "rejected",
            Self::Cancelled => "cancelled",
            Self::Dropped => "dropped",
            Self::Unknown => "unknown",
        }
    }

    const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Rejected | Self::Cancelled | Self::Dropped
        )
    }

    fn terminal_from_status(status: &str) -> Option<Self> {
        let event_type = Self::from_code(status);
        event_type.is_terminal().then_some(event_type)
    }
}

/// Check if a request already has any typed terminal event.
fn check_existing_terminal(
    cxn: &rusqlite::Connection,
    request_id: &str,
) -> Result<bool, LogStoreError> {
    let count: i64 = cxn
        .query_row(
            "SELECT COUNT(*) FROM lifecycle_events WHERE request_id = ? AND is_terminal = 1",
            rusqlite::params![request_id],
            |row| row.get(0),
        )
        .map_err(LogStoreError::Sqlite)?;
    Ok(count > 0)
}

// ─── LogStore repository methods ──────────────

impl LogStore {
    // ════════════════════════════
    //  Summaries
    // ════════════════════════════

    /// Insert a new summary. Returns AlreadyExists on duplicate PK.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_summary(
        &self,
        request_id: &str,
        model: Option<&str>,
        route: Option<&str>,
        provider: Option<&str>,
        engine: Option<&str>,
        occurred_at: &str,
        tenant_id: Option<&str>,
        account_id: Option<&str>,
        user_id: Option<&str>,
    ) -> Result<(), LogStoreError> {
        let conn = self.conn();
        match conn.execute(
            "INSERT INTO summaries (request_id, created_at, model, route, provider, engine, tenant_id, account_id, user_id) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![request_id, occurred_at, model, route, provider, engine, tenant_id, account_id, user_id],
        ) {
            Ok(_) => Ok(()),
            Err(ref e) if is_unique_constraint_error(e) => Err(LogStoreError::AlreadyExists {
                entity: format!("summary {}", request_id),
            }),
            Err(e) => Err(LogStoreError::InsertFailed(e.to_string())),
        }
    }

    /// Insert a summary when absent and otherwise fill only metadata fields
    /// that have not yet been recorded. Lifecycle state, timestamps, and
    /// identity fields remain untouched.
    #[allow(clippy::too_many_arguments)]
    pub fn upsert_summary_metadata(
        &self,
        request_id: &str,
        model: Option<&str>,
        route: Option<&str>,
        provider: Option<&str>,
        engine: Option<&str>,
        occurred_at: &str,
    ) -> Result<(), LogStoreError> {
        self.conn()
            .execute(
                "INSERT INTO summaries (request_id, created_at, model, route, provider, engine) \
                 VALUES (?, ?, ?, ?, ?, ?) \
                 ON CONFLICT(request_id) DO UPDATE SET \
                    model = COALESCE(summaries.model, excluded.model), \
                    route = COALESCE(summaries.route, excluded.route), \
                    provider = COALESCE(summaries.provider, excluded.provider), \
                    engine = COALESCE(summaries.engine, excluded.engine)",
                rusqlite::params![request_id, occurred_at, model, route, provider, engine],
            )
            .map(|_| ())
            .map_err(|error| LogStoreError::InsertFailed(error.to_string()))
    }

    /// Get a summary by request_id. Returns None if not found (no-op style).
    pub fn get_summary(&self, request_id: &str) -> Result<Option<SummaryRow>, LogStoreError> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT request_id, state, created_at, terminal_at, route, model, provider, engine, \
             status_code, error_msg, tenant_id, account_id, user_id \
             FROM summaries WHERE request_id = ?",
        ).map_err(LogStoreError::Sqlite)?;

        let row_fn = |row: &rusqlite::Row<'_>| -> rusqlite::Result<SummaryRow> {
            Ok(SummaryRow {
                request_id: row.get(0)?,
                state: row.get(1)?,
                created_at: row.get(2)?,
                terminal_at: row.get(3).ok(),
                route: row.get(4).ok(),
                model: row.get(5).ok(),
                provider: row.get(6).ok(),
                engine: row.get(7).ok(),
                status_code: row.get(8).ok(),
                error_msg: row.get(9).ok(),
                tenant_id: row.get(10).ok(),
                account_id: row.get(11).ok(),
                user_id: row.get(12).ok(),
            })
        };

        match stmt.query_row(rusqlite::params![request_id], row_fn) {
            Ok(row) => Ok(Some(row)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(LogStoreError::QueryFailed(e.to_string())),
        }
    }

    /// Paginated summary listing keyed on (created_at, request_id).
    pub fn list_summaries(
        &self,
        limit: usize,
        after_cursor: Option<&str>,
    ) -> Result<Page<SummaryRow>, LogStoreError> {
        let conn = self.conn();

        let row_fn = |row: &rusqlite::Row<'_>| -> rusqlite::Result<SummaryRow> {
            Ok(SummaryRow {
                request_id: row.get(0)?,
                state: row.get(1)?,
                created_at: row.get(2)?,
                terminal_at: row.get(3).ok(),
                route: row.get(4).ok(),
                model: row.get(5).ok(),
                provider: row.get(6).ok(),
                engine: row.get(7).ok(),
                status_code: row.get(8).ok(),
                error_msg: row.get(9).ok(),
                tenant_id: row.get(10).ok(),
                account_id: row.get(11).ok(),
                user_id: row.get(12).ok(),
            })
        };

        list_opaque_keyset_page(
            &conn,
            OpaqueKeysetPage {
                table: "summaries",
                columns: "request_id, state, created_at, terminal_at, route, model, provider, engine, \
                          status_code, error_msg, tenant_id, account_id, user_id",
                timestamp_column: "created_at",
                id_column: "request_id",
            },
            limit,
            after_cursor,
            row_fn,
            |row| (&row.created_at, &row.request_id),
        )
    }

    /// Update summary terminal state. No-op if request_id not found (returns 0 rows affected).
    pub fn update_summary_terminal(
        &self,
        request_id: &str,
        terminal_status: &str,
        terminal_at: &str,
    ) -> Result<usize, LogStoreError> {
        let conn = self.conn();
        conn.execute(
            "UPDATE summaries SET state = ?, terminal_at = ? WHERE request_id = ?",
            rusqlite::params![terminal_status, terminal_at, request_id],
        )
        .map_err(LogStoreError::Sqlite)
    }

    // ════════════════════════════
    //  Lifecycle Events
    // ════════════════════════════

    /// Insert a lifecycle event. Caller serializes the payload to JSON before calling.
    pub fn insert_lifecycle_event(
        &self,
        request_id: &str,
        event_id: &str,
        payload_json: &str,
        occurred_at: &str,
    ) -> Result<(), LogStoreError> {
        let conn = self.conn();
        let event_type = LifecycleEventType::from_payload(payload_json);

        // Pre-check for terminal duplicates.
        if event_type.is_terminal() && check_existing_terminal(&conn, request_id)? {
            return Err(LogStoreError::DuplicateTerminalEvent {
                summary_id: request_id.to_string(),
                event_type: event_type.code().to_string(),
            });
        }

        match conn.execute(
            "INSERT INTO lifecycle_events (event_id, request_id, occurred_at, payload_json, event_type, is_terminal) \
             VALUES (?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                event_id,
                request_id,
                occurred_at,
                payload_json,
                event_type.code(),
                i64::from(event_type.is_terminal()),
            ],
        ) {
            Ok(_) => Ok(()),
            Err(ref e) if is_unique_constraint_error(e) => {
                // Could be UNIQUE(request_id, event_id) or the partial terminal index.
                if event_type.is_terminal() {
                    return Err(LogStoreError::DuplicateTerminalEvent {
                        summary_id: request_id.to_string(),
                        event_type: event_type.code().to_string(),
                    });
                }
                Err(LogStoreError::AlreadyExists {
                    entity: "lifecycle_event".to_string(),
                })
            }
            Err(e) => Err(LogStoreError::InsertFailed(e.to_string())),
        }
    }

    /// Atomic write of a terminal event + summary state update. Both succeed or neither does.
    pub fn write_terminal_event(
        &self,
        request_id: &str,
        event_id: &str,
        payload_json: &str,
        terminal_status: &str,
        occurred_at: &str,
    ) -> Result<(), LogStoreError> {
        let event_type =
            LifecycleEventType::terminal_from_status(terminal_status).ok_or_else(|| {
                LogStoreError::InvalidQuery(
                    "terminal status must be a terminal lifecycle type".to_string(),
                )
            })?;
        self.txn(|tx| {
            let has_terminal = tx.query_row(
                "SELECT COUNT(*) FROM lifecycle_events WHERE request_id = ? AND is_terminal = 1",
                rusqlite::params![request_id],
                |row| row.get::<_, i64>(0),
            ).map(|c: i64| c > 0).map_err(LogStoreError::Sqlite)?;

            if has_terminal {
                return Err(LogStoreError::DuplicateTerminalEvent {
                    summary_id: request_id.to_string(),
                    event_type: event_type.code().to_string(),
                });
            }

            tx.execute(
                "INSERT INTO lifecycle_events (event_id, request_id, occurred_at, payload_json, event_type, is_terminal) \
                 VALUES (?, ?, ?, ?, ?, 1)",
                rusqlite::params![
                    event_id,
                    request_id,
                    occurred_at,
                    payload_json,
                    event_type.code(),
                ],
            ).map_err(LogStoreError::Sqlite)?;

            tx.execute(
                "UPDATE summaries SET state = ?, terminal_at = ? WHERE request_id = ?",
                rusqlite::params![terminal_status, occurred_at, request_id],
            ).map_err(LogStoreError::Sqlite)?;

            Ok(()) as Result<(), LogStoreError>
        })
    }

    /// Check if a summary already has any terminal event.
    pub fn has_terminal_event(&self, request_id: &str) -> Result<bool, LogStoreError> {
        let conn = self.conn();
        check_existing_terminal(&conn, request_id)
    }

    /// Paginated lifecycle event listing keyed on (occurred_at, event_id).
    pub fn list_lifecycle_events(
        &self,
        limit: usize,
        after_cursor: Option<&str>,
    ) -> Result<Page<LifecycleEventRow>, LogStoreError> {
        let conn = self.conn();

        let row_fn = |row: &rusqlite::Row<'_>| -> rusqlite::Result<LifecycleEventRow> {
            Ok(LifecycleEventRow {
                event_id: row.get(0)?,
                request_id: row.get(1)?,
                occurred_at: row.get(2)?,
            })
        };

        list_opaque_keyset_page(
            &conn,
            OpaqueKeysetPage {
                table: "lifecycle_events",
                columns: "event_id, request_id, occurred_at",
                timestamp_column: "occurred_at",
                id_column: "event_id",
            },
            limit,
            after_cursor,
            row_fn,
            |row| (&row.occurred_at, &row.event_id),
        )
    }

    /// List all lifecycle events for a specific summary, ordered chronologically.
    pub fn list_events_for_summary(
        &self,
        request_id: &str,
    ) -> Result<Vec<LifecycleEventRow>, LogStoreError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT event_id, request_id, occurred_at FROM lifecycle_events \
             WHERE request_id = ? ORDER BY occurred_at ASC",
            )
            .map_err(LogStoreError::Sqlite)?;

        let rows_iter = stmt
            .query_map(rusqlite::params![request_id], |row: &rusqlite::Row<'_>| {
                Ok(LifecycleEventRow {
                    event_id: row.get(0)?,
                    request_id: row.get(1)?,
                    occurred_at: row.get(2)?,
                })
            })
            .map_err(LogStoreError::Sqlite)?;

        let mut items = Vec::new();
        for result in rows_iter {
            match result {
                Ok(item) => items.push(item),
                Err(e) => return Err(LogStoreError::QueryFailed(e.to_string())),
            }
        }
        Ok(items)
    }

    // ════════════════════════════
    //  Proxy Records
    // ════════════════════════════

    #[allow(clippy::too_many_arguments)]
    pub fn insert_proxy_record(
        &self,
        attempt_id: &str,
        request_id: &str,
        occurred_at: &str,
        target: &str,
        provider: Option<&str>,
        engine: Option<&str>,
        started_at: Option<&str>,
        completed_at: Option<&str>,
        status_code: Option<i64>,
        error_msg: Option<&str>,
    ) -> Result<(), LogStoreError> {
        let conn = self.conn();
        match conn.execute(
            "INSERT INTO proxy_records (attempt_id, request_id, occurred_at, target, provider, engine, started_at, completed_at, status_code, error_msg) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![attempt_id, request_id, occurred_at, target, provider, engine, started_at, completed_at, status_code, error_msg],
        ) {
            Ok(_) => Ok(()),
            Err(ref e) if is_unique_constraint_error(e) => Err(LogStoreError::AlreadyExists {
                entity: format!("proxy_record {}", attempt_id),
            }),
            Err(e) => Err(LogStoreError::InsertFailed(e.to_string())),
        }
    }

    // ════════════════════════════
    //  Audit Entries
    // ════════════════════════════

    pub fn insert_audit_entry(
        &self,
        entry_id: &str,
        request_id: Option<&str>,
        occurred_at: &str,
        actor: &str,
        action: &str,
        detail_json: Option<&str>,
    ) -> Result<(), LogStoreError> {
        let conn = self.conn();
        match conn.execute(
            "INSERT INTO audit_entries (entry_id, request_id, occurred_at, actor, action, detail_json) VALUES (?, ?, ?, ?, ?, ?)",
            rusqlite::params![entry_id, request_id, occurred_at, actor, action, detail_json],
        ) {
            Ok(_) => Ok(()),
            Err(ref e) if is_unique_constraint_error(e) => Err(LogStoreError::AlreadyExists {
                entity: format!("audit_entry {}", entry_id),
            }),
            Err(e) => Err(LogStoreError::InsertFailed(e.to_string())),
        }
    }

    // ════════════════════════════
    //  Webhook Deliveries
    // ════════════════════════════

    /// Create one idempotent, scoped terminal webhook record in the durable
    /// pending state. The caller owns deterministic delivery-id derivation;
    /// re-enqueueing that same ID after a restart returns the existing record
    /// without creating a duplicate terminal delivery.
    pub fn enqueue_webhook_delivery(
        &self,
        delivery_id: &str,
        request_id: &str,
        created_at: &str,
        max_attempts: u32,
    ) -> Result<WebhookDeliveryInsertOutcome, LogStoreError> {
        validate_webhook_identifier(delivery_id, "delivery_id")?;
        validate_webhook_identifier(request_id, "request_id")?;
        validate_webhook_timestamp(created_at, "created_at")?;
        validate_webhook_max_attempts(max_attempts)?;

        self.txn(|tx| {
            let has_terminal = tx
                .query_row(
                    "SELECT COUNT(*) FROM lifecycle_events WHERE request_id = ? AND is_terminal = 1",
                    rusqlite::params![request_id],
                    |row| row.get::<_, i64>(0),
                )
                .map(|count| count > 0)
                .map_err(LogStoreError::Sqlite)?;
            if !has_terminal {
                return Err(LogStoreError::InvalidQuery(
                    "webhook delivery requires a durable terminal event".to_string(),
                ));
            }
            let inserted = tx.execute(
                "INSERT INTO webhook_deliveries \
                    (delivery_id, request_id, occurred_at, target_url, attempt_number, response_body, error_msg, \
                     state, created_at, updated_at, next_attempt_at, lease_expires_at, claim_generation, max_attempts, last_error_code) \
                 VALUES (?, ?, ?, ?, 0, NULL, NULL, ?, ?, ?, ?, NULL, 0, ?, NULL) \
                 ON CONFLICT(delivery_id) DO NOTHING",
                rusqlite::params![
                    delivery_id,
                    request_id,
                    created_at,
                    CONFIGURED_WEBHOOK_TARGET,
                    WebhookDeliveryState::Pending.code(),
                    created_at,
                    created_at,
                    created_at,
                    max_attempts,
                ],
            )
            .map_err(|error| LogStoreError::InsertFailed(error.to_string()))?;
            let record = select_webhook_delivery(tx, delivery_id)?
                .ok_or_else(|| LogStoreError::QueryFailed("webhook delivery insert disappeared".into()))?;
            if record.request_id.as_deref() != Some(request_id) || record.max_attempts != max_attempts
            {
                return Err(LogStoreError::InvalidQuery(
                    "webhook delivery_id conflicts with immutable delivery intent".to_string(),
                ));
            }
            let outcome = if inserted == 1 {
                WebhookDeliveryInsertOutcome::Created(record)
            } else {
                WebhookDeliveryInsertOutcome::Existing(record)
            };
            Ok(outcome)
        })
    }

    /// Atomically claim the oldest eligible pending/retry/manual retry record.
    /// A stale in-flight lease is reclaimable after restart. The incremented
    /// claim generation fences completion/retry writes from displaced workers.
    pub fn claim_next_webhook_delivery(
        &self,
        now: &str,
        lease_expires_at: &str,
    ) -> Result<Option<WebhookDeliveryRecord>, LogStoreError> {
        validate_webhook_timestamp(now, "claim timestamp")?;
        validate_webhook_timestamp(lease_expires_at, "lease expiration")?;

        self.txn(|tx| {
            let candidate: Option<(String, String, i64, i64)> = tx
                .query_row(
                    "SELECT delivery_id, state, attempt_number, max_attempts FROM webhook_deliveries \
                     WHERE (state IN ('pending', 'retry', 'manual_retry') \
                            AND (next_attempt_at IS NULL OR next_attempt_at <= ?)) \
                        OR (state = 'in_flight' AND lease_expires_at <= ?) \
                     ORDER BY COALESCE(next_attempt_at, created_at), created_at, delivery_id LIMIT 1",
                    rusqlite::params![now, now],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()
                .map_err(|error| LogStoreError::QueryFailed(error.to_string()))?;
            let Some((delivery_id, state, attempt_number, max_attempts)) = candidate else {
                return Ok(None);
            };
            if state == WebhookDeliveryState::InFlight.code() && attempt_number >= max_attempts {
                tx.execute(
                    "UPDATE webhook_deliveries \
                     SET state = 'dead_letter', updated_at = ?, lease_expires_at = NULL, \
                         next_attempt_at = NULL, last_error_code = 'transport' \
                     WHERE delivery_id = ? AND state = 'in_flight' \
                       AND lease_expires_at <= ? AND attempt_number >= max_attempts",
                    rusqlite::params![now, delivery_id, now],
                )
                .map_err(|error| LogStoreError::InsertFailed(error.to_string()))?;
                return Ok(None);
            }
            let changed = tx
                .execute(
                    "UPDATE webhook_deliveries \
                     SET state = 'in_flight', attempt_number = attempt_number + 1, \
                         updated_at = ?, lease_expires_at = ?, next_attempt_at = NULL, \
                         claim_generation = claim_generation + 1, last_error_code = NULL \
                     WHERE delivery_id = ? AND (\
                        (state IN ('pending', 'retry', 'manual_retry') \
                         AND (next_attempt_at IS NULL OR next_attempt_at <= ?)) \
                        OR (state = 'in_flight' AND lease_expires_at <= ? \
                            AND attempt_number < max_attempts))",
                    rusqlite::params![now, lease_expires_at, delivery_id, now, now],
                )
                .map_err(|error| LogStoreError::InsertFailed(error.to_string()))?;
            if changed == 0 {
                return Ok(None);
            }
            select_webhook_delivery(tx, &delivery_id)
        })
    }

    /// Complete an in-flight attempt only when its fencing generation still
    /// belongs to this worker. A duplicate/stale completion is a harmless
    /// false result, never a second delivery state transition.
    pub fn complete_webhook_delivery(
        &self,
        delivery_id: &str,
        claim_generation: u64,
        completed_at: &str,
        status_code: u16,
    ) -> Result<bool, LogStoreError> {
        validate_webhook_identifier(delivery_id, "delivery_id")?;
        validate_webhook_timestamp(completed_at, "completion timestamp")?;
        if !(200..=299).contains(&status_code) {
            return Err(LogStoreError::InvalidQuery(
                "webhook success status must be between 200 and 299".to_string(),
            ));
        }
        let conn = self.conn();
        conn.execute(
            "UPDATE webhook_deliveries \
             SET state = 'succeeded', updated_at = ?, lease_expires_at = NULL, \
                 next_attempt_at = NULL, status_code = ?, response_body = NULL, \
                 error_msg = NULL, last_error_code = NULL \
             WHERE delivery_id = ? AND state = 'in_flight' AND claim_generation = ?",
            rusqlite::params![completed_at, status_code, delivery_id, claim_generation],
        )
        .map(|changed| changed == 1)
        .map_err(|error| LogStoreError::InsertFailed(error.to_string()))
    }

    /// Record a bounded failure code and either schedule the next attempt or
    /// atomically enter dead-letter after the configured maximum.
    pub fn retry_or_dead_letter_webhook_delivery(
        &self,
        delivery_id: &str,
        claim_generation: u64,
        updated_at: &str,
        next_attempt_at: &str,
        error_code: WebhookDeliveryErrorCode,
    ) -> Result<Option<WebhookRetryOutcome>, LogStoreError> {
        validate_webhook_identifier(delivery_id, "delivery_id")?;
        validate_webhook_timestamp(updated_at, "update timestamp")?;
        validate_webhook_timestamp(next_attempt_at, "next attempt timestamp")?;

        self.txn(|tx| {
            let Some(record) = select_webhook_delivery(tx, delivery_id)? else {
                return Ok(None);
            };
            if record.state != WebhookDeliveryState::InFlight
                || record.claim_generation != claim_generation
            {
                return Ok(None);
            }
            let (state, retry_at, outcome) = if record.attempt_number >= record.max_attempts {
                (
                    WebhookDeliveryState::DeadLetter,
                    None,
                    WebhookRetryOutcome::DeadLettered,
                )
            } else {
                (
                    WebhookDeliveryState::Retry,
                    Some(next_attempt_at),
                    WebhookRetryOutcome::RetryScheduled,
                )
            };
            tx.execute(
                "UPDATE webhook_deliveries \
                 SET state = ?, updated_at = ?, lease_expires_at = NULL, next_attempt_at = ?, \
                     response_body = NULL, error_msg = NULL, last_error_code = ? \
                 WHERE delivery_id = ? AND state = 'in_flight' AND claim_generation = ?",
                rusqlite::params![
                    state.code(),
                    updated_at,
                    retry_at,
                    error_code.code(),
                    delivery_id,
                    claim_generation,
                ],
            )
            .map_err(|error| LogStoreError::InsertFailed(error.to_string()))?;
            Ok(Some(outcome))
        })
    }

    /// Explicit operator-driven retry opens a new bounded attempt cycle from
    /// a dead-letter record. It leaves the terminal request untouched and
    /// records the distinct manual-retry state for a later audit worker.
    pub fn manually_retry_webhook_delivery(
        &self,
        delivery_id: &str,
        requested_at: &str,
    ) -> Result<bool, LogStoreError> {
        validate_webhook_identifier(delivery_id, "delivery_id")?;
        validate_webhook_timestamp(requested_at, "manual retry timestamp")?;
        let conn = self.conn();
        conn.execute(
            "UPDATE webhook_deliveries \
             SET state = 'manual_retry', attempt_number = 0, updated_at = ?, \
                 next_attempt_at = ?, lease_expires_at = NULL, response_body = NULL, \
                 error_msg = NULL, last_error_code = NULL \
             WHERE delivery_id = ? AND state = 'dead_letter'",
            rusqlite::params![requested_at, requested_at, delivery_id],
        )
        .map(|changed| changed == 1)
        .map_err(|error| LogStoreError::InsertFailed(error.to_string()))
    }

    /// Load one delivery record for restart/resumption and focused tests.
    pub fn webhook_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<Option<WebhookDeliveryRecord>, LogStoreError> {
        validate_webhook_identifier(delivery_id, "delivery_id")?;
        select_webhook_delivery(&self.conn(), delivery_id)
    }

    /// Compatibility helper for retention tests. It deliberately discards
    /// the old URL/body/error inputs at the durable boundary; new code must
    /// use the typed state-machine API above.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_webhook_delivery(
        &self,
        delivery_id: &str,
        request_id: Option<&str>,
        occurred_at: &str,
        attempt_number: i64,
        status_code: Option<i64>,
    ) -> Result<(), LogStoreError> {
        validate_webhook_identifier(delivery_id, "delivery_id")?;
        validate_webhook_timestamp(occurred_at, "occurred_at")?;
        let attempt_number = attempt_number.max(1);
        let state = if status_code.is_some_and(|status| (200..=299).contains(&status)) {
            WebhookDeliveryState::Succeeded
        } else {
            WebhookDeliveryState::DeadLetter
        };
        let conn = self.conn();
        match conn.execute(
            "INSERT INTO webhook_deliveries \
                (delivery_id, request_id, occurred_at, target_url, attempt_number, status_code, response_body, error_msg, \
                 state, created_at, updated_at, max_attempts) \
             VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?)",
            rusqlite::params![
                delivery_id,
                request_id,
                occurred_at,
                CONFIGURED_WEBHOOK_TARGET,
                attempt_number,
                status_code,
                state.code(),
                occurred_at,
                occurred_at,
                attempt_number,
            ],
        ) {
            Ok(_) => Ok(()),
            Err(ref e) if is_unique_constraint_error(e) => Err(LogStoreError::AlreadyExists {
                entity: format!("webhook_delivery {}", delivery_id),
            }),
            Err(e) => Err(LogStoreError::InsertFailed(e.to_string())),
        }
    }

    // ════════════════════════════
    //  Cleanup Runs
    // ════════════════════════════

    pub fn insert_cleanup_run(
        &self,
        run_id: &str,
        occurred_at: &str,
        policy_name: &str,
        cutoff_before: &str,
        deleted_count: i64,
        duration_ms: Option<i64>,
    ) -> Result<(), LogStoreError> {
        let conn = self.conn();
        match conn.execute(
            "INSERT INTO cleanup_runs (run_id, occurred_at, policy_name, cutoff_before, deleted_count, duration_ms) VALUES (?, ?, ?, ?, ?, ?)",
            rusqlite::params![run_id, occurred_at, policy_name, cutoff_before, deleted_count, duration_ms],
        ) {
            Ok(_) => Ok(()),
            Err(ref e) if is_unique_constraint_error(e) => Err(LogStoreError::AlreadyExists {
                entity: format!("cleanup_run {}", run_id),
            }),
            Err(e) => Err(LogStoreError::InsertFailed(e.to_string())),
        }
    }

    // ════════════════════════════
    //  Cascade Cleanup
    // ════════════════════════════

    /// Compatibility entry point for the safe TTL half of retention.
    ///
    /// Unlike the former timestamp-only implementation, this never deletes an
    /// artifact pointer independently of its request summary.  Callers get
    /// only pointers whose owning terminal summary was removed transactionally.
    /// The row cap is deliberately disabled here; runtime retention supplies
    /// its configured bounded cap through [`Self::apply_retention_policy`].
    pub fn cascade_cleanup_before(
        &self,
        cutoff_occurred_at: &str,
    ) -> Result<(i64, Vec<CascadeArtifactPointer>), LogStoreError> {
        let result = self.apply_retention_policy(cutoff_occurred_at, i64::MAX as u64)?;
        Ok((result.ttl_deleted_count, result.artifact_pointers))
    }

    /// Apply the durable logging retention policy in one transaction.
    ///
    /// A terminal summary owns its lifecycle, proxy, and artifact-pointer
    /// children.  Time-to-live and row-cap selection therefore delete those
    /// summaries first and rely on foreign-key cascade for their children.
    /// This prevents an old artifact pointer from removing its file while the
    /// summary that still references it remains available.  Active summaries
    /// and all of their owned rows are deliberately retained.
    ///
    /// The backwards-compatible config pair is expanded into a complete map:
    /// independent audit, webhook, and cleanup receipt rows are each capped;
    /// request-owned lifecycle, proxy, and artifact rows use terminal-owner
    /// cascade selection so their parent/detail invariants remain intact.
    pub fn apply_retention_policy(
        &self,
        cutoff_occurred_at: &str,
        max_terminal_summaries: u64,
    ) -> Result<RetentionCleanupResult, LogStoreError> {
        self.apply_retention_policy_map(&RetentionPolicy::uniform(
            cutoff_occurred_at,
            max_terminal_summaries,
        )?)
    }

    /// Apply a complete per-table retention policy transactionally.  Request
    /// owned rows are never removed from an active summary.  Artifact-pointer
    /// TTL/caps select a terminal owner summary for cascade deletion rather
    /// than deleting a pointer/file independently.
    pub fn apply_retention_policy_map(
        &self,
        policy: &RetentionPolicy,
    ) -> Result<RetentionCleanupResult, LogStoreError> {
        self.txn(|tx| {
            let mut results = RetentionTable::ALL
                .into_iter()
                .map(|table| {
                    (
                        table,
                        RetentionTableResult {
                            table,
                            ttl_deleted_count: 0,
                            max_rows_deleted_count: 0,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let mut artifact_pointers = Vec::new();

            Self::apply_summary_owner_ttl(
                tx,
                policy.table(RetentionTable::Summaries),
                &mut results,
                &mut artifact_pointers,
            )?;
            Self::apply_artifact_owner_ttl(
                tx,
                policy.table(RetentionTable::ArtifactPointers),
                &mut results,
                &mut artifact_pointers,
            )?;
            Self::apply_terminal_owned_ttl(
                tx,
                RetentionTable::LifecycleEvents,
                policy.table(RetentionTable::LifecycleEvents),
                &mut results,
                &mut artifact_pointers,
            )?;
            Self::apply_terminal_owned_ttl(
                tx,
                RetentionTable::ProxyRecords,
                policy.table(RetentionTable::ProxyRecords),
                &mut results,
                &mut artifact_pointers,
            )?;
            for table in [
                RetentionTable::AuditEntries,
                RetentionTable::WebhookDeliveries,
                RetentionTable::CleanupRuns,
            ] {
                Self::apply_standalone_ttl(tx, table, policy.table(table), &mut results)?;
            }
            if let Some(cutoff_updated_at) = policy.webhook_dead_letter_cutoff_at() {
                Self::apply_webhook_dead_letter_ttl(tx, cutoff_updated_at, &mut results)?;
            }

            Self::apply_summary_owner_cap(
                tx,
                policy.table(RetentionTable::Summaries),
                &mut results,
                &mut artifact_pointers,
            )?;
            Self::apply_artifact_owner_cap(
                tx,
                policy.table(RetentionTable::ArtifactPointers),
                &mut results,
                &mut artifact_pointers,
            )?;
            for table in [
                RetentionTable::LifecycleEvents,
                RetentionTable::ProxyRecords,
            ] {
                Self::apply_terminal_owned_cap(
                    tx,
                    table,
                    policy.table(table),
                    &mut results,
                    &mut artifact_pointers,
                )?;
            }
            for table in [
                RetentionTable::AuditEntries,
                RetentionTable::WebhookDeliveries,
                RetentionTable::CleanupRuns,
            ] {
                Self::apply_standalone_cap(tx, table, policy.table(table), &mut results)?;
            }

            artifact_pointers.sort_by(|left, right| {
                (&left.request_id, &left.artifact_id).cmp(&(&right.request_id, &right.artifact_id))
            });
            artifact_pointers.dedup_by(|left, right| {
                left.request_id == right.request_id && left.artifact_id == right.artifact_id
            });
            let table_results = results.into_values().collect::<Vec<_>>();
            let ttl_deleted_count = table_results
                .iter()
                .map(|result| result.ttl_deleted_count)
                .sum();
            let max_rows_deleted_count = table_results
                .iter()
                .map(|result| result.max_rows_deleted_count)
                .sum();
            Ok(RetentionCleanupResult {
                ttl_deleted_count,
                max_rows_deleted_count,
                artifact_pointers,
                table_results,
            })
        })
    }

    fn apply_summary_owner_ttl(
        tx: &Transaction,
        policy: &RetentionTablePolicy,
        results: &mut BTreeMap<RetentionTable, RetentionTableResult>,
        artifact_pointers: &mut Vec<CascadeArtifactPointer>,
    ) -> Result<(), LogStoreError> {
        let before = Self::retention_snapshot(tx)?;
        let candidates = Self::select_terminal_summary_ids_before(tx, &policy.cutoff_occurred_at)?;
        let (_, pointers) = Self::delete_terminal_summary_candidates(tx, &candidates)?;
        artifact_pointers.extend(pointers);
        Self::record_retention_delta(tx, before, results, true)
    }

    fn apply_artifact_owner_ttl(
        tx: &Transaction,
        policy: &RetentionTablePolicy,
        results: &mut BTreeMap<RetentionTable, RetentionTableResult>,
        artifact_pointers: &mut Vec<CascadeArtifactPointer>,
    ) -> Result<(), LogStoreError> {
        let before = Self::retention_snapshot(tx)?;
        let candidates = Self::select_terminal_owner_ids_before(
            tx,
            RetentionTable::ArtifactPointers,
            &policy.cutoff_occurred_at,
        )?;
        let (_, pointers) = Self::delete_terminal_summary_candidates(tx, &candidates)?;
        artifact_pointers.extend(pointers);
        Self::record_retention_delta(tx, before, results, true)
    }

    fn apply_terminal_owned_ttl(
        tx: &Transaction,
        table: RetentionTable,
        policy: &RetentionTablePolicy,
        results: &mut BTreeMap<RetentionTable, RetentionTableResult>,
        artifact_pointers: &mut Vec<CascadeArtifactPointer>,
    ) -> Result<(), LogStoreError> {
        let before = Self::retention_snapshot(tx)?;
        let candidates =
            Self::select_terminal_owner_ids_before(tx, table, &policy.cutoff_occurred_at)?;
        let (_, pointers) = Self::delete_terminal_summary_candidates(tx, &candidates)?;
        artifact_pointers.extend(pointers);
        Self::record_retention_delta(tx, before, results, true)
    }

    fn apply_standalone_ttl(
        tx: &Transaction,
        table: RetentionTable,
        policy: &RetentionTablePolicy,
        results: &mut BTreeMap<RetentionTable, RetentionTableResult>,
    ) -> Result<(), LogStoreError> {
        let before = Self::retention_snapshot(tx)?;
        let (table_name, id_column) = Self::table_sql(table);
        Self::delete_rows_before(tx, table_name, id_column, &policy.cutoff_occurred_at)?;
        Self::record_retention_delta(tx, before, results, true)
    }

    fn apply_webhook_dead_letter_ttl(
        tx: &Transaction,
        cutoff_updated_at: &str,
        results: &mut BTreeMap<RetentionTable, RetentionTableResult>,
    ) -> Result<(), LogStoreError> {
        let before = Self::retention_snapshot(tx)?;
        tx.execute(
            &format!(
                r#"
                DELETE FROM webhook_deliveries
                WHERE delivery_id IN (
                    SELECT delivery_id FROM webhook_deliveries
                    WHERE state = 'dead_letter' AND updated_at < ?1
                    ORDER BY updated_at ASC, delivery_id ASC
                    LIMIT {MAX_SUMMARIES_PER_CAP_PRUNE}
                )
                "#
            ),
            rusqlite::params![cutoff_updated_at],
        )
        .map_err(LogStoreError::Sqlite)?;
        Self::record_retention_delta(tx, before, results, true)
    }

    fn apply_summary_owner_cap(
        tx: &Transaction,
        policy: &RetentionTablePolicy,
        results: &mut BTreeMap<RetentionTable, RetentionTableResult>,
        artifact_pointers: &mut Vec<CascadeArtifactPointer>,
    ) -> Result<(), LogStoreError> {
        let before = Self::retention_snapshot(tx)?;
        let candidates =
            Self::select_terminal_summary_ids_for_cap(tx, Self::policy_max_rows(policy)?)?;
        let (_, pointers) = Self::delete_terminal_summary_candidates(tx, &candidates)?;
        artifact_pointers.extend(pointers);
        Self::record_retention_delta(tx, before, results, false)
    }

    fn apply_artifact_owner_cap(
        tx: &Transaction,
        policy: &RetentionTablePolicy,
        results: &mut BTreeMap<RetentionTable, RetentionTableResult>,
        artifact_pointers: &mut Vec<CascadeArtifactPointer>,
    ) -> Result<(), LogStoreError> {
        let before = Self::retention_snapshot(tx)?;
        let candidates = Self::select_terminal_owner_ids_for_cap(
            tx,
            RetentionTable::ArtifactPointers,
            Self::policy_max_rows(policy)?,
        )?;
        let (_, pointers) = Self::delete_terminal_summary_candidates(tx, &candidates)?;
        artifact_pointers.extend(pointers);
        Self::record_retention_delta(tx, before, results, false)
    }

    fn apply_terminal_owned_cap(
        tx: &Transaction,
        table: RetentionTable,
        policy: &RetentionTablePolicy,
        results: &mut BTreeMap<RetentionTable, RetentionTableResult>,
        artifact_pointers: &mut Vec<CascadeArtifactPointer>,
    ) -> Result<(), LogStoreError> {
        let before = Self::retention_snapshot(tx)?;
        let candidates =
            Self::select_terminal_owner_ids_for_cap(tx, table, Self::policy_max_rows(policy)?)?;
        let (_, pointers) = Self::delete_terminal_summary_candidates(tx, &candidates)?;
        artifact_pointers.extend(pointers);
        Self::record_retention_delta(tx, before, results, false)
    }

    fn apply_standalone_cap(
        tx: &Transaction,
        table: RetentionTable,
        policy: &RetentionTablePolicy,
        results: &mut BTreeMap<RetentionTable, RetentionTableResult>,
    ) -> Result<(), LogStoreError> {
        let before = Self::retention_snapshot(tx)?;
        let (table_name, id_column) = Self::table_sql(table);
        Self::delete_rows_to_max(tx, table_name, id_column, Self::policy_max_rows(policy)?)?;
        Self::record_retention_delta(tx, before, results, false)
    }

    fn policy_max_rows(policy: &RetentionTablePolicy) -> Result<i64, LogStoreError> {
        i64::try_from(policy.max_rows).map_err(|_| {
            LogStoreError::QueryFailed("logging retention max rows is out of range".to_string())
        })
    }

    fn table_sql(table: RetentionTable) -> (&'static str, &'static str) {
        match table {
            RetentionTable::Summaries => ("summaries", "request_id"),
            RetentionTable::LifecycleEvents => ("lifecycle_events", "event_id"),
            RetentionTable::ArtifactPointers => ("artifact_pointers", "artifact_id"),
            RetentionTable::ProxyRecords => ("proxy_records", "attempt_id"),
            RetentionTable::AuditEntries => ("audit_entries", "entry_id"),
            RetentionTable::WebhookDeliveries => ("webhook_deliveries", "delivery_id"),
            RetentionTable::CleanupRuns => ("cleanup_runs", "run_id"),
        }
    }

    fn retention_snapshot(
        tx: &Transaction,
    ) -> Result<BTreeMap<RetentionTable, i64>, LogStoreError> {
        RetentionTable::ALL
            .into_iter()
            .map(|table| {
                let (name, _) = Self::table_sql(table);
                let count = tx
                    .query_row(&format!("SELECT COUNT(*) FROM {name}"), [], |row| {
                        row.get(0)
                    })
                    .map_err(LogStoreError::Sqlite)?;
                Ok((table, count))
            })
            .collect()
    }

    fn record_retention_delta(
        tx: &Transaction,
        before: BTreeMap<RetentionTable, i64>,
        results: &mut BTreeMap<RetentionTable, RetentionTableResult>,
        is_ttl: bool,
    ) -> Result<(), LogStoreError> {
        let after = Self::retention_snapshot(tx)?;
        for table in RetentionTable::ALL {
            let deleted = before[&table].saturating_sub(after[&table]);
            let entry = results
                .get_mut(&table)
                .expect("retention result map covers every durable table");
            if is_ttl {
                entry.ttl_deleted_count += deleted;
            } else {
                entry.max_rows_deleted_count += deleted;
            }
        }
        Ok(())
    }

    /// Trim oldest terminal summaries until at most `max_rows` terminal
    /// summaries remain, deleting no more than one bounded batch per call.
    ///
    /// Active summaries are deliberately excluded: request serving must never
    /// lose its live lifecycle owner merely because durable history exceeds a
    /// retention cap. Candidate ordering is stable by terminal timestamp (or
    /// creation timestamp for legacy terminal rows) and then request ID. The
    /// returned artifact pointers are captured in the same transaction before
    /// the summary cascade removes their rows, so callers can safely perform
    /// post-commit file deletion only for pointers they owned.
    pub fn cascade_prune_terminal_summaries_to_max_rows(
        &self,
        max_rows: u64,
    ) -> Result<(i64, Vec<CascadeArtifactPointer>), LogStoreError> {
        let max_rows = i64::try_from(max_rows).map_err(|_| {
            LogStoreError::QueryFailed("logging retention max rows is out of range".to_string())
        })?;
        if max_rows < 1 {
            return Err(LogStoreError::QueryFailed(
                "logging retention max rows must be at least one".to_string(),
            ));
        }

        self.txn(|tx| {
            let terminal_count: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM summaries WHERE state <> 'active'",
                    [],
                    |row| row.get(0),
                )
                .map_err(LogStoreError::Sqlite)?;
            let excess = terminal_count.saturating_sub(max_rows).max(0);
            let prune_count = excess.min(MAX_SUMMARIES_PER_CAP_PRUNE);
            if prune_count == 0 {
                return Ok((0, Vec::new()));
            }

            let mut pointers_stmt = tx
                .prepare(
                    "WITH candidates AS (\n\
                        SELECT request_id FROM summaries\n\
                        WHERE state <> 'active'\n\
                        ORDER BY COALESCE(terminal_at, created_at) ASC, request_id ASC\n\
                        LIMIT ?1\n\
                    )\n\
                    SELECT ap.artifact_id, ap.request_id\n\
                    FROM artifact_pointers ap\n\
                    INNER JOIN candidates ON candidates.request_id = ap.request_id\n\
                    ORDER BY ap.request_id ASC, ap.artifact_id ASC",
                )
                .map_err(LogStoreError::Sqlite)?;
            let pointers = pointers_stmt
                .query_map([prune_count], |row| {
                    Ok(CascadeArtifactPointer {
                        artifact_id: row.get(0)?,
                        request_id: row.get(1)?,
                    })
                })
                .map_err(LogStoreError::Sqlite)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| LogStoreError::QueryFailed(error.to_string()))?;

            let deleted_rows: i64 = tx
                .query_row(
                    "WITH candidates AS (\n\
                        SELECT request_id FROM summaries\n\
                        WHERE state <> 'active'\n\
                        ORDER BY COALESCE(terminal_at, created_at) ASC, request_id ASC\n\
                        LIMIT ?1\n\
                    )\n\
                    SELECT\n\
                        (SELECT COUNT(*) FROM candidates)\n\
                        + (SELECT COUNT(*) FROM lifecycle_events WHERE request_id IN (SELECT request_id FROM candidates))\n\
                        + (SELECT COUNT(*) FROM artifact_pointers WHERE request_id IN (SELECT request_id FROM candidates))\n\
                        + (SELECT COUNT(*) FROM proxy_records WHERE request_id IN (SELECT request_id FROM candidates))",
                    [prune_count],
                    |row| row.get(0),
                )
                .map_err(LogStoreError::Sqlite)?;

            tx.execute(
                "WITH candidates AS (\n\
                    SELECT request_id FROM summaries\n\
                    WHERE state <> 'active'\n\
                    ORDER BY COALESCE(terminal_at, created_at) ASC, request_id ASC\n\
                    LIMIT ?1\n\
                )\n\
                DELETE FROM summaries\n\
                WHERE request_id IN (SELECT request_id FROM candidates)",
                [prune_count],
            )
            .map_err(LogStoreError::Sqlite)?;

            Ok((deleted_rows, pointers))
        })
    }

    fn select_terminal_summary_ids_before(
        tx: &Transaction,
        cutoff_occurred_at: &str,
    ) -> Result<Vec<String>, LogStoreError> {
        let mut statement = tx
            .prepare(
                "SELECT request_id FROM summaries\n\
                 WHERE state IN ('completed', 'failed', 'rejected', 'cancelled', 'dropped')\n\
                   AND COALESCE(terminal_at, created_at) < ?1\n\
                 ORDER BY COALESCE(terminal_at, created_at) ASC, request_id ASC",
            )
            .map_err(LogStoreError::Sqlite)?;
        Self::collect_request_ids(&mut statement, rusqlite::params![cutoff_occurred_at])
    }

    fn select_terminal_summary_ids_for_cap(
        tx: &Transaction,
        max_terminal_summaries: i64,
    ) -> Result<Vec<String>, LogStoreError> {
        let terminal_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM summaries\n\
                 WHERE state IN ('completed', 'failed', 'rejected', 'cancelled', 'dropped')",
                [],
                |row| row.get(0),
            )
            .map_err(LogStoreError::Sqlite)?;
        let prune_count = terminal_count
            .saturating_sub(max_terminal_summaries)
            .clamp(0, MAX_SUMMARIES_PER_CAP_PRUNE);
        if prune_count == 0 {
            return Ok(Vec::new());
        }

        let mut statement = tx
            .prepare(
                "SELECT request_id FROM summaries\n\
                 WHERE state IN ('completed', 'failed', 'rejected', 'cancelled', 'dropped')\n\
                 ORDER BY COALESCE(terminal_at, created_at) ASC, request_id ASC\n\
                 LIMIT ?1",
            )
            .map_err(LogStoreError::Sqlite)?;
        Self::collect_request_ids(&mut statement, rusqlite::params![prune_count])
    }

    fn select_terminal_owner_ids_before(
        tx: &Transaction,
        table: RetentionTable,
        cutoff_occurred_at: &str,
    ) -> Result<Vec<String>, LogStoreError> {
        let (table_name, id_column) = Self::table_sql(table);
        let mut statement = tx
            .prepare(&format!(
                "SELECT {table_name}.request_id\n\
                 FROM {table_name}\n\
                 INNER JOIN summaries ON summaries.request_id = {table_name}.request_id\n\
                 WHERE summaries.state IN ('completed', 'failed', 'rejected', 'cancelled', 'dropped')\n\
                   AND {table_name}.occurred_at < ?1\n\
                 GROUP BY {table_name}.request_id\n\
                 ORDER BY MIN({table_name}.occurred_at) ASC, MIN({table_name}.{id_column}) ASC, {table_name}.request_id ASC"
            ))
            .map_err(LogStoreError::Sqlite)?;
        Self::collect_request_ids(&mut statement, rusqlite::params![cutoff_occurred_at])
    }

    fn select_terminal_owner_ids_for_cap(
        tx: &Transaction,
        table: RetentionTable,
        max_rows: i64,
    ) -> Result<Vec<String>, LogStoreError> {
        let (table_name, id_column) = Self::table_sql(table);
        let terminal_row_count: i64 = tx
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM {table_name}\n\
                     INNER JOIN summaries ON summaries.request_id = {table_name}.request_id\n\
                     WHERE summaries.state IN ('completed', 'failed', 'rejected', 'cancelled', 'dropped')"
                ),
                [],
                |row| row.get(0),
            )
            .map_err(LogStoreError::Sqlite)?;
        let prune_count = terminal_row_count
            .saturating_sub(max_rows)
            .clamp(0, MAX_SUMMARIES_PER_CAP_PRUNE);
        if prune_count == 0 {
            return Ok(Vec::new());
        }
        let mut statement = tx
            .prepare(&format!(
                "SELECT {table_name}.request_id\n\
                 FROM {table_name}\n\
                 INNER JOIN summaries ON summaries.request_id = {table_name}.request_id\n\
                 WHERE summaries.state IN ('completed', 'failed', 'rejected', 'cancelled', 'dropped')\n\
                 GROUP BY {table_name}.request_id\n\
                 ORDER BY MIN({table_name}.occurred_at) ASC, MIN({table_name}.{id_column}) ASC, {table_name}.request_id ASC\n\
                 LIMIT ?1"
            ))
            .map_err(LogStoreError::Sqlite)?;
        Self::collect_request_ids(&mut statement, rusqlite::params![prune_count])
    }

    fn collect_request_ids(
        statement: &mut rusqlite::Statement<'_>,
        parameters: impl rusqlite::Params,
    ) -> Result<Vec<String>, LogStoreError> {
        statement
            .query_map(parameters, |row| row.get(0))
            .map_err(LogStoreError::Sqlite)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| LogStoreError::QueryFailed(error.to_string()))
    }

    fn delete_terminal_summary_candidates(
        tx: &Transaction,
        request_ids: &[String],
    ) -> Result<(i64, Vec<CascadeArtifactPointer>), LogStoreError> {
        let mut deleted_count = 0;
        let mut pointers = Vec::new();
        for request_id in request_ids {
            let mut pointer_statement = tx
                .prepare(
                    "SELECT artifact_id, request_id FROM artifact_pointers\n\
                     WHERE request_id = ?1 ORDER BY artifact_id ASC",
                )
                .map_err(LogStoreError::Sqlite)?;
            let mut selected_pointers = pointer_statement
                .query_map(rusqlite::params![request_id], |row| {
                    Ok(CascadeArtifactPointer {
                        artifact_id: row.get(0)?,
                        request_id: row.get(1)?,
                    })
                })
                .map_err(LogStoreError::Sqlite)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| LogStoreError::QueryFailed(error.to_string()))?;

            let child_count: i64 = tx
                .query_row(
                    "SELECT\n\
                       1\n\
                       + (SELECT COUNT(*) FROM lifecycle_events WHERE request_id = ?1)\n\
                       + (SELECT COUNT(*) FROM artifact_pointers WHERE request_id = ?1)\n\
                       + (SELECT COUNT(*) FROM proxy_records WHERE request_id = ?1)",
                    rusqlite::params![request_id],
                    |row| row.get(0),
                )
                .map_err(LogStoreError::Sqlite)?;
            let deleted = tx
                .execute(
                    "DELETE FROM summaries WHERE request_id = ?1\n\
                     AND state IN ('completed', 'failed', 'rejected', 'cancelled', 'dropped')",
                    rusqlite::params![request_id],
                )
                .map_err(LogStoreError::Sqlite)?;
            if deleted == 1 {
                deleted_count += child_count;
                pointers.append(&mut selected_pointers);
            }
        }
        Ok((deleted_count, pointers))
    }

    fn delete_rows_before(
        tx: &Transaction,
        table: &str,
        id_column: &str,
        cutoff_occurred_at: &str,
    ) -> Result<i64, LogStoreError> {
        let deleted = tx
            .execute(
                &format!(
                    "DELETE FROM {table} WHERE {id_column} IN (\n\
                     SELECT {id_column} FROM {table}\n\
                     WHERE occurred_at < ?1\n\
                     ORDER BY occurred_at ASC, {id_column} ASC\n\
                     LIMIT {MAX_SUMMARIES_PER_CAP_PRUNE}\n\
                     )"
                ),
                rusqlite::params![cutoff_occurred_at],
            )
            .map_err(LogStoreError::Sqlite)?;
        Ok(deleted as i64)
    }

    fn delete_rows_to_max(
        tx: &Transaction,
        table: &str,
        id_column: &str,
        max_rows: i64,
    ) -> Result<i64, LogStoreError> {
        let count: i64 = tx
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .map_err(LogStoreError::Sqlite)?;
        let prune_count = count
            .saturating_sub(max_rows)
            .clamp(0, MAX_SUMMARIES_PER_CAP_PRUNE);
        if prune_count == 0 {
            return Ok(0);
        }
        let deleted = tx
            .execute(
                &format!(
                    "DELETE FROM {table} WHERE {id_column} IN (\n\
                     SELECT {id_column} FROM {table}\n\
                     ORDER BY occurred_at ASC, {id_column} ASC\n\
                     LIMIT ?1)"
                ),
                rusqlite::params![prune_count],
            )
            .map_err(LogStoreError::Sqlite)?;
        Ok(deleted as i64)
    }

    // ════════════════════════════
    //  Aggregation Queries
    // ════════════════════════════

    /// Count summaries grouped by state.
    pub fn count_summaries_by_status(&self) -> Result<Vec<(String, i64)>, LogStoreError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare("SELECT state, COUNT(*) FROM summaries GROUP BY state")
            .map_err(LogStoreError::Sqlite)?;

        let rows_iter = stmt
            .query_map([], |row: &rusqlite::Row<'_>| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(LogStoreError::Sqlite)?;

        let mut items = Vec::new();
        for result in rows_iter {
            match result {
                Ok(item) => items.push(item),
                Err(e) => return Err(LogStoreError::QueryFailed(e.to_string())),
            }
        }
        Ok(items)
    }

    // ════════════════════════════
    //  Test Helpers
    // ════════════════════════════

    #[cfg(test)]
    pub fn count_table(&self, table_name: &str) -> Result<i64, LogStoreError> {
        let conn = self.conn();
        let sql = format!("SELECT COUNT(*) FROM {}", table_name);
        conn.query_row(&sql, [], |row| row.get::<_, i64>(0))
            .map_err(LogStoreError::Sqlite)
    }

    #[cfg(test)]
    pub fn clear_table(&self, table_name: &str) -> Result<usize, LogStoreError> {
        let conn = self.conn();
        let sql = format!("DELETE FROM {}", table_name);
        conn.execute(&sql, []).map_err(LogStoreError::Sqlite)
    }
}
