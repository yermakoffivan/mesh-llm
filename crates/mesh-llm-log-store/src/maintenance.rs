//! Durable, bounded operator cleanup receipts.
//!
//! Preview snapshots a limited set of terminal-summary owners. Execute consumes
//! that snapshot exactly once in one SQLite transaction; a completed operation
//! ID is replayed without repeating either deletion or its audit entry.

use std::{collections::HashSet, convert::TryFrom, fmt};

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{OptionalExtension, Transaction};
use uuid::Uuid;

use crate::artifacts::{CascadeArtifactDeleteFailure, CascadeArtifactDeleteResult};
use crate::{ArtifactFileStore, CascadeArtifactPointer, LogStore, LogStoreError};
use sha2::{Digest, Sha256};

const MAX_CLEANUP_REQUESTS: usize = 100;
const MAX_REASON_BYTES: usize = 256;
const DELETE_ONE_SCOPE_CUTOFF: &str = "1970-01-01T00:00:00Z";

mod scope_filters;

pub use scope_filters::{CleanupFilters, CleanupOutcome};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceAction {
    Cleanup,
    DeleteOne,
}

impl MaintenanceAction {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Cleanup => "cleanup",
            Self::DeleteOne => "delete_one",
        }
    }

    fn from_str(value: &str) -> Result<Self, LogStoreError> {
        match value {
            "cleanup" => Ok(Self::Cleanup),
            "delete_one" => Ok(Self::DeleteOne),
            _ => Err(LogStoreError::QueryFailed(
                "invalid maintenance action".to_owned(),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct MaintenanceOperationId(Uuid);

impl MaintenanceOperationId {
    pub const fn new(value: Uuid) -> Self {
        Self(value)
    }
}

impl fmt::Display for MaintenanceOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl TryFrom<&str> for MaintenanceOperationId {
    type Error = LogStoreError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Uuid::parse_str(value)
            .map(Self)
            .map_err(|_| LogStoreError::MaintenanceScopeInvalid {
                field: "operation_id",
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceReason(String);

impl MaintenanceReason {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for MaintenanceReason {
    type Error = LogStoreError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let value = value.trim();
        if value.is_empty() || value.len() > MAX_REASON_BYTES || value.chars().any(char::is_control)
        {
            return Err(LogStoreError::MaintenanceScopeInvalid { field: "reason" });
        }
        Ok(Self(value.to_owned()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceTimestamp(String);

impl MaintenanceTimestamp {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for MaintenanceTimestamp {
    type Error = LogStoreError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let value = DateTime::parse_from_rfc3339(value).map_err(|_| {
            LogStoreError::MaintenanceScopeInvalid {
                field: "cutoff_before",
            }
        })?;
        Ok(Self(
            value
                .with_timezone(&Utc)
                .to_rfc3339_opts(SecondsFormat::Secs, true),
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupScope {
    cutoff_before: MaintenanceTimestamp,
    request_limit: u8,
    filters: CleanupFilters,
}

impl CleanupScope {
    pub fn new(
        cutoff_before: MaintenanceTimestamp,
        request_limit: usize,
    ) -> Result<Self, LogStoreError> {
        if !(1..=MAX_CLEANUP_REQUESTS).contains(&request_limit) {
            return Err(LogStoreError::MaintenanceScopeInvalid {
                field: "request_limit",
            });
        }
        Ok(Self {
            cutoff_before,
            request_limit: request_limit as u8,
            filters: CleanupFilters::default(),
        })
    }

    pub fn with_filters(mut self, filters: CleanupFilters) -> Self {
        self.filters = filters;
        self
    }

    pub fn cutoff_before(&self) -> &MaintenanceTimestamp {
        &self.cutoff_before
    }

    pub const fn request_limit(&self) -> usize {
        self.request_limit as usize
    }

    pub const fn filters(&self) -> &CleanupFilters {
        &self.filters
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupPreviewRequest {
    pub operation_id: MaintenanceOperationId,
    pub scope: CleanupScope,
    pub reason: MaintenanceReason,
}

/// Delete exactly one durable request owner. The operation ID is immutable:
/// retries with the same ID replay its receipt, including a missing target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteOneRequest {
    pub operation_id: MaintenanceOperationId,
    pub request_id: String,
    pub reason: MaintenanceReason,
}

impl DeleteOneRequest {
    pub fn new(
        operation_id: MaintenanceOperationId,
        request_id: &str,
        reason: MaintenanceReason,
    ) -> Result<Self, LogStoreError> {
        let request_id = Uuid::parse_str(request_id)
            .map(|value| value.to_string())
            .map_err(|_| LogStoreError::MaintenanceScopeInvalid {
                field: "request_id",
            })?;
        Ok(Self {
            operation_id,
            request_id,
            reason,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MaintenanceCounts {
    pub requests: u64,
    pub events: u64,
    pub artifacts: u64,
    pub proxy_records: u64,
    pub database_rows: u64,
}

/// Path-free, durable progress for one maintenance artifact cascade.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArtifactDeletionProgress {
    /// Durable artifact pointer rows reconciled after a file was removed or
    /// was already missing.
    pub removed: u64,
    /// Pointer-owned files that could not be removed and therefore remain
    /// durable and eligible for an exact same-operation retry.
    pub failed: u64,
    /// Coarse stable class for the current failed set. It never includes an
    /// OS message or filesystem path.
    pub failure_class: Option<ArtifactDeletionFailureClass>,
}

/// Stable classification for a failed maintenance artifact removal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactDeletionFailureClass {
    Io,
    UnsafePath,
}

impl From<CascadeArtifactDeleteFailure> for ArtifactDeletionFailureClass {
    fn from(value: CascadeArtifactDeleteFailure) -> Self {
        match value {
            CascadeArtifactDeleteFailure::Io => Self::Io,
            CascadeArtifactDeleteFailure::UnsafePath => Self::UnsafePath,
        }
    }
}

impl ArtifactDeletionFailureClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Io => "io",
            Self::UnsafePath => "unsafe_path",
        }
    }

    fn from_str(value: &str) -> Result<Self, LogStoreError> {
        match value {
            "io" => Ok(Self::Io),
            "unsafe_path" => Ok(Self::UnsafePath),
            _ => Err(LogStoreError::QueryFailed(
                "invalid artifact deletion failure class".to_owned(),
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceFingerprint(String);

impl MaintenanceFingerprint {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceReceiptState {
    Previewed,
    Completed,
    Partial,
}

impl MaintenanceReceiptState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Previewed => "previewed",
            Self::Completed => "completed",
            Self::Partial => "partial",
        }
    }

    fn from_str(value: &str) -> Result<Self, LogStoreError> {
        match value {
            "previewed" => Ok(Self::Previewed),
            "completed" => Ok(Self::Completed),
            "partial" => Ok(Self::Partial),
            _ => Err(LogStoreError::QueryFailed(
                "invalid maintenance receipt state".to_owned(),
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceReceipt {
    pub operation_id: MaintenanceOperationId,
    pub action: MaintenanceAction,
    pub scope: CleanupScope,
    pub state: MaintenanceReceiptState,
    pub planned: MaintenanceCounts,
    pub executed: MaintenanceCounts,
    pub artifact_deletion: ArtifactDeletionProgress,
    pub has_more: bool,
    pub fingerprint: MaintenanceFingerprint,
    /// Audit entry created with the durable cleanup preview, if applicable.
    pub preview_audit_id: Option<String>,
    /// Most recent audit entry created with successful cleanup/delete execution.
    pub execution_audit_id: Option<String>,
}

/// A narrow cooperative cancellation seam. The caller may back it with a
/// deadline or shutdown flag; execution checks it before each transaction
/// mutation and never commits a partially selected target list.
pub trait MaintenanceExecutionControl: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

impl LogStore {
    /// Return the completed receipt for one matching delete-one operation
    /// without opening a new delete operation. Callers use this to replay a
    /// successful deletion after its request owner has been removed.
    pub fn delete_one_receipt(
        &self,
        request: &DeleteOneRequest,
    ) -> Result<Option<MaintenanceReceipt>, LogStoreError> {
        self.txn(|transaction| {
            let Some(receipt) = load_receipt(transaction, request.operation_id)? else {
                return Ok(None);
            };
            ensure_same_delete_one(transaction, &receipt, request)?;
            Ok(Some(receipt))
        })
    }

    /// Snapshot terminal request owners before a caller-supplied cutoff and
    /// persist the exact target list plus a stable trusted-local audit record.
    pub fn preview_cleanup(
        &self,
        request: &CleanupPreviewRequest,
        control: &dyn MaintenanceExecutionControl,
    ) -> Result<MaintenanceReceipt, LogStoreError> {
        self.txn(|transaction| {
            if let Some(existing) = load_receipt(transaction, request.operation_id)? {
                ensure_same_intent(
                    &existing,
                    &load_reason(transaction, request.operation_id)?,
                    request,
                )?;
                return Ok(existing);
            }

            if control.is_cancelled() {
                return Err(LogStoreError::MaintenanceExecutionCancelled);
            }
            let (targets, has_more) = select_targets(transaction, &request.scope)?;
            let planned = count_targets(transaction, &targets)?;
            // Reads may take longer than a route's wall-clock budget. Check
            // again immediately before the first write so a caller that has
            // already timed out cannot later observe an unrequested receipt.
            if control.is_cancelled() {
                return Err(LogStoreError::MaintenanceExecutionCancelled);
            }
            let preview_audit_id = Uuid::new_v4().to_string();
            let receipt = MaintenanceReceipt {
                operation_id: request.operation_id,
                action: MaintenanceAction::Cleanup,
                scope: request.scope.clone(),
                state: MaintenanceReceiptState::Previewed,
                planned,
                executed: MaintenanceCounts::default(),
                artifact_deletion: ArtifactDeletionProgress::default(),
                has_more,
                fingerprint: selection_fingerprint(MaintenanceAction::Cleanup, &request.scope, &targets),
                preview_audit_id: Some(preview_audit_id.clone()),
                execution_audit_id: None,
            };
            persist_receipt(transaction, &receipt, request.reason.as_str(), &self.now())?;
            for (ordinal, request_id) in targets.iter().enumerate() {
                transaction
                    .execute(
                        "INSERT INTO maintenance_operation_targets (operation_id, ordinal, request_id) VALUES (?1, ?2, ?3)",
                        rusqlite::params![request.operation_id.to_string(), ordinal, request_id],
                    )
                    .map_err(LogStoreError::Sqlite)?;
            }
            let preview_result = if receipt.has_more { "partial" } else { "previewed" };
            write_audit(
                transaction,
                &self.now(),
                &preview_audit_id,
                request.operation_id,
                "log_cleanup_preview",
                preview_result,
                request.reason.as_str(),
            )?;
            Ok(receipt)
        })
    }
}

impl ArtifactFileStore {
    /// Execute one previously previewed cleanup operation. A completed receipt
    /// is returned verbatim on every retry, so the operation ID is idempotent.
    pub fn execute_cleanup(
        &self,
        operation_id: MaintenanceOperationId,
        expected_reason: &MaintenanceReason,
        control: &dyn MaintenanceExecutionControl,
    ) -> Result<MaintenanceReceipt, LogStoreError> {
        let _maintenance = self.maintenance_lock();
        let plan = self.store_ref().txn(|transaction| {
            let receipt = load_receipt(transaction, operation_id)?
                .ok_or(LogStoreError::MaintenanceOperationNotFound)?;
            if receipt.action != MaintenanceAction::Cleanup {
                return Err(LogStoreError::MaintenanceOperationConflict);
            }
            let reason = load_reason(transaction, operation_id)?;
            if reason != expected_reason.as_str() {
                return Err(LogStoreError::MaintenanceOperationConflict);
            }
            if receipt.state == MaintenanceReceiptState::Completed
                || (receipt.state == MaintenanceReceiptState::Partial
                    && receipt.artifact_deletion.failed == 0)
            {
                return Ok(CleanupExecutionPlan::Replay(receipt));
            }
            ensure_maintenance_active(control)?;
            let targets = load_targets(transaction, operation_id)?;
            ensure_maintenance_active(control)?;
            Ok(CleanupExecutionPlan::Execute {
                receipt,
                targets: targets.clone(),
                pointers: terminal_target_artifacts(transaction, &targets)?,
                reason,
            })
        })?;
        match plan {
            CleanupExecutionPlan::Replay(receipt) => Ok(receipt),
            CleanupExecutionPlan::Execute {
                receipt,
                targets,
                pointers,
                reason,
            } => {
                ensure_maintenance_active(control)?;
                let results = self.delete_artifact_files_for_maintenance(&pointers, control)?;
                ensure_maintenance_active(control)?;
                let occurred_at = self.store_ref().now();
                let context = CleanupExecutionContext {
                    targets: &targets,
                    operation_id,
                    reason: &reason,
                    occurred_at: &occurred_at,
                    control,
                };
                self.store_ref().txn(|transaction| {
                    complete_cleanup_execution(transaction, receipt, &results, &context)
                })
            }
        }
    }

    /// Delete one request owner and all of its durable children. The request
    /// ID is frozen into the operation target table even when absent, so a
    /// retry receives the original completed receipt instead of becoming a
    /// new deletion with a broader meaning.
    pub fn delete_request_cascade(
        &self,
        request: &DeleteOneRequest,
        control: &dyn MaintenanceExecutionControl,
    ) -> Result<MaintenanceReceipt, LogStoreError> {
        let _maintenance = self.maintenance_lock();
        let plan = self.store_ref().txn(|transaction| {
            if let Some(existing) = load_receipt(transaction, request.operation_id)? {
                ensure_same_delete_one(transaction, &existing, request)?;
                if existing.state == MaintenanceReceiptState::Completed {
                    return Ok(DeleteRequestPlan::Replay(existing));
                }
                return Ok(DeleteRequestPlan::Retry {
                    receipt: existing,
                    pointers: terminal_request_artifacts(transaction, &request.request_id)?,
                });
            }
            ensure_maintenance_active(control)?;
            let scope = delete_one_scope()?;
            let targets = vec![request.request_id.clone()];
            let planned = count_terminal_request_owner(transaction, &request.request_id)?;
            let pointers = terminal_request_artifacts(transaction, &request.request_id)?;
            ensure_maintenance_active(control)?;
            let fingerprint = selection_fingerprint(MaintenanceAction::DeleteOne, &scope, &targets);
            Ok(DeleteRequestPlan::First {
                scope,
                planned,
                fingerprint,
                pointers,
            })
        })?;

        let DeleteRequestPlan::Replay(receipt) = plan else {
            ensure_maintenance_active(control)?;
            let pointers = plan.pointers();
            let results = self.delete_artifact_files_for_maintenance(pointers, control)?;
            ensure_maintenance_active(control)?;
            let occurred_at = self.store_ref().now();
            return self.store_ref().txn(|transaction| {
                complete_delete_request(transaction, request, plan, &results, &occurred_at, control)
            });
        };
        Ok(receipt)
    }
}

enum DeleteRequestPlan {
    Replay(MaintenanceReceipt),
    First {
        scope: CleanupScope,
        planned: MaintenanceCounts,
        fingerprint: MaintenanceFingerprint,
        pointers: Vec<CascadeArtifactPointer>,
    },
    Retry {
        receipt: MaintenanceReceipt,
        pointers: Vec<CascadeArtifactPointer>,
    },
}

enum CleanupExecutionPlan {
    Replay(MaintenanceReceipt),
    Execute {
        receipt: MaintenanceReceipt,
        targets: Vec<String>,
        pointers: Vec<CascadeArtifactPointer>,
        reason: String,
    },
}

struct CleanupExecutionContext<'a> {
    targets: &'a [String],
    operation_id: MaintenanceOperationId,
    reason: &'a str,
    occurred_at: &'a str,
    control: &'a dyn MaintenanceExecutionControl,
}

impl DeleteRequestPlan {
    fn pointers(&self) -> &[CascadeArtifactPointer] {
        match self {
            Self::Replay(_) => &[],
            Self::First { pointers, .. } | Self::Retry { pointers, .. } => pointers,
        }
    }
}

fn ensure_maintenance_active(
    control: &dyn MaintenanceExecutionControl,
) -> Result<(), LogStoreError> {
    if control.is_cancelled() {
        Err(LogStoreError::MaintenanceExecutionCancelled)
    } else {
        Ok(())
    }
}

fn complete_delete_request(
    transaction: &Transaction<'_>,
    request: &DeleteOneRequest,
    plan: DeleteRequestPlan,
    results: &[CascadeArtifactDeleteResult],
    occurred_at: &str,
    control: &dyn MaintenanceExecutionControl,
) -> Result<MaintenanceReceipt, LogStoreError> {
    ensure_maintenance_active(control)?;
    let (mut receipt, is_first) = match plan {
        DeleteRequestPlan::First {
            scope,
            planned,
            fingerprint,
            ..
        } => (
            MaintenanceReceipt {
                operation_id: request.operation_id,
                action: MaintenanceAction::DeleteOne,
                scope,
                state: MaintenanceReceiptState::Partial,
                planned,
                executed: MaintenanceCounts::default(),
                artifact_deletion: ArtifactDeletionProgress::default(),
                has_more: false,
                fingerprint,
                preview_audit_id: None,
                execution_audit_id: None,
            },
            true,
        ),
        DeleteRequestPlan::Retry { receipt, .. } => (receipt, false),
        DeleteRequestPlan::Replay(_) => {
            return Err(LogStoreError::MaintenanceOperationConflict);
        }
    };
    let removed = remove_successful_artifact_pointers(transaction, results, control)?;
    receipt.executed.artifacts += removed;
    receipt.executed.database_rows += removed;
    receipt.artifact_deletion.removed += removed;
    receipt.artifact_deletion.failed =
        results.iter().filter(|result| !result.succeeded()).count() as u64;
    receipt.artifact_deletion.failure_class = results
        .iter()
        .find_map(CascadeArtifactDeleteResult::failure_class)
        .map(ArtifactDeletionFailureClass::from);

    if receipt.artifact_deletion.failed == 0 {
        let (final_counts, unexpected_pointers) =
            delete_request_owner(transaction, &request.request_id, control)?;
        if !unexpected_pointers.is_empty() {
            return Err(LogStoreError::MaintenanceOperationConflict);
        }
        add_counts(&mut receipt.executed, final_counts);
        receipt.state = if receipt.executed == receipt.planned {
            MaintenanceReceiptState::Completed
        } else {
            MaintenanceReceiptState::Partial
        };
    } else {
        receipt.state = MaintenanceReceiptState::Partial;
    }

    ensure_maintenance_active(control)?;
    let execution_audit_id = Uuid::new_v4().to_string();
    receipt.execution_audit_id = Some(execution_audit_id.clone());
    if is_first {
        persist_receipt(transaction, &receipt, request.reason.as_str(), occurred_at)?;
        transaction
            .execute(
                "INSERT INTO maintenance_operation_targets (operation_id, ordinal, request_id) VALUES (?1, 0, ?2)",
                rusqlite::params![request.operation_id.to_string(), request.request_id],
            )
            .map_err(LogStoreError::Sqlite)?;
    } else {
        update_maintenance_receipt(transaction, &receipt, occurred_at)?;
    }
    ensure_maintenance_active(control)?;
    write_audit(
        transaction,
        occurred_at,
        &execution_audit_id,
        request.operation_id,
        "log_delete_request",
        receipt.state.as_str(),
        request.reason.as_str(),
    )?;
    Ok(receipt)
}

fn complete_cleanup_execution(
    transaction: &Transaction<'_>,
    mut receipt: MaintenanceReceipt,
    results: &[CascadeArtifactDeleteResult],
    context: &CleanupExecutionContext<'_>,
) -> Result<MaintenanceReceipt, LogStoreError> {
    ensure_maintenance_active(context.control)?;
    let removed = remove_successful_artifact_pointers(transaction, results, context.control)?;
    receipt.executed.artifacts += removed;
    receipt.executed.database_rows += removed;
    receipt.artifact_deletion.removed += removed;
    let failed_targets = failed_cleanup_targets(results);
    receipt.artifact_deletion.failed =
        results.iter().filter(|result| !result.succeeded()).count() as u64;
    receipt.artifact_deletion.failure_class = results
        .iter()
        .find_map(CascadeArtifactDeleteResult::failure_class)
        .map(ArtifactDeletionFailureClass::from);

    let completed_targets = delete_reconciled_cleanup_targets(
        transaction,
        context.targets,
        &failed_targets,
        context.control,
    )?;
    add_counts(&mut receipt.executed, completed_targets);
    receipt.state = if receipt.artifact_deletion.failed > 0
        || receipt.has_more
        || receipt.executed != receipt.planned
    {
        MaintenanceReceiptState::Partial
    } else {
        MaintenanceReceiptState::Completed
    };
    ensure_maintenance_active(context.control)?;
    let execution_audit_id = Uuid::new_v4().to_string();
    receipt.execution_audit_id = Some(execution_audit_id.clone());
    update_maintenance_receipt(transaction, &receipt, context.occurred_at)?;
    ensure_maintenance_active(context.control)?;
    write_audit(
        transaction,
        context.occurred_at,
        &execution_audit_id,
        context.operation_id,
        "log_cleanup_execute",
        receipt.state.as_str(),
        context.reason,
    )?;
    Ok(receipt)
}

fn failed_cleanup_targets(results: &[CascadeArtifactDeleteResult]) -> HashSet<String> {
    results
        .iter()
        .filter(|result| !result.succeeded())
        .map(|result| result.pointer().request_id.clone())
        .collect()
}

fn delete_reconciled_cleanup_targets(
    transaction: &Transaction<'_>,
    targets: &[String],
    failed_targets: &HashSet<String>,
    control: &dyn MaintenanceExecutionControl,
) -> Result<MaintenanceCounts, LogStoreError> {
    let mut executed = MaintenanceCounts::default();
    for request_id in targets {
        ensure_maintenance_active(control)?;
        if failed_targets.contains(request_id) {
            continue;
        }
        if !terminal_request_artifacts(transaction, request_id)?.is_empty() {
            return Err(LogStoreError::MaintenanceOperationConflict);
        }
        let counts = count_terminal_request_owner(transaction, request_id)?;
        ensure_maintenance_active(control)?;
        let deleted = transaction
            .execute(
                "DELETE FROM summaries WHERE request_id = ?1 AND state IN ('completed', 'failed', 'rejected', 'cancelled', 'dropped')",
                [request_id],
            )
            .map_err(LogStoreError::Sqlite)?;
        if deleted == 1 {
            add_counts(&mut executed, counts);
        }
    }
    Ok(executed)
}

fn remove_successful_artifact_pointers(
    transaction: &Transaction<'_>,
    results: &[CascadeArtifactDeleteResult],
    control: &dyn MaintenanceExecutionControl,
) -> Result<u64, LogStoreError> {
    let mut removed = 0;
    for result in results.iter().filter(|result| result.succeeded()) {
        ensure_maintenance_active(control)?;
        let pointer = result.pointer();
        removed += transaction
            .execute(
                "DELETE FROM artifact_pointers WHERE artifact_id = ?1 AND request_id = ?2",
                rusqlite::params![pointer.artifact_id, pointer.request_id],
            )
            .map_err(LogStoreError::Sqlite)? as u64;
    }
    Ok(removed)
}

fn update_maintenance_receipt(
    transaction: &Transaction<'_>,
    receipt: &MaintenanceReceipt,
    occurred_at: &str,
) -> Result<(), LogStoreError> {
    transaction
        .execute(
            "UPDATE maintenance_operations SET state = ?2, executed_requests = ?3, executed_events = ?4, executed_artifacts = ?5, executed_proxy_records = ?6, executed_database_rows = ?7, artifact_files_removed = ?8, artifact_files_failed = ?9, artifact_file_failure_class = ?10, completed_at = ?11, execution_audit_id = ?12 WHERE operation_id = ?1",
            rusqlite::params![
                receipt.operation_id.to_string(),
                receipt.state.as_str(),
                receipt.executed.requests,
                receipt.executed.events,
                receipt.executed.artifacts,
                receipt.executed.proxy_records,
                receipt.executed.database_rows,
                receipt.artifact_deletion.removed,
                receipt.artifact_deletion.failed,
                receipt.artifact_deletion.failure_class.map(ArtifactDeletionFailureClass::as_str),
                occurred_at,
                receipt.execution_audit_id,
            ],
        )
        .map_err(LogStoreError::Sqlite)?;
    Ok(())
}

fn add_counts(target: &mut MaintenanceCounts, added: MaintenanceCounts) {
    target.requests += added.requests;
    target.events += added.events;
    target.artifacts += added.artifacts;
    target.proxy_records += added.proxy_records;
    target.database_rows += added.database_rows;
}

fn select_targets(
    transaction: &Transaction<'_>,
    scope: &CleanupScope,
) -> Result<(Vec<String>, bool), LogStoreError> {
    let filters = scope.filters();
    let mut sql = String::from(
        "SELECT request_id FROM summaries WHERE state IN ('completed', 'failed', 'rejected', 'cancelled', 'dropped') AND created_at < ?",
    );
    let mut parameters = vec![rusqlite::types::Value::Text(
        scope.cutoff_before.as_str().to_owned(),
    )];
    for (column, value) in [
        ("created_at >=", filters.from()),
        ("created_at <=", filters.to()),
        ("route =", filters.route()),
        ("model =", filters.model()),
        ("provider =", filters.provider()),
        ("engine =", filters.engine()),
    ] {
        if let Some(value) = value {
            sql.push_str(" AND ");
            sql.push_str(column);
            sql.push_str(" ?");
            parameters.push(rusqlite::types::Value::Text(value.to_owned()));
        }
    }
    if let Some(outcome) = filters.outcome() {
        sql.push_str(" AND state = ?");
        parameters.push(rusqlite::types::Value::Text(outcome.as_str().to_owned()));
    }
    sql.push_str(" ORDER BY created_at ASC, request_id ASC LIMIT ?");
    parameters.push(rusqlite::types::Value::Integer(
        i64::from(scope.request_limit) + 1,
    ));
    let mut statement = transaction.prepare(&sql).map_err(LogStoreError::Sqlite)?;
    let mut targets = statement
        .query_map(rusqlite::params_from_iter(parameters), |row| {
            row.get::<_, String>(0)
        })
        .map_err(LogStoreError::Sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(LogStoreError::Sqlite)?;
    let has_more = targets.len() > usize::from(scope.request_limit);
    targets.truncate(usize::from(scope.request_limit));
    Ok((targets, has_more))
}

fn count_targets(
    transaction: &Transaction<'_>,
    targets: &[String],
) -> Result<MaintenanceCounts, LogStoreError> {
    let mut counts = MaintenanceCounts::default();
    for request_id in targets {
        let events = count_child_rows(transaction, "lifecycle_events", request_id)?;
        let artifacts = count_child_rows(transaction, "artifact_pointers", request_id)?;
        let proxies = count_child_rows(transaction, "proxy_records", request_id)?;
        let exists = transaction
            .query_row(
                "SELECT 1 FROM summaries WHERE request_id = ?1",
                [request_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(LogStoreError::Sqlite)?
            .is_some();
        if exists {
            counts.requests += 1;
            counts.events += events;
            counts.artifacts += artifacts;
            counts.proxy_records += proxies;
            counts.database_rows += 1 + events + artifacts + proxies;
        }
    }
    Ok(counts)
}

fn count_child_rows(
    transaction: &Transaction<'_>,
    table: &'static str,
    request_id: &str,
) -> Result<u64, LogStoreError> {
    transaction
        .query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE request_id = ?1"),
            [request_id],
            |row| row.get::<_, u64>(0),
        )
        .map_err(LogStoreError::Sqlite)
}

fn persist_receipt(
    transaction: &Transaction<'_>,
    receipt: &MaintenanceReceipt,
    reason: &str,
    occurred_at: &str,
) -> Result<(), LogStoreError> {
    let completed_at = if matches!(receipt.state, MaintenanceReceiptState::Previewed) {
        None
    } else {
        Some(occurred_at)
    };
    transaction
        .execute(
            "INSERT INTO maintenance_operations (operation_id, action, cutoff_before, request_limit, reason, state, planned_requests, planned_events, planned_artifacts, planned_proxy_records, planned_database_rows, executed_requests, executed_events, executed_artifacts, executed_proxy_records, executed_database_rows, artifact_files_removed, artifact_files_failed, artifact_file_failure_class, has_more, created_at, completed_at, selection_fingerprint, preview_audit_id, execution_audit_id, cleanup_filters_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)",
            rusqlite::params![
                receipt.operation_id.to_string(),
                receipt.action.as_str(),
                receipt.scope.cutoff_before.as_str(),
                i64::from(receipt.scope.request_limit),
                reason,
                receipt.state.as_str(),
                receipt.planned.requests,
                receipt.planned.events,
                receipt.planned.artifacts,
                receipt.planned.proxy_records,
                receipt.planned.database_rows,
                receipt.executed.requests,
                receipt.executed.events,
                receipt.executed.artifacts,
                receipt.executed.proxy_records,
                receipt.executed.database_rows,
                receipt.artifact_deletion.removed,
                receipt.artifact_deletion.failed,
                receipt.artifact_deletion.failure_class.map(ArtifactDeletionFailureClass::as_str),
                receipt.has_more,
                occurred_at,
                completed_at,
                receipt.fingerprint.as_str(),
                receipt.preview_audit_id,
                receipt.execution_audit_id,
                serde_json::to_string(receipt.scope.filters()).map_err(|error| LogStoreError::QueryFailed(error.to_string()))?,
            ],
        )
        .map_err(LogStoreError::Sqlite)?;
    Ok(())
}

fn load_receipt(
    connection: &rusqlite::Connection,
    operation_id: MaintenanceOperationId,
) -> Result<Option<MaintenanceReceipt>, LogStoreError> {
    connection
        .query_row(
            "SELECT action, cutoff_before, request_limit, state, planned_requests, planned_events, planned_artifacts, planned_proxy_records, planned_database_rows, executed_requests, executed_events, executed_artifacts, executed_proxy_records, executed_database_rows, artifact_files_removed, artifact_files_failed, artifact_file_failure_class, has_more, selection_fingerprint, preview_audit_id, execution_audit_id, cleanup_filters_json FROM maintenance_operations WHERE operation_id = ?1",
            [operation_id.to_string()],
            |row| {
                let action: String = row.get(0)?;
                let cutoff: String = row.get(1)?;
                let request_limit: usize = row.get(2)?;
                let filters: CleanupFilters = serde_json::from_str(&row.get::<_, String>(21)?)
                    .map_err(|error| to_sql_error(LogStoreError::QueryFailed(error.to_string())))?;
                let scope = CleanupScope::new(
                    MaintenanceTimestamp::try_from(cutoff.as_str()).map_err(to_sql_error)?,
                    request_limit,
                )
                .map_err(to_sql_error)?
                .with_filters(filters);
                Ok(MaintenanceReceipt {
                    operation_id,
                    action: MaintenanceAction::from_str(&action).map_err(to_sql_error)?,
                    scope,
                    state: MaintenanceReceiptState::from_str(&row.get::<_, String>(3)?).map_err(to_sql_error)?,
                    planned: MaintenanceCounts {
                        requests: row.get(4)?, events: row.get(5)?, artifacts: row.get(6)?, proxy_records: row.get(7)?, database_rows: row.get(8)?,
                    },
                    executed: MaintenanceCounts { requests: row.get(9)?, events: row.get(10)?, artifacts: row.get(11)?, proxy_records: row.get(12)?, database_rows: row.get(13)? },
                    artifact_deletion: ArtifactDeletionProgress {
                        removed: row.get(14)?,
                        failed: row.get(15)?,
                        failure_class: row.get::<_, Option<String>>(16)?.map(|value| ArtifactDeletionFailureClass::from_str(&value)).transpose().map_err(to_sql_error)?,
                    },
                    has_more: row.get(17)?,
                    fingerprint: MaintenanceFingerprint(row.get(18)?),
                    preview_audit_id: row.get(19)?,
                    execution_audit_id: row.get(20)?,
                })
            },
        )
        .optional()
        .map_err(LogStoreError::Sqlite)
}

fn load_targets(
    transaction: &Transaction<'_>,
    operation_id: MaintenanceOperationId,
) -> Result<Vec<String>, LogStoreError> {
    let mut statement = transaction
        .prepare("SELECT request_id FROM maintenance_operation_targets WHERE operation_id = ?1 ORDER BY ordinal ASC")
        .map_err(LogStoreError::Sqlite)?;
    statement
        .query_map([operation_id.to_string()], |row| row.get(0))
        .map_err(LogStoreError::Sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(LogStoreError::Sqlite)
}

fn delete_request_owner(
    transaction: &Transaction<'_>,
    request_id: &str,
    control: &dyn MaintenanceExecutionControl,
) -> Result<(MaintenanceCounts, Vec<CascadeArtifactPointer>), LogStoreError> {
    ensure_maintenance_active(control)?;
    let counts = count_terminal_request_owner(transaction, request_id)?;
    let pointers = load_request_artifact_pointers(transaction, request_id)?;
    ensure_maintenance_active(control)?;
    let deleted = transaction
        .execute(
            "DELETE FROM summaries WHERE request_id = ?1 AND state IN ('completed', 'failed', 'rejected', 'cancelled', 'dropped')",
            [request_id],
        )
        .map_err(LogStoreError::Sqlite)?;
    Ok(if deleted == 1 {
        (counts, pointers)
    } else {
        (MaintenanceCounts::default(), Vec::new())
    })
}

fn count_terminal_request_owner(
    transaction: &Transaction<'_>,
    request_id: &str,
) -> Result<MaintenanceCounts, LogStoreError> {
    let terminal = transaction
        .query_row(
            "SELECT 1 FROM summaries WHERE request_id = ?1 AND state IN ('completed', 'failed', 'rejected', 'cancelled', 'dropped')",
            [request_id],
            |_| Ok(()),
        )
        .optional()
        .map_err(LogStoreError::Sqlite)?
        .is_some();
    if !terminal {
        return Ok(MaintenanceCounts::default());
    }
    count_targets(transaction, std::slice::from_ref(&request_id.to_owned()))
}

fn terminal_request_artifacts(
    transaction: &Transaction<'_>,
    request_id: &str,
) -> Result<Vec<CascadeArtifactPointer>, LogStoreError> {
    if count_terminal_request_owner(transaction, request_id)?.requests == 0 {
        return Ok(Vec::new());
    }
    load_request_artifact_pointers(transaction, request_id)
}

fn terminal_target_artifacts(
    transaction: &Transaction<'_>,
    targets: &[String],
) -> Result<Vec<CascadeArtifactPointer>, LogStoreError> {
    let mut pointers = Vec::new();
    for request_id in targets {
        pointers.extend(terminal_request_artifacts(transaction, request_id)?);
    }
    Ok(pointers)
}

fn load_request_artifact_pointers(
    transaction: &Transaction<'_>,
    request_id: &str,
) -> Result<Vec<CascadeArtifactPointer>, LogStoreError> {
    let mut statement = transaction
        .prepare(
            "SELECT artifact_id, request_id FROM artifact_pointers WHERE request_id = ?1 ORDER BY artifact_id ASC",
        )
        .map_err(LogStoreError::Sqlite)?;
    statement
        .query_map([request_id], |row| {
            Ok(CascadeArtifactPointer {
                artifact_id: row.get(0)?,
                request_id: row.get(1)?,
            })
        })
        .map_err(LogStoreError::Sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(LogStoreError::Sqlite)
}

fn ensure_same_intent(
    receipt: &MaintenanceReceipt,
    existing_reason: &str,
    request: &CleanupPreviewRequest,
) -> Result<(), LogStoreError> {
    if receipt.action != MaintenanceAction::Cleanup
        || receipt.scope != request.scope
        || existing_reason != request.reason.as_str()
    {
        return Err(LogStoreError::MaintenanceOperationConflict);
    }
    Ok(())
}

fn ensure_same_delete_one(
    transaction: &Transaction<'_>,
    receipt: &MaintenanceReceipt,
    request: &DeleteOneRequest,
) -> Result<(), LogStoreError> {
    let reason = load_reason(transaction, request.operation_id)?;
    let targets = load_targets(transaction, request.operation_id)?;
    if receipt.action != MaintenanceAction::DeleteOne
        || reason != request.reason.as_str()
        || targets != [request.request_id.clone()]
    {
        return Err(LogStoreError::MaintenanceOperationConflict);
    }
    Ok(())
}

fn delete_one_scope() -> Result<CleanupScope, LogStoreError> {
    CleanupScope::new(MaintenanceTimestamp::try_from(DELETE_ONE_SCOPE_CUTOFF)?, 1)
}

fn load_reason(
    transaction: &Transaction<'_>,
    operation_id: MaintenanceOperationId,
) -> Result<String, LogStoreError> {
    transaction
        .query_row(
            "SELECT reason FROM maintenance_operations WHERE operation_id = ?1",
            [operation_id.to_string()],
            |row| row.get(0),
        )
        .map_err(LogStoreError::Sqlite)
}

fn write_audit(
    transaction: &Transaction<'_>,
    occurred_at: &str,
    entry_id: &str,
    operation_id: MaintenanceOperationId,
    action: &'static str,
    result: &'static str,
    reason: &str,
) -> Result<(), LogStoreError> {
    let detail = serde_json::json!({
        "actor": "trusted_local_operator",
        "source": "logs_api",
        "result": result,
        "reason": reason,
        "operationId": operation_id.to_string(),
    })
    .to_string();
    transaction
        .execute(
            "INSERT INTO audit_entries (entry_id, request_id, occurred_at, actor, action, detail_json) VALUES (?1, NULL, ?2, ?3, ?4, ?5)",
            rusqlite::params![entry_id, occurred_at, "trusted_local_operator", action, detail],
        )
        .map_err(LogStoreError::Sqlite)?;
    Ok(())
}

fn to_sql_error(error: LogStoreError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn selection_fingerprint(
    action: MaintenanceAction,
    scope: &CleanupScope,
    targets: &[String],
) -> MaintenanceFingerprint {
    let mut hasher = Sha256::new();
    hasher.update(action.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(scope.cutoff_before.as_str().as_bytes());
    hasher.update([0]);
    hasher.update([scope.request_limit]);
    for value in [
        scope.filters.from(),
        scope.filters.to(),
        scope.filters.route(),
        scope.filters.model(),
        scope.filters.provider(),
        scope.filters.engine(),
        scope.filters.outcome().map(CleanupOutcome::as_str),
    ] {
        hasher.update([0]);
        if let Some(value) = value {
            hasher.update(value.as_bytes());
        }
    }
    for target in targets {
        hasher.update([0]);
        hasher.update(target.as_bytes());
    }
    MaintenanceFingerprint(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    };

    use super::*;
    use crate::RealClock;

    struct NeverCancelled;

    impl MaintenanceExecutionControl for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct Cancelled;

    impl MaintenanceExecutionControl for Cancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }

    #[derive(Default)]
    struct SwitchableCancellation(AtomicBool);

    impl SwitchableCancellation {
        fn cancel(&self) {
            self.0.store(true, Ordering::Release);
        }
    }

    impl MaintenanceExecutionControl for SwitchableCancellation {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::Acquire)
        }
    }

    fn block_first_artifact_removal(
        artifacts: &mut ArtifactFileStore,
    ) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>, Arc<AtomicUsize>) {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let release_rx = Arc::new(Mutex::new(release_rx));
        let calls = Arc::new(AtomicUsize::new(0));
        let removal_calls = Arc::clone(&calls);

        artifacts.set_remove_file_for_test(Arc::new(move |path| {
            if removal_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                started_tx
                    .send(())
                    .map_err(|_| std::io::Error::other("blocked remover start receiver dropped"))?;
                release_rx
                    .lock()
                    .expect("blocked remover release mutex poisoned")
                    .recv()
                    .map_err(|_| std::io::Error::other("blocked remover release sender dropped"))?;
            }
            std::fs::remove_file(path)
        }));

        (started_rx, release_tx, calls)
    }

    fn fixture() -> (tempfile::TempDir, ArtifactFileStore) {
        let root = tempfile::tempdir().expect("temporary root");
        let store = LogStore::open(root.path().join("db"), Arc::new(RealClock)).expect("store");
        let artifacts =
            ArtifactFileStore::open(root.path().join("artifacts"), Arc::new(RealClock), store)
                .expect("artifacts");
        (root, artifacts)
    }

    fn seed_terminal(store: &LogStore, request_id: &str, created_at: &str) {
        seed_terminal_with_metadata(
            store,
            request_id,
            created_at,
            "route",
            "model",
            "provider",
            "engine",
            "completed",
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn seed_terminal_with_metadata(
        store: &LogStore,
        request_id: &str,
        created_at: &str,
        route: &str,
        model: &str,
        provider: &str,
        engine: &str,
        outcome: &str,
    ) {
        store
            .insert_summary(
                request_id,
                Some(model),
                Some(route),
                Some(provider),
                Some(engine),
                created_at,
                None,
                None,
                None,
            )
            .expect("summary");
        store
            .conn()
            .execute(
                "UPDATE summaries SET state = ?2 WHERE request_id = ?1",
                rusqlite::params![request_id, outcome],
            )
            .expect("terminal");
    }

    fn audit_action_for(store: &LogStore, audit_id: &str) -> String {
        store
            .conn()
            .query_row(
                "SELECT action FROM audit_entries WHERE entry_id = ?1",
                [audit_id],
                |row| row.get(0),
            )
            .expect("durable audit entry")
    }

    fn request(id: u128, cutoff: &str) -> CleanupPreviewRequest {
        request_with_limit(id, cutoff, 1)
    }

    fn request_with_limit(id: u128, cutoff: &str, request_limit: usize) -> CleanupPreviewRequest {
        CleanupPreviewRequest {
            operation_id: MaintenanceOperationId::new(Uuid::from_u128(id)),
            scope: CleanupScope::new(
                MaintenanceTimestamp::try_from(cutoff).expect("cutoff"),
                request_limit,
            )
            .expect("scope"),
            reason: MaintenanceReason::try_from("operator cleanup").expect("reason"),
        }
    }

    fn delete_request(id: u128, request_id: &str) -> DeleteOneRequest {
        DeleteOneRequest::new(
            MaintenanceOperationId::new(Uuid::from_u128(id)),
            request_id,
            MaintenanceReason::try_from("operator delete").expect("reason"),
        )
        .expect("delete request")
    }

    #[test]
    fn preview_snapshots_bounded_targets_and_rejects_invalid_typed_input() {
        let (_root, artifacts) = fixture();
        let store = artifacts.store_ref();
        seed_terminal(store, "old-a", "2025-01-01T00:00:00Z");
        seed_terminal(store, "old-b", "2025-01-02T00:00:00Z");
        let request = request(1, "2025-02-01T00:00:00Z");
        let receipt = store
            .preview_cleanup(&request, &NeverCancelled)
            .expect("preview");
        let replay = store
            .preview_cleanup(&request, &NeverCancelled)
            .expect("preview replay");
        assert_eq!(receipt.planned.requests, 1);
        assert!(receipt.has_more);
        assert_eq!(receipt.state, MaintenanceReceiptState::Previewed);
        assert_eq!(replay, receipt);
        let audit_id = receipt
            .preview_audit_id
            .as_deref()
            .expect("preview receipt audit ID");
        assert_eq!(audit_action_for(store, audit_id), "log_cleanup_preview");
        assert!(MaintenanceReason::try_from("\n").is_err());
        assert!(
            CleanupScope::new(
                MaintenanceTimestamp::try_from("2025-01-01T00:00:00Z").unwrap(),
                101
            )
            .is_err()
        );
    }

    #[test]
    fn preview_filters_terminal_ledger_scope_and_rejects_changed_replay() {
        let (_root, artifacts) = fixture();
        let store = artifacts.store_ref();
        let matching = (
            "matching",
            "2025-01-02T00:00:00Z",
            "route-a",
            "model-a",
            "provider-a",
            "engine-a",
            "completed",
        );
        let candidates = [
            matching,
            (
                "before-from",
                "2024-12-31T23:59:59Z",
                "route-a",
                "model-a",
                "provider-a",
                "engine-a",
                "completed",
            ),
            (
                "after-to",
                "2025-02-01T00:00:01Z",
                "route-a",
                "model-a",
                "provider-a",
                "engine-a",
                "completed",
            ),
            (
                "other-route",
                "2025-01-02T00:00:00Z",
                "route-b",
                "model-a",
                "provider-a",
                "engine-a",
                "completed",
            ),
            (
                "other-model",
                "2025-01-02T00:00:00Z",
                "route-a",
                "model-b",
                "provider-a",
                "engine-a",
                "completed",
            ),
            (
                "other-provider",
                "2025-01-02T00:00:00Z",
                "route-a",
                "model-a",
                "provider-b",
                "engine-a",
                "completed",
            ),
            (
                "other-engine",
                "2025-01-02T00:00:00Z",
                "route-a",
                "model-a",
                "provider-a",
                "engine-b",
                "completed",
            ),
            (
                "other-outcome",
                "2025-01-02T00:00:00Z",
                "route-a",
                "model-a",
                "provider-a",
                "engine-a",
                "failed",
            ),
        ];
        for (request_id, created_at, route, model, provider, engine, outcome) in candidates {
            seed_terminal_with_metadata(
                store, request_id, created_at, route, model, provider, engine, outcome,
            );
        }
        store
            .insert_summary(
                "active-match",
                Some("model-a"),
                Some("route-a"),
                Some("provider-a"),
                Some("engine-a"),
                "2025-01-02T00:00:00Z",
                None,
                None,
                None,
            )
            .expect("active summary");
        let filters = CleanupFilters::new(
            Some(MaintenanceTimestamp::try_from("2025-01-01T00:00:00Z").expect("from")),
            Some(MaintenanceTimestamp::try_from("2025-02-01T00:00:00Z").expect("to")),
            Some("route-a".to_owned()),
            Some("model-a".to_owned()),
            Some("provider-a".to_owned()),
            Some("engine-a".to_owned()),
            Some(CleanupOutcome::Completed),
        )
        .expect("filters");
        let request = CleanupPreviewRequest {
            operation_id: MaintenanceOperationId::new(Uuid::from_u128(0x50)),
            scope: CleanupScope::new(
                MaintenanceTimestamp::try_from("2025-03-01T00:00:00Z").expect("cutoff"),
                10,
            )
            .expect("scope")
            .with_filters(filters),
            reason: MaintenanceReason::try_from("operator cleanup").expect("reason"),
        };
        let receipt = store
            .preview_cleanup(&request, &NeverCancelled)
            .expect("filtered preview");
        assert_eq!(receipt.planned.requests, 1);
        assert_eq!(receipt.scope.filters().route(), Some("route-a"));
        assert_eq!(
            receipt.scope.filters().outcome(),
            Some(CleanupOutcome::Completed)
        );
        assert_eq!(
            store
                .conn()
                .query_row(
                    "SELECT request_id FROM maintenance_operation_targets WHERE operation_id = ?1",
                    [request.operation_id.to_string()],
                    |row| row.get::<_, String>(0),
                )
                .expect("exact target"),
            "matching"
        );
        assert_eq!(
            store
                .preview_cleanup(&request, &NeverCancelled)
                .expect("same scope replay"),
            receipt
        );
        let changed_scope = CleanupPreviewRequest {
            scope: request.scope.clone().with_filters(
                CleanupFilters::new(
                    Some(MaintenanceTimestamp::try_from("2025-01-01T00:00:00Z").expect("from")),
                    Some(MaintenanceTimestamp::try_from("2025-02-01T00:00:00Z").expect("to")),
                    Some("route-a".to_owned()),
                    Some("model-b".to_owned()),
                    Some("provider-a".to_owned()),
                    Some("engine-a".to_owned()),
                    Some(CleanupOutcome::Completed),
                )
                .expect("changed filters"),
            ),
            ..request.clone()
        };
        assert!(matches!(
            store.preview_cleanup(&changed_scope, &NeverCancelled),
            Err(LogStoreError::MaintenanceOperationConflict)
        ));
        assert!(
            CleanupFilters::new(
                None,
                None,
                Some("/private/model?token=secret".to_owned()),
                None,
                None,
                None,
                None,
            )
            .is_err()
        );
        assert!(CleanupOutcome::try_from("active").is_err());
    }

    #[test]
    fn execute_replays_completed_receipt_and_cascades_artifact_owner() {
        let (_root, artifacts) = fixture();
        let store = artifacts.store_ref();
        seed_terminal(store, "old-artifact", "2025-01-01T00:00:00Z");
        artifacts
            .write_artifact(
                "artifact-1",
                "old-artifact",
                "response",
                "2025-01-01T00:00:01Z",
                b"redacted",
                None,
                1,
                true,
                false,
                128,
                128,
            )
            .expect("artifact");
        let request = request(2, "2025-02-01T00:00:00Z");
        let preview = store
            .preview_cleanup(&request, &NeverCancelled)
            .expect("preview");
        let first = artifacts
            .execute_cleanup(request.operation_id, &request.reason, &NeverCancelled)
            .expect("execute");
        let replay = artifacts
            .execute_cleanup(request.operation_id, &request.reason, &NeverCancelled)
            .expect("replay");
        assert_eq!(first, replay);
        assert_eq!(first.state, MaintenanceReceiptState::Completed);
        assert_eq!(first.planned, preview.planned);
        assert_eq!(first.executed.requests, 1);
        assert_eq!(store.query_request("old-artifact").unwrap(), None);
        assert!(artifacts.read_artifact("artifact-1").is_err());
        let preview_audit_id = preview
            .preview_audit_id
            .as_deref()
            .expect("preview receipt audit ID");
        let execute_audit_id = first
            .execution_audit_id
            .as_deref()
            .expect("execute receipt audit ID");
        assert_ne!(preview_audit_id, execute_audit_id);
        assert_eq!(
            audit_action_for(store, execute_audit_id),
            "log_cleanup_execute"
        );
        let execute_audits: i64 = store
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM audit_entries WHERE action = 'log_cleanup_execute'",
                [],
                |row| row.get(0),
            )
            .expect("audits");
        assert_eq!(execute_audits, 1);
        let detail: String = store
            .conn()
            .query_row(
                "SELECT detail_json FROM audit_entries WHERE action = 'log_cleanup_execute'",
                [],
                |row| row.get(0),
            )
            .expect("audit detail");
        assert!(detail.contains("trusted_local_operator") && detail.contains("logs_api"));
        assert!(detail.contains(&request.operation_id.to_string()));
    }

    #[test]
    fn cleanup_retains_only_failed_selected_owners_then_retries_exact_targets() {
        let (_root, mut artifacts) = fixture();
        let first_request = "00000000-0000-4000-8000-000000000121";
        let failed_request = "00000000-0000-4000-8000-000000000122";
        let unrelated_request = "00000000-0000-4000-8000-000000000123";
        let first_artifact = "00000000-0000-4000-8000-000000000221";
        let failed_artifact = "00000000-0000-4000-8000-000000000222";
        let unrelated_artifact = "00000000-0000-4000-8000-000000000223";
        seed_terminal(artifacts.store_ref(), first_request, "2025-01-01T00:00:00Z");
        seed_terminal(
            artifacts.store_ref(),
            failed_request,
            "2025-01-02T00:00:00Z",
        );
        seed_terminal(
            artifacts.store_ref(),
            unrelated_request,
            "2025-03-03T00:00:00Z",
        );
        write_delete_artifact(&artifacts, first_artifact, first_request);
        write_delete_artifact(&artifacts, failed_artifact, failed_request);
        write_delete_artifact(&artifacts, unrelated_artifact, unrelated_request);
        let request = request_with_limit(16, "2025-02-01T00:00:00Z", 2);
        artifacts
            .store_ref()
            .preview_cleanup(&request, &NeverCancelled)
            .expect("preview");
        fail_artifact_removal(&mut artifacts, failed_artifact);

        let partial = artifacts
            .execute_cleanup(request.operation_id, &request.reason, &NeverCancelled)
            .expect("partial cleanup");
        assert_eq!(partial.state, MaintenanceReceiptState::Partial);
        assert_eq!(partial.artifact_deletion.removed, 1);
        assert_eq!(partial.artifact_deletion.failed, 1);
        assert_eq!(
            partial.artifact_deletion.failure_class,
            Some(ArtifactDeletionFailureClass::Io)
        );
        assert!(
            artifacts
                .store_ref()
                .query_request(first_request)
                .unwrap()
                .is_none()
        );
        assert!(
            artifacts
                .store_ref()
                .query_request(failed_request)
                .unwrap()
                .is_some()
        );
        assert!(
            artifacts
                .store_ref()
                .query_artifact(failed_artifact)
                .unwrap()
                .is_some()
        );
        assert!(
            artifacts
                .store_ref()
                .query_request(unrelated_request)
                .unwrap()
                .is_some()
        );
        assert!(!format!("{partial:?}").contains("artifacts/"));

        restore_artifact_removal(&mut artifacts);
        let completed = artifacts
            .execute_cleanup(request.operation_id, &request.reason, &NeverCancelled)
            .expect("retry cleanup");
        let replay = artifacts
            .execute_cleanup(request.operation_id, &request.reason, &NeverCancelled)
            .expect("completed replay");
        assert_eq!(completed, replay);
        assert_eq!(completed.state, MaintenanceReceiptState::Completed);
        assert_eq!(completed.executed, completed.planned);
        assert_eq!(completed.artifact_deletion.removed, 2);
        assert_eq!(completed.artifact_deletion.failed, 0);
        assert_ne!(partial.execution_audit_id, completed.execution_audit_id);
        assert_eq!(replay.execution_audit_id, completed.execution_audit_id);
        assert_eq!(
            audit_action_for(
                artifacts.store_ref(),
                completed
                    .execution_audit_id
                    .as_deref()
                    .expect("retry cleanup audit ID"),
            ),
            "log_cleanup_execute"
        );
        assert!(
            artifacts
                .store_ref()
                .query_request(failed_request)
                .unwrap()
                .is_none()
        );
        assert!(
            artifacts
                .store_ref()
                .query_artifact(unrelated_artifact)
                .unwrap()
                .is_some()
        );
        assert_cleanup_execute_audits(&artifacts, 2);
    }

    #[test]
    fn cleanup_request_limit_partial_replays_without_retrying_later_targets() {
        let (_root, artifacts) = fixture();
        let first_request = "00000000-0000-4000-8000-000000000124";
        let later_request = "00000000-0000-4000-8000-000000000125";
        seed_terminal(artifacts.store_ref(), first_request, "2025-01-01T00:00:00Z");
        seed_terminal(artifacts.store_ref(), later_request, "2025-01-02T00:00:00Z");
        let request = request_with_limit(17, "2025-02-01T00:00:00Z", 1);
        let preview = artifacts
            .store_ref()
            .preview_cleanup(&request, &NeverCancelled)
            .expect("preview");
        assert!(preview.has_more);
        let partial = artifacts
            .execute_cleanup(request.operation_id, &request.reason, &NeverCancelled)
            .expect("limited cleanup");
        let replay = artifacts
            .execute_cleanup(request.operation_id, &request.reason, &NeverCancelled)
            .expect("limited cleanup replay");
        assert_eq!(partial, replay);
        assert_eq!(partial.state, MaintenanceReceiptState::Partial);
        assert_eq!(partial.artifact_deletion.failed, 0);
        assert_eq!(partial.executed, partial.planned);
        assert!(
            artifacts
                .store_ref()
                .query_request(first_request)
                .unwrap()
                .is_none()
        );
        assert!(
            artifacts
                .store_ref()
                .query_request(later_request)
                .unwrap()
                .is_some()
        );
        assert_cleanup_execute_audits(&artifacts, 1);
    }

    #[test]
    fn cleanup_reconciles_missing_and_corrupt_pointers_without_file_paths() {
        let (root, artifacts) = fixture();
        let request_id = "00000000-0000-4000-8000-000000000126";
        let missing_artifact = "00000000-0000-4000-8000-000000000224";
        let corrupt_artifact = "00000000-0000-4000-8000-000000000225";
        seed_terminal(artifacts.store_ref(), request_id, "2025-01-01T00:00:00Z");
        write_delete_artifact(&artifacts, missing_artifact, request_id);
        write_delete_artifact(&artifacts, corrupt_artifact, request_id);
        std::fs::remove_file(
            root.path()
                .join("artifacts")
                .join(request_id)
                .join(missing_artifact),
        )
        .expect("remove backing file");
        std::fs::write(
            root.path()
                .join("artifacts")
                .join(request_id)
                .join(corrupt_artifact),
            b"checksum changed",
        )
        .expect("corrupt backing file");
        let request = request_with_limit(18, "2025-02-01T00:00:00Z", 1);
        artifacts
            .store_ref()
            .preview_cleanup(&request, &NeverCancelled)
            .expect("preview");
        let receipt = artifacts
            .execute_cleanup(request.operation_id, &request.reason, &NeverCancelled)
            .expect("reconcile cleanup");
        assert_eq!(receipt.state, MaintenanceReceiptState::Completed);
        assert_eq!(receipt.executed, receipt.planned);
        assert_eq!(receipt.artifact_deletion.removed, 2);
        assert_eq!(receipt.artifact_deletion.failed, 0);
        assert!(!format!("{receipt:?}").contains(&*root.path().to_string_lossy()));
    }

    fn write_delete_artifact(artifacts: &ArtifactFileStore, artifact_id: &str, request_id: &str) {
        artifacts
            .write_artifact(
                artifact_id,
                request_id,
                "response",
                "2025-01-01T00:00:01Z",
                b"redacted",
                None,
                1,
                true,
                false,
                128,
                128,
            )
            .expect("artifact");
    }

    fn fail_artifact_removal(artifacts: &mut ArtifactFileStore, artifact_id: &str) {
        let fail_id = artifact_id.to_owned();
        artifacts.set_remove_file_for_test(Arc::new(move |path| {
            if path.file_name().and_then(|name| name.to_str()) == Some(fail_id.as_str()) {
                Err(std::io::Error::other("injected artifact removal failure"))
            } else {
                std::fs::remove_file(path)
            }
        }));
    }

    fn restore_artifact_removal(artifacts: &mut ArtifactFileStore) {
        artifacts.set_remove_file_for_test(Arc::new(|path: &std::path::Path| {
            std::fs::remove_file(path)
        }));
    }

    fn assert_cleanup_execute_audits(artifacts: &ArtifactFileStore, expected: i64) {
        let audit_count: i64 = artifacts
            .store_ref()
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM audit_entries WHERE action = 'log_cleanup_execute'",
                [],
                |row| row.get(0),
            )
            .expect("cleanup audit count");
        assert_eq!(audit_count, expected);
    }

    fn assert_partial_delete_state(
        root: &tempfile::TempDir,
        artifacts: &ArtifactFileStore,
        request_id: &str,
        successful_artifact: &str,
        failed_artifact: &str,
        unrelated_artifact: &str,
        receipt: &MaintenanceReceipt,
    ) {
        assert_eq!(receipt.state, MaintenanceReceiptState::Partial);
        assert_eq!(receipt.planned.artifacts, 2);
        assert_eq!(receipt.executed.artifacts, 1);
        assert_eq!(receipt.artifact_deletion.removed, 1);
        assert_eq!(receipt.artifact_deletion.failed, 1);
        assert_eq!(
            receipt.artifact_deletion.failure_class,
            Some(ArtifactDeletionFailureClass::Io)
        );
        assert!(
            artifacts
                .store_ref()
                .query_request(request_id)
                .unwrap()
                .is_some()
        );
        assert!(
            artifacts
                .store_ref()
                .query_artifact(successful_artifact)
                .unwrap()
                .is_none()
        );
        assert!(
            artifacts
                .store_ref()
                .query_artifact(failed_artifact)
                .unwrap()
                .is_some()
        );
        assert!(
            artifacts
                .store_ref()
                .query_artifact(unrelated_artifact)
                .unwrap()
                .is_some()
        );
        let artifact_root = root.path().join("artifacts");
        assert!(
            !artifact_root
                .join(request_id)
                .join(successful_artifact)
                .exists()
        );
        assert!(
            artifact_root
                .join(request_id)
                .join(failed_artifact)
                .exists()
        );
        assert!(!format!("{receipt:?}").contains(&*root.path().to_string_lossy()));
    }

    fn assert_completed_delete_state(
        artifacts: &ArtifactFileStore,
        request_id: &str,
        failed_artifact: &str,
        unrelated_request: &str,
        unrelated_artifact: &str,
        completed: &MaintenanceReceipt,
        replay: &MaintenanceReceipt,
    ) {
        assert_eq!(completed, replay);
        assert_eq!(completed.state, MaintenanceReceiptState::Completed);
        assert_eq!(completed.executed, completed.planned);
        assert_eq!(completed.artifact_deletion.removed, 2);
        assert_eq!(completed.artifact_deletion.failed, 0);
        assert_eq!(completed.artifact_deletion.failure_class, None);
        assert!(
            artifacts
                .store_ref()
                .query_request(request_id)
                .unwrap()
                .is_none()
        );
        assert!(
            artifacts
                .store_ref()
                .query_artifact(failed_artifact)
                .unwrap()
                .is_none()
        );
        assert!(
            artifacts
                .store_ref()
                .query_request(unrelated_request)
                .unwrap()
                .is_some()
        );
        assert!(
            artifacts
                .store_ref()
                .query_artifact(unrelated_artifact)
                .unwrap()
                .is_some()
        );
        let audit_count: i64 = artifacts
            .store_ref()
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM audit_entries WHERE action = 'log_delete_request'",
                [],
                |row| row.get(0),
            )
            .expect("audit count");
        assert_eq!(audit_count, 2, "completed replay must not add an audit");
    }

    #[test]
    fn cancelled_execute_leaves_preview_targets_untouched() {
        let (_root, artifacts) = fixture();
        let store = artifacts.store_ref();
        seed_terminal(store, "old-cancelled", "2025-01-01T00:00:00Z");
        let cancelled_preview = request(4, "2025-02-01T00:00:00Z");
        assert!(matches!(
            store.preview_cleanup(&cancelled_preview, &Cancelled),
            Err(LogStoreError::MaintenanceExecutionCancelled)
        ));
        assert_eq!(
            store
                .count_table("maintenance_operations")
                .expect("operations"),
            0
        );
        let request = request(3, "2025-02-01T00:00:00Z");
        let preview = store
            .preview_cleanup(&request, &NeverCancelled)
            .expect("preview");
        assert!(matches!(
            artifacts.execute_cleanup(request.operation_id, &request.reason, &Cancelled),
            Err(LogStoreError::MaintenanceExecutionCancelled)
        ));
        assert!(store.query_request("old-cancelled").unwrap().is_some());
        let replayed_preview = store
            .preview_cleanup(&request, &NeverCancelled)
            .expect("preview replay");
        assert_eq!(replayed_preview, preview);
        assert_eq!(replayed_preview.state, MaintenanceReceiptState::Previewed);
        let execute_audits: i64 = store
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM audit_entries WHERE action = 'log_cleanup_execute'",
                [],
                |row| row.get(0),
            )
            .expect("execute audit count");
        assert_eq!(
            execute_audits, 0,
            "cancelled execution must not orphan an audit"
        );
    }

    #[test]
    fn cancellation_during_cleanup_removal_keeps_the_preview_retryable() {
        let (root, mut artifacts) = fixture();
        let request_id = "00000000-0000-4000-8000-000000000127";
        let first_artifact = "00000000-0000-4000-8000-000000000226";
        let second_artifact = "00000000-0000-4000-8000-000000000227";
        seed_terminal(artifacts.store_ref(), request_id, "2025-01-01T00:00:00Z");
        write_delete_artifact(&artifacts, first_artifact, request_id);
        write_delete_artifact(&artifacts, second_artifact, request_id);
        let request = request(19, "2025-02-01T00:00:00Z");
        artifacts
            .store_ref()
            .preview_cleanup(&request, &NeverCancelled)
            .expect("preview cleanup");
        let (started, release, calls) = block_first_artifact_removal(&mut artifacts);
        let control = SwitchableCancellation::default();

        let cancelled = std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                artifacts.execute_cleanup(request.operation_id, &request.reason, &control)
            });
            started.recv().expect("first removal started");
            control.cancel();
            release.send(()).expect("release first removal");
            worker.join().expect("cleanup worker does not panic")
        });

        assert!(matches!(
            cancelled,
            Err(LogStoreError::MaintenanceExecutionCancelled)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            !root
                .path()
                .join("artifacts")
                .join(request_id)
                .join(first_artifact)
                .exists()
        );
        assert!(
            root.path()
                .join("artifacts")
                .join(request_id)
                .join(second_artifact)
                .exists()
        );
        assert!(
            artifacts
                .store_ref()
                .query_request(request_id)
                .expect("request remains")
                .is_some()
        );
        assert!(
            artifacts
                .store_ref()
                .query_artifact(first_artifact)
                .expect("first pointer remains")
                .is_some()
        );
        assert!(
            artifacts
                .store_ref()
                .query_artifact(second_artifact)
                .expect("second pointer remains")
                .is_some()
        );
        let pending = artifacts
            .store_ref()
            .preview_cleanup(&request, &NeverCancelled)
            .expect("preview remains replayable");
        assert_eq!(pending.state, MaintenanceReceiptState::Previewed);
        assert_eq!(pending.execution_audit_id, None);
        assert_cleanup_execute_audits(&artifacts, 0);

        let completed = artifacts
            .execute_cleanup(request.operation_id, &request.reason, &NeverCancelled)
            .expect("retry cleanup");
        assert_eq!(completed.state, MaintenanceReceiptState::Completed);
        assert_eq!(completed.executed, completed.planned);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            artifacts
                .store_ref()
                .query_request(request_id)
                .expect("request reconciled")
                .is_none()
        );
        assert_cleanup_execute_audits(&artifacts, 1);
    }

    #[test]
    fn delete_one_cascades_owned_artifact_and_replays_its_completed_receipt() {
        let (root, artifacts) = fixture();
        let store = artifacts.store_ref();
        let request_id = "00000000-0000-4000-8000-000000000101";
        let artifact_id = "00000000-0000-4000-8000-000000000201";
        seed_terminal(store, request_id, "2025-01-01T00:00:00Z");
        artifacts
            .write_artifact(
                artifact_id,
                request_id,
                "response",
                "2025-01-01T00:00:01Z",
                b"redacted",
                None,
                1,
                true,
                false,
                128,
                128,
            )
            .expect("artifact");
        let request = delete_request(10, request_id);
        let first = artifacts
            .delete_request_cascade(&request, &NeverCancelled)
            .expect("delete");
        let replay = artifacts
            .delete_request_cascade(&request, &NeverCancelled)
            .expect("replay");

        assert_eq!(first, replay);
        assert_eq!(first.action, MaintenanceAction::DeleteOne);
        assert_eq!(first.state, MaintenanceReceiptState::Completed);
        assert_eq!(first.planned.requests, 1);
        assert_eq!(first.executed, first.planned);
        assert!(store.query_request(request_id).unwrap().is_none());
        assert!(artifacts.read_artifact(artifact_id).is_err());
        assert!(
            !root
                .path()
                .join("artifacts")
                .join(request_id)
                .join(artifact_id)
                .exists()
        );
        let audit_id = first
            .execution_audit_id
            .as_deref()
            .expect("delete receipt audit ID");
        assert_eq!(audit_action_for(store, audit_id), "log_delete_request");
        let audit_count: i64 = store
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM audit_entries WHERE action = 'log_delete_request'",
                [],
                |row| row.get(0),
            )
            .expect("audit count");
        assert_eq!(audit_count, 1);
        let detail: String = store
            .conn()
            .query_row(
                "SELECT detail_json FROM audit_entries WHERE action = 'log_delete_request'",
                [],
                |row| row.get(0),
            )
            .expect("audit detail");
        assert!(detail.contains("trusted_local_operator") && detail.contains("logs_api"));
        assert!(detail.contains(&request.operation_id.to_string()));
    }

    #[test]
    fn delete_one_retains_failed_artifacts_and_same_operation_retry_reconciles_them() {
        let (root, mut artifacts) = fixture();
        let request_id = "00000000-0000-4000-8000-000000000111";
        let successful_artifact = "00000000-0000-4000-8000-000000000211";
        let failed_artifact = "00000000-0000-4000-8000-000000000212";
        let unrelated_request = "00000000-0000-4000-8000-000000000112";
        let unrelated_artifact = "00000000-0000-4000-8000-000000000213";
        seed_terminal(artifacts.store_ref(), request_id, "2025-01-01T00:00:00Z");
        seed_terminal(
            artifacts.store_ref(),
            unrelated_request,
            "2025-01-02T00:00:00Z",
        );
        write_delete_artifact(&artifacts, successful_artifact, request_id);
        write_delete_artifact(&artifacts, failed_artifact, request_id);
        write_delete_artifact(&artifacts, unrelated_artifact, unrelated_request);
        fail_artifact_removal(&mut artifacts, failed_artifact);

        let request = delete_request(14, request_id);
        let partial = artifacts
            .delete_request_cascade(&request, &NeverCancelled)
            .expect("partial delete");
        assert_partial_delete_state(
            &root,
            &artifacts,
            request_id,
            successful_artifact,
            failed_artifact,
            unrelated_artifact,
            &partial,
        );

        restore_artifact_removal(&mut artifacts);
        let completed = artifacts
            .delete_request_cascade(&request, &NeverCancelled)
            .expect("retry succeeds");
        let replay = artifacts
            .delete_request_cascade(&request, &NeverCancelled)
            .expect("completed replay");
        assert_completed_delete_state(
            &artifacts,
            request_id,
            failed_artifact,
            unrelated_request,
            unrelated_artifact,
            &completed,
            &replay,
        );
        assert_ne!(partial.execution_audit_id, completed.execution_audit_id);
        assert_eq!(replay.execution_audit_id, completed.execution_audit_id);
        assert_eq!(
            audit_action_for(
                artifacts.store_ref(),
                completed
                    .execution_audit_id
                    .as_deref()
                    .expect("retry delete audit ID"),
            ),
            "log_delete_request"
        );
    }

    #[test]
    fn delete_one_reconciles_missing_and_corrupt_artifact_pointers_without_path_leakage() {
        let (root, artifacts) = fixture();
        let request_id = "00000000-0000-4000-8000-000000000113";
        let missing_artifact = "00000000-0000-4000-8000-000000000214";
        let corrupt_artifact = "00000000-0000-4000-8000-000000000215";
        seed_terminal(artifacts.store_ref(), request_id, "2025-01-01T00:00:00Z");
        for artifact_id in [missing_artifact, corrupt_artifact] {
            artifacts
                .write_artifact(
                    artifact_id,
                    request_id,
                    "response",
                    "2025-01-01T00:00:01Z",
                    b"redacted",
                    None,
                    1,
                    true,
                    false,
                    128,
                    128,
                )
                .expect("artifact");
        }
        let missing_path = root
            .path()
            .join("artifacts")
            .join(request_id)
            .join(missing_artifact);
        std::fs::remove_file(&missing_path).expect("remove backing file");
        let corrupt_path = root
            .path()
            .join("artifacts")
            .join(request_id)
            .join(corrupt_artifact);
        std::fs::write(&corrupt_path, b"changed-after-checksum").expect("corrupt backing file");

        let receipt = artifacts
            .delete_request_cascade(&delete_request(15, request_id), &NeverCancelled)
            .expect("reconcile missing and corrupt pointers");
        assert_eq!(receipt.state, MaintenanceReceiptState::Completed);
        assert_eq!(receipt.executed, receipt.planned);
        assert_eq!(receipt.artifact_deletion.removed, 2);
        assert_eq!(receipt.artifact_deletion.failed, 0);
        assert_eq!(receipt.artifact_deletion.failure_class, None);
        assert!(
            artifacts
                .store_ref()
                .query_request(request_id)
                .unwrap()
                .is_none()
        );
        assert!(
            artifacts
                .store_ref()
                .query_artifact(missing_artifact)
                .unwrap()
                .is_none()
        );
        assert!(
            artifacts
                .store_ref()
                .query_artifact(corrupt_artifact)
                .unwrap()
                .is_none()
        );
        assert!(!format!("{receipt:?}").contains(&*root.path().to_string_lossy()));
    }

    #[test]
    fn delete_one_missing_request_is_a_stable_completed_noop() {
        let (_root, artifacts) = fixture();
        let missing_id = "00000000-0000-4000-8000-000000000102";
        let request = delete_request(11, missing_id);
        let first = artifacts
            .delete_request_cascade(&request, &NeverCancelled)
            .expect("missing delete");
        let replay = artifacts
            .delete_request_cascade(&request, &NeverCancelled)
            .expect("missing replay");
        assert_eq!(first, replay);
        assert_eq!(first.action, MaintenanceAction::DeleteOne);
        assert_eq!(first.planned, MaintenanceCounts::default());
        assert_eq!(first.executed, MaintenanceCounts::default());
    }

    #[test]
    fn delete_one_never_removes_an_active_request() {
        let (_root, artifacts) = fixture();
        let store = artifacts.store_ref();
        let request_id = "00000000-0000-4000-8000-000000000104";
        store
            .insert_summary(
                request_id,
                Some("model"),
                Some("route"),
                None,
                None,
                "2025-01-01T00:00:00Z",
                None,
                None,
                None,
            )
            .expect("active summary");
        let request = delete_request(13, request_id);
        let receipt = artifacts
            .delete_request_cascade(&request, &NeverCancelled)
            .expect("active no-op");
        assert_eq!(receipt.planned, MaintenanceCounts::default());
        assert_eq!(receipt.executed, MaintenanceCounts::default());
        assert!(store.query_request(request_id).unwrap().is_some());
    }

    #[test]
    fn cancelled_delete_one_leaves_no_receipt_and_can_be_retried() {
        let (_root, artifacts) = fixture();
        let store = artifacts.store_ref();
        let request_id = "00000000-0000-4000-8000-000000000103";
        seed_terminal(store, request_id, "2025-01-01T00:00:00Z");
        let request = delete_request(12, request_id);
        assert!(matches!(
            artifacts.delete_request_cascade(&request, &Cancelled),
            Err(LogStoreError::MaintenanceExecutionCancelled)
        ));
        assert!(store.query_request(request_id).unwrap().is_some());
        assert_eq!(
            store
                .count_table("maintenance_operations")
                .expect("operations"),
            0
        );
        assert_eq!(
            artifacts
                .delete_request_cascade(&request, &NeverCancelled)
                .expect("retry")
                .state,
            MaintenanceReceiptState::Completed
        );
    }

    #[test]
    fn cancellation_during_delete_removal_leaves_no_receipt_and_can_be_retried() {
        let (root, mut artifacts) = fixture();
        let request_id = "00000000-0000-4000-8000-000000000128";
        let first_artifact = "00000000-0000-4000-8000-000000000228";
        let second_artifact = "00000000-0000-4000-8000-000000000229";
        seed_terminal(artifacts.store_ref(), request_id, "2025-01-01T00:00:00Z");
        write_delete_artifact(&artifacts, first_artifact, request_id);
        write_delete_artifact(&artifacts, second_artifact, request_id);
        let request = delete_request(20, request_id);
        let (started, release, calls) = block_first_artifact_removal(&mut artifacts);
        let control = SwitchableCancellation::default();

        let cancelled = std::thread::scope(|scope| {
            let worker = scope.spawn(|| artifacts.delete_request_cascade(&request, &control));
            started.recv().expect("first removal started");
            control.cancel();
            release.send(()).expect("release first removal");
            worker.join().expect("delete worker does not panic")
        });

        assert!(matches!(
            cancelled,
            Err(LogStoreError::MaintenanceExecutionCancelled)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            !root
                .path()
                .join("artifacts")
                .join(request_id)
                .join(first_artifact)
                .exists()
        );
        assert!(
            root.path()
                .join("artifacts")
                .join(request_id)
                .join(second_artifact)
                .exists()
        );
        assert!(
            artifacts
                .store_ref()
                .query_request(request_id)
                .expect("request remains")
                .is_some()
        );
        assert!(
            artifacts
                .store_ref()
                .query_artifact(first_artifact)
                .expect("first pointer remains")
                .is_some()
        );
        assert!(
            artifacts
                .store_ref()
                .query_artifact(second_artifact)
                .expect("second pointer remains")
                .is_some()
        );
        assert!(
            artifacts
                .store_ref()
                .delete_one_receipt(&request)
                .expect("receipt lookup")
                .is_none()
        );
        let execute_audits: i64 = artifacts
            .store_ref()
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM audit_entries WHERE action = 'log_delete_request'",
                [],
                |row| row.get(0),
            )
            .expect("delete audit count");
        assert_eq!(execute_audits, 0);

        let completed = artifacts
            .delete_request_cascade(&request, &NeverCancelled)
            .expect("retry delete");
        assert_eq!(completed.state, MaintenanceReceiptState::Completed);
        assert_eq!(completed.executed, completed.planned);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            artifacts
                .store_ref()
                .query_request(request_id)
                .expect("request reconciled")
                .is_none()
        );
        let execute_audits: i64 = artifacts
            .store_ref()
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM audit_entries WHERE action = 'log_delete_request'",
                [],
                |row| row.get(0),
            )
            .expect("delete audit count");
        assert_eq!(execute_audits, 1);
    }
}
