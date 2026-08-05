//! Trusted-local delete-one request endpoint.
//!
//! This handler owns only strict HTTP parsing, active-request protection, a
//! bounded cooperative deadline, and a path-free receipt DTO. Durable receipt
//! semantics and confined artifact deletion remain in the log-store facade.

use std::time::Duration;

use mesh_llm_log_store::{
    ArtifactDeletionFailureClass, ArtifactDeletionProgress, MaintenanceCounts, MaintenanceReceipt,
};
use serde::Serialize;
use tokio::net::TcpStream;

use super::{
    LoggingQueryFacade, LoggingRuntimeState, LogsError,
    maintenance_control::{MaintenanceDeadline, timeout_maintenance},
    run_blocking,
};

const DELETE_TIME_CAP: Duration = Duration::from_secs(2);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeleteReceiptDto {
    operation_id: String,
    audit_id: String,
    request_id: String,
    state: &'static str,
    selection_fingerprint: String,
    planned: DeleteCountsDto,
    executed: DeleteCountsDto,
    artifact_deletion: ArtifactDeletionDto,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeleteCountsDto {
    requests: u64,
    events: u64,
    artifacts: u64,
    proxy_records: u64,
    database_rows: u64,
}

impl From<MaintenanceCounts> for DeleteCountsDto {
    fn from(value: MaintenanceCounts) -> Self {
        Self {
            requests: value.requests,
            events: value.events,
            artifacts: value.artifacts,
            proxy_records: value.proxy_records,
            database_rows: value.database_rows,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactDeletionDto {
    removed: u64,
    failed: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure_class: Option<&'static str>,
}

impl From<ArtifactDeletionProgress> for ArtifactDeletionDto {
    fn from(value: ArtifactDeletionProgress) -> Self {
        Self {
            removed: value.removed,
            failed: value.failed,
            failure_class: value.failure_class.map(|class| match class {
                ArtifactDeletionFailureClass::Io => "io",
                ArtifactDeletionFailureClass::UnsafePath => "unsafe_path",
            }),
        }
    }
}

impl DeleteReceiptDto {
    fn from_receipt(request_id: String, receipt: MaintenanceReceipt) -> Result<Self, LogsError> {
        let audit_id = receipt
            .execution_audit_id
            .clone()
            .ok_or(LogsError::StoreUnavailable)?;
        Ok(Self {
            operation_id: receipt.operation_id.to_string(),
            audit_id,
            request_id,
            state: receipt.state.as_str(),
            selection_fingerprint: receipt.fingerprint.as_str().to_owned(),
            planned: receipt.planned.into(),
            executed: receipt.executed.into(),
            artifact_deletion: receipt.artifact_deletion.into(),
        })
    }
}

pub(super) async fn handle(
    stream: &mut TcpStream,
    state: &LoggingRuntimeState,
    request_id: &str,
    path: &str,
    body: &str,
) -> Result<(), LogsError> {
    let request = super::parse::delete_request(request_id, path, body)?;
    let facade = state.query_facade().ok_or(LogsError::ServiceUnavailable)?;
    let audit_facade = facade.clone();
    let reason = request.reason.as_str().to_owned();
    let control = MaintenanceDeadline::new(DELETE_TIME_CAP);
    let worker_control = control.clone();
    let result = timeout_maintenance(
        DELETE_TIME_CAP,
        &control,
        run_blocking(move || delete_terminal_request(&facade, request, &worker_control)),
    )
    .await;
    write_failure_audit(&audit_facade, reason, &result);
    let response = result?;
    crate::api::http::respond_json(stream, 200, &response)
        .await
        .map_err(|_| LogsError::StoreUnavailable)
}

fn delete_terminal_request(
    facade: &LoggingQueryFacade,
    request: super::parse::DeleteRequest,
    control: &MaintenanceDeadline,
) -> Result<DeleteReceiptDto, LogsError> {
    let delete_request = mesh_llm_log_store::DeleteOneRequest::new(
        request.operation_id,
        &request.request_id,
        request.reason,
    )?;
    let request_id = delete_request.request_id.clone();
    if let Some(receipt) = facade.delete_one_receipt(&delete_request)? {
        return DeleteReceiptDto::from_receipt(request_id, receipt);
    }
    match facade.request(&request_id)? {
        Some(record) if record.outcome == "active" => return Err(LogsError::ActiveRequest),
        Some(_) => {}
        None => return Err(LogsError::NotFound),
    }
    let receipt = facade.delete_request_cascade(&delete_request, control)?;
    DeleteReceiptDto::from_receipt(request_id, receipt)
}

fn write_failure_audit(
    facade: &LoggingQueryFacade,
    reason: String,
    result: &Result<DeleteReceiptDto, LogsError>,
) {
    if result.is_err() {
        let _ = facade.write_operator_audit("log_delete_request", reason, "failed");
    }
}

#[cfg(test)]
mod tests {
    use mesh_llm_log_store::{ArtifactDeletionFailureClass, ArtifactDeletionProgress};

    use super::ArtifactDeletionDto;

    #[test]
    fn artifact_deletion_dto_uses_only_stable_failure_classes() {
        let value = serde_json::to_value(ArtifactDeletionDto::from(ArtifactDeletionProgress {
            removed: 0,
            failed: 1,
            failure_class: Some(ArtifactDeletionFailureClass::UnsafePath),
        }))
        .expect("serialize partial progress");
        assert_eq!(
            value,
            serde_json::json!({
                "removed": 0,
                "failed": 1,
                "failureClass": "unsafe_path",
            })
        );
    }
}
