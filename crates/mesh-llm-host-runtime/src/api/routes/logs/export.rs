//! Bounded trusted-local durable log export.
//!
//! This route deliberately exports the privacy-safe DTOs used by the read
//! API, rather than raw SQLite rows or artifact paths. Artifact bytes are an
//! explicit opt-in and remain unavailable unless redacted capture is active.

use std::time::{Duration, Instant};

use mesh_llm_log_store::{ArtifactRecord, LogStoreError, PageQuery, QuerySort, RequestRecord};
use serde::Serialize;
use tokio::net::TcpStream;

use super::dto::{ArtifactDto, EventDto, RequestDto};
use super::{LoggingQueryFacade, LoggingRuntimeState, LogsError, run_blocking};

const EXPORT_TIME_CAP: Duration = Duration::from_secs(2);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExportDto {
    items: Vec<ExportItemDto>,
    next_cursor: Option<String>,
    truncated: bool,
    retry_required: bool,
    artifact_content_included: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExportItemDto {
    summary: RequestDto,
    events: Vec<EventDto>,
    artifacts: Vec<ArtifactDto>,
    child_incomplete: bool,
}

pub(super) async fn handle(
    stream: &mut TcpStream,
    state: &LoggingRuntimeState,
    path: &str,
    body: &str,
) -> Result<(), LogsError> {
    let request = super::parse::export_request(path, body)?;
    let facade = state.query_facade().ok_or(LogsError::ServiceUnavailable)?;
    if request.include_artifacts && !facade.artifact_export_enabled() {
        return Err(LogsError::ArtifactExportForbidden);
    }
    let deadline = Instant::now() + EXPORT_TIME_CAP;
    let byte_limit = facade.export_limit_bytes();
    let reason = request.reason.clone();
    let export = tokio::time::timeout(
        EXPORT_TIME_CAP,
        run_blocking(move || {
            let selection = build_export(
                &facade,
                request.query,
                request.include_artifacts,
                byte_limit,
                deadline,
            );
            let result = match &selection {
                Ok(value) if value.truncated => "partial",
                Ok(_) => "succeeded",
                Err(_) => "failed",
            };
            // Audit persistence is deliberately best-effort: an unavailable audit
            // table must not turn a successful export into a failure, or conceal
            // the original store/timeout error from the caller.
            let _ = facade.write_operator_audit("log_export", reason, result);
            selection
        }),
    )
    .await
    .map_err(|_| LogsError::ExportTimedOut)??;
    crate::api::http::respond_json(stream, 200, &export)
        .await
        .map_err(|_| LogsError::StoreUnavailable)
}

fn build_export(
    facade: &LoggingQueryFacade,
    query: mesh_llm_log_store::RequestQuery,
    include_artifacts: bool,
    byte_limit: usize,
    deadline: Instant,
) -> Result<ExportDto, LogsError> {
    ensure_before(deadline)?;
    let page = facade.requests(&query)?;
    ensure_before(deadline)?;

    let mut export = ExportDto {
        items: Vec::new(),
        next_cursor: None,
        truncated: false,
        retry_required: false,
        artifact_content_included: false,
    };
    let mut remaining_rows = super::parse::MAX_EXPORT_ROWS;
    let mut fully_exported = true;
    let page_has_more = page.next_cursor.is_some();
    let page_len = page.items.len();
    for (index, record) in page.items.into_iter().enumerate() {
        if remaining_rows == 0 {
            export.truncated = true;
            set_resume_cursor(&mut export);
            fully_exported = false;
            break;
        }
        let built = export_item(
            facade,
            record,
            include_artifacts,
            remaining_rows.saturating_sub(1),
            byte_limit,
            deadline,
        )?;
        let request_has_later = index + 1 < page_len || page_has_more;
        let used_rows = 1 + built.item.events.len() + built.item.artifacts.len();
        export.items.push(built.item);
        // Account for the largest final envelope before deciding whether an
        // item fits. Clearing this flag at the end can only shrink it.
        export.truncated = true;
        export.retry_required |= built.child_truncated;
        export.next_cursor =
            (!built.child_truncated && request_has_later).then(|| cursor_for_last(&export));
        if serialized_len(&export)? > byte_limit {
            export.items.pop();
            export.truncated = true;
            set_resume_cursor(&mut export);
            if export.next_cursor.is_none() {
                export.retry_required = true;
            }
            fully_exported = false;
            break;
        }
        for (artifact_index, content) in built.content_artifacts {
            let item = export
                .items
                .last_mut()
                .expect("export item was just pushed");
            let metadata = std::mem::replace(&mut item.artifacts[artifact_index], content);
            if serialized_len(&export)? > byte_limit {
                let item = export
                    .items
                    .last_mut()
                    .expect("export item was just pushed");
                item.artifacts[artifact_index] = metadata;
                export.truncated = true;
                fully_exported = false;
            } else {
                export.artifact_content_included = true;
            }
        }
        if built.child_truncated {
            // A request cursor would advance past partial child history. Keep
            // the partial item visible, require an explicit retry, and never
            // claim that the summary page can safely advance.
            export
                .items
                .last_mut()
                .expect("export item was just pushed")
                .child_incomplete = true;
            export.next_cursor = None;
            export.truncated = true;
            export.retry_required = true;
            fully_exported = false;
            break;
        }
        fully_exported &= !request_has_later;
        remaining_rows = remaining_rows.saturating_sub(used_rows);
    }
    if fully_exported {
        export.truncated = false;
    }
    ensure_final_size(&export, byte_limit)?;
    Ok(export)
}

fn cursor_for_last(export: &ExportDto) -> String {
    let item = export.items.last().expect("cursor requires an export item");
    mesh_llm_log_store::encode_cursor(item.summary.created_at(), item.summary.request_id())
}

fn set_resume_cursor(export: &mut ExportDto) {
    export.next_cursor = (!export.items.is_empty()).then(|| cursor_for_last(export));
}

fn export_item(
    facade: &LoggingQueryFacade,
    record: RequestRecord,
    include_artifacts: bool,
    child_row_limit: usize,
    byte_limit: usize,
    deadline: Instant,
) -> Result<ExportItemBuild, LogsError> {
    let request_id = record.request_id.clone();
    let mut remaining_rows = child_row_limit;
    let event_limit = remaining_rows.min(super::parse::MAX_EXPORT_ROWS);
    ensure_before(deadline)?;
    let page = facade.events(
        &request_id,
        &PageQuery {
            // Query one row even when the shared budget is exhausted so a
            // missing child can never be mistaken for a complete export.
            limit: event_limit.max(1),
            cursor: None,
            sort: QuerySort::Ascending,
        },
    )?;
    let mut truncated = page.next_cursor.is_some() || event_limit == 0 && !page.items.is_empty();
    let events = page
        .items
        .into_iter()
        .take(event_limit)
        .map(EventDto::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    remaining_rows = remaining_rows.saturating_sub(events.len());

    let mut artifacts = Vec::new();
    let mut content_artifacts = Vec::new();
    ensure_before(deadline)?;
    let artifact_limit = remaining_rows.min(super::parse::MAX_EXPORT_ROWS);
    let artifact_page = facade.artifacts(
        &request_id,
        &PageQuery {
            limit: artifact_limit.max(1),
            cursor: None,
            sort: QuerySort::Ascending,
        },
    )?;
    truncated |= artifact_page.next_cursor.is_some()
        || artifact_limit == 0 && !artifact_page.items.is_empty();
    for record in artifact_page.items.into_iter().take(artifact_limit) {
        ensure_before(deadline)?;
        let content_omitted = include_artifacts
            && super::dto::artifact_state(&record) == "available"
            && (record.bytes.is_negative()
                || usize::try_from(record.bytes).unwrap_or(usize::MAX) > byte_limit / 2);
        let (metadata, content) =
            export_artifact(facade, record, include_artifacts, byte_limit, deadline)?;
        truncated |= content_omitted;
        let artifact_index = artifacts.len();
        artifacts.push(metadata);
        if let Some(content) = content {
            content_artifacts.push((artifact_index, content));
        }
    }

    Ok(ExportItemBuild {
        item: ExportItemDto {
            summary: RequestDto::durable(record),
            events,
            artifacts,
            child_incomplete: false,
        },
        child_truncated: truncated,
        content_artifacts,
    })
}

struct ExportItemBuild {
    item: ExportItemDto,
    child_truncated: bool,
    content_artifacts: Vec<(usize, ArtifactDto)>,
}

fn export_artifact(
    facade: &LoggingQueryFacade,
    record: ArtifactRecord,
    include_content: bool,
    byte_limit: usize,
    deadline: Instant,
) -> Result<(ArtifactDto, Option<ArtifactDto>), LogsError> {
    // Base64 grows the captured content by roughly a third. Reserve half the
    // response for the summary/event envelope and avoid reading a file that
    // could never fit this bounded response; its pointer metadata is still
    // useful and remains retryable through a narrower export.
    let content_budget = byte_limit / 2;
    if record.bytes.is_negative()
        || usize::try_from(record.bytes).unwrap_or(usize::MAX) > content_budget
    {
        return Ok((ArtifactDto::metadata(record), None));
    }
    if !include_content || super::dto::artifact_state(&record) != "available" {
        return Ok((ArtifactDto::metadata(record), None));
    }
    ensure_before(deadline)?;
    match facade.read_artifact(&record.artifact_id) {
        Ok(content) if content.redacted => {
            let metadata = ArtifactDto::metadata(record.clone());
            Ok((metadata, Some(ArtifactDto::content(record, content))))
        }
        Ok(_) => Ok((ArtifactDto::metadata(record), None)),
        Err(LogStoreError::ArtifactMissing { .. }) => Ok((
            ArtifactDto::metadata(ArtifactRecord {
                missing: true,
                ..record
            }),
            None,
        )),
        Err(LogStoreError::ArtifactCorrupt { .. }) => Ok((
            ArtifactDto::metadata(ArtifactRecord {
                corrupt: true,
                ..record
            }),
            None,
        )),
        Err(error) => Err(error.into()),
    }
}

fn ensure_before(deadline: Instant) -> Result<(), LogsError> {
    if Instant::now() >= deadline {
        Err(LogsError::ExportTimedOut)
    } else {
        Ok(())
    }
}

fn serialized_len(value: &ExportDto) -> Result<usize, LogsError> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|_| LogsError::StoreUnavailable)
}

fn ensure_final_size(export: &ExportDto, byte_limit: usize) -> Result<(), LogsError> {
    if serialized_len(export)? > byte_limit {
        return Err(LogsError::StoreUnavailable);
    }
    Ok(())
}
