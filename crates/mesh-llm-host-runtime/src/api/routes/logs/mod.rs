//! Trusted-local log-query route core.
//!
//! The management dispatcher validates trusted-local access before entering
//! this module. Its handlers then parse untrusted route input, obtain the
//! narrow host-owned query facade, and perform every SQLite/file operation on
//! the blocking pool before producing typed, privacy-safe DTOs.

mod cleanup;
mod delete;
mod dto;
mod error;
pub(super) mod events;
mod export;
mod maintenance_control;
mod parse;
mod webhook_retry;

use std::collections::HashSet;

pub(crate) use dto::{ArtifactDto, EventDto, PageDto, ProxyDto, RequestDto};
pub(crate) use error::LogsError;
use mesh_llm_log_store::{ArtifactRecord, LogStoreError, QuerySort, RequestRecord};

use self::dto::artifact_state;
use self::parse::SourceFilter;
use tokio::net::TcpStream;

use crate::logging::{LoggingQueryFacade, LoggingRuntimeState, RequestSummaryEntry};

pub(super) fn is_route(path: &str) -> bool {
    path == "/api/logs" || path.starts_with("/api/logs/")
}

/// Dispatch a bounded trusted-local log query. The management server has
/// already classified `/api/logs/**` as trusted-local before it reaches here.
/// Keep route/method/query validation ahead of acquiring the process-owned
/// logging state so malformed requests cannot touch SQLite or artifact files.
pub(super) async fn handle(
    stream: &mut TcpStream,
    method: &str,
    path: &str,
    body: &str,
    raw_request: &[u8],
) -> anyhow::Result<()> {
    let endpoint = path.split('?').next();
    if let Some(result) = handle_mutating_route(stream, method, path, body, endpoint).await {
        return result;
    }

    if method != "GET" || !body.is_empty() {
        return LogsError::MethodNotAllowed.write(stream).await;
    }

    if path.split('?').next() == Some("/api/logs/events") {
        return handle_event_stream(stream, path, raw_request).await;
    }

    handle_query(stream, path).await
}

/// Dispatch the trusted-local mutation subset before generic query handling.
/// Each recognized route validates its method before acquiring global logging
/// state, keeping malformed requests from reaching SQLite or artifacts.
async fn handle_mutating_route(
    stream: &mut TcpStream,
    method: &str,
    path: &str,
    body: &str,
    endpoint: Option<&str>,
) -> Option<anyhow::Result<()>> {
    let route = classify_mutating_route(endpoint, method)?;
    if method != "POST" {
        return Some(method_error(route).write(stream).await);
    }
    let Some(state) = crate::logging_runtime_state() else {
        return Some(LogsError::ServiceUnavailable.write(stream).await);
    };
    Some(dispatch_mutating_route(stream, &state, route, path, body).await)
}

enum MutatingRoute<'a> {
    CleanupPreview,
    CleanupRun,
    Delete(&'a str),
    WebhookRetry(&'a str),
    Export,
}

fn classify_mutating_route<'a>(
    endpoint: Option<&'a str>,
    method: &str,
) -> Option<MutatingRoute<'a>> {
    match endpoint {
        Some("/api/logs/cleanup/preview") => Some(MutatingRoute::CleanupPreview),
        Some("/api/logs/cleanup/run") => Some(MutatingRoute::CleanupRun),
        Some(endpoint) if delete_request_id(endpoint).is_some() => {
            delete_request_id(endpoint).map(MutatingRoute::Delete)
        }
        Some(endpoint) if webhook_retry_delivery_id(endpoint).is_some() => {
            webhook_retry_delivery_id(endpoint).map(MutatingRoute::WebhookRetry)
        }
        Some("/api/logs/requests/export") if method == "POST" => Some(MutatingRoute::Export),
        _ => None,
    }
}

const fn method_error(route: MutatingRoute<'_>) -> LogsError {
    match route {
        MutatingRoute::CleanupPreview | MutatingRoute::CleanupRun => {
            LogsError::CleanupMethodNotAllowed
        }
        MutatingRoute::Delete(_) => LogsError::DeleteMethodNotAllowed,
        MutatingRoute::WebhookRetry(_) => LogsError::WebhookRetryMethodNotAllowed,
        MutatingRoute::Export => LogsError::MethodNotAllowed,
    }
}

async fn dispatch_mutating_route(
    stream: &mut TcpStream,
    state: &LoggingRuntimeState,
    route: MutatingRoute<'_>,
    path: &str,
    body: &str,
) -> anyhow::Result<()> {
    match route {
        MutatingRoute::CleanupPreview => match cleanup::preview(stream, state, path, body).await {
            Ok(()) => Ok(()),
            Err(error) => error.write(stream).await,
        },
        MutatingRoute::CleanupRun => match cleanup::run(stream, state, path, body).await {
            Ok(()) => Ok(()),
            Err(error) => error.write(stream).await,
        },
        MutatingRoute::Delete(request_id) => {
            match delete::handle(stream, state, request_id, path, body).await {
                Ok(()) => Ok(()),
                Err(error) => error.write(stream).await,
            }
        }
        MutatingRoute::WebhookRetry(delivery_id) => {
            match webhook_retry::handle(stream, state, delivery_id, path, body).await {
                Ok(()) => Ok(()),
                Err(error) => error.write(stream).await,
            }
        }
        MutatingRoute::Export => match export::handle(stream, state, path, body).await {
            Ok(()) => Ok(()),
            Err(error) => error.write(stream).await,
        },
    }
}

async fn handle_query(stream: &mut TcpStream, path: &str) -> anyhow::Result<()> {
    let route = classify(path);
    if !route.accepts_query() && path.contains('?') {
        return LogsError::InvalidQuery("query is not supported for this route")
            .write(stream)
            .await;
    }
    if matches!(route, Route::Unknown) {
        return LogsError::NotFound.write(stream).await;
    }

    let state = crate::logging_runtime_state().ok_or(LogsError::ServiceUnavailable);
    match (route, state) {
        (Route::Requests, Ok(state)) => {
            write_result(stream, list_requests(&state, path).await).await
        }
        (Route::RequestDetail(request_id), Ok(state)) => {
            write_result(stream, request_detail(&state, &request_id).await).await
        }
        (Route::RequestEvents(request_id), Ok(state)) => {
            write_result(stream, request_events(&state, path, &request_id).await).await
        }
        (Route::RequestArtifacts(request_id), Ok(state)) => {
            write_result(stream, request_artifacts(&state, path, &request_id).await).await
        }
        (Route::Artifact(artifact_id), Ok(state)) => {
            write_result(stream, artifact_content(&state, &artifact_id).await).await
        }
        (Route::Proxy, Ok(state)) => write_result(stream, proxy_records(&state, path).await).await,
        (Route::Unknown, _) => LogsError::NotFound.write(stream).await,
        (_, Err(error)) => error.write(stream).await,
    }
}

async fn handle_event_stream(
    stream: &mut TcpStream,
    path: &str,
    raw_request: &[u8],
) -> anyhow::Result<()> {
    let subscription = match events::parse_subscription(path, raw_request) {
        Ok(subscription) => subscription,
        Err(error) => return error.write(stream).await,
    };
    let Some(state) = crate::logging_runtime_state() else {
        return LogsError::ServiceUnavailable.write(stream).await;
    };
    let Some(bus) = state.replay_bus() else {
        return LogsError::ServiceUnavailable.write(stream).await;
    };
    let recovery_cursor = event_recovery_cursor(&state).await;
    events::stream(stream, subscription, bus, recovery_cursor).await
}

/// A replay gap always points to the durable request listing. Its cursor is a
/// best-effort, already-redacted keyset boundary; recovery still works when
/// the logging store is momentarily unavailable because the endpoint itself
/// is present in every gap frame.
async fn event_recovery_cursor(state: &LoggingRuntimeState) -> Option<String> {
    let page = list_requests(state, "/api/logs/requests?limit=1")
        .await
        .ok()?;
    let newest = page.items.first()?;
    Some(mesh_llm_log_store::encode_cursor(
        newest.created_at(),
        newest.request_id(),
    ))
}

async fn write_result<T>(stream: &mut TcpStream, result: Result<T, LogsError>) -> anyhow::Result<()>
where
    T: serde::Serialize,
{
    match result {
        Ok(response) => crate::api::http::respond_json(stream, 200, &response).await,
        Err(error) => error.write(stream).await,
    }
}

enum Route {
    Requests,
    RequestDetail(String),
    RequestEvents(String),
    RequestArtifacts(String),
    Artifact(String),
    Proxy,
    Unknown,
}

impl Route {
    const fn accepts_query(&self) -> bool {
        matches!(
            self,
            Self::Requests | Self::RequestEvents(_) | Self::RequestArtifacts(_) | Self::Proxy
        )
    }
}

fn classify(path: &str) -> Route {
    let path = path.split('?').next().unwrap_or(path);
    if path == "/api/logs/requests" {
        return Route::Requests;
    }
    if path == "/api/logs/proxy" {
        return Route::Proxy;
    }
    if let Some(artifact_id) = path.strip_prefix("/api/logs/artifacts/")
        && !artifact_id.is_empty()
        && !artifact_id.contains('/')
    {
        return Route::Artifact(artifact_id.to_owned());
    }
    let Some(remainder) = path.strip_prefix("/api/logs/requests/") else {
        return Route::Unknown;
    };
    if let Some(request_id) = remainder.strip_suffix("/events")
        && !request_id.is_empty()
        && !request_id.contains('/')
    {
        return Route::RequestEvents(request_id.to_owned());
    }
    if let Some(request_id) = remainder.strip_suffix("/artifacts")
        && !request_id.is_empty()
        && !request_id.contains('/')
    {
        return Route::RequestArtifacts(request_id.to_owned());
    }
    if !remainder.is_empty() && !remainder.contains('/') {
        return Route::RequestDetail(remainder.to_owned());
    }
    Route::Unknown
}

fn delete_request_id(path: &str) -> Option<&str> {
    path.strip_prefix("/api/logs/requests/")?
        .strip_suffix("/delete")
        .filter(|request_id| !request_id.is_empty() && !request_id.contains('/'))
}

fn webhook_retry_delivery_id(path: &str) -> Option<&str> {
    path.strip_prefix("/api/logs/webhooks/")?
        .strip_suffix("/retry")
        .filter(|delivery_id| !delivery_id.is_empty() && !delivery_id.contains('/'))
}

/// Parse and run `GET /api/logs/requests` against one active snapshot plus
/// durable history. Every query uses a bounded keyset scan; active request IDs
/// take precedence while durable metadata supplements their display fields.
pub(crate) async fn list_requests(
    state: &LoggingRuntimeState,
    path: &str,
) -> Result<PageDto<RequestDto>, LogsError> {
    let parsed = parse::request_query(path)?;
    let facade = state.query_facade().ok_or(LogsError::ServiceUnavailable)?;
    let active = facade.snapshot_active().into_entries();
    run_blocking(move || list_requests_blocking(facade, active, parsed)).await
}

/// Parse and run `GET /api/logs/requests/:id`.
pub(crate) async fn request_detail(
    state: &LoggingRuntimeState,
    request_id: &str,
) -> Result<RequestDto, LogsError> {
    let request_id = parse::id(request_id)?;
    let facade = state.query_facade().ok_or(LogsError::ServiceUnavailable)?;
    let audit_facade = facade.clone();
    let result = if let Some(active) = facade
        .snapshot_active()
        .into_entries()
        .into_iter()
        .find(|entry| entry.request_id == request_id)
    {
        run_blocking(move || Ok(RequestDto::active(active, facade.request(&request_id)?))).await
    } else {
        run_blocking(move || {
            facade
                .request(&request_id)?
                .map(RequestDto::durable)
                .ok_or(LogsError::NotFound)
        })
        .await
    };
    audit_read(
        &audit_facade,
        "log_request_detail_read",
        "request detail read",
        result.is_ok(),
    );
    result
}

/// Parse and run `GET /api/logs/requests/:id/events` without exposing the
/// stored canonical JSON envelope.
pub(crate) async fn request_events(
    state: &LoggingRuntimeState,
    path: &str,
    request_id: &str,
) -> Result<PageDto<EventDto>, LogsError> {
    let request_id = parse::id(request_id)?;
    let query = parse::page_query(path)?;
    let facade = state.query_facade().ok_or(LogsError::ServiceUnavailable)?;
    run_blocking(move || {
        if facade.request(&request_id)?.is_none() {
            return Err(LogsError::NotFound);
        }
        let page = facade.events(&request_id, &query)?;
        let items = page
            .items
            .into_iter()
            .map(EventDto::try_from)
            .collect::<Result<_, _>>()?;
        Ok(PageDto {
            items,
            next_cursor: page.next_cursor,
        })
    })
    .await
}

/// Parse and run `GET /api/logs/requests/:id/artifacts`, returning pointer
/// metadata only. Content uses the dedicated single-artifact core below.
pub(crate) async fn request_artifacts(
    state: &LoggingRuntimeState,
    path: &str,
    request_id: &str,
) -> Result<PageDto<ArtifactDto>, LogsError> {
    let request_id = parse::id(request_id)?;
    let query = parse::page_query(path)?;
    let facade = state.query_facade().ok_or(LogsError::ServiceUnavailable)?;
    run_blocking(move || {
        if facade.request(&request_id)?.is_none() {
            return Err(LogsError::NotFound);
        }
        let page = facade.artifacts(&request_id, &query)?;
        Ok(PageDto {
            items: page.items.into_iter().map(ArtifactDto::metadata).collect(),
            next_cursor: page.next_cursor,
        })
    })
    .await
}

/// Parse and run `GET /api/logs/artifacts/:id`. Only already-redacted content
/// can cross this boundary. Missing/corrupt files resolve to typed metadata
/// states rather than storage paths or backend errors.
pub(crate) async fn artifact_content(
    state: &LoggingRuntimeState,
    artifact_id: &str,
) -> Result<ArtifactDto, LogsError> {
    let artifact_id = parse::id(artifact_id)?;
    let facade = state.query_facade().ok_or(LogsError::ServiceUnavailable)?;
    let audit_facade = facade.clone();
    let result = run_blocking(move || {
        let record = facade.artifact(&artifact_id)?.ok_or(LogsError::NotFound)?;
        if artifact_state(&record) != "available" {
            return Ok(ArtifactDto::metadata(record));
        }
        match facade.read_artifact(&artifact_id) {
            Ok(content) if content.redacted => Ok(ArtifactDto::content(record, content)),
            Ok(_) => Ok(ArtifactDto::metadata(mark_unavailable(record))),
            Err(LogStoreError::ArtifactMissing { .. }) => {
                Ok(ArtifactDto::metadata(mark_missing(record)))
            }
            Err(LogStoreError::ArtifactCorrupt { .. }) => {
                Ok(ArtifactDto::metadata(mark_corrupt(record)))
            }
            Err(error) => Err(error.into()),
        }
    })
    .await;
    let succeeded = result
        .as_ref()
        .is_ok_and(ArtifactDto::has_available_content);
    audit_read(
        &audit_facade,
        "log_artifact_read",
        "artifact read",
        succeeded,
    );
    result
}

/// Operator read auditing is deliberately best-effort. The audit payload is
/// fixed metadata only: endpoint action, trusted-local source, outcome, and a
/// generic reason. Request IDs, DTOs, artifact paths, and bytes never cross
/// this boundary, and a write failure never changes the original read result.
fn audit_read(
    facade: &LoggingQueryFacade,
    action: &'static str,
    reason: &'static str,
    succeeded: bool,
) {
    let result = if succeeded { "succeeded" } else { "failed" };
    let _ = facade.write_operator_audit(action, reason.to_owned(), result);
}

/// Parse and run `GET /api/logs/proxy`. Proxy attempts remain separate from
/// summaries and their target is reduced to scheme/host/port by `ProxyDto`.
pub(crate) async fn proxy_records(
    state: &LoggingRuntimeState,
    path: &str,
) -> Result<PageDto<ProxyDto>, LogsError> {
    let query = parse::proxy_query(path)?;
    let facade = state.query_facade().ok_or(LogsError::ServiceUnavailable)?;
    run_blocking(move || {
        let page = facade.proxy_records(&query)?;
        Ok(PageDto {
            items: page.items.into_iter().map(ProxyDto::from).collect(),
            next_cursor: page.next_cursor,
        })
    })
    .await
}

fn list_requests_blocking(
    facade: LoggingQueryFacade,
    active: Vec<RequestSummaryEntry>,
    parsed: parse::RequestListQuery,
) -> Result<PageDto<RequestDto>, LogsError> {
    let active_ids = active
        .iter()
        .map(|entry| entry.request_id.clone())
        .collect::<HashSet<_>>();
    let cursor_is_active = parsed
        .cursor_boundary
        .as_ref()
        .is_some_and(|(_, id)| active_ids.contains(id));

    validate_cursor_scope(&facade, &active, &parsed, cursor_is_active)?;

    let mut items = Vec::new();
    if parsed.source != Some(SourceFilter::Durable) {
        for entry in active {
            let metadata = facade.request(&entry.request_id)?;
            if active_matches(&entry, metadata.as_ref(), &parsed) {
                items.push(RequestDto::active(entry, metadata));
            }
        }
    }

    if parsed.source != Some(SourceFilter::Active) {
        let durable = collect_durable(&facade, &active_ids, &parsed, cursor_is_active)?;
        items.extend(durable.into_iter().map(RequestDto::durable));
    }

    items.sort_by(|left, right| {
        left.created_at()
            .cmp(right.created_at())
            .then_with(|| left.request_id().cmp(right.request_id()))
    });
    if parsed.store.sort == QuerySort::Descending {
        items.reverse();
    }
    let has_more = items.len() > parsed.store.limit;
    items.truncate(parsed.store.limit);
    let next_cursor = has_more.then(|| {
        let last = items.last().expect("non-empty page has a cursor anchor");
        mesh_llm_log_store::encode_cursor(last.created_at(), last.request_id())
    });
    Ok(PageDto { items, next_cursor })
}

fn validate_cursor_scope(
    facade: &LoggingQueryFacade,
    active: &[RequestSummaryEntry],
    parsed: &parse::RequestListQuery,
    cursor_is_active: bool,
) -> Result<(), LogsError> {
    let Some((_, request_id)) = &parsed.cursor_boundary else {
        return Ok(());
    };
    if cursor_is_active {
        let entry = active
            .iter()
            .find(|entry| &entry.request_id == request_id)
            .expect("active cursor ID was derived from active IDs");
        let metadata = facade.request(request_id)?;
        if !active_matches_filters(entry, metadata.as_ref(), parsed) {
            return Err(LogsError::CursorExpired);
        }
        return Ok(());
    }
    // Let the typed store prove that a durable cursor belongs to exactly this
    // filter scope. That rejects forged IDs and filter-mismatch cursors.
    let mut probe = parsed.store.clone();
    probe.limit = 1;
    facade.requests(&probe)?;
    Ok(())
}

fn collect_durable(
    facade: &LoggingQueryFacade,
    active_ids: &HashSet<String>,
    parsed: &parse::RequestListQuery,
    cursor_is_active: bool,
) -> Result<Vec<RequestRecord>, LogsError> {
    let mut query = parsed.store.clone();
    if cursor_is_active {
        // An active-only cursor is not a durable row, so passing it to SQLite
        // would falsely classify a valid merged page as forged. Apply the
        // shared cursor boundary below while starting the durable scan at its
        // own first keyset page.
        query.cursor = None;
    }
    query.limit = parse::MAX_LIMIT;
    let mut result = Vec::new();
    let max_pages = active_ids.len().saturating_add(2);
    for _ in 0..max_pages {
        let page = facade.requests(&query)?;
        for record in page.items {
            if !active_ids.contains(&record.request_id) && durable_after_cursor(&record, parsed) {
                result.push(record);
            }
        }
        if result.len() > parsed.store.limit || page.next_cursor.is_none() {
            break;
        }
        query.cursor = page.next_cursor;
    }
    Ok(result)
}

fn durable_after_cursor(record: &RequestRecord, parsed: &parse::RequestListQuery) -> bool {
    let Some((timestamp, request_id)) = &parsed.cursor_boundary else {
        return true;
    };
    let row = (record.created_at.as_str(), record.request_id.as_str());
    let cursor = (timestamp.as_str(), request_id.as_str());
    match parsed.store.sort {
        QuerySort::Ascending => row > cursor,
        QuerySort::Descending => row < cursor,
    }
}

fn active_matches(
    entry: &RequestSummaryEntry,
    metadata: Option<&RequestRecord>,
    parsed: &parse::RequestListQuery,
) -> bool {
    if !active_matches_filters(entry, metadata, parsed) {
        return false;
    }
    if let Some((timestamp, request_id)) = &parsed.cursor_boundary {
        let row = (entry.created_at.as_str(), entry.request_id.as_str());
        let cursor = (timestamp.as_str(), request_id.as_str());
        return match parsed.store.sort {
            QuerySort::Ascending => row > cursor,
            QuerySort::Descending => row < cursor,
        };
    }
    true
}

fn active_matches_filters(
    entry: &RequestSummaryEntry,
    metadata: Option<&RequestRecord>,
    parsed: &parse::RequestListQuery,
) -> bool {
    let query = &parsed.store;
    if let Some(route) = &query.route
        && entry
            .metadata
            .route()
            .or_else(|| metadata.and_then(|record| record.route.as_deref()))
            != Some(route.as_str())
    {
        return false;
    }
    if let Some(model) = &query.model
        && entry
            .metadata
            .model()
            .or_else(|| metadata.and_then(|record| record.model.as_deref()))
            != Some(model.as_str())
    {
        return false;
    }
    if let Some(provider) = &query.provider
        && entry
            .metadata
            .provider()
            .or_else(|| metadata.and_then(|record| record.provider.as_deref()))
            != Some(provider.as_str())
    {
        return false;
    }
    if let Some(engine) = &query.engine
        && entry
            .metadata
            .engine()
            .or_else(|| metadata.and_then(|record| record.engine.as_deref()))
            != Some(engine.as_str())
    {
        return false;
    }
    if let Some(status_code) = query.status_code
        && metadata.and_then(|record| record.status_code) != Some(i64::from(status_code))
    {
        return false;
    }
    if let Some(outcome) = query.outcome
        && entry.state != outcome.as_str()
    {
        return false;
    }
    if query
        .from
        .as_ref()
        .is_some_and(|from| entry.created_at < *from)
        || query.to.as_ref().is_some_and(|to| entry.created_at > *to)
    {
        return false;
    }
    true
}

fn mark_missing(mut record: ArtifactRecord) -> ArtifactRecord {
    record.missing = true;
    record
}

fn mark_corrupt(mut record: ArtifactRecord) -> ArtifactRecord {
    record.corrupt = true;
    record
}

fn mark_unavailable(mut record: ArtifactRecord) -> ArtifactRecord {
    record.redacted = false;
    record
}

async fn run_blocking<T, F>(operation: F) -> Result<T, LogsError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, LogsError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|_| LogsError::StoreUnavailable)?
}

#[cfg(test)]
mod tests {
    use mesh_llm_events::logging::identifiers::RequestId;

    use super::*;

    fn runtime() -> (tempfile::TempDir, LoggingRuntimeState) {
        let temp = tempfile::tempdir().expect("temporary application state root");
        let root = temp.path().join("logging-state");
        let foundation = crate::logging::LoggingFoundation::init(true, Some(&root));
        let config = mesh_llm_config::LoggingConfig {
            application_state_root: Some(root),
            ..Default::default()
        };
        (temp, LoggingRuntimeState::initialize(&foundation, &config))
    }

    #[tokio::test]
    async fn list_merges_active_and_durable_without_duplicate_ids() {
        let (_temp, state) = runtime();
        let durable_id = RequestId::new().as_uuid().to_string();
        state
            .store()
            .expect("store")
            .insert_summary(
                &durable_id,
                Some("durable-model"),
                Some("management"),
                None,
                None,
                "2026-01-01T00:00:00Z",
                None,
                None,
                None,
            )
            .expect("seed durable summary");

        let active_id = RequestId::new();
        let service = state.service_for_test().expect("logging service");
        let (active_guard, _) = service.register_request(active_id);

        let page = list_requests(&state, "/api/logs/requests?limit=10")
            .await
            .expect("list request core");
        let json = serde_json::to_value(page).expect("serialize page");
        let items = json["items"].as_array().expect("items");
        assert_eq!(items.len(), 2);
        assert!(items.iter().any(|item| item["requestId"] == durable_id));
        assert!(
            items
                .iter()
                .any(|item| item["requestId"] == active_id.as_uuid().to_string())
        );
        assert_eq!(
            items
                .iter()
                .filter(|item| item["requestId"] == active_id.as_uuid().to_string())
                .count(),
            1
        );

        let first_page = list_requests(&state, "/api/logs/requests?limit=1")
            .await
            .expect("first merged page");
        let cursor = first_page.next_cursor.expect("active cursor");
        let second_page = list_requests(
            &state,
            &format!("/api/logs/requests?limit=1&cursor={cursor}"),
        )
        .await
        .expect("active cursor resumes into durable history");
        let json = serde_json::to_value(second_page).expect("serialize resumed page");
        assert_eq!(json["items"][0]["requestId"], durable_id);
        drop(active_guard);
    }

    #[tokio::test]
    async fn active_request_uses_registered_metadata_before_durable_persistence() {
        let (_temp, state) = runtime();
        let request_id = RequestId::new();
        let service = state.service_for_test().expect("logging service");
        let (guard, _) = service.register_request_with_metadata(
            request_id,
            crate::logging::RequestSummaryMetadata::from_parts(
                Some("chat_completions"),
                None,
                None,
                None,
            ),
        );
        service.merge_request_metadata(
            request_id,
            crate::logging::RequestSummaryMetadata::from_parts(
                None,
                Some("acme/model"),
                Some("mesh"),
                Some("raw_ingress"),
            ),
        );

        assert!(
            state
                .store()
                .expect("store")
                .query_request(&request_id.as_uuid().to_string())
                .expect("query active durable row")
                .is_none()
        );
        let page = list_requests(
            &state,
            "/api/logs/requests?source=active&route=chat_completions&model=acme%2Fmodel&provider=mesh&engine=raw_ingress",
        )
        .await
        .expect("active metadata query");
        let json = serde_json::to_value(page).expect("active metadata JSON");
        assert_eq!(json["items"].as_array().expect("items").len(), 1);
        assert_eq!(
            json["items"][0]["requestId"],
            request_id.as_uuid().to_string()
        );
        assert_eq!(json["items"][0]["route"], "chat_completions");
        assert_eq!(json["items"][0]["model"], "acme/model");
        assert_eq!(json["items"][0]["provider"], "mesh");
        assert_eq!(json["items"][0]["engine"], "raw_ingress");
        assert_eq!(json["items"][0]["source"], "active");
        drop(guard);
    }

    #[tokio::test]
    async fn registered_metadata_is_persisted_and_durably_filterable_without_fabrication() {
        let (_temp, state) = runtime();
        let service = state.service_for_test().expect("logging service");

        let known = RequestId::new();
        let (known_guard, _) = service.register_request_with_metadata(
            known,
            crate::logging::RequestSummaryMetadata::from_parts(
                Some("chat_completions"),
                None,
                None,
                None,
            ),
        );
        service.merge_request_metadata(
            known,
            crate::logging::RequestSummaryMetadata::from_parts(
                None,
                Some("acme/model"),
                Some("mesh"),
                Some("raw_ingress"),
            ),
        );
        service
            .transition_terminal(
                known,
                &known_guard,
                crate::logging::TerminalOutcome::Completed,
            )
            .expect("known request terminalizes");

        let absent = RequestId::new();
        let (absent_guard, _) = service.register_request(absent);
        service
            .transition_terminal(
                absent,
                &absent_guard,
                crate::logging::TerminalOutcome::Completed,
            )
            .expect("metadata-absent request terminalizes");
        assert!(service.pump_sync().await > 0);

        let page = list_requests(
            &state,
            "/api/logs/requests?source=durable&route=chat_completions&model=acme%2Fmodel&provider=mesh&engine=raw_ingress&outcome=completed",
        )
        .await
        .expect("durable metadata query");
        let json = serde_json::to_value(page).expect("durable metadata JSON");
        assert_eq!(json["items"].as_array().expect("items").len(), 1);
        assert_eq!(json["items"][0]["requestId"], known.as_uuid().to_string());
        assert_eq!(json["items"][0]["route"], "chat_completions");
        assert_eq!(json["items"][0]["model"], "acme/model");
        assert_eq!(json["items"][0]["provider"], "mesh");
        assert_eq!(json["items"][0]["engine"], "raw_ingress");
        assert_eq!(json["items"][0]["source"], "durable");

        let absent_detail = request_detail(&state, &absent.as_uuid().to_string())
            .await
            .expect("metadata-absent detail");
        let absent_json = serde_json::to_value(absent_detail).expect("absent detail JSON");
        assert_eq!(absent_json["route"], serde_json::Value::Null);
        assert_eq!(absent_json["model"], serde_json::Value::Null);
        assert_eq!(absent_json["provider"], serde_json::Value::Null);
        assert_eq!(absent_json["engine"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn detail_and_related_routes_are_typed_and_keep_envelope_private() {
        let (_temp, state) = runtime();
        let request_id = RequestId::new();
        let request_key = request_id.as_uuid().to_string();
        let store = state.store().expect("store");
        store
            .insert_summary(
                &request_key,
                None,
                None,
                None,
                None,
                "2026-01-01T00:00:00Z",
                None,
                None,
                None,
            )
            .expect("seed durable summary");
        let event_id = mesh_llm_events::logging::identifiers::EventId::new();
        let envelope = mesh_llm_events::logging::envelope::CanonicalEnvelope::new(
            event_id,
            request_id,
            mesh_llm_events::logging::replay::ReplayChannel::Requests,
            1,
            "2026-01-01T00:00:01Z".into(),
            mesh_llm_events::logging::events::LifecycleEvent::Admitted {
                model: Some("Bearer never-expose".into()),
                method: Some("GET".into()),
            },
        );
        store
            .insert_lifecycle_event(
                &request_key,
                &event_id.as_uuid().to_string(),
                &serde_json::to_string(&envelope).expect("canonical envelope"),
                "2026-01-01T00:00:01Z",
            )
            .expect("seed lifecycle record");

        let detail = request_detail(&state, &request_key)
            .await
            .expect("detail core");
        assert_eq!(
            serde_json::to_value(detail).expect("detail json")["requestId"],
            request_key
        );

        let events = request_events(&state, "/api/logs/requests/id/events", &request_key)
            .await
            .expect("related events core");
        let json = serde_json::to_string(&events).expect("event dto JSON");
        assert!(json.contains("admitted"));
        assert!(!json.contains("never-expose"));
        assert!(!json.contains("payload_json"));
    }
}
