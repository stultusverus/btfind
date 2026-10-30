use crate::magnet;
use crate::metrics::WebMetrics;
use crate::store::{CrawlStats, Store, TorrentRecord, TorrentSearchFilters};
use axum::body::Body;
use axum::extract::{MatchedPath, Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub metrics: Arc<WebMetrics>,
}

impl AppState {
    fn database_error(&self, error: crate::store::Error) -> ApiError {
        self.metrics.record_database_error();
        tracing::warn!(
            event = "database_request_failed",
            reason = "database",
            error = %error,
            "database request failed"
        );
        ApiError::Db("database temporarily unavailable".into())
    }
}

#[derive(Debug)]
enum ApiError {
    BadRequest(String),
    NotFound(String),
    Db(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::Db(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

impl From<crate::store::Error> for ApiError {
    fn from(e: crate::store::Error) -> Self {
        tracing::warn!(
            event = "database_request_failed",
            reason = "database",
            error = %e,
            "database request failed"
        );
        ApiError::Db("database temporarily unavailable".into())
    }
}

impl From<String> for ApiError {
    fn from(s: String) -> Self {
        ApiError::BadRequest(s)
    }
}

pub fn router(store: Store, metrics: Arc<WebMetrics>) -> axum::Router {
    let state = AppState { store, metrics };
    let request_metrics = state.metrics.clone();

    axum::Router::new()
        .route("/", get(dashboard))
        .route("/dashboard.css", get(dashboard_css))
        .route("/dashboard.js", get(dashboard_js))
        .route("/api/stats", get(api_stats))
        .route("/api/search", get(api_search))
        .route("/api/torrents/{info_hash}", get(api_torrent))
        .route("/api/torrents/{info_hash}/files", get(api_torrent_files))
        .route("/api/torrents/{info_hash}/magnet", get(api_magnet))
        .with_state(state)
        .layer(axum::middleware::from_fn(request_deadline))
        .layer(axum::middleware::from_fn(move |request, next| {
            let metrics = request_metrics.clone();
            async move { record_request(metrics, request, next).await }
        }))
}

async fn record_request(
    metrics: Arc<WebMetrics>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_string())
        .unwrap_or_default();
    let started = Instant::now();
    metrics.begin_request();
    let response = next.run(request).await;
    metrics.record_request(&route, response.status().as_u16(), started.elapsed());
    metrics.end_request();
    response
}

fn static_response(content_type: &'static str, content: &'static str) -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header(header::REFERRER_POLICY, "no-referrer")
        .header(
            header::CONTENT_SECURITY_POLICY,
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'none'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
        )
        .body(Body::from(content))
        .expect("static dashboard response headers must be valid")
}

async fn dashboard() -> Response {
    static_response(
        "text/html; charset=utf-8",
        include_str!("web_dashboard.html"),
    )
}

async fn dashboard_css() -> Response {
    static_response("text/css; charset=utf-8", include_str!("web_dashboard.css"))
}

async fn dashboard_js() -> Response {
    static_response(
        "text/javascript; charset=utf-8",
        include_str!("web_dashboard.js"),
    )
}

#[derive(Deserialize)]
struct SearchQuery {
    q: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
    sort: Option<String>,
    complete_only: Option<bool>,
    min_size: Option<i64>,
    max_size: Option<i64>,
}

fn default_limit() -> usize {
    50
}

#[derive(Debug, serde::Serialize)]
struct SearchResult {
    info_hash: String,
    name: Option<String>,
    total_size: i64,
    file_count: i64,
    first_seen: i64,
    last_seen: i64,
    magnet: String,
}

#[derive(Debug, serde::Serialize)]
struct SearchResponse {
    results: Vec<SearchResult>,
    limit: usize,
    offset: usize,
    has_next: bool,
    effective_sort: String,
}

fn torrent_to_search_result(t: &TorrentRecord) -> SearchResult {
    let hash_hex = hex::encode(t.info_hash);
    let dn = t.name.as_deref();
    SearchResult {
        info_hash: hash_hex,
        name: t.name.clone(),
        total_size: t.total_size,
        file_count: t.file_count,
        first_seen: t.first_seen,
        last_seen: t.last_seen,
        magnet: magnet::magnet_uri_from_hash(&t.info_hash, dn),
    }
}

fn validate_sort(sort: &str) -> Result<(), ApiError> {
    match sort {
        "rank" | "first_seen" | "last_seen" | "total_size" | "name" => Ok(()),
        other => Err(ApiError::BadRequest(format!("invalid sort: {other}"))),
    }
}

fn clamp_limit(limit: usize) -> usize {
    limit.clamp(1, 500)
}

fn clamp_offset(offset: usize) -> usize {
    offset.min(i64::MAX as usize)
}

fn effective_sort(query: &str, requested_sort: Option<&str>) -> &'static str {
    match requested_sort {
        Some("first_seen") => "first_seen",
        Some("total_size") => "total_size",
        Some("name") => "name",
        Some("last_seen") => "last_seen",
        Some("rank") if query.trim().chars().count() >= 2 => "rank",
        Some("rank") => "last_seen",
        Some(_) => "last_seen",
        None if query.trim().chars().count() >= 2 => "rank",
        None => "last_seen",
    }
}

fn validate_search_filters(params: &SearchQuery) -> Result<TorrentSearchFilters, ApiError> {
    if matches!(params.min_size, Some(size) if size < 0) {
        return Err(ApiError::BadRequest("min_size must be non-negative".into()));
    }
    if matches!(params.max_size, Some(size) if size < 0) {
        return Err(ApiError::BadRequest("max_size must be non-negative".into()));
    }
    if let (Some(min_size), Some(max_size)) = (params.min_size, params.max_size) {
        if max_size < min_size {
            return Err(ApiError::BadRequest(
                "max_size must be greater than or equal to min_size".into(),
            ));
        }
    }
    Ok(TorrentSearchFilters {
        complete_only: params.complete_only.unwrap_or(false),
        min_size: params.min_size,
        max_size: params.max_size,
    })
}

fn validate_info_hash_hex(hex_str: &str) -> Result<(), ApiError> {
    if hex_str.len() != 40 || !hex_str.chars().all(|c| c.is_ascii_hexdigit()) {
        Err(ApiError::BadRequest(
            "invalid info_hash: must be 40 hex characters".into(),
        ))
    } else {
        Ok(())
    }
}

#[axum::debug_handler]
async fn api_stats(State(state): State<AppState>) -> Result<Json<CrawlStats>, ApiError> {
    let stats = state
        .store
        .get_stats()
        .await
        .map_err(|error| state.database_error(error))?;
    Ok(Json(stats))
}

async fn api_search(
    State(state): State<AppState>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<SearchResponse>, ApiError> {
    let limit = clamp_limit(params.limit);
    let offset = clamp_offset(params.offset);
    let filters = validate_search_filters(&params)?;

    if let Some(ref sort) = params.sort {
        validate_sort(sort)?;
    }

    let query = params.q.unwrap_or_default();
    let sort = effective_sort(&query, params.sort.as_deref());

    crate::store::search_query(&query).map_err(ApiError::BadRequest)?;
    let page = state
        .store
        .search_torrents_filtered_page(&query, limit, offset, sort, filters)
        .await
        .map_err(|error| state.database_error(error))?;

    let results: Vec<SearchResult> = page.results.iter().map(torrent_to_search_result).collect();
    let has_next = page.has_next;
    Ok(Json(SearchResponse {
        results,
        limit,
        offset,
        has_next,
        effective_sort: sort.to_string(),
    }))
}

#[derive(serde::Serialize)]
struct TorrentDetail {
    info_hash: String,
    name: Option<String>,
    total_size: i64,
    file_count: i64,
    first_seen: i64,
    last_seen: i64,
    metadata_complete: bool,
    magnet: String,
}

#[derive(Debug, serde::Serialize)]
struct FileEntry {
    path: String,
    size: i64,
}

#[derive(Deserialize)]
struct FileSearchQuery {
    q: Option<String>,
    #[serde(default = "default_file_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
}

fn default_file_limit() -> usize {
    100
}

#[derive(Debug, serde::Serialize)]
struct FileSearchResponse {
    files: Vec<FileEntry>,
    limit: usize,
    offset: usize,
    has_next: bool,
}

async fn api_torrent(
    State(state): State<AppState>,
    Path(info_hash_hex): Path<String>,
) -> Result<Json<TorrentDetail>, ApiError> {
    validate_info_hash_hex(&info_hash_hex)?;

    let result = state
        .store
        .get_torrent_by_hex(&info_hash_hex)
        .await
        .map_err(|error| state.database_error(error))?;

    match result {
        Some(t) => {
            let hash_hex = hex::encode(t.info_hash);
            let dn = t.name.as_deref();
            Ok(Json(TorrentDetail {
                info_hash: hash_hex,
                name: t.name.clone(),
                total_size: t.total_size,
                file_count: t.file_count,
                first_seen: t.first_seen,
                last_seen: t.last_seen,
                metadata_complete: t.metadata_complete,
                magnet: magnet::magnet_uri_from_hash(&t.info_hash, dn),
            }))
        }
        None => Err(ApiError::NotFound("torrent not found".into())),
    }
}

async fn api_torrent_files(
    State(state): State<AppState>,
    Path(info_hash_hex): Path<String>,
    Query(params): Query<FileSearchQuery>,
) -> Result<Json<FileSearchResponse>, ApiError> {
    validate_info_hash_hex(&info_hash_hex)?;
    let hash_bytes: [u8; 20] = hex::decode(&info_hash_hex)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .try_into()
        .map_err(|_| ApiError::BadRequest("invalid hash length".into()))?;
    let limit = clamp_limit(params.limit);
    let offset = clamp_offset(params.offset);
    let query = params.q.unwrap_or_default();
    if state
        .store
        .get_torrent_by_hex(&info_hash_hex)
        .await
        .map_err(|error| state.database_error(error))?
        .is_none()
    {
        return Err(ApiError::NotFound("torrent not found".into()));
    }
    let page = state
        .store
        .search_files_page(&hash_bytes, &query, limit, offset)
        .await
        .map_err(|error| state.database_error(error))?;

    let files: Vec<FileEntry> = page
        .results
        .into_iter()
        .map(|(path, size)| FileEntry { path, size })
        .collect();
    let has_next = page.has_next;
    Ok(Json(FileSearchResponse {
        files,
        limit,
        offset,
        has_next,
    }))
}

async fn api_magnet(
    State(state): State<AppState>,
    Path(info_hash_hex): Path<String>,
) -> Result<axum::response::Response, ApiError> {
    validate_info_hash_hex(&info_hash_hex)?;

    let hash_bytes: [u8; 20] = hex::decode(&info_hash_hex)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .try_into()
        .map_err(|_| ApiError::BadRequest("invalid hash length".into()))?;

    let result = state
        .store
        .get_torrent_by_hex(&info_hash_hex)
        .await
        .map_err(|error| state.database_error(error))?;

    let dn = result.as_ref().and_then(|t| t.name.as_deref());
    let uri = magnet::magnet_uri_from_hash(&hash_bytes, dn);

    Ok(axum::response::Response::builder()
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(axum::body::Body::from(uri))
        .expect("static magnet response headers are valid"))
}

async fn request_deadline(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    match tokio::time::timeout(std::time::Duration::from_secs(10), next.run(request)).await {
        Ok(response) => response,
        Err(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            Json(json!({"error":"database request timed out"})),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_validation() {
        assert_eq!(effective_sort("東京", None), "rank");
        assert_eq!(effective_sort("", None), "last_seen");
        assert!(validate_info_hash_hex(&"a".repeat(40)).is_ok());
        assert!(validate_info_hash_hex("invalid").is_err());
        assert!(validate_sort("name; DROP TABLE torrents").is_err());
        assert_eq!(clamp_limit(0), 1);
        assert_eq!(clamp_limit(999), 500);
    }
    #[tokio::test]
    #[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
    async fn api_pagination_and_details() {
        let store = Store::open_test_database().await.unwrap();
        for n in 1..=2 {
            store
                .commit_metadata(
                    &[n; 20],
                    "東京 中文 Ubuntu",
                    16,
                    2,
                    &[("中文.iso".into(), 1), ("notes.txt".into(), 1)],
                )
                .await
                .unwrap();
        }
        let state = AppState {
            store,
            metrics: Arc::new(WebMetrics::default()),
        };
        let Json(response) = api_search(
            State(state.clone()),
            Query(SearchQuery {
                q: Some("中文".into()),
                limit: 1,
                offset: 0,
                sort: None,
                complete_only: Some(true),
                min_size: None,
                max_size: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(response.results.len(), 1);
        assert!(response.has_next);
        assert!(serde_json::to_value(&response)
            .unwrap()
            .get("total")
            .is_none());
        let Json(detail) = api_torrent(State(state.clone()), Path(hex::encode([1; 20])))
            .await
            .unwrap();
        assert!(serde_json::to_value(detail).unwrap().get("files").is_none());
        let Json(files) = api_torrent_files(
            State(state.clone()),
            Path(hex::encode([1; 20])),
            Query(FileSearchQuery {
                q: None,
                limit: 1,
                offset: 0,
            }),
        )
        .await
        .unwrap();
        assert!(files.has_next);
        assert_eq!(files.files.len(), 1);
        assert!(matches!(
            api_torrent(State(state), Path(hex::encode([99; 20]))).await,
            Err(ApiError::NotFound(_))
        ));
    }
}
