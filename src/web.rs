use crate::magnet;
use crate::store::{CrawlStats, Store, TorrentRecord, TorrentSearchFilters};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use std::path::PathBuf;

#[derive(Clone)]
pub struct AppState {
    pub db_path: PathBuf,
}

#[derive(Debug)]
enum ApiError {
    BadRequest(String),
    NotFound(String),
    Db(String),
    Join(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::Db(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            ApiError::Join(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

impl From<rusqlite::Error> for ApiError {
    fn from(e: rusqlite::Error) -> Self {
        ApiError::Db(e.to_string())
    }
}

impl From<String> for ApiError {
    fn from(s: String) -> Self {
        ApiError::BadRequest(s)
    }
}

pub fn router(db_path: PathBuf) -> axum::Router {
    let state = AppState { db_path };

    axum::Router::new()
        .route("/", get(dashboard))
        .route("/api/stats", get(api_stats))
        .route("/api/search", get(api_search))
        .route("/api/torrents/{info_hash}", get(api_torrent))
        .route("/api/torrents/{info_hash}/magnet", get(api_magnet))
        .with_state(state)
}

async fn dashboard() -> axum::response::Html<String> {
    axum::response::Html(include_str!("web_dashboard.html").to_string())
}

#[derive(Deserialize)]
struct SearchQuery {
    q: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
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
    let db_path = state.db_path.clone();
    let stats = tokio::task::spawn_blocking(move || -> Result<CrawlStats, ApiError> {
        let store = Store::open(&db_path)?;
        Ok(store.get_stats()?)
    })
    .await
    .map_err(|e| ApiError::Join(e.to_string()))??;
    Ok(Json(stats))
}

async fn api_search(
    State(state): State<AppState>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<Vec<SearchResult>>, ApiError> {
    let limit = clamp_limit(params.limit);
    let filters = validate_search_filters(&params)?;

    if let Some(ref sort) = params.sort {
        validate_sort(sort)?;
    }

    let db_path = state.db_path.clone();
    let results = tokio::task::spawn_blocking(move || -> Result<Vec<TorrentRecord>, ApiError> {
        let store = Store::open(&db_path)?;
        let query = params.q.as_deref().unwrap_or("");
        let sort = params.sort.as_deref().unwrap_or(if query.is_empty() {
            "last_seen"
        } else {
            "rank"
        });
        Ok(store.search_torrents_filtered(query, limit, sort, filters)?)
    })
    .await
    .map_err(|e| ApiError::Join(e.to_string()))??;

    let results: Vec<SearchResult> = results.iter().map(torrent_to_search_result).collect();
    Ok(Json(results))
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
    files: Vec<FileEntry>,
}

#[derive(serde::Serialize)]
struct FileEntry {
    path: String,
    size: i64,
}

async fn api_torrent(
    State(state): State<AppState>,
    Path(info_hash_hex): Path<String>,
) -> Result<Json<TorrentDetail>, ApiError> {
    validate_info_hash_hex(&info_hash_hex)?;

    let db_path = state.db_path.clone();
    let info_hash_hex2 = info_hash_hex.clone();
    let result = tokio::task::spawn_blocking(
        move || -> Result<(Option<TorrentRecord>, Vec<FileEntry>), ApiError> {
            let store = Store::open(&db_path)?;
            let torrent = store.get_torrent_by_hex(&info_hash_hex2)?;
            let files = match torrent {
                Some(ref t) => {
                    let file_pairs = store.get_files(&t.info_hash)?;
                    file_pairs
                        .into_iter()
                        .map(|(path, size)| FileEntry { path, size })
                        .collect()
                }
                None => Vec::new(),
            };
            Ok((torrent, files))
        },
    )
    .await
    .map_err(|e| ApiError::Join(e.to_string()))??;

    match result {
        (Some(t), files) => {
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
                files,
            }))
        }
        (None, _) => Err(ApiError::NotFound("torrent not found".into())),
    }
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

    let db_path = state.db_path.clone();
    let info_hash_hex2 = info_hash_hex;
    let result = tokio::task::spawn_blocking(move || -> Result<Option<TorrentRecord>, ApiError> {
        let store = Store::open(&db_path)?;
        Ok(store.get_torrent_by_hex(&info_hash_hex2)?)
    })
    .await
    .map_err(|e| ApiError::Join(e.to_string()))??;

    let dn = result.as_ref().and_then(|t| t.name.as_deref());
    let uri = magnet::magnet_uri_from_hash(&hash_bytes, dn);

    Ok(axum::response::Response::builder()
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(axum::body::Body::from(uri))
        .unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{Query, State};
    use std::fs;

    fn temp_db_path(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("btfind-web-test-{name}-{}.db", std::process::id()));
        let _ = fs::remove_file(&path);
        path
    }

    fn seed_web_search_db(path: &PathBuf) {
        let store = Store::open(path).unwrap();
        let incomplete = [0xD0u8; 20];
        let complete = [0xD1u8; 20];

        store
            .upsert_torrent(incomplete, Some("ubuntu unknown".into()), 999, 0, "dht")
            .unwrap();
        store
            .upsert_torrent(complete, Some("ubuntu complete".into()), 2_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&complete, "ubuntu complete", 262144, 2_000, 1)
            .unwrap();
        store
            .insert_files(&complete, &[("ubuntu.iso".into(), 2_000)])
            .unwrap();
    }

    #[tokio::test]
    async fn test_api_search_complete_only_excludes_incomplete_torrents() {
        let db_path = temp_db_path("complete-only");
        seed_web_search_db(&db_path);

        let Json(results) = api_search(
            State(AppState {
                db_path: db_path.clone(),
            }),
            Query(SearchQuery {
                q: Some("ubuntu".into()),
                limit: 10,
                sort: Some("last_seen".into()),
                complete_only: Some(true),
                min_size: None,
                max_size: None,
            }),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("ubuntu complete"));
        let _ = fs::remove_file(db_path);
    }

    #[tokio::test]
    async fn test_api_search_rejects_invalid_size_range() {
        let db_path = temp_db_path("invalid-size-range");
        seed_web_search_db(&db_path);

        let result = api_search(
            State(AppState {
                db_path: db_path.clone(),
            }),
            Query(SearchQuery {
                q: Some("ubuntu".into()),
                limit: 10,
                sort: Some("last_seen".into()),
                complete_only: Some(true),
                min_size: Some(5_000),
                max_size: Some(1_000),
            }),
        )
        .await;

        match result {
            Err(ApiError::BadRequest(message)) => {
                assert_eq!(
                    message,
                    "max_size must be greater than or equal to min_size"
                );
            }
            other => panic!("expected bad request, got {other:?}"),
        }
        let _ = fs::remove_file(db_path);
    }
}
