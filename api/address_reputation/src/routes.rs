// Copyright © MoveIndustries
// SPDX-License-Identifier: Apache-2.0

use crate::db::{self, DbPool};
use axum::{
    extract::{Query, Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use std::{collections::HashSet, sync::Arc};
use tracing::{error, info, warn};

// ---------------------------------------------------------------------------
// Address validation
// ---------------------------------------------------------------------------

/// Movement (Aptos) address: `0x` + exactly 64 lowercase or uppercase hex chars.
fn is_valid_mvt_address(addr: &str) -> bool {
    addr.len() == 66 && addr.starts_with("0x") && addr[2..].bytes().all(|b| b.is_ascii_hexdigit())
}

/// EVM address: `0x` + exactly 40 lowercase or uppercase hex chars.
fn is_valid_evm_address(addr: &str) -> bool {
    addr.len() == 42 && addr.starts_with("0x") && addr[2..].bytes().all(|b| b.is_ascii_hexdigit())
}

fn bad_address(addr: &str, kind: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "error": format!("invalid {kind} address: {addr:?}")
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Shared application state
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AppState {
    pub pool: Arc<DbPool>,
    pub api_keys: Arc<HashSet<String>>,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn build_router(state: AppState) -> Router {
    // Protected routes require a valid X-Api-Key header.
    let protected = Router::new()
        .route("/v1/reputation/address/since", get(handle_since))
        .route("/v1/reputation/address/mvt_fetch", post(handle_mvt_fetch))
        .route("/v1/reputation/score/evms", post(handle_evms))
        .route("/v1/reputation/score/mvts", post(handle_mvts))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    // Health check is public — no API key required.
    Router::new()
        .route("/v1/reputation/health", get(handle_health))
        .merge(protected)
        .with_state(state)
}

// ---------------------------------------------------------------------------
// API key middleware
// ---------------------------------------------------------------------------

pub async fn auth_middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let key = req.headers().get("X-Api-Key").and_then(|v| v.to_str().ok());

    if key.is_some_and(|k| state.api_keys.contains(k)) {
        next.run(req).await
    } else {
        warn!(
            path = %req.uri().path(),
            key_present = key.is_some(),
            "auth: rejected request — invalid or missing X-Api-Key"
        );
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        )
            .into_response()
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct SinceParams {
    since: i64,
}

async fn handle_since(
    State(state): State<AppState>,
    Query(params): Query<SinceParams>,
) -> Response {
    let since_dt = match chrono::DateTime::from_timestamp(params.since, 0) {
        Some(dt) => dt.naive_utc(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid 'since' unix timestamp"})),
            )
                .into_response()
        },
    };

    match db::query_since(&state.pool, since_dt).await {
        Ok(rows) => {
            info!(
                since = params.since,
                count = rows.len(),
                "address/since: returning {} rows",
                rows.len()
            );
            Json(&rows).into_response()
        },
        Err(e) => {
            error!(err = %e, "address/since: query failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        },
    }
}

async fn handle_mvt_fetch(
    State(state): State<AppState>,
    Json(addrs): Json<Vec<String>>,
) -> Response {
    if addrs.is_empty() {
        return Json(serde_json::json!([])).into_response();
    }
    if let Some(bad) = addrs.iter().find(|a| !is_valid_mvt_address(a)) {
        return bad_address(bad, "movement");
    }

    let input_count = addrs.len();
    match db::query_mvt_fetch(&state.pool, addrs).await {
        Ok(rows) => {
            info!(
                input_count,
                row_count = rows.len(),
                "address/mvt_fetch: returning {} rows for {} input addresses",
                rows.len(),
                input_count
            );
            Json(&rows).into_response()
        },
        Err(e) => {
            error!(err = %e, "address/mvt_fetch: query failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        },
    }
}

async fn handle_evms(State(state): State<AppState>, Json(addrs): Json<Vec<String>>) -> Response {
    if addrs.is_empty() {
        return Json(serde_json::json!([])).into_response();
    }
    if let Some(bad) = addrs.iter().find(|a| !is_valid_evm_address(a)) {
        return bad_address(bad, "evm");
    }

    let input_count = addrs.len();
    match db::query_evms(&state.pool, addrs).await {
        Ok(rows) => {
            info!(
                input_count,
                row_count = rows.len(),
                "score/evms: returning {} rows for {} input addresses",
                rows.len(),
                input_count
            );
            Json(&rows).into_response()
        },
        Err(e) => {
            error!(err = %e, "score/evms: query failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        },
    }
}

async fn handle_health(State(state): State<AppState>) -> Response {
    match db::ping(&state.pool).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})).into_response(),
        Err(e) => {
            error!(err = %e, "health: DB ping failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"status": "error", "detail": "db unavailable"})),
            )
                .into_response()
        },
    }
}

async fn handle_mvts(State(state): State<AppState>, Json(addrs): Json<Vec<String>>) -> Response {
    if addrs.is_empty() {
        return Json(serde_json::json!([])).into_response();
    }
    if let Some(bad) = addrs.iter().find(|a| !is_valid_mvt_address(a)) {
        return bad_address(bad, "movement");
    }

    let input_count = addrs.len();
    match db::query_mvt_scores(&state.pool, addrs).await {
        Ok(rows) => {
            info!(
                input_count,
                row_count = rows.len(),
                "score/mvts: returning {} rows for {} input addresses",
                rows.len(),
                input_count
            );
            Json(&rows).into_response()
        },
        Err(e) => {
            error!(err = %e, "score/mvts: query failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        },
    }
}
