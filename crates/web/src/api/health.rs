use axum::http::StatusCode;

#[tracing::instrument]
pub async fn health_check() -> StatusCode {
    StatusCode::OK
}
