use axum::http::StatusCode;
use olap_engine::query_engine::EngineError;

pub fn to_http_error(err: EngineError) -> (StatusCode, String) {
    let status = match &err {
        EngineError::InvalidSql(_) => StatusCode::BAD_REQUEST,
        EngineError::Execution(_) => StatusCode::BAD_REQUEST,
        EngineError::DeltaTable(_) => StatusCode::INTERNAL_SERVER_ERROR,
        EngineError::Table(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, err.to_string())
}
