use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::Span;

use olap_engine::query_engine::QueryEngine;

use crate::api;
use crate::configuration::Settings;

pub struct Application {
    listener: TcpListener,
    router: Router,
}

impl Application {
    pub async fn build(settings: &Settings, engine: QueryEngine) -> std::io::Result<Self> {
        let addr = format!(
            "{}:{}",
            settings.application.host, settings.application.port
        );
        let listener = TcpListener::bind(&addr).await?;

        let shared_engine = Arc::new(engine);

        let router = Router::new()
            .route("/health", get(api::health_check))
            .route("/query", post(api::query_handler))
            .route("/explain", post(api::explain_handler))
            .route("/tables", get(api::list_tables).post(api::register_table))
            .route("/tables/{name}/schema", get(api::table_schema))
            .route("/tables/{name}/stats", get(api::table_stats))
            .route("/tables/{name}/optimize", post(api::optimize_handler))
            .route("/tables/{name}/vacuum", post(api::vacuum_handler))
            .with_state(shared_engine)
            .layer(
                TraceLayer::new_for_http()
                    .on_request(|req: &axum::http::Request<_>, _span: &Span| {
                        tracing::info!(method = %req.method(), uri = %req.uri(), "request");
                    })
                    .on_response(
                        |res: &axum::http::Response<_>,
                         latency: std::time::Duration,
                         _span: &Span| {
                            let status = res.status().as_u16();
                            if status >= 500 {
                                tracing::error!(status, latency_ms = ?latency, "response");
                            } else if status >= 400 {
                                tracing::warn!(status, latency_ms = ?latency, "response");
                            } else {
                                tracing::info!(status, latency_ms = ?latency, "response");
                            }
                        },
                    ),
            );

        Ok(Self { listener, router })
    }

    pub fn port(&self) -> u16 {
        self.listener.local_addr().unwrap().port()
    }

    pub async fn run(self) -> std::io::Result<()> {
        axum::serve(self.listener, self.router)
            .with_graceful_shutdown(shutdown_signal())
            .await
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install signal handler");
    tracing::info!("shutdown signal received, starting graceful shutdown");
}
