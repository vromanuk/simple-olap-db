use olap_web::configuration::get_configuration;
use olap_web::startup::Application;
use olap_web::telemetry::init_tracing;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_tracing();

    let settings = get_configuration().expect("failed to read configuration");
    let app = Application::build(&settings).await?;

    tracing::info!(
        "listening on http://{}:{}",
        settings.application.host,
        app.port()
    );

    app.run().await
}
