use std::net::SocketAddr;

use open_workshop_storage::config::AppConfig;
use open_workshop_storage::runtime::AppState;
use open_workshop_storage::web::build_loader_router;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let config = AppConfig::load();
    let state = AppState::new(config)?;
    let app = build_loader_router(state);
    let host = std::env::var("OPEN_WORKSHOP_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("OPEN_WORKSHOP_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8001);
    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
