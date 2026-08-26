use anyhow::{Context, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = match queryflux_cli::run_cli().await? {
        queryflux_cli::CliAction::Exit => return Ok(()),
        queryflux_cli::CliAction::Migrate { config } => {
            return queryflux::run_migration(&config).await;
        }
        queryflux_cli::CliAction::Serve { config } => config,
    };

    queryflux::QueryFlux::builder()
        .config_path(config_path)
        .with_builtin_plugins()
        .build()
        .await
        .context("Failed to build QueryFlux")?
        .serve()
        .await
}
