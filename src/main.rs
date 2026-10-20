use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "hie",
    version,
    about = "Helios Integration Engine: feeds into HFS"
)]
struct Cli {
    /// Engine configuration file.
    #[arg(long, short, default_value = "hie.yaml", env = "HIE_CONFIG")]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Watch every feed and serve the API (default).
    Run,
    /// Load the configuration and feeds, report problems, and exit.
    Check,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("HIE_LOG")
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    let (config, feeds) = hie::config::load(&cli.config)?;
    let runtime_dir = std::env::var_os("HIE_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            cli.config
                .parent()
                .map(|p| p.join("hie_runtime"))
                .unwrap_or_else(|| "hie_runtime".into())
        });

    if matches!(cli.command, Some(Command::Check)) {
        println!(
            "config ok: {} source(s), HFS at {}",
            feeds.len(),
            config.fhir_base_url
        );
        for f in &feeds {
            println!(
                "  {} ({}) <- {} via {}",
                f.id,
                f.name,
                f.source.describe(),
                f.transform.display()
            );
        }
        return Ok(());
    }

    let auth = hie::auth::build(&hie::auth::AuthSettings::from_env()).await?;
    match &auth {
        Some(_) => tracing::info!("API authentication on: bearer tokens required under /api"),
        None => tracing::warn!(
            "API authentication is off (HIE_AUTH_ENABLED is not true): anyone who can reach the API can change sources and transforms"
        ),
    }
    let (_manager, state) = hie::build(&config, feeds, &runtime_dir, auth)?;
    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    tracing::info!("API on http://{}", config.listen);
    axum::serve(listener, hie::api::router(state)).await?;
    Ok(())
}
