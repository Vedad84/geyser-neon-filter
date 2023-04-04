mod client;
mod config;
mod executor;

use std::sync::Arc;

use clap::{Arg, Command};
use config::Config;
use executor::start_tasks;
use fast_log::consts::LogSize;
use fast_log::plugin::file_split::RollingType;
use fast_log::plugin::packer::LogPacker;
use fast_log::Logger;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::signal::unix::{signal, SignalKind};

use log::{info, Log};

async fn read_file_to_string(path: &str) -> Result<String, std::io::Error> {
    let mut file = File::open(path).await?;
    let mut data = String::new();
    file.read_to_string(&mut data).await?;
    Ok(data)
}

#[tokio::main]
async fn main() {
    let app = Command::new("clickhouse-scheduler")
        .version("1.0")
        .about("Neonlabs clickhouse scheduler utility")
        .arg(
            Arg::new("config")
                .short('c')
                .required(true)
                .long("config")
                .value_parser(clap::value_parser!(String))
                .help("Path to the config"),
        )
        .get_matches();

    let config_path = app
        .get_one::<String>("config")
        .expect("Config path is required");

    let data = read_file_to_string(config_path)
        .await
        .unwrap_or_else(|e| panic!("Unable to read the config file {config_path}, error: {e}"));

    let config: Config = serde_json::from_str(&data).expect("Unable to parse JSON config");

    let logger: &'static Logger = fast_log::init(fast_log::Config::new().console().file_split(
        &config.log_path,
        LogSize::KB(512),
        RollingType::All,
        LogPacker {},
    ))
    .expect("Failed to initialize fast_log");

    logger.set_level(config.log_level.clone().into());

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

    let sigterm_handler = tokio::spawn(async move {
        let mut signal =
            signal(SignalKind::terminate()).expect("Unable to register SIGTERM handler");
        signal.recv().await;
        info!("SIGTERM received, shutting down");
        let _ = shutdown_tx.send(());
    });

    start_tasks(Arc::new(config), shutdown_rx).await;

    let _ = sigterm_handler.await;

    logger.flush();
}
