mod client;
mod config;
mod executor;
mod prometheus;

use clap::{Arg, Command};
use config::Config;
use executor::add_tasks;
use fast_log::consts::LogSize;
use fast_log::plugin::file_split::RollingType;
use fast_log::plugin::packer::LogPacker;
use fast_log::Logger;
use log::{info, Log};
use prometheus::{start_prometheus, TaskMetric};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::signal::unix::{signal, SignalKind};
use tokio_cron_scheduler::JobScheduler;

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

    let (prometheus_down_tx, prometheus_down_rx) = tokio::sync::oneshot::channel::<()>();

    let sigterm_handler = tokio::spawn(async move {
        let mut signal =
            signal(SignalKind::terminate()).expect("Unable to register SIGTERM handler");
        signal.recv().await;

        info!("SIGTERM received, shutting down");

        prometheus_down_tx.send(()).ok();
        let _ = shutdown_tx.send(());
    });

    let (task_names, task_metrics): (Vec<String>, Vec<TaskMetric>) = config
        .tasks
        .iter()
        .map(|task| (task.task_name.clone(), TaskMetric::default()))
        .unzip();

    let prometheus_port = config
        .prometheus_port
        .parse()
        .unwrap_or_else(|e| panic!("Wrong prometheus port number, error: {e}"));

    let prometheus_handler = tokio::spawn(start_prometheus(
        task_metrics.clone(),
        task_names,
        prometheus_port,
        prometheus_down_rx,
    ));

    let mut sched = JobScheduler::new()
        .await
        .expect("Failed to create scheduler");

    add_tasks(&mut sched, Arc::new(config), shutdown_rx, task_metrics).await;

    sched.shutdown_on_signal(SignalKind::terminate());

    sched.start().await.expect("Failed to start scheduler");

    let _ = prometheus_handler.await;

    let _ = sigterm_handler.await;

    logger.flush();
}
