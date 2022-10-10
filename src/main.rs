mod config;
mod consumer;
mod db;
mod filter;

use std::sync::Arc;

use crate::consumer::consumer;
use config::FilterConfig;
use crossbeam_queue::SegQueue;
use db::{db_statement_executor, initialize_db_client, DbAccountInfo};
use fast_log::{
    consts::LogSize,
    plugin::{file_split::RollingType, packer::LogPacker},
    Config, Logger,
};
use filter::filter;
use log::error;
use tokio::fs;

#[tokio::main]
async fn main() {
    let logger: &'static Logger = fast_log::init(Config::new().console().file_split(
        "/var/logs/neon_filter.log",
        LogSize::KB(512),
        RollingType::All,
        LogPacker {},
    ))
    .expect("Failed to initialize fast_log");

    let contents = fs::read_to_string("filter_config.json")
        .await
        .expect("Failed to read filter_config.json");

    let result: serde_json::Result<FilterConfig> = serde_json::from_str(&contents);
    match result {
        Ok(config) => {
            let config = Arc::new(config);
            let db_queue: Arc<SegQueue<DbAccountInfo>> = Arc::new(SegQueue::new());

            logger.set_level((&config.global_log_level).into());

            let client = initialize_db_client(config.clone()).await;

            let (filter_tx, filter_rx) = flume::unbounded();
            let filter_loop_handle =
                tokio::spawn(filter(config.clone(), db_queue.clone(), filter_rx));
            let consumer_loop_handle = tokio::spawn(consumer(config.clone(), filter_tx));
            let db_statement_executor_handle =
                tokio::spawn(db_statement_executor(config, client, db_queue));

            let _ = consumer_loop_handle.await;
            let _ = filter_loop_handle.await;
            let _ = db_statement_executor_handle.await;
        }
        Err(e) => error!("Failed to parse filter config, error {e}"),
    }
}
