mod build_info;
mod config;
mod config_hot_reload;
mod consumer;
mod consumer_stats;
mod db;
mod db_inserts;
mod db_statements;
mod db_types;
mod filter;
mod filter_config;
mod prometheus;

use std::sync::Arc;

use crate::{
    build_info::get_build_info,
    config_hot_reload::async_watch,
    consumer::consumer,
    consumer_stats::ContextWithStats,
    db_types::{DbAccountInfo, DbBlockInfo, DbTransaction},
    filter::{block_filter, slot_filter, transaction_filter},
};
use clap::{Arg, Command};
use config::{env_build_config, AppConfig};
use crossbeam_queue::SegQueue;
use db::{db_stmt_executor, initialize_db_client};
use fast_log::{
    consts::LogSize,
    plugin::{file_split::RollingType, packer::LogPacker},
    Logger,
};
use filter::account_filter;
use filter_config::FilterConfig;
use kafka_common::kafka_structs::{
    NotifyBlockMetaData, NotifyTransaction, UpdateAccount, UpdateSlotStatus,
};
use log::info;
use prometheus::start_prometheus;
use tokio::{fs, sync::RwLock};

async fn run(mut config: AppConfig, filter_config: FilterConfig) {
    let logger: &'static Logger = fast_log::init(fast_log::Config::new().console().file_split(
        &config.filter_log_path,
        LogSize::KB(512),
        RollingType::All,
        LogPacker {},
    ))
    .expect("Failed to initialize fast_log");

    info!("{}", get_build_info());

    let prometheus_port = config
        .prometheus_port
        .parse()
        .unwrap_or_else(|e| panic!("Wrong prometheus port number, error: {e}"));

    let ctx_stats = ContextWithStats::default();

    let update_slot_topic = config
        .update_slot_topic
        .take()
        .expect("notify_slot_topic is not present in config");

    let update_account_topic = config
        .update_account_topic
        .take()
        .expect("update_account_topic is not present in config");

    let notify_transaction_topic = config
        .notify_transaction_topic
        .take()
        .expect("notify_transaction_topic is not present in config");

    let notify_block_topic = config
        .notify_block_topic
        .take()
        .expect("notify_block_topic is not present in config");

    let config = Arc::new(config);

    let prometheus = tokio::spawn(start_prometheus(
        ctx_stats.stats.clone(),
        update_account_topic.clone(),
        update_slot_topic.clone(),
        notify_transaction_topic.clone(),
        notify_block_topic.clone(),
        prometheus_port,
    ));

    let filter_config = Arc::new(RwLock::new(filter_config));

    let account_capacity = config.update_account_queue_capacity();
    let slot_capacity = config.update_slot_queue_capacity();
    let block_capacity = config.notify_block_queue_capacity();
    let transaction_capacity = config.notify_transaction_queue_capacity();

    let db_account_queue: Arc<SegQueue<DbAccountInfo>> = Arc::new(SegQueue::new());
    let db_slot_queue: Arc<SegQueue<UpdateSlotStatus>> = Arc::new(SegQueue::new());
    let db_block_queue: Arc<SegQueue<DbBlockInfo>> = Arc::new(SegQueue::new());
    let db_transaction_queue: Arc<SegQueue<DbTransaction>> = Arc::new(SegQueue::new());

    logger.set_level((&config.global_log_level).into());

    let client = initialize_db_client(config.clone()).await;

    let (filter_account_tx, filter_account_rx) = flume::bounded::<UpdateAccount>(account_capacity);
    let (filter_slot_tx, filter_slot_rx) = flume::bounded::<UpdateSlotStatus>(slot_capacity);
    let (filter_transaction_tx, filter_transaction_rx) =
        flume::bounded::<NotifyTransaction>(transaction_capacity);
    let (filter_block_tx, filter_block_rx) = flume::bounded::<NotifyBlockMetaData>(block_capacity);

    let cfg_watcher = tokio::spawn(async_watch(config.clone(), filter_config.clone()));

    let account_filter = tokio::spawn(account_filter(
        filter_config.clone(),
        db_account_queue.clone(),
        filter_account_rx,
    ));

    let transaction_filter = tokio::spawn(transaction_filter(
        db_transaction_queue.clone(),
        filter_transaction_rx,
    ));

    let block_filter = tokio::spawn(block_filter(db_block_queue.clone(), filter_block_rx));

    let slot_filter = tokio::spawn(slot_filter(db_slot_queue.clone(), filter_slot_rx));

    let consumer_update_account = tokio::spawn(consumer(
        config.clone(),
        update_account_topic,
        filter_account_tx,
        ctx_stats.clone(),
    ));

    let consumer_update_slot = tokio::spawn(consumer(
        config.clone(),
        update_slot_topic,
        filter_slot_tx,
        ctx_stats.clone(),
    ));

    let consumer_transaction = tokio::spawn(consumer(
        config.clone(),
        notify_transaction_topic,
        filter_transaction_tx,
        ctx_stats.clone(),
    ));

    let consumer_notify_block = tokio::spawn(consumer(
        config.clone(),
        notify_block_topic,
        filter_block_tx,
        ctx_stats.clone(),
    ));

    let db_stmt_executor = tokio::spawn(db_stmt_executor(
        config.clone(),
        client,
        ctx_stats.stats.clone(),
        db_account_queue,
        db_slot_queue,
        db_transaction_queue,
        db_block_queue,
    ));

    let _ = tokio::join!(
        consumer_update_account,
        consumer_update_slot,
        consumer_transaction,
        consumer_notify_block,
        account_filter,
        transaction_filter,
        block_filter,
        slot_filter,
        db_stmt_executor,
        prometheus,
        cfg_watcher
    );
}

#[tokio::main]
async fn main() {
    let app = Command::new("geyser-neon-filter")
        .version("1.0")
        .about("Neonlabs filtering service")
        .arg(
            Arg::new("config")
                .short('c')
                .required(false)
                .long("config")
                .value_name("Config path")
                .help("Sets the path to the config file"),
        )
        .arg(
            Arg::new("filter-config")
                .short('f')
                .required(false)
                .long("fconfig")
                .value_name("Filter config path")
                .help("Sets the path to the filter config"),
        )
        .get_matches();

    let (app_config, filter_config) = match (
        app.get_one::<String>("config"),
        app.get_one::<String>("filter-config"),
    ) {
        (Some(config_path), Some(filter_config_path)) => {
            let contents = fs::read_to_string(config_path)
                .await
                .unwrap_or_else(|e| panic!("Failed to read config: {config_path}, error: {e}"));
            let app_config = serde_json::from_str(&contents)
                .unwrap_or_else(|e| panic!("Failed to parse config: {config_path}, error: {e}"));
            let contents = fs::read_to_string(filter_config_path)
                .await
                .unwrap_or_else(|e| {
                    panic!("Failed to read filter config: {filter_config_path}, error: {e}")
                });
            let filter_config = serde_json::from_str(&contents).unwrap_or_else(|e| {
                panic!("Failed to parse filter config: {filter_config_path}, error: {e}")
            });
            (app_config, filter_config)
        }
        _ => env_build_config(),
    };

    run(app_config, filter_config).await;
}
