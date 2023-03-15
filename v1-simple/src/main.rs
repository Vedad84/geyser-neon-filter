mod app_config;
mod build_info;
mod consumer;
mod consumer_stats;
mod db;
mod db_inserts;
mod db_statements;
mod db_types;
mod filter;
mod filter_config;
mod filter_config_hot_reload;
mod prometheus;

use std::sync::Arc;

use crate::{
    build_info::get_build_info,
    consumer::consumer,
    consumer_stats::ContextWithStats,
    db::create_db_pool,
    db_types::{DbAccountInfo, DbBlockInfo, DbTransaction},
    filter_config_hot_reload::async_watch,
};
use app_config::{env_build_config, AppConfig};
use clap::{Arg, Command};
use crate::db::{
    db_stmt_executor,
    exec_account_statement,
    exec_block_statement,
    exec_slot_statement,
    exec_transaction_statement,
};
use fast_log::{
    consts::LogSize,
    plugin::{file_split::RollingType, packer::LogPacker},
    Logger,
};
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

    logger.set_level((&config.global_log_level).into());

    let db_pool = create_db_pool(config.clone())
        .await
        .unwrap_or_else(|e| panic!("Failed to create db pool, error: {e}"));

    let (account_tx, account_rx) = flume::bounded::<DbAccountInfo>(account_capacity);
    let (slot_tx, slot_rx) = flume::bounded::<UpdateSlotStatus>(slot_capacity);
    let (transaction_tx, transaction_rx) = flume::bounded::<DbTransaction>(transaction_capacity);
    let (block_tx, block_rx) = flume::bounded::<DbBlockInfo>(block_capacity);

    let cfg_watcher = tokio::spawn(async_watch(config.clone(), filter_config.clone()));

    let consumer_update_account = tokio::spawn(consumer::<UpdateAccount, DbAccountInfo>(
        config.clone(),
        filter_config.clone(),
        update_account_topic,
        account_tx.clone(),
        ctx_stats.clone(),
    ));

    let consumer_update_slot = tokio::spawn(consumer(
        config.clone(),
        filter_config.clone(),
        update_slot_topic,
        slot_tx.clone(),
        ctx_stats.clone(),
    ));

    let consumer_transaction = tokio::spawn(consumer::<NotifyTransaction, DbTransaction>(
        config.clone(),
        filter_config.clone(),
        notify_transaction_topic,
        transaction_tx.clone(),
        ctx_stats.clone(),
    ));

    let consumer_notify_block = tokio::spawn(consumer::<NotifyBlockMetaData, DbBlockInfo>(
        config.clone(),
        filter_config.clone(),
        notify_block_topic,
        block_tx.clone(),
        ctx_stats.clone(),
    ));

    let account_db_stmt_executor = tokio::spawn(db_stmt_executor(
        Arc::clone(&db_pool),
        Arc::clone(&ctx_stats.stats),
        account_tx,
        account_rx,
        move |client, stats, queue_tx, queue_rx| {
            async move {
                let channel_len = exec_account_statement(client, Arc::clone(&stats), queue_tx, queue_rx).await;
                stats.queue_len_update_account.set(channel_len as f64);
            }
        }
    ));

    let slot_db_stmt_executor = tokio::spawn(db_stmt_executor(
        Arc::clone(&db_pool),
        Arc::clone(&ctx_stats.stats),
        slot_tx,
        slot_rx,
        move |client, stats, queue_tx, queue_rx| {
            async move {
                let channel_len = exec_slot_statement(client, Arc::clone(&stats), queue_tx, queue_rx).await;
                stats.queue_len_update_slot.set(channel_len as f64);
            }
        }
    ));

    let transaction_db_stmt_executor = tokio::spawn(db_stmt_executor(
        Arc::clone(&db_pool),
        Arc::clone(&ctx_stats.stats),
        transaction_tx,
        transaction_rx,
        move |client, stats, queue_tx, queue_rx| {
            async move {
                let channel_len = exec_transaction_statement(client, Arc::clone(&stats), queue_tx, queue_rx).await;
                stats.queue_len_notify_transaction.set(channel_len as f64);
            }
        }
    ));

    let block_db_stmt_executor = tokio::spawn(db_stmt_executor(
        Arc::clone(&db_pool),
        Arc::clone(&ctx_stats.stats),
        block_tx,
        block_rx,
        move |client, stats, queue_tx, queue_rx| {
            async move {
                let channel_len = exec_block_statement(client, stats.clone(), queue_tx, queue_rx).await;
                stats.queue_len_notify_block.set(channel_len as f64);
            }
        }
    ));

    let _ = tokio::join!(
        consumer_update_account,
        consumer_update_slot,
        consumer_transaction,
        consumer_notify_block,
        account_db_stmt_executor,
        slot_db_stmt_executor,
        transaction_db_stmt_executor,
        block_db_stmt_executor,
        prometheus,
        cfg_watcher,
    );
}

#[tokio::main]
async fn main() {
    let app = Command::new("geyser-neon-filter")
        .version("1.0")
        .about("Neon Kafka filtering service")
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
