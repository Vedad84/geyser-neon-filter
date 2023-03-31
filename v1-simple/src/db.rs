use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use anyhow::Result;

use deadpool_postgres::Client;
use deadpool_postgres::Manager;
use deadpool_postgres::ManagerConfig;
use deadpool_postgres::Pool;
use deadpool_postgres::RecyclingMethod;
use flume::{Receiver, Sender};
use kafka_common::kafka_structs::UpdateSlotStatus;
use log::error;
use log::info;
use prometheus_client::metrics::gauge::Gauge;
use rdkafka::consumer::StreamConsumer;
use tokio::select;
use tokio::sync::watch;
use tokio_postgres::NoTls;

use crate::app_config::AppConfig;
use crate::consumer::QueueMsg;
use crate::consumer_stats::{ContextWithStats, RAIICounter, Stats};

use crate::db_inserts::insert_into_account_audit;
use crate::db_inserts::insert_into_block_metadata;
use crate::db_inserts::insert_into_transaction;
use crate::db_inserts::insert_slot_status_internal;
use crate::db_statements::create_account_insert_statement;
use crate::db_statements::create_block_metadata_insert_statement;
use crate::db_statements::create_slot_insert_statement_with_parent;
use crate::db_statements::create_slot_insert_statement_without_parent;
use crate::db_statements::create_transaction_insert_statement;
use crate::db_types::DbAccountInfo;
use crate::db_types::DbBlockInfo;
use crate::db_types::DbTransaction;
use crate::offset_manager::{offset_manager_service, OffsetManagerCommand};

pub async fn create_db_pool(config: Arc<AppConfig>) -> Result<Arc<Pool>> {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config.host(config.postgres_host.as_str());
    pg_config.port(config.postgres_port.parse::<u16>()?);
    pg_config.user(config.postgres_user.as_str());
    pg_config.password(config.postgres_password.as_str());
    pg_config.dbname(config.postgres_db_name.as_str());
    pg_config.keepalives_idle(Duration::from_secs(3));

    let pool_size = config.postgres_pool_size.parse().unwrap_or(96);

    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Verified,
    };

    let mgr = Manager::from_config(pg_config, NoTls, mgr_config);
    let pool = Pool::builder(mgr)
        .max_size(pool_size)
        .runtime(deadpool_postgres::Runtime::Tokio1)
        .wait_timeout(Some(Duration::from_secs(10)))
        .build()
        .expect("Failed to create Postgres pool");

    {
        let mut clients = vec![];
        for _ in 0..pool.status().max_size {
            let client = pool.get().await.unwrap_or_else(|e| {
                panic!("Failed to get a client to the database, error: {e}");
            });
            clients.push(client);
        }
    }

    if pool.status().available <= 0 {
        anyhow::bail!("No available connections in the pool");
    }

    info!(
        "Created a database pool with {} connections",
        pool.status().available
    );

    Ok(Arc::new(pool))
}

pub async fn exec_account_statement(
    client: Arc<Client>,
    db_account_info: Arc<DbAccountInfo>,
) -> Result<u64> {
    let statement = create_account_insert_statement(&client).await?;
    insert_into_account_audit(db_account_info, &statement, &client).await
}

pub async fn exec_transaction_statement(
    client: Arc<Client>,
    db_transaction_info: Arc<DbTransaction>,
) -> Result<u64> {
    let statement = create_transaction_insert_statement(&client).await?;
    insert_into_transaction(db_transaction_info, &statement, &client).await
}

pub async fn exec_block_statement(
    client: Arc<Client>,
    db_block_info: Arc<DbBlockInfo>,
) -> Result<u64> {
    let statement = create_block_metadata_insert_statement(&client).await?;
    insert_into_block_metadata(db_block_info, &statement, &client).await
}

pub async fn exec_slot_statement(
    client: Arc<Client>,
    db_slot_info: Arc<UpdateSlotStatus>,
) -> Result<u64> {
    let statement = match db_slot_info.parent {
        Some(_) => create_slot_insert_statement_with_parent(&client).await,
        None => create_slot_insert_statement_without_parent(&client).await,
    }?;

    insert_slot_status_internal(db_slot_info, &statement, &client).await
}

#[allow(clippy::too_many_arguments)]
pub async fn db_stmt_executor<M, F>(
    consumer: Arc<StreamConsumer<ContextWithStats>>,
    topic: String,
    db_pool: Arc<Pool>,
    stats: Arc<Stats>,
    queue_rx: Receiver<QueueMsg<M>>,
    queue_len_gauge: Gauge<f64, AtomicU64>,
    sigterm_rx: watch::Receiver<()>,
    process_msg_async: fn (Arc<Client>, Arc<M>) -> F,
) where
    M: Send + Sync + 'static,
    F: Future<Output = Result<u64>> + Send + 'static,
{
    let (offsets_tx, offsets_rx) = flume::unbounded::<OffsetManagerCommand>();

    tokio::spawn(offset_manager_service(topic.clone(), Arc::clone(&consumer), offsets_rx));

    while let Ok((message, offset)) = queue_rx.recv_async().await {
        queue_len_gauge.set(queue_rx.len() as f64);
        if let Err(err) = offsets_tx.send_async(OffsetManagerCommand::StartProcessing(offset.clone())).await {
            error!("Unable to send offset being processed for topic `{topic}`. Offset manager service down? Error: {err}");
        }

        while db_pool.status().available < 5 {
            tokio::task::yield_now().await;
        }

        tokio::spawn(
            process_message(
                Arc::clone(&db_pool),
                topic.clone(),
                (message, offset),
                offsets_tx.clone(),
                sigterm_rx.clone(),
                Arc::clone(&stats),
                RAIICounter::new(&stats.processing_tokio_tasks),
                process_msg_async,
            ),
        );
    }

    info!("DB statements executor for topic: `{topic}` has shut down");
}

#[allow(clippy::too_many_arguments)]
async fn process_message<M, F>(
    db_pool: Arc<Pool>,
    topic: String,
    queue_msg: QueueMsg<M>,
    offsets_tx: Sender<OffsetManagerCommand>,
    mut sigterm_rx: watch::Receiver<()>,
    stats: Arc<Stats>,
    // Don't remove this field, it tracks number of tasks, scheduled for execution
    _tasks_raii_counter: RAIICounter<f64, AtomicU64>,
    process_msg_async: impl Fn(Arc<Client>, Arc<M>) -> F,
) where
    F: Future<Output = Result<u64>>,
{
    let (message, offset) = queue_msg;
    let mut idle_interval = tokio::time::interval(Duration::from_millis(50));

    let client = loop {
        select! {
                _ = sigterm_rx.changed() => return,
                db_pool_result = db_pool.get() => match db_pool_result {
                    Ok(client) => break Arc::new(client),
                    Err(err) => {
                        error!("Failed to get a client from the pool, error: {err:?}");
                        idle_interval.tick().await;
                    },
                },
            };
    };

    loop {
        let process_msg_future = process_msg_async(Arc::clone(&client), Arc::clone(&message));
        let result = select! {
            _ = sigterm_rx.changed() => return,
            result = process_msg_future => result,
        };
        match result {
            Ok(_) => break,
            Err(err) => {
                error!("Failed to execute store value from topic `{topic}` into the DB, error {err}");
                stats.db_errors.inc();
                idle_interval.tick().await;
            }
        }
    }

    if let Err(err) = offsets_tx.send_async(OffsetManagerCommand::ProcessedSuccessfully(offset)).await {
        error!("Unable to send offset successfully processed status for topic `{topic}`. Offset manager service down?, error: {err}");
    }
}
