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
use flume::Receiver;
use kafka_common::kafka_structs::UpdateSlotStatus;
use log::error;
use log::info;
use prometheus_client::metrics::gauge::Gauge;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio_postgres::NoTls;

use crate::app_config::AppConfig;
use crate::consumer::QueueMsg;
use crate::consumer_stats::{ContextWithStats, Stats};

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

pub async fn db_stmt_executor<M, F>(
    consumer: Arc<StreamConsumer<ContextWithStats>>,
    topic: String,
    db_pool: Arc<Pool>,
    stats: Arc<Stats>,
    queue_rx: Receiver<QueueMsg<M>>,
    queue_len_gauge: Gauge<f64, AtomicU64>,
    process_msg_async: fn (Arc<Client>, Arc<M>) -> F,
) where
    M: Send + Sync + 'static,
    F: Future<Output = Result<u64>> + Send + 'static,
{
    let mut shutdown_stream = signal(SignalKind::terminate()).unwrap();

    loop {
        let (message, offset): QueueMsg<M> = select! {
            _ = shutdown_stream.recv() => {
                info!("DB statements executor for topic: `{topic}` is shut down");
                return
            },
            recv_result = queue_rx.recv_async() => match recv_result {
                Ok(item) => item,
                Err(err) => {
                    error!("Failed to get message for topic: {topic}, error: {err:?}");
                    continue;
                },
            },
        };

        queue_len_gauge.set(queue_rx.len() as f64);

        tokio::spawn(
            process_message(
                Arc::clone(&db_pool),
                Arc::clone(&consumer),
                topic.clone(),
                (Arc::clone(&message), offset),
                Arc::clone(&stats),
                process_msg_async,
            ),
        );
    }
}

async fn process_message<M, F>(
    db_pool: Arc<Pool>,
    consumer: Arc<StreamConsumer<ContextWithStats>>,
    topic: String,
    queue_msg: QueueMsg<M>,
    stats: Arc<Stats>,
    process_msg_async: impl Fn(Arc<Client>, Arc<M>) -> F,
) where
    F: Future<Output = Result<u64>>,
{
    let (message, offset) = queue_msg;
    let mut shutdown_stream = signal(SignalKind::terminate()).unwrap();

    let client = loop {
        select! {
            _ = shutdown_stream.recv() => return,
            db_pool_result = db_pool.get() => match db_pool_result {
                Ok(client) => break Arc::new(client),
                Err(err) => {
                    error!("Failed to get a client from the pool, error: {err:?}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                },
            },
        };
    };

    loop {
        let process_msg_future = process_msg_async(Arc::clone(&client), Arc::clone(&message));
        let result = select! {
            _ = shutdown_stream.recv() => return,
            result = process_msg_future => result,
        };
        match result {
            Ok(_) => break,
            Err(err) => {
                error!("Failed to execute store value from topic `{topic}` into the DB, error {err}");
                stats.db_errors.inc();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    consumer.store_offset(&topic, offset.partition, offset.offset)
        .unwrap_or_else(|err| error!("Failed to update offset for topic `{topic}`. Kafka error: {err}"));
}
