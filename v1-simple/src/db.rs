use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

use deadpool_postgres::Client;
use deadpool_postgres::Manager;
use deadpool_postgres::ManagerConfig;
use deadpool_postgres::Pool;
use deadpool_postgres::RecyclingMethod;
use flume::Receiver;
use flume::Sender;
use kafka_common::kafka_structs::UpdateSlotStatus;
use log::error;
use log::info;
use tokio_postgres::NoTls;

use crate::app_config::AppConfig;
use crate::consumer_stats::Stats;

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

async fn exec_account_statement(
    client: Arc<Client>,
    stats: Arc<Stats>,
    account_tx: Sender<DbAccountInfo>,
    account_rx: Receiver<DbAccountInfo>,
) -> usize {
    if let Ok(db_account_info) = account_rx.recv_async().await {
        let statement = match create_account_insert_statement(&client).await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to execute create_account_insert_statement, error {e}");
                stats.db_errors.inc();
                if let Err(e) = account_tx.send_async(db_account_info).await {
                    error!("Failed to push account_info back to the database queue, error: {e}");
                }
                return account_rx.len();
            }
        };

        if let Err(error) = insert_into_account_audit(&db_account_info, &statement, &client).await {
            error!("Failed to insert the data to account_audit, error: {error}");
            stats.db_errors.inc();
            // Push account_info back to the database queue
            if let Err(e) = account_tx.send_async(db_account_info).await {
                error!("Failed to push account_info back to the database queue, error: {e}");
            }
        }
    }
    account_rx.len()
}

async fn exec_transaction_statement(
    client: Arc<Client>,
    stats: Arc<Stats>,
    transaction_tx: Sender<DbTransaction>,
    transaction_rx: Receiver<DbTransaction>,
) -> usize {
    if let Ok(db_transaction_info) = transaction_rx.recv_async().await {
        let statement = match create_transaction_insert_statement(&client).await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to execute create_transaction_insert_statement, error {e}");
                stats.db_errors.inc();
                if let Err(e) = transaction_tx.send_async(db_transaction_info).await {
                    error!(
                        "Failed to push transaction_info back to the database queue, error: {e}"
                    );
                }
                return transaction_rx.len();
            }
        };

        if let Err(error) = insert_into_transaction(&db_transaction_info, &statement, &client).await
        {
            error!("Failed to insert the data to transaction_audit, error: {error}");
            stats.db_errors.inc();
            // Push transaction_info back to the database queue
            if let Err(e) = transaction_tx.send_async(db_transaction_info).await {
                error!("Failed to push transaction_info back to the database queue, error: {e}");
            }
        }
    }
    transaction_rx.len()
}

async fn exec_block_statement(
    client: Arc<Client>,
    stats: Arc<Stats>,
    block_tx: Sender<DbBlockInfo>,
    block_rx: Receiver<DbBlockInfo>,
) -> usize {
    if let Ok(db_block_info) = block_rx.recv_async().await {
        let statement = match create_block_metadata_insert_statement(&client).await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to prepare create_block_metadata_insert_statement, error {e}");
                stats.db_errors.inc();
                if let Err(e) = block_tx.send_async(db_block_info).await {
                    error!("Failed to push block_info back to the database queue, error: {e}");
                }
                return block_rx.len();
            }
        };

        if let Err(error) = insert_into_block_metadata(&db_block_info, &statement, &client).await {
            error!("Failed to insert the data to block_metadata, error: {error}");
            stats.db_errors.inc();
            // Push block_info back to the database queue
            if let Err(e) = block_tx.send_async(db_block_info).await {
                error!("Failed to push block_info back to the database queue, error: {e}");
            }
        }
    }
    block_rx.len()
}

async fn exec_slot_statement(
    client: Arc<Client>,
    stats: Arc<Stats>,
    slot_tx: Sender<UpdateSlotStatus>,
    slot_rx: Receiver<UpdateSlotStatus>,
) -> usize {
    if let Ok(db_slot_info) = slot_rx.recv_async().await {
        let statement = match db_slot_info.parent {
            Some(_) => create_slot_insert_statement_with_parent(&client).await,
            None => create_slot_insert_statement_without_parent(&client).await,
        };

        match statement {
            Ok(statement) => {
                if let Err(e) =
                    insert_slot_status_internal(&db_slot_info, &statement, &client).await
                {
                    error!("Failed to execute insert_slot_status_internal, error {e}");
                    stats.db_errors.inc();
                    if let Err(e) = slot_tx.send_async(db_slot_info).await {
                        error!("Failed to send slot info back to the queue, error {e}");
                    }
                }
            }
            Err(e) => {
                error!("Failed to prepare create_slot_insert_statement, error {e}");
                stats.db_errors.inc();
                if let Err(e) = slot_tx.send_async(db_slot_info).await {
                    error!("Failed to send slot info back to the queue, error {e}");
                }
            }
        }
    }
    slot_rx.len()
}

pub async fn db_stmt_executor(
    db_pool: Arc<Pool>,
    stats: Arc<Stats>,
    account_queue: (Sender<DbAccountInfo>, Receiver<DbAccountInfo>),
    slot_queue: (Sender<UpdateSlotStatus>, Receiver<UpdateSlotStatus>),
    transaction_queue: (Sender<DbTransaction>, Receiver<DbTransaction>),
    block_queue: (Sender<DbBlockInfo>, Receiver<DbBlockInfo>),
) {
    let mut idle_interval = tokio::time::interval(Duration::from_millis(500));

    let (account_tx, account_rx) = account_queue;
    let (slot_tx, slot_rx) = slot_queue;
    let (transaction_tx, transaction_rx) = transaction_queue;
    let (block_tx, block_rx) = block_queue;

    loop {
        if let Ok(client) = db_pool.get().await {
            let client = Arc::new(client);

            if account_tx.is_empty()
                && slot_tx.is_empty()
                && transaction_tx.is_empty()
                && block_tx.is_empty()
            {
                idle_interval.tick().await;
            }

            if !account_rx.is_empty() {
                let client = client.clone();
                let stats = stats.clone();
                let account_tx = account_tx.clone();
                let account_rx = account_rx.clone();

                tokio::spawn(async move {
                    let channel_len =
                        exec_account_statement(client, stats.clone(), account_tx, account_rx).await;
                    stats.queue_len_update_account.set(channel_len as f64);
                });
            }

            if !slot_rx.is_empty() {
                let client = client.clone();
                let stats = stats.clone();
                let slot_tx = slot_tx.clone();
                let slot_rx = slot_rx.clone();

                tokio::spawn(async move {
                    let channel_len =
                        exec_slot_statement(client, stats.clone(), slot_tx, slot_rx).await;
                    stats.queue_len_update_slot.set(channel_len as f64);
                });
            }

            if !transaction_rx.is_empty() {
                let client = client.clone();
                let stats = stats.clone();
                let transaction_tx = transaction_tx.clone();
                let transaction_rx = transaction_rx.clone();

                tokio::spawn(async move {
                    let channel_len = exec_transaction_statement(
                        client,
                        stats.clone(),
                        transaction_tx,
                        transaction_rx,
                    )
                    .await;
                    stats.queue_len_notify_transaction.set(channel_len as f64);
                });
            }

            if !block_rx.is_empty() {
                let client = client.clone();
                let stats = stats.clone();
                let block_tx = block_tx.clone();
                let block_rx = block_rx.clone();

                tokio::spawn(async move {
                    let channel_len =
                        exec_block_statement(client, stats.clone(), block_tx, block_rx).await;
                    stats.queue_len_notify_block.set(channel_len as f64);
                });
            }
        } else {
            error!("Failed to get a client from the pool");
        }
    }
}
