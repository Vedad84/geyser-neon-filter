use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

use flume::Receiver;
use flume::Sender;
use kafka_common::kafka_structs::UpdateSlotStatus;
use log::error;
use log::info;
use log::warn;

use tokio_postgres::Client;
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

pub async fn initialize_db_client(config: Arc<AppConfig>) -> Arc<Client> {
    let client;
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        match connect_to_db(config.clone()).await {
            Ok(c) => {
                info!("A new Postgres client was created and successfully connected to the server");
                client = c;
                break;
            }
            Err(e) => {
                error!("Failed to connect to the database, error: {e}",);
                interval.tick().await;
            }
        };
    }
    client
}

async fn connect_to_db(config: Arc<AppConfig>) -> Result<Arc<Client>> {
    let (client, connection) =
        tokio_postgres::connect(&config.postgres_connection_str, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Postgres connection error: {}", e);
        }
    });

    Ok(Arc::new(client))
}

async fn exec_account_statement(
    client: Arc<Client>,
    stats: Arc<Stats>,
    account_tx: Sender<DbAccountInfo>,
    account_rx: Receiver<DbAccountInfo>,
) {
    if let Ok(db_account_info) = account_rx.recv_async().await {
        let statement = match create_account_insert_statement(client.clone()).await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to execute create_account_insert_statement, error {e}");
                stats.db_errors.inc();
                if let Err(e) = account_tx.send_async(db_account_info).await {
                    error!("Failed to push account_info back to the database queue, error: {e}");
                }
                return;
            }
        };

        if let Err(error) = insert_into_account_audit(&db_account_info, &statement, client).await {
            error!("Failed to insert the data to account_audit, error: {error}");
            stats.db_errors.inc();
            // Push account_info back to the database queue
            if let Err(e) = account_tx.send_async(db_account_info).await {
                error!("Failed to push account_info back to the database queue, error: {e}");
            }
        }
    }
}

async fn exec_transaction_statement(
    client: Arc<Client>,
    stats: Arc<Stats>,
    transaction_tx: Sender<DbTransaction>,
    transaction_rx: Receiver<DbTransaction>,
) {
    if let Ok(db_transaction_info) = transaction_rx.recv_async().await {
        let statement = match create_transaction_insert_statement(client.clone()).await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to execute create_transaction_insert_statement, error {e}");
                stats.db_errors.inc();
                if let Err(e) = transaction_tx.send_async(db_transaction_info).await {
                    error!(
                        "Failed to push transaction_info back to the database queue, error: {e}"
                    );
                }
                return;
            }
        };

        if let Err(error) = insert_into_transaction(&db_transaction_info, &statement, client).await
        {
            error!("Failed to insert the data to transaction_audit, error: {error}");
            stats.db_errors.inc();
            // Push transaction_info back to the database queue
            if let Err(e) = transaction_tx.send_async(db_transaction_info).await {
                error!("Failed to push transaction_info back to the database queue, error: {e}");
            }
        }
    }
}

async fn exec_block_statement(
    client: Arc<Client>,
    stats: Arc<Stats>,
    block_tx: Sender<DbBlockInfo>,
    block_rx: Receiver<DbBlockInfo>,
) {
    if let Ok(db_block_info) = block_rx.recv_async().await {
        let statement = match create_block_metadata_insert_statement(client.clone()).await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to prepare create_block_metadata_insert_statement, error {e}");
                stats.db_errors.inc();
                if let Err(e) = block_tx.send_async(db_block_info).await {
                    error!("Failed to push block_info back to the database queue, error: {e}");
                }
                return;
            }
        };

        if let Err(error) = insert_into_block_metadata(&db_block_info, &statement, client).await {
            error!("Failed to insert the data to block_metadata, error: {error}");
            stats.db_errors.inc();
            // Push block_info back to the database queue
            if let Err(e) = block_tx.send_async(db_block_info).await {
                error!("Failed to push block_info back to the database queue, error: {e}");
            }
        }
    }
}

async fn exec_slot_statement(
    client: Arc<Client>,
    stats: Arc<Stats>,
    slot_tx: Sender<UpdateSlotStatus>,
    slot_rx: Receiver<UpdateSlotStatus>,
) {
    if let Ok(db_slot_info) = slot_rx.recv_async().await {
        let statement = match db_slot_info.parent {
            Some(_) => create_slot_insert_statement_with_parent(client.clone()).await,
            None => create_slot_insert_statement_without_parent(client.clone()).await,
        };

        match statement {
            Ok(statement) => {
                if let Err(e) = insert_slot_status_internal(&db_slot_info, &statement, client).await
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
}

pub async fn db_stmt_executor(
    config: Arc<AppConfig>,
    mut client: Arc<Client>,
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
        if client.is_closed() {
            warn!("Postgres client was unexpectedly closed");
            client = initialize_db_client(config.clone()).await;
        }

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
                exec_account_statement(client, stats, account_tx, account_rx).await;
            });
        }

        if !slot_rx.is_empty() {
            let client = client.clone();
            let stats = stats.clone();
            let slot_tx = slot_tx.clone();
            let slot_rx = slot_rx.clone();

            tokio::spawn(async move {
                exec_slot_statement(client, stats, slot_tx, slot_rx).await;
            });
        }

        if !transaction_rx.is_empty() {
            let client = client.clone();
            let stats = stats.clone();
            let transaction_tx = transaction_tx.clone();
            let transaction_rx = transaction_rx.clone();

            tokio::spawn(async move {
                exec_transaction_statement(client, stats, transaction_tx, transaction_rx).await;
            });
        }

        if !block_rx.is_empty() {
            let client = client.clone();
            let stats = stats.clone();
            let block_tx = block_tx.clone();
            let block_rx = block_rx.clone();

            tokio::spawn(async move {
                exec_block_statement(client, stats, block_tx, block_rx).await;
            });
        }
    }
}
