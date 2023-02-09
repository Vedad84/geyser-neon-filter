use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use crossbeam_queue::SegQueue;

use kafka_common::kafka_structs::UpdateSlotStatus;
use log::error;
use log::info;
use log::warn;

use tokio_postgres::Client;
use tokio_postgres::NoTls;

use crate::config::AppConfig;
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

async fn account_stmt_executor(
    client: Arc<Client>,
    stats: Arc<Stats>,
    account_queue: Arc<SegQueue<DbAccountInfo>>,
) {
    if let Some(db_account_info) = account_queue.pop() {
        tokio::spawn(async move {
            let statement = match create_account_insert_statement(client.clone()).await {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to execute create_account_insert_statement, error {e}");
                    stats.db_errors.inc();
                    account_queue.push(db_account_info);
                    return;
                }
            };

            if let Err(error) =
                insert_into_account_audit(&db_account_info, &statement, client).await
            {
                error!("Failed to insert the data to account_audit, error: {error}");
                stats.db_errors.inc();
                // Push account_info back to the database queue
                account_queue.push(db_account_info);
            }
        });
    }
}

async fn transaction_stmt_executor(
    client: Arc<Client>,
    stats: Arc<Stats>,
    transaction_queue: Arc<SegQueue<DbTransaction>>,
) {
    if let Some(db_transaction_info) = transaction_queue.pop() {
        tokio::spawn(async move {
            let statement = match create_transaction_insert_statement(client.clone()).await {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to execute create_transaction_insert_statement, error {e}");
                    stats.db_errors.inc();
                    transaction_queue.push(db_transaction_info);
                    return;
                }
            };

            if let Err(error) =
                insert_into_transaction(&db_transaction_info, &statement, client).await
            {
                error!("Failed to insert the data to transaction_audit, error: {error}");
                stats.db_errors.inc();
                // Push transaction_info back to the database queue
                transaction_queue.push(db_transaction_info);
            }
        });
    }
}

async fn block_stmt_executor(
    client: Arc<Client>,
    stats: Arc<Stats>,
    block_queue: Arc<SegQueue<DbBlockInfo>>,
) {
    if let Some(db_block_info) = block_queue.pop() {
        tokio::spawn(async move {
            let statement = match create_block_metadata_insert_statement(client.clone()).await {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to prepare create_block_metadata_insert_statement, error {e}");
                    stats.db_errors.inc();
                    block_queue.push(db_block_info);
                    return;
                }
            };

            if let Err(error) = insert_into_block_metadata(&db_block_info, &statement, client).await
            {
                error!("Failed to insert the data to block_metadata, error: {error}");
                stats.db_errors.inc();
                // Push block_info back to the database queue
                block_queue.push(db_block_info);
            }
        });
    }
}

async fn slot_stmt_executor(
    client: Arc<Client>,
    stats: Arc<Stats>,
    slot_queue: Arc<SegQueue<UpdateSlotStatus>>,
) {
    if let Some(db_slot_info) = slot_queue.pop() {
        tokio::spawn(async move {
            let statement = match db_slot_info.parent {
                Some(_) => create_slot_insert_statement_with_parent(client.clone()).await,
                None => create_slot_insert_statement_without_parent(client.clone()).await,
            };

            match statement {
                Ok(statement) => {
                    if let Err(e) =
                        insert_slot_status_internal(&db_slot_info, &statement, client).await
                    {
                        error!("Failed to execute insert_slot_status_internal, error {e}");
                        stats.db_errors.inc();
                        slot_queue.push(db_slot_info);
                    }
                }
                Err(e) => {
                    error!("Failed to prepare create_slot_insert_statement, error {e}");
                    stats.db_errors.inc();
                    slot_queue.push(db_slot_info);
                }
            }
        });
    }
}

pub async fn db_stmt_executor(
    config: Arc<AppConfig>,
    mut client: Arc<Client>,
    stats: Arc<Stats>,
    account_queue: Arc<SegQueue<DbAccountInfo>>,
    slot_queue: Arc<SegQueue<UpdateSlotStatus>>,
    transaction_queue: Arc<SegQueue<DbTransaction>>,
    block_queue: Arc<SegQueue<DbBlockInfo>>,
) {
    let mut idle_interval = tokio::time::interval(Duration::from_millis(500));

    loop {
        if client.is_closed() {
            warn!("Postgres client was unexpectedly closed");
            client = initialize_db_client(config.clone()).await;
        }

        if account_queue.is_empty() && block_queue.is_empty() && slot_queue.is_empty() {
            idle_interval.tick().await;
        }

        account_stmt_executor(client.clone(), stats.clone(), account_queue.clone()).await;
        transaction_stmt_executor(client.clone(), stats.clone(), transaction_queue.clone()).await;
        block_stmt_executor(client.clone(), stats.clone(), block_queue.clone()).await;
        slot_stmt_executor(client.clone(), stats.clone(), slot_queue.clone()).await;
    }
}
