use anyhow::anyhow;
use anyhow::Result;
use chrono::Utc;
use deadpool_postgres::Client;
use kafka_common::kafka_structs::UpdateSlotStatus;
use std::sync::Arc;
use tokio_postgres::Statement;

use crate::db_types::DbAccountInfo;
use crate::db_types::DbBlockInfo;
use crate::db_types::DbTransaction;

pub async fn insert_into_account_audit(
    account: Arc<DbAccountInfo>,
    statement: &Statement,
    client: &Client,
) -> Result<u64> {
    let updated_on = Utc::now().naive_utc();
    client
        .execute(
            statement,
            &[
                &account.pubkey,
                &account.slot,
                &account.owner,
                &account.lamports,
                &account.executable,
                &account.rent_epoch,
                &account.data,
                &account.write_version,
                &updated_on,
                &account.txn_signature,
            ],
        )
        .await
        .map_err(|error| anyhow!("DbAccountInfo statement execution failed, error: {error}"))
}

pub async fn insert_into_transaction(
    transaction: Arc<DbTransaction>,
    statement: &Statement,
    client: &Client,
) -> Result<u64> {
    let updated_on = Utc::now().naive_utc();
    client
        .execute(
            statement,
            &[
                &transaction.signature,
                &transaction.is_vote,
                &transaction.slot,
                &transaction.message_type,
                &transaction.legacy_message,
                &transaction.v0_loaded_message,
                &transaction.signatures,
                &transaction.message_hash,
                &transaction.meta,
                &transaction.index,
                &updated_on,
            ],
        )
        .await
        .map_err(|error| anyhow!("DbTransaction statement execution failed, error: {error}"))
}

pub async fn insert_into_block_metadata(
    block_info: Arc<DbBlockInfo>,
    statement: &Statement,
    client: &Client,
) -> Result<u64> {
    let updated_on = Utc::now().naive_utc();
    client
        .execute(
            statement,
            &[
                &block_info.slot,
                &block_info.blockhash,
                &block_info.rewards,
                &block_info.block_time,
                &block_info.block_height,
                &updated_on,
            ],
        )
        .await
        .map_err(|error| anyhow!("DbBlockInfo statement execution failed, error: {error}"))
}

pub async fn insert_slot_status_internal(
    update_slot: Arc<UpdateSlotStatus>,
    statement: &Statement,
    client: &Client,
) -> Result<u64> {
    let updated_on = Utc::now().naive_utc();
    let status_str = update_slot.status.to_string();

    match update_slot.parent {
        Some(_) => {
            client
                .execute(
                    statement,
                    &[
                        &(update_slot.slot as i64),
                        &(update_slot.parent.map(|v| v as i64)),
                        &status_str,
                        &updated_on,
                    ],
                )
                .await
        }
        None => {
            client
                .execute(
                    statement,
                    &[&(update_slot.slot as i64), &status_str, &updated_on],
                )
                .await
        }
    }
    .map_err(|error| anyhow!("UpdateSlotStatus statement execution failed, error: {error}"))
}
