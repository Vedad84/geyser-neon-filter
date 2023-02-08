use std::sync::Arc;

use crate::{
    db_types::{DbAccountInfo, DbBlockInfo, DbTransaction},
    filter_config::FilterConfig,
};
use anyhow::Result;
use crossbeam_queue::SegQueue;
use flume::Receiver;
use kafka_common::kafka_structs::{
    NotifyBlockMetaData, NotifyTransaction, UpdateAccount, UpdateSlotStatus,
};
use log::{error, trace};
use tokio::sync::RwLock;

#[inline(always)]
async fn check_account(
    config: Arc<RwLock<FilterConfig>>,
    account_queue: Arc<SegQueue<DbAccountInfo>>,
    update_account: &UpdateAccount,
    owner: &Vec<u8>,
    pubkey: &Vec<u8>,
) -> Result<()> {
    let owner = bs58::encode(owner).into_string();
    let pubkey = bs58::encode(pubkey).into_string();
    if config.read().await.filter_include_pubkeys.contains(&pubkey)
        || config.read().await.filter_include_owners.contains(&owner)
    {
        account_queue.push(update_account.try_into()?);
        trace!(
            "Add update_account entry to db queue for pubkey {}, owner {}",
            pubkey,
            owner
        );
    }
    Ok(())
}

async fn process_account_info(
    config: Arc<RwLock<FilterConfig>>,
    account_queue: Arc<SegQueue<DbAccountInfo>>,
    update_account: UpdateAccount,
) -> Result<()> {
    match &update_account.account {
        // for 1.13.x or earlier
        kafka_common::kafka_structs::KafkaReplicaAccountInfoVersions::V0_0_1(account_info) => {
            check_account(
                config,
                account_queue,
                &update_account,
                &account_info.owner,
                &account_info.pubkey,
            )
            .await?;
        }
        kafka_common::kafka_structs::KafkaReplicaAccountInfoVersions::V0_0_2(account_info) => {
            check_account(
                config,
                account_queue,
                &update_account,
                &account_info.owner,
                &account_info.pubkey,
            )
            .await?;
        }
    }
    Ok(())
}

pub async fn account_filter(
    config: Arc<RwLock<FilterConfig>>,
    account_queue: Arc<SegQueue<DbAccountInfo>>,
    filter_rx: Receiver<UpdateAccount>,
) {
    loop {
        if let Ok(update_account) = filter_rx.recv_async().await {
            let config = config.clone();
            let account_queue = account_queue.clone();

            tokio::spawn(async move {
                if let Err(e) = process_account_info(config, account_queue, update_account).await {
                    error!("Failed to process account info, error: {e}");
                }
            });
        }
    }
}

pub async fn transaction_filter(
    transaction_queue: Arc<SegQueue<DbTransaction>>,
    filter_rx: Receiver<NotifyTransaction>,
) {
    loop {
        if let Ok(transaction) = filter_rx.recv_async().await {
            transaction_queue.push(transaction.into());
        }
    }
}

pub async fn block_filter(
    block_queue: Arc<SegQueue<DbBlockInfo>>,
    filter_rx: Receiver<NotifyBlockMetaData>,
) {
    loop {
        if let Ok(notify_block_data) = filter_rx.recv_async().await {
            match notify_block_data.block_info {
                kafka_common::kafka_structs::KafkaReplicaBlockInfoVersions::V0_0_1(bi) => {
                    block_queue.push(bi.into());
                }
            }
        }
    }
}

pub async fn slot_filter(
    slot_queue: Arc<SegQueue<UpdateSlotStatus>>,
    filter_rx: Receiver<UpdateSlotStatus>,
) {
    loop {
        if let Ok(update_slot) = filter_rx.recv_async().await {
            slot_queue.push(update_slot)
        }
    }
}
