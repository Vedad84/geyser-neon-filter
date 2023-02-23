use std::{slice::Iter, sync::Arc};

use crate::filter_config::FilterConfig;
use kafka_common::kafka_structs::{
    KafkaReplicaAccountInfoVersions, KafkaReplicaTransactionInfoVersions, KafkaSanitizedMessage,
    NotifyTransaction, UpdateAccount,
};
use solana_sdk::{message::v0::LoadedAddresses, pubkey::Pubkey};
use tokio::sync::RwLock;

#[inline(always)]
async fn check_account<'a>(
    config: Arc<RwLock<FilterConfig>>,
    owner: Option<&'a [u8]>,
    pubkey: &'a [u8],
) -> bool {
    let read_guard = config.read().await;
    if read_guard.filter_include_pubkeys.is_empty() && read_guard.filter_include_owners.is_empty() {
        return true;
    }

    let owner = bs58::encode(owner.unwrap_or_else(|| [].as_ref())).into_string();
    let pubkey = bs58::encode(pubkey).into_string();
    if read_guard.filter_include_pubkeys.contains(&pubkey)
        || read_guard.filter_include_owners.contains(&owner)
    {
        return true;
    }
    false
}

fn account_keys(message: &KafkaSanitizedMessage) -> Iter<'_, Pubkey> {
    match message {
        KafkaSanitizedMessage::Legacy(message) => message.message.account_keys.iter(),
        KafkaSanitizedMessage::V0(message) => message.message.account_keys.iter(),
    }
}

fn loaded_addresses(message: &KafkaSanitizedMessage) -> LoadedAddresses {
    match message {
        KafkaSanitizedMessage::Legacy(_) => LoadedAddresses::default(),
        KafkaSanitizedMessage::V0(message) => LoadedAddresses::clone(&message.loaded_addresses),
    }
}

#[inline(always)]
async fn check_transaction(
    config: Arc<RwLock<FilterConfig>>,
    transaction_info: &KafkaReplicaTransactionInfoVersions,
) -> bool {
    let (keys, loaded_addresses) = match transaction_info {
        KafkaReplicaTransactionInfoVersions::V0_0_1(replica) => (
            account_keys(&replica.transaction.message),
            loaded_addresses(&replica.transaction.message),
        ),
        KafkaReplicaTransactionInfoVersions::V0_0_2(replica) => (
            account_keys(&replica.transaction.message),
            loaded_addresses(&replica.transaction.message),
        ),
    };

    for i in keys {
        if check_account(config.clone(), None, &i.to_bytes()).await {
            return true;
        }
    }

    let pubkey_iter = loaded_addresses
        .writable
        .iter()
        .chain(loaded_addresses.readonly.iter());

    for i in pubkey_iter {
        if check_account(config.clone(), None, &i.to_bytes()).await {
            return true;
        }
    }

    false
}

pub async fn process_transaction_info(
    config: Arc<RwLock<FilterConfig>>,
    transaction: &NotifyTransaction,
) -> bool {
    match &transaction.transaction_info {
        KafkaReplicaTransactionInfoVersions::V0_0_1(transaction_replica) => {
            if !transaction_replica.is_vote
                && check_transaction(config, &transaction.transaction_info).await
            {
                return true;
            }
        }
        KafkaReplicaTransactionInfoVersions::V0_0_2(transaction_replica) => {
            if !transaction_replica.is_vote
                && check_transaction(config, &transaction.transaction_info).await
            {
                return true;
            }
        }
    }
    false
}

pub async fn process_account_info(
    config: Arc<RwLock<FilterConfig>>,
    update_account: &UpdateAccount,
) -> bool {
    match &update_account.account {
        // for 1.13.x or earlier
        KafkaReplicaAccountInfoVersions::V0_0_1(account_info) => {
            if check_account(
                config,
                Some(account_info.owner.as_slice()),
                account_info.pubkey.as_slice(),
            )
            .await
            {
                return true;
            }
        }
        KafkaReplicaAccountInfoVersions::V0_0_2(account_info) => {
            if check_account(
                config,
                Some(account_info.owner.as_slice()),
                account_info.pubkey.as_slice(),
            )
            .await
            {
                return true;
            }
        }
    }
    false
}
