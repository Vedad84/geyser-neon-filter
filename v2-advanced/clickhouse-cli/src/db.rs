use anyhow::anyhow;
use anyhow::Result;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Row, Serialize, Deserialize)]
pub struct BlockInfo {
    block_json: String,
    retrieved_time: String,
}

#[derive(Debug, Default, Row, Serialize, Deserialize)]
pub struct TransactionInfo {
    transaction_json: String,
    retrieved_time: String,
}

#[derive(Debug, Default, Row, Serialize, Deserialize)]
pub struct UpdateAccountInfo {
    pubkey: Vec<u8>,
    lamports: u64,
    owner: Vec<u8>,
    executable: bool,
    rent_epoch: u64,
    data: Vec<u8>,
    write_version: u64,
    txn_signature: Vec<u8>,
    slot: u64,
    is_startup: bool,
    retrieved_time: String,
}

pub async fn fetch_block_info(
    client: &Client,
    slot: Option<u64>,
    hash: &Option<String>,
) -> Result<BlockInfo> {
    let query = "SELECT notify_block_json, toString(retrieved_time) FROM events.notify_block_local WHERE (slot = toUInt64(?) OR hash = toString(?))";
    if let (Some(slot), Some(hash)) = (slot, hash) {
        client
            .query(query)
            .bind(slot)
            .bind(hash)
            .fetch_one::<BlockInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else if let Some(slot) = slot {
        client
            .query(query)
            .bind(slot)
            .bind("")
            .fetch_one::<BlockInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else if let Some(hash) = hash {
        client
            .query(query)
            .bind(0)
            .bind(hash)
            .fetch_one::<BlockInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else {
        Err(anyhow!("Both slot and hash is empty"))
    }
}

pub async fn fetch_transaction_info(
    client: &Client,
    slot: Option<u64>,
    signature: &Option<Vec<u8>>,
) -> Result<TransactionInfo> {
    let empty_vec: Vec<u8> = vec![];
    let query = "SELECT notify_transaction_json, toString(retrieved_time) FROM events.notify_transaction_local WHERE slot = ? OR signature = ?";
    if let (Some(slot), Some(signature)) = (slot, signature) {
        client
            .query(query)
            .bind(slot)
            .bind(signature)
            .fetch_one::<TransactionInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else if let Some(slot) = slot {
        client
            .query(query)
            .bind(slot)
            .bind(empty_vec)
            .fetch_one::<TransactionInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else if let Some(signature) = signature {
        client
            .query(query)
            .bind(0)
            .bind(signature)
            .fetch_one::<TransactionInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else {
        Err(anyhow!("Both slot and signature is empty"))
    }
}

pub async fn fetch_update_account(
    client: &Client,
    write_version: Option<u64>,
    pubkey: &Option<Vec<u8>>,
) -> Result<UpdateAccountInfo> {
    let empty_vec: Vec<u8> = vec![];
    let query = "SELECT pubkey, lamports, owner, executable, rent_epoch, data, write_version, txn_signature, slot, is_startup, toString(retrieved_time)
                FROM events.update_account_local
                WHERE write_version = ? OR pubkey = ?";
    if let (Some(write_version), Some(pubkey)) = (write_version, pubkey) {
        client
            .query(query)
            .bind(write_version)
            .bind(pubkey)
            .fetch_one::<UpdateAccountInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else if let Some(write_version) = write_version {
        client
            .query(query)
            .bind(write_version)
            .bind(empty_vec)
            .fetch_one::<UpdateAccountInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else if let Some(pubkey) = pubkey {
        client
            .query(query)
            .bind(0)
            .bind(pubkey)
            .fetch_one::<UpdateAccountInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else {
        Err(anyhow!("Both write_version and pubkey is empty"))
    }
}

#[allow(dead_code)]
pub async fn fetch_update_slot(_client: &Client, _slot: Option<u64>) -> Result<BlockInfo> {
    unimplemented!()
}
