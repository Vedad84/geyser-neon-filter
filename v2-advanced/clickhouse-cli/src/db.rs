use anyhow::anyhow;
use anyhow::Result;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use time::OffsetDateTime;

use crate::parse::SlotOrHash;
use crate::parse::SlotOrSignature;
use crate::parse::VersionOrPubkey;

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct BlockInfo {
    block_json: String,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    pub retrieved_time: OffsetDateTime,
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct TransactionInfo {
    transaction_json: String,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    pub retrieved_time: OffsetDateTime,
}

#[derive(Debug, Row, Serialize, Deserialize)]
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
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    pub retrieved_time: OffsetDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum SlotStatus {
    Processed,
    Rooted,
    Confirmed,
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct UpdateSlot {
    pub slot: u64,
    pub parent: Option<u64>,
    pub status: SlotStatus,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    pub retrieved_time: OffsetDateTime,
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct TableInfo {
    pub database: String,
    pub table: String,
    pub rows: u64,
    pub latest_modification: String,
    pub disk_size: String,
    pub primary_keys_size: String,
    pub engine: String,
    pub bytes_size: u64,
    pub compressed_size: String,
    pub uncompressed_size: String,
    pub compression_ratio: f64,
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct TableRowCount {
    pub table_name: String,
    pub row_count: u64,
}

pub async fn fetch_block_info(client: &Client, soh: &SlotOrHash) -> Result<BlockInfo> {
    let query = "SELECT notify_block_json, retrieved_time FROM events.notify_block_distributed WHERE (slot = toUInt64(?) OR hash = toString(?))";
    if let (Some(slot), Some(hash)) = (soh.slot, soh.hash.as_ref()) {
        client
            .query(query)
            .bind(slot)
            .bind(hash)
            .fetch_one::<BlockInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else if let Some(slot) = soh.slot {
        client
            .query(query)
            .bind(slot)
            .bind("")
            .fetch_one::<BlockInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else if let Some(hash) = &soh.hash {
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
    sos: &SlotOrSignature,
) -> Result<TransactionInfo> {
    let empty_vec: Vec<u8> = vec![];
    let query = "SELECT notify_transaction_json, retrieved_time FROM events.notify_transaction_distributed WHERE slot = ? OR signature = ?";
    if let (Some(slot), Some(signature)) = (sos.slot, sos.signature.as_ref()) {
        client
            .query(query)
            .bind(slot)
            .bind(signature)
            .fetch_one::<TransactionInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else if let Some(slot) = sos.slot {
        client
            .query(query)
            .bind(slot)
            .bind(empty_vec)
            .fetch_one::<TransactionInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else if let Some(signature) = &sos.signature {
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
    vop: &VersionOrPubkey,
) -> Result<UpdateAccountInfo> {
    let empty_vec: Vec<u8> = vec![];
    let query = "SELECT pubkey, lamports, owner, executable, rent_epoch, data, write_version, txn_signature, slot, is_startup, retrieved_time
                FROM events.update_account_distributed
                WHERE write_version = ? OR pubkey = ?";
    if let (Some(write_version), Some(pubkey)) = (vop.write_version, vop.pubkey.as_ref()) {
        client
            .query(query)
            .bind(write_version)
            .bind(pubkey)
            .fetch_one::<UpdateAccountInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else if let Some(write_version) = vop.write_version {
        client
            .query(query)
            .bind(write_version)
            .bind(empty_vec)
            .fetch_one::<UpdateAccountInfo>()
            .await
            .map_err(anyhow::Error::msg)
    } else if let Some(pubkey) = &vop.pubkey {
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

pub async fn fetch_update_slot(client: &Client, slot: u64) -> Result<UpdateSlot> {
    let query = "SELECT slot, parent, slot_status, retrieved_time
    FROM events.update_slot
    WHERE slot = ?
    ";
    client
        .query(query)
        .bind(slot)
        .fetch_one::<UpdateSlot>()
        .await
        .map_err(anyhow::Error::msg)
}

pub async fn fetch_table_info(client: &Client, table_name: &str) -> Result<Vec<TableInfo>> {
    let query = "
    SELECT
    parts.*,
    columns.compressed_size,
    columns.uncompressed_size,
    columns.compression_ratio
FROM (
    SELECT database,
        table,
        formatReadableSize(sum(data_uncompressed_bytes))          AS uncompressed_size,
        formatReadableSize(sum(data_compressed_bytes))            AS compressed_size,
        sum(data_compressed_bytes) / sum(data_uncompressed_bytes) AS compression_ratio
    FROM system.columns
    GROUP BY database, table
) columns RIGHT JOIN (
    SELECT database,
           table,
           sum(rows)                                            AS rows,
           toString(max(modification_time))                     AS latest_modification,
           formatReadableSize(sum(bytes))                       AS disk_size,
           formatReadableSize(sum(primary_key_bytes_in_memory)) AS primary_keys_size,
           any(engine)                                          AS engine,
           sum(bytes)                                           AS bytes_size
    FROM system.parts
    WHERE active and table like concat(toString(?), '%')
    GROUP BY database, table
) parts ON ( columns.database = parts.database AND columns.table = parts.table )
ORDER BY parts.bytes_size DESC
";
    client
        .query(query)
        .bind(table_name)
        .fetch_all::<TableInfo>()
        .await
        .map_err(anyhow::Error::msg)
}

pub async fn fetch_row_count(client: &Client, table_name: &str) -> Result<TableRowCount> {
    let query = "SELECT table, rows
    FROM system.parts
    WHERE table like ?";
    client
        .query(query)
        .bind(table_name)
        .fetch_one::<TableRowCount>()
        .await
        .map_err(anyhow::Error::msg)
}

pub async fn fetch_function_list(client: &Client) -> Result<Vec<String>> {
    let query = "SELECT name, create_query FROM system.functions
    WHERE origin = 'SQLUserDefined'";
    client
        .query(query)
        .fetch_all::<String>()
        .await
        .map_err(anyhow::Error::msg)
}
