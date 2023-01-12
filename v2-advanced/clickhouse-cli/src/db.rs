use anyhow::anyhow;
use anyhow::Result;
use chrono::NaiveDateTime;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Row, Serialize, Deserialize)]
pub struct BlockInfo {
    block_json: String,
    retrieved_time: NaiveDateTime,
}

pub async fn fetch_block_info(
    client: &Client,
    slot: Option<u64>,
    hash: Option<String>,
) -> Result<BlockInfo> {
    let block_info: Result<BlockInfo> = match (slot, hash) {
        (None, None) => Err(anyhow!("Both slot and hash is empty")),

        (None, Some(hash)) => client
            .query("SELECT notify_block_json, retrieved_time FROM events.notify_block_local WHERE hash = '?'")
            .bind(hash)
            .fetch_one::<BlockInfo>()
            .await
            .map_err(anyhow::Error::msg),

        (Some(slot), None) => client
            .query("SELECT notify_block_json FROM events.notify_block_local WHERE slot = ?")
            .bind(slot)
            .fetch_one::<BlockInfo>()
            .await
            .map_err(anyhow::Error::msg),

        (Some(hash), Some(slot)) => client
        .query("SELECT notify_block_json, retrieved_time FROM events.notify_block_local WHERE slot = toUInt64(?) AND hash = '?'")
        .bind(slot)
        .bind(hash)
        .fetch_one::<BlockInfo>()
        .await
        .map_err(anyhow::Error::msg),
    };

    block_info
}
