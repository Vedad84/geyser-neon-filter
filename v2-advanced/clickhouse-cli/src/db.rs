use anyhow::anyhow;
use anyhow::Result;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Row, Serialize, Deserialize)]
pub struct BlockInfo {
    block_json: String,
    retrieved_time: String,
}

pub async fn fetch_block_info(
    client: &Client,
    slot: Option<u64>,
    hash: &Option<String>,
) -> Result<BlockInfo> {
    let block_info: Result<BlockInfo> = match (slot, hash) {
        (None, None) => Err(anyhow!("Both slot and hash is empty")),

        (None, Some(hash)) => client
            .query("SELECT notify_block_json, toString(retrieved_time) FROM events.notify_block_local WHERE hash = toString(?)")
            .bind(hash)
            .fetch_one::<BlockInfo>()
            .await
            .map_err(anyhow::Error::msg),

        (Some(slot), None) => client
            .query("SELECT notify_block_json, toString(retrieved_time) FROM events.notify_block_local WHERE slot = toUInt64(?)")
            .bind(slot)
            .fetch_one::<BlockInfo>()
            .await
            .map_err(anyhow::Error::msg),

        (Some(hash), Some(slot)) =>  {

        let with_slot = client
            .query("SELECT notify_block_json, toString(retrieved_time) FROM events.notify_block_local WHERE slot = toUInt64(?)")
            .bind(slot)
            .fetch_one::<BlockInfo>()
            .await
            .map_err(anyhow::Error::msg);

        let with_hash = client
            .query("SELECT notify_block_json, toString(retrieved_time) FROM events.notify_block_local WHERE hash = toString(?)")
            .bind(hash)
            .fetch_one::<BlockInfo>()
            .await
            .map_err(anyhow::Error::msg);

        if with_slot.is_err() {
            with_hash
        } else {
            with_slot
        }

    }

    };

    block_info
}
