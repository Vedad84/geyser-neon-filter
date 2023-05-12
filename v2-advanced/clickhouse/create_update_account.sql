CREATE DATABASE IF NOT EXISTS events ON CLUSTER 'events';

CREATE TABLE IF NOT EXISTS events.update_account_local ON CLUSTER '{cluster}' (
    pubkey Array(UInt8) CODEC(ZSTD),
    lamports UInt64 CODEC(DoubleDelta, ZSTD),
    owner Array(UInt8) CODEC(ZSTD),
    executable Bool CODEC(ZSTD),
    rent_epoch UInt64 CODEC(DoubleDelta, ZSTD),
    data Array(UInt8) CODEC(ZSTD),
    write_version Int64 CODEC(DoubleDelta, ZSTD),
    txn_signature Array(Nullable(UInt8)) CODEC(ZSTD),
    slot UInt64 CODEC(DoubleDelta, ZSTD),
    is_startup Bool CODEC(ZSTD),
    retrieved_time DateTime64 CODEC(DoubleDelta, ZSTD)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/update_account_local',
    '{replica}',
    retrieved_time
) PRIMARY KEY (pubkey, slot, write_version)
PARTITION BY toInt32(slot / 216000)
ORDER BY (pubkey, slot, write_version)
SETTINGS index_granularity=8192;


CREATE TABLE IF NOT EXISTS events.update_account_distributed ON CLUSTER '{cluster}' AS events.update_account_local
ENGINE = Distributed('{cluster}', events, update_account_local, xxHash64(arrayStringConcat(pubkey,'')));

CREATE TABLE IF NOT EXISTS events.update_account_queue ON CLUSTER '{cluster}' (
    pubkey Array(UInt8),
    lamports UInt64,
    owner Array(UInt8),
    executable Bool,
    rent_epoch UInt64,
    data Array(UInt8),
    write_version Int64,
    txn_signature Array(Nullable(UInt8)),
    slot UInt64,
    is_startup Bool,
    retrieved_time DateTime64
)   ENGINE = Kafka SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'update_account',
    kafka_group_name = 'clickhouse',
    kafka_num_consumers = 1,
    kafka_poll_timeout_ms = 200,
    kafka_flush_interval_ms = 1000,
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS events.update_account_queue_mv ON CLUSTER '{cluster}' to events.update_account_distributed
AS  SELECT pubkey,
           lamports,
           owner,
           executable,
           rent_epoch,
           data,
           write_version,
           txn_signature,
           slot,
           is_startup,
           retrieved_time
FROM events.update_account_queue
WHERE is_startup = FALSE;

CREATE MATERIALIZED VIEW IF NOT EXISTS events.older_account_queue_mv ON CLUSTER '{cluster}' to events.older_account_distributed
AS  SELECT pubkey,
           lamports,
           owner,
           executable,
           rent_epoch,
           data,
           write_version,
           txn_signature,
           slot,
           is_startup,
           retrieved_time
FROM events.update_account_queue
WHERE is_startup = TRUE;
