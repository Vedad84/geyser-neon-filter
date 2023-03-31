CREATE DATABASE IF NOT EXISTS events ON CLUSTER 'events';

CREATE TABLE IF NOT EXISTS events.temp_update_account_local ON CLUSTER '{cluster}' (
    pubkey Array(UInt8) CODEC(ZSTD),
    lamports UInt64 CODEC(DoubleDelta, ZSTD),
    owner Array(UInt8) CODEC(ZSTD),
    executable Bool CODEC(ZSTD),
    rent_epoch UInt64 CODEC(DoubleDelta, ZSTD),
    data Array(UInt8) CODEC(ZSTD),
    txn_signature Array(UInt8) DEFAULT [] CODEC(ZSTD),
    slot UInt64 CODEC(DoubleDelta, ZSTD),
    is_startup Bool CODEC(ZSTD),
    retrieved_time DateTime64 CODEC(DoubleDelta, ZSTD)
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/temp_update_account_local',
    '{replica}'
) PRIMARY KEY (txn_signature, slot, pubkey)
PARTITION BY slot % 4500
ORDER BY (txn_signature, slot, pubkey)
SETTINGS index_granularity=8192;


CREATE TABLE IF NOT EXISTS events.temp_update_account_processed_local ON CLUSTER '{cluster}' (
    pubkey Array(UInt8) CODEC(ZSTD),
    txn_signature Array(UInt8) DEFAULT [] CODEC(ZSTD),
    slot UInt64 CODEC(DoubleDelta, ZSTD)
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/temp_update_account_processed_local',
    '{replica}'
) PRIMARY KEY (txn_signature, slot, pubkey)
PARTITION BY slot % 4500
ORDER BY (txn_signature, slot, pubkey)
SETTINGS index_granularity=8192;


CREATE TABLE IF NOT EXISTS events.temp_update_account_distributed ON CLUSTER '{cluster}' AS events.temp_update_account_local
    ENGINE = Distributed('{cluster}', events, temp_update_account_local, xxHash64(CONCAT(toString(slot), arrayStringConcat(txn_signature,''))));


CREATE TABLE IF NOT EXISTS events.temp_update_account_queue ON CLUSTER '{cluster}' (
    pubkey Array(UInt8),
    lamports UInt64,
    owner Array(UInt8),
    executable Bool,
    rent_epoch UInt64,
    data Array(UInt8),
    txn_signature Array(Nullable(UInt8)),
    slot UInt64,
    is_startup Bool,
    retrieved_time DateTime64 DEFAULT now64()
)   ENGINE = Kafka SETTINGS
kafka_broker_list = 'kafka:29092',
kafka_topic_list = 'update_account',
kafka_group_name = 'clickhouse',
kafka_num_consumers = 1,
kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS events.temp_update_account_queue_mv ON CLUSTER '{cluster}' to events.temp_update_account_distributed
AS  SELECT pubkey,
           lamports,
           owner,
           executable,
           rent_epoch,
           data,
           txn_signature,
           slot,
           is_startup,
           retrieved_time
FROM events.temp_update_account_queue;
