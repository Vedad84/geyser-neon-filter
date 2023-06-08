CREATE DATABASE IF NOT EXISTS events ON CLUSTER 'events';

CREATE TABLE IF NOT EXISTS events.notify_block_local ON CLUSTER '{cluster}' (
    slot UInt64 CODEC(DoubleDelta, ZSTD),
    hash String CODEC(ZSTD(5)),
    notify_block_json String CODEC(ZSTD(5)),
    retrieved_time DateTime64 CODEC(DoubleDelta, ZSTD)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/notify_block_local',
    '{replica}'
) PRIMARY KEY (slot, hash)
PARTITION BY toYYYYMMDD(retrieved_time)
ORDER BY (slot, hash)
SETTINGS index_granularity=8192;

CREATE TABLE IF NOT EXISTS events.notify_block_distributed ON CLUSTER '{cluster}' AS events.notify_block_local
ENGINE = Distributed('{cluster}', events, notify_block_local, rand());

CREATE TABLE IF NOT EXISTS events.notify_block_queue ON CLUSTER '{cluster}' (
    notify_block_json String
)   ENGINE = Kafka SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'notify_block',
    kafka_group_name = 'clickhouse',
    kafka_num_consumers = 1,
    kafka_poll_timeout_ms = 200,
    kafka_flush_interval_ms = 400,
    kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW IF NOT EXISTS events.notify_block_queue_mv ON CLUSTER '{cluster}' TO events.notify_block_distributed AS
SELECT JSONExtract(notify_block_json, 'slot', 'UInt64') AS slot,
       _key as hash,
       notify_block_json,
       parseDateTime64BestEffort(JSONExtract(notify_block_json, 'retrieved_time', 'String')) AS retrieved_time
FROM events.notify_block_queue
