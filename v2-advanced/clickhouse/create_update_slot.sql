CREATE DATABASE IF NOT EXISTS events ON CLUSTER 'events';

CREATE TABLE IF NOT EXISTS events.update_slot ON CLUSTER '{cluster}' (
    slot UInt64 CODEC(DoubleDelta, ZSTD),
    parent Nullable(UInt64) default 0 CODEC(DoubleDelta, ZSTD),
    status Enum('Confirmed' = 1, 'Processed' = 2, 'Rooted' = 3) CODEC(DoubleDelta, ZSTD),
    retrieved_time DateTime64 CODEC(DoubleDelta, ZSTD)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/update_slot',
    '{replica}'
) PRIMARY KEY(slot, status)
PARTITION BY toInt32(slot / 216000)
ORDER BY (slot, status)
SETTINGS index_granularity=8192;

CREATE TABLE IF NOT EXISTS events.update_slot_queue ON CLUSTER '{cluster}' (
    slot UInt64,
    parent Nullable(UInt64),
    status Enum('Confirmed' = 1, 'Processed' = 2, 'Rooted' = 3),
    retrieved_time DateTime64
) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:29092',
kafka_topic_list = 'update_slot',
kafka_group_name = 'clickhouse',
kafka_num_consumers = 1,
kafka_poll_timeout_ms = 200,
kafka_flush_interval_ms = 400,
kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS events.update_slot_queue_mv ON CLUSTER '{cluster}' to events.update_slot AS
SELECT slot,
    parent,
    status,
    retrieved_time
FROM events.update_slot_queue;
