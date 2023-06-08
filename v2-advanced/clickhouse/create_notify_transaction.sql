CREATE DATABASE IF NOT EXISTS events ON CLUSTER 'events';

CREATE TABLE IF NOT EXISTS events.notify_transaction_local ON CLUSTER '{cluster}' (
    slot UInt64 CODEC(DoubleDelta, ZSTD),
    signature Array(UInt8) CODEC(ZSTD),
    notify_transaction_json String CODEC(ZSTD(5)),
    retrieved_time DateTime64 CODEC(DoubleDelta, ZSTD),
    idx UInt64 CODEC(DoubleDelta, ZSTD)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/notify_transaction_local',
    '{replica}'
) PRIMARY KEY (signature, slot)
PARTITION BY toInt32(slot / 216000)
ORDER BY (signature, slot)
SETTINGS index_granularity=8192;

CREATE TABLE IF NOT EXISTS events.notify_transaction_distributed ON CLUSTER '{cluster}' AS events.notify_transaction_local
ENGINE = Distributed('{cluster}', events, notify_transaction_local, xxHash64(CONCAT(toString(slot), arrayStringConcat(signature,''))));

CREATE TABLE IF NOT EXISTS events.notify_transaction_queue ON CLUSTER '{cluster}' (
    notify_transaction_json String
)   ENGINE = Kafka SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'notify_transaction',
    kafka_group_name = 'clickhouse',
    kafka_num_consumers = 1,
    kafka_poll_timeout_ms = 200,
    kafka_flush_interval_ms = 1000,
    kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW IF NOT EXISTS events.notify_transaction_queue_mv ON CLUSTER '{cluster}' TO events.notify_transaction_distributed AS
SELECT JSONExtract(notify_transaction_json, 'slot', 'UInt64') AS slot,
       JSONExtract(notify_transaction_json, 'signature', 'Array(Nullable(UInt8))') AS signature,
       notify_transaction_json,
       parseDateTime64BestEffort(JSONExtract(notify_transaction_json, 'retrieved_time', 'String')) AS retrieved_time,
       JSONExtract(notify_transaction_json, 'index', 'UInt64') AS idx
FROM events.notify_transaction_queue;
