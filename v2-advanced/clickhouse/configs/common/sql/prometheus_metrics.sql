CREATE OR REPLACE VIEW system.prometheus_metrics (
        `name` String,
        `value` Float64,
        `help` String,
        `labels` Map(String, String),
        `type` String
    ) AS
SELECT 'local_event_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in the table' AS help,
    map('hostname', hostName(), 'table', 'notify_block_local') AS labels,
    'gauge' AS type
FROM events.notify_block_local
UNION ALL
SELECT 'local_event_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in the table' AS help,
    map('hostname', hostName(), 'table', 'notify_transaction_local') AS labels,
    'gauge' AS type
FROM events.notify_transaction_local
UNION ALL
SELECT 'local_event_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in the table' AS help,
    map('hostname', hostName(), 'table', 'update_account_local') AS labels,
    'gauge' AS type
FROM events.update_account_local
UNION ALL
SELECT 'local_event_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in the table' AS help,
    map('hostname', hostName(), 'table', 'update_slot_local') AS labels,
    'gauge' AS type
FROM events.update_slot_local
--- Distributed
UNION ALL
SELECT 'event_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in the table' AS help,
    map('hostname', hostName(), 'table', 'notify_block_distributed') AS labels,
    'gauge' AS type
FROM events.notify_block_distributed
UNION ALL
SELECT 'event_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in the table' AS help,
    map('hostname', hostName(), 'table', 'notify_transaction_distributed') AS labels,
    'gauge' AS type
FROM events.notify_transaction_distributed
UNION ALL
SELECT 'event_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in the table' AS help,
    map('hostname', hostName(), 'table', 'update_account_distributed') AS labels,
    'gauge' AS type
FROM events.update_account_distributed
UNION ALL
SELECT 'event_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in the table' AS help,
    map('hostname', hostName(), 'table', 'update_slot_distributed') AS labels,
    'gauge' AS type
FROM events.update_slot_distributed
UNION ALL
--- Clickhouse stats
SELECT 'clickhouse_select_query' as name,
       toFloat64(value) AS value,
       description as help,
       map('hostname', hostName()) as labels,
       'counter' as type
FROM system.events
WHERE event = 'SelectQuery'
---
UNION ALL
---
SELECT 'clickhouse_failed_query' as name,
       toFloat64(value) AS value,
       description as help,
       map('hostname', hostName()) as labels,
       'counter' as type
FROM system.events
WHERE event = 'FailedQuery'
---
UNION ALL
---
SELECT 'clickhouse_failed_select_query' as name,
       toFloat64(value) AS value,
       description as help,
       map('hostname', hostName()) as labels,
       'counter' as type
FROM system.events
WHERE event = 'FailedSelectQuery'
---
UNION ALL
---
SELECT 'clickhouse_inserted_rows' as name,
       toFloat64(value) AS value,
       description as help,
       map('hostname', hostName()) as labels,
       'counter' as type
FROM system.events
WHERE event = 'InsertedRows'
---
UNION ALL
---
SELECT 'clickhouse_kafka_messages_polled' as name,
       toFloat64(value) AS value,
       description as help,
       map('hostname', hostName()) as labels,
       'counter' as type
FROM system.events
WHERE event = 'KafkaMessagesPolled'
---
UNION ALL
---
SELECT 'clickhouse_delayed_inserts' as name,
       toFloat64(value) AS value,
       description as help,
       map('hostname', hostName()) as labels,
       'gauge' as type
FROM system.metrics
WHERE metric = 'DelayedInserts'
---
UNION ALL
---
SELECT 'clickhouse_query' as name,
       toFloat64(value) AS value,
       description as help,
       map('hostname', hostName()) as labels,
       'gauge' as type
FROM system.metrics
WHERE metric = 'Query'
---
UNION ALL
---
SELECT 'clickhouse_merge' as name,
       toFloat64(value) AS value,
       description as help,
       map('hostname', hostName()) as labels,
       'gauge' as type
FROM system.metrics
WHERE metric = 'Merge'
---
UNION ALL
---
SELECT 'clickhouse_move' as name,
       toFloat64(value) AS value,
       description as help,
       map('hostname', hostName()) as labels,
       'gauge' as type
FROM system.metrics
WHERE metric = 'Move'
---
UNION ALL
---
SELECT 'table_compressed_size_gbytes' AS name,
    toFloat64(SUM(data_compressed_bytes) / (1024 * 1024 * 1024)) AS value,
    'Compressed table size in gigabytes' AS help,
    map('hostname', hostName(), 'table', 'notify_block_local') AS labels,
    'gauge' AS type
FROM system.parts
WHERE active = 1 AND table = 'notify_block_local'
GROUP BY table
---
UNION ALL
---
SELECT 'table_uncompressed_size_gbytes' AS name,
    toFloat64(SUM(data_uncompressed_bytes) / (1024 * 1024 * 1024)) AS value,
    'Uncompressed table size in gigabytes' AS help,
    map('hostname', hostName(), 'table', 'notify_block_local') AS labels,
    'gauge' AS type
FROM system.parts
WHERE active = 1 AND table = 'notify_block_local'
GROUP BY table
---
UNION ALL
---
SELECT 'table_compressed_size_gbytes' AS name,
    toFloat64(SUM(data_compressed_bytes) / (1024 * 1024 * 1024)) AS value,
    'Compressed table size in gigabytes' AS help,
    map('hostname', hostName(), 'table', 'notify_transaction_local') AS labels,
    'gauge' AS type
FROM system.parts
WHERE active = 1 AND table = 'notify_transaction_local'
GROUP BY table
---
UNION ALL
---
SELECT 'table_uncompressed_size_gbytes' AS name,
    toFloat64(SUM(data_uncompressed_bytes) / (1024 * 1024 * 1024)) AS value,
    'Uncompressed table size in gigabytes' AS help,
    map('hostname', hostName(), 'table', 'notify_transaction_local') AS labels,
    'gauge' AS type
FROM system.parts
WHERE active = 1 AND table = 'notify_transaction_local'
GROUP BY table
---
UNION ALL
---
SELECT 'table_compressed_size_gbytes' AS name,
    toFloat64(SUM(data_compressed_bytes) / (1024 * 1024 * 1024)) AS value,
    'Compressed table size in gigabytes' AS help,
    map('hostname', hostName(), 'table', 'update_account_local') AS labels,
    'gauge' AS type
FROM system.parts
WHERE active = 1 AND table = 'update_account_local'
GROUP BY table
---
UNION ALL
---
SELECT 'table_uncompressed_size_gbytes' AS name,
    toFloat64(SUM(data_uncompressed_bytes) / (1024 * 1024 * 1024)) AS value,
    'Uncompressed table size in gigabytes' AS help,
    map('hostname', hostName(), 'table', 'update_account_local') AS labels,
    'gauge' AS type
FROM system.parts
WHERE active = 1 AND table = 'update_account_local'
GROUP BY table
---
UNION ALL
---
SELECT 'table_compressed_size_gbytes' AS name,
    toFloat64(SUM(data_compressed_bytes) / (1024 * 1024 * 1024)) AS value,
    'Compressed table size in gigabytes' AS help,
    map('hostname', hostName(), 'table', 'update_slot_local') AS labels,
    'gauge' AS type
FROM system.parts
WHERE active = 1 AND table = 'update_slot_local'
GROUP BY table
---
UNION ALL
---
SELECT 'table_uncompressed_size_gbytes' AS name,
    toFloat64(SUM(data_uncompressed_bytes) / (1024 * 1024 * 1024)) AS value,
    'Uncompressed table size in gigabytes' AS help,
    map('hostname', hostName(), 'table', 'update_slot_local') AS labels,
    'gauge' AS type
FROM system.parts
WHERE active = 1 AND table = 'update_slot_local'
GROUP BY table
ORDER BY name ASC;
