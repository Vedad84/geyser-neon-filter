CREATE OR REPLACE VIEW system.prometheus_metrics (
        `name` String,
        `value` Float64,
        `help` String,
        `labels` Map(String, String),
        `type` String
    ) AS
SELECT 'events_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in notify_block_local' AS help,
    map('hostname', hostName(), 'table', 'notify_block_local') AS labels,
    'gauge' AS type
FROM events.notify_block_local
UNION ALL
SELECT 'events_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in notify_transaction_local' AS help,
    map('hostname', hostName(), 'table', 'notify_transaction_local') AS labels,
    'gauge' AS type
FROM events.notify_transaction_local
UNION ALL
SELECT 'events_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in update_account_local' AS help,
    map('hostname', hostName(), 'table', 'update_account_local') AS labels,
    'gauge' AS type
FROM events.update_account_local
UNION ALL
SELECT 'events_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in update_slot_local' AS help,
    map('hostname', hostName(), 'table', 'update_slot_local') AS labels,
    'gauge' AS type
FROM events.update_slot_local
--- Distributed
UNION ALL
SELECT 'events_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in notify_block_distributed' AS help,
    map('hostname', hostName(), 'table', 'notify_block_distributed') AS labels,
    'gauge' AS type
FROM events.notify_block_distributed
UNION ALL
SELECT 'events_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in notify_transaction_distributed' AS help,
    map('hostname', hostName(), 'table', 'notify_transaction_distributed') AS labels,
    'gauge' AS type
FROM events.notify_transaction_distributed
UNION ALL
SELECT 'events_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in update_account_distributed' AS help,
    map('hostname', hostName(), 'table', 'update_account_distributed') AS labels,
    'gauge' AS type
FROM events.update_account_distributed
UNION ALL
SELECT 'events_row_count' AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in update_slot_distributed' AS help,
    map('hostname', hostName(), 'table', 'update_slot_distributed') AS labels,
    'gauge' AS type
FROM events.update_slot_distributed
ORDER BY name ASC;
