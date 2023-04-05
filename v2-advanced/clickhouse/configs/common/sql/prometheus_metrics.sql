CREATE OR REPLACE VIEW system.prometheus_metrics (
        `name` String,
        `value` Float64,
        `help` String,
        `labels` Map(String, String),
        `type` String
    ) AS
SELECT concat('events_', 'notify_block_local') AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in notify_block_local' AS help,
    map('hostname', hostName()) AS labels,
    'gauge' AS type
FROM events.notify_block_local
UNION ALL
SELECT concat('events_', 'notify_transaction_local') AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in notify_transaction_local' AS help,
    map('hostname', hostName()) AS labels,
    'gauge' AS type
FROM events.notify_transaction_local
UNION ALL
SELECT concat('events_', 'update_account_local') AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in update_account_local' AS help,
    map('hostname', hostName()) AS labels,
    'gauge' AS type
FROM events.update_account_local
UNION ALL
SELECT concat('events_', 'update_slot_local') AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in update_slot_local' AS help,
    map('hostname', hostName()) AS labels,
    'gauge' AS type
FROM events.update_slot_local
--- Distributed
UNION ALL
SELECT concat('events_', 'notify_block_distributed') AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in notify_block_distributed' AS help,
    map('hostname', hostName()) AS labels,
    'gauge' AS type
FROM events.notify_block_distributed
UNION ALL
SELECT concat('events_', 'notify_transaction_distributed') AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in notify_transaction_distributed' AS help,
    map('hostname', hostName()) AS labels,
    'gauge' AS type
FROM events.notify_transaction_distributed
UNION ALL
SELECT concat('events_', 'update_account_distributed') AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in update_account_distributed' AS help,
    map('hostname', hostName()) AS labels,
    'gauge' AS type
FROM events.update_account_distributed
UNION ALL
SELECT concat('events_', 'update_slot_distributed') AS name,
    toFloat64(COUNT(*)) AS value,
    'Number of rows in update_slot_distributed' AS help,
    map('hostname', hostName()) AS labels,
    'gauge' AS type
FROM events.update_slot_distributed
ORDER BY name ASC;
