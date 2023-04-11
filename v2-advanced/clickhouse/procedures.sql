CREATE FUNCTION get_recent_update_slot ON CLUSTER 'events' AS (_pubkey, _slot)->(
    SELECT slot
    FROM events.update_account_distributed
    WHERE pubkey = _pubkey
        AND slot <= _slot
    ORDER BY write_version DESC
    LIMIT 1
);

CREATE FUNCTION get_earliest_slot ON CLUSTER 'events' AS ()->(
    SELECT MIN(slot) FROM events.update_slot
);
