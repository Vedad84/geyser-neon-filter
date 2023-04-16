-- update_account_local and older_account_local are guaranteed to
-- contain data for the same pubkeys in a single shard (see sharding keys for distributed tables)
-- Therefore next operation will not produce inter-node traffic

INSERT INTO events.older_account_distributed
SELECT DISTINCT ON (ual.pubkey)
    ual.pubkey,
    ual.lamports,
    ual.owner,
    ual.executable,
    ual.rent_epoch,
    ual.`data`,
    ual.write_version,
    ual.txn_signature,
    ual.slot,
    ual.is_startup,
    ual.retrieved_time
FROM events.update_account_distributed ual
INNER JOIN events.update_slot us
ON us.slot = ual.slot AND us.status = 'Rooted'
WHERE
    ual.slot > (SELECT MAX(oal.slot) FROM events.older_account_distributed oal)
    AND ual.slot <= (SELECT MAX(usd.slot) - 6480000 FROM events.update_slot usd)
ORDER BY ual.slot DESC, ual.write_version DESC;

OPTIMIZE TABLE events.older_account_local ON CLUSTER '{cluster}' DEDUPLICATE;
