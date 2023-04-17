-- update_account_local and older_account_local are guaranteed to
-- contain data for the same pubkeys in a single shard (see sharding keys for distributed tables)
-- Therefore next operation will not produce inter-node traffic

INSERT INTO events.older_account_distributed
SELECT
    uad.pubkey,
    uad.lamports,
    uad.owner,
    uad.executable,
    uad.rent_epoch,
    uad.`data`,
    uad.write_version,
    uad.txn_signature,
    uad.slot,
    uad.is_startup,
    uad.retrieved_time
FROM events.update_account_distributed uad
INNER JOIN events.update_slot us1
ON
    us1.slot = uad.slot AND us1.status = 'Rooted'
WHERE
    uad.slot > (SELECT MAX(oad.slot) FROM events.older_account_distributed oad)
    AND uad.slot <= (SELECT MAX(us2.slot) - 6480000 FROM events.update_slot us2)
ORDER BY uad.slot DESC, uad.pubkey DESC, uad.write_version DESC
LIMIT 1 BY uad.pubkey;

OPTIMIZE TABLE events.older_account_local ON CLUSTER '{cluster}' DEDUPLICATE;
