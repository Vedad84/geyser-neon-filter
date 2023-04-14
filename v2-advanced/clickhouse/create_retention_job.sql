INSERT INTO events.older_account_local
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
    ual.retrieved_time,
    (SELECT MAX(oal.retention_counter) + 1 FROM events.older_account_local oal)
FROM events.update_account_local ual
INNER JOIN events.update_slot us
ON us.slot = ual.slot AND us.status = 'Rooted'
WHERE
    ual.slot > (SELECT MAX(oal.slot) FROM events.older_account_local oal)
    AND ual.slot <= (SELECT MAX(usd.slot) - 6480000 FROM events.update_slot usd)
ORDER BY ual.pubkey DESC, ual.slot DESC, ual.write_version DESC;

OPTIMIZE TABLE events.older_account_local DEDUPLICATE;
