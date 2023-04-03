INSERT INTO events.items_to_retention
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
WHERE
    ual.slot > (SELECT MAX(oal.slot) FROM events.older_account_distributed oal)
    AND ual.slot <= (SELECT MAX(usd.slot) - 6480000 FROM events.update_slot_distributed usd)
ORDER BY ual.pubkey, ual.slot DESC, ual.write_version DESC;

INSERT INTO events.older_account_local
SELECT * FROM events.items_to_retention;

TRUNCATE TABLE events.items_to_retention;

--DELETE FROM events.older_account_local
--WHERE pubkey IN (SELECT itm.pubkey FROM events.items_to_retention itm);

