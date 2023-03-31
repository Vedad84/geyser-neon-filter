CREATE TABLE IF NOT EXISTS events.items_to_move (
    pubkey Array(UInt8),
    lamports UInt64,
    owner Array(UInt8),
    executable Bool,
    rent_epoch UInt64,
    data Array(UInt8),
    write_version Int64,
    txn_signature Array(UInt8) DEFAULT [],
    slot UInt64,
    is_startup Bool,
    retrieved_time DateTime64
) ENGINE = Memory;

INSERT INTO events.items_to_move
SELECT
    tual.pubkey,
    tual.lamports,
    tual.owner,
    tual.executable,
    tual.rent_epoch,
    tual.`data`,
    ntl.idx,
    tual.txn_signature,
    tual.slot,
    tual.is_startup,
    tual.retrieved_time
FROM events.temp_update_account_local tual
INNER JOIN events.notify_transaction_local ntl
ON tual.txn_signature = ntl.signature AND tual.slot = ntl.slot
WHERE (tual.txn_signature, tual.slot, tual.pubkey) NOT IN (
    SELECT tuapl.txn_signature, tuapl.slot, tuapl.pubkey FROM events.temp_update_account_processed_local tuapl
);


INSERT INTO events.items_to_move
SELECT
    tual.pubkey,
    tual.lamports,
    tual.owner,
    tual.executable,
    tual.rent_epoch,
    tual.`data`,
    -1,
    tual.txn_signature,
    tual.slot,
    tual.is_startup,
    tual.retrieved_time
FROM events.temp_update_account_local tual
WHERE
    tual.txn_signature = []
    AND (tual.txn_signature, tual.slot, tual.pubkey) NOT IN (
        SELECT tuapl.txn_signature, tuapl.slot, tuapl.pubkey FROM events.temp_update_account_processed_local tuapl
    )

INSERT INTO events.update_account_local
SELECT * FROM events.items_to_move;

INSERT INTO events.temp_update_account_processed_local (
    pubkey, txn_signature, slot
)
SELECT itm.pubkey, itm.txn_signature, itm.slot FROM events.items_to_move itm;

TRUNCATE TABLE events.items_to_move;