CREATE DATABASE IF NOT EXISTS events ON CLUSTER 'events';

CREATE TABLE IF NOT EXISTS events.older_account_local ON CLUSTER '{cluster}' (
    pubkey Array(UInt8) CODEC(ZSTD),
    lamports UInt64 CODEC(DoubleDelta, ZSTD),
    owner Array(UInt8) CODEC(ZSTD),
    executable Bool CODEC(ZSTD),
    rent_epoch UInt64 CODEC(DoubleDelta, ZSTD),
    data Array(UInt8) CODEC(ZSTD),
    write_version Int64 CODEC(DoubleDelta, ZSTD),
    txn_signature Array(Nullable(UInt8)) CODEC(ZSTD),
    slot UInt64 CODEC(DoubleDelta, ZSTD),
    is_startup Bool CODEC(ZSTD),
    retrieved_time DateTime64 CODEC(DoubleDelta, ZSTD)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/older_account_local',
    '{replica}',
    slot
) PRIMARY KEY (pubkey)
ORDER BY (pubkey)
SETTINGS index_granularity=8192;


CREATE TABLE IF NOT EXISTS events.older_account_distributed ON CLUSTER '{cluster}' AS events.older_account_local
    ENGINE = Distributed('{cluster}', events, older_account_local, xxHash64(arrayStringConcat(pubkey,'')));
