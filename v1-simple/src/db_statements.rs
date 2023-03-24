use anyhow::anyhow;
use anyhow::Result;
use deadpool_postgres::Client;
use tokio_postgres::Statement;

pub async fn create_account_insert_statement(client: &Client) -> Result<Statement> {
    let stmt = "CALL process_account_update($1, $2, $3, $4, $5, $6, $7, $8, $9)";

    let stmt = client.prepare_cached(stmt).await;

    match stmt {
        Ok(update_account_stmt) => Ok(update_account_stmt),
        Err(err) => Err(anyhow!(err)),
    }
}

pub async fn create_transaction_insert_statement(client: &Client) -> Result<Statement> {
    let stmt =
        "INSERT INTO transaction AS txn (signature, is_vote, slot, message_type, legacy_message, \
        v0_loaded_message, signatures, message_hash, meta, write_version, updated_on) \
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
        ON CONFLICT (slot, signature) DO UPDATE SET is_vote=excluded.is_vote, \
        message_type=excluded.message_type, \
        legacy_message=excluded.legacy_message, \
        v0_loaded_message=excluded.v0_loaded_message, \
        signatures=excluded.signatures, \
        message_hash=excluded.message_hash, \
        meta=excluded.meta, \
        write_version=excluded.write_version, \
        updated_on=excluded.updated_on";

    let stmt = client.prepare_cached(stmt).await;

    match stmt {
        Ok(transaction_insert_stmt) => Ok(transaction_insert_stmt),
        Err(err) => Err(anyhow!(err)),
    }
}

pub async fn create_block_metadata_insert_statement(client: &Client) -> Result<Statement> {
    let stmt =
        "INSERT INTO block (slot, blockhash, rewards, block_time, block_height, updated_on) \
    VALUES ($1, $2, $3, $4, $5, $6) \
    ON CONFLICT DO NOTHING";

    let stmt = client.prepare_cached(stmt).await;

    match stmt {
        Ok(notify_block_metadata_stmt) => Ok(notify_block_metadata_stmt),
        Err(err) => Err(anyhow!(err)),
    }
}

pub async fn create_slot_insert_statement_with_parent(client: &Client) -> Result<Statement> {
    let stmt = "INSERT INTO slot (slot, parent, status, updated_on) \
    VALUES ($1, $2, $3, $4) \
    ON CONFLICT (slot) DO UPDATE SET parent=excluded.parent, status=excluded.status, updated_on=excluded.updated_on";

    let stmt = client.prepare_cached(stmt).await;

    match stmt {
        Ok(notify_block_metadata_stmt) => Ok(notify_block_metadata_stmt),
        Err(err) => Err(anyhow!(err)),
    }
}

pub async fn create_slot_insert_statement_without_parent(client: &Client) -> Result<Statement> {
    let stmt = "INSERT INTO slot (slot, status, updated_on) \
    VALUES ($1, $2, $3) \
    ON CONFLICT (slot) DO UPDATE SET status=excluded.status, updated_on=excluded.updated_on";

    let stmt = client.prepare_cached(stmt).await;

    match stmt {
        Ok(notify_block_metadata_stmt) => Ok(notify_block_metadata_stmt),
        Err(err) => Err(anyhow!(err)),
    }
}
