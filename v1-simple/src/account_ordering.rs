use {
    deadpool_postgres::Pool,
    log::{ info, warn },
    tokio::{ self, sync::watch },
    std::sync::Arc,
};

pub async fn run_account_ordering(
    db_pool: Arc<Pool>,
    mut sigterm_rx: watch::Receiver<()>,
) {
    info!("Starting account ordering...");
    let sleep_int = std::time::Duration::from_secs(1);
    let mut interval = tokio::time::interval(sleep_int);

    interval.tick().await;
    loop {
        tokio::select! {
            _ = sigterm_rx.changed() => {
                break;
            }
            _ = interval.tick() => {
                match db_pool.get().await {
                    Ok(client) => {
                        if let Ok(stmt) = client.prepare("CALL public.order_accounts()").await {
                            let res = client.query(&stmt, &[]).await;
                            if let Err(res) = res {
                                warn!("Failed to run order_accounts: {:?}", res);
                            }
                        } else {
                            warn!("Failed to prepare order_accounts query");
                        }
                    }
                    Err(err) => {
                        warn!("Account ordering: Failed to get client from pool: {:?}", err);
                    }
                }
            }
        }
    }

    info!("Account ordering stopped.");
}
