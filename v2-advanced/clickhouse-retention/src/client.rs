use {
    clickhouse::Client,
    crate::config::Config,
    log::info,
    std::{sync::Arc, time::Instant},
    rand::Rng,
};

pub struct Connection<'a> {
    pub client: Arc<Client>,
    pub config: &'a Config,
}

pub async fn insert_range(client: Arc<Client>, first_slot: u64, last_slot: u64, partition: String)
                          -> Result<(), clickhouse::error::Error> {

    let start = Instant::now();
    info!("slots {}..{} are being copied", first_slot, last_slot);
    let sql = r#"
             INSERT INTO events.older_account_distributed
             SELECT
                uad.pubkey,
                uad.lamports,
                uad.owner,
                uad.executable,
                uad.rent_epoch,
                uad.data,
                uad.write_version,
                uad.txn_signature,
                uad.slot,
                uad.is_startup,
                uad.retrieved_time
                FROM events.update_account_distributed AS uad
            WHERE
                    uad._partition_id = ?
                and uad.slot IN (
                    SELECT us.slot
                    FROM
                        events.update_slot AS us
                    WHERE
                        us.slot >= ?
                    and us.slot <  ?
                    and	us.status = 'Rooted'
            )
            ORDER BY
                uad.pubkey ASC,
                uad.slot DESC,
                uad.write_version DESC
            LIMIT 1 BY uad.pubkey
        "#;

    client
        .query(sql)
        .bind(partition)
        .bind(first_slot)
        .bind(last_slot)
        .execute().await?;

    let elapsed = Instant::now().duration_since(start).as_secs_f64();

    info!("slots {}..{} done, task took {:.2} sec" , first_slot,last_slot, elapsed);
    Ok(())
}

impl <'a>Connection<'a> {
    pub fn new(config: &'a Config) -> Self {
        let id = rand::thread_rng().gen_range(0..config.clickhouse_url.len());
        let url = config.clickhouse_url.get(id).expect("clickhouse_url not found");

        let connection = match (&config.clickhouse_user, &config.clickhouse_password) {
            (None, None | Some(_)) => Client::default().with_url(url),
            (Some(user), None) => Client::default().with_url(url).with_user(user),
            (Some(user), Some(password)) => Client::default()
                .with_url(url)
                .with_user(user)
                .with_password(password),
        };

        Connection {
            client: Arc::new(connection),
            config: config,
        }
    }

    async fn row_count(&self, first_slot: u64, last_slot: u64, partition: &String)
                                 -> Result<u64, clickhouse::error::Error> {
        let sql = r#"
            select count() from (
            SELECT uad.pubkey
                FROM events.update_account_distributed AS uad final
            WHERE
                    uad._partition_id = ?
                and uad.slot IN (
                    SELECT us.slot
                    FROM
                        events.update_slot AS us
                    WHERE
                        us.slot >= ?
                    and us.slot <= ?
                    and	us.status = 'Rooted'
            )
            ORDER BY
                uad.pubkey ASC,
                uad.slot DESC,
                uad.write_version DESC
            LIMIT 1 BY uad.pubkey
            )
        "#;

        let count = self.client
            .query(sql)
            .bind(partition)
            .bind(first_slot)
            .bind(last_slot)
            .fetch_one()
            .await?;

        Ok(count)
    }

    async fn get_partition(&self, offset: u64) -> Result<String, clickhouse::error::Error> {
        let sql = "select max(slot) from events.update_account_distributed final";
        let max_slot: u64 = self.client.query(sql).fetch_one().await?;
        let partition: u64 = max_slot/ 216000 - offset;
        Ok(partition.to_string())
    }

    pub async fn insert_to_older_account(&self) -> Result<(), clickhouse::error::Error> {
        let time_start = Instant::now();
        let partition = self.get_partition(self.config.copy_partition_offset).await?;

        let sql = "select min(slot) from events.update_account_distributed where _partition_id = ?";
        let slot_start: u64 = self.client.query(sql).bind(&partition).fetch_one().await?;

        let sql = "select max(slot) from events.update_account_distributed where _partition_id = ?";
        let slot_end: u64 = self.client.query(sql).bind(&partition).fetch_one().await?;

        info!("partition to copy: {:?}", &partition);

        let row_count = self.row_count(slot_start, slot_end, &partition).await?;
        if row_count == 0 {
            return Err(clickhouse::error::Error::Custom("no rows found to copy".to_string()));
        }
        info!("{} rows should be copied", row_count);

        info!("slots to copy: {:?}..{:?}", slot_start, slot_end);
        info!("tasks at same time {}, slot range to task {}", self.config.tasks_at_same_time, self.config.slot_range_to_task);
        let mut slot = slot_start;
        let range = self.config.slot_range_to_task;
        let mut tasks = vec![];

        loop {
            tasks.push(
                tokio::spawn(
                    insert_range( self.client.clone(), slot, slot + range, partition.clone())
                )
            );
            if tasks.len() as u64 == self.config.tasks_at_same_time {
                for task in &mut *tasks {
                    task.await.expect("task await error")?;
                }
                tasks.clear();
            }

            slot = slot + range;
            if slot > slot_end {
                break;
            }
        }

        if !tasks.is_empty() {
            for task in tasks {
                task.await.expect("task await error")?;
            }
        }
        let execution_time = Instant::now().duration_since(time_start);
        info!("execution time: {} sec", execution_time.as_secs());

        Ok(())
    }

    pub async fn drop_partition(&self) -> Result<(), clickhouse::error::Error> {
        let partition = self.get_partition(self.config.drop_partition_offset).await?;
        info!("partition to drop: {:?}", &partition);
        let sql = "ALTER TABLE events.update_account_local ON CLUSTER '{cluster}' DROP PARTITION ?";
        self.client.query(sql).bind(&partition).execute().await?;
        Ok(())
    }
}
