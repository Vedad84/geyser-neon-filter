use anyhow::Result;
use clickhouse::Client;
use log::{info, error};
use tokio::sync::watch::Receiver;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::time::interval;

use crate::client::ClickHouse;
use crate::config::{Config, Task};

async fn execute_queries(client: &Client, task: &Task) -> Result<()> {
    for q in task.queries.iter() {
        match client.query(q).execute().await {
            Ok(_) => {}
            Err(e) => {
                error!("Error executing query: {}, error: {}", q, e);
                return Err(e.into());
            }
        }
    }
    Ok(())
}

async fn execute_task(
    config: Arc<Config>,
    clients: Arc<Vec<Client>>,
    task: Task,
    mut shutdown: Receiver<()>,
) {
    let mut task_interval = interval(task.task_interval);
    let retries = config.http_settings.retries;
    let backoff = config.http_settings.backoff;

    loop {
        tokio::select! {
            _ = task_interval.tick() => {
                info!("Executing task: {}, with interval {:#?}", task.task_name, task.task_interval);

                for client in clients.iter() {
                    let retry_fn = tryhard::retry_fn(|| execute_queries(client, &task))
                                   .retries(retries)
                                   .fixed_backoff(backoff);

                    if let Err(_) = retry_fn.await {
                        error!("Error executing task {}, {} retries", task.task_name,retries);
                        continue;
                    }
                    break;
                }
            }
            _ = shutdown.changed() => {
                info!("Shutting down task: {}", task.task_name);
                break;
            }
        }
    }
}

pub async fn start_tasks(config: Arc<Config>, shutdown: Receiver<()>) {
    let mut set = JoinSet::new();

    let clients = Arc::new(ClickHouse::from_config(&config));
    let tasks = config.tasks.clone();

    for task in tasks {
        let config = Arc::clone(&config);
        let clients = clients.clone();
        let task_shutdown = shutdown.clone();
        set.spawn(async move { execute_task(config, clients, task, task_shutdown).await });
    }

    while let Some(_res) = set.join_next().await {}
}
