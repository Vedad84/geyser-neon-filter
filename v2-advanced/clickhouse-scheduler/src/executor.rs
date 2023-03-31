use clickhouse::Client;
use log::info;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::interval;

use crate::client::ClickHouse;
use crate::config::{Config, Task};

async fn execute_task(_clients: Arc<Vec<Client>>, task: Task, shutdown: Arc<Notify>) {
    let mut task_interval = interval(task.task_interval);

    loop {
        tokio::select! {
            _ = task_interval.tick() => {
                info!("Executing task: {}, with interval {:#?}", task.task_name, task.task_interval);
            }
            _ = shutdown.notified() => {
                info!("Shutting down task: {}", task.task_name);
                break;
            }
        }
    }
}

pub async fn start_tasks(config: Config, shutdown: Arc<Notify>) {
    let mut set = JoinSet::new();

    let clients = Arc::new(ClickHouse::from_config(&config));

    for task in config.tasks.into_iter() {
        let clients = clients.clone();
        let task_shutdown = shutdown.clone();
        set.spawn(async move { execute_task(clients, task, task_shutdown).await });
    }

    while let Some(_res) = set.join_next().await {}
}
