use clickhouse::Client;
use log::info;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::interval;

use humantime::parse_duration;
use serde::Deserialize;

use crate::client::ClickHouse;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: String,
    pub username: String,
    pub password: String,
    pub tasks: TaskList,
    pub log_path: String,
}

#[derive(Debug, Deserialize)]
pub struct Task {
    task_name: String,
    cron_time: String,
}

pub type TaskList = Vec<Task>;

async fn execute_task(_client: Client, task: Task, shutdown: Arc<Notify>) {
    let duration = parse_duration(&task.cron_time).expect("Unable to parse cron_time as duration");
    let mut task_interval = interval(duration);

    loop {
        tokio::select! {
            _ = task_interval.tick() => {
                info!("Executing task: {}", task.task_name);
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

    let client = ClickHouse::from_config(&config);

    for task in config.tasks.into_iter() {
        let client = client.clone();
        let task_shutdown = shutdown.clone();
        set.spawn(async move { execute_task(client, task, task_shutdown).await });
    }

    while let Some(_res) = set.join_next().await {}
}
