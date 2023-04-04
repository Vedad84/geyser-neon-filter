use anyhow::{anyhow, Result};
use clickhouse::Client;
use log::{error, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch::Receiver;
use tokio::task::JoinSet;
use tokio::time::interval;
use tryhard::backoff_strategies::BackoffStrategy;
use tryhard::RetryPolicy;

use crate::client::ClickHouse;
use crate::config::{Config, Task};

async fn execute_queries(client: &Client, task: &Task) -> Result<()> {
    for q in task.queries.iter() {
        if let Err(e) = client.query(q).execute().await {
            error!("Error executing query: {}, error: {}", q, e);
            return Err(e.into());
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ShutdownBackoff {
    pub(crate) retry_delay: Duration,
    pub(crate) should_stop: Arc<AtomicBool>,
}

impl<'a, E> BackoffStrategy<'a, E> for ShutdownBackoff {
    type Output = RetryPolicy;

    #[inline]
    fn delay(&mut self, _attempt: u32, _error: &'a E) -> RetryPolicy {
        if self.should_stop.load(Ordering::Relaxed) {
            return RetryPolicy::Break;
        }
        RetryPolicy::Delay(self.retry_delay)
    }
}
async fn execute_task(
    config: Arc<Config>,
    clients: Arc<Vec<Client>>,
    task: Task,
    mut shutdown: Receiver<()>,
) {
    let mut task_interval = interval(task.task_interval);
    let retries = config.http_settings.retries;
    let retry_delay = config.http_settings.retry_delay;
    let should_stop = Arc::new(AtomicBool::new(false));
    let shutdown_backoff = ShutdownBackoff {
        retry_delay,
        should_stop: should_stop.clone(),
    };

    loop {
        tokio::select! {
            _ = task_interval.tick() => {
                execute_interval(&clients, &task, retries, &shutdown_backoff, shutdown.clone()).await;
            }
            _ = shutdown.changed() => {
                info!("Shutting down task: {}", task.task_name);
                should_stop.store(true, Ordering::Relaxed);
                break;
            }
        }
    }
}

async fn execute_interval(
    clients: &Arc<Vec<Client>>,
    task: &Task,
    retries: u32,
    shutdown_backoff: &ShutdownBackoff,
    shutdown: Receiver<()>,
) {
    info!(
        "Executing task: {}, with interval {:#?}",
        task.task_name, task.task_interval
    );

    for client in clients.iter() {
        if execute_with_retry(client, task, retries, shutdown_backoff, shutdown.clone())
            .await
            .is_err()
        {
            error!(
                "Error executing task {}, {} retries",
                task.task_name, retries
            );
            continue;
        }
        break;
    }
}

async fn execute_with_retry(
    client: &Client,
    task: &Task,
    retries: u32,
    shutdown_backoff: &ShutdownBackoff,
    mut shutdown: Receiver<()>,
) -> Result<(), anyhow::Error> {
    let retry_fn = tryhard::retry_fn(|| execute_queries(client, task))
        .retries(retries)
        .custom_backoff(shutdown_backoff.clone());

    tokio::select! {
        result = retry_fn => result,
        _ = shutdown.changed() => Err(anyhow!("Shutting down task: {}", task.task_name)),
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
