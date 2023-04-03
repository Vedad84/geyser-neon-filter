use anyhow::Result;
use clickhouse::Client;
use log::{error, info};
use std::sync::atomic::AtomicBool;
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

#[derive(Debug, Clone)]
pub struct ShutdownBackoff {
    pub(crate) delay: Duration,
    pub(crate) should_stop: Arc<AtomicBool>,
}

impl<'a, E> BackoffStrategy<'a, E> for ShutdownBackoff {
    type Output = RetryPolicy;

    #[inline]
    fn delay(&mut self, _attempt: u32, _error: &'a E) -> RetryPolicy {
        if self.should_stop.load(std::sync::atomic::Ordering::Relaxed) {
            return RetryPolicy::Break;
        }
        RetryPolicy::Delay(self.delay)
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
    let backoff = config.http_settings.backoff;
    let should_stop = Arc::new(AtomicBool::new(false));
    let shutdown_backoff = ShutdownBackoff {
        delay: backoff,
        should_stop: should_stop.clone(),
    };

    loop {
        tokio::select! {
            _ = task_interval.tick() => {
                info!("Executing task: {}, with interval {:#?}", task.task_name, task.task_interval);

                for client in clients.iter() {
                    let retry_fn = tryhard::retry_fn(|| execute_queries(client, &task))
                                   .retries(retries)
                                   .custom_backoff(shutdown_backoff.clone());

                    if retry_fn.await.is_err() {
                        error!("Error executing task {}, {} retries", task.task_name,retries);
                        continue;
                    }
                    break;
                }
            }
            _ = shutdown.changed() => {
                info!("Shutting down task: {}", task.task_name);
                should_stop.store(true, std::sync::atomic::Ordering::Relaxed);
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
