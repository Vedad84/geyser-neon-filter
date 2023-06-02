use anyhow::{anyhow, Result};
use clickhouse::Client;
use log::{error, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::watch::Receiver;
use tokio_cron_scheduler::{Job, JobScheduler};
use tryhard::backoff_strategies::BackoffStrategy;
use tryhard::RetryPolicy;

use crate::client::ClickHouse;
use crate::config::{Config, Task};
use crate::prometheus::TaskMetric;

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

async fn execute_interval(
    clients: Arc<Vec<Client>>,
    task: &Task,
    retries: u32,
    shutdown_backoff: ShutdownBackoff,
    shutdown: Receiver<()>,
) -> Option<f64> {
    let time_start = Instant::now();
    for client in clients.iter() {
        if execute_with_retry(
            client,
            task,
            retries,
            shutdown_backoff.clone(),
            shutdown.clone(),
        )
        .await
        .is_err()
        {
            error!(
                "Error executing task {}, {} retries",
                task.task_name, retries
            );
            continue;
        }
        let execution_time = Instant::now().duration_since(time_start);
        return Some(execution_time.as_secs_f64());
    }

    None
}

async fn execute_with_retry(
    client: &Client,
    task: &Task,
    retries: u32,
    shutdown_backoff: ShutdownBackoff,
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

pub async fn add_tasks(
    sched: &mut JobScheduler,
    config: Arc<Config>,
    shutdown: Receiver<()>,
    _task_metrics: Vec<TaskMetric>,
) {
    let retries = config.http_settings.retries;
    let retry_delay = config.http_settings.retry_delay;
    let should_stop = Arc::new(AtomicBool::new(false));
    let shutdown_backoff = ShutdownBackoff {
        retry_delay,
        should_stop: should_stop.clone(),
    };
    let clients = Arc::new(ClickHouse::from_config(config.clone()));

    for task in config.tasks.iter().cloned() {
        let task = Arc::new(task);
        let task_cron = task.cron.clone();
        let task_shutdown = shutdown.clone();
        let shutdown_backoff = shutdown_backoff.clone();
        let clients = clients.clone();
        let should_stop = Arc::clone(&should_stop);

        sched
            .add(
                Job::new_async(task_cron.as_str(), move |_uuid, _l| {
                    let task = Arc::clone(&task);
                    let mut task_shutdown = task_shutdown.clone();
                    let shutdown_backoff = shutdown_backoff.clone();
                    let clients = Arc::clone(&clients);
                    let should_stop = Arc::clone(&should_stop);

                    Box::pin(async move {
                        info!("Starting task execution: {}", task.task_name);
                        tokio::select! {
                            _execution_time = execute_interval(
                                clients,
                                &task,
                                retries,
                                shutdown_backoff,
                                task_shutdown.clone(),
                            ) => {
                                // Use execution_time for metrics
                            }
                            _ = task_shutdown.changed() => {
                                should_stop.store(true, Ordering::SeqCst);
                            }
                        }
                    })
                })
                .expect("Failed to create new_async task"),
            )
            .await
            .expect("Failed to add task to scheduler");
    }
}
