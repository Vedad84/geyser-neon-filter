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
use std::time::SystemTime;

use crate::client::ClickHouse;
use crate::config::{Config, Task};
use crate::prometheus::TaskMetric;
use std::collections::VecDeque;

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
    metric: TaskMetric,
) {
    let mut task_interval = interval(task.task_interval);
    let retries = config.http_settings.retries;
    let retry_delay = config.http_settings.retry_delay;
    let should_stop = Arc::new(AtomicBool::new(false));
    let shutdown_backoff = ShutdownBackoff {
        retry_delay,
        should_stop: should_stop.clone(),
    };
    let mut measurement_queue: VecDeque<f64> = VecDeque::from(vec![0_f64; task.average_depth]);

    loop {
        tokio::select! {
            _ = task_interval.tick() => {
                let execution_time = execute_interval(&clients, &task, retries, &shutdown_backoff, shutdown.clone()).await;
                if let Some(execution_time) = execution_time {
                    measurement_queue.push_back(execution_time);
                    measurement_queue.pop_front();
                    metric.avg_time.set(measurement_queue.iter().sum::<f64>() / measurement_queue.len() as f64);
                    metric.min_time.set(measurement_queue.iter().min_by( |a, b| a.total_cmp(b)).unwrap_or(&0_f64).clone());
                    metric.max_time.set(measurement_queue.iter().max_by( |a, b| a.total_cmp(b)).unwrap_or(&0_f64).clone());
                 }
                else {
                    metric.errors.inc();
                }
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
) -> Option<f64> {
    info!(
        "Executing task: {}, with interval {:#?}",
        task.task_name, task.task_interval
    );

    let time_start = SystemTime::now();
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
        let execution_time = SystemTime::now().duration_since(time_start).unwrap_or(Duration::default());
        return  Some(execution_time.as_secs_f64());
    }

    return None
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

pub async fn start_tasks(config: Arc<Config>, shutdown: Receiver<()>, task_metrics: Vec<TaskMetric>  ) {
    let mut set = JoinSet::new();

    let clients = Arc::new(ClickHouse::from_config(&config));
    let tasks = config.tasks.clone();

    for (task, metric) in tasks.iter().zip(task_metrics) {
        let task = task.clone();
        // let metric = metric.clone();
        let config = Arc::clone(&config);
        let clients = clients.clone();
        let task_shutdown = shutdown.clone();
        set.spawn(async move { execute_task(config, clients, task, task_shutdown, metric).await });
    }

    while let Some(_res) = set.join_next().await {}
}
