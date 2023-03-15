use flume::Sender;
use kafka_common::message_type::{EventType, GetEvent, GetMessageType, MessageType};
use log::{error, info};
use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::BorrowedMessage,
    ClientConfig, Message,
};
use serde::Deserialize;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::RwLock;

use crate::{
    app_config::AppConfig,
    consumer_stats::{ContextWithStats, Stats},
    filter::{process_account_info, process_transaction_info},
    filter_config::FilterConfig,
};

pub fn extract_from_message<'a>(message: &'a BorrowedMessage<'a>) -> Option<&'a str> {
    let payload = match message.payload_view::<str>() {
        None => None,
        Some(Ok(s)) => Some(s),
        Some(Err(e)) => {
            error!("Error while deserializing message payload: {:?}", e);
            None
        }
    };
    payload
}

pub fn get_counters(
    stats: &Arc<Stats>,
    message_type: MessageType,
) -> (&Counter<u64, AtomicU64>, &Gauge<f64, AtomicU64>) {
    match message_type {
        MessageType::UpdateAccount => {
            (&stats.kafka_update_account, &stats.queue_len_update_account)
        }
        MessageType::UpdateSlot => (&stats.kafka_update_slot, &stats.queue_len_update_slot),
        MessageType::NotifyTransaction => (
            &stats.kafka_notify_transaction,
            &stats.queue_len_notify_transaction,
        ),
        MessageType::NotifyBlock => (&stats.kafka_notify_block, &stats.queue_len_notify_block),
    }
}

async fn process_event<'a>(
    filter_config: Arc<RwLock<FilterConfig>>,
    event: &'a EventType<'a>,
) -> bool {
    match event {
        EventType::UpdateAccount(update_account) => {
            process_account_info(filter_config, update_account).await
        }
        EventType::NotifyTransaction(notify_transaction) => {
            process_transaction_info(filter_config, notify_transaction).await
        }
        _ => true,
    }
}

pub async fn process_message<T, S>(
    filter_config: Arc<RwLock<FilterConfig>>,
    message: BorrowedMessage<'_>,
    filter_tx: Sender<S>,
    stats: Arc<Stats>,
) where
    T: for<'a> Deserialize<'a> + Send + 'static + GetMessageType + GetEvent,
    S: From<T> + Send + 'static,
{
    if let Some(payload) = extract_from_message(&message) {
        let type_name = std::any::type_name::<T>();
        stats
            .kafka_bytes_rx
            .inner()
            .fetch_add(payload.len() as u64, Ordering::Relaxed);

        let result: serde_json::Result<T> = serde_json::from_str::<T>(payload);

        match result {
            Ok(event) => {
                let event_inner_type = event.as_ref();
                if !process_event(filter_config.clone(), &event_inner_type).await {
                    stats.filtered_events.inc();
                    return;
                }

                let (received, queue_len) = get_counters(&stats, event.get_type());
                queue_len.set(filter_tx.len() as f64);
                received.inc();

                if let Err(e) = filter_tx.send_async(Into::<S>::into(event)).await {
                    error!("Failed to send the data {type_name}, error {e}");
                }
            }
            Err(e) => {
                error!("Failed to deserialize {type_name} {e}");
                stats.kafka_errors_deserialize.inc();
            }
        }
    } else {
        error!("Extracted empty payload!");
    }
}

pub async fn consumer<T, S>(
    config: Arc<AppConfig>,
    filter_config: Arc<RwLock<FilterConfig>>,
    topic: String,
    filter_tx: Sender<S>,
    ctx_stats: ContextWithStats,
) where
    T: for<'a> Deserialize<'a> + Send + 'static + GetMessageType + GetEvent,
    S: From<T> + Send + 'static,
{
    let type_name = std::any::type_name::<T>();
    let stats = ctx_stats.stats.clone();
    let consumer: StreamConsumer<ContextWithStats> = ClientConfig::new()
        .set("group.id", &config.kafka_consumer_group_id)
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", &config.session_timeout_ms)
        .set("auto.commit.interval.ms", &config.auto_commit_interval_ms)
        .set("queued.min.messages", &config.queued_min_messages)
        .set("fetch.wait.max.ms", &config.fetch_wait_max_ms)
        .set("fetch.message.max.bytes", &config.fetch_message_max_bytes)
        .set("fetch.min.bytes", &config.fetch_min_bytes)
        .set("enable.auto.commit", "true")
        .set("security.protocol", &config.security_protocol)
        .set("sasl.mechanism", &config.sasl_mechanism)
        .set("sasl.username", &config.sasl_username)
        .set("sasl.password", &config.sasl_password)
        .set("statistics.interval.ms", &config.statistics_interval_ms)
        .set_log_level((&config.kafka_log_level).into())
        .create_with_context(ctx_stats)
        .expect("Consumer creation failed");

    consumer.subscribe(&[&topic]).unwrap_or_else(|e| {
        panic!("Couldn't subscribe to specified topic with {type_name}, error: {e}")
    });

    let mut shutdown_stream = signal(SignalKind::terminate()).unwrap();

    info!("The consumer loop for {type_name} is about to start: {:?}", consumer.position().unwrap());

    loop {
        let msg_result: Result<_, _> = select! {
            _ = shutdown_stream.recv() => break,
            msg_result = consumer.recv() => msg_result,
        };

        match msg_result {
            Ok(message) => {
                process_message(
                    Arc::clone(&filter_config),
                    message,
                    filter_tx.clone(),
                    Arc::clone(&stats),
                )
                .await;
            }
            Err(e) => {
                stats.kafka_errors_consumer.inc();
                error!("Kafka consumer error: {}", e);
            }
        }
    }
}
