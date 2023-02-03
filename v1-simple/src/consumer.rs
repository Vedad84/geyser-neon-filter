use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use flume::Sender;
use kafka_common::message_type::{GetMessageType, MessageType};
use log::{error, info};
use prometheus_client::metrics::counter::Counter;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::BorrowedMessage,
    ClientConfig, Message,
};
use serde::Deserialize;

use crate::{
    config::Config,
    consumer_stats::{ContextWithStats, Stats},
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

pub fn get_counter(stats: &Arc<Stats>, message_type: MessageType) -> &Counter<u64, AtomicU64> {
    match message_type {
        MessageType::UpdateAccount => &stats.kafka_update_account,
        MessageType::UpdateSlot => &stats.kafka_update_slot,
        MessageType::NotifyTransaction => &stats.kafka_notify_transaction,
        MessageType::NotifyBlock => &stats.kafka_notify_block,
    }
}

pub async fn process_message<T>(
    message: BorrowedMessage<'_>,
    filter_tx: Sender<T>,
    stats: Arc<Stats>,
) where
    T: for<'a> Deserialize<'a> + std::marker::Send + 'static + GetMessageType,
{
    if let Some(payload) = extract_from_message(&message) {
        let type_name = std::any::type_name::<T>();
        stats
            .kafka_bytes_rx
            .inner()
            .fetch_add(payload.len() as u64, Ordering::Relaxed);

        let result: serde_json::Result<T> = serde_json::from_str::<T>(payload);

        tokio::spawn(async move {
            match result {
                Ok(event) => {
                    let received = get_counter(&stats, event.get_type());
                    if let Err(e) = filter_tx.send_async(event).await {
                        error!("Failed to send the data {type_name}, error {e}");
                    }
                    received.inc();
                }
                Err(e) => {
                    error!("Failed to deserialize {type_name} {e}");
                    stats.kafka_errors_deserialize.inc();
                }
            }
        });
    } else {
        error!("Extracted empty payload!");
    }
}

pub async fn consumer<T>(
    config: Arc<Config>,
    topic: String,
    filter_tx: Sender<T>,
    ctx_stats: ContextWithStats,
) where
    T: for<'a> Deserialize<'a> + std::marker::Send + 'static + GetMessageType,
{
    let type_name = std::any::type_name::<T>();
    let stats = ctx_stats.stats.clone();
    let consumer: StreamConsumer<ContextWithStats> = ClientConfig::new()
        .set("group.id", &config.kafka_consumer_group_id)
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", &config.session_timeout_ms)
        .set("fetch.message.max.bytes", &config.fetch_message_max_bytes)
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

    info!("The consumer loop for {type_name} is about to start!");

    loop {
        match consumer.recv().await {
            Ok(message) => {
                process_message(message, filter_tx.clone(), stats.clone()).await;
            }
            Err(e) => {
                stats.kafka_errors_consumer.inc();
                error!("Kafka consumer error: {}", e);
            }
        };
    }
}
