use std::sync::{atomic::AtomicU64, Arc};

use log::info;
use prometheus_client::metrics::gauge::Atomic;
use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use rdkafka::{consumer::ConsumerContext, ClientContext, Statistics};

#[derive(Default)]
pub struct Stats {
    pub filtered_events: Counter<u64, AtomicU64>,
    pub kafka_update_account: Counter<u64, AtomicU64>,
    pub kafka_update_slot: Counter<u64, AtomicU64>,
    pub kafka_notify_transaction: Counter<u64, AtomicU64>,
    pub kafka_notify_block: Counter<u64, AtomicU64>,
    pub kafka_errors_consumer: Counter<u64, AtomicU64>,
    pub kafka_errors_deserialize: Counter<u64, AtomicU64>,
    pub kafka_bytes_rx: Counter<u64, AtomicU64>,
    pub queue_len_update_account: Gauge<f64, AtomicU64>,
    pub queue_len_update_slot: Gauge<f64, AtomicU64>,
    pub queue_len_notify_transaction: Gauge<f64, AtomicU64>,
    pub queue_len_notify_block: Gauge<f64, AtomicU64>,
    pub processing_tokio_tasks: Gauge<f64, AtomicU64>,
    pub db_errors: Counter<u64, AtomicU64>,
}

pub trait GetCounters {
    fn get_counters(&self) -> (&AtomicU64, &AtomicU64);
}

#[derive(Default, Clone)]
pub struct ContextWithStats {
    pub stats: Arc<Stats>,
}

impl ClientContext for ContextWithStats {
    fn stats(&self, stats: Statistics) {
        info!("{:?}", stats);
    }
}

impl ConsumerContext for ContextWithStats {}

pub struct RAIICounter<N, A: Atomic<N>> {
    gauge: Gauge<N, A>,
}

impl<N, A: Atomic<N>> RAIICounter<N, A> {
    pub fn new(gauge: &Gauge<N, A>) -> Self {
        gauge.inc();
        Self {
            gauge: gauge.clone(),
        }
    }
}

impl<N, A: Atomic<N>> Drop for RAIICounter<N, A> {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}
