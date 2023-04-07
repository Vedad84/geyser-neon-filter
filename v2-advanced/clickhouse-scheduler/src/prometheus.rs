use std::{
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::{Arc, atomic::AtomicU64},
};

use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};

use prometheus_client::{encoding::text::encode, registry::Registry, metrics::counter::Counter};
use tokio::sync::oneshot;
use log::info;



pub async fn start_prometheus(
    stat: Vec<Counter<u64, AtomicU64>>,
    task_names: Vec<String>,
    port: u16,
    shutdown_rx: oneshot::Receiver<()>,
) {
    let mut registry = <Registry>::default();

    task_names.iter()
        .zip(stat)
        .for_each(|(task, counter)| {
            registry.register(task.clone(), task, counter.clone());
        });

    let metrics_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
    start_metrics_server(metrics_addr, registry, shutdown_rx).await
}

async fn start_metrics_server(metrics_addr: SocketAddr, registry: Registry, rx_term: oneshot::Receiver<()>) {

    println!("Starting metrics server on {metrics_addr}");

    let registry = Arc::new(registry);
    Server::bind(&metrics_addr)
        .serve(make_service_fn(move |_conn| {
            let registry = registry.clone();
            async move {
                let handler = make_handler(registry);
                Ok::<_, io::Error>(service_fn(handler))
            }
        }))
        .with_graceful_shutdown(async move {
            rx_term.await.ok();
            info!("Shutdown prometheus task");
        })
        .await
        .expect("Failed to bind hyper server with graceful_shutdown");
}

fn make_handler(
    registry: Arc<Registry>,
) -> impl Fn(Request<Body>) -> Pin<Box<dyn Future<Output = io::Result<Response<Body>>> + Send>> {
    // This closure accepts a request and responds with the OpenMetrics encoding of our metrics.
    move |_req: Request<Body>| {
        let reg = registry.clone();
        Box::pin(async move {
            let mut buf = String::new();
            encode(&mut buf, &reg.clone())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                .map(|_| {
                    let body = Body::from(buf);
                    Response::builder()
                        .header(
                            hyper::header::CONTENT_TYPE,
                            "application/openmetrics-text; version=1.0.0; charset=utf-8",
                        )
                        .body(body)
                        .unwrap()
                })
        })
    }
}
