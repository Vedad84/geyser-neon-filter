use log::info;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch::Sender;

pub async fn sigterm_notifier(sender: Sender<()>) {
    let mut sigterm_stream =
        signal(SignalKind::terminate()).expect("Failed to create SIGTERM signal stream");

    sigterm_stream.recv().await;

    info!("SIGTERM received");

    sender
        .send(())
        .unwrap_or_else(|err| panic!("Error sending SIGTERM broadcast to services: {err}"));
}
