use crate::config::AppConfig;
use crate::filter_config::read_filter_config;
use log::info;
use notify::{
    event::{DataChange, ModifyKind},
    Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver};

async fn async_watcher() -> notify::Result<(RecommendedWatcher, Receiver<notify::Result<Event>>)> {
    let (tx, rx) = channel(1);

    let watcher = RecommendedWatcher::new(
        move |res| {
            let _ = tx.blocking_send(res);
        },
        Config::default(),
    )?;

    Ok((watcher, rx))
}

pub async fn async_watch(config: Arc<AppConfig>) -> notify::Result<()> {
    let (mut watcher, mut rx) = async_watcher().await?;

    watcher.watch(config.filter_config_path.as_ref(), RecursiveMode::Recursive)?;

    while let Some(res) = rx.recv().await {
        match res {
            Ok(event) => {
                if event.kind == EventKind::Modify(ModifyKind::Data(DataChange::Any)) {
                    info!("Filter config changed, reloading...");
                    match read_filter_config(config.filter_config_path.as_ref()).await {
                        Ok(_new_filter_config) => {}
                        Err(e) => {
                            info!("Couldn't read filter config: {}", e);
                        }
                    }
                }
            }
            Err(e) => println!("watch error: {e:?}"),
        }
    }

    Ok(())
}
