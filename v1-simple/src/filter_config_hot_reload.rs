use crate::filter_config::read_filter_config;
use crate::{app_config::AppConfig, filter_config::FilterConfig};
use ahash::AHashSet;
use log::{error, info};
use notify::{
    event::{DataChange, ModifyKind},
    Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::collections::hash_set::{Difference, Intersection};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::{watch, RwLock};

struct HDiff<'a, T: 'a + Eq + Hash, S: std::hash::BuildHasher> {
    added: Difference<'a, T, S>,
    removed: Difference<'a, T, S>,
    unchanged: Intersection<'a, T, S>,
}

fn hashset_diff<'a, T, S>(
    set1: &'a AHashSet<T>,
    set2: &'a AHashSet<T>,
) -> HDiff<'a, T, ahash::RandomState>
where
    T: Eq + Hash,
{
    let added = set2.difference(set1);
    let removed = set1.difference(set2);
    let unchanged = set1.intersection(set2);

    HDiff {
        added,
        removed,
        unchanged,
    }
}

fn print_rows<T: Debug>(elements: impl IntoIterator<Item = T>) {
    let mut count = 0;
    let mut output = String::new();
    for item in elements {
        output.push_str(&format!("   {item:?}"));
        count += 1;
        if count % 4 == 0 {
            output.push('\n');
        }
    }

    if count % 4 != 0 {
        output.push('\n');
    }

    if count > 0 {
        info!("{}", output);
    } else {
        info!("None");
    }
}

fn log_diff<T, S>(hashset_name: &str, hashset_diff: HDiff<T, S>)
where
    T: Eq + Hash + Debug,
    S: std::hash::BuildHasher,
{
    info!("Diff for {hashset_name}");
    info!("[+] Elements added in the updated configuration:");
    print_rows(hashset_diff.added);

    info!("[-] Elements missing in the old configuration compared to the new one:");
    print_rows(hashset_diff.removed);

    info!("[=] Elements that remain the same in both configurations:");
    print_rows(hashset_diff.unchanged);
}

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

pub async fn async_watch(
    config: Arc<AppConfig>,
    filter_config: Arc<RwLock<FilterConfig>>,
    mut sigterm_rx: watch::Receiver<()>,
) -> notify::Result<()> {
    let (mut watcher, mut rx) = async_watcher().await?;

    watcher.watch(config.filter_config_path.as_ref(), RecursiveMode::Recursive)?;

    while let Some(res) = select! {
        _ = sigterm_rx.changed() => None,
        recv_result = rx.recv() => recv_result,
    } {
        match res {
            Ok(event) if event.kind == EventKind::Modify(ModifyKind::Data(DataChange::Any)) => {
                if let Ok(new_filter_config) =
                    read_filter_config(config.filter_config_path.as_ref()).await
                {
                    let read_guard = filter_config.read().await;
                    let include_owners_diff = hashset_diff::<std::string::String, ahash::RandomState>(
                        &read_guard.filter_include_owners,
                        &new_filter_config.filter_include_owners,
                    );
                    let include_pubkeys_diff =
                        hashset_diff::<std::string::String, ahash::RandomState>(
                            &read_guard.filter_include_pubkeys,
                            &new_filter_config.filter_include_pubkeys,
                        );

                    log_diff("Owners:", include_owners_diff);
                    log_diff("Pubkeys:", include_pubkeys_diff);

                    drop(read_guard);

                    *filter_config.write().await = new_filter_config;

                    info!("Filter config reloaded successfully");
                } else {
                    info!("Couldn't read filter config {}", config.filter_config_path);
                }
            }
            Ok(_) => {}
            Err(e) => error!("Filter config watch error: {e:?}"),
        }
    }

    info!("Config watcher has shut down");

    Ok(())
}
