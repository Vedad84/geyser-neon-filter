mod config;
mod client;

use {
    client::Connection,
    fast_log::{
        consts::LogSize,
        plugin::{file_split::RollingType, packer::LogPacker},
        Config, Logger,
    },
    log::{error, info, LevelFilter, Log},
};

#[tokio::main]
async fn main() {

    let config = config::load();
    let connection = Connection::new(&config);

    let logger: &'static Logger = fast_log::init(Config::new().console().file_split(
        &config.log_file_name,
        LogSize::KB(512),
        RollingType::All,
        LogPacker {},
    ))
        .expect("Failed to initialize fast_log");
    logger.set_level(LevelFilter::Info);

    info!("Start copying to older_account");
    match connection.insert_to_older_account().await {
        Ok(()) => info!("Copying completed successfully"),
        Err(e) => {
            error!("error: {:?}", e);
            return;
        }
    }

    info!("Start to drop partition");
    match connection.drop_partition().await {
        Ok(()) => info!("Partition has been dropped"),
        Err(e) => {
            error!("error: {:?}", e);
            return;
        }
    }

    logger.flush();
}