use std::{env, io, fs::File, path::Path};
use clap::{crate_description, crate_name, App, Arg};

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
pub struct Config {
    pub clickhouse_url: Vec<String>,
    pub clickhouse_user: Option<String>,
    pub clickhouse_password: Option<String>,
    pub copy_partition_offset: u64,
    pub drop_partition_offset: u64,
    pub log_file_name: String,
    pub slot_range_to_task: u64,
    pub tasks_at_same_time: u64,
}

pub fn load() -> Config {
    let options = App::new(crate_name!())
        .about(crate_description!())
        .version(concat!("ClickhouseRetention/v", env!("CARGO_PKG_VERSION")))
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .takes_value(true)
                .global(true)
                .help("configuration file")
        )
        .get_matches();

    let config = options
        .value_of("config");

    let config = if let Some(config) = config {
        load_config_file(config).expect("load config file error")
    } else {
        load_config_env()
    };

    if config.drop_partition_offset <= config.copy_partition_offset {
        panic!("drop partition offset should be greater then copy partition offset");
    };

    config
}

fn load_config_file<T, P>(config_file: P) -> Result<T, io::Error>
    where
        T: serde::de::DeserializeOwned,
        P: AsRef<Path>,
{
    let file = File::open(config_file)?;
    let config = serde_yaml::from_reader(file)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("{:?}", err)))?;
    Ok(config)
}

fn load_config_env() -> Config {
    let clickhouse_url = env::var("NEON_DB_CLICKHOUSE_URLS")
        .map(|urls| {
            urls.split(';')
                .map(std::borrow::ToOwned::to_owned)
                .collect::<Vec<String>>()
        })
        .expect("neon clickhouse db urls valiable must be set");

    let clickhouse_user = env::var("NEON_DB_CLICKHOUSE_USER")
        .map(Some)
        .unwrap_or(None);

    let clickhouse_password = env::var("NEON_DB_CLICKHOUSE_PASSWORD")
        .map(Some)
        .unwrap_or(None);

    let copy_partition_offset: u64 =  env::var("COPY_PARTITION_OFFSET")
        .expect("COPY_PARTITION_OFFSET should be set").parse::<u64>()
        .expect("copy_partition_offset parse error");

    let drop_partition_offset: u64 =  env::var("DROP_PARTITION_OFFSET")
        .expect("DROP_PARTITION_OFFSET should be set").parse::<u64>()
        .expect("drop_partition_offset parse error");

    let log_file_name: String = env::var("LOG_FILE_NAME").expect("LOG_FILE_NAME should bu set");

    let slot_range_to_task = env::var("SLOT_RANGE_TO_TASK")
        .expect("SLOT_RANGE_TO_TASK should be set").parse::<u64>()
        .expect("slot_range_to_task parse error");

    let tasks_at_same_time = env::var("TASKS_AT_SAME_TIME")
        .expect("TASKS_AT_SAME_TIME should be set").parse::<u64>()
        .expect("tasks_at_same_time parse error");

    Config {
        clickhouse_url,
        clickhouse_user,
        clickhouse_password,
        copy_partition_offset,
        drop_partition_offset,
        log_file_name,
        slot_range_to_task,
        tasks_at_same_time,
    }
}

