use std::time::Duration;

use log::LevelFilter;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct HTTPSettings {
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub keepalive: Duration,
    #[serde(with = "humantime_serde")]
    pub pool_idle_timeout: Duration,
    pub no_delay: bool,
    pub reuse_address: bool,
    pub retries: u32,
    #[serde(with = "humantime_serde")]
    pub backoff: Duration,
}

pub type TaskList = Vec<Task>;

#[derive(Debug, Clone, Deserialize)]
pub enum LogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<LogLevel> for LevelFilter {
    fn from(log_level: LogLevel) -> Self {
        match log_level {
            LogLevel::Off => LevelFilter::Off,
            LogLevel::Error => LevelFilter::Error,
            LogLevel::Warn => LevelFilter::Warn,
            LogLevel::Info => LevelFilter::Info,
            LogLevel::Debug => LevelFilter::Debug,
            LogLevel::Trace => LevelFilter::Trace,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub http_settings: HTTPSettings,
    pub log_level: LogLevel,
    pub log_path: String,
    pub servers: Vec<String>,
    pub username: String,
    pub password: String,
    pub tasks: TaskList,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Task {
    pub task_name: String,
    pub queries: Vec<String>,
    #[serde(with = "humantime_serde")]
    pub task_interval: Duration,
}
