use std::time::Duration;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct HTTPSettings {
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub keepalive: Duration,
    #[serde(with = "humantime_serde")]
    pub pool_idle_timeout: Duration,
    pub no_delay: bool,
    pub reuse_address: bool,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub http_settings: HTTPSettings,
    pub log_path: String,
    pub servers: Vec<String>,
    pub username: String,
    pub password: String,
    pub tasks: TaskList,
}

#[derive(Debug, Deserialize)]
pub struct Task {
    pub task_name: String,
    pub queries: Vec<String>,
    #[serde(with = "humantime_serde")]
    pub task_interval: Duration,
}

pub type TaskList = Vec<Task>;
