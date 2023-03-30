use clickhouse::Client;

use crate::executor::Config;

pub struct ClickHouse;

impl ClickHouse {
    pub fn from_config(config: &Config) -> Client {
        let server = config.server.clone();
        let username = config.username.clone();
        let password = config.password.clone();

        match (config.username.is_empty(), config.password.is_empty()) {
            (false, false) => Client::default().with_url(server),
            (true, false) => Client::default().with_url(server).with_user(username),
            (true, true) => Client::default()
                .with_url(server)
                .with_user(username)
                .with_password(password),
            (false, true) => Client::default().with_url(server),
        }
    }
}
