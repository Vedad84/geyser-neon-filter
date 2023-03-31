use std::time::Duration;

use hyper::client::HttpConnector;

use crate::executor::Config;

pub struct ClickHouse;

fn build_client() -> hyper::Client<HttpConnector> {
    let mut http_connector = HttpConnector::new();
    http_connector.set_keepalive(Some(Duration::from_secs(60)));
    http_connector.set_nodelay(true);
    http_connector.set_connect_timeout(Some(Duration::from_secs(5)));

    // Create a Client builder
    let client: hyper::client::Client<_, hyper::Body> = hyper::Client::builder()
        .http2_only(true)
        .retry_canceled_requests(false)
        .pool_idle_timeout(Some(Duration::from_secs(5)))
        .pool_max_idle_per_host(5)
        .build(http_connector);

    client
}

impl ClickHouse {
    pub fn from_config(config: &Config) -> Vec<clickhouse::Client> {
        let mut servers: Vec<clickhouse::Client> = vec![];

        for i in config.servers.iter() {
            let username = config.username.clone();
            let password = config.password.clone();
            let http_client = build_client();

            let ch_client = match (config.username.is_empty(), config.password.is_empty()) {
                (false, false) => clickhouse::Client::with_http_client(http_client).with_url(i),
                (true, false) => clickhouse::Client::with_http_client(http_client)
                    .with_url(i)
                    .with_user(username),
                (true, true) => clickhouse::Client::with_http_client(http_client)
                    .with_url(i)
                    .with_user(username)
                    .with_password(password),
                (false, true) => clickhouse::Client::with_http_client(http_client).with_url(i),
            };
            servers.push(ch_client);
        }
        servers
    }
}
