use hyper::client::HttpConnector;

use crate::config::Config;

pub struct ClickHouse;

fn build_client(config: &Config) -> hyper::Client<HttpConnector> {
    let mut http_connector = HttpConnector::new();
    http_connector.set_keepalive(Some(config.http_settings.keepalive));
    http_connector.set_nodelay(config.http_settings.no_delay);
    http_connector.set_reuse_address(config.http_settings.reuse_address);
    http_connector.set_connect_timeout(Some(config.http_settings.connect_timeout));

    // Create a Client builder
    let client: hyper::client::Client<_, hyper::Body> = hyper::Client::builder()
        .retry_canceled_requests(true)
        .pool_idle_timeout(Some(config.http_settings.pool_idle_timeout))
        .build(http_connector);

    client
}

impl ClickHouse {
    pub fn from_config(config: &Config) -> Vec<clickhouse::Client> {
        let mut servers: Vec<clickhouse::Client> = vec![];

        for i in config.servers.iter() {
            let username = config.username.clone();
            let password = config.password.clone();
            let http_client = build_client(&config);

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
