## Geyser neon filter V1
This service filters incoming data from Kafka cluster by account owner and puts the result in the Postgres database. The data in the Kafka cluster is generated by [our Solana geyser plugin](https://github.com/neonlabsorg/geyser-neon-plugin).

### Configuration File Format
By default, the service is configured using a configuration file named filter_config.json.
You can change the path to the config file by command line option **--config** or **-c**
\
An example configuration file looks like the following:
```
{
    "filter_log_path" : "/var/log/neon/filter.log",
    "bootstrap_servers": "167.235.75.213:9092,159.69.197.26:9092,167.235.151.85:9092",
    "postgres_connection_str": "postgresql://username:password@1.1.1.1:3333/neon-db",
    "sasl_username": "username",
    "sasl_password": "password",
    "sasl_mechanism": "SCRAM-SHA-512",
    "security_protocol": "SASL_SSL",
    "kafka_consumer_group_id": "filter",
    "update_account_topic": "update_account",
    "notify_block_topic": "notify_block",
    "update_slot_topic": "update_slot",
    "session_timeout_ms": "45000",
    "filter_include_owners" : ["base58_string","base58_string"],
    "filter_include_pubkeys" : ["base58_string","base58_string"],
    "kafka_log_level": "Info",
    "global_log_level": "Info"
}
```

The filter can also be configured through environment variables:
```
FILTER_LOG_PATH="/var/log/neon/filter.log"
BOOTSTRAP_SERVERS="167.235.75.213:9092,159.69.197.26:9092,167.235.151.85:9092"
KAFKA_CONSUMER_GROUP_ID="filter"
POSTGRES_CONNECTION_STR="postgresql://username:password@1.1.1.1:3333/neon-db"
SASL_USERNAME="username"
SASL_PASSWORD="password"
SASL_MECHANISM="SCRAM-SHA-512"
SECURITY_PROTOCOL="SASL_SSL"
UPDATE_ACCOUNT_TOPIC="update_account"
NOTIFY_BLOCK_TOPIC="notify_block"
UPDATE_SLOT_TOPIC="update_slot"
SESSION_TIMEOUT_MS="45000"
FILTER_INCLUDE_OWNERS="owner_base58,owner2_base58"
FILTER_INCLUDE_PUBKEYS="pubkey_base58,pubkey2_base58"
KAFKA_LOG_LEVEL="Info"
GLOBAL_LOG_LEVEL="Info"
```

## Geyser neon filter V2 (Experimental)
The functionality is the same as in V1, but the service is based on Clickhouse's ability to act as a consumer of Kafka messages and the subsequent materialization of the data into tables. This solution allows storing large amounts of historical blockchain data in a compressed form.
