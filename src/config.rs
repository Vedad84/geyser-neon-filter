use log::LevelFilter;
use rdkafka::config::RDKafkaLogLevel;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LogLevel {
    /// Higher priority then [`Level::Error`](log::Level::Error) from the log
    /// crate.
    Emerg = 0,
    /// Higher priority then [`Level::Error`](log::Level::Error) from the log
    /// crate.
    Alert = 1,
    /// Higher priority then [`Level::Error`](log::Level::Error) from the log
    /// crate.
    Critical = 2,
    /// Equivalent to [`Level::Error`](log::Level::Error) from the log crate.
    Error = 3,
    /// Equivalent to [`Level::Warn`](log::Level::Warn) from the log crate.
    Warning = 4,
    /// Higher priority then [`Level::Info`](log::Level::Info) from the log
    /// crate.
    Notice = 5,
    /// Equivalent to [`Level::Info`](log::Level::Info) from the log crate.
    Info = 6,
    /// Equivalent to [`Level::Debug`](log::Level::Debug) from the log crate.
    Debug = 7,
}

impl From<&LogLevel> for RDKafkaLogLevel {
    fn from(log_level: &LogLevel) -> Self {
        match log_level {
            LogLevel::Emerg => RDKafkaLogLevel::Emerg,
            LogLevel::Alert => RDKafkaLogLevel::Alert,
            LogLevel::Critical => RDKafkaLogLevel::Critical,
            LogLevel::Error => RDKafkaLogLevel::Error,
            LogLevel::Warning => RDKafkaLogLevel::Warning,
            LogLevel::Notice => RDKafkaLogLevel::Notice,
            LogLevel::Info => RDKafkaLogLevel::Info,
            LogLevel::Debug => RDKafkaLogLevel::Debug,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GlobalLogLevel {
    /// A level lower than all log levels.
    Off,
    /// Corresponds to the `Error` log level.
    Error,
    /// Corresponds to the `Warn` log level.
    Warn,
    /// Corresponds to the `Info` log level.
    Info,
    /// Corresponds to the `Debug` log level.
    Debug,
    /// Corresponds to the `Trace` log level.
    Trace,
}

impl From<&GlobalLogLevel> for LevelFilter {
    fn from(log_level: &GlobalLogLevel) -> Self {
        match log_level {
            GlobalLogLevel::Off => LevelFilter::Off,
            GlobalLogLevel::Error => LevelFilter::Error,
            GlobalLogLevel::Warn => LevelFilter::Warn,
            GlobalLogLevel::Info => LevelFilter::Info,
            GlobalLogLevel::Debug => LevelFilter::Debug,
            GlobalLogLevel::Trace => LevelFilter::Trace,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilterConfig {
    pub bootstrap_servers: String,
    pub postgres_connection_str: String,
    pub update_account_topic: String,
    pub session_timeout_ms: String,
    // Filter by account owners in base58
    pub filter_include_owners: Vec<String>,
    // Exception list for filter ( public keys from 32 to 44 characters in base58 )
    pub filter_exceptions: Vec<String>,
    pub rdkafka_log_level: LogLevel,
    pub global_log_level: GlobalLogLevel,
}