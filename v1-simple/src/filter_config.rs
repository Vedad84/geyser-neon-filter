use ahash::AHashSet;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use tokio::fs;

pub async fn _read_filter_config(filer_path: &str) -> Result<FilterConfig> {
    let filter_config = fs::read_to_string(filer_path).await?;
    Ok(from_str(&filter_config)?)
}

#[derive(Default, Serialize, Deserialize)]
pub struct FilterConfig {
    // Filter by account owners in base58
    pub filter_include_owners: AHashSet<String>,
    // Alway include list for filter ( public keys from 32 to 44 characters in base58 )
    pub filter_include_pubkeys: AHashSet<String>,
}
