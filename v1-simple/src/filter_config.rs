use std::path::Path;

use ahash::AHashSet;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::fs;

pub async fn read_filter_config(filer_path: &Path) -> Result<FilterConfig> {
    let filter_config = fs::read_to_string(filer_path).await?;
    Ok(serde_json::from_str(&filter_config)?)
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct FilterConfig {
    // Filter by account owners in base58
    pub filter_include_owners: AHashSet<String>,
    // Always include list for filter ( public keys from 32 to 44 characters in base58 )
    pub filter_include_pubkeys: AHashSet<String>,
}
