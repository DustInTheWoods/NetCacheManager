use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub socket: SocketConfig,
    pub storage: StorageConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SocketConfig {
    pub mode: String,         // "unix" oder "tcp"
    pub path: Option<String>, // Pfad für Unix-Socket
    pub addr: Option<String>, // Adresse für TCP
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageConfig {
    pub max_ram_size: usize,
    pub ttl_checktime: u64,
}

impl Config {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}
