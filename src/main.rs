//! NetCacheManager - Hauptmodul

mod config;
mod protocol;
mod cache_handler;
mod eventrelay;
mod replication_layer;

use config::config::Config;
use cache_handler::socket_handler::start_server;
use eventrelay::eventrelay::EventRelay;
use log::{debug, info};
use cache_handler::ram_handler::RamStore;
use replication_layer::Replication;
use std::{env, sync::Arc};

#[tokio::main]
async fn main() {
    env_logger::init();
    println!("Starte NetCacheManager...");

    // 1. Konfiguration laden
    let args: Vec<String> = env::args().collect();
    let config_path = args.get(1).map(|s| s.as_str()).unwrap_or("config.toml");
    let config = Config::load(config_path)
        .unwrap_or_else(|_| panic!("Fehler beim Laden der Konfiguration aus: {}", config_path));
    debug!("Config geladen: {:?}", config);

    // 2. RAM-Store erstellen
    let store = Arc::new(RamStore::new(config.storage.clone())); 
    // (Optional) TTL-Cleaner starten
    store.clone().start_ttl_cleaner();

    // 3. EventRelay parallel starten
    let eventrelay = Arc::new(EventRelay::new());
    let eventrelay_config = config.eventrelay.clone();
    tokio::spawn({
        let eventrelay = eventrelay.clone();
        async move {
            eventrelay.start_server(&eventrelay_config).await;
        }
    });

    // 4. Replication starten, falls aktiviert
    let mut sync_manager = None;
    if let Some(sync_cfg) = &config.sync {
        if sync_cfg.enabled {
            let manager = Replication::new(
                sync_cfg.server_addr.clone(),
                sync_cfg.peers.clone(),
                sync_cfg.sync_interval,
                sync_cfg.sync_timeout.unwrap_or(sync_cfg.sync_interval * 3),
                store.clone(),
            );
            tokio::spawn(manager.clone().start());
            sync_manager = Some(manager);
        }
    }

    // 5. Server starten mit Store
    info!("Starte NetCacheManager im {:?}-Modus", config.socket.mode);
    start_server(config, store, sync_manager.clone()).await;
}