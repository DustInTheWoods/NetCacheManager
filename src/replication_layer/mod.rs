use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use log::{info, warn, error, debug};
use bytes::BufMut;

use crate::protocol::tlv::{TlvField, serialize_message, read_message};
use crate::cache_handler::ram_handler::RamStore;
use crate::cache_handler::storage_handler;

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Master,
    Slave,
    Unknown,
}

pub struct Replication {
    pub role: Arc<Mutex<Role>>,
    pub self_addr: String,
    pub master_addr: Arc<Mutex<Option<String>>>,
    pub peers: Vec<String>,
    pub slaves: Arc<Mutex<Vec<Arc<Mutex<WriteHalf<TcpStream>>>>>>,
    pub master_write: Arc<Mutex<Option<Arc<Mutex<WriteHalf<TcpStream>>>>>>,
    pub master_read: Arc<Mutex<Option<Arc<Mutex<ReadHalf<TcpStream>>>>>>,
    pub sync_interval: u64,
    pub sync_timeout: u64,
    pub store: Arc<RamStore>,
}

impl Replication {
    pub fn new(self_addr: String, peers: Vec<String>, sync_interval: u64, sync_timeout: u64, store: Arc<RamStore>) -> Arc<Self> {
        Arc::new(Self {
            role: Arc::new(Mutex::new(Role::Unknown)),
            self_addr,
            master_addr: Arc::new(Mutex::new(None)),
            peers,
            slaves: Arc::new(Mutex::new(Vec::new())),
            master_write: Arc::new(Mutex::new(None)),
            master_read: Arc::new(Mutex::new(None)),
            sync_interval,
            sync_timeout,
            store,
        })
    }

    pub async fn start(self: Arc<Self>) {
        info!("[Replication] Starte Replication-Loop");
        let myself = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(myself.sync_interval)).await;
                let role = myself.role.lock().await.clone();
                debug!("[Replication] Aktuelle Rolle: {:?}", role);
                match role {
                    Role::Unknown => myself.check_role().await,
                    Role::Master => {},
                    Role::Slave => {},
                }
            }
        });
    }

    async fn check_role(self: &Arc<Self>) {
        info!("[Replication] Prüfe Rolle, Peers: {:?}", self.peers);
        let self_addr = self.self_addr.trim_start_matches("tcp://");
        let mut found_master = false;
        for (idx, peer) in self.peers.iter().enumerate() {
            let peer_addr = peer.trim_start_matches("tcp://");
            info!("[Replication] Prüfe Peer: {} (Index {})", peer_addr, idx);
            if TcpStream::connect(peer_addr).await.is_ok() {
                if peer_addr == self_addr {
                    info!("[Replication] Eigene Adresse erreichbar, aber prüfe weiter...");
                } else {
                    info!("[Replication] Peer {} ist erreichbar, werde Slave.", peer_addr);
                    {
                        let mut role = self.role.lock().await;
                        *role = Role::Slave;
                    }
                    let mut master_addr = self.master_addr.lock().await;
                    *master_addr = Some(peer_addr.to_string());
                    info!("[Replication] Verbinde als Slave zu Master: {}", peer_addr);
                    let myself = self.clone();
                    tokio::spawn(async move { myself.run_slave().await; });
                    found_master = true;
                    break;
                }
            } else {
                warn!("[Replication] Peer {} nicht erreichbar.", peer_addr);
            }
        }
        if !found_master {
            info!("[Replication] Kein Peer erreichbar, werde Master.");
            let mut role = self.role.lock().await;
            *role = Role::Master;
            let mut master_addr = self.master_addr.lock().await;
            *master_addr = Some(self.self_addr.clone());
            info!("[Replication] Starte als Master: {}", self.self_addr);
            let myself = self.clone();
            tokio::spawn(async move { myself.run_master().await; });
        }
    }

    async fn run_master(self: Arc<Self>) {
        let listener = TcpListener::bind(&self.self_addr).await.expect("[Replication] Master: Bind fehlgeschlagen");
        info!("[Replication] Master lauscht auf {}", self.self_addr);
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("[Replication] Slave verbunden: {}", addr);
                    let rep = self.clone();
                    let slaves = rep.slaves.clone();
                    let (mut read_half, write_half) = tokio::io::split(stream);
                    let write_half = Arc::new(Mutex::new(write_half));
                    {
                        let mut locked = slaves.lock().await;
                        locked.push(write_half.clone());
                    }
                    tokio::spawn(async move {
                        loop {
                            match read_message(&mut read_half).await {
                                Ok((event, tlvs)) => {
                                    if event == 0x85 {
                                        // SYNC_SNAPSHOT_REQUEST: Slave will kompletten Snapshot
                                        let entries = rep.store.all_entries();
                                        let mut snapshot_tlvs = Vec::new();
                                        for (key, entry) in entries {
                                            snapshot_tlvs.push(TlvField::new(crate::protocol::tlv::TlvFieldTypes::KEY, key));
                                            snapshot_tlvs.push(TlvField::new(crate::protocol::tlv::TlvFieldTypes::VALUE, entry.value));
                                        }
                                        let msg = serialize_message(0x83, &snapshot_tlvs);
                                        // Sende Snapshot nur an diesen Slave (nicht broadcast!)
                                        let _ = write_half.lock().await.write_all(&msg).await;
                                    } else if event < 0x80 {
                                        // 1. Lokal im Store ausführen
                                        let _ = storage_handler::handle_event(event, tlvs.clone(), rep.store.clone()).await;
                                        // 2. Replizieren: immer das passende SYNC_* Event an die Slaves!
                                        let _ = rep.handle_event(event, tlvs.clone()).await;
                                    } else {
                                        // SYNC_* Events nur broadcasten
                                        rep.broadcast_to_slaves(&serialize_message(event, &tlvs)).await;
                                    }
                                }
                                Err(e) => {
                                    error!("[Replication] Fehler beim Lesen von Nachricht von Slave {}: {}", addr, e);
                                    let mut locked = slaves.lock().await;
                                    locked.retain(|s| !Arc::ptr_eq(s, &write_half));
                                    break;
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    error!("[Replication] Fehler beim Annehmen: {}", e);
                }
            }
        }
    }

    async fn run_slave(self: Arc<Self>) {
        let mut failed_attempts = 0;
        loop {
            let master_addr = self.master_addr.lock().await.clone().expect("Master-Addr fehlt");
            info!("[Replication] Slave: Versuche Verbindung zu Master: {}", master_addr);
            match TcpStream::connect(&master_addr).await {
                Ok(stream) => {
                    info!("[Replication] Slave: Verbindung zu Master aufgebaut: {}", master_addr);
                    failed_attempts = 0;
                    let (mut read_half, write_half) = tokio::io::split(stream);
                    {
                        let mut w = self.master_write.lock().await;
                        *w = Some(Arc::new(Mutex::new(write_half)));
                    }
                    // Nach Verbindungsaufbau: Sende SYNC_SNAPSHOT_REQUEST (0x85)
                    let msg = serialize_message(0x85, &[]);
                    let _ = self.send_to_master(&msg).await;
                    let store = self.store.clone();
                    let event_task = tokio::spawn(async move {
                        debug!("[Replication] Slave: Starte Empfangsloop");
                        loop {
                            match read_message(&mut read_half).await {
                                Ok((event, tlvs)) => {
                                    debug!("[Replication] Slave: Empfange Event 0x{:02X} {:?}", event, tlvs);
                                    if event == 0x83 {
                                        // SYNC_SNAPSHOT: Kompletter Snapshot
                                        storage_handler::handle_sync_event(event, tlvs, &store).await;
                                    } else if event >= 0x80 && event <= 0x84 {
                                        storage_handler::handle_sync_event(event, tlvs, &store).await;
                                    }
                                }
                                Err(e) => {
                                    warn!("[Replication] Fehler beim Lesen der Nachricht: {}", e);
                                    break;
                                }
                            }
                        }
                    });
                    tokio::select! {
                        res = event_task => {
                            warn!("[Replication] Event-Task beendet: {:?}", res);
                        }
                    }
                    warn!("[Replication] Slave-Verbindung zu Master beendet, reconnect in 3s...");
                    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                }
                Err(e) => {
                    warn!("[Replication] Verbindung zu Master fehlgeschlagen: {}. Neuer Versuch in 3s...", e);
                    failed_attempts += 1;
                    if failed_attempts >= 2 {
                        warn!("[Replication] Nach 2 Fehlversuchen: Setze Rolle auf Unknown für Failover.");
                        let mut role = self.role.lock().await;
                        *role = Role::Unknown;
                        break;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                }
            }
        }
    }

    async fn send_to_master(&self, msg: &[u8]) -> std::io::Result<()> {
        let mut guard = self.master_write.lock().await;
        if let Some(ref write_half_arc) = *guard {
            let mut write_half = write_half_arc.lock().await;
            write_half.write_all(msg).await
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::NotConnected, "Keine Verbindung zum Master"))
        }
    }

    async fn broadcast_to_slaves(&self, msg: &[u8]) {
        let slaves = self.slaves.lock().await;
        debug!("[Replication] Aktuelle Slaves: {}", slaves.len());
        debug!("[Replication] Zu sendendes Paket ({} Bytes): {:?}", msg.len(), msg);
        for write_half in slaves.iter() {
            let write_half = write_half.clone();
            let packet = msg.to_vec();
            tokio::spawn(async move {
                use tokio::time::{timeout, Duration};
                debug!("[Replication] Tokio-Task für Broadcast gestartet");
                let lock_result = timeout(Duration::from_secs(2), write_half.lock()).await;
                match lock_result {
                    Ok(mut guard) => {
                        debug!("[Replication] WriteHalf gelockt");
                        // peer_addr ist auf WriteHalf nicht direkt verfügbar, daher ggf. Adresse im slaves-Array mitführen, falls benötigt
                        if let Err(e) = guard.write_all(&packet).await {
                            error!("[Replication] Fehler beim Broadcast an Slave: {}", e);
                        } else {
                            info!("[Replication] Broadcast an Slave erfolgreich");
                        }
                    }
                    Err(_) => {
                        error!("[Replication] Timeout beim Locken des Slave-WriteHalf!");
                    }
                }
            });
        }
    }

    pub async fn sync_message(&self, event: u8, tlvs: &[TlvField]) -> std::io::Result<()> {
        let msg = serialize_message(event, tlvs);
        let role = self.role.lock().await.clone();
        match role {
            Role::Master => {
                self.broadcast_to_slaves(&msg).await;
                Ok(())
            }
            Role::Slave => self.send_to_master(&msg).await,
            Role::Unknown => Err(std::io::Error::new(std::io::ErrorKind::Other, "Unknown role")),
        }
    }

    pub async fn handle_event(&self, event: u8, tlvs: Vec<TlvField>) {
        debug!("[Replication] Handle Event: 0x{:02X} {:?}", event, tlvs);
        match event {
            0x20 => {
                debug!("[Replication] Mapping Event 0x20 (SET) → 0x80 (SYNC_SET)");
                let _ = self.sync_message(0x80, &tlvs).await;
            }
            0x21 => {
                debug!("[Replication] Event 0x21 (GET): Keine Sync-Weiterleitung");
                let _ = self.sync_message(0x82, &tlvs).await;
            }
            0x22 => {
                debug!("[Replication] Mapping Event 0x22 (DELETE) → 0x81 (SYNC_DELETE)");
                let _ = self.sync_message(0x81, &tlvs).await;
            }
            0x23 => {
                debug!("[Replication] Mapping Event 0x23 (FLUSH) → 0x83 (SYNC_FLUSH)");
                let _ = self.sync_message(0x83, &tlvs).await;
            }
            0x24 => {
                debug!("[Replication] Mapping Event 0x24 (TOUCH) → 0x82 (SYNC_TOUCH)");
                let _ = self.sync_message(0x82, &tlvs).await;
            }
            _ => {
                warn!("[Replication] Unbekanntes oder nicht synchronisiertes Event: 0x{:02X}", event);
            }
        }
    }
}