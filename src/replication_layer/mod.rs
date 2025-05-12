use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use log::{info, warn, error, debug};

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
    pub slaves: Arc<Mutex<Vec<Arc<Mutex<TcpStream>>>>>,
    pub master_write: Arc<Mutex<Option<Arc<Mutex<WriteHalf<TcpStream>>>>>>,
    pub master_read: Arc<Mutex<Option<Arc<Mutex<ReadHalf<TcpStream>>>>>>,
    pub sync_interval: u64,
    pub sync_timeout: u64,
}

const HEARTBEAT: u8 = 0x85;

impl Replication {
    pub fn new(self_addr: String, peers: Vec<String>, sync_interval: u64, sync_timeout: u64) -> Arc<Self> {
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
        })
    }

    pub async fn start(self: Arc<Self>) {
        info!("[Replication] Starte Replication-Loop");
        let myself = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(myself.sync_interval)).await;
                let role = myself.role.lock().await.clone();
                info!("[Replication] Aktuelle Rolle: {:?}", role);
                match role {
                    Role::Unknown => myself.check_role().await,
                    Role::Master => myself.send_heartbeat().await,
                    Role::Slave => { /* Im Slave-Betrieb reicht Heartbeat/Verbindungsüberwachung im run_slave-Task */ }
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
                    // Nicht sofort Master werden, sondern weiter prüfen!
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

    pub async fn run_master(self: Arc<Self>) {
        let listener = TcpListener::bind(&self.self_addr).await.expect("[Replication] Master: Bind fehlgeschlagen");
        info!("[Replication] Master lauscht auf {}", self.self_addr);
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("[Replication] Slave verbunden: {}", addr);
                    let slaves = self.slaves.clone();
                    let stream = Arc::new(Mutex::new(stream));
                    {
                        let mut locked = slaves.lock().await;
                        locked.push(stream.clone());
                    }
                    tokio::spawn({
                        let slaves = self.slaves.clone();
                        let stream = stream.clone();
                        async move {
                            let mut buf = [0u8; 1024];
                            loop {
                                let mut guard = stream.lock().await;
                                match guard.read(&mut buf).await {
                                    Ok(0) => {
                                        info!("[Replication] Slave {} hat Verbindung geschlossen", addr);
                                        let mut locked = slaves.lock().await;
                                        locked.retain(|s| !Arc::ptr_eq(s, &stream));
                                        break;
                                    }
                                    Ok(n) => {
                                        if n == 1 && buf[0] == HEARTBEAT {
                                            debug!("[Replication] Heartbeat von Slave {addr}");
                                            // Verbindung offen lassen, einfach weiter im Loop bleiben
                                            continue;
                                        }
                                        info!("[Replication] Empfange {} Bytes von {}", n, addr);
                                        // ...weitere Sync-Logik...
                                    }
                                    Err(e) => {
                                        error!("[Replication] Fehler bei Slave {}: {}", addr, e);
                                        let mut locked = slaves.lock().await;
                                        locked.retain(|s| !Arc::ptr_eq(s, &stream));
                                        break;
                                    }
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

    pub async fn run_slave(self: Arc<Self>) {
        let mut failed_attempts = 0;
        loop {
            let master_addr = self.master_addr.lock().await.clone().expect("Master-Addr fehlt");
            info!("[Replication] Slave: Versuche Verbindung zu Master: {}", master_addr);
            match TcpStream::connect(&master_addr).await {
                Ok(stream) => {
                    info!("[Replication] Slave: Verbindung zu Master aufgebaut: {}", master_addr);
                    failed_attempts = 0; // Reset bei Erfolg
                    let (read_half, write_half) = tokio::io::split(stream);
                    let write_half = Arc::new(Mutex::new(write_half));
                    let read_half = Arc::new(Mutex::new(read_half));
                    {
                        let mut w = self.master_write.lock().await;
                        *w = Some(write_half.clone());
                    }
                    {
                        let mut r = self.master_read.lock().await;
                        *r = Some(read_half.clone());
                    }
                    // Heartbeat-Sender
                    let sync_interval = self.sync_interval;
                    let write_half_clone = write_half.clone();
                    let heartbeat_task = tokio::spawn(async move {
                        loop {
                            tokio::time::sleep(tokio::time::Duration::from_secs(sync_interval)).await;
                            let mut w = write_half_clone.lock().await;
                            if let Err(e) = w.write_all(&[HEARTBEAT]).await {
                                warn!("[Replication] Heartbeat an Master fehlgeschlagen: {}", e);
                                break;
                            }
                        }
                    });
                    // Heartbeat-Monitor
                    let sync_timeout = self.sync_timeout;
                    let read_half_clone = read_half.clone();
                    let monitor_task = tokio::spawn(async move {
                        use tokio::time::{timeout, Duration};
                        loop {
                            let mut buf = [0u8; 1];
                            let mut s = read_half_clone.lock().await;
                            match timeout(Duration::from_secs(sync_timeout), s.read_exact(&mut buf)).await {
                                Ok(Ok(_)) if buf[0] == HEARTBEAT => {
                                    debug!("[Replication] Heartbeat vom Master empfangen");
                                }
                                _ => {
                                    warn!("[Replication] Kein Heartbeat mehr vom Master, starte Failover!");
                                    return;
                                }
                            }
                        }
                    });
                    tokio::select! {
                        res = heartbeat_task => {
                            warn!("[Replication] Heartbeat-Task beendet: {:?}", res);
                        }
                        _ = monitor_task => {
                            warn!("[Replication] Heartbeat-Monitor beendet");
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

    pub async fn send_heartbeat(&self) {
        let slaves = self.slaves.lock().await;
        for stream in slaves.iter() {
            let stream = stream.clone();
            tokio::spawn(async move {
                let mut guard = stream.lock().await;
                let _ = guard.write_all(&[HEARTBEAT]).await;
            });
        }
    }

    pub async fn send_to_master(&self, data: &[u8]) -> std::io::Result<()> {
        let mut guard = self.master_write.lock().await;
        if let Some(ref write_half_arc) = *guard {
            let mut write_half = write_half_arc.lock().await;
            write_half.write_all(data).await
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::NotConnected, "Keine Verbindung zum Master"))
        }
    }

    pub async fn broadcast_to_slaves(&self, data: &[u8]) {
        let slaves = self.slaves.lock().await;
        for stream in slaves.iter() {
            let stream = stream.clone();
            let packet = data.to_vec();
            tokio::spawn(async move {
                let mut guard = stream.lock().await;
                if let Err(e) = guard.write_all(&packet).await {
                    warn!("[Replication] Fehler beim Broadcast an Slave: {}", e);
                }
            });
        }
    }

    /// Sende eine Nachricht je nach Rolle an Master oder als Broadcast an alle Slaves
    pub async fn sync_message(&self, data: &[u8]) -> std::io::Result<()> {
        let role = self.role.lock().await.clone();
        match role {
            Role::Master => {
                self.broadcast_to_slaves(data).await;
                Ok(())
            }
            Role::Slave => self.send_to_master(data).await,
            Role::Unknown => Err(std::io::Error::new(std::io::ErrorKind::Other, "Unknown role")),
        }
    }
}