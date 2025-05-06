use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{mpsc};
use uuid::Uuid;
use log::{info, warn, error, debug};
use crate::config::config::EventRelayConfig;

const ERROR_FROM_UUID: [u8; 16] = [0xFF; 16];

fn build_error_msg(to_uuid: &Uuid, code: u32, text: &str) -> Vec<u8> {
    let text_bytes = text.as_bytes();
    let payload_len = 4 + text_bytes.len();
    let total_len = 16 + 16 + payload_len; // TO + FROM + PAYLOAD
    let mut msg = Vec::with_capacity(4 + total_len);
    msg.extend_from_slice(&(total_len as u32).to_be_bytes());
    msg.extend_from_slice(to_uuid.as_bytes());
    msg.extend_from_slice(&ERROR_FROM_UUID);
    msg.extend_from_slice(&code.to_be_bytes());
    msg.extend_from_slice(text_bytes);
    msg
}

/// Struktur für einen verbundenen Client
#[derive(Clone)]
struct Client {
    uuid: Uuid,
    sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
}

/// EventRelay-Server
pub struct EventRelay {
    clients: Arc<Mutex<HashMap<Uuid, Client>>>,
}

impl EventRelay {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Startet den EventRelay-Server
    pub async fn start_server(self: Arc<Self>, config: &EventRelayConfig) {
        match config.mode.as_str() {
            "tcp" => {
                if let Some(addr) = &config.addr {
                    let listener = TcpListener::bind(addr).await.expect("TCP konnte nicht gebunden werden");
                    info!("[EventRelay] Lausche auf TCP: {}", addr);
                    loop {
                        match listener.accept().await {
                            Ok((stream, _)) => {
                                let relay = self.clone();
                                tokio::spawn(async move {
                                    relay.handle_client_tcp(stream).await;
                                });
                            }
                            Err(e) => error!("[EventRelay] Fehler beim Annehmen: {}", e),
                        }
                    }
                } else {
                    error!("[EventRelay] TCP-Modus, aber keine Adresse konfiguriert!");
                }
            }
            "unix" => {
                if let Some(path) = &config.path {
                    match std::fs::remove_file(path) {
                        Ok(_) => info!("[EventRelay] Entferne altes Socket-File: {}", path),
                        Err(e) => debug!("[EventRelay] Entfernen des Socket-Files fehlgeschlagen (kann ignoriert werden): {}", e),
                    }
                    let listener = UnixListener::bind(path).expect("UnixSocket konnte nicht gebunden werden");
                    info!("[EventRelay] Lausche auf Unix-Socket: {}", path);
                    loop {
                        match listener.accept().await {
                            Ok((stream, _)) => {
                                let relay = self.clone();
                                tokio::spawn(async move {
                                    relay.handle_client_unix(stream).await;
                                });
                            }
                            Err(e) => error!("[EventRelay] Fehler beim Annehmen: {}", e),
                        }
                    }
                } else {
                    error!("[EventRelay] Unix-Modus, aber kein Pfad konfiguriert!");
                }
            }
            _ => error!("[EventRelay] Unbekannter Modus: {}", config.mode),
        }
    }

    async fn handle_client<S>(self: Arc<Self>, mut read: ReadHalf<S>, mut write: WriteHalf<S>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        // 1. UUID lesen (16 Byte)
        let mut uuid_buf = [0u8; 16];
        let n = match read.read(&mut uuid_buf).await {
            Ok(n) => n,
            Err(e) => {
                error!("[EventRelay] Fehler beim Lesen der UUID: {}", e);
                let err_msg = build_error_msg(&Uuid::nil(), 0x00000001, "Ungültige UUID (Lesefehler)");
                let _ = write.write_all(&err_msg).await;
                let _ = write.flush().await;
                return;
            }
        };
        if n < 16 {
            error!("[EventRelay] UUID zu kurz: nur {} Bytes empfangen", n);
            let err_msg = build_error_msg(&Uuid::nil(), 0x00000001, "Ungültige UUID (zu kurz)");
            let _ = write.write_all(&err_msg).await;
            let _ = write.flush().await;
            return;
        }
        let uuid = match Uuid::from_slice(&uuid_buf) {
            Ok(u) => u,
            Err(_) => {
                error!("[EventRelay] Ungültige UUID erhalten");
                let err_msg = build_error_msg(&Uuid::nil(), 0x00000001, "Ungültige UUID");
                let _ = write.write_all(&err_msg).await;
                let _ = write.flush().await;
                return;
            }
        };
        // 2. Prüfen ob UUID schon verbunden
        let already_connected = {
            let clients = self.clients.lock().unwrap();
            clients.contains_key(&uuid)
        };
        if already_connected {
            warn!("[EventRelay] UUID bereits verbunden: {}", uuid);
            let err_msg = build_error_msg(&uuid, 0x00000002, "UUID bereits verbunden");
            let _ = write.write_all(&err_msg).await;
            let _ = write.flush().await;
            return;
        }
        // 3. Channel für ausgehende Nachrichten
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        {
            let mut clients = self.clients.lock().unwrap();
            clients.insert(uuid, Client { uuid, sender: tx.clone() });
        }
        info!("[EventRelay] Client verbunden: {}", uuid);
        let uuid_for_send = uuid;
        // 4. Task für Senden
        tokio::spawn(async move {
            let mut write = write;
            while let Some(msg) = rx.recv().await {
                info!("[EventRelay] Sende Nachricht an {} ({} Bytes)", uuid_for_send, msg.len());
                if let Err(e) = write.write_all(&msg).await {
                    error!("[EventRelay] Fehler beim Senden an {}: {}", uuid_for_send, e);
                    break;
                }
            }
        });
        // 5. Nachrichten empfangen und weiterleiten
        loop {
            let mut len_buf = [0u8; 4];
            if let Err(e) = read.read_exact(&mut len_buf).await {
                error!("[EventRelay] Fehler beim Lesen der Länge: {}", e);
                break;
            }
            let msg_len = u32::from_be_bytes(len_buf) as usize;
            let mut msg_buf = vec![0u8; msg_len];
            if let Err(e) = read.read_exact(&mut msg_buf).await {
                error!("[EventRelay] Fehler beim Lesen der Nachricht: {}", e);
                break;
            }
            if msg_len < 32 {
                warn!("[EventRelay] Nachricht zu kurz");
                let err_msg = build_error_msg(&uuid, 0x00000003, "Nachricht zu kurz");
                if tx.send(err_msg).is_err() {
                    error!("[EventRelay] Fehler beim Senden der Fehlermeldung an {}", uuid);
                }
                continue;
            }
            let to_uuid = Uuid::from_slice(&msg_buf[0..16]).unwrap_or(Uuid::nil());
            let from_uuid = Uuid::from_slice(&msg_buf[16..32]).unwrap_or(Uuid::nil());
            let payload = &msg_buf[32..];
            info!("[EventRelay] Empfange Nachricht von {} an {} ({} Bytes)", from_uuid, to_uuid, payload.len());
            if to_uuid.is_nil() {
                // Broadcast
                info!("[EventRelay] Broadcast von {} an alle", from_uuid);
                let client_list = {
                    let clients = self.clients.lock().unwrap();
                    clients.iter()
                        .filter(|(other_uuid, _)| **other_uuid != uuid)
                        .map(|(other_uuid, client)| {
                            info!("[EventRelay] Sende Broadcast an {}", other_uuid);
                            client.sender.clone()
                        })
                        .collect::<Vec<_>>()
                };
                for sender in client_list {
                    let mut out = Vec::with_capacity(4 + msg_len);
                    out.extend_from_slice(&len_buf);
                    out.extend_from_slice(&msg_buf);
                    if sender.send(out).is_err() {
                        error!("[EventRelay] Fehler beim Senden des Broadcasts");
                    }
                }
            } else {
                let found_sender = {
                    let clients = self.clients.lock().unwrap();
                    clients.get(&to_uuid).map(|c| c.sender.clone())
                };
                if let Some(sender) = found_sender {
                    info!("[EventRelay] Sende Direktnachricht an {}", to_uuid);
                    let mut out = Vec::with_capacity(4 + msg_len);
                    out.extend_from_slice(&len_buf);
                    out.extend_from_slice(&msg_buf);
                    if sender.send(out).is_err() {
                        error!("[EventRelay] Fehler beim Senden der Direktnachricht an {}", to_uuid);
                    }
                } else {
                    warn!("[EventRelay] Ziel-UUID nicht verbunden: {}", to_uuid);
                    let err_msg = build_error_msg(&uuid, 0x00000005, "Ziel-UUID nicht verbunden");
                    if tx.send(err_msg).is_err() {
                        error!("[EventRelay] Fehler beim Senden der Fehlermeldung an {}", uuid);
                    }
                }
            }
        }
        {
            let mut clients = self.clients.lock().unwrap();
            clients.remove(&uuid);
        }
        info!("[EventRelay] Client getrennt: {}", uuid);
    }

    /// Handler für einen neuen TCP-Client
    async fn handle_client_tcp(self: Arc<Self>, stream: TcpStream) {
        let (read, write) = tokio::io::split(stream);
        self.handle_client(read, write).await;
    }

    /// Handler für einen neuen Unix-Client
    async fn handle_client_unix(self: Arc<Self>, stream: UnixStream) {
        let (read, write) = tokio::io::split(stream);
        self.handle_client(read, write).await;
    }
}
