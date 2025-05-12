use bytes::Bytes;
use crate::config::config::Config;
use crate::protocol::tlv::{TlvField, TlvFieldTypes, serialize_message, read_message};
use crate::cache_handler::ram_handler::RamStore;
use crate::cache_handler::storage_handler::{handle_event, StorageError};
use crate::replication_layer::Replication;
use log::{error, info, warn};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};

pub async fn start_server(config: Config, store: Arc<RamStore>, sync_manager: Option<Arc<Replication>>) {
    info!("[start_server] Starte Server im Modus: {:?}", config.socket.mode);
    match config.socket.mode.as_str() {
        "unix" => {
            if let Some(path) = &config.socket.path {
                let _ = std::fs::remove_file(path);
                info!("[start_server] Binde Unix-Socket an Pfad: {}", path);
                let listener = UnixListener::bind(path).expect("UnixSocket konnte nicht gebunden werden");
                info!("[start_server] Lausche auf Unix-Socket: {}", path);
                loop {
                    let (stream, addr) = listener.accept().await.expect("Fehler beim Annehmen");
                    info!("[start_server] Neue Verbindung von: {:?}", addr);
                    let store = store.clone();
                    let sync_manager = sync_manager.clone();
                    tokio::spawn(async move {
                        handle_unix_client(stream, store, sync_manager).await;
                    });
                }
            }
        }
        "tcp" => {
            if let Some(addr) = &config.socket.addr {
                info!("[start_server] Binde TCP an Adresse: {}", addr);
                let listener = TcpListener::bind(addr).await.expect("TCP konnte nicht gestartet werden");
                info!("[start_server] Lausche auf TCP-Adresse: {}", addr);
                loop {
                    let (stream, addr) = listener.accept().await.expect("Fehler beim Annehmen");
                    info!("[start_server] Neue Verbindung von: {:?}", addr);
                    let store = store.clone();
                    let sync_manager = sync_manager.clone();
                    tokio::spawn(async move {
                        handle_tcp_client(stream, store, sync_manager).await;
                    });
                }
            }
        }
        _ => {
            error!("[start_server] Unbekannter Socket-Modus: {}", config.socket.mode);
            panic!("Unbekannter Socket-Modus: {}", config.socket.mode)
        },
    }
}

async fn handle_unix_client(stream: UnixStream, store: Arc<RamStore>, sync_manager: Option<Arc<Replication>>) {
    info!("[handle_unix_client] Starte Handler für Unix-Client");
    handle_client(stream, store, sync_manager).await;
}

async fn handle_tcp_client(stream: TcpStream, store: Arc<RamStore>, sync_manager: Option<Arc<Replication>>) {
    info!("[handle_tcp_client] Starte Handler für TCP-Client");
    stream.set_nodelay(true).expect("Setze TCP_NODELAY fehlgeschlagen");
    handle_client(stream, store, sync_manager).await;
}

async fn handle_client<S>(mut stream: S, store: Arc<RamStore>, sync_manager: Option<Arc<Replication>>)
where
    S: AsyncReadExt + AsyncWriteExt + Unpin + Send + 'static,
{
    info!("[handle_client] Starte Client-Handler");
    loop {
        let msg = match read_message(&mut stream).await {
            Ok((event, fields)) => (event, fields),
            Err(e) => {
                error!("[handle_client] Fehler beim Lesen der Nachricht: {}", e);
                break;
            }
        };
        let (event, fields) = msg;

        if let Some(sync_manager) = &sync_manager {
            info!("[handle_client] Leite Nachricht an SyncManager weiter");
            if let Err(e) = sync_manager.sync_message(event, &fields).await {
                warn!("[handle_client] Sync-Weiterleitung fehlgeschlagen: {}", e);
            }
        }

        let response = match handle_event(event, fields.clone(), store.clone()).await {
            Ok(tlvs) => ResponseBuilder::ok(Some(tlvs)),
            Err(err) => ResponseBuilder::error(err, &fields),
        };
        if let Err(e) = stream.write_all(&response).await {
            error!("[handle_client] Fehler beim Senden der Antwort: {}", e);
            break;
        }
    }
    info!("[handle_client] Client-Handler beendet.");
}

struct ResponseBuilder;

impl ResponseBuilder {
    pub fn ok(tlvs: Option<Vec<TlvField>>) -> Vec<u8> {
        // Nutze serialize_message für Antwort
        let tlvs = tlvs.unwrap_or_default();
        serialize_message(0x01, &tlvs).to_vec()
    }

    pub fn error(err: StorageError, req_tlvs: &[TlvField]) -> Vec<u8> {
        let (status, text) = match err {
            StorageError::NotFound => (0x02, "Not Found"),
            StorageError::InvalidInput => (0x03, "Invalid Input"),
            StorageError::AlreadyExists => (0x04, "Already Exists"),
            StorageError::InternalError => (0x05, "Internal Error"),
        };

        warn!("[ResponseBuilder::error] Baue Fehler-Antwort: Status=0x{:02X}, Text='{}'", status, text);

        let mut tlvs = Vec::new();
        // MESSAGE_ID aus Request-TLVs extrahieren
        if let Some(msg_id) = req_tlvs.iter().find(|tlv| tlv.type_id == TlvFieldTypes::MessageId) {
            tlvs.push(TlvField::new(TlvFieldTypes::MessageId, msg_id.value.clone()));
        }
        // ERROR-TLV anhängen
        tlvs.push(TlvField::new(TlvFieldTypes::Error, Bytes::from_static(text.as_bytes())));
        serialize_message(status, &tlvs).to_vec()
    }
}