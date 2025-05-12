use bincode::de;
use bytes::{BufMut, Bytes, BytesMut};
use crate::config::config::Config;
use crate::protocol::tlv::{encode_tlv_fields, parse_tlv_fields, TlvField, TlvFieldTypes};
use crate::cache_handler::ram_handler::RamStore;
use crate::cache_handler::storage_handler::{handle_event, StorageError};
use crate::replication_layer::Replication;
use log::{debug, error, info, warn};
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
    info!("[handle_client] Starte Client-Handler (Thread: {:?})", std::thread::current().id());
    loop {
        let mut len_buf = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut len_buf).await {
            error!("[handle_client] Fehler beim Lesen der Nachrichtenlänge: {}", e);
            break;
        }

        // Überprüfen, ob die Länge 0 ist (Verbindung geschlossen)
        if len_buf == [0, 0, 0, 0] {
            info!("[handle_client] Verbindung geschlossen (Länge 0)");
            break;
        }

        debug!("[handle_client] Gelesene Nachrichtenlänge: {:?}", len_buf);

        let msg_len = u32::from_be_bytes(len_buf) as usize;
        let mut msg_buf = vec![0u8; msg_len];
        if let Err(e) = stream.read_exact(&mut msg_buf).await {
            error!("[handle_client] Fehler beim Lesen der Nachricht: {}", e);
            break;
        }

        let full_packet = [&len_buf[..], &msg_buf[..]].concat();

        // Sync-Weiterleitung: Wenn SyncManager vorhanden, leite an Master/Slaves weiter
        if let Some(sync_manager) = &sync_manager {
            info!("[handle_client] Leite Nachricht an SyncManager weiter");
            if let Err(e) = sync_manager.sync_message(&full_packet).await {
                warn!("[handle_client] Sync-Weiterleitung fehlgeschlagen: {}", e);
            }
        }

        let event = msg_buf[0];
        let fields = parse_tlv_fields(Bytes::copy_from_slice(&msg_buf[1..]));
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
        let tlv_data = tlvs.map_or(Bytes::new(), |fields| encode_tlv_fields(&fields));
        let total_len = 1 + tlv_data.len() as u32;

        debug!("[ResponseBuilder::ok] Baue OK-Antwort mit {} TLV-Bytes", tlv_data.len());

        let mut buf = BytesMut::with_capacity(4 + total_len as usize);
        buf.put_u32(total_len);
        buf.put_u8(0x01); // STATUS_OK
        buf.extend_from_slice(&tlv_data);
        buf.to_vec()
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

        let tlv_data = encode_tlv_fields(&tlvs);
        let total_len = 1 + tlv_data.len() as u32;

        let mut buf = BytesMut::with_capacity(4 + total_len as usize);
        buf.put_u32(total_len);
        buf.put_u8(status);
        buf.extend_from_slice(&tlv_data);
        buf.to_vec()
    }
}