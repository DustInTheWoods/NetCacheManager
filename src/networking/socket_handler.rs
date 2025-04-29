use bytes::{BufMut, Bytes, BytesMut};
use crate::config::config::Config;
use crate::protocol::tlv::{encode_tlv_fields, parse_tlv_fields, TlvField, TlvFieldTypes};
use crate::storage::ram_handler::RamStore;
use crate::storage::storage_handler::{handle_event, StorageError};
use log::{debug, error, info};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::time::{timeout, Duration};

pub async fn start_server(config: Config, store: Arc<RamStore>) {
    match config.socket.mode.as_str() {
        "unix" => {
            if let Some(path) = &config.socket.path {
                let _ = std::fs::remove_file(path);
                let listener = UnixListener::bind(path).expect("UnixSocket konnte nicht gebunden werden");
                info!("Lausche auf Unix-Socket: {}", path);
                loop {
                    let (stream, addr) = listener.accept().await.expect("Fehler beim Annehmen");

                    info!("Neue Verbindung von: {:?}", addr);

                    let store = store.clone(); // <- Store weiterreichen
                    tokio::spawn(async move {
                        handle_unix_client(stream, store).await;
                    });
                }
            }
        }
        "tcp" => {
            if let Some(addr) = &config.socket.addr {
                let listener = TcpListener::bind(addr).await.expect("TCP konnte nicht gestartet werden");
                info!("Lausche auf TCP-Adresse: {}", addr);
                loop {
                    let (stream, addr) = listener.accept().await.expect("Fehler beim Annehmen");

                    info!("Neue Verbindung von: {:?}", addr);

                    let store = store.clone();
                    tokio::spawn(async move {
                        handle_tcp_client(stream, store).await;
                    });
                }
            }
        }
        _ => panic!("Unbekannter Socket-Modus: {}", config.socket.mode),
    }
}

async fn handle_unix_client(stream: UnixStream, store: Arc<RamStore>) {
    handle_client(stream, store).await;
}

async fn handle_tcp_client(stream: TcpStream, store: Arc<RamStore>) {
    // TCP-Nodelay aktivieren
    stream.set_nodelay(true).expect("Setze TCP_NODELAY fehlgeschlagen");

    handle_client(stream, store).await;
}

async fn handle_client<S>(mut stream: S, store: Arc<RamStore>)
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    loop {
        let mut len_buf = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut len_buf).await {
            error!("Fehler beim Lesen der Nachrichtenlänge: {}", e);
            break; // Verbindung wird beendet, wenn Client trennt oder Fehler
        }
        // Gibt die einzelnen Bytes in Hex aus
        debug!(
            "Gelesene Bytes: {:02x} {:02x} {:02x} {:02x}",
            len_buf[0], len_buf[1], len_buf[2], len_buf[3]
        );

        // Gibt die Länge in Hex aus
        debug!("Gelesene Nachrichtenlänge: {:#04x}", u32::from_be_bytes(len_buf));

        let start_time = Instant::now();
        
        let msg_len = u32::from_be_bytes(len_buf) as usize;
        debug!("Die Nachrichtenlänge beträgt: {} Bytes", msg_len);

        let mut msg_buf = vec![0u8; msg_len];
        let mut read = 0;
        while read < msg_len {
            match timeout(Duration::from_secs(6), stream.read(&mut msg_buf[read..])).await {
                Ok(Ok(0)) => {
                    error!("Verbindung vorzeitig geschlossen");
                    break;
                }
                Ok(Ok(n)) => {
                    read += n;
                }
                Ok(Err(e)) => {
                    error!("Fehler beim Lesen der Nachricht: {}", e);
                    break;
                }
                Err(_) => {
                    error!("Timeout beim Lesen der Nachricht überschritten");
                    break;
                }
            }
        }
        if read != msg_len {
            error!("Unerwartete Nachrichtenlänge: erwartet {}, gelesen {}", msg_len, read);
            break;
        }

        let event = msg_buf[0];
        let fields = parse_tlv_fields(Bytes::copy_from_slice(&msg_buf[1..]));

        info!("Empfangenes Event: {:#04x}, {} TLVs", event, fields.len());

        let response = match handle_event(event, fields.clone(), store.clone()).await {
            Ok(tlvs) => ResponseBuilder::ok(Some(tlvs)),
            Err(err) => ResponseBuilder::error(err, &fields),
        };

        debug!(
            "Zeit zur Verarbeitung der Anfrage: {:?}. Sende Nachricht",
            Instant::now() - start_time
        );

        if let Err(e) = stream.write_all(&response).await {
            error!("Fehler beim Senden der Antwort: {}", e);
            break;
        }
    }
}

struct ResponseBuilder;

impl ResponseBuilder {
    pub fn ok(tlvs: Option<Vec<TlvField>>) -> Vec<u8> {
        let tlv_data = tlvs.map_or(Bytes::new(), |fields| encode_tlv_fields(&fields));
        let total_len = 1 + tlv_data.len() as u32;

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