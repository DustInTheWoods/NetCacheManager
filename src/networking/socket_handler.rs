use bytes::{BufMut, Bytes, BytesMut};
use crate::config::config::Config;
use crate::protocol::tlv::{encode_tlv_fields, parse_tlv_fields, TlvField};
use crate::storage::ram_handler::RamStore;
use crate::storage::storage_handler::{handle_event, StorageError};
use log::{error, info};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};

pub async fn start_server(config: Config, store: Arc<RamStore>) {
    match config.socket.mode.as_str() {
        "unix" => {
            if let Some(path) = &config.socket.path {
                let _ = std::fs::remove_file(path);
                let listener = UnixListener::bind(path).expect("UnixSocket konnte nicht gebunden werden");
                info!("Lausche auf Unix-Socket: {}", path);
                loop {
                    let (stream, _) = listener.accept().await.expect("Fehler beim Annehmen");
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
                    let (stream, _) = listener.accept().await.expect("Fehler beim Annehmen");
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
    handle_client(stream, store).await;
}

async fn handle_client<S>(mut stream: S, store: Arc<RamStore>)
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err() {
        error!("Fehler beim Lesen der Nachrichtenlänge");
        return;
    }
    
    let msg_len = u32::from_be_bytes(len_buf) as usize;
    let mut msg_buf = vec![0u8; msg_len];
    if stream.read_exact(&mut msg_buf).await.is_err() {
        error!("Fehler beim Lesen der Nachricht");
        return;
    }

    let event = msg_buf[0];
    let fields = parse_tlv_fields(Bytes::copy_from_slice(&msg_buf[1..]));

    info!("Empfangenes Event: {:#04x}, {} TLVs", event, fields.len());

    let response = match handle_event(event, fields, store).await {
        Ok(tlvs) => ResponseBuilder::ok(Some(tlvs)),
        Err(err) => ResponseBuilder::error(err),
    };

    let _ = stream.write_all(&response).await;
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

    pub fn error(err: StorageError) -> Vec<u8> {
        let (status, text) = match err {
            StorageError::NotFound => (0x02, "Not Found"),
            StorageError::InvalidInput => (0x03, "Invalid Input"),
            StorageError::AlreadyExists => (0x04, "Already Exists"),
            StorageError::InternalError => (0x05, "Internal Error"),
        };

        let text_bytes = text.as_bytes();
        let total_len = 1 + 1 + 4 + text_bytes.len() as u32; // status + error TLV

        let mut buf = BytesMut::with_capacity(4 + total_len as usize);
        buf.put_u32(total_len);
        buf.put_u8(status);
        buf.put_u8(0xFF); // Error TLV type
        buf.put_u32(text_bytes.len() as u32);
        buf.extend_from_slice(text_bytes);
        buf.to_vec()
    }
}