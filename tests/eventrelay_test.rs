// Integrationstest für das EventRelay-Protokoll
// Testet Broadcast, Direktnachricht, Fehlerfälle und Authentifizierung

use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpStream, UnixStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use uuid::Uuid;
use netcache_manager::eventrelay::eventrelay::EventRelay;
use netcache_manager::config::config::EventRelayConfig;
use netcache_manager::cache_handler::ram_handler::RamStore;
use netcache_manager::protocol::tlv::{TlvField, TlvFieldTypes};
use bytes::{Bytes, BytesMut};

async fn start_eventrelay(config: &EventRelayConfig) -> Arc<EventRelay> {
    let relay = Arc::new(EventRelay::new());
    let relay_clone = relay.clone();
    let config = config.clone();
    tokio::spawn(async move {
        relay_clone.start_server(&config).await;
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    relay
}

async fn handshake<S: AsyncWriteExt + Unpin>(stream: &mut S, uuid: Uuid) {
    stream.write_all(uuid.as_bytes()).await.unwrap();
}

async fn send_message<S: AsyncWriteExt + Unpin>(stream: &mut S, to: Uuid, from: Uuid, payload: &[u8]) {
    let len = (16 + 16 + payload.len()) as u32;
    stream.write_all(&len.to_be_bytes()).await.unwrap();
    stream.write_all(to.as_bytes()).await.unwrap();
    stream.write_all(from.as_bytes()).await.unwrap();
    stream.write_all(payload).await.unwrap();
}

async fn recv_message<S: AsyncReadExt + Unpin>(stream: &mut S) -> (Uuid, Uuid, Vec<u8>) {
    let mut len_buf = [0u8; 4];
    timeout(Duration::from_secs(1), stream.read_exact(&mut len_buf)).await.unwrap().unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut msg = vec![0u8; len];
    timeout(Duration::from_secs(1), stream.read_exact(&mut msg)).await.unwrap().unwrap();
    let to = Uuid::from_slice(&msg[0..16]).unwrap();
    let from = Uuid::from_slice(&msg[16..32]).unwrap();
    let payload = msg[32..].to_vec();
    (to, from, payload)
}

#[tokio::test]
async fn test_broadcast_and_direct_tcp() {
    let _ = env_logger::builder().is_test(true).try_init();
    let addr = "127.0.0.1:9997";
    let config = EventRelayConfig {
        mode: "tcp".to_string(),
        addr: Some(addr.to_string()),
        path: None,
    };
    let _relay = start_eventrelay(&config).await;
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();
    let mut c1 = TcpStream::connect(addr).await.unwrap();
    let mut c2 = TcpStream::connect(addr).await.unwrap();
    let mut c3 = TcpStream::connect(addr).await.unwrap();
    handshake(&mut c1, uuid1).await;
    handshake(&mut c2, uuid2).await;
    handshake(&mut c3, uuid3).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Broadcast von c1
    let payload = b"BROADCAST";
    send_message(&mut c1, Uuid::nil(), uuid1, payload).await;
    let (_to2, from2, pay2) = recv_message(&mut c2).await;
    let (_to3, from3, pay3) = recv_message(&mut c3).await;
    assert_eq!(from2, uuid1);
    assert_eq!(from3, uuid1);
    assert_eq!(pay2, payload);
    assert_eq!(pay3, payload);
    // Direktnachricht von c2 an c3
    let payload2 = b"DIRECT";
    send_message(&mut c2, uuid3, uuid2, payload2).await;
    let (to, from, pay) = recv_message(&mut c3).await;
    assert_eq!(to, uuid3);
    assert_eq!(from, uuid2);
    assert_eq!(pay, payload2);
    // c1 und c2 dürfen nichts empfangen (Timeout)
    let res1 = timeout(Duration::from_millis(100), recv_message(&mut c1)).await;
    assert!(res1.is_err());
    let res2 = timeout(Duration::from_millis(100), recv_message(&mut c2)).await;
    assert!(res2.is_err());
}

#[tokio::test]
async fn test_unix_auth_and_error() {
    let _ = env_logger::builder().is_test(true).try_init();
    let path = "/tmp/test_eventrelay.sock";
    let config = EventRelayConfig {
        mode: "unix".to_string(),
        addr: None,
        path: Some(path.to_string()),
    };
    let _relay = start_eventrelay(&config).await;
    let uuid1 = Uuid::new_v4();
    // Ungültige UUID (zu kurz)
    let mut c1 = UnixStream::connect(path).await.unwrap();
    c1.write_all(&[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]).await.unwrap();
    let mut err_buf = [0u8; 64];
    let n = match timeout(Duration::from_secs(1), c1.read(&mut err_buf)).await {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => panic!("Fehler beim Lesen: {:?}", e),
        Err(_) => panic!("Timeout: Keine Antwort vom Server bei ungültiger UUID"),
    };
    assert!(n > 0);
    // Gültige UUID
    let mut c2 = UnixStream::connect(path).await.unwrap();
    handshake(&mut c2, uuid1).await;
    // Nochmal gleiche UUID (Fehler)
    let mut c3 = UnixStream::connect(path).await.unwrap();
    handshake(&mut c3, uuid1).await;
    let mut err_buf2 = [0u8; 64];
    let n2 = c3.read(&mut err_buf2).await.unwrap();
    assert!(n2 > 0);
}

#[tokio::test]
async fn test_direct_error_tcp() {
    let _ = env_logger::builder().is_test(true).try_init();
    let addr = "127.0.0.1:9996";
    let config = EventRelayConfig {
        mode: "tcp".to_string(),
        addr: Some(addr.to_string()),
        path: None,
    };
    let _relay = start_eventrelay(&config).await;
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let mut c1 = TcpStream::connect(addr).await.unwrap();
    handshake(&mut c1, uuid1).await;
    // Nachricht an nicht verbundenen Empfänger
    let payload = b"NOCLIENT";
    send_message(&mut c1, uuid2, uuid1, payload).await;
    let (_to, from, pay) = recv_message(&mut c1).await;
    assert_eq!(from.as_bytes(), &[0xFF; 16]);
    assert!(pay.starts_with(&0x00_00_00_05u32.to_be_bytes()));
}