use netcache_manager::config::config::{Config, SocketConfig, StorageConfig};
use netcache_manager::cache_handler::socket_handler::start_server;
use netcache_manager::protocol::tlv::{TlvField, TlvFieldTypes, parse_tlv_fields};
use netcache_manager::cache_handler::ram_handler::RamStore;
use bytes::{BytesMut, Bytes};
use tokio::net::{UnixStream, TcpStream};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::sync::Arc;
use std::path::Path;
use std::fs;
use std::time::{Duration, Instant};

fn extract_status(response: &[u8]) -> u8 {
    if response.len() > 4 {
        response[4]
    } else {
        0 // oder ein spezieller Fehlerwert
    }
}

fn assert_message_id(response: &[u8], msg_id: u32) {
    let tlvs = parse_tlv_fields(Bytes::copy_from_slice(&response[5..]));
    let found = tlvs.iter().any(|tlv| {
        tlv.type_id == TlvFieldTypes::MessageId && tlv.value.as_ref() == msg_id.to_be_bytes()
    });
    assert!(found, "MESSAGE_ID not found or incorrect in response");
}

async fn setup_unix_server(socket_path: &str) {
    if Path::new(socket_path).exists() {
        let _ = fs::remove_file(socket_path);
    }
    let config = Config {
        socket: SocketConfig {
            mode: "unix".to_string(),
            path: Some(socket_path.to_string()),
            addr: None,
        },
        storage: StorageConfig {
            max_ram_size: 512 * 1024 * 1024,
            ttl_checktime: 2,
        },
        eventrelay: netcache_manager::config::config::EventRelayConfig {
            mode: "tcp".to_string(),
            addr: Some("127.0.0.1:9999".to_string()),
            path: None,
        },
        sync: None,
    };
    let store = Arc::new(RamStore::new(config.storage.clone()));
    tokio::spawn(start_server(config, store));
    let mut waited = 0;
    while !Path::new(socket_path).exists() && waited < 1000 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        waited += 10;
    }
}

async fn setup_tcp_server(addr: &str) {
    let config = Config {
        socket: SocketConfig {
            mode: "tcp".to_string(),
            path: None,
            addr: Some(addr.to_string()),
        },
        storage: StorageConfig {
            max_ram_size: 512 * 1024 * 1024,
            ttl_checktime: 2,
        },
        eventrelay: netcache_manager::config::config::EventRelayConfig {
            mode: "tcp".to_string(),
            addr: Some("127.0.0.1:9999".to_string()),
            path: None,
        },
        sync: None,
    };
    let store = Arc::new(RamStore::new(config.storage.clone()));
    tokio::spawn(start_server(config, store));
    tokio::time::sleep(Duration::from_millis(100)).await;
}

async fn perf_test_unix(socket: &str) {
    setup_unix_server(socket).await;
    let mut client = UnixStream::connect(socket).await.unwrap();
    perf_test_core(&mut client, "UNIX").await;
}

async fn perf_test_tcp(addr: &str) {
    setup_tcp_server(addr).await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.set_nodelay(true).unwrap();

    perf_test_core(&mut client, "TCP").await;
}

async fn perf_test_core<S>(client: &mut S, label: &str)
where
    S: AsyncWriteExt + AsyncReadExt + Unpin,
{
    let small_key = b"perf_key";
    let small_value = b"perf_value";
    let big_key = b"big_key";
    let big_value = vec![42u8; 10 * 1024 * 1024]; // 10 MB

    let n = 10_000;

    // Kleine Werte: SET
    let start = Instant::now();
    for i in 0..n {
        let key = format!("{}_{}", std::str::from_utf8(small_key).unwrap(), i);
        let mut payload = BytesMut::new();
        TlvField::new(TlvFieldTypes::KEY, Bytes::from(key.clone())).encode(&mut payload);
        TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(small_value)).encode(&mut payload);
        let msg_id = i as u32;
        TlvField::new(TlvFieldTypes::MessageId, Bytes::from(msg_id.to_be_bytes().to_vec())).encode(&mut payload);
        let mut full_msg = vec![0x20];
        full_msg.extend_from_slice(&payload);
        let len = (full_msg.len() as u32).to_be_bytes();
        client.write_all(&len).await.unwrap();
        client.write_all(&full_msg).await.unwrap();
        let mut response = vec![0u8; 4096];
        let nresp = client.read(&mut response).await.unwrap();
        assert_eq!(extract_status(&response[..nresp]), 0x01);
        assert_message_id(&response[..nresp], msg_id);
    }
    let duration_set = start.elapsed();

    // Kleine Werte: GET
    let start = Instant::now();
    for i in 0..n {
        let key = format!("{}_{}", std::str::from_utf8(small_key).unwrap(), i);
        let mut payload = BytesMut::new();
        TlvField::new(TlvFieldTypes::KEY, Bytes::from(key.clone())).encode(&mut payload);
        let msg_id = i as u32;
        TlvField::new(TlvFieldTypes::MessageId, Bytes::from(msg_id.to_be_bytes().to_vec())).encode(&mut payload);
        let mut full_msg = vec![0x21];
        full_msg.extend_from_slice(&payload);
        let len = (full_msg.len() as u32).to_be_bytes();
        client.write_all(&len).await.unwrap();
        client.write_all(&full_msg).await.unwrap();
        let mut response = vec![0u8; 4096];
        let nresp = client.read(&mut response).await.unwrap();
        assert_eq!(extract_status(&response[..nresp]), 0x01);
        assert_message_id(&response[..nresp], msg_id);
    }
    let duration_get = start.elapsed();

    // Große Werte: SET
    let start = Instant::now();
    let mut payload = BytesMut::new();
    TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(big_key)).encode(&mut payload);
    TlvField::new(TlvFieldTypes::VALUE, Bytes::from(big_value.clone())).encode(&mut payload);
    let msg_id: u32 = 0xDEADBEEF;
    TlvField::new(TlvFieldTypes::MessageId, Bytes::from(msg_id.to_be_bytes().to_vec())).encode(&mut payload);
    let mut full_msg = vec![0x20];
    full_msg.extend_from_slice(&payload);
    let len = (full_msg.len() as u32).to_be_bytes();
    client.write_all(&len).await.unwrap();
    client.write_all(&full_msg).await.unwrap();
    let mut response = vec![0u8; 4096];
    let nresp = client.read(&mut response).await.unwrap();
    assert_eq!(extract_status(&response[..nresp]), 0x01);
    assert_message_id(&response[..nresp], msg_id);
    let duration_big_set = start.elapsed();

    // Große Werte: GET
    let start = Instant::now();
    let mut payload = BytesMut::new();
    TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(big_key)).encode(&mut payload);
    let msg_id: u32 = 0xDEADBEEF;
    TlvField::new(TlvFieldTypes::MessageId, Bytes::from(msg_id.to_be_bytes().to_vec())).encode(&mut payload);
    let mut full_msg = vec![0x21];
    full_msg.extend_from_slice(&payload);
    let len = (full_msg.len() as u32).to_be_bytes();
    client.write_all(&len).await.unwrap();
    client.write_all(&full_msg).await.unwrap();
    let mut response = vec![0u8; 10 * 1024 * 1024 + 1024];
    let nresp = client.read(&mut response).await.unwrap();
    assert_eq!(extract_status(&response[..nresp]), 0x01);
    assert_message_id(&response[..nresp], msg_id);
    let duration_big_get = start.elapsed();

    println!("\n--- Performance Report ({}) ---", label);
    println!("SET ({} kleine Werte, je {} Bytes):   {:.2?} (avg: {:.4?} pro SET)", n, small_value.len(), duration_set, duration_set / n as u32);
    println!("GET ({} kleine Werte, je {} Bytes):   {:.2?} (avg: {:.4?} pro GET)", n, small_value.len(), duration_get, duration_get / n as u32);
    println!("SET (1 großer Wert, {} Bytes):     {:.2?}", big_value.len(), duration_big_set);
    println!("GET (1 großer Wert, {} Bytes):     {:.2?}", big_value.len(), duration_big_get);
    println!("-------------------------\n");
}

#[tokio::test]
async fn test_performance_unix() {
    let _ = env_logger::builder().is_test(true).try_init();
    perf_test_unix("/tmp/test_unix_perf.sock").await;
}

#[tokio::test]
async fn test_performance_tcp() {
    let _ = env_logger::builder().is_test(true).try_init();
    perf_test_tcp("127.0.0.1:8787").await;
}

#[tokio::test]
async fn test_parallel_set_get() {
    let _ = env_logger::builder().is_test(true).try_init();
    let addr = "127.0.0.1:8788";
    setup_tcp_server(addr).await;
    let n_clients = 20;
    let n_ops = 500;

    let mut handles = Vec::new();
    for c in 0..n_clients {
        let addr = addr.to_string();
        handles.push(tokio::spawn(async move {
            let mut client = TcpStream::connect(&addr).await.unwrap();
            client.set_nodelay(true).unwrap();
            for i in 0..n_ops {
                let key = format!("par_key_{}_{}", c, i);
                let mut payload = BytesMut::new();
                TlvField::new(TlvFieldTypes::KEY, Bytes::from(key.clone())).encode(&mut payload);
                TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(b"par_value")).encode(&mut payload);
                let msg_id = (c * n_ops + i) as u32;
                TlvField::new(TlvFieldTypes::MessageId, Bytes::from(msg_id.to_be_bytes().to_vec())).encode(&mut payload);
                let mut full_msg = vec![0x20];
                full_msg.extend_from_slice(&payload);
                let len = (full_msg.len() as u32).to_be_bytes();
                client.write_all(&len).await.unwrap();
                client.write_all(&full_msg).await.unwrap();
                let mut response = vec![0u8; 4096];
                let nresp = client.read(&mut response).await.unwrap();
                assert_eq!(extract_status(&response[..nresp]), 0x01);
                assert_message_id(&response[..nresp], msg_id);
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_mass_delete() {
    let _ = env_logger::builder().is_test(true).try_init();
    let addr = "127.0.0.1:8789";
    setup_tcp_server(addr).await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.set_nodelay(true).unwrap();
    let n = 5000;
    // Vorbereiten
    for i in 0..n {
        let key = format!("del_perf_{}", i);
        let mut payload = BytesMut::new();
        TlvField::new(TlvFieldTypes::KEY, Bytes::from(key.clone())).encode(&mut payload);
        TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(b"v")).encode(&mut payload);
        let msg_id = i as u32;
        TlvField::new(TlvFieldTypes::MessageId, Bytes::from(msg_id.to_be_bytes().to_vec())).encode(&mut payload);
        let mut full_msg = vec![0x20];
        full_msg.extend_from_slice(&payload);
        let len = (full_msg.len() as u32).to_be_bytes();
        client.write_all(&len).await.unwrap();
        client.write_all(&full_msg).await.unwrap();
        let mut response = vec![0u8; 4096];
        let nresp = client.read(&mut response).await.unwrap();
        assert_eq!(extract_status(&response[..nresp]), 0x01);
        assert_message_id(&response[..nresp], msg_id);
    }
    // Löschen messen
    let start = Instant::now();
    for i in 0..n {
        let key = format!("del_perf_{}", i);
        let mut payload = BytesMut::new();
        TlvField::new(TlvFieldTypes::KEY, Bytes::from(key.clone())).encode(&mut payload);
        // MESSAGE_ID hinzufügen
        let msg_id = i as u32;
        TlvField::new(TlvFieldTypes::MessageId, Bytes::from(msg_id.to_be_bytes().to_vec())).encode(&mut payload);
        let mut full_msg = vec![0x22];
        full_msg.extend_from_slice(&payload);
        let len = (full_msg.len() as u32).to_be_bytes();
        client.write_all(&len).await.unwrap();
        client.write_all(&full_msg).await.unwrap();
        let mut response = vec![0u8; 4096];
        let nresp = client.read(&mut response).await.unwrap();
        assert_eq!(extract_status(&response[..nresp]), 0x01);
        assert_message_id(&response[..nresp], msg_id);
    }
    let duration = start.elapsed();
    println!("DELETE ({} Keys): {:.2?} (avg: {:.4?} pro DEL)", n, duration, duration / n as u32);
}