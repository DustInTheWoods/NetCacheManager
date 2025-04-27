use netcache_manager::config::config::{Config, SocketConfig, StorageConfig};
use netcache_manager::networking::socket_handler::start_server;
use netcache_manager::protocol::tlv::{TlvField, TlvFieldTypes};
use netcache_manager::storage::ram_handler::RamStore;
use bytes::{BytesMut, Bytes};
use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests {
    use super::*;

    async fn send_request(event: u8, tlvs: Vec<TlvField>, addr: &str) -> Vec<u8> {
        let mut stream = TcpStream::connect(addr)
            .await
            .expect("Verbindung fehlgeschlagen");

        let mut payload = BytesMut::new();
        for tlv in &tlvs {
            tlv.encode(&mut payload);
        }

        let mut full_msg = vec![event];
        full_msg.extend_from_slice(&payload);

        let len = (full_msg.len() as u32).to_be_bytes();
        stream.write_all(&len).await.unwrap();
        stream.write_all(&full_msg).await.unwrap();

        let mut response = vec![0u8; 1024];
        let n = stream.read(&mut response).await.unwrap();
        response[..n].to_vec()
    }

    fn extract_status(response: &[u8]) -> u8 {
        response[4]
    }

    async fn setup_server(addr: &str) {
        let config = Config {
            socket: SocketConfig {
                mode: "tcp".to_string(),
                path: None,
                addr: Some(addr.to_string()),
            },
            storage: StorageConfig {
                max_ram_size: 100 * 1024 * 1024,
                ttl_checktime: 5,
            }
        };

        let store = Arc::new(RamStore::new(config.storage.clone()));

        tokio::spawn(start_server(config, store));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_basic() {
        let addr = "127.0.0.1:8890";
        setup_server(addr).await;

        let key = b"test_key";
        let value = b"test_value";

        let resp = send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE , Bytes::from_static(value)),
        ], addr).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = send_request(0x21, vec![
            TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(key)),
        ], addr).await;
        assert_eq!(extract_status(&resp), 0x01);
    }

    #[tokio::test]
    async fn test_group() {
        let addr = "127.0.0.1:8891";
        setup_server(addr).await;

        let group = b"test_group";

        for (k, v) in [("key1", "val1"), ("key2", "val2"), ("key3", "val3")] {
            let resp = send_request(0x20, vec![
                TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(k.as_bytes())),
                TlvField::new(TlvFieldTypes::VALUE , Bytes::from_static(v.as_bytes())),
                TlvField::new(TlvFieldTypes::GROUP , Bytes::from_static(group)),
            ], addr).await;
            assert_eq!(extract_status(&resp), 0x01);
        }
    }

    #[tokio::test]
    async fn test_ttl() {
        let addr = "127.0.0.1:8892";
        setup_server(addr).await;

        let key = b"ttl_key";
        let value = b"short_lived";

        send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE , Bytes::from_static(value)),
            TlvField::new(TlvFieldTypes::TTL , Bytes::from_static(b"1")),
        ], addr).await;

        tokio::time::sleep(Duration::from_secs(2)).await;

        let resp = send_request(0x21, vec![
            TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(key)),
        ], addr).await;
        assert_eq!(extract_status(&resp), 0x02);
    }

    #[tokio::test]
    async fn test_delete() {
        let addr = "127.0.0.1:8893";
        setup_server(addr).await;

        let key = b"delete_key";
        let value = b"to_be_deleted";

        send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE , Bytes::from_static(value)),
        ], addr).await;

        send_request(0x22, vec![
            TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(key)),
        ], addr).await;

        let resp = send_request(0x21, vec![ 
            TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(key)),
        ], addr).await;
        assert_eq!(extract_status(&resp), 0x02);
    }

    #[tokio::test]
    async fn test_performance() {
        let addr = "127.0.0.1:8894";
        setup_server(addr).await;

        let value = Bytes::from_static(b"perfdata");
        let keys: Vec<String> = (0..1000).map(|i| format!("key{}", i)).collect();

        let start_set = Instant::now();
        for k in keys.clone() {
            let resp = send_request(0x20, vec![
                TlvField::new(TlvFieldTypes::KEY , Bytes::from(k.clone())),
                TlvField::new(TlvFieldTypes::VALUE , value.clone()),
            ], addr).await;
            assert_eq!(extract_status(&resp), 0x01);
        }
        let duration_set = start_set.elapsed();

        let start_get = Instant::now();
        for k in keys.clone() {
            let resp = send_request(0x21, vec![
                TlvField::new(TlvFieldTypes::KEY , Bytes::from(k.clone())),
            ], addr).await;
            assert_eq!(extract_status(&resp), 0x01);
        }
        let duration_get = start_get.elapsed();

        println!(
            "Performance test (via tcp connection): 1000 SETs in {:?}, 1000 GETs in {:?}",
            duration_set, duration_get
        );
    }
}
