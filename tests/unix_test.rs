#[cfg(test)]
mod tests {
    use netcache_manager::config::config::{Config, SocketConfig, StorageConfig};
    use netcache_manager::networking::socket_handler::start_server;
    use netcache_manager::protocol::tlv::{TlvField, TlvFieldTypes};
    use netcache_manager::storage::ram_handler::RamStore;
    use bytes::{BytesMut, Bytes};
    use tokio::net::UnixStream;
    use tokio::io::{AsyncWriteExt, AsyncReadExt};
    use std::path::Path;
    use std::sync::Arc;
    use std::fs;
    use std::time::{Duration, Instant};

    struct TestClient {
        stream: UnixStream,
    }

    impl TestClient {
        async fn connect(socket_path: &str) -> Self {
            let stream = UnixStream::connect(socket_path)
                .await
                .expect("Verbindung fehlgeschlagen");
            Self { stream }
        }

        async fn send_request(&mut self, event: u8, tlvs: Vec<TlvField>) -> Vec<u8> {
            let mut payload = BytesMut::new();
            for tlv in &tlvs {
                tlv.encode(&mut payload);
            }

            let mut full_msg = vec![event];
            full_msg.extend_from_slice(&payload);

            let len = (full_msg.len() as u32).to_be_bytes();
            self.stream.write_all(&len).await.unwrap();
            self.stream.write_all(&full_msg).await.unwrap();

            let mut response = vec![0u8; 1024];
            let n = self.stream.read(&mut response).await.unwrap();
            response[..n].to_vec()
        }
    }

    fn extract_status(response: &[u8]) -> u8 {
        response[4] // 4-Bytes Länge + 1-Byte Status
    }

    async fn setup_server(socket_path: &str) {
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
                max_ram_size: 100 * 1024 * 1024,
                ttl_checktime: 5
            }
        };

        let store = Arc::new(RamStore::new(config.storage.clone()));
        tokio::spawn(start_server(config, store));

        let mut waited = 0;
        while !Path::new(socket_path).exists() && waited < 1000 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            waited += 10;
        }
    }

    #[tokio::test]
    async fn test_basic() {
        let socket = "/tmp/test_socket_basic.sock";
        setup_server(socket).await;

        let mut client = TestClient::connect(socket).await;

        let key = b"test_key";
        let value = b"test_value";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x21, vec![
            TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);
    }

    #[tokio::test]
    async fn test_group() {
        let socket = "/tmp/test_socket_group.sock";
        setup_server(socket).await;

        let mut client = TestClient::connect(socket).await;
        let group = b"test_group";

        for (k, v) in [("key1", "val1"), ("key2", "val2"), ("key3", "val3")] {
            let resp = client.send_request(0x20, vec![
                TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(k.as_bytes())),
                TlvField::new(TlvFieldTypes::VALUE , Bytes::from_static(v.as_bytes())),
                TlvField::new(TlvFieldTypes::GROUP , Bytes::from_static(group)),
            ]).await;
            assert_eq!(extract_status(&resp), 0x01);
        }
    }

    #[tokio::test]
    async fn test_ttl() {
        let socket = "/tmp/test_socket_ttl.sock";
        setup_server(socket).await;

        let mut client = TestClient::connect(socket).await;

        let key = b"ttl_key";
        let value = b"short_lived";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY  , Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE , Bytes::from_static(value)),
            TlvField::new(TlvFieldTypes::TTL , Bytes::from_static(b"1")),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        tokio::time::sleep(Duration::from_secs(2)).await;

        let resp = client.send_request(0x21, vec![
            TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x02); // NOT FOUND
    }

    #[tokio::test]
    async fn test_delete() {
        let socket = "/tmp/test_socket_delete.sock";
        setup_server(socket).await;

        let mut client = TestClient::connect(socket).await;

        let key = b"delete_key";
        let value = b"to_be_deleted";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x22, vec![
            TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x21, vec![
            TlvField::new(TlvFieldTypes::KEY , Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x02);
    }

    #[tokio::test]
    async fn test_flush() {
        let socket = "/tmp/test_socket_flush.sock";
        setup_server(socket).await;

        let mut client = TestClient::connect(socket).await;

        for i in 0..5 {
            let key = format!("flush_key{}", i);
            let resp = client.send_request(0x20, vec![
                TlvField::new(TlvFieldTypes::KEY , Bytes::from(key)),
                TlvField::new(TlvFieldTypes::VALUE , Bytes::from_static(b"dummy")),
            ]).await;
            assert_eq!(extract_status(&resp), 0x01);
        }

        let resp = client.send_request(0x23, vec![]).await;
        assert_eq!(extract_status(&resp), 0x01);
    }

    #[tokio::test]
    async fn test_performance() {
        let socket = "/tmp/test_socket_perf.sock";
        setup_server(socket).await;

        let mut client = TestClient::connect(socket).await;

        let value = Bytes::from_static(b"perfdata");
        let keys: Vec<String> = (0..1000).map(|i| format!("key{}", i)).collect();

        let start_set = Instant::now();
        for k in &keys {
            let resp = client.send_request(0x20, vec![
                TlvField::new(TlvFieldTypes::KEY, Bytes::from(k.clone())),
                TlvField::new(TlvFieldTypes::VALUE , value.clone()),
            ]).await;
            assert_eq!(extract_status(&resp), 0x01);
        }
        let duration_set = start_set.elapsed();

        let start_get = Instant::now();
        for k in &keys {
            let resp = client.send_request(0x21, vec![
                TlvField::new(TlvFieldTypes::KEY, Bytes::from(k.clone())),
            ]).await;
            assert_eq!(extract_status(&resp), 0x01);
        }
        let duration_get = start_get.elapsed();

        println!(
            "Performance test (via unix connection): 1000 SETs in {:?}, 1000 GETs in {:?}",
            duration_set, duration_get
        );
    }
}
