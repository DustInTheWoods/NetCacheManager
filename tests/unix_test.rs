#[cfg(test)]
mod tests {
    use netcache_manager::config::config::{Config, SocketConfig, StorageConfig};
    use netcache_manager::cache_handler::socket_handler::start_server;
    use netcache_manager::protocol::tlv::{TlvField, TlvFieldTypes, parse_tlv_fields};
    use netcache_manager::cache_handler::ram_handler::RamStore;
    use bytes::{BytesMut, Bytes};
    use tokio::net::UnixStream;
    use tokio::io::{AsyncWriteExt, AsyncReadExt};
    use std::sync::Arc;
    use std::path::Path;
    use std::fs;
    use std::time::{Duration, Instant};

    struct TestClient {
        stream: UnixStream,
        next_msg_id: u32,
    }

    impl TestClient {
        async fn connect(socket_path: &str) -> Self {
            let stream = UnixStream::connect(socket_path)
                .await
                .expect("Verbindung fehlgeschlagen");
            Self { stream, next_msg_id: 1 }
        }

        async fn send_request(&mut self, event: u8, mut tlvs: Vec<TlvField>) -> Vec<u8> {
            if event != 0x01 {
                let msg_id = self.next_msg_id;
                self.next_msg_id += 1;
                tlvs.push(TlvField::new(
                    TlvFieldTypes::MessageId,
                    Bytes::from(msg_id.to_be_bytes().to_vec()),
                ));
            }

            let mut payload = BytesMut::new();
            for tlv in &tlvs {
                tlv.encode(&mut payload);
            }

            let mut full_msg = vec![event];
            full_msg.extend_from_slice(&payload);

            let len = (full_msg.len() as u32).to_be_bytes();
            self.stream.write_all(&len).await.unwrap();
            self.stream.write_all(&full_msg).await.unwrap();

            let mut response = vec![0u8; 4096];
            let n = self.stream.read(&mut response).await.unwrap();

            // Für PING keine MESSAGE_ID prüfen!
            if event != 0x01 {
                let msg_id = self.next_msg_id - 1;
                // TLVs ab Offset 5 parsen
                let tlvs = parse_tlv_fields(Bytes::copy_from_slice(&response[5..n]));
                let found = tlvs.iter().any(|tlv| {
                    tlv.type_id == TlvFieldTypes::MessageId && tlv.value.as_ref() == msg_id.to_be_bytes()
                });
                assert!(found, "MESSAGE_ID not found in response");
            }

            response[..n].to_vec()
        }
    }

    fn extract_status(response: &[u8]) -> u8 {
        response[4]
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
                ttl_checktime: 2,
            },
            eventrelay: netcache_manager::config::config::EventRelayConfig {
                mode: "unix".to_string(),
                addr: None,
                path: Some("/tmp/dummy.sock".to_string()),
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

    #[tokio::test]
    async fn test_set_and_get() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_setget.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let key = b"test_key";
        let value = b"test_value";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x21, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);
    }

    #[tokio::test]
    async fn test_ttl_expiry() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_ttl.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let key = b"ttl_key";
        let value = b"short_lived";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
            TlvField::new(TlvFieldTypes::TTL, Bytes::from_static(b"1")),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        tokio::time::sleep(Duration::from_secs(2)).await;

        let resp = client.send_request(0x21, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x02);
    }

    #[tokio::test]
    async fn test_delete_key() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_delete.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let key = b"delete_key";
        let value = b"to_be_deleted";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x22, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x21, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x02);
    }

    #[tokio::test]
    async fn test_ping_pong() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_ping.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        // Sende PING (Event 0x01) ohne TLVs
        let ping_msg = vec![0x00, 0x00, 0x00, 0x01, 0x01];
        client.stream.write_all(&ping_msg).await.unwrap();

        let mut response = vec![0u8; 5];
        client.stream.read_exact(&mut response).await.unwrap();

        // Erwartet: [LENGTH=1][STATUS=0x01]
        assert_eq!(response, vec![0x00, 0x00, 0x00, 0x01, 0x01]);
    }

    #[tokio::test]
    async fn test_group_query() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_group.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let group = b"group1";
        let key1 = b"group_key1";
        let value1 = b"value1";
        let key2 = b"group_key2";
        let value2 = b"value2";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key1)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value1)),
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key2)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value2)),
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x21, vec![
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);
        assert!(resp.len() > 5);
    }

    #[tokio::test]
    async fn test_info() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_info.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let key = b"info_key";
        let value = b"info_value";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x30, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);
        assert!(resp.len() > 5);
    }

    #[tokio::test]
    async fn test_copy_key() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_copy.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let key = b"copy_key";
        let value = b"copy_value";
        let new_key = b"new_copy_key";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x25, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::NewKey, Bytes::from_static(new_key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x21, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(new_key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);
        assert!(resp.len() > 5);
    }

    #[tokio::test]
    async fn test_touch() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_touch.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let key = b"touch_key";
        let value = b"touch_value";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
            TlvField::new(TlvFieldTypes::TTL, Bytes::from_static(b"2")),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x24, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        tokio::time::sleep(Duration::from_secs(1)).await;

        let resp = client.send_request(0x21, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);
    }

    #[tokio::test]
    async fn test_touch_without_ttl() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_touch_no_ttl.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let key = b"no_ttl_key";
        let value = b"no_ttl_value";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x24, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x03);
    }

    #[tokio::test]
    async fn test_flush() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_flush.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let key = b"flush_key";
        let value = b"flush_value";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x23, vec![]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x21, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x02);
    }

    #[tokio::test]
    async fn test_exists_key() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_exists_key.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let key = b"exists_key";
        let value = b"exists_value";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x10, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);
    }

    #[tokio::test]
    async fn test_overwrite_existing_key() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_overwrite.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let key = b"overwrite_key";
        let value1 = b"value1";
        let value2 = b"value2";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value1)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value2)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x21, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert!(resp.contains(&value2[0]));
    }

    #[tokio::test]
    async fn test_delete_nonexistent_key() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_delete_nonexist.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let key = b"not_existing_key";

        let resp = client.send_request(0x22, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert!(extract_status(&resp) == 0x01 || extract_status(&resp) == 0x02);
    }

    #[tokio::test]
    async fn test_exists_group() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_exists_group.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let group = b"exists_group";
        let key = b"grouped_key";
        let value = b"grouped_value";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x10, vec![
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);
    }

    #[tokio::test]
    async fn test_info_nonexistent_key() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_info_nonexist.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let key = b"no_info_key";

        let resp = client.send_request(0x30, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x02);
    }

    #[tokio::test]
    async fn test_delete_group() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_delete_group.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let group = b"del_group";
        let key1 = b"del_group_key1";
        let key2 = b"del_group_key2";
        let value = b"val";

        for key in &[key1, key2] {
            let resp = client.send_request(0x20, vec![
                TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(*key)),
                TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
                TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
            ]).await;
            assert_eq!(extract_status(&resp), 0x01);
        }

        let resp = client.send_request(0x22, vec![
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        for key in &[key1, key2] {
            let resp = client.send_request(0x21, vec![
                TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(*key)),
            ]).await;
            assert_eq!(extract_status(&resp), 0x02);
        }
    }

    #[tokio::test]
    async fn test_flush_removes_groups() {
        let _ = env_logger::builder().is_test(true).try_init();
        let socket = "/tmp/test_unix_flush_groups.sock";
        setup_server(socket).await;
        let mut client = TestClient::connect(socket).await;

        let group = b"flush_group";
        let key = b"flush_group_key";
        let value = b"flush_group_value";

        let resp = client.send_request(0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x23, vec![]).await;
        assert_eq!(extract_status(&resp), 0x01);

        let resp = client.send_request(0x10, vec![
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x02);
    }
}
