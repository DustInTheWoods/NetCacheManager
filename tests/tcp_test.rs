use netcache_manager::config::config::{Config, SocketConfig, StorageConfig};
use netcache_manager::networking::socket_handler::start_server;
use netcache_manager::protocol::tlv::{TlvField, TlvFieldTypes};
use netcache_manager::storage::ram_handler::RamStore;
use bytes::{BytesMut, Bytes};
use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU32, Ordering};

static NEXT_MSG_ID: AtomicU32 = AtomicU32::new(1);

#[cfg(test)]
mod tests {
    use super::*;

    async fn send_request<S>(stream: &mut S, event: u8, mut tlvs: Vec<TlvField>) -> Vec<u8>
    where
        S: AsyncWriteExt + AsyncReadExt + Unpin,
    {
        // Automatisch MESSAGE_ID hinzufügen
        let msg_id = NEXT_MSG_ID.fetch_add(1, Ordering::SeqCst);
        tlvs.push(TlvField::new(
            TlvFieldTypes::MessageId,
            Bytes::from(msg_id.to_be_bytes().to_vec()),
        ));

        let mut payload = BytesMut::new();
        for tlv in &tlvs {
            tlv.encode(&mut payload);
        }

        let mut full_msg = vec![event];
        full_msg.extend_from_slice(&payload);

        let len = (full_msg.len() as u32).to_be_bytes();
        stream.write_all(&len).await.unwrap();
        stream.write_all(&full_msg).await.unwrap();

        let mut response = vec![0u8; 4096];
        let n = stream.read(&mut response).await.unwrap();

        // Suche MESSAGE_ID im TLV-Teil der Antwort
        let mut found_msg_id = false;
        let mut response_tlvs = &response[5..n];
        while !response_tlvs.is_empty() {
            let mut response_bytes = Bytes::copy_from_slice(response_tlvs);
            if let Some(tlv) = TlvField::decode(&mut response_bytes) {
                if tlv.type_id == TlvFieldTypes::MessageId {
                    assert_eq!(tlv.value.as_ref(), &msg_id.to_be_bytes(), "MESSAGE_ID mismatch");
                    found_msg_id = true;
                    break;
                }
                response_tlvs = &response_tlvs[(1 + 4 + tlv.value.len())..];
            } else {
                break;
            }
        }
        assert!(found_msg_id, "MESSAGE_ID not found in response");

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
            },
        };

        let store = Arc::new(RamStore::new(config.storage.clone()));

        tokio::spawn(start_server(config, store));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let addr = "127.0.0.1:8787";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let key = b"test_key";
        let value = b"test_value";

        // Test SET
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "SET operation failed");
        assert!(resp.len() >= 5, "Response should be at least 5 bytes long");

        // Test GET
        let resp = send_request(&mut stream, 0x21, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "GET operation failed");
        assert!(resp.len() > 5, "Response should contain additional TLV fields");

        // Verify TLV fields
        let mut response_tlvs = &resp[5..];
        while !response_tlvs.is_empty() {
            let mut response_bytes = Bytes::copy_from_slice(response_tlvs);
            if let Some(tlv) = TlvField::decode(&mut response_bytes) {
                response_tlvs = &response_tlvs[(1 + 4 + tlv.value.len())..]; // Typ (1 Byte) + Länge (4 Bytes) + Wert
                match tlv.type_id {
                    TlvFieldTypes::KEY => {
                        assert_eq!(tlv.value.as_ref(), key, "Key in TLV does not match");
                    }
                    TlvFieldTypes::VALUE => {
                        assert_eq!(tlv.value.as_ref(), value, "Value in TLV does not match");
                    }
                    _ => {}
                }
            } else {
                panic!("Failed to decode TLV");
            }
        }

        assert!(resp.len() > 5, "Response should contain additional TLV fields");
    }

    #[tokio::test]
    async fn test_ttl_expiry() {
        let addr = "127.0.0.1:8892";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let key = b"ttl_key";
        let value = b"short_lived";

        // Test SET with TTL
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
            TlvField::new(TlvFieldTypes::TTL, Bytes::from_static(b"1")),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "SET with TTL failed");

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Test GET after TTL expiry
        let resp = send_request(&mut stream, 0x21, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x02, "Key should have expired (NotFound)");
    }

    #[tokio::test]
    async fn test_delete_key() {
        let addr = "127.0.0.1:8893";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let key = b"delete_key";
        let value = b"to_be_deleted";

        // Test SET
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "SET operation failed");

        // Test DELETE
        let resp = send_request(&mut stream, 0x22, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "DELETE operation failed");

        // Test GET after DELETE
        let resp = send_request(&mut stream, 0x21, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x02, "Key should not exist after DELETE");
    }

    #[tokio::test]
    async fn test_ping_pong() {
        let addr = "127.0.0.1:8895";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        // Sende PING (Event 0x01) ohne TLVs
        let ping_msg = vec![0x00, 0x00, 0x00, 0x01, 0x01];
        stream.write_all(&ping_msg).await.unwrap();

        // Lese Antwort
        let mut resp = [0u8; 16];
        let n = stream.read(&mut resp).await.unwrap();
        assert!(n >= 5, "Response too short");
        assert_eq!(resp[4], 0x01, "Ping-Pong failed (STATUS ungleich OK)");
    }

    #[tokio::test]
    async fn test_group_query() {
        let addr = "127.0.0.1:8896";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let group = b"group1";
        let key1 = b"group_key1";
        let value1 = b"value1";
        let key2 = b"group_key2";
        let value2 = b"value2";

        // Test SET für key1
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key1)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value1)),
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "SET operation for key1 failed");

        // Test SET für key2
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key2)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value2)),
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "SET operation for key2 failed");

        // Test GET für die Gruppe
        let resp = send_request(&mut stream, 0x21, vec![
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "GET operation for group failed");
        assert!(resp.len() > 5, "Response should contain TLV fields for the group");

        // Überprüfen der TLV-Felder
        let mut response_tlvs = &resp[5..];
        let mut found_key1 = false;
        let mut found_key2 = false;

        while !response_tlvs.is_empty() {
            let mut response_bytes = Bytes::copy_from_slice(response_tlvs);
            if let Some(tlv) = TlvField::decode(&mut response_bytes) {
                response_tlvs = &response_tlvs[(1 + 4 + tlv.value.len())..]; // Typ (1 Byte) + Länge (4 Bytes) + Wert
                match tlv.type_id {
                    TlvFieldTypes::KEY => {
                        if tlv.value.as_ref() == key1 {
                            found_key1 = true;
                        } else if tlv.value.as_ref() == key2 {
                            found_key2 = true;
                        }
                    }
                    TlvFieldTypes::VALUE => {
                        if tlv.value.as_ref() == value1 {
                            assert!(found_key1, "Value1 received before Key1");
                        } else if tlv.value.as_ref() == value2 {
                            assert!(found_key2, "Value2 received before Key2");
                        }
                    }
                    _ => {}
                }
            } else {
                panic!("Failed to decode TLV");
            }
        }

        assert!(found_key1, "Key1 not found in group response");
        assert!(found_key2, "Key2 not found in group response");
    }

    #[tokio::test]
    async fn test_info() {
        let addr = "127.0.0.1:8901";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let key = b"info_key";
        let value = b"info_value";

        // Test SET
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "SET operation failed");

        // Test INFO
        let resp = send_request(&mut stream, 0x30, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "INFO operation failed");
        assert!(resp.len() > 5, "Response should contain metadata TLV fields");
    }

    #[tokio::test]
    async fn test_copy_key() {
        let addr = "127.0.0.1:8900";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let key = b"copy_key";
        let value = b"copy_value";
        let new_key = b"new_copy_key";

        // Test SET
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "SET operation failed");

        // Test COPY
        let resp = send_request(&mut stream, 0x25, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::NewKey, Bytes::from_static(new_key)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "COPY operation failed");

        // Test GET for new key
        let resp = send_request(&mut stream, 0x21, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(new_key)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "GET operation for new key failed");
        assert!(resp.len() > 5, "Response should contain TLV fields for the new key");
    }

    #[tokio::test]
    async fn test_touch() {
        let addr = "127.0.0.1:8899";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let key = b"touch_key";
        let value = b"touch_value";

        // Test SET with TTL
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
            TlvField::new(TlvFieldTypes::TTL, Bytes::from_static(b"2")),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "SET with TTL failed");

        // Test TOUCH
        let resp = send_request(&mut stream, 0x24, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "TOUCH operation failed");

        // Warte weniger als die neue TTL (z.B. 1 Sekunde)
        tokio::time::sleep(Duration::from_secs(1)).await;

        let resp = send_request(&mut stream, 0x21, vec![ 
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "Key should still exist after TOUCH");
    }

    #[tokio::test]
    async fn test_touch_without_ttl() {
        let addr = "127.0.0.1:8894";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let key = b"no_ttl_key";
        let value = b"no_ttl_value";

        // SET ohne TTL
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "SET without TTL failed");

        // TOUCH sollte fehlschlagen
        let resp = send_request(&mut stream, 0x24, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x03, "TOUCH without TTL should fail (INVALID_INPUT)");
    }

    #[tokio::test]
    async fn test_flush() {
        let addr = "127.0.0.1:8898";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let key = b"flush_key";
        let value = b"flush_value";

        // Test SET
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "SET operation failed");

        // Test FLUSH
        let resp = send_request(&mut stream, 0x23, vec![]).await;
        assert_eq!(extract_status(&resp), 0x01, "FLUSH operation failed");

        // Test GET after FLUSH
        let resp = send_request(&mut stream, 0x21, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x02, "Key should not exist after FLUSH");
    }

    #[tokio::test]
    async fn test_exists_key() {
        let addr = "127.0.0.1:8897";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let key = b"exists_key";
        let value = b"exists_value";

        // Test SET
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "SET operation failed");

        // Test EXISTS
        let resp = send_request(&mut stream, 0x10, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ])
        .await;

        assert_eq!(extract_status(&resp), 0x01, "EXISTS operation failed");
    }

    #[tokio::test]
    async fn test_overwrite_existing_key() {
        let addr = "127.0.0.1:8910";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let key = b"overwrite_key";
        let value1 = b"value1";
        let value2 = b"value2";

        // SET initial
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value1)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01, "Initial SET failed");

        // SET overwrite
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value2)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01, "Overwrite SET failed");

        // GET and check value
        let resp = send_request(&mut stream, 0x21, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert!(resp.contains(&value2[0]), "Value was not overwritten");
    }

    #[tokio::test]
    async fn test_delete_nonexistent_key() {
        let addr = "127.0.0.1:8911";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let key = b"not_existing_key";

        // DELETE non-existent key
        let resp = send_request(&mut stream, 0x22, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        // Je nach Implementierung: 0x01 (OK) oder 0x02 (NOT_FOUND) ist akzeptabel
        assert!(extract_status(&resp) == 0x01 || extract_status(&resp) == 0x02, "DELETE on non-existent key should not panic");
    }

    #[tokio::test]
    async fn test_exists_group() {
        let addr = "127.0.0.1:8912";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let group = b"exists_group";
        let key = b"grouped_key";
        let value = b"grouped_value";

        // SET with group
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01, "SET with group failed");

        // EXISTS for group
        let resp = send_request(&mut stream, 0x10, vec![
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01, "EXISTS for group failed");
    }

    #[tokio::test]
    async fn test_info_nonexistent_key() {
        let addr = "127.0.0.1:8913";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let key = b"no_info_key";

        // INFO for non-existent key
        let resp = send_request(&mut stream, 0x30, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x02, "INFO for non-existent key should return NOT_FOUND");
    }

    #[tokio::test]
    async fn test_delete_group() {
        let addr = "127.0.0.1:8914";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let group = b"del_group";
        let key1 = b"del_group_key1";
        let key2 = b"del_group_key2";
        let value = b"val";

        // SET two keys in group
        for key in &[key1, key2] {
            let resp = send_request(&mut stream, 0x20, vec![
                TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(*key)),
                TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
                TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
            ]).await;
            assert_eq!(extract_status(&resp), 0x01, "SET in group failed");
        }

        // DELETE group
        let resp = send_request(&mut stream, 0x22, vec![
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01, "DELETE group failed");

        // GET for both keys should fail
        for key in &[key1, key2] {
            let resp = send_request(&mut stream, 0x21, vec![
                TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(*key)),
            ]).await;
            assert_eq!(extract_status(&resp), 0x02, "Key should not exist after group DELETE");
        }
    }

    #[tokio::test]
    async fn test_flush_removes_groups() {
        let addr = "127.0.0.1:8915";
        setup_server(addr).await;

        let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to server");

        let group = b"flush_group";
        let key = b"flush_group_key";
        let value = b"flush_group_value";

        // SET with group
        let resp = send_request(&mut stream, 0x20, vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from_static(key)),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(value)),
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x01, "SET with group failed");

        // FLUSH
        let resp = send_request(&mut stream, 0x23, vec![]).await;
        assert_eq!(extract_status(&resp), 0x01, "FLUSH failed");

        // EXISTS for group should fail
        let resp = send_request(&mut stream, 0x10, vec![
            TlvField::new(TlvFieldTypes::GROUP, Bytes::from_static(group)),
        ]).await;
        assert_eq!(extract_status(&resp), 0x02, "Group should not exist after FLUSH");
    }
}
