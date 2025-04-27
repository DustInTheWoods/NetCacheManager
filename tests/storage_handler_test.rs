use netcache_manager::protocol::tlv::{TlvField, TlvFieldTypes};
use netcache_manager::storage::storage_handler::*;
use netcache_manager::storage::ram_handler::RamStore;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

mod tests {
    use netcache_manager::config::config::StorageConfig;

    use super::*;

    #[tokio::test]
    async fn test_basic() {
        let config = StorageConfig {
            max_ram_size: 104857600,
            ttl_checktime: 5,
        };
    
        let store = Arc::new(RamStore::new(config));
        handle_flush(&store).await.expect("Flush sollte funktionieren");

        let key = Bytes::from_static(b"test_key");
        let value = Bytes::from_static(b"test_value");

        handle_set(vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from(key.clone())),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from(value.clone())),
        ], &store).await.expect("SET sollte STATUS_OK liefern");

        handle_get(vec![
            TlvField::new(TlvFieldTypes::KEY, key.clone()),
        ], &store).await.expect("GET sollte STATUS_OK liefern");
    }

    #[tokio::test]
    async fn test_group() {
        let config = StorageConfig {
            max_ram_size: 104857600,
            ttl_checktime: 5,
        };
    
        let store = Arc::new(RamStore::new(config));
        handle_flush(&store).await.expect("Flush sollte funktionieren");

        let group = Bytes::from_static(b"test_group");

        for (k, v) in [("key1", "val1"), ("key2", "val2"), ("key3", "val3")] {
            handle_set(vec![
                TlvField::new(TlvFieldTypes::KEY, Bytes::from(k)),
                TlvField::new(TlvFieldTypes::VALUE, Bytes::from(v)),
                TlvField::new(TlvFieldTypes::GROUP, Bytes::from(group.clone())),
            ], &store).await.expect(&format!("SET für {} sollte STATUS_OK liefern", k));
        }
    }

    #[tokio::test]
    async fn test_ttl() {
        let config = StorageConfig {
            max_ram_size: 104857600,
            ttl_checktime: 5,
        };
    
        let store = Arc::new(RamStore::new(config));
        handle_flush(&store).await.expect("Flush sollte funktionieren");

        let key = Bytes::from_static(b"ttl_test_key");
        let value = Bytes::from_static(b"ttl_test_value");

        handle_set(vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from(key.clone())),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from(value)),
            TlvField::new(TlvFieldTypes::TTL, Bytes::from_static(b"1")),
        ], &store).await.expect("SET sollte STATUS_OK liefern");

        sleep(Duration::from_secs(2)).await;

        let result = handle_get(vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from(key.clone())),
        ], &store).await;

        assert!(matches!(result, Err(StorageError::NotFound)), "GET nach Ablauf sollte NOT_FOUND liefern");
    }

    #[tokio::test]
    async fn test_delete() {
        let config = StorageConfig {
            max_ram_size: 104857600,
            ttl_checktime: 5,
        };
    
        let store = Arc::new(RamStore::new(config));
        handle_flush(&store).await.expect("Flush sollte funktionieren");

        let key = Bytes::from_static(b"delete_test_key");
        let value = Bytes::from_static(b"delete_test_value");

        handle_set(vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from(key.clone())),
            TlvField::new(TlvFieldTypes::VALUE, Bytes::from(value)),
        ], &store).await.expect("SET sollte STATUS_OK liefern");

        handle_delete(vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from(key.clone())),
        ], &store).await.expect("DELETE sollte STATUS_OK liefern");

        let result = handle_get(vec![
            TlvField::new(TlvFieldTypes::KEY, Bytes::from(key.clone())),
        ], &store).await;

        assert!(matches!(result, Err(StorageError::NotFound)), "GET nach Delete sollte NOT_FOUND liefern");
    }

    #[tokio::test]
    async fn test_flush() {
        let config = StorageConfig {
            max_ram_size: 104857600,
            ttl_checktime: 5,
        };
    
        let store = Arc::new(RamStore::new(config));
        handle_flush(&store).await.expect("Flush sollte funktionieren");

        for i in 0..5 {
            handle_set(vec![
                TlvField::new(TlvFieldTypes::KEY, Bytes::from(format!("flush_key{}", i))),
                TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(b"dummy")),
            ], &store).await.expect("SET sollte STATUS_OK liefern");
        }

        handle_flush(&store).await.expect("Flush sollte STATUS_OK liefern");
        handle_sysinfo(&store).await.expect("SYSINFO sollte STATUS_OK liefern");
    }

    #[tokio::test]
    async fn test_count() {
        let config = StorageConfig {
            max_ram_size: 104857600,
            ttl_checktime: 5,
        };
    
        let store = Arc::new(RamStore::new(config));
        handle_flush(&store).await.expect("Flush sollte funktionieren");

        for i in 0..3 {
            handle_set(vec![
                TlvField::new(TlvFieldTypes::KEY, Bytes::from(format!("count_key{}", i))),
                TlvField::new(TlvFieldTypes::VALUE, Bytes::from_static(b"dummy")),
            ], &store).await.expect("SET sollte STATUS_OK liefern");
        }

        handle_sysinfo(&store).await.expect("SYSINFO sollte STATUS_OK liefern");
    }

    #[tokio::test]
    async fn test_ram_exceeded() {
        let config = StorageConfig {
            max_ram_size: 1048576,
            ttl_checktime: 5,
        };
    
        let store = Arc::new(RamStore::new(config));
        handle_flush(&store).await.expect("Flush sollte funktionieren");

        let big_value = vec![0u8; 512 * 1024];
        for i in 0..5 {
            handle_set(vec![
                TlvField::new(TlvFieldTypes::KEY, Bytes::from(format!("ramkey{}", i))),
                TlvField::new(TlvFieldTypes::VALUE, Bytes::from(big_value.clone())),
            ], &store).await.expect("SET sollte STATUS_OK liefern");
        }

        handle_sysinfo(&store).await.expect("SYSINFO sollte STATUS_OK liefern");
    }

    #[tokio::test]
    async fn test_performance() {
        let config = StorageConfig {
            max_ram_size: 104857600,
            ttl_checktime: 5,
        };
    
        let store = Arc::new(RamStore::new(config));
        handle_flush(&store).await.expect("Flush sollte funktionieren");
    
        let value = Bytes::from(vec![1u8; 100]);
        let keys: Vec<String> = (0..1000)
            .map(|i| format!("perfkey{}", i))
            .collect();
    
        let start_set = Instant::now();
        for k in &keys {
            handle_set(vec![
                TlvField::new(TlvFieldTypes::KEY, Bytes::from(k.clone())),
                TlvField::new(TlvFieldTypes::VALUE, value.clone()),
            ], &store).await.expect("SET sollte STATUS_OK liefern");
        }
        let duration_set = start_set.elapsed();
    
        let start_get = Instant::now();
        for k in &keys {
            handle_get(vec![
                TlvField::new(TlvFieldTypes::KEY, Bytes::from(k.clone())),
            ], &store).await.expect("GET sollte STATUS_OK liefern");
        }
        let duration_get = start_get.elapsed();
    
        println!(
            "Performance Test (via storage handler): 1000 SETs in {:?}, 1000 GETs in {:?}",
            duration_set, duration_get
        );
    }
    
}
