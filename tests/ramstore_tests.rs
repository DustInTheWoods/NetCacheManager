use netcache_manager::{config::config::StorageConfig, storage::ram_handler::{Entry, RamStore}};
use bytes::Bytes;
use std::{sync::Arc, time::{Duration, Instant}};
use tokio::time::sleep;

#[tokio::test]
async fn test_basic() {
    let config = StorageConfig {
        max_ram_size: 104857600,
        ttl_checktime: 5,
    };

    let store = Arc::new(RamStore::new(config));  

    let key = Bytes::copy_from_slice(b"test_key");
    let value = Bytes::copy_from_slice(b"test_value");

    let entry = Entry {
        value: value.clone(),
        group: None,
        expires_at: None,
        compressed: false,
        ttl: 0,
    };

    store.set(key.clone(), entry).await;

    let res = store.get(&key).await;
    assert!(res.is_some());
    assert_eq!(res.unwrap().value, value);
}

#[tokio::test]
async fn test_group() {
    let config = StorageConfig {
        max_ram_size: 104857600,
        ttl_checktime: 5,
    };

    let store = Arc::new(RamStore::new(config));  

    let group = Bytes::copy_from_slice(b"test_group");

    let entries: Vec<(Bytes, Bytes)> = vec![
        (Bytes::copy_from_slice(b"key1"), Bytes::from("val1")),
        (Bytes::copy_from_slice(b"key2"), Bytes::from("val2")),
        (Bytes::copy_from_slice(b"key3"), Bytes::from("val3")),
    ];

    for (key, value) in &entries {
        let entry = Entry {
            value: value.clone(),
            group: Some(group.clone()),
            expires_at: None,
            compressed: false,
            ttl: 0,
        };
        store.set(key.clone(), entry).await;
    }

    for (key, value) in &entries {
        let res = store.get(key).await;
        assert!(res.is_some());
        assert_eq!(res.unwrap().value, *value);
    }

    let group_result = store.get_by_group(&group).await;
    assert!(group_result.is_some());

    let result_entries = group_result.unwrap();
    assert_eq!(result_entries.len(), 3);

    let mut values: Vec<_> = result_entries.into_iter().map(|e| e.value).collect();
    values.sort();
    let mut expected: Vec<_> = entries.iter().map(|(_, v)| v.clone()).collect();
    expected.sort();

    assert_eq!(values, expected);
}

#[tokio::test]
async fn test_ttl() {
    let config = StorageConfig {
        max_ram_size: 104857600,
        ttl_checktime: 5,
    };

    let store = Arc::new(RamStore::new(config));  

    let key = Bytes::copy_from_slice(b"hello");
    let value = Bytes::from("world");

    let entry = Entry {
        value: value.clone(),
        group: Some(Bytes::copy_from_slice(b"group1")),
        expires_at: Some(Instant::now() + Duration::from_secs(1)),
        compressed: false,
        ttl: 1,
    };

    store.set(key.clone(), entry.clone()).await;

    let res = store.get(&key).await;
    assert!(res.is_some());
    assert_eq!(res.unwrap().value, value);

    sleep(Duration::from_secs(2)).await;

    let expired = store.get(&key).await;
    assert!(expired.is_none());
}

#[tokio::test]
async fn test_delete() {
    let config = StorageConfig {
        max_ram_size: 104857600,
        ttl_checktime: 5,
    };
    
    let store = Arc::new(RamStore::new(config)); 

    let key = Bytes::copy_from_slice(b"delete_me");
    let value = Bytes::from("to be deleted");

    let entry = Entry {
        value: value.clone(),
        group: None,
        expires_at: None,
        compressed: false,
        ttl: 0,
    };

    store.set(key.clone(), entry).await;
    assert!(store.get(&key).await.is_some());

    store.delete(&key).await;
    assert!(store.get(&key).await.is_none());
}

#[tokio::test]
async fn test_flush() {
    let config = StorageConfig {
        max_ram_size: 104857600,
        ttl_checktime: 5,
        };
    
    let store = Arc::new(RamStore::new(config));  
    
    for i in 0..10 {
        let key = Bytes::copy_from_slice(format!("key{}", i).as_bytes());
        let value = Bytes::from(format!("value{}", i));
        let entry = Entry {
            value,
            group: None,
            expires_at: None,
            compressed: false,
            ttl: 0,
        };
        store.set(key.clone(), entry).await;
    }

    for i in 0..10 {
        let key = Bytes::copy_from_slice(format!("key{}", i).as_bytes());
        assert!(store.get(&key).await.is_some());
    }

    store.flush().await;

    for i in 0..10 {
        let key = Bytes::copy_from_slice(format!("key{}", i).as_bytes());
        assert!(store.get(&key).await.is_none());
    }
}

#[tokio::test]
async fn test_count() {
    let config = StorageConfig {
        max_ram_size: 104857600,
        ttl_checktime: 5,
        };
    
    let store = Arc::new(RamStore::new(config));  
    let group = Bytes::copy_from_slice(b"test_group");

    for i in 0..5 {
        let key = Bytes::copy_from_slice(format!("key{}", i).as_bytes());
        let value = Bytes::from(format!("value{}", i));
        let entry = Entry {
            value,
            group: Some(group.clone()),
            expires_at: None,
            compressed: false,
            ttl: 0,
        };
        store.set(key.clone(), entry).await;
    }

    assert_eq!(store.count().await, 5);

    let key_to_delete = Bytes::copy_from_slice(b"key2");
    store.delete(&key_to_delete).await;
    assert_eq!(store.count().await, 4);

    store.delete_group(&group).await;
    assert_eq!(store.count().await, 0);
}

#[tokio::test]
async fn test_ram_exceeded() {    
    let config = StorageConfig {
        max_ram_size: 1048576, // 1MB
        ttl_checktime: 5,
    };

    let store = Arc::new(RamStore::new(config)); 

    let value = Bytes::from(vec![1u8; 20]);

    for i in 0..10 {
        let key = Bytes::copy_from_slice(format!("key{}", i).as_bytes());
        let entry = Entry {
            value: value.clone(),
            group: None,
            expires_at: None,
            compressed: false,
            ttl: 0,
        };
        store.set(key.clone(), entry).await;
    }

    let total_size = store.total_size().await;
    println!("RAM total size: {} bytes", total_size);
    assert!(total_size <= 300, "RAM usage sollte max 300 bytes sein");
}

#[tokio::test]
async fn test_performance() {
    let config = StorageConfig {
        max_ram_size: 10485760,
        ttl_checktime: 5,
    };

    let store = Arc::new(RamStore::new(config)); 

    let value = Bytes::from(vec![0u8; 100]);
    let keys: Vec<Bytes> = (0..1000)
        .map(|i| Bytes::copy_from_slice(format!("key{}", i).as_bytes()))
        .collect();

    let start_set = Instant::now();
    for key in &keys {
        let entry = Entry {
            value: value.clone(),
            group: None,
            expires_at: None,
            compressed: false,
            ttl: 0,
        };
        store.set(key.clone(), entry).await;
    }
    let duration_set = start_set.elapsed();

    let start_get = Instant::now();
    for key in &keys {
        let result = store.get(key).await;
        assert!(result.is_some(), "Key {:?} nicht gefunden", key);
    }
    let duration_get = start_get.elapsed();

    println!(
        "Performance Test (direct ram read write): 1000 SETs in {:?}, 1000 GETs in {:?}",
        duration_set, duration_get
    );
}
