use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use bytes::Bytes;
use tokio::sync::RwLock;
use tokio::time::sleep;
use log::debug;

use crate::config::config::StorageConfig;

#[derive(Clone)]
pub struct Entry {
    pub value: Bytes,
    pub group: Option<Bytes>,
    pub expires_at: Option<Instant>,
    pub compressed: bool,
    pub ttl: u32,
}

pub struct RamStore {
    data: RwLock<HashMap<Bytes, Entry>>,
    group_index: RwLock<HashMap<Bytes, Vec<Bytes>>>,
    access_order: RwLock<VecDeque<Bytes>>,
    max_bytes: usize,
    ttl_checktime : u64
}

impl RamStore {
    pub fn new(config: StorageConfig) -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            group_index: RwLock::new(HashMap::new()),
            access_order: RwLock::new(VecDeque::new()),
            max_bytes: config.max_ram_size,
            ttl_checktime: config.ttl_checktime
        }
    }

    pub async fn set(&self, key: Bytes, entry: Entry) {
        debug!("[RAM.set] key: {:?}, expires_at: {:?}", key, entry.expires_at);

        let mut data = self.data.write().await;
        let mut order = self.access_order.write().await;

        let new_entry_size = entry.value.len();
        let mut current_size: usize = data.values().map(|e| e.value.len()).sum();

        while current_size + new_entry_size > self.max_bytes {
            if let Some(oldest_key) = order.pop_front() {
                if let Some(removed) = data.remove(&oldest_key) {
                    current_size -= removed.value.len();
                    debug!("[RAM.evict] Entferne Key: {:?}", oldest_key);

                    if let Some(group) = removed.group {
                        let mut groups = self.group_index.write().await;
                        if let Some(vec) = groups.get_mut(&group) {
                            vec.retain(|k| k != &oldest_key);
                            if vec.is_empty() {
                                groups.remove(&group);
                            }
                        }
                    }
                }
            } else {
                break;
            }
        }

        if let Some(pos) = order.iter().position(|k| k == &key) {
            order.remove(pos);
        }
        order.push_back(key.clone());

        if let Some(group) = &entry.group {
            let mut groups = self.group_index.write().await;
            groups.entry(group.clone()).or_default().push(key.clone());
        }

        data.insert(key, entry);
    }

    pub async fn get(&self, key: &[u8]) -> Option<Entry> {
        let key = Bytes::copy_from_slice(key);
        let mut data = self.data.write().await;
        let mut order = self.access_order.write().await;

        if let Some(entry) = data.get(&key) {
            if entry.expires_at.map(|t| t > Instant::now()).unwrap_or(true) {
                if let Some(pos) = order.iter().position(|k| k == &key) {
                    let key = order.remove(pos).unwrap();
                    order.push_back(key);
                }
                return Some(entry.clone());
            } else {
                data.remove(&key);
                if let Some(pos) = order.iter().position(|k| k == &key) {
                    order.remove(pos);
                }
            }
        }

        None
    }

    pub async fn delete(&self, key: &[u8]) {
        let key = Bytes::copy_from_slice(key);
        let mut data = self.data.write().await;
        let mut order = self.access_order.write().await;

        if let Some(entry) = data.remove(&key) {
            order.retain(|k| k != &key);

            if let Some(group) = entry.group {
                let mut groups = self.group_index.write().await;
                if let Some(vec) = groups.get_mut(&group) {
                    vec.retain(|k| k != &key);
                    if vec.is_empty() {
                        groups.remove(&group);
                    }
                }
            }
        }
    }

    pub async fn delete_by_group(&self, group: &[u8]) {
        let group_key = Bytes::copy_from_slice(group);
        let mut group_index = self.group_index.write().await;
        let mut data = self.data.write().await;
        let mut order = self.access_order.write().await;

        if let Some(keys) = group_index.remove(&group_key) {
            for key in keys {
                data.remove(&key);
                order.retain(|k| k != &key);
            }
        }
    }

    pub async fn get_by_group(&self, group: &[u8]) -> Option<Vec<Entry>> {
        let group_key = Bytes::copy_from_slice(group);
        let group_index = self.group_index.read().await;
        let data = self.data.read().await;

        let result: Vec<Entry> = group_index
            .get(&group_key)
            .into_iter()
            .flat_map(|keys| keys.iter())
            .filter_map(|key| data.get(key).cloned())
            .collect();

        (!result.is_empty()).then_some(result)
    }

    pub async fn flush(&self) {
        let mut data = self.data.write().await;
        let mut group_index = self.group_index.write().await;
        let mut order = self.access_order.write().await;

        debug!("[RAM.flush] Alle Einträge entfernt");

        data.clear();
        group_index.clear();
        order.clear();
    }

    pub async fn count(&self) -> usize {
        self.data.read().await.len()
    }

    pub async fn total_size(&self) -> usize {
        self.data.read().await.values().map(|entry| entry.value.len()).sum()
    }

    pub async fn group_count(&self) -> usize {
        self.group_index.read().await.len()
    }

    pub async fn start_ttl_cleaner(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(self.ttl_checktime)).await;

                let now = Instant::now();
                let mut data = self.data.write().await;
                let mut order = self.access_order.write().await;
                let mut groups = self.group_index.write().await;

                let expired_keys: Vec<_> = data
                    .iter()
                    .filter_map(|(key, entry)| {
                        if entry.expires_at.map(|t| t <= now).unwrap_or(false) {
                            Some(key.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                for key in expired_keys {
                    if let Some(entry) = data.remove(&key) {
                        if let Some(group) = entry.group {
                            if let Some(vec) = groups.get_mut(&group) {
                                vec.retain(|k| k != &key);
                                if vec.is_empty() {
                                    groups.remove(&group);
                                }
                            }
                        }
                    }
                    order.retain(|k| k != &key);
                }
            }
        });
    }
}