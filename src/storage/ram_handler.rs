use dashmap::DashMap;
use indexmap::IndexSet;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering, AtomicU64}};
use std::time::{Duration, Instant};
use bytes::Bytes;
use tokio::time::sleep;
use log::debug;

use crate::config::config::StorageConfig;

#[derive(Clone)]
pub struct Entry {
    pub key : Bytes,
    pub value: Bytes,
    pub group: Option<Bytes>,
    pub expires_at: Option<Instant>,
    pub compressed: bool,
    pub ttl: u32,
}

pub struct RamStore {
    data: DashMap<Bytes, Entry>,
    group_index: DashMap<Bytes, IndexSet<Bytes>>,
    access_order: DashMap<u64, Bytes>, // LRU als Map: Counter -> Key
    lru_counter: DashMap<Bytes, u64>,  // Key -> Counter
    max_bytes: usize,
    ttl_checktime : u64,
    counter: AtomicU64,
    size: AtomicUsize,
}

impl RamStore {
    pub fn new(config: StorageConfig) -> Self {
        Self {
            data: DashMap::new(),
            group_index: DashMap::new(),
            access_order: DashMap::new(),
            lru_counter: DashMap::new(),
            max_bytes: config.max_ram_size,
            ttl_checktime: config.ttl_checktime,
            counter: AtomicU64::new(0),
            size: AtomicUsize::new(0),
        }
    }

    pub fn set(&self, key: Bytes, entry: Entry) {
        let new_entry_size = entry.value.len();

        // LRU-Eviction
        while self.size.load(Ordering::SeqCst) + new_entry_size > self.max_bytes {
            if let Some(oldest) = self.access_order.iter().min_by_key(|e| *e.key()) {
                let oldest_counter = *oldest.key();
                let oldest_key = oldest.value().clone();
                self.access_order.remove(&oldest_counter);
                self.lru_counter.remove(&oldest_key);
                if let Some((_, removed)) = self.data.remove(&oldest_key) {
                    self.size.fetch_sub(removed.value.len(), Ordering::SeqCst);
                    if let Some(group) = removed.group {
                        if let Some(mut set) = self.group_index.get_mut(&group) {
                            set.remove(&oldest_key);
                            if set.is_empty() {
                                self.group_index.remove(&group);
                            }
                        }
                    }
                }
            } else {
                break;
            }
        }

        // LRU-Update: alten Counter entfernen, falls vorhanden
        if let Some(old_counter) = self.lru_counter.remove(&key) {
            self.access_order.remove(&old_counter.1);
        }
        let counter = self.counter.fetch_add(1, Ordering::SeqCst);
        self.lru_counter.insert(key.clone(), counter);
        self.access_order.insert(counter, key.clone());

        // Group-Index
        if let Some(group) = &entry.group {
            self.group_index.entry(group.clone()).or_insert_with(IndexSet::new).insert(key.clone());
        }

        // Size-Update
        if let Some(old) = self.data.insert(key.clone(), entry) {
            self.size.fetch_sub(old.value.len(), Ordering::SeqCst);
        }
        self.size.fetch_add(new_entry_size, Ordering::SeqCst);
    }

    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        let key = Bytes::copy_from_slice(key);

        if let Some(entry) = self.data.get(&key) {
            if entry.expires_at.map(|t| t > Instant::now()).unwrap_or(true) {
                // LRU-Update: alten Counter entfernen, falls vorhanden
                if let Some(old_counter) = self.lru_counter.remove(&key) {
                    self.access_order.remove(&old_counter.1);
                }
                let counter = self.counter.fetch_add(1, Ordering::SeqCst);
                self.lru_counter.insert(key.clone(), counter);
                self.access_order.insert(counter, key.clone());
                return Some(entry.clone());
            }
        }
        None
    }

    pub fn delete(&self, key: &[u8]) {
        let key = Bytes::copy_from_slice(key);

        if let Some((_, entry)) = self.data.remove(&key) {
            self.size.fetch_sub(entry.value.len(), Ordering::SeqCst);
            if let Some(old_counter) = self.lru_counter.remove(&key) {
                self.access_order.remove(&old_counter.1);
            }
            if let Some(group) = entry.group {
                if let Some(mut set) = self.group_index.get_mut(&group) {
                    set.remove(&key);
                    if set.is_empty() {
                        self.group_index.remove(&group);
                    }
                }
            }
        }
    }

    pub fn delete_by_group(&self, group: &[u8]) {
        let group_key = Bytes::copy_from_slice(group);

        if let Some((_, keys)) = self.group_index.remove(&group_key) {
            for key in keys {
                self.delete(&key);
            }
        }
    }

    pub fn get_by_group(&self, group: &[u8]) -> Option<Vec<Entry>> {
        let group_key = Bytes::copy_from_slice(group);

        let result: Vec<Entry> = self.group_index
            .get(&group_key)
            .map(|set| set.iter().filter_map(|key| self.data.get(key).map(|entry| entry.clone())).collect())
            .unwrap_or_default();

        (!result.is_empty()).then_some(result)
    }

    pub fn flush(&self) {
        self.data.clear();
        self.group_index.clear();
        self.access_order.clear();
        self.lru_counter.clear();
        self.size.store(0, Ordering::SeqCst);

        debug!("[RAM.flush] Alle Einträge entfernt");
    }

    pub fn count(&self) -> usize {
        self.data.len()
    }

    pub fn total_size(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }

    pub fn group_count(&self) -> usize {
        self.group_index.len()
    }

    pub fn start_ttl_cleaner(self: Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(this.ttl_checktime)).await;
                let now = Instant::now();
                let mut expired = Vec::new();
                for item in this.data.iter() {
                    if item.value().expires_at.map(|t| t <= now).unwrap_or(false) {
                        expired.push(item.key().clone());
                    }
                }
                for key in expired {
                    this.delete(&key);
                }
            }
        });
    }
}