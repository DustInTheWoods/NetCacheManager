use dashmap::DashMap;
use indexmap::IndexSet;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering, AtomicU64}};
use std::time::{Duration, Instant};
use bytes::Bytes;
use tokio::time::sleep;
use log::{debug, warn};

use crate::config::config::StorageConfig;

#[derive(Clone)]
pub struct Entry {
    pub key : Bytes, // Bytes für den Schlüssel
    pub value: Bytes, // Bytes für den Wert
    pub group: Option<Bytes>, // Option<Bytes> für Gruppen
    pub expires_at: Option<Instant>, // Option<Instant> für TTL
    pub compressed: bool, // true, wenn komprimiert
    pub ttl: u32, // Time-To-Live in Sekunden
    pub timestamp: u128, // Timestamp in Millisekunden
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
        debug!("[RamStore::new] Initialisiere RAM-Store mit max_ram_size={} Bytes, ttl_checktime={}s", config.max_ram_size, config.ttl_checktime);
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
        debug!("[RamStore::set] Setze Key: {:?} ({} Bytes), Gruppe: {:?}, TTL: {}, Komprimiert: {}", key, entry.value.len(), entry.group, entry.ttl, entry.compressed);
        let new_entry_size = entry.value.len();

        // LRU-Eviction
        while self.size.load(Ordering::SeqCst) + new_entry_size > self.max_bytes {
            if let Some(oldest) = self.access_order.iter().min_by_key(|e| *e.key()) {
                let oldest_counter = *oldest.key();
                let oldest_key = oldest.value().clone();
                debug!("[RamStore::set] LRU-Eviction: Entferne ältesten Key: {:?} (Counter={})", oldest_key, oldest_counter);
                self.access_order.remove(&oldest_counter);
                self.lru_counter.remove(&oldest_key);
                if let Some((_, removed)) = self.data.remove(&oldest_key) {
                    self.size.fetch_sub(removed.value.len(), Ordering::SeqCst);
                    if let Some(group) = removed.group {
                        if let Some(mut set) = self.group_index.get_mut(&group) {
                            set.remove(&oldest_key);
                            if set.is_empty() {
                                self.group_index.remove(&group);
                                debug!("[RamStore::set] Gruppe nach LRU-Eviction entfernt: {:?}", group);
                            }
                        }
                    }
                }
            } else {
                warn!("[RamStore::set] LRU-Eviction: Kein weiterer Eintrag zum Entfernen gefunden!");
                break;
            }
        }

        // LRU-Update: alten Counter entfernen, falls vorhanden
        if let Some(old_counter) = self.lru_counter.remove(&key) {
            debug!("[RamStore::set] Entferne alten LRU-Counter für Key: {:?} (Counter={})", key, old_counter.1);
            self.access_order.remove(&old_counter.1);
        }
        let counter = self.counter.fetch_add(1, Ordering::SeqCst);
        self.lru_counter.insert(key.clone(), counter);
        self.access_order.insert(counter, key.clone());

        // Group-Index
        if let Some(group) = &entry.group {
            debug!("[RamStore::set] Füge Key {:?} zur Gruppe {:?} hinzu", key, group);
            self.group_index.entry(group.clone()).or_insert_with(IndexSet::new).insert(key.clone());
        }

        // Size-Update
        if let Some(old) = self.data.insert(key.clone(), entry) {
            debug!("[RamStore::set] Überschreibe bestehenden Key: {:?} (alte Größe: {} Bytes)", key, old.value.len());
            self.size.fetch_sub(old.value.len(), Ordering::SeqCst);
        }
        self.size.fetch_add(new_entry_size, Ordering::SeqCst);
        debug!("[RamStore::set] Neuer RAM-Store-Size: {} Bytes", self.size.load(Ordering::SeqCst));
    }

    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        let key = Bytes::copy_from_slice(key);
        debug!("[RamStore::get] Suche Key: {:?}", key);

        if let Some(entry) = self.data.get(&key) {
            if entry.expires_at.map(|t| t > Instant::now()).unwrap_or(true) {
                debug!("[RamStore::get] Key gefunden: {:?} ({} Bytes)", key, entry.value.len());
                // LRU-Update: alten Counter entfernen, falls vorhanden
                if let Some(old_counter) = self.lru_counter.remove(&key) {
                    debug!("[RamStore::get] Entferne alten LRU-Counter für Key: {:?} (Counter={})", key, old_counter.1);
                    self.access_order.remove(&old_counter.1);
                }
                let counter = self.counter.fetch_add(1, Ordering::SeqCst);
                self.lru_counter.insert(key.clone(), counter);
                self.access_order.insert(counter, key.clone());
                return Some(entry.clone());
            } else {
                debug!("[RamStore::get] Key abgelaufen: {:?}", key);
            }
        } else {
            debug!("[RamStore::get] Key nicht gefunden: {:?}", key);
        }
        None
    }

    pub fn delete(&self, key: &[u8]) {
        let key = Bytes::copy_from_slice(key);
        debug!("[RamStore::delete] Lösche Key: {:?}", key);

        if let Some((_, entry)) = self.data.remove(&key) {
            debug!("[RamStore::delete] Key entfernt, Größe: {} Bytes", entry.value.len());
            self.size.fetch_sub(entry.value.len(), Ordering::SeqCst);
            if let Some(old_counter) = self.lru_counter.remove(&key) {
                debug!("[RamStore::delete] Entferne LRU-Counter: {}", old_counter.1);
                self.access_order.remove(&old_counter.1);
            }
            if let Some(group) = entry.group {
                if let Some(mut set) = self.group_index.get_mut(&group) {
                    set.swap_remove(&key);
                    if set.is_empty() {
                        self.group_index.remove(&group);
                        debug!("[RamStore::delete] Gruppe nach Löschen entfernt: {:?}", group);
                    }
                }
            }
        } else {
            debug!("[RamStore::delete] Key nicht vorhanden: {:?}", key);
        }
    }

    pub fn delete_by_group(&self, group: &[u8]) {
        let group_key = Bytes::copy_from_slice(group);
        debug!("[RamStore::delete_by_group] Lösche Gruppe: {:?}", group_key);

        if let Some((_, keys)) = self.group_index.remove(&group_key) {
            debug!("[RamStore::delete_by_group] {} Keys in Gruppe werden gelöscht", keys.len());
            for key in keys {
                self.delete(&key);
            }
        } else {
            debug!("[RamStore::delete_by_group] Gruppe nicht gefunden: {:?}", group_key);
        }
    }

    pub fn get_by_group(&self, group: &[u8]) -> Option<Vec<Entry>> {
        let group_key = Bytes::copy_from_slice(group);
        debug!("[RamStore::get_by_group] Suche Gruppe: {:?}", group_key);

        let result: Vec<Entry> = self.group_index
            .get(&group_key)
            .map(|set| set.iter().filter_map(|key| self.data.get(key).map(|entry| entry.clone())).collect())
            .unwrap_or_default();

        debug!("[RamStore::get_by_group] {} Einträge gefunden", result.len());
        (!result.is_empty()).then_some(result)
    }

    pub fn flush(&self) {
        debug!("[RamStore::flush] Entferne alle Einträge aus RAM-Store!");
        self.data.clear();
        self.group_index.clear();
        self.access_order.clear();
        self.lru_counter.clear();
        self.size.store(0, Ordering::SeqCst);

        debug!("[RAM.flush] Alle Einträge entfernt");
    }

    pub fn count(&self) -> usize {
        let count = self.data.len();
        debug!("[RamStore::count] Aktuelle Key-Anzahl: {}", count);
        count
    }

    pub fn total_size(&self) -> usize {
        let size = self.size.load(Ordering::SeqCst);
        debug!("[RamStore::total_size] Aktuelle RAM-Größe: {} Bytes", size);
        size
    }

    pub fn group_count(&self) -> usize {
        let count = self.group_index.len();
        debug!("[RamStore::group_count] Aktuelle Gruppen-Anzahl: {}", count);
        count
    }

    pub fn all_entries(&self) -> Vec<(Bytes, Entry)> {
        self.data.iter().map(|e| (e.key().clone(), e.value().clone())).collect()
    }

    pub fn start_ttl_cleaner(self: Arc<Self>) {
        let this = self.clone();
        debug!("[RamStore::start_ttl_cleaner] Starte TTL-Cleaner mit Intervall {}s", this.ttl_checktime);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(this.ttl_checktime)).await;
                let now = Instant::now();
                let mut expired = Vec::new();
                for item in this.data.iter() {
                    if item.value().expires_at.map(|t| t <= now).unwrap_or(false) {
                        debug!("[TTL-Cleaner] Key abgelaufen: {:?}", item.key());
                        expired.push(item.key().clone());
                    }
                }
                for key in expired {
                    debug!("[TTL-Cleaner] Entferne abgelaufenen Key: {:?}", key);
                    this.delete(&key);
                }
                //debug!("[TTL-Cleaner] Durchlauf beendet. Nächster Check in {}s", this.ttl_checktime);
            }
        });
    }

    pub fn touch(&self, key: &[u8]) -> Result<(), ()> {
        let key = Bytes::copy_from_slice(key);
        if let Some(mut entry) = self.data.get(&key) {
            if entry.ttl == 0 {
                warn!("[RamStore::touch] Key hat keine TTL: {:?}", key);
                return Err(());
            }
            let mut entry = entry.clone();
            entry.expires_at = Some(Instant::now() + Duration::from_secs(entry.ttl as u64));
            self.set(key, entry);
            Ok(())
        } else {
            Err(())
        }
    }
}