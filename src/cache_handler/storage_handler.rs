use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use log::{info, debug, error, warn};
use crate::protocol::tlv::{TlvField, TlvFieldTypes};
use crate::cache_handler::ram_handler::RamStore;
use super::ram_handler::Entry;

#[derive(Debug)]
pub enum StorageError {
    NotFound,
    InvalidInput,
    AlreadyExists,
    InternalError,
}

pub async fn handle_event(event: u8, tlvs: Vec<TlvField>, store: Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    info!("==> [handle_event] Event: 0x{:02X}, TLVs: {:?}, Store: {:?}", event, tlvs, Arc::as_ptr(&store));
    match event {
        0x01 => handle_ping(tlvs).await,
        0x20 => handle_set(tlvs, &store).await,
        0x21 => handle_get(tlvs, &store).await,
        0x22 => handle_delete(tlvs, &store).await,
        0x23 => handle_flush(tlvs,&store).await,
        0x24 => handle_touch(tlvs, &store).await,
        0x10 => handle_exists(tlvs, &store).await,
        0x25 => handle_copy(tlvs, &store).await,
        0x30 => handle_info(tlvs, &store).await,
        0x31 => handle_sysinfo(tlvs, &store).await,
        _ => {
            error!("[handle_event] Unbekanntes Event: 0x{:02X}", event);
            Err(StorageError::InvalidInput)
        },
    }
}

pub fn get_tlv_value(tlvs: &[TlvField], type_id: TlvFieldTypes) -> Option<&TlvField> {
    tlvs.iter().find(|tlv| tlv.type_id == type_id)
}

pub async fn handle_set(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    info!("[handle_set] Starte SET mit TLVs: {:?}", tlvs);
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;
    let key = get_tlv_value(&tlvs, TlvFieldTypes::KEY).ok_or(StorageError::InvalidInput)?;
    let value = get_tlv_value(&tlvs, TlvFieldTypes::VALUE).ok_or(StorageError::InvalidInput)?;
    let group = get_tlv_value(&tlvs, TlvFieldTypes::GROUP);
    let compress = get_tlv_value(&tlvs, TlvFieldTypes::COMPRESS);
    let ttl = get_tlv_value(&tlvs, TlvFieldTypes::TTL);

    let ttl_secs = ttl
        .and_then(|t| String::from_utf8(t.value.to_vec()).ok()?.parse::<u32>().ok())
        .unwrap_or(0);
    let expires_at = if ttl_secs > 0 {
        Some(Instant::now() + Duration::from_secs(ttl_secs as u64))
    } else {
        None
    };

    info!("[handle_set] Key: {:?}, Value: ({} bytes), Group: {:?}, TTL: {}s, Compress: {}, ExpiresAt: {:?}", 
        key.value, value.value.len(), group.map(|g| g.value.clone()), ttl_secs, compress.is_some(), expires_at);

    let entry = Entry {
        key : key.value.clone(),
        value: value.value.clone(),
        group: group.map(|g| g.value.clone()),
        expires_at,
        compressed: compress.is_some(),
        ttl: ttl_secs,
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis(),
    };

    store.set(key.value.clone(), entry);
    info!("[handle_set] SET erfolgreich für Key: {:?}", key.value);
    Ok(vec![
        TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
    ])
}

pub async fn handle_get(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    info!("[handle_get] Starte GET mit TLVs: {:?}", tlvs);
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;

    if let Some(key) = get_tlv_value(&tlvs, TlvFieldTypes::KEY) {
        info!("[handle_get] GET für Key: {:?}", key.value);
        if let Some(entry) = store.get(&key.value) {
            info!("[handle_get] Key gefunden: {:?} ({} bytes)", key.value, entry.value.len());
            Ok(vec![
                TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone()),
                TlvField::new(TlvFieldTypes::KEY, key.value.clone()),
                TlvField::new(TlvFieldTypes::VALUE, entry.value),
            ])
        } else {
            warn!("[handle_get] Key nicht gefunden: {:?}", key.value);
            Err(StorageError::NotFound)
        }
    } else if let Some(group) = get_tlv_value(&tlvs, TlvFieldTypes::GROUP) {
        info!("[handle_get] GET für Gruppe: {:?}", group.value);
        if let Some(entries) = store.get_by_group(&group.value) {
            info!("[handle_get] {} Einträge in Gruppe gefunden", entries.len());
            let mut result = Vec::new();
            result.push(TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone()));
            for entry in entries {
                result.push(TlvField::new(TlvFieldTypes::KEY, entry.key));
                result.push(TlvField::new(TlvFieldTypes::VALUE, entry.value));
            }
            Ok(result)
        } else {
            warn!("[handle_get] Gruppe nicht gefunden: {:?}", group.value);
            Err(StorageError::NotFound)
        }
    } else {
        error!("[handle_get] Weder KEY noch GROUP angegeben!");
        Err(StorageError::InvalidInput)
    }
}

pub async fn handle_delete(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    info!("[handle_delete] Starte DELETE mit TLVs: {:?}", tlvs);
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;

    if let Some(key) = get_tlv_value(&tlvs, TlvFieldTypes::KEY) {
        info!("[handle_delete] Lösche Key: {:?}", key.value);
        store.delete(&key.value);
        Ok(vec![
            TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
        ])
    } else if let Some(group) = get_tlv_value(&tlvs, TlvFieldTypes::GROUP) {
        info!("[handle_delete] Lösche Gruppe: {:?}", group.value);
        store.delete_by_group(&group.value);
        Ok(vec![
            TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
        ])
    } else {
        error!("[handle_delete] Weder KEY noch GROUP angegeben!");
        Err(StorageError::InvalidInput)
    }
}

pub async fn handle_flush(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    info!("[handle_flush] Starte FLUSH mit TLVs: {:?}", tlvs);
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;
    store.flush();
    info!("[handle_flush] RAM-Store komplett geleert!");
    Ok(vec![
        TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
    ])
}

pub async fn handle_touch(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    info!("[handle_touch] Starte TOUCH mit TLVs: {:?}", tlvs);
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;
    let key = get_tlv_value(&tlvs, TlvFieldTypes::KEY).ok_or(StorageError::InvalidInput)?;

    match store.touch(&key.value) {
        Ok(()) => {
            info!("[handle_touch] TOUCH erfolgreich für Key: {:?}", key.value);
            Ok(vec![
                TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
            ])
        }
        Err(()) => {
            warn!("[handle_touch] TOUCH fehlgeschlagen für Key: {:?}", key.value);
            Err(StorageError::NotFound)
        }
    }
}

pub async fn handle_exists(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    info!("[handle_exists] Starte EXISTS mit TLVs: {:?}", tlvs);
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;

    if let Some(key) = get_tlv_value(&tlvs, TlvFieldTypes::KEY ) {
        info!("[handle_exists] EXISTS für Key: {:?}", key.value);
        if store.get(&key.value).is_some() {
            info!("[handle_exists] Key existiert: {:?}", key.value);
            return Ok(vec![
                TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
            ])
        } else {
            warn!("[handle_exists] Key existiert NICHT: {:?}", key.value);
            return Err(StorageError::NotFound)
        }
    } else if let Some(group) = get_tlv_value(&tlvs, TlvFieldTypes::GROUP ) {
        info!("[handle_exists] EXISTS für Gruppe: {:?}", group.value);
        if store.get_by_group(&group.value).is_some() {
            info!("[handle_exists] Gruppe existiert: {:?}", group.value);
            return Ok(vec![
                TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
            ])
        } else {
            warn!("[handle_exists] Gruppe existiert NICHT: {:?}", group.value);
            return Err(StorageError::NotFound)
        }
    }

    error!("[handle_exists] Weder KEY noch GROUP angegeben!");
    Err(StorageError::InvalidInput)
}

pub async fn handle_copy(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    info!("[handle_copy] Starte COPY mit TLVs: {:?}", tlvs);
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;
    let old_key = get_tlv_value(&tlvs, TlvFieldTypes::KEY ).ok_or(StorageError::InvalidInput)?;
    let new_key = get_tlv_value(&tlvs, TlvFieldTypes::NewKey ).ok_or(StorageError::InvalidInput)?;

    if let Some(entry) = store.get(&old_key.value) {
        info!("[handle_copy] Kopiere Key {:?} nach {:?}", old_key.value, new_key.value);
        store.set(new_key.value.clone(), entry);
        Ok(vec![
            TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
        ])
    } else {
        warn!("[handle_copy] Quell-Key nicht gefunden: {:?}", old_key.value);
        Err(StorageError::NotFound)
    }
}

pub async fn handle_info(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    info!("[handle_info] Starte INFO mit TLVs: {:?}", tlvs);
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;
    let key = get_tlv_value(&tlvs, TlvFieldTypes::KEY ).ok_or(StorageError::InvalidInput)?;

    if let Some(entry) = store.get(&key.value) {
        info!("[handle_info] Key gefunden: {:?}, Value-Size: {}, TTL: {}, Compressed: {}", key.value, entry.value.len(), entry.ttl, entry.compressed);
        let mut fields = vec![
            TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone()),
            TlvField::new(TlvFieldTypes::RamSize, Bytes::from(entry.value.len().to_string())),
            TlvField::new(TlvFieldTypes::TTL, Bytes::from(entry.ttl.to_string())),
            TlvField::new(TlvFieldTypes::COMPRESS, Bytes::from(vec![entry.compressed as u8])),
        ];
        if let Some(group) = entry.group {
            fields.push(TlvField::new(TlvFieldTypes::GROUP, group));
        }
        Ok(fields)
    } else {
        warn!("[handle_info] Key nicht gefunden: {:?}", key.value);
        Err(StorageError::NotFound)
    }
}

pub async fn handle_sysinfo(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    info!("[handle_sysinfo] Starte SYSINFO mit TLVs: {:?}", tlvs);
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;
    let ram_count = store.count();
    let ram_size = store.total_size();

    info!("[handle_sysinfo] RAM-Keys: {}, RAM-Size: {}", ram_count, ram_size);

    Ok(vec![
        TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone()),
        TlvField::new(TlvFieldTypes::RamKeyCount, Bytes::from(ram_count.to_string())),
        TlvField::new(TlvFieldTypes::RamSize, Bytes::from(ram_size.to_string())),
    ])
}

pub async fn handle_ping(_: Vec<TlvField>) -> Result<Vec<TlvField>, StorageError> {
    info!("[handle_ping] PING empfangen, sende leere Antwort zurück.");
    Ok(vec![])
}

/// Verarbeitet ein Sync-Event (z.B. SYNC_SET, SYNC_DELETE, SYNC_TOUCH, ...)
pub async fn handle_sync_event(event: u8, tlvs: Vec<TlvField>, store: &Arc<RamStore>) {
    match event {
        0x80 => {
            // SYNC_SET: Key/Value-Update
            let _ = handle_set(tlvs, store).await;
            info!("[handle_sync_event] SYNC_SET: Key/Value-Update erfolgreich!");
        }
        0x81 => {
            // SYNC_DELETE: Key löschen
            let _ = handle_delete(tlvs, store).await;
            info!("[handle_sync_event] SYNC_DELETE: Key gelöscht!");
        }
        0x82 => {
            // SYNC_TOUCH: TTL verlängern
            let _ = handle_touch(tlvs, store).await;
            info!("[handle_sync_event] SYNC_TOUCH: TTL verlängert!");
        }
        0x83 => {
            // SYNC_FLUSH oder SYNC_SNAPSHOT: Unterscheide an der TLV-Länge
            if tlvs.is_empty() {
                // SYNC_FLUSH: Löscht den gesamten Speicher auf dem Slave
                store.flush();
                info!("[handle_sync_event] SYNC_FLUSH: RAM-Store auf Slave komplett geleert!");
            } else {
                // SYNC_SNAPSHOT: Importiere alle Key/Value-Paare
                store.flush();
                let mut key: Option<bytes::Bytes> = None;
                for tlv in tlvs {
                    use crate::protocol::tlv::TlvFieldTypes;
                    match tlv.type_id {
                        TlvFieldTypes::KEY => key = Some(tlv.value),
                        TlvFieldTypes::VALUE => {
                            if let Some(k) = key.take() {
                                let entry = super::ram_handler::Entry {
                                    key: k.clone(),
                                    value: tlv.value,
                                    group: None,
                                    expires_at: None,
                                    compressed: false,
                                    ttl: 0,
                                    timestamp: 0,
                                };
                                store.set(k, entry);
                            }
                        }
                        _ => {}
                    }
                }
                info!("[handle_sync_event] SYNC_SNAPSHOT: {} Einträge importiert!", store.count());
            }
        }
        0x84 => {
            // SYNC_CONFLICT: Konfliktbehandlung (optional)
            // TODO: Implementiere Konflikt-Logik
        }
        _ => {
            warn!("[handle_sync_event] Unbekanntes Sync-Event: 0x{:02X}", event);
        }
    }
}