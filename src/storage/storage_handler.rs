use std::sync::Arc;
use std::time::{Duration, Instant};
use bytes::Bytes;
use log::info;
use log::debug;
use crate::protocol::tlv::{TlvField, TlvFieldTypes};
use crate::storage::ram_handler::RamStore;
use super::ram_handler::Entry;


#[derive(Debug)]
pub enum StorageError {
    NotFound,
    InvalidInput,
    AlreadyExists,
    InternalError,
}

pub async fn handle_event(event: u8, tlvs: Vec<TlvField>, store: Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    info!("Handle Event: 0x{:02X} mit {} TLVs", event, tlvs.len());
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
        _ => Err(StorageError::InvalidInput),
    }
}

pub fn get_tlv_value(tlvs: &[TlvField], type_id: TlvFieldTypes) -> Option<&TlvField> {
    tlvs.iter().find(|tlv| tlv.type_id == type_id)
}

pub async fn handle_set(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
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

    let entry = Entry {
        key : key.value.clone(),
        value: value.value.clone(),
        group: group.map(|g| g.value.clone()),
        expires_at,
        compressed: compress.is_some(),
        ttl: ttl_secs,
    };

    store.set(key.value.clone(), entry);
    Ok(vec![
        TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
    ])
}

pub async fn handle_get(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;

    if let Some(key) = get_tlv_value(&tlvs, TlvFieldTypes::KEY) {
        if let Some(entry) = store.get(&key.value) {
            Ok(vec![
                TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone()),
                TlvField::new(TlvFieldTypes::KEY, key.value.clone()),
                TlvField::new(TlvFieldTypes::VALUE, entry.value),
            ])
        } else {
            Err(StorageError::NotFound)
        }
    } else if let Some(group) = get_tlv_value(&tlvs, TlvFieldTypes::GROUP) {
        if let Some(entries) = store.get_by_group(&group.value) {
            let mut result = Vec::new();
            result.push(TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone()));
            for entry in entries {
                result.push(TlvField::new(TlvFieldTypes::KEY, entry.key));
                result.push(TlvField::new(TlvFieldTypes::VALUE, entry.value));
            }
            Ok(result)
        } else {
            Err(StorageError::NotFound)
        }
    } else {
        Err(StorageError::InvalidInput)
    }
}

pub async fn handle_delete(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;

    if let Some(key) = get_tlv_value(&tlvs, TlvFieldTypes::KEY) {
        store.delete(&key.value);
        Ok(vec![
            TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
        ])
    } else if let Some(group) = get_tlv_value(&tlvs, TlvFieldTypes::GROUP) {
        store.delete_by_group(&group.value);
        Ok(vec![
            TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
        ])
    } else {
        Err(StorageError::InvalidInput)
    }
}

pub async fn handle_flush(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;
    store.flush();
    Ok(vec![
        TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
    ])
}

pub async fn handle_touch(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;
    let key = get_tlv_value(&tlvs, TlvFieldTypes::KEY).ok_or(StorageError::InvalidInput)?;

    if let Some(mut entry) = store.get(&key.value) {
        if entry.ttl == 0 {
            return Err(StorageError::InvalidInput);
        }
        entry.expires_at = Some(Instant::now() + Duration::from_secs(entry.ttl as u64));
        debug!("TOUCH: Aktualisiere expires_at für Key {:?} auf {:?}", key.value, entry.expires_at);
        store.set(key.value.clone(), entry);
        Ok(vec![
            TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
        ])
    } else {
        Err(StorageError::NotFound)
    }
}

pub async fn handle_exists(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;

    if let Some(key) = get_tlv_value(&tlvs, TlvFieldTypes::KEY ) {
        if store.get(&key.value).is_some() {
            return Ok(vec![
                TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
            ])
        } else {
            return Err(StorageError::NotFound)
        }
    } else if let Some(group) = get_tlv_value(&tlvs, TlvFieldTypes::GROUP ) {
        if store.get_by_group(&group.value).is_some() {
            return Ok(vec![
                TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
            ])
        } else {
            return Err(StorageError::NotFound)
        }
    }

    Err(StorageError::InvalidInput)
}

pub async fn handle_copy(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;
    let old_key = get_tlv_value(&tlvs, TlvFieldTypes::KEY ).ok_or(StorageError::InvalidInput)?;
    let new_key = get_tlv_value(&tlvs, TlvFieldTypes::NewKey ).ok_or(StorageError::InvalidInput)?;

    if let Some(entry) = store.get(&old_key.value) {
        store.set(new_key.value.clone(), entry);
        Ok(vec![
            TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone())
        ])
    } else {
        Err(StorageError::NotFound)
    }
}

pub async fn handle_info(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;
    let key = get_tlv_value(&tlvs, TlvFieldTypes::KEY ).ok_or(StorageError::InvalidInput)?;

    if let Some(entry) = store.get(&key.value) {
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
        Err(StorageError::NotFound)
    }
}

pub async fn handle_sysinfo(tlvs: Vec<TlvField>, store: &Arc<RamStore>) -> Result<Vec<TlvField>, StorageError> {
    let message_id = get_tlv_value(&tlvs, TlvFieldTypes::MessageId).ok_or(StorageError::InvalidInput)?;
    let ram_count = store.count();
    let ram_size = store.total_size();

    Ok(vec![
        TlvField::new(TlvFieldTypes::MessageId, message_id.value.clone()),
        TlvField::new(TlvFieldTypes::RamKeyCount, Bytes::from(ram_count.to_string())),
        TlvField::new(TlvFieldTypes::RamSize, Bytes::from(ram_size.to_string())),
    ])
}

pub async fn handle_ping(_: Vec<TlvField>) -> Result<Vec<TlvField>, StorageError> {
    Ok(vec![])
}