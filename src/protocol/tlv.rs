//! TLV (Type-Length-Value) Protokollmodul

use bytes::{Buf, BufMut, Bytes, BytesMut};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TlvFieldTypes {
    KEY = 0xA0,
    VALUE = 0xA1,     
    GROUP = 0xA2,
    NewKey = 0xA3 ,
    MessageId = 0xA4,
    COMPRESS = 0xB0,
    PERSISTENT = 0xB1,
    TTL = 0xB2,
    OVERWRITE = 0xB3,
    DiskKeyCount = 0xD0,
    DiskSize = 0xD1,
    RamKeyCount = 0xD2,
    RamSize= 0xD3,
    Error = 0xFF,
    TIMESTAMP = 0xC5,
    TTL_REMAINING = 0xC6,
    IS_COMPRESSED = 0xC7,
    IS_PERSISTENT = 0xC8,
    HASH = 0xC9,
}

impl TlvFieldTypes {
    pub fn from_u8(n: u8) -> Option<Self> {
        match n {
            0xA0 => Some(Self::KEY),
            0xA1 => Some(Self::VALUE),
            0xA2 => Some(Self::GROUP),
            0xA3 => Some(Self::NewKey),
            0xA4 => Some(Self::MessageId),
            0xB0 => Some(Self::COMPRESS),
            0xB1 => Some(Self::PERSISTENT),
            0xB2 => Some(Self::TTL),
            0xB3 => Some(Self::OVERWRITE),
            0xD0 => Some(Self::DiskKeyCount),
            0xD1 => Some(Self::DiskSize),
            0xD2 => Some(Self::RamKeyCount),
            0xD3 => Some(Self::RamSize),
            0xFF => Some(Self::Error),
            0xC5 => Some(Self::TIMESTAMP),
            0xC6 => Some(Self::TTL_REMAINING),
            0xC7 => Some(Self::IS_COMPRESSED),
            0xC8 => Some(Self::IS_PERSISTENT),
            0xC9 => Some(Self::HASH),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlvField {
    pub type_id: TlvFieldTypes,
    pub value: Bytes,
}

impl TlvField {
    pub fn new(type_id: TlvFieldTypes, value: Bytes) -> Self {
        Self { type_id, value }
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.type_id as u8);
        buf.put_u32(self.value.len() as u32);
        buf.put_slice(&self.value);
    }
    pub fn decode(buf: &mut Bytes) -> Option<Self> {
        if buf.remaining() < 5 {
            return None;
        }
        let raw_type = buf.get_u8();
        let type_id = TlvFieldTypes::from_u8(raw_type)?;
        let len = buf.get_u32() as usize;
        if buf.remaining() < len {
            return None;
        }
        let value = buf.copy_to_bytes(len);
        Some(Self { type_id, value })
    }
}

pub fn parse_tlv_fields(mut buf: Bytes) -> Vec<TlvField> {
    let mut fields = Vec::new();
    while buf.has_remaining() {
        if let Some(field) = TlvField::decode(&mut buf) {
            fields.push(field);
        } else {
            break;
        }
    }
    fields
}

pub fn encode_tlv_fields(fields: &[TlvField]) -> Bytes {
    use bytes::BufMut;

    // Grobgröße: 5 Byte Overhead pro TLV + tatsächliche Daten
    let total_size: usize = fields.iter()
        .map(|f| 5 + f.value.len())
        .sum();

    let mut buf = BytesMut::with_capacity(total_size);
    for field in fields {
        buf.put_u8(field.type_id as u8);
        buf.put_u32(field.value.len() as u32);
        buf.extend_from_slice(&field.value);
    }
    buf.freeze()
}

/// Serialisiert eine Nachricht im Format [LENGTH (4 bytes)][EVENT (1 byte)][TLV fields...]
pub fn serialize_message(event: u8, tlvs: &[TlvField]) -> bytes::Bytes {
    use bytes::BufMut;
    let tlv_bytes = encode_tlv_fields(tlvs);
    let total_len = 1 + tlv_bytes.len(); // 1 Byte Event + TLVs
    let mut buf = BytesMut::with_capacity(4 + total_len);
    buf.put_u32(total_len as u32);
    buf.put_u8(event);
    buf.extend_from_slice(&tlv_bytes);
    buf.freeze()
}

/// Liest eine Nachricht im Format [LENGTH (4 bytes)][EVENT (1 byte)][TLV fields...]
pub async fn read_message<R: tokio::io::AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<(u8, Vec<TlvField>)> {
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let msg_len = u32::from_be_bytes(len_buf) as usize;
    if msg_len < 1 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Message length < 1"));
    }
    let mut msg_buf = vec![0u8; msg_len];
    reader.read_exact(&mut msg_buf).await?;
    let event = msg_buf[0];
    let tlv_bytes = bytes::Bytes::copy_from_slice(&msg_buf[1..]);
    let tlvs = parse_tlv_fields(tlv_bytes);
    Ok((event, tlvs))
}
