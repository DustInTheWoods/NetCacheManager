# TLV Protocol Documentation (NetCacheManager)

## Protocol Structure

Each message consists of a fixed 4-byte length field, an event byte, and optional TLV fields.

### Message Structure
```
[LENGTH (4 bytes)] [EVENT (1 byte)] [TLV fields...]
```

### TLV Field Structure
```
[TYPE (1 byte)] [LENGTH (4 bytes)] [VALUE (length bytes)]
```

### Response Structure
```
[LENGTH (4 bytes)] [STATUS (1 byte)] [TLV fields]
```

## Status Codes
- `0x01` STATUS_OK
- `0x02` NOT_FOUND
- `0x03` INVALID_INPUT
- `0x04` EXISTS_ALREADY
- `0x05` INTERNAL_ERROR

## TLV Field Types
- `0xA0` KEY
- `0xA1` VALUE
- `0xA2` GROUP
- `0xA3` NEW_KEY
- `0xA4` MESSAGE_ID *(identifies request/response pairing)*
- `0xB0` COMPRESS
- `0xB1` PERSISTENT
- `0xB2` TTL
- `0xB3` OVERWRITE
- `0xD0` DISK_KEY_COUNT
- `0xD1` DISK_SIZE
- `0xD2` RAM_KEY_COUNT
- `0xD3` RAM_SIZE

## Message ID Handling
The **MESSAGE_ID** field (`0xA4`) is mandatory in every request (exept ping) and must be mirrored back in the response.
- Format: 4-byte UINT32.
- Ensures matching of requests and responses, especially for asynchronous operations.

Example:
```
Client Request: [Length][Event][MESSAGE_ID=123][KEY='example']
Server Response: [Length][Status][MESSAGE_ID=123][KEY='example'][VALUE='data']
```

## Events

### Event 0x01 — PING
**Request:**

**Response:**
- `STATUS`

### Event 0x10 — EXISTS
Check if a key or group exists.

**Request:**
- `KEY` (required if `GROUP` not set)
- `GROUP` (required if `KEY` not set)
- `MESSAGE_ID` (required)

**Response:**
- `STATUS`
- `MESSAGE_ID`
- `ERROR` (optional error description)

### Event 0x20 — SET
Store a new key with value and optional metadata.

**Request:**
- `KEY` (required)
- `VALUE` (required)
- `GROUP` (optional)
- `MESSAGE_ID` (required)
- `COMPRESS` (optional)
- `PERSISTENT` (optional)
- `TTL` (optional)

**Response:**
- `STATUS`
- `MESSAGE_ID`
- `ERROR` (optional)

### Event 0x21 — GET
Retrieve a key or group of keys.

**Request:**
- `KEY` (required if `GROUP` not set)
- `GROUP` (required if `KEY` not set)
- `MESSAGE_ID` (required)

**Response:**
- `STATUS`
- `MESSAGE_ID`
- `ERROR` (optional)
- `KEY` and `VALUE` fields

_Note: If requesting by group, returns a list of key/value pairs._

### Event 0x22 — DELETE
Delete a single key or group.

**Request:**
- `KEY` (required if `GROUP` not set)
- `GROUP` (required if `KEY` not set)
- `MESSAGE_ID` (required)

**Response:**
- `STATUS`
- `MESSAGE_ID`
- `ERROR` (optional)

### Event 0x23 — FLUSH
Flush all RAM and optionally persistent storage.

**Request:**
- `MESSAGE_ID` (required)

**Response:**
- `STATUS`
- `MESSAGE_ID`
- `ERROR` (optional)

### Event 0x24 — TOUCH
Extend the TTL of a key.

**Request:**
- `KEY` (required)
- `MESSAGE_ID` (required)

**Response:**
- `STATUS`
- `MESSAGE_ID`
- `ERROR` (optional)

### Event 0x25 — COPY
Copy a key to a new key name.

**Request:**
- `KEY` (source, required)
- `NEW_KEY` (destination, required)
- `MESSAGE_ID` (required)

**Response:**
- `STATUS`
- `MESSAGE_ID`
- `ERROR` (optional)

### Event 0x30 — INFO
Return metadata for a key.

**Request:**
- `KEY` (required)
- `MESSAGE_ID` (required)

**Response:**
- `STATUS`
- `MESSAGE_ID`
- `ERROR` (optional)
- `SIZE`, `TTL_REMAINING`, `IS_COMPRESSED`, `IS_PERSISTENT`

### Event 0x31 — SYSINFO
Return system-level information.

**Request:**
- `MESSAGE_ID` (required)

**Response:**
- `STATUS`
- `MESSAGE_ID`
- `ERROR` (optional)
- `DISK_KEY_COUNT`, `DISK_SIZE`, `RAM_KEY_COUNT`, `RAM_SIZE`

---

> **Note:** All messages (including errors and heartbeats, if used) must always follow the structure `[LENGTH (4 bytes)] [EVENT/STATUS (1 byte)] [TLV fields...]`. The MESSAGE_ID field is required in every request (except ping) and must be mirrored in the response.

