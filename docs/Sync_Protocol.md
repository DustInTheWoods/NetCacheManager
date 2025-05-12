# NetCacheManager Sync-Protokoll

## Ziel

Verteilte Konsistenz und Hochverfügbarkeit für NetCacheManager-Instanzen in einem lokalen Netzwerk (LAN oder Cluster).

---

## Architektur

* Jeder Node kennt alle Peers (inkl. sich selbst).
* Die Peer-Liste ist **fix konfiguriert** und **geordnet**.
* **Der erste erreichbare Peer in der Liste wird zum Master**.
* Nur **Slaves bauen Verbindungen zum Master auf**.
* **Der Master akzeptiert nur eingehende Verbindungen**.
* Alle **Writes laufen über den Master**, der an alle verbundenen Slaves broadcastet.

---

## Rollenverhalten

### Master

* Akzeptiert nur eingehende Verbindungen von Slaves.
* Verarbeitet eingehende `SYNC_SET`, `SYNC_DELETE`, `SYNC_CONFLICT`.
* Verteilt bestätigte Writes an alle verbundenen Slaves.
* Beantwortet `SYNC_POPULATE` und `SYNC_DIFF_REQUEST`.

### Slave

* Baut beim Start oder Verbindungsverlust Verbindung zum ersten erreichbaren Peer auf (laut `peers`).
* Fragt initial `SYNC_POPULATE` oder `SYNC_DIFF_REQUEST` an.
* Sendet eigene Writes (SET/DELETE) immer an Master.

---

## Nachrichtenformate (TLV)

| Event-ID | Name                | Beschreibung                                       |
| -------- | ------------------- | -------------------------------------------------- |
| `0x80`   | SYNC_SET            | Key/Value-Update vom Master an Slaves              |
| `0x81`   | SYNC_DELETE         | Löschung eines Keys                                |
| `0x82`   | SYNC_TOUCH          | TTL wird verlängert, wird geschickt bei GET        |
| `0x83`   | SYNC_FLUSH          | Löschung des gesamten Speichers                    |
| `0x84`   | SYNC_SNAPSHOT       | Snapshot mit allen Key/Value/Timestamps            |
| `0x85`   | SYNC_CONFLICT       | Slave meldet abweichenden/neuen Key zurück         |
| `0x86`   | SYNC_DIFF_REQUEST   | Slave meldet Key/Hash/TS zur Deltasynchronisierung |

### Message Struktur 

```
[LENGTH (4 bytes)] [EVENT (1 byte)] [TLV fields]
```

### TLV-Feldstruktur (kompatibel mit ByteWizard)

```
[TYPE (1 Byte)] [LENGTH (4 Byte)] [VALUE (... Byte)]
```

### Verwendete Typen für Sync-Protokoll

* `0xA0` = KEY (UTF-8)
* `0xA1` = VALUE (binary)
* `0xA2` = GROUP (UTF-8)
* `0xA4` = MESSAGE_ID (u32)
* `0xB0` = COMPRESS (u8)
* `0xB1` = PERSISTENT (u8)
* `0xB2` = TTL (u32)
* `0xC5` = TIMESTAMP (u64)
* `0xC6` = TTL_REMAINING (u32)
* `0xC7` = IS_COMPRESSED (u8)
* `0xC8` = IS_PERSISTENT (u8)
* `0xC9` = HASH (blake3-128, 16 Byte)

### SYNC_SET / SYNC_CONFLICT / SYNC_SNAPSHOT

* `0xA0` KEY
* `0xA1` VALUE
* `0xC5` TIMESTAMP
* `0xB2` TTL (optional)
* `0xB0` COMPRESS (optional)
* `0xB1` PERSISTENT (optional)
* `0xA2` GROUP (optional)
* `0xA4` MESSAGE_ID (optional)

### SYNC_DELETE

* `0xA0` KEY
* `0xC5` TIMESTAMP
* `0xA4` MESSAGE_ID (optional)

### SYNC_DIFF_REQUEST

* Liste von Key-Einträgen mit:
  * `0xA0` KEY
  * `0xC5` TIMESTAMP
  * `0xC9` HASH
  * `0xA4` MESSAGE_ID

---

## Ablauf: Startup, Reconnect, Failover

### 1. Startup

* Jeder Node lädt die geordnete `peers`-Liste.
* Prüft die Peers in Reihenfolge:
  * Wenn `self.addr` als erstes erreichbar ist → Master.
  * Andernfalls → Slave, Verbindung zum ersten erreichbaren Peer.

### 2. Reconnect

* Bei Disconnect:
  * Verbindung schließen.
  * Liste erneut prüfen.
  * Neu verbinden.
  * Anfordern:
    * `SYNC_DIFF_REQUEST` wenn `sync_use_delta = true`
    * Sonst `SYNC_POPULATE`

### 3. Master-Failover

* Sobald aktueller Master nicht mehr erreichbar:
  * Nächster erreichbarer Peer in der Liste wird neuer Master.

### 4. Delta-Sync-Ablauf

* Slave berechnet Hash + Timestamp pro Key.
* Sendet `SYNC_DIFF_REQUEST` mit allen bekannten Keys.
* Master vergleicht Werte:
  * Wenn Key identisch → ignorieren
  * Wenn Key fehlt oder veraltet → `SYNC_SET` senden
  * Optional: `SYNC_DELETE` für Keys, die beim Master fehlen (konfigurierbar)

### 5. Snapshot-Ablauf

* Wenn kein Delta-Sync gewünscht oder zu lange offline:
  * `SYNC_POPULATE` senden → Master antwortet mit `SYNC_SNAPSHOT`

---

## Konfiguration (Beispiel)

```toml
[sync]
enabled = true
server_addr = "tcp://192.168.1.10:9000"
peers = [
  "tcp://192.168.1.10:9000",
  "tcp://192.168.1.11:9000",
  "tcp://192.168.1.12:9000"
]
sync_interval = 10        # Sekunden zwischen Syncs
sync_timeout = 30         # Master gilt als offline nach 30s
sync_use_delta = true     # Delta statt Snapshot beim Sync
```

---

> Hinweis: Für alle Fehlerantworten kann ein optionales `ERROR`-Feld mit Text enthalten sein.
