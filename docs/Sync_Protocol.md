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
* Sendet regelmäßig `SYNC_HEARTBEAT` (alle `sync_interval` Sekunden).
* Verarbeitet eingehende `SYNC_SET`, `SYNC_DELETE`, `SYNC_CONFLICT`.
* Verteilt bestätigte Writes an alle verbundenen Slaves.
* Beantwortet `SYNC_POPULATE` und `SYNC_DIFF_REQUEST`.

### Slave

* Baut beim Start oder Verbindungsverlust Verbindung zum ersten erreichbaren Peer auf (laut `peers`).
* Erkennt Masterverlust nach `sync_timeout` Sekunden ohne `SYNC_HEARTBEAT`.
* Fragt initial `SYNC_POPULATE` oder `SYNC_DIFF_REQUEST` an.
* Sendet eigene Writes (SET/DELETE) immer an Master.

---

## Nachrichtenformate (TLV)

| Event-ID | Name                | Beschreibung                                       |
| -------- | ------------------- | -------------------------------------------------- |
| `0x80`   | SYNC\_SET           | Key/Value-Update vom Master an Slaves              |
| `0x81`   | SYNC\_DELETE        | Löschung eines Keys                                |
| `0x82`   | SYNC\_POPULATE      | Vollständiger Snapshot wird angefordert            |
| `0x83`   | SYNC\_SNAPSHOT      | Snapshot mit allen Key/Value/Timestamps            |
| `0x84`   | SYNC\_CONFLICT      | Slave meldet abweichenden/neuen Key zurück         |
| `0x85`   | SYNC\_HEARTBEAT     | Lebenszeichen vom Master                           |
| `0x86`   | SYNC\_DIFF\_REQUEST | Slave meldet Key/Hash/TS zur Deltasynchronisierung |

### Message Struktur 

````
[LENGTH (4 bytes)] [EVENT (1 byte)] [TLV fields]
````

### TLV-Feldstruktur (kompatibel mit ByteWizard)

```
[TYPE (1 Byte)] [LENGTH (4 Byte)] [VALUE (... Byte)]
```

### Verwendete Typen für Sync-Protokoll

* `0xA0` = KEY (UTF-8)
* `0xA1` = VALUE (binary)
* `0xA2` = GROUP (UTF-8)
* `0xA4` = MESSAGE\_ID (u32)
* `0xB0` = COMPRESS (u8)
* `0xB1` = PERSISTENT (u8)
* `0xB2` = TTL (u32)
* `0xC5` = TIMESTAMP (u64)
* `0xC6` = TTL\_REMAINING (u32)
* `0xC7` = IS\_COMPRESSED (u8)
* `0xC8` = IS\_PERSISTENT (u8)
* `0xC9` = HASH (blake3-128, 16 Byte)

### SYNC\_SET / SYNC\_CONFLICT / SYNC\_SNAPSHOT

* `0xA0` KEY
* `0xA1` VALUE
* `0xC5` TIMESTAMP
* `0xB2` TTL (optional)
* `0xB0` COMPRESS (optional)
* `0xB1` PERSISTENT (optional)
* `0xA2` GROUP (optional)
* `0xA4` MESSAGE\_ID (optional)

### SYNC\_DELETE

* `0xA0` KEY
* `0xC5` TIMESTAMP
* `0xA4` MESSAGE\_ID (optional)

### SYNC\_DIFF\_REQUEST

* Liste von Key-Einträgen mit:

  * `0xA0` KEY
  * `0xC5` TIMESTAMP
  * `0xC9` HASH
  * `0xA4` MESSAGE\_ID

### SYNC\_HEARTBEAT

* `0xC5` TIMESTAMP

---

## Ablauf: Startup, Reconnect, Failover

### 1. Startup

* Jeder Node lädt die geordnete `peers`-Liste.
* Prüft die Peers in Reihenfolge:

  * Wenn `self.addr` als erstes erreichbar ist → Master.
  * Andernfalls → Slave, Verbindung zum ersten erreichbaren Peer.

### 2. Reconnect

* Bei Disconnect oder fehlendem Heartbeat > `sync_timeout`:

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
sync_interval = 10        # Sekunden zwischen Heartbeats
sync_timeout = 30         # Master gilt als offline nach 30s
sync_use_delta = true     # Delta statt Snapshot beim Sync
```

---

## KI-Implementierungsplan: Schritt-für-Schritt

### 1. **Grundstruktur & Konfiguration**

* [ ] `SyncManager`-Struktur mit `role` (Master/Slave), `peer_list`, `server_addr`
* [ ] Initiale Master-Entscheidung: `find_master()`
* [ ] TCP-Server starten (Master), TCP-Client starten (Slave)

### 2. **Message Routing & TLV-Handler**

* [ ] Parser für alle TLV-Typen: `SYNC_SET`, `SYNC_DELETE`, ...
* [ ] Broadcast-Mechanismus im Master
* [ ] Write-Verarbeitung: Empfang → Weiterleitung → Bestätigung → Broadcast

### 3. **Startup-Sync**

* [ ] `SYNC_POPULATE` senden & empfangen
* [ ] `SYNC_SNAPSHOT` erzeugen (Iterator über alle Key/Value mit TS)

### 4. **Delta-Sync**

* [ ] Hash pro Key berechnen (z. B. BLAKE3)
* [ ] `SYNC_DIFF_REQUEST` mit allen Keys, Hashes, Timestamps senden
* [ ] Master vergleicht, sendet `SYNC_SET` nur bei Änderungen

### 5. **Heartbeat & Timeout**

* [ ] `SYNC_HEARTBEAT` im Master regelmäßig senden
* [ ] Slaves überwachen Heartbeat (Timeout + Retry)

### 6. **Failover-Logik**

* [ ] Bei Timeout: Verbindung schließen, nächster Peer in Liste → neuer Master
* [ ] Rollenwechsel: z. B. Slave wird Master

### 7. **Konfliktlösung**

* [ ] Bei `SYNC_CONFLICT`: timestamp-basierte Entscheidung
* [ ] Master aktualisiert Wert, broadcastet neuen Stand

### 8. **Testinfrastruktur**

* [ ] Lokale 3-Node-Testumgebung (Master + 2 Slaves)
* [ ] Simuliere Disconnect, Timeout, Reconnect
* [ ] Snapshot vs Delta vergleichen

---

> Hinweis: Für alle Fehlerantworten kann ein optionales `ERROR`-Feld mit Text enthalten sein.
