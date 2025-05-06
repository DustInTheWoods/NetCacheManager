# Event Relay Protocol Documentation

## Übersicht

Das Event Relay ermöglicht es mehreren Clients, sich per TCP oder Unix-Socket mit dem Server zu verbinden und in Echtzeit Nachrichten (Events) auszutauschen. Jeder Client identifiziert sich eindeutig per UUID.

## Verbindungsaufbau
- Nach Verbindungsaufbau sendet der Client sofort seine UUID (16 Byte, RFC4122, binär).
- Der Server prüft, ob die UUID gültig und nicht bereits verbunden ist. Bei Fehler wird die Verbindung geschlossen.

## Nachrichtenformat
Jede Nachricht besteht aus:
1. **Länge** (4 Byte, uint32, Netzwerk-Byteorder): Länge des folgenden Payloads in Bytes (ohne die 4 Byte Länge selbst).
2. **Empfänger-UUID** (16 Byte):
   - Für Broadcast: 16x 0x00 (alle Bytes Null)
   - Sonst: Ziel-UUID
3. **Sender-UUID** (16 Byte): UUID des sendenden Clients
4. **Payload** (Länge = [Länge] - 32): Beliebige Binärdaten

**Struktur:**
```
[LEN:4][TO_UUID:16][FROM_UUID:16][PAYLOAD:LEN-32]
```

## Routing
- **Broadcast:** Empfänger-UUID = 16x 0x00 → Nachricht wird an alle verbundenen Clients (außer Sender) weitergeleitet.
- **Direkt:** Empfänger-UUID = Ziel-UUID → Nachricht wird nur an diesen Client weitergeleitet.
- Ist die Ziel-UUID nicht verbunden, wird die Nachricht verworfen (kein Queueing, keine Rückmeldung).

## Fehlerfälle und Fehlercodes

Fehlernachrichten haben das gleiche Format wie normale Nachrichten:
```
[LEN:4][TO_UUID:16][FROM_UUID:16][PAYLOAD:LEN-32]
```
- **TO_UUID**: UUID des Empfängers (Client, der den Fehler erhält)
- **FROM_UUID**: 16x 0xFF (Kennzeichnung „Server/Fehler“)
- **PAYLOAD**: Fehlercode (4 Byte, uint32, Netzwerk-Byteorder) + optionaler Fehlertext (UTF-8)

**Fehlercodes:**
- 0x00000001: Ungültige UUID (Formatfehler)
- 0x00000002: UUID bereits verbunden
- 0x00000003: Nachricht zu kurz/ungültiges Format
- 0x00000004: Nicht autorisiert (z.B. Nachricht vor Auth)
- 0x00000005: Ziel-UUID nicht verbunden
- 0x00000006: Interner Serverfehler

Nach einem kritischen Fehler (z.B. Authentifizierungsfehler) wird die Verbindung geschlossen.

## Beispiel
**Broadcast:**
```
[00 00 00 10][00...00][Sender-UUID][Daten...]
```
**Direkt:**
```
[00 00 00 10][Empfänger-UUID][Sender-UUID][Daten...]
```
**Fehler:**
```
[00 00 00 14][Client-UUID][FF...FF][00 00 00 01][Ungültige UUID]
```

## Hinweise
- Keine Heartbeats, keine Queues, keine Rückmeldungen.
- Latenzoptimiert: Nachrichten werden direkt weitergeleitet.
- Keine Verschlüsselung oder ACLs.

## Fehlerfälle
- Ungültige oder doppelte UUID: Verbindung wird sofort geschlossen.
- Nachrichten an nicht verbundene UUIDs werden verworfen.

---

**Letzte Aktualisierung:** 30.04.2025
