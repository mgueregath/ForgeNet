# Wire protocol

This is the formal spec of ForgeNet's binary protocol — the thing all 6 language implementations agree on byte-for-byte. If you're implementing a 7th language client, or auditing one of the existing ones for drift, this document (not any single language's source) is the source of truth; `go/networkcore` is the reference *implementation* of what's written here.

All multi-byte integers are **big-endian**. There is no protocol-level versioning field today — see [ROADMAP.md](ROADMAP.md).

## Header (every packet)

9 bytes, present on every packet regardless of type:

| Bytes | Field | Type |
|---|---|---|
| 0-3 | `Seq` | `uint32` |
| 4-7 | `Ack` | `uint32` |
| 8 | `TypeAndFlags` | `uint8` — low nibble = packet type, high nibble = flags |

```
TypeAndFlags = (packetType & 0x0F) | ((flags & 0x0F) << 4)
```

### Packet types (low nibble)

| Value | Name | Direction |
|---|---|---|
| `0x01` | Snapshot | server → client |
| `0x02` | Input | client → server |
| `0x03` | Ack | either — see [Acknowledgement convention](#acknowledgement-convention) |
| `0x04` | Ping | client → server, echoed back |
| `0x05` | Handshake | client → server (request) / server → client (response) |
| `0x06` | Disconnect | server → client — handshake rejection today; not otherwise used server-initiated (see ROADMAP) |

### Flags (high nibble)

| Bit | Name | Meaning |
|---|---|---|
| `0x01` | Compressed | Reserved — not implemented by any language yet |
| `0x02` | Reliable | On Input: server ACKs it. On Snapshot: this packet carries a `QueueReliableEvent`, retried until ACKed |
| `0x04` | Ordered | Set by clients on non-reliable input by convention; not enforced by the core |

## Handshake

### Request (client → server)

Header (`PacketHandshake`, seq/ack unused by convention — `0,0`) followed by:

| Bytes | Field | Type |
|---|---|---|
| 0 | `Mode` | `uint8` — `0x00` = Create, `0x01` = Join |
| 1 | `Role` | `uint8` — opaque, game-defined |
| 2 | `RoomCodeLen` | `uint8` |
| 3..3+N | `RoomCode` | ASCII, `N = RoomCodeLen` bytes. Ignored (should be empty) when `Mode == Create` |

### Success response (server → client)

Header (`PacketHandshake`, `seq` = the request's `seq`, `ack = 0`) followed by:

| Bytes | Field | Type |
|---|---|---|
| 0-1 | `PlayerID` | `uint16` |
| 2 | `RoomCodeLen` | `uint8` |
| 3..3+N | `RoomCode` | ASCII. Present whether the client created or joined — a creator needs it to share (QR, deep link), a joiner gets it echoed back for confirmation |

### Rejection response (server → client)

Header (`PacketDisconnect`, `seq` = the request's `seq`) followed by a single byte:

| Value | Name | Meaning |
|---|---|---|
| `0x01` | `ReasonRoomNotFound` | `Mode == Join` against a code that doesn't exist |
| `0x02` | `ReasonRoomFull` | Room already at `ServerOptions.MaxPlayersPerRoom` |

### Idempotency / retry

If the exact same peer (same transport-level identity — see [Peer identity](#peer-identity)) sends another Handshake request after already being admitted, the server does **not** re-run admission or create a second room — it resends the cached success response verbatim. This makes a lost ack safe to retry from the client side with no special-casing.

### Reconnection

A Handshake whose sender IP matches a *disconnected-but-not-yet-reaped* client already in the target room is treated as that same player reconnecting, not a new one: the response carries the player's **original** `PlayerID` and **original** `Role` (the `Role` byte in this new request is ignored in that case). See [ARCHITECTURE.md § Reconnection](ARCHITECTURE.md#reconnection) for the server-side mechanics and its limitations.

## Input (client → server)

Header (`PacketInput`, `Seq` = client's own increasing counter, flags as above) followed by a fixed 10-byte payload:

| Bytes | Field | Type |
|---|---|---|
| 0-1 | `DeltaX` | `int16` |
| 2-3 | `DeltaY` | `int16` |
| 4-5 | `Rotation` | `uint16` |
| 6-9 | `Actions` | `uint32` (bitmask) |

None of these four fields have a fixed meaning past "a 2D delta, a rotation, and 32 free bits" — see the README's [Repurposing `PlayerInput`](../README.md#repurposing-playerinput-a-real-example) for how far a real game pushed that.

## Snapshot (server → client)

Header (`PacketSnapshot`) followed by:

| Bytes | Field | Type |
|---|---|---|
| 0-7 | `Tick` | `uint64` |
| 8-11 | `StatePayloadLen` | `uint32` |
| 12..12+L | `StatePayload` | opaque, `L = StatePayloadLen` bytes — game-defined format entirely |
| 12+L | `EventCount` | `uint8` |
| ...repeated `EventCount` times | `Event` | see below |

Each `Event`:

| Bytes | Field | Type |
|---|---|---|
| 0-1 | `PlayerID` | `uint16` |
| 2 | `EventType` | `uint8` — opaque, game-defined |
| 3 | `DataLen` | `uint8` |
| 4..4+D | `Data` | opaque, `D = DataLen` bytes |

A Snapshot with the `Reliable` flag set is a `QueueReliableEvent` delivery, not the regular per-tick broadcast — it typically carries an empty `StatePayload` and exactly one `Event`. See [Acknowledgement convention](#acknowledgement-convention).

## Ping (client → server, echoed)

Header (`PacketPing`) followed by 8 bytes: the sender's own `UnixMilli()` timestamp, big-endian `int64`. The server overwrites the payload with its own current timestamp and echoes the same packet type back; the client computes RTT/2 as ping.

## Acknowledgement convention

`PacketAck`'s header is unusual: **the sequence number being acknowledged is carried in the `Seq` field itself** (not `Ack`), and by convention a sender sets `Seq == Ack == <the number being acked>` when building one. Concretely: to acknowledge a received packet whose header `Seq` was `N`, send `PacketAck` with header `Seq = N, Ack = N`. This applies in both directions:

- Server acking a client's reliable Input: `handleInput`/`sendAck` do exactly this.
- Client acking a server's reliable Snapshot (`QueueReliableEvent`): every language's `NetworkClient` does this automatically on receipt — a client implementation that skips this will see the server retry that event forever (up to its retry cap).

## Peer identity

The core has no session token today. A connection's identity is:

- **Raw UDP**: `"udp:" + <ip>:<port>` as the routing key (`Peer.Key()`), and the bare IP (`Peer.IP()`) for reconnection matching.
- **WebTransport**: a per-session opaque key for routing, and the session's remote IP for reconnection matching.

This means: source-address spoofing is possible (nothing authenticates that a packet really came from the peer whose address it claims), and reconnection-by-IP is a heuristic, not a guarantee (see ARCHITECTURE.md). Both are known, deliberately deferred limitations — see [ROADMAP.md](ROADMAP.md).
