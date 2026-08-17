# Architecture

This describes how ForgeNet's Go implementation (`go/networkcore`) is structured internally — the reference the other 5 languages mirror. If you're using ForgeNet to build a game, you don't need this; see the root [README](../README.md) instead. This is for modifying ForgeNet itself.

## Layers

```
Server            owns the transport(s), multiplexes many rooms, handles the
                   handshake (create/join/reject/idempotent-retry), routes
                   every other packet to the right room's NetworkHost
  │
  ├── NetworkHost  one room / one match's protocol state: connected clients,
  │                tick loop, reliable-retransmission, heartbeat. Knows
  │                nothing about rooms, transports, or other rooms.
  │
  └── Peer         transport-level identity (Send/Key/IP) — implemented by
                   udpPeer and wtPeer. NetworkHost only ever talks to Peer,
                   never to a socket or a QUIC session directly.
```

`NetworkClient` is a separate, parallel piece — it speaks the same wire protocol as a peer talking *to* a `Server`, but shares no code with the server side beyond `protocol.go`'s encode/decode helpers.

### Why `NetworkHost` doesn't own a transport

Early on (see `git log` before the room/`Server` refactor), `NetworkHost` bound its own UDP socket and parsed its own handshake — fine when a process hosted exactly one match. Once rooms needed to be multiplexed (many matches, one listening socket), something had to own the socket and decide *which room* a packet belongs to before that room's own protocol logic runs — that's `Server`. `NetworkHost` lost socket ownership and handshake parsing entirely; it now only implements `HandlePacket` for packet types that already imply "you know what room this is" (Input/Ack/Ping), because `Server` guarantees it only calls `HandlePacket` on the right room's `NetworkHost`.

This split is why one `NetworkHost` instance = one room, always, and why a `RoomFactory` (`func() *NetworkHost`) exists — `Server` calls it fresh per room so each match gets independent state (independent tick loop, independent client list, independent game hooks), with no risk of one match's state leaking into another's.

### Why `Peer` is an interface, not a concrete type

`NetworkHost.HandlePacket(data []byte, peer Peer)` doesn't know or care whether `data` arrived over a raw UDP socket or a WebTransport datagram — both `udpPeer` and `wtPeer` just need to be able to `Send([]byte)`, report a stable `Key()` for routing, and report an `IP()` for reconnection matching. This is what lets `Server.StartUDP` and `Server.StartWebTransport` feed the *same* `NetworkHost`/room, so a native client and a browser client end up in the same match without either side's code changing.

## Room lifecycle

1. **Created** — either by a client's Handshake (`HandshakeModeCreate`) or directly via `Server.CreateRoom()` (no client, no handshake — see the README's Embedded Host mode). Either way, `Server.createRoom()` (lowercase, internal) does the actual work: generate a unique code, call the `RoomFactory`, start the room's loops (`NetworkHost.start()`), register it in `Server.rooms[code]`.
2. **Populated** — clients arrive via `HandshakeModeJoin` against the code, or (for the creator, in dedicated-server mode) implicitly as part of the create call. Each admission goes through `NetworkHost.AdmitPlayer`, which is also where reconnection is detected (see below).
3. **Idle-but-alive** — a room with connected clients just runs; a room that was `CreateRoom()`'d but has never had a real client (`hadPlayer == false`) is *never* touched by the janitor, no matter how long it sits empty. This is deliberate: an embedded-host app calling `CreateRoom()` at startup and waiting in a lobby screen shouldn't have its room silently disappear before anyone joins.
4. **Empty after having players** — once `hadPlayer` becomes `true` (first real admission), the room becomes subject to `ServerOptions.EmptyRoomGracePeriod`: `Server.sweepEmptyRooms()` (called every 5s from `janitorLoop`) tracks, per room, how long `ConnectedPlayerCount()` has been `0`, and destroys the room only once that duration exceeds the grace period. This exists specifically so reconnection (next section) has a window to work — reaping instantly would make it moot.
5. **Destroyed** — `NetworkHost.Stop()` (halts its loops) plus removal from `Server.rooms` and `Server.peers`.

## Reconnection

`NetworkHost.AdmitPlayer(peer, role)` does three things, in order:

1. If `peer.Key()` already maps to an admitted client (same transport identity re-sending a handshake — e.g. a lost ack being retried), return that client's existing `PlayerID` unchanged, `reconnected=false`, no new hook call. This is the idempotent-retry case, not reconnection.
2. Otherwise, scan connected clients in **this room only** for one that's marked `Connected == false` (i.e. timed out via `heartbeatLoop`, but not yet destroyed — see below) whose stored `Peer.IP()` matches this new peer's IP. If found: reuse that client's `PlayerID`, restore its **original** `Role` (the role in *this* handshake is discarded), mark it connected again with the new `Peer`, `reconnected=true`.
3. Otherwise, this is a genuinely new player: allocate the next `PlayerID`, store the given `Role`, `reconnected=false`.

**Why disconnected clients aren't deleted immediately**: `heartbeatLoop` only ever sets `Connected = false` and removes the stale routing key (`clientsByKey`) — it deliberately leaves the `ClientConnection` struct itself in `NetworkHost.clients` so step 2 above can find it later. This is also why `ConnectedPlayerCount()` filters on `Connected == true` rather than just counting map entries — the map now legitimately holds reclaimable ghosts.

**Known limitation**: matching is by IP alone, because the protocol carries no session token. Two devices behind the same public IP (shared NAT/carrier-grade NAT) that both drop and both try to reconnect at once will race for the same reclaimed identity. This is accepted as "good enough for home/mobile-data networks, not for a fully public deployment" — see [ROADMAP.md](ROADMAP.md) for the real fix (a per-session token issued at handshake time, carried on subsequent packets).

## Reliable delivery

Two independent mechanisms share one retry/ack machinery (`ClientConnection.ReliableQueue`, drained by `sendLoop` every 50ms up to `maxRetries`, cleared by `handleAck`):

- **Reliable Input** (client → server): a client sets `FlagReliable` on an Input packet; the server immediately ACKs it (`sendAck`) — this is a one-shot ack, not retried, since it's the *server* confirming receipt of something the *client* already has.
- **`QueueReliableEvent`** (server → client, game-initiated): builds a standalone Snapshot packet (empty `StatePayload`, one `Event`) per connected client, marked `FlagReliable`, sends it immediately, and enqueues it in that client's `ReliableQueue` for retransmission until that specific client ACKs it. The same event is *also* pushed through the normal `QueueEvent` path (so it rides the very next regular snapshot too) — delivery is at-least-once, not exactly-once, by design: a game event should be an idempotent notification ("a goal happened, resync"), not something whose arrival count matters.

## Hardening added for public exposure

These exist specifically because `Server` is meant to be run as a long-lived, internet-facing process (not just a LAN-local example):

- **`ServerOptions.HandshakeRateLimit`/`Window`**: a fixed-window counter per `Peer.Key()` — not a token bucket, deliberately simple, throttles a single misbehaving source rather than defending against a distributed flood (that needs infrastructure-level mitigation, out of scope for the library).
- **`WebTransportOptions.AllowedOrigins`**: `CheckOrigin` defaults to accepting anything (fine for local dev), but is fully overridable — a production deployment should restrict this to its real domain.
- **`LoadTLSCertificate`**: `GenerateDevCertificate`'s self-signed cert only works with browsers via `serverCertificateHashes` pinning, which is a dev/testing mechanism, not how a real public HTTPS-adjacent service should present a cert. `LoadTLSCertificate` loads a real CA-issued cert/key pair instead.
- **Graceful shutdown** (`Server.Stop()`, wired to `SIGTERM`/`SIGINT` in `ejemplo-tacataca/main.go`): drains rooms and closes transports instead of dropping every connection mid-packet on a redeploy.

None of this touches the wire protocol — a hardened `Server` and a bare-bones one are indistinguishable to a client.

## What's still not generic-core scope (by design)

Per-role capacity limits, what a role *means*, position/state validation ("anti-cheat"), and matchmaking/lobby logic are all deliberately **not** in `networkcore` — `ServerOptions.MaxPlayersPerRoom` is the one generic, role-blind numeric cap the core provides; everything else belongs in the game's own hooks. See [CONTRIBUTING.md](CONTRIBUTING.md) for why this boundary is enforced strictly on any change to the core.
