# ForgeNet

A generic multiplayer networking core, implemented from scratch **in 6 languages** (Go, Rust, Python, C++, TypeScript, C#), sharing one wire protocol and one design principle: **the transport layer knows nothing about the game running on top of it.**

Handshake, heartbeat, input transport, snapshots, reliable delivery, reconnection, room/session management and ping are all handled by the core. Your game's state, your game's events, and what a "player role" means are all opaque to it — you supply those, the core just moves the bytes reliably. This document is for the second case: **you're building a game and want to use ForgeNet as its network layer.**

If you want to understand or modify ForgeNet itself (protocol internals, architecture, roadmap), see [`docs/`](docs/) instead — this README only covers using it.

## What ForgeNet gives you

- A binary UDP (and, in Go, WebTransport/QUIC for browsers) transport with handshake, heartbeat-based disconnect detection, ordered/reliable delivery, and ping/RTT — so you don't write socket code.
- **Rooms**: many concurrent matches multiplexed over one running server, joined by a short code — so "player creates a match, others join with a code" is built in, not something you build on top.
- **Reconnection**: a dropped client that comes back is recognized and rejoins as the same player, not a stranger — built in, not something you build on top.
- **Four hooks + a handful of opaque fields** where your game plugs in. The core never inspects game state, input meaning, event meaning, or role meaning — see [Design principle](#design-principle-the-core-never-interprets-your-game) below for why that's not just an implementation detail.

## The two modes

ForgeNet supports two topologies with **the same protocol, the same client code, and the same room mechanics** — you pick per deployment, not per protocol version:

| | Embedded Host (LAN / offline) | Dedicated Server (online) |
|---|---|---|
| Who runs the server | One of the players' own devices (e.g. a "board" app) | A separate always-on process you deploy |
| How the room is made | The host calls `Server.CreateRoom()` directly at startup — no network round trip, no client needed | A client's handshake creates it (`NetworkClient.CreateRoom`) — the server generates and returns the room code |
| How others join | `NetworkClient.JoinRoom(host, port, role, code)` — `host` is the LAN device's local IP | Same call — `host` is the server's public IP/domain instead |
| Internet required | No | Yes |
| Typical use | Local party play, no backend to run | Play with people not on the same network |

Both paths produce an identical room — same `NetworkHost` instance underneath, same hooks fire, same clients connect the same way. A game can support both by choosing at runtime which one it calls: `Server.CreateRoom()` if hosting locally, or connect out with `NetworkClient.CreateRoom(...)`/`JoinRoom(...)` against a deployed server if playing online. Nothing about your game logic needs to know which mode is active — the hooks and the wire format are identical either way.

```
Embedded Host                         Dedicated Server
┌─────────────────────┐               ┌─────────────────────┐
│ Board app            │               │ Deployed process     │
│  Server.CreateRoom() │               │  (game creates room  │
│  → room exists, no   │               │   via a client's     │
│    client needed     │               │   CreateRoom call)   │
└──────────┬───────────┘               └──────────┬───────────┘
           │ LAN                                    │ Internet
   ┌───────┴───────┐                        ┌───────┴───────┐
   │ NetworkClient │                        │ NetworkClient │
   │ .JoinRoom(... │                        │ .JoinRoom(... │
   │  LAN IP ...)  │                        │  public IP...)│
   └───────────────┘                        └───────────────┘
```

## Design principle: the core never interprets your game

Every hook and payload the core exposes is **opaque** — it stores and forwards bytes/values without ever branching on what they mean. This isn't a style preference; it's what lets the exact same `networkcore` package run a paddle game, a shooter, or a board game without modification, and it's the rule any change to the core itself must preserve (see [`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md)). Concretely:

- `StatePayload` (in every snapshot): raw bytes. The core moves them; only your `state_provider` hook writes them and your client-side decoder reads them.
- `GameEvent.EventType` / `GameEvent.Data`: same deal — you define your own event type table (e.g. `1 = GOAL`, `2 = MATCH_START`) and payload format.
- `Role` (a single opaque byte per connected player): you define what values mean for your game (e.g. `1 = paddle`, `2 = board`) and what to do when a given role connects. The core only ever stores it and hands it back to you.
- `PlayerInput` (`DeltaX`, `DeltaY`, `Rotation`, `Actions`): a generic 2D-delta + rotation + 32-bit action bitmask. The core transports these numbers; what they mean is entirely up to you — see [Repurposing `PlayerInput`](#repurposing-playerinput-a-real-example) below for a real game that packs an entire lobby-selection UI into `Actions`, not just movement.

If you ever find yourself wanting the core to know what a paddle or a goal is, that logic belongs in your game's hooks, not in a fork of `networkcore`.

## Building your own game on top of ForgeNet

This section walks through the integration contract using Go (the reference implementation) for code samples — the shape is the same in every language; see each language folder's own docs for exact method names/casing (`csharp/`, `python/`, etc.) and [`docs/STATUS.md`](docs/STATUS.md) for which languages support which side (server/client).

### 1. Define your room factory and hooks

A `RoomFactory` is called once per room — it returns a fresh `NetworkHost` with your game's hooks attached, so every match has its own independent state:

```go
func newGameRoom() *networkcore.NetworkHost {
    host := networkcore.NewNetworkHost()
    game := newMyGameState() // your own struct — the core never sees inside it

    host.OnPlayerConnected = func(playerID uint16, role uint8, reconnected bool) {
        // role is YOUR value — decide what this connection is allowed to do
        // reconnected==true: this is the same player as before (see §5) —
        // don't reset their state, they're resuming
    }
    host.OnPlayerDisconnected = func(playerID uint16) {
        // mark them gone in your own state; the core keeps their session
        // "reclaimable" for reconnection on its own, you don't manage that
    }
    host.OnInput = func(input networkcore.PlayerInput) {
        // apply movement, or decode packed bits from Actions — see §6
    }
    host.OnTick = func(tick uint64) {
        // step your simulation once per tick (60Hz, matches the core's tick rate)
    }
    host.StateProvider = func() []byte {
        return game.encode() // YOUR binary format — the core just moves these bytes
    }

    return host
}
```

### 2. Start a `Server` with that factory

```go
server := networkcore.NewServer(newGameRoom, networkcore.ServerOptions{
    MaxPlayersPerRoom: 4, // purely numeric cap, doesn't know about roles — see §7
})
server.StartUDP(9999)                                  // native clients (desktop/mobile)
server.StartWebTransport(networkcore.WebTransportOptions{Addr: ":9443"}) // browser clients, optional
```

### 3a. Embedded Host mode: create the room yourself

```go
roomCode := server.CreateRoom() // no client involved — safe to call at app startup
// show roomCode (or a LAN-discovery broadcast of it) to other devices
```

### 3b. Dedicated Server mode: let a client create it

```go
client := networkcore.NewNetworkClient()
err := client.CreateRoom("game.example.com", 9999, myRole, 5*time.Second)
// client.RoomCode is now set — share it (QR, deep link, typed code) for others to JoinRoom
```

### 4. Everyone else joins

```go
client := networkcore.NewNetworkClient()
err := client.JoinRoom(hostOrIP, port, myRole, roomCode, 5*time.Second)
if err != nil {
    if rej, ok := err.(*networkcore.HandshakeRejectedError); ok {
        // rej.Reason == networkcore.ReasonRoomNotFound or ReasonRoomFull
    }
}

client.OnSnapshot = func(s *networkcore.GameSnapshot) {
    // s.StatePayload — decode with YOUR format, s.Events — YOUR event types
}
client.SendInput(deltaX, deltaY, rotation, actions, false) // called every tick from your input loop
```

### 5. Handle reconnection

If a client's connection drops and it calls `JoinRoom` again (same room code) before `ServerOptions.EmptyRoomGracePeriod` (default 30s) elapses, the server recognizes it by IP and hands back the **same `PlayerID`** and the **original `Role`** — `OnPlayerConnected`'s `reconnected` argument tells your game not to treat it as a new player. This is an IP-based heuristic, not a session token — good enough for typical home/mobile-data networks, not airtight against two devices sharing one public IP. See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the exact mechanics and [`docs/ROADMAP.md`](docs/ROADMAP.md) for planned session-token hardening.

### 6. Two ways to send events: best-effort vs reliable

```go
host.QueueEvent(networkcore.GameEvent{PlayerID: id, EventType: myEventGoal, Data: nil})
// goes out in the next snapshot only — fine for anything with a "newer version coming soon"
// (like most game state), NOT fine for a one-off notification that must not be silently lost

host.QueueReliableEvent(networkcore.GameEvent{PlayerID: id, EventType: myEventGoal, Data: nil})
// sent immediately AND retried until the client ACKs it (automatic in every language's client) —
// use this for anything that changes a score/outcome and can't just disappear on packet loss
```

### 7. Role-based capacity is your job, not the core's

`ServerOptions.MaxPlayersPerRoom` is a flat numeric cap — the `Server` counts connections, it doesn't know your role values. If you need "exactly 2 paddles + 1 board," enforce that inside `OnPlayerConnected` by checking `role` and your own per-role counters, the same way `ejemplo-tacataca` does it in every language.

### Repurposing `PlayerInput`: a real example

`PlayerInput` only has four fields (`DeltaX`, `DeltaY`, `Rotation`, `Actions`), but nothing says they have to mean "move" and "spin." A real game built on ForgeNet packs an entire lobby UI (side selection, role, team composition, ready state) into the `Actions` bitmask, sent in the *same* packet as movement, every tick — so a lobby choice never gets lost to packet loss (there's always a newer packet on the way) and never races with movement data:

```
bits 1-2   side: 0=unset, 1=left, 2=right
bits 3-4   role: 0=unset, 1=solo, 2=attack, 3=defense
bit 5      ready
bits 6-20  team composition (three 4-bit counts)
```

`Rotation` (16 bits, otherwise just "spin") got split in half to carry two independent values — left-hand and right-hand spin — since the game only needed one rotation-shaped number of headroom for something else entirely. The point: the core enforces none of this. It transports `DeltaX/DeltaY/Rotation/Actions` as four numbers and calls `OnInput` — what they mean is a decision you make once, in your own hook, same as `StatePayload`.

## API reference

### `NetworkHost` (one room's game-protocol state)

| Member | Purpose |
|---|---|
| `OnPlayerConnected func(playerID uint16, role uint8, reconnected bool)` | Fires on admission — new or reconnecting |
| `OnPlayerDisconnected func(playerID uint16)` | Fires on heartbeat timeout (~10s of silence) |
| `OnInput func(input PlayerInput)` | Fires once per received input packet, in order |
| `OnTick func(tick uint64)` | Fires once per server tick (60Hz) |
| `StateProvider func() []byte` | Called every tick to build the next snapshot's `StatePayload` |
| `QueueEvent(evt GameEvent)` | Best-effort event, next snapshot only |
| `QueueReliableEvent(evt GameEvent)` | Sent now + retried until ACKed (at-least-once — may also still arrive via the next regular snapshot) |
| `GetClientRole(playerID) (role uint8, ok bool)` | Look up a connected player's role outside the connect hook |
| `PendingReliableCount(playerID) int` | Diagnostic: how many reliable packets are still unacked for this player |
| `ConnectedPlayerCount() int` | Currently-connected count (excludes reclaimable-but-disconnected clients) |

### `Server` (multi-room host + transport owner)

| Member | Purpose |
|---|---|
| `NewServer(factory RoomFactory, opts ServerOptions) *Server` | `factory` is called once per room |
| `StartUDP(port uint16) error` | `port=0` binds an OS-assigned ephemeral port |
| `UDPPort() uint16` | Real bound port (useful after `StartUDP(0)`) |
| `StartWebTransport(opts WebTransportOptions) (*DevCertificate, error)` | Browser transport, Go only today |
| `CreateRoom() string` | Direct room creation, no handshake — embedded-host mode (see §3a above) |
| `Stop()` | Drains all rooms and closes transports — call on shutdown |

**`ServerOptions`**

| Field | Default | Purpose |
|---|---|---|
| `RoomCodeLength` | `6` | Length of generated room codes (alphabet excludes ambiguous `0`/`O`/`1`/`I`) |
| `MaxPlayersPerRoom` | `0` (unlimited) | Flat numeric cap — see §7 above for role-aware caps |
| `HandshakeRateLimit` / `HandshakeRateLimitWindow` | `10` / `10s` | Handshake attempts allowed per `Peer` per window before silently dropping the rest |
| `EmptyRoomGracePeriod` | `30s` | How long a room that *had* a player stays alive at zero connections before being destroyed — gives reconnection (§5) a real window. A room made via `CreateRoom()` that never got a player is never subject to this — it waits forever |

**`WebTransportOptions`** (Go only)

| Field | Default | Purpose |
|---|---|---|
| `Addr` | — | e.g. `":9443"` |
| `Path` | `/webtransport` | Endpoint path |
| `TLSConfig` | dev self-signed cert | Pass a real cert (`LoadTLSCertificate(certFile, keyFile)`) for production |
| `AllowedOrigins` | unset (any) | Restrict which web origins may open a session |

### `NetworkClient`

| Member | Purpose |
|---|---|
| `CreateRoom(host, port, role, timeout) error` | Handshake that makes a new room |
| `JoinRoom(host, port, role, roomCode, timeout) error` | Handshake that joins an existing room |
| `PlayerID`, `RoomCode` | Set after a successful handshake |
| `OnSnapshot func(*GameSnapshot)` | Fires on every received snapshot |
| `OnPong func(ms int)` | Fires on ping response |
| `OnClosed func()` | Fires if the connection drops |
| `SendInput(deltaX, deltaY int16, rotation uint16, actions uint32, reliable bool)` | Send this tick's input |
| `SendPing()` | Measure RTT |
| `Disconnect()` | Clean local shutdown |

A failed `CreateRoom`/`JoinRoom` returns a `*HandshakeRejectedError{Reason}` when the server explicitly rejected it (`ReasonRoomNotFound`, `ReasonRoomFull`) — any other error (timeout, DNS) is a plain `error`.

## Status

- All 6 implementations are compiled and actually run (not just written) — see [`docs/STATUS.md`](docs/STATUS.md) for the full per-language capability matrix (server/client role, rooms, reconnection, WebTransport).
- Browser support (WebTransport/QUIC) validated against real Chrome via Playwright: handshake, input, and full game-state decoding in the browser.
- Cross-language interoperability verified with real cross-process tests, not just self-consistency — C# ↔ Go and TypeScript ↔ Go each connect a real client against a real `go/ejemplo-tacataca` subprocess.

## Repository layout

```
go/           server + client — the reference implementation. Accepts UDP
              and WebTransport at once, in the same match; the only
              language with WebTransport support server-side.
rust/         server
python/       server + client
cpp/          server, header-only, C++17, no external dependencies
typescript/   client (Node.js, via dgram)
csharp/       server (embedded host) + client — the real base for Unity
              integration
web/          browser client via WebTransport/QUIC — a browser can't open
              UDP sockets, so it talks to the same go/networkcore over a
              different transport
docs/         engine internals: architecture, wire protocol spec, roadmap
              — read this if you're modifying ForgeNet itself
```

## Quick start (per language)

```bash
# Go — reference server (UDP :9999, WebTransport :9443, HTTP :8080)
cd go && go test ./networkcore/... && go run ./ejemplo-tacataca

# Rust
cd rust && cargo test -p networkcore && cargo run -p ejemplo-tacataca

# Python — self-test host<->client + example game
cd python && python3 test_networkcore.py

# C++ — header-only, POSIX sockets
cd cpp && g++ -std=c++17 -O2 -pthread -o /tmp/test_networkcore tests/test_networkcore.cpp && /tmp/test_networkcore

# TypeScript client — needs go/ejemplo-tacataca already running on :9999
cd go/ejemplo-tacataca && go run . &
cd typescript && npx tsc && node dist/test-networkcore.js

# C# — cross-tests against Go require `go` on PATH
cd csharp && dotnet run --project NetworkCore.Tests

# Web client (WebTransport) — serve go/ejemplo-tacataca, open the browser page
cd web && npm install && npx tsc
cd ../go/ejemplo-tacataca && go run .   # then open http://localhost:8080/
```

Also usable as a real dependency, not just vendored: `go get github.com/mgueregath/ForgeNet/go@latest` (tagged releases follow the `go/vX.Y.Z` convention required for a Go module living in a subdirectory).

## Deploying `go/networkcore` as an online server

`Server` is meant to run continuously and host many concurrent matches — this is what backs `ejemplo-tacataca`, and the reference for Dedicated Server mode (see [The two modes](#the-two-modes)). Configuration is by environment variable, so the same binary runs unmodified in a container:

| Variable | Default | Purpose |
|---|---|---|
| `UDP_PORT` | `9999` | Raw UDP listener port |
| `WEBTRANSPORT_ADDR` | `:9443` | WebTransport/QUIC listener address |
| `HTTP_ADDR` | `:8080` | Serves the browser test page + `/certhash` |
| `WEB_DIR` | `../../web` | Static files for the browser page; skipped (server still runs) if the path doesn't exist |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` | unset | A real CA certificate (e.g. from Let's Encrypt/Certbot run separately). Unset falls back to the self-signed dev certificate, fine for local testing but not production |
| `ALLOWED_ORIGINS` | unset (any) | Comma-separated origins allowed to open a WebTransport session |
| `MAX_PLAYERS_PER_ROOM` | `3` (taca-taca specific) | See §7 above |

The process handles `SIGTERM`/`SIGINT` by draining (`Server.Stop()`) before exiting — needed for `docker stop` or a rolling redeploy to not just kill active matches outright.

```bash
docker build -t forgenet-tacataca .
docker run --rm -p 9999:9999/udp -p 9443:9443/udp -p 8080:8080/tcp forgenet-tacataca
```

## Where to go next

- Modifying or extending ForgeNet itself → [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), [`docs/PROTOCOL.md`](docs/PROTOCOL.md), [`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md)
- What's implemented where → [`docs/STATUS.md`](docs/STATUS.md)
- What's planned → [`docs/ROADMAP.md`](docs/ROADMAP.md)

## License

MIT — see [LICENSE](LICENSE).
