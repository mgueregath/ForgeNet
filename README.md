# ForgeNet

A generic multiplayer networking core, implemented from scratch **in 6 languages** (Go, Rust, Python, C++, TypeScript, C#), sharing the same wire protocol and the same design principle: **the transport layer knows nothing about the game running on top of it.**

Handshake, heartbeat, input, snapshots, reliable delivery and ping are handled by the core. Game state travels as an opaque `StatePayload` — bytes the core moves but never interprets — and the game plugs in through four hooks, plus one opaque field:

```
on_player_connected(player_id, role) / on_player_disconnected(player_id)
                  — role is an opaque byte the core stores and forwards,
                    never interprets: each game defines what its values
                    mean (e.g. taca-taca uses one for "paddle", another
                    for "board")
on_input          — raw input per tick (delta_x/delta_y/rotation/actions)
on_tick           — fired once per tick so the game can step its simulation
state_provider    — the game hands back its current state bytes on each snapshot
queue_event       — the game queues its own events (e.g. GOAL) with arbitrary type/data
```

Every language folder ships a `networkcore` library plus a small `tacataca` example (a 2-paddle ball game) built **on top of** the core — proving the same core can host different games without being touched.

`go/networkcore` additionally accepts **two transports at once** — raw UDP and WebTransport/QUIC — so a browser tab and a native (Unity) client can be in the same match, talking to the same host, without the core caring which is which.

`go/networkcore` also multiplexes many concurrent matches over one running server (`Server`, in `server.go`), in either of two ways — both produce the exact same kind of room, joined the exact same way, so a game can pick per-deployment, not per-protocol:

- **Client-created (online mode)**: a client's handshake creates a new room (server generates and returns a short join code) or joins an existing one by that code. This is what an always-on public `Server` uses — one deployed process hosting many simultaneous matches, none of them pre-existing until a client asks for one. The join code is plain data (a short string), so turning it into a QR code, a deep link, or a typed-in code is entirely up to whatever renders it client-side.
- **Host-created (embedded/LAN mode)**: `Server.CreateRoom()` makes a room directly, with no client and no network round trip — for an app that *is* the host (e.g. a board on the local network) and wants one fixed room from the moment it starts, instead of waiting for someone to "create" it. A room made this way is immune to the empty-room janitor until it admits its first real client, so it can sit open in a lobby indefinitely.

Everything downstream — `JoinRoom`, input, snapshots, reconnection — works identically regardless of which path created the room.

`go/networkcore` also has a `NetworkClient` (in `client.go`) — Go isn't server-only anymore. Any Go process can `CreateRoom`/`JoinRoom` against a `Server` (this one or another language's), not just host one, which matters for an app that needs to be either side (e.g. embed a `Server` for local/offline play, or connect out as a `NetworkClient` to a deployed one for online play, picking at runtime).

Reconnection is built into `Server`/`NetworkHost`: if a client's connection drops (missed heartbeats) and a new handshake arrives from the same IP into the same room before `ServerOptions.EmptyRoomGracePeriod` elapses, it's treated as the same player — same `PlayerID`, same original `Role` — instead of a stranger joining fresh (`OnPlayerConnected`'s `reconnected` argument tells the game which case it is). This is an IP-based heuristic, not a session token (the protocol doesn't carry one yet), so it's not airtight against two devices sharing one public IP (NAT) both dropping at once — good enough for typical home/mobile-data networks, not a substitute for real session auth on a fully public deployment.

## Why this exists

Most open-source netcode libraries commit early to one engine and one language (Mirror and FishNet to Unity/C#, Netcode for GameObjects the same, ENet to a C ABI you bind per-engine). ForgeNet inverts that: the protocol and the state machine are specified once, then implemented natively per language/runtime, so integrating it into a new engine is "write a thin binding," not "port a library."

- **Go** is the reference implementation and the target for classic **Dedicated Server** topology (multi-room, many clients, one centralized process).
- **C#** targets **Embedded Host** topology — the server runs *inside* the client app (e.g. a Unity build hosting a local match), which is why this is the real base for the Unity integration.
- **Rust, Python, C++, TypeScript** cover server and/or client roles for embedding into engines and tools built on those runtimes (e.g. native plugins, headless simulation, tooling, Node-based clients).
- **Web** is not a language, it's a transport: a browser can't open raw UDP sockets, so `web/` talks to `go/networkcore` over WebTransport/QUIC instead — same protocol, different `Peer`.

## Status

- All 6 implementations are compiled and actually run (not just written) — see the per-language breakdown below.
- Browser support (WebTransport/QUIC) validated against real Chrome via Playwright, not just compiled: handshake, input, and full game-state decoding in the browser.
- Cross-language interoperability verified with real cross-process tests, not just self-consistency:
  - **C# ↔ Go**: `csharp/NetworkCore.Tests` spawns `go/ejemplo-tacataca` as a real subprocess and connects a C# `NetworkClient` against it.
  - **TypeScript ↔ Go**: `typescript/test-networkcore.ts` connects against a running `go/ejemplo-tacataca`, decoding the full example schema byte-for-byte across languages.
- A heartbeat bug (last-activity timestamp never refreshed, causing disconnects after ~10s regardless of traffic) was found and fixed across 4 languages (Go, Rust, Python, C++), each with its own regression test.

## Repository layout

```
go/           server + client — Go is the reference implementation for the
              classic Dedicated Server topology. Accepts UDP and
              WebTransport at once, in the same match; NetworkClient lets
              any Go process connect out to a Server too (this one or
              another language's).
rust/         server
python/       server + client — the only other language with both roles
cpp/          server, header-only, C++17, no external dependencies
typescript/   client (Node.js, via dgram)
csharp/       server (embedded host) + client — the real base for Unity
              integration
web/          browser client via WebTransport/QUIC — a browser can't open
              UDP sockets, so it talks to the same go/networkcore over a
              different transport
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

Full per-language detail (test counts, design notes, known caveats) lives inline in each folder; see `csharp/`, `go/`, etc.

## Running `go/networkcore` as an online server

`Server` (in `go/networkcore/server.go`) is meant to run continuously and host many concurrent matches — this is what backs `ejemplo-tacataca`. Configuration is by environment variable, so the same binary runs unmodified in a container:

| Variable | Default | Purpose |
|---|---|---|
| `UDP_PORT` | `9999` | Raw UDP listener port |
| `WEBTRANSPORT_ADDR` | `:9443` | WebTransport/QUIC listener address |
| `HTTP_ADDR` | `:8080` | Serves the browser test page + `/certhash` |
| `WEB_DIR` | `../../web` | Static files for the browser page; skipped (server still runs) if the path doesn't exist |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` | unset | A real CA certificate (e.g. from Let's Encrypt/Certbot run separately) for WebTransport. Unset falls back to the self-signed dev certificate (`GenerateDevCertificate`), which is fine for local testing but not for production — browsers only accept it via the `serverCertificateHashes` pinning dance, not as a trusted CA |
| `ALLOWED_ORIGINS` | unset (any) | Comma-separated origins allowed to open a WebTransport session. Leaving this unset accepts any origin, which is fine for local dev but means any web page could open a session against a public deployment |
| `MAX_PLAYERS_PER_ROOM` | `3` (taca-taca specific: 2 paddles + 1 board) | Generic numeric cap per room, enforced by `Server` regardless of role |

Also built in: a handshake rate limit per `Peer` (`ServerOptions.HandshakeRateLimit`, default 10 attempts/10s — throttles a broken/looping client or a simple single-source flood, not a distributed one); `QueueReliableEvent` for game events that must survive real packet loss (retransmitted via the same reliable-delivery machinery used for input, until the client ACKs — Go's own `NetworkClient` handles this ACK automatically); and `ServerOptions.EmptyRoomGracePeriod` (default 30s) — a room that had players but is momentarily at zero doesn't get torn down instantly, so a dropped connection has a real window to reconnect (see IP-based reconnection above) before its room disappears.

The process handles `SIGTERM`/`SIGINT` by draining (`Server.Stop()`, which stops every room's loops and closes the transports) before exiting — needed for `docker stop` or a rolling redeploy to not just kill active matches outright.

```bash
docker build -t forgenet-tacataca .
docker run --rm -p 9999:9999/udp -p 9443:9443/udp -p 8080:8080/tcp forgenet-tacataca
```

## Roadmap

The near-term goal is packaging this as an embeddable library per engine rather than a standalone example:

1. **Unity**: a thin `MonoBehaviour` wrapper around `csharp/NetworkCore`'s embedded host, for offline/LAN play — instantiate a real server inside the app when a player hosts a match, and a matching client-only mode ("controller" role) that never renders the authoritative state.
2. **Online play**: a `NetworkClient` mode that connects to a standalone `go/networkcore` `Server` deployment instead of an embedded host — for taca-taca specifically, the board becomes a client like any paddle controller (just a different `role` value), creates a room on start, and turns the returned join code into a QR for the paddles to scan. `Server` itself is now hardened for this (see above) and Go now has a real `NetworkClient` to do this from; reconnection by IP is also built in now (see above). Still missing before real public exposure: per-session auth tokens (raw UDP still has none — a client's identity is its transport address, which can be spoofed; the IP-based reconnection heuristic isn't a substitute for real session auth either).
3. **Role selection + LAN discovery** at app startup — mDNS/Bonjour/NSD from day one (confirmed necessary for iOS; raw UDP broadcast doesn't reach it).
4. **Godot / Unreal bindings** built the same way: thin native bindings over the existing `cpp/` or language-native cores, no protocol changes required.

## License

MIT — see [LICENSE](LICENSE).
