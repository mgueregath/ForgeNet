# ForgeNet

A generic multiplayer networking core, implemented from scratch **in 6 languages** (Go, Rust, Python, C++, TypeScript, C#), sharing the same wire protocol and the same design principle: **the transport layer knows nothing about the game running on top of it.**

Handshake, heartbeat, input, snapshots, reliable delivery and ping are handled by the core. Game state travels as an opaque `StatePayload` — bytes the core moves but never interprets — and the game plugs in through four hooks:

```
on_player_connected / on_player_disconnected
on_input          — raw input per tick (delta_x/delta_y/rotation/actions)
on_tick           — fired once per tick so the game can step its simulation
state_provider    — the game hands back its current state bytes on each snapshot
queue_event       — the game queues its own events (e.g. GOAL) with arbitrary type/data
```

Every language folder ships a `networkcore` library plus a small `tacataca` example (a 2-paddle ball game) built **on top of** the core — proving the same core can host different games without being touched.

`go/networkcore` additionally accepts **two transports at once** — raw UDP and WebTransport/QUIC — so a browser tab and a native (Unity) client can be in the same match, talking to the same host, without the core caring which is which.

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
go/           server — Go is the reference implementation for the classic
              Dedicated Server topology. Accepts UDP and WebTransport at
              once, in the same match.
rust/         server
python/       server + client — the only language with both roles
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

## Roadmap

The near-term goal is packaging this as an embeddable library per engine rather than a standalone example:

1. **Unity**: a thin `MonoBehaviour` wrapper around `csharp/NetworkCore`'s embedded host — instantiate a real server inside the app when a player hosts a match, and a matching client-only mode ("controller" role) that never renders the authoritative state.
2. **Role selection + LAN discovery** at app startup — mDNS/Bonjour/NSD from day one (confirmed necessary for iOS; raw UDP broadcast doesn't reach it).
3. **Godot / Unreal bindings** built the same way: thin native bindings over the existing `cpp/` or language-native cores, no protocol changes required.

## License

MIT — see [LICENSE](LICENSE).
