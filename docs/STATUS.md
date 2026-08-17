# Implementation status

What each language actually has, as of this writing — not aspirational. "✅ tested" means there's an automated test exercising it in that language's own test suite, run and passing; a blank cell means the feature doesn't apply to that language's role (e.g. a client-only language has no room-creation-side features).

| Feature | Go | Rust | Python | C++ | C# | TypeScript | web |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Server role | ✅ | ✅ | ✅ | ✅ | ✅ (embedded host) | — | — |
| Client role | ✅ | — | ✅ | — | ✅ | ✅ | ✅ |
| Rooms (create/join by code) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ (client side) | ✅ (client side) |
| `Server.CreateRoom()` (embedded-host, no handshake) | ✅ | ✅ | ✅ | ✅ | ✅ | n/a | n/a |
| IP-based reconnection | ✅ | ✅ | ✅ | ✅ | ✅ | n/a (server-side feature) | n/a |
| `EmptyRoomGracePeriod` | ✅ | ✅ | ✅ | ✅ | ✅ | n/a | n/a |
| Ephemeral port bind + readback | ✅ | ✅ | ✅ | ✅ | ✅ | n/a | n/a |
| `QueueReliableEvent` (server) + auto-ack (client) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ (client auto-ack) | ✅ (client auto-ack) |
| Handshake rate limiting | ✅ | — | — | — | — | n/a | n/a |
| WebTransport/QUIC transport | ✅ (server) | — | — | — | — | — | ✅ (client) |
| `AllowedOrigins` / real-cert loading | ✅ | n/a | n/a | n/a | n/a | n/a | n/a |
| Env-configured deploy + graceful shutdown | ✅ (`ejemplo-tacataca`) | — | — | — | — | n/a | n/a |
| Dockerfile | ✅ | — | — | — | — | n/a | n/a |
| Heartbeat-refresh regression test | ✅ | ✅ | ✅ | ✅ | ✅ | n/a | n/a |
| Cross-process interop test against Go | — (is the reference) | — | — | — | ✅ (spawns `go/ejemplo-tacataca`) | ✅ (connects to a running one) | — (untested in a real browser) |

## Why the gaps are where they are

- **Rust and C++ have no client role** — this project never needed one for them (their example apps are server-only); nothing prevents adding one, it just hasn't been asked for.
- **Handshake rate limiting is Go-only** — it was added specifically because Go's `Server` is the one meant to run as a long-lived public process today. Straightforward to port if/when another server-role language needs the same public-exposure hardening (see [ROADMAP.md](ROADMAP.md)).
- **WebTransport is Go-server / web-client only** — no other language has a QUIC/HTTP-3 implementation wired in; porting it to e.g. Rust or C# would mean pulling in that language's own QUIC library, a much bigger lift than the other features in this table.
- **web/'s WebTransport path has never been exercised against a real browser** since the room/reconnection protocol update — it compiles and mirrors the already-cross-tested `typescript/` client closely, but hasn't had an actual Chrome-driven test run against it (unlike the original WebTransport transport itself, which *was* validated with real Chrome via Playwright before the room protocol existed).
- **No language besides Go has env-config/graceful-shutdown/Dockerfile** — these are deploy concerns tied to *running* `ejemplo-tacataca` as a service, which only Go's example does today.

## Cross-language wire compatibility

All 6 languages share the exact byte-for-byte protocol in [PROTOCOL.md](PROTOCOL.md) — verified two ways:
1. Each language's own test suite talks to itself (self-consistency).
2. C# and TypeScript additionally talk to a **real Go process** (`go/ejemplo-tacataca`), decoding its actual snapshot bytes — this is the strongest evidence the protocol doc and the Go reference haven't drifted from what other languages assume. Rust, Python, and C++ currently only have self-consistency tests; adding a Go cross-test for each (same pattern as C#/TypeScript) is a good first contribution if you're looking for one — see [CONTRIBUTING.md](CONTRIBUTING.md).
