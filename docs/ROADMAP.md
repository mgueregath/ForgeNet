# Roadmap

Ordered roughly by what unblocks the most, not by calendar. Each item notes why it matters and what "done" looks like, so a contributor can pick one up without archaeology. See [STATUS.md](STATUS.md) for exactly what exists today, and [PROTOCOL.md](PROTOCOL.md)/[ARCHITECTURE.md](ARCHITECTURE.md) before touching the core.

## 1. Session tokens (replace IP-based reconnection and open the door to real auth)

**Why**: reconnection today (`AdmitPlayer`, see ARCHITECTURE.md) matches on source IP alone, because the protocol carries no session identifier. This breaks down when two devices share a public IP (carrier-grade NAT, a shared home connection) and both drop at once — and separately, nothing stops a third party from spoofing a UDP source address to impersonate an existing player's identity, since there's no secret involved anywhere.

**Shape of the fix**: the Handshake success response already has room to grow (see PROTOCOL.md) — add a server-generated token to it, require it on some/all subsequent packets (a header extension, or a dedicated field), and match reconnection on token-presented rather than IP-observed. This is a **wire protocol change**, so it needs the same "port to all 6 languages, verify each independently" treatment the room/role protocol got — budget for that, not just the Go side.

**Done when**: reconnection no longer depends on `Peer.IP()`, and a packet claiming to be from player X without knowing their token is rejected.

## 2. Port `HandshakeRateLimit` to the other server-role languages

**Why**: Rust, Python, and C++ can all run as long-lived servers the same way Go's `ejemplo-tacataca` does, but only Go has handshake flood protection. Anyone deploying one of those as a public server today is doing so without it.

**Done when**: `ServerOptions` in Rust/Python/C++ has the same rate-limit fields and behavior as Go's, with a test mirroring Go's pattern.

## 3. Cross-language interop tests for Rust, Python, C++ (against Go)

**Why**: only C# and TypeScript currently prove wire compatibility against a *real* Go process (see STATUS.md); Rust/Python/C++ only test self-consistency. Protocol drift in one of those three could go unnoticed until someone actually mixes clients in production.

**Done when**: each has a test analogous to `csharp/NetworkCore.Tests/Program.cs`'s Test 3 or `typescript/test-networkcore.ts` — spawn or connect to `go/ejemplo-tacataca` and decode its real snapshot bytes.

## 4. web/'s WebTransport path validated against a real browser

**Why**: the room/reconnection protocol update to `web/networkcore.ts` compiles and mirrors the already-verified `typescript/` client, but has never actually been driven by a real browser (Chrome via Playwright, the same way the underlying WebTransport transport itself was originally validated — see STATUS.md). It's the one path in the whole matrix running on trust rather than a passing test.

**Done when**: a Playwright (or similar) test exercises `createRoom`/`joinRoom`/input/snapshot decode/reliable-event-ack in a real browser against a real `go/ejemplo-tacataca`.

## 5. A playable UI on top of `web/`

**Why**: `web/index.html` today is a protocol test page (handshakes, decodes state) — it has no controls wired to `sendInput()`. Nothing architectural blocks a fully browser-based player (see the room/reconnection work — a browser client can already `createRoom`/`joinRoom`/`sendInput` for real); what's missing is UI.

**Done when**: taca-taca (or another example) is actually playable end-to-end from a browser tab, not just decodable.

## 6. Real TLS/ACME automation for WebTransport deployments

**Why**: `LoadTLSCertificate` (see README) covers loading an already-issued cert, but nothing in ForgeNet automates *getting* one (Let's Encrypt/ACME renewal). Today that's entirely an ops concern left to whoever deploys — reasonable for a library, but worth documenting a recommended pattern (e.g. a sidecar/reverse-proxy doing ACME termination) rather than leaving it fully open-ended.

**Done when**: `docs/` (or the README) has a concrete, tested recipe for a real public WebTransport deployment with a trusted cert, not just the pieces.

## 7. A lobby/matchmaking HTTP front door (optional, evaluate demand first)

**Why**: today, joining a Dedicated Server room requires already knowing host:port and a room code obtained out-of-band (QR, deep link, typed in). A lightweight HTTP API (list public rooms, request a match, get connection info back) would help games that want browsable/matchmade rooms instead of always-invite-only ones. This is explicitly **not** something to build into `networkcore` itself (would violate the generic-core boundary — a "public room list" is a policy decision, not a transport concern) — it'd be a separate, optional service that talks to `Server` from the outside.

**Done when**: there's a reference implementation (even minimal) of such a front door, kept clearly separate from the core.

## 8. Engine bindings: Godot, Unreal

**Why**: the README's "why this exists" pitch is "write a thin binding, not port a library" — Unity (via `csharp/`) is the only engine that actually has that binding today (see the Unity item below). Godot and Unreal would be the next proof points that the pitch holds.

**Done when**: a thin native binding over `cpp/networkcore.hpp` (or a language-native core) exists for at least one of the two, with a playable example.

## 9. Unity integration (`MonoBehaviour` wrapper)

**Why**: `csharp/NetworkCore` is the base for this, and is feature-complete for it (embedded host + client, rooms, reconnection), but there's no actual Unity project wrapping it yet — this is packaging/integration work, not core work.

**Done when**: a real Unity project can drop in `csharp/NetworkCore`, instantiate an embedded host on "host a match," and a client-only "controller" mode that never renders authoritative state.

---

## Explicitly not planned (and why)

- **Packet compression** (`FlagCompressed` is reserved in the protocol but unimplemented everywhere): no language currently needs it; implementing it without a real workload to validate against risks guessing wrong at the tradeoffs. Revisit if a concrete game's `StatePayload` size becomes a problem.
- **Anti-cheat / input validation in the core**: deliberately out of scope — see [ARCHITECTURE.md § What's still not generic-core scope](ARCHITECTURE.md#whats-still-not-generic-core-scope-by-design). Any "is this input plausible" logic belongs in a game's own `OnInput` hook, not in `networkcore`.
