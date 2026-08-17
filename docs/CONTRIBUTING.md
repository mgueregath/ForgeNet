# Contributing to ForgeNet

## The one rule that overrides all others: the core stays generic

`networkcore` (in any language) must never reference a specific game's concepts — no `paddle`, `board`, `goal`, `rod`, `score`, or anything like it, anywhere in the core library. Every example app (`ejemplo-tacataca` in each language) exists specifically to prove the core doesn't need to know these things; a change that makes the core aware of one defeats the entire point of the project (see the README's [Design principle](../README.md#design-principle-the-core-never-interprets-your-game)).

This has bitten real proposed changes before. The concrete pattern to follow when a feature seems to need game-awareness:

- The core exposes an **opaque, generic primitive** (an untyped byte, a length-prefixed blob, a numeric ID) that it stores and forwards without interpreting — `Role`, `StatePayload`, `GameEvent.EventType`/`Data`, `PlayerInput.Actions` are all this pattern.
- The core exposes **hooks/getters** so a game can read that primitive and decide what to do — `OnPlayerConnected`, `OnInput`, `StateProvider`, `GetClientRole` are all this pattern.
- Anything that requires knowing what a value *means* — what's a valid position, what a role implies about capacity, what counts as a win — belongs in the example/game layer, never in the core.

If you're not sure whether a change belongs in the core or the example, ask: "would a completely different game (a shooter, a card game, a racer) also need this, without knowing what it's *for*?" If yes, it's core. If the answer depends on knowing it's specifically taca-taca, it's example code.

## Wire compatibility across languages is load-bearing

All 6 languages implement the exact same protocol (see [PROTOCOL.md](PROTOCOL.md)) — this isn't a style choice, it's what lets a C# Unity client, a browser tab, and a Go-hosted match all interoperate. Any change to the wire format (new packet type, new flag, new field in an existing payload) must:

1. Be written up in `PROTOCOL.md` first — that document, not any one language's source, is the spec other implementations should match.
2. Land in Go first (the reference implementation), with tests.
3. Get ported to every other language that has the relevant role (server-role languages for a server-side change, client-role languages for a client-side one — see [STATUS.md](STATUS.md) for who has which role today).
4. Each port gets **independently built and tested**, not just visually diffed against Go — a change that "looks right" but doesn't compile or doesn't pass that language's test suite is not done. If you're reviewing someone else's port (including an AI agent's), rebuild and rerun it yourself; a report saying tests passed is a claim to verify, not a fact to accept.

A feature that's purely internal to one language's server implementation (not wire-visible — e.g. how a room's internal map is locked) doesn't need this treatment. When in doubt, check whether the change affects anything in `PROTOCOL.md`; if it does, it needs the full port.

## Adding a new language

1. Implement `protocol.go`'s framing exactly (header, packet types, flags — see PROTOCOL.md) — get a round-trip self-test passing before anything else.
2. Implement the server role (`NetworkHost` + `Server` equivalents) and/or client role (`NetworkClient` equivalent), matching the method shapes in the README's [API reference](../README.md#api-reference), adapted to your language's idioms (naming convention, error handling style, concurrency model) — don't force Go's exact API shape where it fights the language (e.g. Rust's `NetworkHostHandle` being `Clone` instead of matching Go's raw pointer reuse, because `start()` consumes `self` in Rust's ownership model — see that port's own commit message for the reasoning).
3. Write an `ejemplo-tacataca` equivalent that demonstrates: role-based connection handling (a "board" and a "paddle" role, matching the other languages' example so cross-language testing is meaningful), the four hooks, and reliable events.
4. If your language can act as a client, add a cross-language test against a real `go/ejemplo-tacataca` process (see C#'s `NetworkCore.Tests/Program.cs` Test 3, or `typescript/test-networkcore.ts`) — this is the strongest evidence your port is actually wire-compatible, not just internally consistent.
5. Update [STATUS.md](STATUS.md)'s matrix and the README's repository layout.

## Verifying changes before committing (for humans and agents alike)

- Run each affected language's actual build and test command — every claim in this codebase's commit history ("X/X tests pass") should be reproducible by re-running the command in the commit message, not taken on faith.
- Check for genericity leaks mechanically, not just by reading: `grep -nio "board\|paddle\|goal\|rod" <core files>` should only ever match illustrative comments (e.g. "ej. GOAL" explaining what a game *might* do with `QueueEvent`), never actual logic. This exact check has been used to verify every language port in this project's history.
- If a change touches the wire protocol, confirm `PROTOCOL.md` was updated to match — a code change without a spec update is how implementations drift.
