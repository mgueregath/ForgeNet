# nucleo-multiplayer

Implementación del protocolo UDP genérico de este proyecto — **en 6
lenguajes**, todos con la misma arquitectura: el core de red no sabe nada
del juego que corre encima. Cada lenguaje vive en su propia carpeta, con
su propio `networkcore` (librería) + `ejemplo-tacataca` (o `test`, según el
rol) que demuestra la genericidad. Además, **`go/networkcore` acepta dos
transportes a la vez** (UDP crudo y WebTransport/QUIC), lo que permite que
una página web se conecte directamente al mismo core que usan los clientes
nativos — ver `web/`.

```
nucleo-multiplayer/
  go/           servidor (host) — Go se queda como la referencia para la
                topología Servidor Dedicado clásica. Acepta UDP y
                WebTransport a la vez, en la misma partida.
  rust/         servidor (host)
  python/       servidor (host) + cliente — único lenguaje con ambos roles
  cpp/          servidor (host), header-only
  typescript/   cliente (Node.js, vía dgram) — nunca tuvo servidor
  csharp/       servidor (host embebido) + cliente — la base real para
                taca-taca en Unity (Android + iOS)
  web/          cliente de NAVEGADOR (vía WebTransport, no dgram — un
                browser no puede abrir sockets UDP). No es un lenguaje
                nuevo, es un transporte nuevo sobre el mismo protocolo.
```

## Genericidad: ningún core sabe qué juego corre encima

Todos comparten el mismo diseño: el transporte (handshake, heartbeat,
input, snapshot, reliable, ping) es 100% agnóstico al juego. El estado de
juego viaja como `StatePayload` — bytes opacos que el core transporta pero
nunca interpreta — y el juego se conecta vía 4 hooks:

- `on_player_connected` / `on_player_disconnected`
- `on_input` — recibe cada input crudo (`delta_x/delta_y/rotation/actions`),
  sin ninguna interpretación del core
- `on_tick` — se dispara una vez por tick para que el juego actualice su
  simulación
- `state_provider` — el juego entrega los bytes de su estado actual en
  cada snapshot
- `queue_event` — el juego encola sus propios eventos (ej. `GOAL`) con
  tipo/datos arbitrarios

**Esto no siempre fue así.** La primera versión (solo en C#) tenía
`PlayerState{Health,Ammo}` hardcodeado dentro del core, heredado del
prototipo original de este proyecto (que sí tenía ese esquema fijo en los
6 lenguajes). Se detectó y corrigió — ver
`arquitectura/ARQUITECTURA_MULTIPLAYER_GENERICA.md` — y esa corrección se
aplicó consistentemente al reescribir los otros 5 lenguajes.

En cada carpeta, un ejemplo `TacaTacaState`/`TacaTacaGame` (Ball/Rod/Score)
construido **encima** del core, no dentro de él, demuestra que el diseño
funciona: si mañana hay otro juego con otro esquema, se escribe una clase
análoga sin tocar `networkcore`.

## Por qué 6 lenguajes en vez de uno solo

- **Go** es la referencia principal y se queda como la implementación de
  la topología **Servidor Dedicado clásica** (multi-sala, muchos clientes,
  un proceso centralizado) — decisión de lenguaje confirmada, no se
  reemplaza por C#/Unity headless para ese caso.
- **C#** es necesario porque taca-taca usa la topología **Host Embebido**:
  el servidor corre *dentro* de la app Unity que renderiza el tablero (no
  puede ser un proceso Go separado), y el cliente Unity (el "mando")
  siempre necesita hablar el protocolo en C#.
- **Rust, Python, C++, TypeScript**: existían en el prototipo original
  (comparación de lenguajes, benchmarking) y se reescribieron a la misma
  arquitectura genérica para no dejar implementaciones "de segunda"
  desactualizadas en el repo.
- **`client_csharp_unity.cs` del prototipo original quedó retirado, no
  reescrito** — `csharp/NetworkCore` ya cubre el rol cliente en C# de
  forma genérica y validada; una tercera variante sería pura duplicación.

## Compatibilidad entre lenguajes: verificada, no asumida

Todos comparten el **framing** (header, handshake, input, ack, ping,
heartbeat) — y ahora, al ser todos genéricos, también comparten el
**envelope de snapshot** (`Tick + StatePayloadLen + StatePayload +
EventCount + Events`). Lo que cada juego pone *dentro* de `StatePayload`
es su propio esquema (ej. taca-taca), pero el transporte es
interoperable entre lenguajes.

Esto está probado con cross-tests reales entre implementaciones, no solo
autoconsistencia:

- **C# ↔ Go**: `csharp/NetworkCore.Tests` levanta `go/ejemplo-tacataca`
  como subproceso real y conecta el `NetworkClient` de C# contra él —
  handshake, ping, y decodificación del snapshot genérico, todo verificado.
- **TypeScript ↔ Go**: `typescript/test-networkcore.ts` conecta contra
  `go/ejemplo-tacataca` corriendo por separado — handshake, ping, y
  decodificación completa del esquema taca-taca (`Ball/Rod/Score`)
  codificado por Go y decodificado por TS, byte a byte.

## Heartbeat: el mismo bug, corregido en 4 lenguajes

Antes de reescribir Rust/Python/C++ a la arquitectura genérica, se
encontró que las tres tenían el mismo bug que ya habíamos corregido en Go:
el campo de última actividad nunca se refrescaba en los handlers de
input/ping, así que cualquier cliente se desconectaba a los ~10s sin
importar cuánto tráfico mandara. Los 4 cores nuevos (Go, Rust, Python, C++)
tienen el fix desde el diseño, y cada uno tiene un test de regresión
específico (loop de 12s de actividad, verifica que el cliente siga
conectado) — ver la sección de estado por lenguaje abajo.

---

## Estado por lenguaje (todos compilados y corridos de verdad)

### Go (`go/`)

```
go/
  networkcore/          librería (handshake, heartbeat, input, snapshot,
                        reliable, ping + hooks genéricos)
    host.go                    lógica del protocolo — agnóstica al
                               transporte, habla con Peer (interfaz)
    transport_udp.go            Peer para UDP crudo (clientes nativos)
    transport_webtransport.go   Peer para WebTransport/QUIC (navegador) +
                                generador de certificado dev
    host_test.go                 2 tests, incl. heartbeat (12s)
    webtransport_test.go         2 tests: WebTransport solo, y UDP +
                                 WebTransport conectados al mismo host
                                 A LA VEZ (misma partida, distinto Peer)
  ejemplo-tacataca/     binario que engancha Ball/Rod/Score al core,
                        escuchando UDP (:9999) y WebTransport (:9443) a
                        la vez, más un server HTTP (:8080) que sirve la
                        página de prueba de web/
```

```bash
cd nucleo-multiplayer/go
go test ./networkcore/...        # 4/4 PASS (2 UDP + 2 WebTransport)
go run ./ejemplo-tacataca         # UDP :9999, WebTransport :9443, HTTP :8080
```

**Peer: la abstracción que permite dos transportes en el mismo host.**
`ClientConnection.Addr *net.UDPAddr` se generalizó a `ClientConnection.Peer`
(interfaz con `Send([]byte)` + `Key() string`). `host.go` no sabe si un
paquete vino de un socket UDP o de una sesión WebTransport — cualquier cosa
que implemente `Peer` puede jugar. Esto es lo que permite que un cliente
Unity (UDP) y un cliente de navegador (WebTransport) terminen en la misma
partida sin que el core note la diferencia — verificado en
`TestUDPAndWebTransportSamePlayerSpace`.

### Rust (`rust/`)

```
rust/
  networkcore/          crate (mismo diseño que Go, con EventQueue
                        clonable para encolar eventos desde hooks
                        configurados antes de start())
  networkcore/tests/    2 tests de integración, incl. heartbeat (12s)
  ejemplo-tacataca/     binario análogo al de Go
```

```bash
cd nucleo-multiplayer/rust
cargo test -p networkcore        # 2/2 PASS
cargo run -p ejemplo-tacataca
```

Nota de diseño: `queue_event` no puede vivir solo en el "handle" que
devuelve `start()`, porque los hooks (`on_input`, etc.) se configuran
*antes* de arrancar. Se resolvió con `EventQueue`, una cola compartible
que se obtiene con `NetworkHost::events()` antes de `start()` y se clona
dentro de los hooks que la necesiten.

### Python (`python/`)

Único lenguaje con **ambos roles** (servidor y cliente) en este proyecto.

```
python/
  networkcore.py         NetworkHost + NetworkClient
  ejemplo_tacataca.py     TacaTacaGame enganchado al host
  test_networkcore.py     3 baterías, 18 checks — self-test host<->client
                          + heartbeat (12s) + ejemplo taca-taca
```

```bash
cd nucleo-multiplayer/python
python3 test_networkcore.py      # 18/18 PASS
```

### C++ (`cpp/`)

Header-only, C++17, sin dependencias externas (POSIX sockets + stdlib —
se dejó de usar zstd, que estaba en el prototipo original pero ningún
cliente implementaba la descompresión del lado receptor, un bug latente).

```
cpp/
  networkcore/networkcore.hpp   header-only, hooks vía std::function
  ejemplo-tacataca/main.cpp
  tests/test_networkcore.cpp    2 baterías, 15 checks, incl. heartbeat (12s)
```

```bash
cd nucleo-multiplayer/cpp
g++ -std=c++17 -O2 -pthread -o /tmp/test_networkcore tests/test_networkcore.cpp
/tmp/test_networkcore                # 15/15 PASS
```

### TypeScript (`typescript/`)

Solo cliente (nunca tuvo servidor en este proyecto). Su validación es un
cross-test real contra Go — no autoconsistencia, porque no hay host propio.

```
typescript/
  networkcore.ts          NetworkClient
  ejemplo-tacataca.ts       decodificador del esquema Ball/Rod/Score
  test-networkcore.ts       requiere que go/ejemplo-tacataca ya esté
                            corriendo en :9999 (spawnear procesos desde
                            Node no es confiable en todos los entornos)
```

```bash
cd nucleo-multiplayer/go/ejemplo-tacataca && go run . &   # en otra terminal
cd nucleo-multiplayer/typescript
npx tsc && node dist/test-networkcore.js                  # 7/7 PASS
```

### C# (`csharp/`)

Es la base real para taca-taca (topología Host Embebido). Ver detalle
completo más abajo.

```
csharp/
  NetworkCore/            librería (netstandard2.1, sin UnityEngine)
  NetworkCore.Tests/       4 baterías, 22 checks, incl. cross-test contra
                          go/ejemplo-tacataca real (framing + envelope de
                          snapshot genérico, ahora que ambos lados lo son)
```

```bash
export PATH="$PATH:/opt/homebrew/opt/dotnet/bin"   # si dotnet no está en PATH
cd nucleo-multiplayer/csharp
dotnet run --project NetworkCore.Tests             # 22/22 PASS
```

Requiere `go` en el PATH para el Test 3 (cross-test) — si no está
disponible, ese test específico falla pero los otros tres igual corren.

### Web / navegador (`web/`)

No es un lenguaje nuevo — es un **transporte** nuevo (WebTransport/QUIC en
vez de UDP crudo, porque un navegador no puede abrir sockets UDP) sobre el
mismo protocolo, hablando con `go/networkcore` vía su nuevo `Peer` de
WebTransport.

```
web/
  networkcore.ts        NetworkClient para navegador (WebTransport en vez
                        de dgram — dgram es Node-only, no existe en el browser)
  ejemplo-tacataca.ts     decodificador del esquema Ball/Rod/Score
  index.html               página de prueba
```

```bash
cd nucleo-multiplayer/web && npm install && npx tsc
cd ../go/ejemplo-tacataca && go run .   # sirve UDP :9999, WebTransport :9443, HTTP :8080
# abrir http://localhost:8080/
```

Validado con **Chrome real** (Playwright, no solo compilación): 7/7 checks
— handshake WebTransport real, input real, decodificación del esquema
taca-taca completo en el navegador, ping/pong real. Ver `web/README.md`
para el detalle (incluye por qué WebTransport y no WebSocket, y las
limitaciones del certificado de desarrollo).

## Qué falta (ver `arquitectura/ARQUITECTURA_MULTIPLAYER_GENERICA.md` §5-6)

1. Wrapper "Embedded Host" dentro de un proyecto Unity real: un
   `MonoBehaviour` delgado que instancia `NetworkHost` (de `csharp/`) en
   la escena del tablero cuando el usuario elige "Crear mesa", enganchando
   el esquema real de taca-taca con física de verdad (no el placeholder
   de los ejemplos).
2. Cliente "mando": UI de control que usa `NetworkClient`, sin renderizar
   el tablero.
3. Selector de rol al iniciar la app.
4. Descubrimiento en LAN — con Bonjour/NSD desde el día uno (Android + iOS
   confirmados, broadcast UDP crudo no alcanza para iOS).
