# web/

Cliente de **navegador** para `nucleo-multiplayer`, vía [WebTransport](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport_API)
(QUIC) — la única forma real de que una página web hable con el core, porque
un navegador no puede abrir sockets UDP crudos.

No es un lenguaje nuevo del mismo tipo que `go/`, `rust/`, etc. — es un
**transporte** nuevo para el mismo protocolo. El servidor (`go/networkcore`)
ahora puede aceptar clientes UDP (Unity) y clientes WebTransport (navegador)
**a la vez, en la misma partida** — ver
`go/networkcore/webtransport_test.go::TestUDPAndWebTransportSamePlayerSpace`.

## Por qué WebTransport y no WebSocket

WebTransport expone datagramas **no confiables** (como UDP: se pueden
perder, pueden llegar desordenados) además de streams confiables —
WebSocket es TCP puro, siempre confiable y ordenado, con el head-of-line
blocking que eso implica para un juego en tiempo real. Cada datagrama
WebTransport mapea 1:1 a un paquete de nuestro protocolo.

Soporte de navegador: baseline en los 4 motores principales (Chrome, Edge,
Firefox, Safari) desde marzo 2026.

## Estructura

```
web/
  networkcore.ts        NetworkClient (equivalente browser de
                        typescript/networkcore.ts, pero con WebTransport
                        en vez de dgram — dgram es Node-only)
  ejemplo-tacataca.ts     decodificador del esquema Ball/Rod/Score
  index.html               página de prueba: conecta, manda input, muestra
                           el estado decodificado
  tsconfig.json            target navegador (lib DOM, sin tipos de Node)
```

## Cómo probarlo

1. Compilar el cliente:
   ```bash
   cd web
   npm install
   npx tsc
   ```

2. Levantar el servidor (Go) — sirve UDP, WebTransport, y la página de
   prueba, todo junto:
   ```bash
   cd ../go/ejemplo-tacataca
   go run .
   ```
   Imprime el hash del certificado de desarrollo y queda escuchando en:
   - `:9999` UDP
   - `:9443` WebTransport (`/webtransport`)
   - `:8080` HTTP — sirve `web/index.html` + `/certhash`

3. Abrir `http://localhost:8080/` en el navegador. La página obtiene el
   hash del certificado solo (`fetch('/certhash')`), conecta, crea una sala
   nueva (`createRoom`) y muestra el `playerId` y el `roomCode` asignado en
   pantalla. Para unirse a una sala ya creada por otra pestaña/dispositivo
   en vez de crear una nueva, abrir `http://localhost:8080/?room=CODIGO`
   (usa `joinRoom` con ese código).

Validado con Chrome real (vía Playwright, no solo compilación): handshake,
envío de input, decodificación del esquema taca-taca completo, y
ping/pong — los 7 checks pasan.

## Certificados: solo para desarrollo

`GenerateDevCertificate()` (en `go/networkcore/transport_webtransport.go`)
genera un certificado self-signed ECDSA P-256 válido ~13 días — el máximo
que acepta `serverCertificateHashes` del navegador son 14. **Esto no sirve
para producción**: ahí hace falta un certificado real de una CA (Let's
Encrypt, etc.) pasado como `TLSConfig` en `WebTransportOptions`, y el
navegador lo valida solo (sin `serverCertificateHashes`).

## Qué falta si esto se usa en serio (no solo como ejemplo)

- El certificado de desarrollo se regenera en cada arranque del servidor
  (clave nueva cada vez) — para producción, usar un certificado real y
  fijo, no `GenerateDevCertificate`.
- `CheckOrigin` en el servidor está hardcodeado a aceptar cualquier origen
  (`func(*http.Request) bool { return true }`) — en producción debería
  restringirse al dominio real donde vive la página.
- No hay reconexión automática si la sesión WebTransport se cae.
