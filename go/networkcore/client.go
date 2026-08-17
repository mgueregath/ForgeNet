// client.go añade el rol "cliente" al core en Go — hasta acá, este paquete
// solo traía NetworkHost/Server (topología Servidor Dedicado); un cliente
// Go real hacía falta para que un proceso Go (ej. un binario embebido en
// una app, como netservice.go en el POC de taca-taca) pueda conectarse a un
// Server externo sin reimplementar el protocolo a mano. Habla el mismo
// framing binario que el resto de los clientes (typescript/networkcore.ts,
// csharp/NetworkCore/NetworkClient.cs, etc.).
package networkcore

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"time"
)

// HandshakeRejectedError: el server rechazó el handshake — sala inexistente
// (Join a un código que no existe) o sala llena (tope de ServerOptions.
// MaxPlayersPerRoom).
type HandshakeRejectedError struct {
	Reason uint8
}

func (e *HandshakeRejectedError) Error() string {
	switch e.Reason {
	case ReasonRoomNotFound:
		return "networkcore: sala no encontrada"
	case ReasonRoomFull:
		return "networkcore: sala llena"
	default:
		return "networkcore: handshake rechazado"
	}
}

// NetworkClient: rol "cliente" del protocolo — se conecta a un Server (Go,
// o cualquier otro lenguaje que hable el mismo framing).
type NetworkClient struct {
	conn     *net.UDPConn
	sequence uint32
	seqMux   sync.Mutex

	PlayerID  uint16
	RoomCode  string
	Connected bool
	LastPing  int

	pendingPings map[uint32]time.Time
	pingMux      sync.Mutex

	running bool

	// --- Hooks: el juego se engancha acá, igual que en NetworkHost ---
	OnSnapshot func(snapshot *GameSnapshot)
	OnPong     func(ms int)
	OnClosed   func()
}

func NewNetworkClient() *NetworkClient {
	return &NetworkClient{pendingPings: make(map[uint32]time.Time)}
}

// CreateRoom hace un handshake HandshakeModeCreate: le pide al server que
// arranque una sala nueva. Si conecta a tiempo, arranca el loop de
// recepción en background y deja el código de sala asignado en RoomCode
// (para poder compartirlo — ej. como QR). Bloquea hasta que llega la
// respuesta o expira timeout.
func (c *NetworkClient) CreateRoom(host string, port uint16, role uint8, timeout time.Duration) error {
	return c.handshake(host, port, HandshakeModeCreate, role, "", timeout)
}

// JoinRoom se une a una sala existente por código (HandshakeModeJoin) — ya
// sea porque el código lo compartió otro cliente, o porque este mismo
// cliente lo recuerda de una conexión anterior y está reconectando (ver el
// comentario grande en NetworkHost.AdmitPlayer: el server reconoce la
// reconexión por IP y devuelve la misma identidad).
func (c *NetworkClient) JoinRoom(host string, port uint16, role uint8, roomCode string, timeout time.Duration) error {
	return c.handshake(host, port, HandshakeModeJoin, role, roomCode, timeout)
}

func (c *NetworkClient) handshake(host string, port uint16, mode, role uint8, roomCode string, timeout time.Duration) error {
	addr, err := net.ResolveUDPAddr("udp", host+":"+strconv.Itoa(int(port)))
	if err != nil {
		return err
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	c.conn = conn

	payload := EncodeJoinPayload(mode, role, roomCode)
	packet := append(buildHeader(0, 0, PacketHandshake, 0), payload...)
	if _, err := conn.Write(packet); err != nil {
		conn.Close()
		return err
	}

	conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 2048)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			return errors.New("networkcore: timeout esperando handshake")
		}
		_, _, packetType, _, ok := parseHeader(buf[:n])
		if !ok {
			continue
		}
		body := buf[HeaderSize:n]

		if packetType == PacketDisconnect {
			conn.Close()
			var reason uint8
			if len(body) > 0 {
				reason = body[0]
			}
			return &HandshakeRejectedError{Reason: reason}
		}
		if packetType != PacketHandshake {
			continue
		}
		playerID, code, ok := decodeHandshakeAck(body)
		if !ok {
			continue
		}
		c.PlayerID = playerID
		c.RoomCode = code
		break
	}

	conn.SetReadDeadline(time.Time{})
	c.Connected = true
	c.running = true
	go c.receiveLoop()
	return nil
}

func (c *NetworkClient) Disconnect() {
	c.running = false
	c.Connected = false
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *NetworkClient) SendInput(deltaX, deltaY int16, rotation uint16, actions uint32, reliable bool) {
	if !c.Connected {
		return
	}
	seq := c.nextSeq()
	flags := byte(FlagOrdered)
	if reliable {
		flags = FlagReliable
	}
	header := buildHeader(seq, 0, PacketInput, flags)
	payload := EncodeInputPayload(deltaX, deltaY, rotation, actions)
	c.conn.Write(append(header, payload...))
}

func (c *NetworkClient) SendPing() {
	if !c.Connected {
		return
	}
	seq := c.nextSeq()
	header := buildHeader(seq, 0, PacketPing, 0)
	now := time.Now().UnixMilli()
	payload := make([]byte, 8)
	for i := 0; i < 8; i++ {
		payload[7-i] = byte(now >> (8 * i))
	}

	c.pingMux.Lock()
	c.pendingPings[seq] = time.Now()
	c.pingMux.Unlock()

	c.conn.Write(append(header, payload...))
}

func (c *NetworkClient) receiveLoop() {
	buf := make([]byte, 2048)
	for c.running {
		n, err := c.conn.Read(buf)
		if err != nil {
			if c.running && c.OnClosed != nil {
				c.OnClosed()
			}
			return
		}
		c.handlePacket(buf[:n])
	}
}

func (c *NetworkClient) handlePacket(data []byte) {
	seq, _, packetType, flags, ok := parseHeader(data)
	if !ok {
		return
	}
	payload := data[HeaderSize:]

	switch packetType {
	case PacketSnapshot:
		// Un snapshot marcado FlagReliable viene de QueueReliableEvent en
		// el host — confirmarlo es lo que hace que el server deje de
		// reintentarlo (ver sendAck/handleAck en host.go, misma
		// convención: el seq que se confirma es el propio seq del paquete
		// recibido, en ambos campos del header del ack).
		if flags&FlagReliable != 0 {
			ack := buildHeader(seq, seq, PacketAck, 0)
			c.conn.Write(ack)
		}
		snapshot, err := DecodeSnapshot(payload)
		if err == nil && c.OnSnapshot != nil {
			c.OnSnapshot(snapshot)
		}
	case PacketPing:
		c.pingMux.Lock()
		sentAt, exists := c.pendingPings[seq]
		if exists {
			delete(c.pendingPings, seq)
		}
		c.pingMux.Unlock()
		if exists {
			c.LastPing = int(time.Since(sentAt).Milliseconds())
			if c.OnPong != nil {
				c.OnPong(c.LastPing)
			}
		}
	}
}

func (c *NetworkClient) nextSeq() uint32 {
	c.seqMux.Lock()
	defer c.seqMux.Unlock()
	c.sequence++
	return c.sequence
}
