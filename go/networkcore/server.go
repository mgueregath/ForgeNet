package networkcore

import (
	"crypto/rand"
	"sync"
	"time"
)

// RoomFactory crea un NetworkHost nuevo, con sus propios hooks, cada vez
// que se crea una sala — cada juego decide qué enganchar (ver
// ejemplo-tacataca), el Server no sabe nada de eso.
type RoomFactory func() *NetworkHost

// ServerOptions configura el Server. Todo acá es genérico — no hay nada
// específico de ningún juego.
type ServerOptions struct {
	// RoomCodeLength: longitud de los códigos de sala generados. Default: 6.
	RoomCodeLength int
	// MaxPlayersPerRoom: tope de clientes admitidos por sala (0 = sin
	// tope). El Server solo cuenta clientes — no sabe qué rol tiene cada
	// uno, así que un límite que dependa del rol (ej. "2 barras + 1
	// tablero" en taca-taca) es responsabilidad del juego, no de acá.
	MaxPlayersPerRoom int
	// HandshakeRateLimit: máximo de paquetes de handshake aceptados desde
	// un mismo Peer (misma IP:puerto UDP, o misma sesión WebTransport) por
	// HandshakeRateLimitWindow — el resto se descarta en silencio. Default:
	// 10 intentos / 10s. Esto NO protege contra un atacante que rota de
	// origen en cada intento (el core no ve la IP real, solo Peer.Key()) —
	// sí frena un cliente roto reintentando en loop o un flood simple
	// desde una única conexión.
	HandshakeRateLimit       int
	HandshakeRateLimitWindow time.Duration
	// EmptyRoomGracePeriod: cuánto se espera, desde que una sala que ya
	// tuvo jugadores queda en 0 conectados, antes de destruirla — no
	// instantáneo, para darle tiempo a AdmitPlayer de reconocer una
	// reconexión (ver el comentario grande ahí) antes de que la sala misma
	// deje de existir. Default: 30s.
	EmptyRoomGracePeriod time.Duration
}

type room struct {
	code      string
	host      *NetworkHost
	hadPlayer bool
	// emptySince: desde cuándo ConnectedPlayerCount() está en 0 — tiempo
	// cero mientras la sala tiene al menos un cliente conectado. Ver
	// sweepEmptyRooms.
	emptySince time.Time
}

// peerInfo recuerda a qué sala pertenece cada Peer y guarda el último ack
// de handshake mandado, para poder responder handshakes reintentados
// (ack perdido) sin volver a admitir al cliente ni crear una sala nueva.
type peerInfo struct {
	roomCode string
	ack      []byte
}

// Server multiplexa muchas salas (partidas) concurrentes sobre el mismo
// transporte compartido — genérico, no sabe qué juego corre en cada una.
// Un cliente que manda PacketHandshake con HandshakeModeCreate arranca una
// sala nueva y recibe un código; con HandshakeModeJoin se une a la sala de
// ese código. Todo lo demás (input, ack, ping) se rutea a la sala del
// cliente que lo mandó.
type Server struct {
	opts        ServerOptions
	roomFactory RoomFactory

	mu    sync.RWMutex
	rooms map[string]*room
	peers map[string]*peerInfo // key = Peer.Key()

	handshakeMu       sync.Mutex
	handshakeAttempts map[string]*handshakeWindow

	closers    []func()
	closersMux sync.Mutex
	done       chan struct{}

	udpPort uint16
}

type handshakeWindow struct {
	start time.Time
	count int
}

// NewServer crea un Server sin arrancar ningún transporte todavía — llamar
// StartUDP y/o StartWebTransport después.
func NewServer(factory RoomFactory, opts ServerOptions) *Server {
	if opts.RoomCodeLength <= 0 {
		opts.RoomCodeLength = 6
	}
	if opts.HandshakeRateLimit <= 0 {
		opts.HandshakeRateLimit = 10
	}
	if opts.HandshakeRateLimitWindow <= 0 {
		opts.HandshakeRateLimitWindow = 10 * time.Second
	}
	if opts.EmptyRoomGracePeriod <= 0 {
		opts.EmptyRoomGracePeriod = 30 * time.Second
	}
	s := &Server{
		opts:              opts,
		roomFactory:       factory,
		rooms:             make(map[string]*room),
		peers:             make(map[string]*peerInfo),
		handshakeAttempts: make(map[string]*handshakeWindow),
		done:              make(chan struct{}),
	}
	go s.janitorLoop()
	return s
}

// Stop cierra todos los transportes activos y detiene todas las salas.
func (s *Server) Stop() {
	close(s.done)

	s.closersMux.Lock()
	closers := s.closers
	s.closers = nil
	s.closersMux.Unlock()
	for _, closeFn := range closers {
		closeFn()
	}

	s.mu.Lock()
	rooms := s.rooms
	s.rooms = make(map[string]*room)
	s.peers = make(map[string]*peerInfo)
	s.mu.Unlock()
	for _, r := range rooms {
		r.host.Stop()
	}
}

// registerCloser: cada transporte (StartUDP, StartWebTransport) registra
// acá cómo cerrarse, para que Stop() pueda desbloquear sus loops de
// recepción.
func (s *Server) registerCloser(closeFn func()) {
	s.closersMux.Lock()
	defer s.closersMux.Unlock()
	s.closers = append(s.closers, closeFn)
}

// HandlePacket es el punto de entrada común para cualquier transporte —
// igual que antes lo era NetworkHost.HandlePacket, pero a nivel Server,
// porque el Server es quien sabe a qué sala pertenece cada paquete.
func (s *Server) HandlePacket(data []byte, peer Peer) {
	seq, _, packetType, _, ok := parseHeader(data)
	if !ok {
		return
	}

	if packetType == PacketHandshake {
		s.handleHandshake(seq, peer, data[HeaderSize:])
		return
	}

	s.mu.RLock()
	info, known := s.peers[peer.Key()]
	var r *room
	if known {
		r = s.rooms[info.roomCode]
	}
	s.mu.RUnlock()

	if r == nil {
		return
	}
	r.host.HandlePacket(data, peer)
}

func (s *Server) handleHandshake(seq uint32, peer Peer, payload []byte) {
	key := peer.Key()

	if !s.allowHandshakeAttempt(key) {
		return
	}

	// Reintento de un handshake ya admitido (el ack anterior se perdió):
	// no crear una sala nueva ni volver a admitir, solo reenviar el ack.
	s.mu.RLock()
	if info, exists := s.peers[key]; exists {
		s.mu.RUnlock()
		peer.Send(info.ack)
		return
	}
	s.mu.RUnlock()

	mode, role, roomCode, ok := decodeJoinPayload(payload)
	if !ok {
		return
	}

	var r *room
	switch mode {
	case HandshakeModeCreate:
		r = s.createRoom()
	case HandshakeModeJoin:
		s.mu.RLock()
		r = s.rooms[roomCode]
		s.mu.RUnlock()
		if r == nil {
			peer.Send(append(buildHeader(seq, 0, PacketDisconnect, 0), ReasonRoomNotFound))
			return
		}
	default:
		return
	}

	if s.opts.MaxPlayersPerRoom > 0 && r.host.ConnectedPlayerCount() >= s.opts.MaxPlayersPerRoom {
		peer.Send(append(buildHeader(seq, 0, PacketDisconnect, 0), ReasonRoomFull))
		return
	}

	playerID, _ := r.host.AdmitPlayer(peer, role)
	r.hadPlayer = true

	ack := append(buildHeader(seq, 0, PacketHandshake, 0), encodeHandshakeAck(playerID, r.code)...)
	peer.Send(ack)

	s.mu.Lock()
	s.peers[key] = &peerInfo{roomCode: r.code, ack: ack}
	s.mu.Unlock()
}

// allowHandshakeAttempt aplica el rate limit de HandshakeRateLimit por
// Peer — ventana fija, no token bucket: simple y suficiente para lo que
// protege (ver ServerOptions.HandshakeRateLimit).
func (s *Server) allowHandshakeAttempt(key string) bool {
	s.handshakeMu.Lock()
	defer s.handshakeMu.Unlock()

	now := time.Now()
	w, exists := s.handshakeAttempts[key]
	if !exists || now.Sub(w.start) > s.opts.HandshakeRateLimitWindow {
		s.handshakeAttempts[key] = &handshakeWindow{start: now, count: 1}
		return true
	}
	if w.count >= s.opts.HandshakeRateLimit {
		return false
	}
	w.count++
	return true
}

func (s *Server) sweepHandshakeAttempts() {
	s.handshakeMu.Lock()
	defer s.handshakeMu.Unlock()
	now := time.Now()
	for key, w := range s.handshakeAttempts {
		if now.Sub(w.start) > s.opts.HandshakeRateLimitWindow {
			delete(s.handshakeAttempts, key)
		}
	}
}

func (s *Server) createRoom() *room {
	code := s.generateUniqueRoomCode()
	host := s.roomFactory()
	host.start()

	r := &room{code: code, host: host}
	s.mu.Lock()
	s.rooms[code] = r
	s.mu.Unlock()
	return r
}

// roomCodeAlphabet excluye 0/O y 1/I — ambiguos al leerlos a mano si el QR
// falla y alguien tiene que tipear el código.
const roomCodeAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

func (s *Server) generateUniqueRoomCode() string {
	for {
		code := randomRoomCode(s.opts.RoomCodeLength)
		s.mu.RLock()
		_, exists := s.rooms[code]
		s.mu.RUnlock()
		if !exists {
			return code
		}
	}
}

func randomRoomCode(length int) string {
	raw := make([]byte, length)
	rand.Read(raw)
	code := make([]byte, length)
	for i, b := range raw {
		code[i] = roomCodeAlphabet[int(b)%len(roomCodeAlphabet)]
	}
	return string(code)
}

// janitorLoop destruye salas que se quedaron sin ningún cliente conectado
// durante más de EmptyRoomGracePeriod — genérico, no sabe qué juego corre
// en ellas. Una sala recién creada que todavía no tuvo ningún cliente no se
// toca (hadPlayer).
func (s *Server) janitorLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.sweepEmptyRooms()
			s.sweepHandshakeAttempts()
		}
	}
}

func (s *Server) sweepEmptyRooms() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for code, r := range s.rooms {
		if !r.hadPlayer {
			continue
		}
		if r.host.ConnectedPlayerCount() > 0 {
			r.emptySince = time.Time{}
			continue
		}
		if r.emptySince.IsZero() {
			r.emptySince = now
			continue
		}
		if now.Sub(r.emptySince) < s.opts.EmptyRoomGracePeriod {
			continue
		}

		r.host.Stop()
		delete(s.rooms, code)
		for peerKey, info := range s.peers {
			if info.roomCode == code {
				delete(s.peers, peerKey)
			}
		}
	}
}
