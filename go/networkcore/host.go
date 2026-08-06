package networkcore

import (
	"sync"
	"time"
)

const (
	tickRate         = 16 * time.Millisecond // 60Hz
	retryInterval    = 100 * time.Millisecond
	heartbeatEvery   = 1 * time.Second
	heartbeatTimeout = 10 * time.Second
	maxRetries       = 3
	inputQueueMax    = 1024
)

// Peer es la identidad de un cliente a nivel transporte — un socket UDP
// crudo (ver transport_udp.go) o una sesión WebTransport (ver
// transport_webtransport.go). El core no distingue entre ambos: cualquier
// cosa que pueda mandar bytes y tenga una clave estable sirve. Esto es lo
// que permite que clientes Unity (UDP) y clientes de navegador
// (WebTransport) jueguen en la misma partida, contra el mismo NetworkHost.
type Peer interface {
	Send(data []byte)
	Key() string
}

// ClientConnection: estado de sesión de un cliente. No tiene ningún campo
// de estado de juego — eso vive en el juego, no acá.
type ClientConnection struct {
	PlayerID      uint16
	Peer          Peer
	LastSeq       uint32
	LastHeartbeat time.Time
	Ping          int
	Connected     bool
	ReliableQueue []reliableMessage
	queueMux      sync.Mutex
}

type reliableMessage struct {
	Seq     uint32
	Data    []byte
	Sent    time.Time
	Retries uint8
}

// NetworkHost: rol "servidor" del protocolo. No sabe nada del juego que
// corre encima — se engancha vía los campos de callback de abajo, igual
// que NetworkHost en el core de C#. Tampoco sabe de qué transporte vienen
// los paquetes (UDP, WebTransport, o ambos a la vez) — ver Peer.
type NetworkHost struct {
	clients      map[uint16]*ClientConnection
	clientsByKey map[string]*ClientConnection // key = Peer.Key()
	clientsMux   sync.RWMutex

	nextPlayerID    uint16
	sequenceCounter uint32
	seqMux          sync.Mutex

	tick          uint64
	pendingEvents []GameEvent
	eventsMux     sync.Mutex

	inputQueue chan PlayerInput
	running    bool
	loopsOnce  sync.Once

	closers    []func()
	closersMux sync.Mutex

	// --- Hooks genéricos: acá es donde el juego se engancha ---
	OnPlayerConnected    func(playerID uint16)
	OnPlayerDisconnected func(playerID uint16)
	OnInput              func(input PlayerInput)
	OnTick               func(tick uint64)
	StateProvider        func() []byte
}

// NewNetworkHost crea un host sin arrancarlo todavía.
func NewNetworkHost() *NetworkHost {
	return &NetworkHost{
		clients:      make(map[uint16]*ClientConnection),
		clientsByKey: make(map[string]*ClientConnection),
		nextPlayerID: 1,
		inputQueue:   make(chan PlayerInput, inputQueueMax),
	}
}

// Stop detiene los loops de fondo (tick, retransmisión, heartbeat) y cierra
// todos los transportes activos (sockets UDP, listener WebTransport) —
// cualquier transporte que se haya registrado vía registerCloser.
func (h *NetworkHost) Stop() {
	h.running = false

	h.closersMux.Lock()
	closers := h.closers
	h.closers = nil
	h.closersMux.Unlock()

	for _, close := range closers {
		close()
	}
}

// registerCloser: cada transporte (StartUDP, StartWebTransport) registra
// acá cómo cerrarse, para que Stop() pueda desbloquear sus loops de
// recepción (ej. un ReadFromUDP bloqueado no se entera de que running
// pasó a false hasta que el socket se cierra).
func (h *NetworkHost) registerCloser(closeFn func()) {
	h.closersMux.Lock()
	defer h.closersMux.Unlock()
	h.closers = append(h.closers, closeFn)
}

// ensureLoopsStarted arranca el tick loop, la retransmisión de confiables y
// el heartbeat — comunes a cualquier transporte. Es seguro llamarlo desde
// StartUDP y StartWebTransport a la vez: sync.Once garantiza que los loops
// arrancan una sola vez sin importar cuántos transportes se agreguen.
func (h *NetworkHost) ensureLoopsStarted() {
	h.loopsOnce.Do(func() {
		h.running = true
		go h.gameLoop()
		go h.sendLoop()
		go h.heartbeatLoop()
	})
}

// QueueEvent: el juego encola sus propios eventos (ej. GOAL) para que
// salgan en el próximo snapshot.
func (h *NetworkHost) QueueEvent(evt GameEvent) {
	h.eventsMux.Lock()
	defer h.eventsMux.Unlock()
	h.pendingEvents = append(h.pendingEvents, evt)
}

func (h *NetworkHost) ConnectedPlayerCount() int {
	h.clientsMux.RLock()
	defer h.clientsMux.RUnlock()
	return len(h.clients)
}

// --- Recepción: punto de entrada común para cualquier transporte ---

// HandlePacket procesa un paquete crudo recibido de un Peer. Cada
// transporte (UDP, WebTransport) llama a esto por cada paquete que recibe
// — es la única puerta de entrada al protocolo, sin importar de dónde
// vino el paquete.
func (h *NetworkHost) HandlePacket(data []byte, peer Peer) {
	seq, _, packetType, flags, ok := parseHeader(data)
	if !ok {
		return
	}

	switch packetType {
	case PacketHandshake:
		h.handleHandshake(seq, peer)
	case PacketInput:
		h.handleInput(seq, peer, flags, data[HeaderSize:])
	case PacketAck:
		h.handleAck(seq, peer)
	case PacketPing:
		h.handlePing(seq, peer, data[HeaderSize:])
	}
}

func (h *NetworkHost) handleHandshake(seq uint32, peer Peer) {
	key := peer.Key()

	h.clientsMux.Lock()
	if _, exists := h.clientsByKey[key]; exists {
		h.clientsMux.Unlock()
		return
	}

	playerID := h.nextPlayerID
	h.nextPlayerID++

	client := &ClientConnection{
		PlayerID:      playerID,
		Peer:          peer,
		LastSeq:       seq,
		LastHeartbeat: time.Now(),
		Connected:     true,
	}
	h.clients[playerID] = client
	h.clientsByKey[key] = client
	h.clientsMux.Unlock()

	response := buildHeader(seq, 0, PacketHandshake, 0)
	response = append(response, byte(playerID>>8), byte(playerID))
	peer.Send(response)

	if h.OnPlayerConnected != nil {
		h.OnPlayerConnected(playerID)
	}
}

func (h *NetworkHost) handleInput(seq uint32, peer Peer, flags byte, payload []byte) {
	client := h.findClientByKey(peer.Key())
	if client == nil {
		return
	}

	dx, dy, rot, actions, ok := decodeInputPayload(payload)
	if !ok {
		return
	}

	input := PlayerInput{
		PlayerID:  client.PlayerID,
		Seq:       seq,
		Timestamp: time.Now().UnixMilli(),
		DeltaX:    dx,
		DeltaY:    dy,
		Rotation:  rot,
		Actions:   actions,
	}

	if flags&FlagReliable != 0 {
		h.sendAck(client, seq)
	}

	select {
	case h.inputQueue <- input:
	default:
	}

	client.LastSeq = seq
	client.LastHeartbeat = time.Now()
}

func (h *NetworkHost) handleAck(seq uint32, peer Peer) {
	client := h.findClientByKey(peer.Key())
	if client == nil {
		return
	}

	client.LastHeartbeat = time.Now()

	client.queueMux.Lock()
	defer client.queueMux.Unlock()
	for i := len(client.ReliableQueue) - 1; i >= 0; i-- {
		if client.ReliableQueue[i].Seq <= seq {
			client.ReliableQueue = append(client.ReliableQueue[:i], client.ReliableQueue[i+1:]...)
		}
	}
}

func (h *NetworkHost) handlePing(seq uint32, peer Peer, payload []byte) {
	client := h.findClientByKey(peer.Key())
	if client == nil || len(payload) < 8 {
		return
	}

	clientTime := int64(uint64(payload[0])<<56 | uint64(payload[1])<<48 | uint64(payload[2])<<40 | uint64(payload[3])<<32 |
		uint64(payload[4])<<24 | uint64(payload[5])<<16 | uint64(payload[6])<<8 | uint64(payload[7]))
	serverTime := time.Now().UnixMilli()

	client.Ping = int((serverTime - clientTime) / 2)
	client.LastHeartbeat = time.Now()

	response := buildHeader(seq, 0, PacketPing, 0)
	tsBuf := make([]byte, 8)
	for i := 0; i < 8; i++ {
		tsBuf[7-i] = byte(serverTime >> (8 * i))
	}
	response = append(response, tsBuf...)
	peer.Send(response)
}

func (h *NetworkHost) sendAck(client *ClientConnection, seq uint32) {
	response := buildHeader(seq, seq, PacketAck, 0)
	client.Peer.Send(response)
}

// --- Tick de juego ---

func (h *NetworkHost) gameLoop() {
	ticker := time.NewTicker(tickRate)
	defer ticker.Stop()

	for h.running {
		<-ticker.C
		if !h.running {
			break
		}

		h.dispatchInputs()
		if h.OnTick != nil {
			h.OnTick(h.tick)
		}
		h.broadcastSnapshot()

		h.tick++
	}
}

// El core solo desencola y reenvía los inputs al juego, en orden — no
// interpreta DeltaX/DeltaY/Rotation/Actions de ninguna forma.
func (h *NetworkHost) dispatchInputs() {
	for {
		select {
		case input := <-h.inputQueue:
			if h.OnInput != nil {
				h.OnInput(input)
			}
		default:
			return
		}
	}
}

func (h *NetworkHost) broadcastSnapshot() {
	var statePayload []byte
	if h.StateProvider != nil {
		statePayload = h.StateProvider()
	}

	h.eventsMux.Lock()
	events := h.pendingEvents
	h.pendingEvents = nil
	h.eventsMux.Unlock()

	snapshot := GameSnapshot{
		Tick:         h.tick,
		Timestamp:    time.Now().UnixMilli(),
		StatePayload: statePayload,
		Events:       events,
	}
	payload := snapshot.Encode()
	seq := h.nextSeqNumber()

	h.clientsMux.RLock()
	clients := make([]*ClientConnection, 0, len(h.clients))
	for _, c := range h.clients {
		clients = append(clients, c)
	}
	h.clientsMux.RUnlock()

	header := buildHeader(seq, 0, PacketSnapshot, 0)
	packet := append(append([]byte{}, header...), payload...)

	for _, client := range clients {
		if !client.Connected {
			continue
		}
		client.Peer.Send(packet)
	}
}

// --- Retransmisión de confiables ---

func (h *NetworkHost) sendLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for h.running {
		<-ticker.C
		if !h.running {
			break
		}

		h.clientsMux.RLock()
		clients := make([]*ClientConnection, 0, len(h.clients))
		for _, c := range h.clients {
			clients = append(clients, c)
		}
		h.clientsMux.RUnlock()

		now := time.Now()
		for _, client := range clients {
			client.queueMux.Lock()
			for i := range client.ReliableQueue {
				msg := &client.ReliableQueue[i]
				if now.Sub(msg.Sent) > retryInterval && msg.Retries < maxRetries {
					client.Peer.Send(msg.Data)
					msg.Sent = now
					msg.Retries++
				}
			}
			client.queueMux.Unlock()
		}
	}
}

// --- Heartbeat / desconexión ---

func (h *NetworkHost) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatEvery)
	defer ticker.Stop()

	for h.running {
		<-ticker.C
		if !h.running {
			break
		}

		now := time.Now()
		var toRemove []uint16

		h.clientsMux.Lock()
		for id, client := range h.clients {
			if now.Sub(client.LastHeartbeat) > heartbeatTimeout {
				toRemove = append(toRemove, id)
			}
		}
		for _, id := range toRemove {
			client := h.clients[id]
			client.Connected = false
			delete(h.clients, id)
			delete(h.clientsByKey, client.Peer.Key())
		}
		h.clientsMux.Unlock()

		for _, id := range toRemove {
			if h.OnPlayerDisconnected != nil {
				h.OnPlayerDisconnected(id)
			}
		}
	}
}

// --- Helpers ---

func (h *NetworkHost) findClientByKey(key string) *ClientConnection {
	h.clientsMux.RLock()
	defer h.clientsMux.RUnlock()
	return h.clientsByKey[key]
}

func (h *NetworkHost) nextSeqNumber() uint32 {
	h.seqMux.Lock()
	defer h.seqMux.Unlock()
	h.sequenceCounter++
	return h.sequenceCounter
}
