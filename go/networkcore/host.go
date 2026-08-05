package networkcore

import (
	"log"
	"net"
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

// ClientConnection: estado de sesión de un cliente. No tiene ningún campo
// de estado de juego — eso vive en el juego, no acá.
type ClientConnection struct {
	PlayerID      uint16
	Addr          *net.UDPAddr
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
// que NetworkHost en el core de C#.
type NetworkHost struct {
	conn *net.UDPConn
	port uint16

	clients      map[uint16]*ClientConnection
	clientsByKey map[string]*ClientConnection // key = addr.String()
	clientsMux   sync.RWMutex

	nextPlayerID    uint16
	sequenceCounter uint32
	seqMux          sync.Mutex

	tick          uint64
	pendingEvents []GameEvent
	eventsMux     sync.Mutex

	inputQueue chan PlayerInput
	running    bool

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

// Start liga el socket UDP y arranca los loops en background.
func (h *NetworkHost) Start(port uint16) error {
	addr := net.UDPAddr{Port: int(port), IP: net.ParseIP("0.0.0.0")}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		return err
	}

	conn.SetReadBuffer(1 << 20)
	conn.SetWriteBuffer(1 << 20)

	h.conn = conn
	h.port = port
	h.running = true

	go h.receiveLoop()
	go h.gameLoop()
	go h.sendLoop()
	go h.heartbeatLoop()

	log.Printf("🎮 NetworkHost escuchando en :%d (60Hz)", port)
	return nil
}

func (h *NetworkHost) Stop() {
	h.running = false
	if h.conn != nil {
		h.conn.Close()
	}
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

// --- Recepción ---

func (h *NetworkHost) receiveLoop() {
	buffer := make([]byte, 2048)
	for h.running {
		n, remoteAddr, err := h.conn.ReadFromUDP(buffer)
		if err != nil {
			if h.running {
				continue
			}
			return
		}

		packet := make([]byte, n)
		copy(packet, buffer[:n])
		h.handlePacket(packet, remoteAddr)
	}
}

func (h *NetworkHost) handlePacket(data []byte, addr *net.UDPAddr) {
	seq, _, packetType, flags, ok := parseHeader(data)
	if !ok {
		return
	}

	switch packetType {
	case PacketHandshake:
		h.handleHandshake(seq, addr)
	case PacketInput:
		h.handleInput(seq, addr, flags, data[HeaderSize:])
	case PacketAck:
		h.handleAck(seq, addr)
	case PacketPing:
		h.handlePing(seq, addr, data[HeaderSize:])
	}
}

func (h *NetworkHost) handleHandshake(seq uint32, addr *net.UDPAddr) {
	key := addr.String()

	h.clientsMux.Lock()
	if _, exists := h.clientsByKey[key]; exists {
		h.clientsMux.Unlock()
		return
	}

	playerID := h.nextPlayerID
	h.nextPlayerID++

	client := &ClientConnection{
		PlayerID:      playerID,
		Addr:          addr,
		LastSeq:       seq,
		LastHeartbeat: time.Now(),
		Connected:     true,
	}
	h.clients[playerID] = client
	h.clientsByKey[key] = client
	h.clientsMux.Unlock()

	response := buildHeader(seq, 0, PacketHandshake, 0)
	response = append(response, byte(playerID>>8), byte(playerID))
	h.conn.WriteToUDP(response, addr)

	if h.OnPlayerConnected != nil {
		h.OnPlayerConnected(playerID)
	}
}

func (h *NetworkHost) handleInput(seq uint32, addr *net.UDPAddr, flags byte, payload []byte) {
	client := h.findClientByAddr(addr)
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

func (h *NetworkHost) handleAck(seq uint32, addr *net.UDPAddr) {
	client := h.findClientByAddr(addr)
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

func (h *NetworkHost) handlePing(seq uint32, addr *net.UDPAddr, payload []byte) {
	client := h.findClientByAddr(addr)
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
	h.conn.WriteToUDP(response, addr)
}

func (h *NetworkHost) sendAck(client *ClientConnection, seq uint32) {
	response := buildHeader(seq, seq, PacketAck, 0)
	h.conn.WriteToUDP(response, client.Addr)
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
		h.conn.WriteToUDP(packet, client.Addr)
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
					h.conn.WriteToUDP(msg.Data, client.Addr)
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
			delete(h.clientsByKey, client.Addr.String())
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

func (h *NetworkHost) findClientByAddr(addr *net.UDPAddr) *ClientConnection {
	h.clientsMux.RLock()
	defer h.clientsMux.RUnlock()
	return h.clientsByKey[addr.String()]
}

func (h *NetworkHost) nextSeqNumber() uint32 {
	h.seqMux.Lock()
	defer h.seqMux.Unlock()
	h.sequenceCounter++
	return h.sequenceCounter
}
