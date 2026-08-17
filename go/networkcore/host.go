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
//
// IP(): el protocolo no lleva ningún token de sesión, así que AdmitPlayer
// usa la IP (sin el puerto, que cambia en cada reconexión — nuevo socket
// UDP o nueva sesión QUIC) como heurística para reconocer "es el mismo
// dispositivo reconectándose" — ver el comentario grande ahí.
type Peer interface {
	Send(data []byte)
	Key() string
	IP() string
}

// ClientConnection: estado de sesión de un cliente. No tiene ningún campo
// de estado de juego — eso vive en el juego, no acá. Role es opaco: el core
// lo guarda y lo expone (ver GetClientRole) pero nunca lo interpreta — cada
// juego define sus propios valores y qué significan.
type ClientConnection struct {
	PlayerID      uint16
	Peer          Peer
	Role          uint8
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

// NetworkHost: rol "servidor" del protocolo para UNA sala/partida. No sabe
// nada del juego que corre encima — se engancha vía los campos de callback
// de abajo, igual que NetworkHost en el core de C#. Tampoco sabe de qué
// transporte vienen los paquetes (UDP, WebTransport, o ambos a la vez) — ver
// Peer — ni a qué sala pertenece: eso lo decide Server, que crea una
// instancia de NetworkHost por sala y le rutea sus paquetes (ver server.go).
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

	// --- Hooks genéricos: acá es donde el juego se engancha ---
	// OnPlayerConnected: reconnected=true si esto fue una reconexión
	// reconocida por IP (ver AdmitPlayer) — el juego puede usarlo para NO
	// pisar el estado que ya tenía ese jugador con uno vacío.
	OnPlayerConnected    func(playerID uint16, role uint8, reconnected bool)
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

// Stop detiene los loops de fondo de esta sala (tick, retransmisión,
// heartbeat). No toca ningún transporte — eso es responsabilidad de Server,
// que es quien los posee (ver server.go).
func (h *NetworkHost) Stop() {
	h.running = false
}

// start arranca el tick loop, la retransmisión de confiables y el
// heartbeat de esta sala. La llama Server exactamente una vez, al crear la
// sala (ver Server.createRoom) — sync.Once es solo una salvaguarda.
func (h *NetworkHost) start() {
	h.loopsOnce.Do(func() {
		h.running = true
		go h.gameLoop()
		go h.sendLoop()
		go h.heartbeatLoop()
	})
}

// QueueEvent: el juego encola sus propios eventos (ej. GOAL) para que
// salgan en el próximo snapshot. Entrega "mejor esfuerzo": si el paquete se
// pierde, el evento no vuelve a mandarse (a diferencia del snapshot en
// general, no hay una versión "más nueva" de un evento puntual). En LAN
// casi no importa; en una red con pérdida real (internet) un evento que
// cambia el marcador puede perderse en silencio — para eso ver
// QueueReliableEvent.
func (h *NetworkHost) QueueEvent(evt GameEvent) {
	h.eventsMux.Lock()
	defer h.eventsMux.Unlock()
	h.pendingEvents = append(h.pendingEvents, evt)
}

// QueueReliableEvent hace lo mismo que QueueEvent (el evento sale en el
// próximo snapshot de todos modos) pero además lo manda ya mismo, a cada
// cliente conectado, como un paquete aparte marcado FlagReliable — el core
// lo reintenta (mismo mecanismo que ya existía para reliable input, ver
// sendLoop) hasta que el cliente lo confirma con PacketAck o se agotan los
// reintentos. Pensado para eventos que el juego no puede permitirse perder
// en una red con pérdida real (ej. GOAL en un server público).
//
// La entrega es "al menos una vez", no "exactamente una vez": el cliente
// puede recibir el mismo evento acá y de nuevo en el snapshot normal — el
// juego debe tratarlo como una notificación idempotente (ej. "hubo un gol,
// resincronizá con StatePayload"), no como algo que suma un contador él
// mismo.
func (h *NetworkHost) QueueReliableEvent(evt GameEvent) {
	h.QueueEvent(evt)

	snap := GameSnapshot{Tick: h.tick, Timestamp: time.Now().UnixMilli(), Events: []GameEvent{evt}}
	payload := snap.Encode()

	h.clientsMux.RLock()
	clients := make([]*ClientConnection, 0, len(h.clients))
	for _, c := range h.clients {
		clients = append(clients, c)
	}
	h.clientsMux.RUnlock()

	for _, client := range clients {
		seq := h.nextSeqNumber()
		packet := append(buildHeader(seq, 0, PacketSnapshot, FlagReliable), payload...)

		client.queueMux.Lock()
		client.ReliableQueue = append(client.ReliableQueue, reliableMessage{Seq: seq, Data: packet, Sent: time.Now()})
		client.queueMux.Unlock()

		client.Peer.Send(packet)
	}
}

// PendingReliableCount: cuántos paquetes reliable (ver QueueReliableEvent)
// todavía no fueron confirmados por este cliente. Diagnóstico genérico —
// útil para cualquier juego que quiera saber si un cliente se está
// quedando atrás, no es específico de ningún evento en particular.
func (h *NetworkHost) PendingReliableCount(playerID uint16) int {
	h.clientsMux.RLock()
	client, ok := h.clients[playerID]
	h.clientsMux.RUnlock()
	if !ok {
		return 0
	}
	client.queueMux.Lock()
	defer client.queueMux.Unlock()
	return len(client.ReliableQueue)
}

// ConnectedPlayerCount solo cuenta Connected==true — h.clients también
// incluye clientes desconectados que quedaron "reclamables" para una
// reconexión (ver heartbeatLoop/AdmitPlayer), así que len(h.clients) ya no
// alcanza.
func (h *NetworkHost) ConnectedPlayerCount() int {
	h.clientsMux.RLock()
	defer h.clientsMux.RUnlock()
	n := 0
	for _, c := range h.clients {
		if c.Connected {
			n++
		}
	}
	return n
}

// --- Recepción: punto de entrada común para cualquier transporte ---

// HandlePacket procesa un paquete crudo recibido de un Peer que YA
// pertenece a esta sala. El handshake no pasa por acá: es Server quien lo
// intercepta primero para decidir a qué sala corresponde (crear una nueva o
// unirse a una existente) y recién ahí llama a AdmitPlayer — ver server.go.
func (h *NetworkHost) HandlePacket(data []byte, peer Peer) {
	seq, _, packetType, flags, ok := parseHeader(data)
	if !ok {
		return
	}

	switch packetType {
	case PacketInput:
		h.handleInput(seq, peer, flags, data[HeaderSize:])
	case PacketAck:
		h.handleAck(seq, peer)
	case PacketPing:
		h.handlePing(seq, peer, data[HeaderSize:])
	}
}

// AdmitPlayer registra un cliente nuevo en esta sala, o reconoce una
// reconexión, y dispara OnPlayerConnected(playerID, role, reconnected). La
// llama Server, después de decidir (crear/unirse) a qué sala pertenece el
// cliente. Role es opaco para el core — cada juego define sus propios
// valores y qué hacer con ellos (ej. no crear una "barra" de juego para el
// rol que representa al tablero).
//
// Reconexión: si hay un cliente marcado desconectado (ver heartbeatLoop)
// con la misma IP (Peer.IP()) que quien está haciendo el handshake, se le
// devuelve la MISMA identidad (PlayerID, y el Role ORIGINAL — no el que
// venga en este handshake, para no perder de vista quién era) en vez de
// crear un jugador nuevo. Es una heurística por IP, no un token de sesión
// (el protocolo no lleva uno todavía): funciona bien en la LAN/datos
// móviles típica, donde cada dispositivo tiene su propia IP — no es a
// prueba de balas si dos jugadores comparten la misma IP pública (NAT
// compartido) y ambos están desconectados a la vez.
//
// Idempotente además en el sentido de siempre: un cliente que ya está
// admitido (mismo Peer.Key(), sin pasar por desconexión) devuelve su
// PlayerID actual sin duplicar estado ni disparar el hook de nuevo (cubre
// el reintento de un handshake cuyo ack se perdió).
func (h *NetworkHost) AdmitPlayer(peer Peer, role uint8) (playerID uint16, reconnected bool) {
	key := peer.Key()

	h.clientsMux.Lock()
	if existing, exists := h.clientsByKey[key]; exists {
		h.clientsMux.Unlock()
		return existing.PlayerID, false
	}

	var reclaimed *ClientConnection
	if ip := peer.IP(); ip != "" {
		for _, c := range h.clients {
			if !c.Connected && c.Peer.IP() == ip {
				reclaimed = c
				break
			}
		}
	}

	if reclaimed != nil {
		delete(h.clientsByKey, reclaimed.Peer.Key())
		reclaimed.Peer = peer
		reclaimed.Connected = true
		reclaimed.LastHeartbeat = time.Now()
		h.clientsByKey[key] = reclaimed
		playerID = reclaimed.PlayerID
		role = reclaimed.Role
		reconnected = true
	} else {
		playerID = h.nextPlayerID
		h.nextPlayerID++
		client := &ClientConnection{
			PlayerID:      playerID,
			Peer:          peer,
			Role:          role,
			LastHeartbeat: time.Now(),
			Connected:     true,
		}
		h.clients[playerID] = client
		h.clientsByKey[key] = client
	}
	h.clientsMux.Unlock()

	if h.OnPlayerConnected != nil {
		h.OnPlayerConnected(playerID, role, reconnected)
	}
	return playerID, reconnected
}

// GetClientRole devuelve el rol opaco con el que se admitió a un jugador
// (ver AdmitPlayer). ok=false si el jugador no existe (ya desconectado, o
// ID inválido).
func (h *NetworkHost) GetClientRole(playerID uint16) (role uint8, ok bool) {
	h.clientsMux.RLock()
	defer h.clientsMux.RUnlock()
	client, exists := h.clients[playerID]
	if !exists {
		return 0, false
	}
	return client.Role, true
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
		// Solo se marca Connected=false — a propósito NO se borra de
		// h.clients: se mantiene "reclamable" por AdmitPlayer si el mismo
		// dispositivo (misma IP) vuelve a conectarse, así el juego puede
		// restaurar su estado en vez de tratarlo como uno nuevo.
		// clientsByKey sí se limpia: esa key (ej. ip:puerto UDP viejo) ya
		// no sirve para rutear nada. El Server, por su lado, no destruye
		// esta sala instantáneamente al quedar en 0 conectados — ver
		// ServerOptions.EmptyRoomGracePeriod.
		for _, id := range toRemove {
			client := h.clients[id]
			client.Connected = false
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
