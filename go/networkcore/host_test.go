package networkcore

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"
)

// testClient es un cliente UDP mínimo, solo para estos tests — este
// proyecto nunca tuvo un "cliente Go" público (los clientes existentes son
// TypeScript/Python/Unity), así que no hace falta exponerlo como parte del
// paquete.
type testClient struct {
	conn     *net.UDPConn
	addr     *net.UDPAddr
	playerID uint16
	roomCode string
	seq      uint32
}

func newTestClient(t *testing.T, port int) *testClient {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:"+itoa(port))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	conn.SetReadBuffer(1 << 16)
	return &testClient{conn: conn, addr: addr}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// handshake crea una sala nueva (HandshakeModeCreate). role es opaco para
// el core — estos tests usan valores arbitrarios, ningún significado.
func (c *testClient) handshake(t *testing.T, role uint8) {
	t.Helper()
	c.joinOrCreate(t, HandshakeModeCreate, role, "")
}

// join se une a una sala existente por código (HandshakeModeJoin).
func (c *testClient) join(t *testing.T, role uint8, roomCode string) {
	t.Helper()
	c.joinOrCreate(t, HandshakeModeJoin, role, roomCode)
}

func (c *testClient) joinOrCreate(t *testing.T, mode, role uint8, roomCode string) {
	t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	payload := EncodeJoinPayload(mode, role, roomCode)
	req := append(buildHeader(0, 0, PacketHandshake, 0), payload...)
	if _, err := c.conn.Write(req); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 64)
	n, err := c.conn.Read(buf)
	if err != nil {
		t.Fatalf("handshake sin respuesta: %v", err)
	}
	_, _, packetType, _, ok := parseHeader(buf[:n])
	if !ok {
		t.Fatalf("respuesta de handshake malformada")
	}
	if packetType == PacketDisconnect {
		t.Fatalf("handshake rechazado, reason=%d", buf[HeaderSize])
	}
	if n < HeaderSize+3 {
		t.Fatalf("respuesta de handshake muy corta: %d bytes", n)
	}
	c.playerID = binary.BigEndian.Uint16(buf[HeaderSize : HeaderSize+2])
	codeLen := int(buf[HeaderSize+2])
	c.roomCode = string(buf[HeaderSize+3 : HeaderSize+3+codeLen])
}

// handshakeExpectReject intenta un handshake que se espera que el server
// rechace (ej. unirse a un código inexistente), y devuelve el motivo.
func (c *testClient) handshakeExpectReject(t *testing.T, mode, role uint8, roomCode string) byte {
	t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	payload := EncodeJoinPayload(mode, role, roomCode)
	req := append(buildHeader(0, 0, PacketHandshake, 0), payload...)
	if _, err := c.conn.Write(req); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 64)
	n, err := c.conn.Read(buf)
	if err != nil {
		t.Fatalf("sin respuesta: %v", err)
	}
	_, _, packetType, _, ok := parseHeader(buf[:n])
	if !ok || packetType != PacketDisconnect {
		t.Fatalf("esperaba PacketDisconnect, packetType=%d ok=%v", packetType, ok)
	}
	return buf[HeaderSize]
}

func (c *testClient) sendInput(t *testing.T, dx, dy int16, rot uint16, actions uint32, reliable bool) {
	t.Helper()
	c.seq++
	var flags byte
	if reliable {
		flags = FlagReliable
	}
	packet := append(buildHeader(c.seq, 0, PacketInput, flags), EncodeInputPayload(dx, dy, rot, actions)...)
	if _, err := c.conn.Write(packet); err != nil {
		t.Fatal(err)
	}
}

func (c *testClient) sendPing(t *testing.T) {
	t.Helper()
	c.seq++
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, uint64(time.Now().UnixMilli()))
	packet := append(buildHeader(c.seq, 0, PacketPing, 0), payload...)
	if _, err := c.conn.Write(packet); err != nil {
		t.Fatal(err)
	}
}

// sendAck confirma un paquete reliable del server. El campo "seq" del
// header de un PacketAck es, por convención de este protocolo, el número
// de secuencia que se está confirmando (ver NetworkHost.sendAck y
// handleAck) — no un seq propio del cliente.
func (c *testClient) sendAck(t *testing.T, ackedSeq uint32) {
	t.Helper()
	packet := buildHeader(ackedSeq, ackedSeq, PacketAck, 0)
	if _, err := c.conn.Write(packet); err != nil {
		t.Fatal(err)
	}
}

// readUntil lee paquetes hasta que match devuelva true o se cumpla el
// deadline, descartando lo que no matchea (para no bloquear el test con
// paquetes de otro tipo mezclados en el flujo).
func (c *testClient) readUntil(t *testing.T, timeout time.Duration, match func(packetType byte, payload []byte) bool) []byte {
	t.Helper()
	_, payload := c.readUntilWithSeq(t, timeout, match)
	return payload
}

// readUntilWithSeq es igual que readUntil, pero también devuelve el campo
// seq del header — hace falta para poder ackear un paquete reliable
// puntual (ver sendAck) en vez de solo inspeccionar su payload.
func (c *testClient) readUntilWithSeq(t *testing.T, timeout time.Duration, match func(packetType byte, payload []byte) bool) (seq uint32, payload []byte) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	buf := make([]byte, 4096)
	for time.Now().Before(deadline) {
		c.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, err := c.conn.Read(buf)
		if err != nil {
			continue
		}
		pktSeq, _, packetType, _, ok := parseHeader(buf[:n])
		if !ok {
			continue
		}
		p := append([]byte{}, buf[HeaderSize:n]...)
		if match(packetType, p) {
			return pktSeq, p
		}
	}
	return 0, nil
}

func TestHandshakeInputSnapshotPing(t *testing.T) {
	host := NewNetworkHost()
	fakeState := []byte{10, 20, 30, 40, 50}
	host.StateProvider = func() []byte { return fakeState }

	var mu sync.Mutex
	var receivedInputs []PlayerInput
	host.OnInput = func(input PlayerInput) {
		mu.Lock()
		defer mu.Unlock()
		receivedInputs = append(receivedInputs, input)
	}

	server := NewServer(func() *NetworkHost { return host }, ServerOptions{})
	if err := server.StartUDP(29999); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	time.Sleep(200 * time.Millisecond)

	client := newTestClient(t, 29999)
	client.handshake(t, 1)
	if client.playerID == 0 {
		t.Fatal("playerID no asignado")
	}
	if client.roomCode == "" {
		t.Fatal("no se recibió código de sala")
	}

	client.sendInput(t, 15, -7, 90, 0, false)
	client.sendInput(t, 3, 3, 45, 0x99, true)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(receivedInputs)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(receivedInputs) != 2 {
		t.Fatalf("esperaba 2 inputs, llegaron %d", len(receivedInputs))
	}
	if receivedInputs[0].DeltaX != 15 || receivedInputs[0].Rotation != 90 {
		t.Errorf("1er input crudo incorrecto: %+v", receivedInputs[0])
	}
	if receivedInputs[1].DeltaX != 3 || receivedInputs[1].Rotation != 45 || receivedInputs[1].Actions != 0x99 {
		t.Errorf("2do input crudo incorrecto: %+v", receivedInputs[1])
	}

	// Snapshot: StatePayload debe llegar byte a byte igual al que provee el juego.
	snapPayload := client.readUntil(t, 2*time.Second, func(pt byte, _ []byte) bool { return pt == PacketSnapshot })
	if snapPayload == nil {
		t.Fatal("no llegó snapshot")
	}
	snap, err := DecodeSnapshot(snapPayload)
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if len(snap.StatePayload) != len(fakeState) {
		t.Fatalf("StatePayload longitud distinta: %d vs %d", len(snap.StatePayload), len(fakeState))
	}
	for i := range fakeState {
		if snap.StatePayload[i] != fakeState[i] {
			t.Fatalf("StatePayload difiere en byte %d", i)
		}
	}

	// Evento custom vía QueueEvent.
	host.QueueEvent(GameEvent{PlayerID: client.playerID, EventType: 42, Data: []byte{9, 9}})
	evtPayload := client.readUntil(t, 2*time.Second, func(pt byte, payload []byte) bool {
		if pt != PacketSnapshot {
			return false
		}
		s, err := DecodeSnapshot(payload)
		return err == nil && len(s.Events) > 0
	})
	if evtPayload == nil {
		t.Fatal("no llegó snapshot con el evento custom")
	}
	snap2, _ := DecodeSnapshot(evtPayload)
	if snap2.Events[0].EventType != 42 || len(snap2.Events[0].Data) != 2 {
		t.Fatalf("evento custom no preservado: %+v", snap2.Events[0])
	}

	// Ping/pong.
	client.sendPing(t)
	pongPayload := client.readUntil(t, 2*time.Second, func(pt byte, _ []byte) bool { return pt == PacketPing })
	if pongPayload == nil {
		t.Fatal("no llegó pong")
	}
}

func TestHeartbeatSurvives10Seconds(t *testing.T) {
	if testing.Short() {
		t.Skip("test largo (12s), se salta con -short")
	}

	host := NewNetworkHost()
	server := NewServer(func() *NetworkHost { return host }, ServerOptions{})
	if err := server.StartUDP(29998); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	time.Sleep(200 * time.Millisecond)

	client := newTestClient(t, 29998)
	client.handshake(t, 1)

	start := time.Now()
	for time.Since(start) < 12*time.Second {
		client.sendInput(t, 1, 1, 0, 0, false)
		time.Sleep(500 * time.Millisecond)
	}

	if host.ConnectedPlayerCount() != 1 {
		t.Fatalf("cliente se desconectó tras 12s de actividad (bug de heartbeat reintroducido)")
	}
}

// TestCreateAndJoinRoom prueba el flujo genérico de salas: un cliente crea
// una sala (sin código todavía) y recibe uno; otro cliente se une con ese
// código y termina en la MISMA sala (mismo NetworkHost) que el primero —
// sin que el core sepa nada de qué juego es ni de qué son los clientes.
func TestCreateAndJoinRoom(t *testing.T) {
	var created []*NetworkHost
	factory := func() *NetworkHost {
		h := NewNetworkHost()
		created = append(created, h)
		return h
	}
	server := NewServer(factory, ServerOptions{})
	if err := server.StartUDP(29997); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	time.Sleep(200 * time.Millisecond)

	creator := newTestClient(t, 29997)
	creator.handshake(t, 1)
	if creator.roomCode == "" {
		t.Fatal("el creador no recibió código de sala")
	}

	joiner := newTestClient(t, 29997)
	joiner.join(t, 2, creator.roomCode)

	if joiner.roomCode != creator.roomCode {
		t.Fatalf("joiner terminó en otra sala: %q vs %q", joiner.roomCode, creator.roomCode)
	}
	if creator.playerID == joiner.playerID {
		t.Fatalf("ambos clientes recibieron el mismo PlayerID (%d)", creator.playerID)
	}
	if len(created) != 1 {
		t.Fatalf("se creó más de una sala (%d) para un solo Create + un solo Join", len(created))
	}
	if got := created[0].ConnectedPlayerCount(); got != 2 {
		t.Fatalf("esperaba 2 clientes en la sala, hay %d", got)
	}
}

// TestJoinNonexistentRoomRejected: unirse a un código que no existe debe
// rechazarse con PacketDisconnect/ReasonRoomNotFound, sin crear nada.
func TestJoinNonexistentRoomRejected(t *testing.T) {
	server := NewServer(func() *NetworkHost { return NewNetworkHost() }, ServerOptions{})
	if err := server.StartUDP(29996); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	time.Sleep(200 * time.Millisecond)

	client := newTestClient(t, 29996)
	reason := client.handshakeExpectReject(t, HandshakeModeJoin, 1, "ZZZZZZ")
	if reason != ReasonRoomNotFound {
		t.Fatalf("reason=%d, esperaba ReasonRoomNotFound(%d)", reason, ReasonRoomNotFound)
	}
}

// TestMaxPlayersPerRoomRejectsExtraJoins: el tope es puramente numérico —
// el Server no sabe qué rol tiene cada cliente, solo cuenta.
func TestMaxPlayersPerRoomRejectsExtraJoins(t *testing.T) {
	server := NewServer(func() *NetworkHost { return NewNetworkHost() }, ServerOptions{MaxPlayersPerRoom: 2})
	if err := server.StartUDP(29995); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	time.Sleep(200 * time.Millisecond)

	creator := newTestClient(t, 29995)
	creator.handshake(t, 1)

	second := newTestClient(t, 29995)
	second.join(t, 1, creator.roomCode)

	third := newTestClient(t, 29995)
	reason := third.handshakeExpectReject(t, HandshakeModeJoin, 1, creator.roomCode)
	if reason != ReasonRoomFull {
		t.Fatalf("reason=%d, esperaba ReasonRoomFull(%d)", reason, ReasonRoomFull)
	}
}

// TestRoleIsOpaqueButExposed: el core no interpreta Role, pero lo guarda y
// lo pasa a OnPlayerConnected y a GetClientRole — de ahí en más es 100%
// decisión del juego qué hacer con ese valor.
func TestRoleIsOpaqueButExposed(t *testing.T) {
	host := NewNetworkHost()

	var mu sync.Mutex
	gotRoles := map[uint16]uint8{}
	host.OnPlayerConnected = func(playerID uint16, role uint8, reconnected bool) {
		mu.Lock()
		defer mu.Unlock()
		gotRoles[playerID] = role
	}

	server := NewServer(func() *NetworkHost { return host }, ServerOptions{})
	if err := server.StartUDP(29994); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	time.Sleep(200 * time.Millisecond)

	const roleA, roleB uint8 = 7, 42
	clientA := newTestClient(t, 29994)
	clientA.handshake(t, roleA)
	clientB := newTestClient(t, 29994)
	clientB.join(t, roleB, clientA.roomCode)

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if gotRoles[clientA.playerID] != roleA {
		t.Errorf("rol de A: got %d, want %d", gotRoles[clientA.playerID], roleA)
	}
	if gotRoles[clientB.playerID] != roleB {
		t.Errorf("rol de B: got %d, want %d", gotRoles[clientB.playerID], roleB)
	}

	if role, ok := host.GetClientRole(clientA.playerID); !ok || role != roleA {
		t.Errorf("GetClientRole(A) = %d, %v — want %d, true", role, ok, roleA)
	}
}

// TestQueueReliableEventRetransmitsUntilAcked: un evento reliable llega,
// se retransmite mientras el cliente no lo confirme, y deja de retransmitir
// (la cola queda vacía) apenas el cliente manda el ack.
func TestQueueReliableEventRetransmitsUntilAcked(t *testing.T) {
	host := NewNetworkHost()
	server := NewServer(func() *NetworkHost { return host }, ServerOptions{})
	if err := server.StartUDP(29993); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	time.Sleep(200 * time.Millisecond)

	client := newTestClient(t, 29993)
	client.handshake(t, 1)

	host.QueueReliableEvent(GameEvent{PlayerID: client.playerID, EventType: 9, Data: []byte{7}})

	isTargetEvent := func(pt byte, payload []byte) bool {
		if pt != PacketSnapshot {
			return false
		}
		s, err := DecodeSnapshot(payload)
		return err == nil && len(s.Events) == 1 && s.Events[0].EventType == 9
	}

	seq, payload := client.readUntilWithSeq(t, 1*time.Second, isTargetEvent)
	if payload == nil {
		t.Fatal("no llegó el evento reliable")
	}

	if host.PendingReliableCount(client.playerID) == 0 {
		t.Fatal("el evento reliable no quedó en la cola de retransmisión")
	}

	// retryInterval es 100ms — sin ackear, debería seguir llegando.
	if _, retried := client.readUntilWithSeq(t, 500*time.Millisecond, isTargetEvent); retried == nil {
		t.Fatal("el evento reliable no se retransmitió sin ack")
	}

	client.sendAck(t, seq)

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if host.PendingReliableCount(client.playerID) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("la cola de retransmisión no se vació después del ack")
}

// TestReconnectionByIPPreservesIdentity: un cliente que se cae (deja de
// mandar heartbeat) y vuelve a conectarse desde la misma IP a la misma sala
// recibe el mismo PlayerID y el Role ORIGINAL — no el nuevo que mande en el
// segundo handshake — ver el comentario grande en AdmitPlayer.
func TestReconnectionByIPPreservesIdentity(t *testing.T) {
	if testing.Short() {
		t.Skip("test largo (10s+), se salta con -short")
	}

	host := NewNetworkHost()

	var mu sync.Mutex
	var playerIDs []uint16
	var reconnectedFlags []bool
	host.OnPlayerConnected = func(playerID uint16, role uint8, reconnected bool) {
		mu.Lock()
		defer mu.Unlock()
		playerIDs = append(playerIDs, playerID)
		reconnectedFlags = append(reconnectedFlags, reconnected)
	}

	server := NewServer(func() *NetworkHost { return host }, ServerOptions{})
	if err := server.StartUDP(29992); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	time.Sleep(200 * time.Millisecond)

	first := newTestClient(t, 29992)
	first.handshake(t, 7)
	firstID := first.playerID
	roomCode := first.roomCode
	first.conn.Close() // simula la caída: deja de mandar heartbeat

	time.Sleep(heartbeatTimeout + 500*time.Millisecond)

	second := newTestClient(t, 29992)
	second.join(t, 3, roomCode) // role distinto a propósito: debe ignorarse

	if second.playerID != firstID {
		t.Fatalf("reconexión no preservó el PlayerID: got %d, want %d", second.playerID, firstID)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(reconnectedFlags) != 2 {
		t.Fatalf("esperaba 2 llamadas a OnPlayerConnected, hubo %d", len(reconnectedFlags))
	}
	if reconnectedFlags[0] {
		t.Error("la primera conexión no debería marcarse reconnected")
	}
	if !reconnectedFlags[1] {
		t.Error("la segunda conexión debería reconocerse como reconnected")
	}
	if role, ok := host.GetClientRole(firstID); !ok || role != 7 {
		t.Errorf("Role tras reconexión = %d, ok=%v — quería el original (7), no el de la reconexión (3)", role, ok)
	}
}

// TestUDPPortEphemeral: pedir el puerto 0 le deja al SO elegir uno libre, y
// UDPPort() debe devolver cuál asignó de verdad (no 0) — hace falta para un
// deployment que no quiere depender de que un puerto fijo esté siempre
// libre.
func TestUDPPortEphemeral(t *testing.T) {
	server := NewServer(func() *NetworkHost { return NewNetworkHost() }, ServerOptions{})
	if err := server.StartUDP(0); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	if server.UDPPort() == 0 {
		t.Fatal("UDPPort() debería devolver el puerto real asignado por el SO, no 0")
	}

	client := newTestClient(t, int(server.UDPPort()))
	client.handshake(t, 1)
	if client.playerID == 0 {
		t.Fatal("handshake contra el puerto efímero falló")
	}
}
