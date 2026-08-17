package networkcore

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// TestNetworkClientCreateAndJoinRoom: el NetworkClient real (no el
// testClient de host_test.go, que es un doble mínimo para tests) crea una
// sala, otro se une por código, y ambos terminan hablando con el mismo
// Server/sala.
func TestNetworkClientCreateAndJoinRoom(t *testing.T) {
	fakeState := []byte{1, 2, 3}
	factory := func() *NetworkHost {
		h := NewNetworkHost()
		h.StateProvider = func() []byte { return fakeState }
		return h
	}
	server := NewServer(factory, ServerOptions{})
	if err := server.StartUDP(29988); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	time.Sleep(200 * time.Millisecond)

	creator := NewNetworkClient()
	var mu sync.Mutex
	var snapshots int
	creator.OnSnapshot = func(s *GameSnapshot) {
		mu.Lock()
		defer mu.Unlock()
		snapshots++
	}
	if err := creator.CreateRoom("127.0.0.1", 29988, 1, 2*time.Second); err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	defer creator.Disconnect()
	if creator.PlayerID == 0 {
		t.Fatal("PlayerID no asignado")
	}
	if creator.RoomCode == "" {
		t.Fatal("RoomCode no asignado")
	}

	joiner := NewNetworkClient()
	if err := joiner.JoinRoom("127.0.0.1", 29988, 2, creator.RoomCode, 2*time.Second); err != nil {
		t.Fatalf("JoinRoom: %v", err)
	}
	defer joiner.Disconnect()
	if joiner.PlayerID == creator.PlayerID {
		t.Fatal("ambos clientes recibieron el mismo PlayerID")
	}
	if joiner.RoomCode != creator.RoomCode {
		t.Fatalf("joiner terminó en otra sala: %q vs %q", joiner.RoomCode, creator.RoomCode)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := snapshots
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if snapshots == 0 {
		t.Fatal("el cliente creador nunca recibió un snapshot")
	}
}

func TestNetworkClientJoinNonexistentRoomRejected(t *testing.T) {
	server := NewServer(func() *NetworkHost { return NewNetworkHost() }, ServerOptions{})
	if err := server.StartUDP(29987); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	time.Sleep(200 * time.Millisecond)

	client := NewNetworkClient()
	err := client.JoinRoom("127.0.0.1", 29987, 1, "ZZZZZZ", 2*time.Second)
	if err == nil {
		t.Fatal("esperaba error de handshake rechazado")
	}
	rejErr, ok := err.(*HandshakeRejectedError)
	if !ok {
		t.Fatalf("error no es *HandshakeRejectedError: %v (%T)", err, err)
	}
	if rejErr.Reason != ReasonRoomNotFound {
		t.Fatalf("reason=%d, esperaba ReasonRoomNotFound(%d)", rejErr.Reason, ReasonRoomNotFound)
	}
}

// TestNetworkClientAutoAcksReliableEvent: el cliente real debe confirmar
// automáticamente un snapshot FlagReliable (ver QueueReliableEvent en
// host.go) — si no lo hiciera, el server lo reintentaría para siempre.
func TestNetworkClientAutoAcksReliableEvent(t *testing.T) {
	host := NewNetworkHost()
	server := NewServer(func() *NetworkHost { return host }, ServerOptions{})
	if err := server.StartUDP(29986); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	time.Sleep(200 * time.Millisecond)

	client := NewNetworkClient()
	var mu sync.Mutex
	var gotEvent bool
	client.OnSnapshot = func(s *GameSnapshot) {
		mu.Lock()
		defer mu.Unlock()
		if len(s.Events) > 0 && s.Events[0].EventType == 9 {
			gotEvent = true
		}
	}
	if err := client.CreateRoom("127.0.0.1", 29986, 1, 2*time.Second); err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	defer client.Disconnect()

	host.QueueReliableEvent(GameEvent{PlayerID: client.PlayerID, EventType: 9, Data: []byte{1}})

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := gotEvent
		mu.Unlock()
		if got {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	got := gotEvent
	mu.Unlock()
	if !got {
		t.Fatal("el cliente nunca recibió el evento reliable")
	}

	deadline = time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if host.PendingReliableCount(client.PlayerID) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("el cliente no ackeó el evento reliable (la cola de retransmisión no se vació)")
}

// TestEmbeddedHostMode prueba el modo "host embebido" (ej. un tablero LAN
// que corre el Server él mismo): la sala existe de entrada, vía
// Server.CreateRoom() — sin ningún cliente todavía — y los mandos se unen
// después con JoinRoom, exactamente igual que en el modo "server online"
// (TestNetworkClientCreateAndJoinRoom, donde es un CLIENTE el que crea la
// sala por handshake de red). Mismo Server, mismo NetworkClient, misma
// sala — la única diferencia es quién la creó y cómo.
func TestEmbeddedHostMode(t *testing.T) {
	fakeState := []byte{9, 9, 9}
	factory := func() *NetworkHost {
		h := NewNetworkHost()
		h.StateProvider = func() []byte { return fakeState }
		return h
	}
	server := NewServer(factory, ServerOptions{})
	if err := server.StartUDP(29985); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	roomCode := server.CreateRoom()
	if roomCode == "" {
		t.Fatal("CreateRoom() no devolvió código de sala")
	}
	time.Sleep(200 * time.Millisecond)

	mando := NewNetworkClient()
	if err := mando.JoinRoom("127.0.0.1", 29985, 1, roomCode, 2*time.Second); err != nil {
		t.Fatalf("JoinRoom contra la sala pre-creada: %v", err)
	}
	defer mando.Disconnect()
	if mando.PlayerID == 0 {
		t.Fatal("PlayerID no asignado")
	}
	if mando.RoomCode != roomCode {
		t.Fatalf("el mando terminó en otra sala: %q vs %q", mando.RoomCode, roomCode)
	}
}

// TestEmbeddedHostRoomSurvivesEmptyGracePeriod: una sala pre-creada (modo
// host embebido) que todavía no admitió a NADIE no debe destruirse nunca
// por el janitor, sin importar cuánto tiempo pase — el tablero puede
// quedarse esperando en el lobby indefinidamente. Solo empieza a correr el
// EmptyRoomGracePeriod una vez que la sala tuvo al menos un cliente (ver
// hadPlayer en server.go).
func TestEmbeddedHostRoomSurvivesEmptyGracePeriod(t *testing.T) {
	server := NewServer(func() *NetworkHost { return NewNetworkHost() }, ServerOptions{
		EmptyRoomGracePeriod: 300 * time.Millisecond,
	})
	if err := server.StartUDP(29984); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	roomCode := server.CreateRoom()

	// Esperar bastante más que el (muy corto, a propósito) grace period —
	// si la sala vacía-sin-nadie-nunca fuera tratada igual que una vacía-
	// tras-tener-gente, el janitor ya la habría destruido acá.
	time.Sleep(1 * time.Second)

	client := NewNetworkClient()
	if err := client.JoinRoom("127.0.0.1", 29984, 1, roomCode, 2*time.Second); err != nil {
		t.Fatalf("la sala pre-creada no sobrevivió la espera: %v", err)
	}
	defer client.Disconnect()
}

// TestCreateRoomWithCode: variante de TestEmbeddedHostMode con un código
// elegido por el caller en vez de uno random — el caso "host embebido, una
// sola sala fija por proceso" (ver el comentario grande de
// CreateRoomWithCode en server.go).
func TestCreateRoomWithCode(t *testing.T) {
	server := NewServer(func() *NetworkHost { return NewNetworkHost() }, ServerOptions{})
	if err := server.StartUDP(29983); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	code, err := server.CreateRoomWithCode("MESA01")
	if err != nil {
		t.Fatalf("CreateRoomWithCode: %v", err)
	}
	if code != "MESA01" {
		t.Fatalf("código devuelto no coincide: %q", code)
	}

	mando := NewNetworkClient()
	if err := mando.JoinRoom("127.0.0.1", 29983, 1, "MESA01", 2*time.Second); err != nil {
		t.Fatalf("JoinRoom contra la sala de código fijo: %v", err)
	}
	defer mando.Disconnect()

	if _, err := server.CreateRoomWithCode("MESA01"); !errors.Is(err, ErrRoomCodeTaken) {
		t.Fatalf("esperaba ErrRoomCodeTaken creando un código repetido, recibí: %v", err)
	}
	if _, err := server.CreateRoomWithCode(""); !errors.Is(err, ErrRoomCodeEmpty) {
		t.Fatalf("esperaba ErrRoomCodeEmpty con código vacío, recibí: %v", err)
	}
}
