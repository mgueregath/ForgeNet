package networkcore

import (
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
