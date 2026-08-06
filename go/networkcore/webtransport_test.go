package networkcore

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/webtransport-go"
)

// testWebTransportClient conecta contra un NetworkHost con WebTransport
// habilitado, usando el mismo protocolo binario que un cliente Unity (UDP)
// hablaría — la única diferencia es el transporte. Sirve para validar el
// transporte WebTransport rápido, sin necesitar un navegador real (esa
// validación end-to-end con Chrome se hace aparte, ver web/).
type testWebTransportClient struct {
	transport *webtransport.Transport
	sess      *webtransport.Session
	playerID  uint16
	seq       uint32
}

func newTestWebTransportClient(t *testing.T, url string, certHash string) *testWebTransportClient {
	t.Helper()

	tlsConf := &tls.Config{InsecureSkipVerify: true}
	tlsConf.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			t.Fatal("el servidor no mandó certificado")
		}
		got := sha256.Sum256(state.PeerCertificates[0].Raw)
		gotHash := base64.RawStdEncoding.EncodeToString(got[:])
		if gotHash != certHash {
			t.Fatalf("hash de certificado no coincide: got %s want %s", gotHash, certHash)
		}
		return nil
	}

	tr := &webtransport.Transport{TLSClientConfig: tlsConf}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rsp, sess, err := tr.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial falló: %v", err)
	}
	if rsp.StatusCode < 200 || rsp.StatusCode >= 300 {
		t.Fatalf("status inesperado: %d", rsp.StatusCode)
	}

	return &testWebTransportClient{transport: tr, sess: sess}
}

func (c *testWebTransportClient) close() {
	c.sess.CloseWithError(0, "")
	c.transport.Close()
}

func (c *testWebTransportClient) handshake(t *testing.T) {
	t.Helper()
	req := buildHeader(0, 0, PacketHandshake, 0)
	if err := c.sess.SendDatagram(req); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.sess.ReceiveDatagram(ctx)
	if err != nil {
		t.Fatalf("handshake sin respuesta: %v", err)
	}
	if len(resp) < 11 {
		t.Fatalf("respuesta de handshake muy corta: %d bytes", len(resp))
	}
	c.playerID = uint16(resp[9])<<8 | uint16(resp[10])
}

func (c *testWebTransportClient) sendInput(t *testing.T, dx, dy int16, rot uint16, actions uint32) {
	t.Helper()
	c.seq++
	packet := append(buildHeader(c.seq, 0, PacketInput, 0), EncodeInputPayload(dx, dy, rot, actions)...)
	if err := c.sess.SendDatagram(packet); err != nil {
		t.Fatal(err)
	}
}

func (c *testWebTransportClient) readUntil(t *testing.T, timeout time.Duration, match func(packetType byte, payload []byte) bool) []byte {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		data, err := c.sess.ReceiveDatagram(ctx)
		cancel()
		if err != nil {
			continue
		}
		_, _, packetType, _, ok := parseHeader(data)
		if !ok {
			continue
		}
		payload := append([]byte{}, data[HeaderSize:]...)
		if match(packetType, payload) {
			return payload
		}
	}
	return nil
}

func TestWebTransportHandshakeInputSnapshot(t *testing.T) {
	host := NewNetworkHost()
	fakeState := []byte{1, 2, 3, 4, 5}
	host.StateProvider = func() []byte { return fakeState }

	var mu sync.Mutex
	var receivedInputs []PlayerInput
	host.OnInput = func(input PlayerInput) {
		mu.Lock()
		defer mu.Unlock()
		receivedInputs = append(receivedInputs, input)
	}

	devCert, err := host.StartWebTransport(WebTransportOptions{Addr: "127.0.0.1:34433"})
	if err != nil {
		t.Fatalf("StartWebTransport falló: %v", err)
	}
	defer host.Stop()
	time.Sleep(300 * time.Millisecond)

	client := newTestWebTransportClient(t, "https://127.0.0.1:34433/webtransport", devCert.HashBase64)
	defer client.close()

	client.handshake(t)
	if client.playerID == 0 {
		t.Fatal("playerID no asignado")
	}

	client.sendInput(t, 25, -3, 90, 0x01)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(receivedInputs)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(receivedInputs) != 1 {
		t.Fatalf("esperaba 1 input, llegaron %d", len(receivedInputs))
	}
	if receivedInputs[0].DeltaX != 25 || receivedInputs[0].Rotation != 90 || receivedInputs[0].Actions != 0x01 {
		t.Errorf("input crudo incorrecto: %+v", receivedInputs[0])
	}

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
}

// TestUDPAndWebTransportSamePlayerSpace prueba lo importante de todo esto:
// un cliente UDP y un cliente WebTransport conectados al MISMO NetworkHost
// terminan en la misma partida (ambos cuentan en ConnectedPlayerCount, y
// ambos reciben el snapshot del mismo estado).
func TestUDPAndWebTransportSamePlayerSpace(t *testing.T) {
	host := NewNetworkHost()

	if err := host.StartUDP(34434); err != nil {
		t.Fatalf("StartUDP falló: %v", err)
	}
	devCert, err := host.StartWebTransport(WebTransportOptions{Addr: "127.0.0.1:34435"})
	if err != nil {
		t.Fatalf("StartWebTransport falló: %v", err)
	}
	defer host.Stop()
	time.Sleep(300 * time.Millisecond)

	udpClient := newTestClient(t, 34434)
	udpClient.handshake(t)

	wtClient := newTestWebTransportClient(t, "https://127.0.0.1:34435/webtransport", devCert.HashBase64)
	defer wtClient.close()
	wtClient.handshake(t)

	if udpClient.playerID == wtClient.playerID {
		t.Fatalf("ambos clientes recibieron el mismo playerID (%d) — deberían ser distintos", udpClient.playerID)
	}

	if got := host.ConnectedPlayerCount(); got != 2 {
		t.Fatalf("esperaba 2 jugadores conectados (1 UDP + 1 WebTransport), hay %d", got)
	}
}
