package networkcore

import (
	"log"
	"net"
)

// udpPeer implementa Peer para un cliente UDP crudo (ej. un cliente Unity).
type udpPeer struct {
	conn *net.UDPConn
	addr *net.UDPAddr
}

func (p *udpPeer) Send(data []byte) {
	p.conn.WriteToUDP(data, p.addr)
}

func (p *udpPeer) Key() string {
	return "udp:" + p.addr.String()
}

// Start es un alias de StartUDP — nombre histórico, se mantiene por
// compatibilidad con el código existente (tests, ejemplos).
func (h *NetworkHost) Start(port uint16) error {
	return h.StartUDP(port)
}

// StartUDP liga un socket UDP y arranca su loop de recepción. Se puede
// combinar con StartWebTransport en el mismo NetworkHost — ambos
// transportes alimentan el mismo HandlePacket, así que un cliente UDP
// (Unity) y un cliente WebTransport (navegador) terminan en la misma
// partida.
func (h *NetworkHost) StartUDP(port uint16) error {
	addr := net.UDPAddr{Port: int(port), IP: net.ParseIP("0.0.0.0")}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		return err
	}

	conn.SetReadBuffer(1 << 20)
	conn.SetWriteBuffer(1 << 20)

	h.ensureLoopsStarted()
	h.registerCloser(func() { conn.Close() })
	go h.udpReceiveLoop(conn)

	log.Printf("🎮 NetworkHost (UDP) escuchando en :%d", port)
	return nil
}

func (h *NetworkHost) udpReceiveLoop(conn *net.UDPConn) {
	buffer := make([]byte, 2048)
	for h.running {
		n, remoteAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if h.running {
				continue
			}
			return
		}

		packet := make([]byte, n)
		copy(packet, buffer[:n])
		h.HandlePacket(packet, &udpPeer{conn: conn, addr: remoteAddr})
	}
}
