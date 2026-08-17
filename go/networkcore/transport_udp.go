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

// StartUDP liga un socket UDP y arranca su loop de recepción. Se puede
// combinar con StartWebTransport en el mismo Server — ambos transportes
// alimentan el mismo Server.HandlePacket, así que un cliente UDP (Unity) y
// un cliente WebTransport (navegador) pueden terminar en la misma sala.
func (s *Server) StartUDP(port uint16) error {
	addr := net.UDPAddr{Port: int(port), IP: net.ParseIP("0.0.0.0")}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		return err
	}

	conn.SetReadBuffer(1 << 20)
	conn.SetWriteBuffer(1 << 20)

	s.registerCloser(func() { conn.Close() })
	go s.udpReceiveLoop(conn)

	log.Printf("🎮 Server (UDP) escuchando en :%d", port)
	return nil
}

func (s *Server) udpReceiveLoop(conn *net.UDPConn) {
	buffer := make([]byte, 2048)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				continue
			}
		}

		packet := make([]byte, n)
		copy(packet, buffer[:n])
		s.HandlePacket(packet, &udpPeer{conn: conn, addr: remoteAddr})
	}
}
