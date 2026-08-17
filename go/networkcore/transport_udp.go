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

func (p *udpPeer) IP() string {
	return p.addr.IP.String()
}

// StartUDP liga un socket UDP y arranca su loop de recepción. Se puede
// combinar con StartWebTransport en el mismo Server — ambos transportes
// alimentan el mismo Server.HandlePacket, así que un cliente UDP (Unity) y
// un cliente WebTransport (navegador) pueden terminar en la misma sala.
//
// port=0 le pide al SO cualquier puerto disponible — útil para un
// deployment que no quiere depender de que un puerto fijo esté libre. El
// puerto real que asignó el SO queda disponible en UDPPort().
func (s *Server) StartUDP(port uint16) error {
	addr := net.UDPAddr{Port: int(port), IP: net.ParseIP("0.0.0.0")}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		return err
	}

	conn.SetReadBuffer(1 << 20)
	conn.SetWriteBuffer(1 << 20)
	s.udpPort = uint16(conn.LocalAddr().(*net.UDPAddr).Port)

	s.registerCloser(func() { conn.Close() })
	go s.udpReceiveLoop(conn)

	log.Printf("🎮 Server (UDP) escuchando en :%d", s.udpPort)
	return nil
}

// UDPPort devuelve el puerto UDP real en el que quedó escuchando el
// server — el mismo que se pidió en StartUDP, salvo que se haya pedido 0
// (cualquiera disponible), en cuyo caso es el que el SO asignó de verdad.
// 0 si StartUDP nunca se llamó (o falló).
func (s *Server) UDPPort() uint16 {
	return s.udpPort
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
