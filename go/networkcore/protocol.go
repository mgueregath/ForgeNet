// Package networkcore es el port a Go de nucleo-multiplayer/csharp/NetworkCore:
// transporte UDP genérico (handshake, heartbeat, input, snapshot, reliable,
// ping) sin ningún esquema de juego hardcodeado. El juego se engancha vía
// callbacks (OnInput, OnTick, StateProvider, QueueEvent) — ver host.go.
//
// Es la misma spec de protocolo binario que el core en C#: comparten
// framing (header, handshake, input, ack, ping) y, a diferencia de la
// versión previa de este servidor (que hardcodeaba PlayerState), ahora
// también comparten el formato de snapshot (Tick + StatePayload opaco +
// Events), porque los dos se generalizaron de la misma forma.
package networkcore

import "encoding/binary"

// Packet types
const (
	PacketSnapshot   = 0x01
	PacketInput      = 0x02
	PacketAck        = 0x03
	PacketPing       = 0x04
	PacketHandshake  = 0x05
	PacketDisconnect = 0x06
)

// Flags
const (
	FlagCompressed = 0x01
	FlagReliable   = 0x02
	FlagOrdered    = 0x04
)

// HeaderSize: seq(4) + ack(4) + typeAndFlags(1)
const HeaderSize = 9

func buildHeader(seq, ack uint32, packetType, flags byte) []byte {
	buf := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], seq)
	binary.BigEndian.PutUint32(buf[4:8], ack)
	buf[8] = (packetType & 0x0F) | ((flags & 0x0F) << 4)
	return buf
}

func parseHeader(data []byte) (seq, ack uint32, packetType, flags byte, ok bool) {
	if len(data) < HeaderSize {
		return 0, 0, 0, 0, false
	}
	seq = binary.BigEndian.Uint32(data[0:4])
	ack = binary.BigEndian.Uint32(data[4:8])
	typeAndFlags := data[8]
	packetType = typeAndFlags & 0x0F
	flags = (typeAndFlags >> 4) & 0x0F
	return seq, ack, packetType, flags, true
}
