// Package networkcore es el port a Go de forgenet/csharp/NetworkCore:
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

// --- Payload del handshake (join) ---
//
// El core no sabe qué juego corre encima, así que Role viaja como un byte
// opaco: cada juego define sus propios valores y su propio significado (ej.
// taca-taca usa un valor para "barra" y otro para "tablero" — ver su
// ejemplo), igual que ya pasa con StatePayload y GameEvent.EventType.

// Modos de handshake — quién crea una sala nueva vs. quién se une a una
// existente por código. Esto sí es genérico: cualquier juego con sesiones
// de partida necesita esta distinción.
const (
	HandshakeModeCreate uint8 = 0x00
	HandshakeModeJoin   uint8 = 0x01
)

// Motivos de rechazo del handshake, enviados en un PacketDisconnect.
const (
	ReasonRoomNotFound uint8 = 0x01
	ReasonRoomFull     uint8 = 0x02
)

// EncodeJoinPayload: Mode(1) + Role(1) + RoomCodeLen(1) + RoomCode(N). Va
// después del header en un PacketHandshake enviado por el cliente. RoomCode
// se ignora en HandshakeModeCreate (el server genera el código).
func EncodeJoinPayload(mode, role uint8, roomCode string) []byte {
	buf := make([]byte, 0, 3+len(roomCode))
	buf = append(buf, mode, role, byte(len(roomCode)))
	buf = append(buf, roomCode...)
	return buf
}

func decodeJoinPayload(payload []byte) (mode, role uint8, roomCode string, ok bool) {
	if len(payload) < 3 {
		return 0, 0, "", false
	}
	codeLen := int(payload[2])
	if len(payload) < 3+codeLen {
		return 0, 0, "", false
	}
	return payload[0], payload[1], string(payload[3 : 3+codeLen]), true
}

// encodeHandshakeAck: PlayerID(2) + RoomCodeLen(1) + RoomCode(N). Va después
// del header en la respuesta del server — tanto quien crea la sala como
// quien se une reciben el código, para poder compartirlo (ej. un QR).
func encodeHandshakeAck(playerID uint16, roomCode string) []byte {
	buf := make([]byte, 0, 3+len(roomCode))
	buf = append(buf, byte(playerID>>8), byte(playerID), byte(len(roomCode)))
	buf = append(buf, roomCode...)
	return buf
}
