package networkcore

import (
	"encoding/binary"
	"errors"
)

// GameEvent es genérico: el core no sabe qué significa EventType ni qué hay
// en Data — cada juego define su propia tabla de tipos y formato de payload.
type GameEvent struct {
	PlayerID  uint16
	EventType uint8
	Data      []byte
}

// GameSnapshot: Tick/Timestamp son del core (sincronización); el estado del
// juego en sí viaja como bytes opacos en StatePayload — el core nunca lo
// interpreta, solo lo transporta.
type GameSnapshot struct {
	Tick         uint64
	Timestamp    int64
	StatePayload []byte
	Events       []GameEvent
}

// Encode: Tick(8) + StatePayloadLen(4) + StatePayload(N) + EventCount(1) +
// Events(PlayerID(2)+EventType(1)+DataLen(1)+Data).
func (s *GameSnapshot) Encode() []byte {
	buf := make([]byte, 0, HeaderSize+len(s.StatePayload)+32)

	tickBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(tickBuf, s.Tick)
	buf = append(buf, tickBuf...)

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(s.StatePayload)))
	buf = append(buf, lenBuf...)
	buf = append(buf, s.StatePayload...)

	buf = append(buf, byte(len(s.Events)))
	for _, e := range s.Events {
		buf = append(buf, byte(e.PlayerID>>8), byte(e.PlayerID))
		buf = append(buf, e.EventType)
		buf = append(buf, byte(len(e.Data)))
		buf = append(buf, e.Data...)
	}

	return buf
}

var errMalformedSnapshot = errors.New("networkcore: snapshot payload malformado")

// DecodeSnapshot nunca hace panic: un payload corrupto o truncado devuelve
// error en vez de indexar fuera de rango.
func DecodeSnapshot(payload []byte) (*GameSnapshot, error) {
	offset := 0
	if len(payload) < 8+4+1 {
		return nil, errMalformedSnapshot
	}

	s := &GameSnapshot{Tick: binary.BigEndian.Uint64(payload[offset:])}
	offset += 8

	stateLen := binary.BigEndian.Uint32(payload[offset:])
	offset += 4
	if offset+int(stateLen) > len(payload) {
		return nil, errMalformedSnapshot
	}
	s.StatePayload = make([]byte, stateLen)
	copy(s.StatePayload, payload[offset:offset+int(stateLen)])
	offset += int(stateLen)

	if offset >= len(payload) {
		return nil, errMalformedSnapshot
	}
	eventCount := int(payload[offset])
	offset++

	for i := 0; i < eventCount; i++ {
		if offset+4 > len(payload) {
			return nil, errMalformedSnapshot
		}
		e := GameEvent{PlayerID: binary.BigEndian.Uint16(payload[offset:])}
		offset += 2
		e.EventType = payload[offset]
		offset++
		dataLen := int(payload[offset])
		offset++
		if offset+dataLen > len(payload) {
			return nil, errMalformedSnapshot
		}
		e.Data = make([]byte, dataLen)
		copy(e.Data, payload[offset:offset+dataLen])
		offset += dataLen

		s.Events = append(s.Events, e)
	}

	return s, nil
}

// PlayerInput: forma genérica (movimiento 2D + rotación + bitmask de
// acciones) — el core no interpreta estos valores, solo los transporta y
// se los pasa al juego vía NetworkHost.OnInput.
type PlayerInput struct {
	PlayerID  uint16
	Seq       uint32
	Timestamp int64
	DeltaX    int16
	DeltaY    int16
	Rotation  uint16
	Actions   uint32
}

// EncodeInputPayload: payload de 10 bytes que va después del header en un
// PacketInput.
func EncodeInputPayload(deltaX, deltaY int16, rotation uint16, actions uint32) []byte {
	buf := make([]byte, 10)
	binary.BigEndian.PutUint16(buf[0:2], uint16(deltaX))
	binary.BigEndian.PutUint16(buf[2:4], uint16(deltaY))
	binary.BigEndian.PutUint16(buf[4:6], rotation)
	binary.BigEndian.PutUint32(buf[6:10], actions)
	return buf
}

func decodeInputPayload(payload []byte) (deltaX, deltaY int16, rotation uint16, actions uint32, ok bool) {
	if len(payload) < 10 {
		return 0, 0, 0, 0, false
	}
	deltaX = int16(binary.BigEndian.Uint16(payload[0:2]))
	deltaY = int16(binary.BigEndian.Uint16(payload[2:4]))
	rotation = binary.BigEndian.Uint16(payload[4:6])
	actions = binary.BigEndian.Uint32(payload[6:10])
	return deltaX, deltaY, rotation, actions, true
}
