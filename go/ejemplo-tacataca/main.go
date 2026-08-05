// Ejemplo de "juego" construido ENCIMA de networkcore, no dentro de él.
// Prueba que el core es de verdad genérico: NetworkHost no sabe qué es una
// "barra" ni una "pelota" — este paquete es el que define ese esquema y lo
// conecta a los hooks genéricos (OnInput/OnTick/StateProvider/QueueEvent).
// Análogo a nucleo-multiplayer/csharp/NetworkCore.Tests/TacaTacaExample.cs.
package main

import (
	"encoding/binary"
	"log"
	"sync"

	"nucleo-multiplayer/networkcore"
)

const eventTypeGoal = 1

type rodState struct {
	playerID uint16
	position int32
	rotation uint16
}

type tacaTacaState struct {
	mu         sync.Mutex
	ballX      int32
	ballY      int32
	scoreTeamA uint8
	scoreTeamB uint8
	rods       []*rodState
}

// encode: formato propio de este ejemplo (no es parte del protocolo core —
// vive dentro de StatePayload, que el core trata como bytes opacos):
// BallX(4) + BallY(4) + ScoreA(1) + ScoreB(1) + RodCount(1) + por barra:
// PlayerID(2) + Position(4) + Rotation(2)
func (s *tacaTacaState) encode() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf := make([]byte, 0, 10+len(s.rods)*8)
	tmp4 := make([]byte, 4)

	binary.BigEndian.PutUint32(tmp4, uint32(s.ballX))
	buf = append(buf, tmp4...)
	binary.BigEndian.PutUint32(tmp4, uint32(s.ballY))
	buf = append(buf, tmp4...)
	buf = append(buf, s.scoreTeamA, s.scoreTeamB, byte(len(s.rods)))

	for _, r := range s.rods {
		tmp2 := make([]byte, 2)
		binary.BigEndian.PutUint16(tmp2, r.playerID)
		buf = append(buf, tmp2...)
		binary.BigEndian.PutUint32(tmp4, uint32(r.position))
		buf = append(buf, tmp4...)
		binary.BigEndian.PutUint16(tmp2, r.rotation)
		buf = append(buf, tmp2...)
	}

	return buf
}

func (s *tacaTacaState) attachTo(host *networkcore.NetworkHost) {
	host.OnPlayerConnected = func(id uint16) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.rods = append(s.rods, &rodState{playerID: id})
	}

	host.OnPlayerDisconnected = func(id uint16) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, r := range s.rods {
			if r.playerID == id {
				s.rods = append(s.rods[:i], s.rods[i+1:]...)
				break
			}
		}
	}

	host.OnInput = func(input networkcore.PlayerInput) {
		s.mu.Lock()
		var rod *rodState
		for _, r := range s.rods {
			if r.playerID == input.PlayerID {
				rod = r
				break
			}
		}
		if rod == nil {
			s.mu.Unlock()
			return
		}

		rod.position += int32(input.DeltaX) // el juego decide qué significa DeltaX
		rod.rotation = input.Rotation

		goal := input.Actions&0x01 != 0 // bit 0 = "gol" (definido acá, no por el core)
		if goal {
			s.scoreTeamA++
		}
		s.mu.Unlock()

		if goal {
			host.QueueEvent(networkcore.GameEvent{
				PlayerID:  input.PlayerID,
				EventType: eventTypeGoal,
				Data:      []byte{},
			})
		}
	}

	host.StateProvider = s.encode
}

func main() {
	host := networkcore.NewNetworkHost()
	state := &tacaTacaState{}
	state.attachTo(host)

	if err := host.Start(9999); err != nil {
		log.Fatalf("error: %v", err)
	}

	select {}
}
