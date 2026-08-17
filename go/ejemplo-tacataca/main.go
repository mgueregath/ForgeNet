// Ejemplo de "juego" construido ENCIMA de networkcore, no dentro de él.
// Prueba que el core es de verdad genérico: ni NetworkHost ni Server saben
// qué es una "barra", una "pelota" ni un "tablero" — este paquete es el que
// define ese esquema y lo conecta a los hooks genéricos
// (OnInput/OnTick/StateProvider/QueueEvent), incluido el significado de
// Role, que el core solo transporta sin interpretar.
// Análogo a forgenet/csharp/NetworkCore.Tests/TacaTacaExample.cs.
package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/mgueregath/ForgeNet/go/networkcore"
)

const eventTypeGoal = 1

// Roles de taca-taca — opacos para el core, definidos y usados solo acá.
// roleBoard es el tablero (recibe el mismo snapshot que todos, pero no
// controla ninguna barra); rolePaddle es un mando/jugador.
const (
	roleBoard  uint8 = 0x01
	rolePaddle uint8 = 0x02
)

// maxPaddles: cupo de mandos por partida — regla de taca-taca, no del
// core (Server.MaxPlayersPerRoom es un tope numérico genérico que no
// distingue roles; este chequeo sí lo hace).
const maxPaddles = 2

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
	// Role es opaco para el core: acá es donde taca-taca decide qué hacer
	// con cada valor. El tablero (roleBoard) recibe el mismo snapshot que
	// todos (ve la partida completa) pero no controla ninguna barra. Un
	// mando (rolePaddle) más allá del cupo tampoco recibe barra — se queda
	// conectado, pero sin efecto en el juego (rechazarlo requeriría que el
	// core soporte desconexión post-admisión, que hoy no tiene).
	host.OnPlayerConnected = func(id uint16, role uint8) {
		if role != rolePaddle {
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if len(s.rods) >= maxPaddles {
			return
		}
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
	// roomFactory: se llama una vez por sala creada (Server.HandshakeModeCreate)
	// — cada partida de taca-taca es independiente, con su propio estado
	// (pelota, score, barras). El Server no sabe nada de esto: solo llama
	// a la factory y le rutea paquetes a lo que devuelve.
	roomFactory := func() *networkcore.NetworkHost {
		host := networkcore.NewNetworkHost()
		state := &tacaTacaState{}
		state.attachTo(host)
		return host
	}

	// MaxPlayersPerRoom es el tope genérico del Server (cuenta clientes,
	// sin distinguir roles) — para taca-taca da igual a maxPaddles+1
	// (tablero). El cupo específico por rol (2 barras exactas) lo aplica
	// tacaTacaState.attachTo arriba.
	server := networkcore.NewServer(roomFactory, networkcore.ServerOptions{
		MaxPlayersPerRoom: maxPaddles + 1,
	})

	// Transporte UDP — clientes nativos (ej. Unity).
	if err := server.StartUDP(9999); err != nil {
		log.Fatalf("error arrancando UDP: %v", err)
	}

	// Transporte WebTransport — clientes de navegador. Mismo Server, así
	// que un cliente UDP y uno de navegador pueden terminar en la misma
	// sala (ver TestUDPAndWebTransportSamePlayerSpace en networkcore).
	devCert, err := server.StartWebTransport(networkcore.WebTransportOptions{Addr: ":9443"})
	if err != nil {
		log.Fatalf("error arrancando WebTransport: %v", err)
	}
	log.Printf("🔑 Certificate hash (sha-256, base64): %s", devCert.HashBase64)

	// Servidor HTTP aparte, solo para servir la página de prueba del
	// navegador (forgenet/web/) y exponer el hash del certificado dev sin
	// tener que copiarlo a mano.
	startBrowserPageServer(devCert.HashBase64)

	select {}
}

func startBrowserPageServer(certHash string) {
	const addr = ":8080"
	const webDir = "../../web" // forgenet/go/ejemplo-tacataca -> forgenet/web

	mux := http.NewServeMux()
	mux.HandleFunc("/certhash", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprint(w, certHash)
	})
	mux.Handle("/", http.FileServer(http.Dir(webDir)))

	go func() {
		log.Printf("🌐 Página de prueba en http://localhost%s/", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("servidor de página de prueba detenido: %v", err)
		}
	}()
}
