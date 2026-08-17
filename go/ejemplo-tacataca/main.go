// Ejemplo de "juego" construido ENCIMA de networkcore, no dentro de él.
// Prueba que el core es de verdad genérico: ni NetworkHost ni Server saben
// qué es una "barra", una "pelota" ni un "tablero" — este paquete es el que
// define ese esquema y lo conecta a los hooks genéricos
// (OnInput/OnTick/StateProvider/QueueEvent), incluido el significado de
// Role, que el core solo transporta sin interpretar.
// Análogo a forgenet/csharp/NetworkCore.Tests/TacaTacaExample.cs.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

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

// Config por variable de entorno — pensado para correr en un contenedor
// (ver ../Dockerfile), donde hardcodear puertos/paths no sirve. Todos
// tienen default para que `go run .` sin nada configurado siga andando
// igual que antes.
type config struct {
	udpPort           uint16
	webTransportAddr  string
	httpAddr          string
	webDir            string
	tlsCertFile       string // vacío = usar certificado dev self-signed
	tlsKeyFile        string
	allowedOrigins    []string
	maxPlayersPerRoom int
}

func loadConfig() config {
	cfg := config{
		udpPort:           9999,
		webTransportAddr:  ":9443",
		httpAddr:          ":8080",
		webDir:            "../../web", // forgenet/go/ejemplo-tacataca -> forgenet/web
		maxPlayersPerRoom: maxPaddles + 1,
	}

	if v := os.Getenv("UDP_PORT"); v != "" {
		if port, err := strconv.ParseUint(v, 10, 16); err == nil {
			cfg.udpPort = uint16(port)
		} else {
			log.Fatalf("UDP_PORT inválido: %v", err)
		}
	}
	if v := os.Getenv("WEBTRANSPORT_ADDR"); v != "" {
		cfg.webTransportAddr = v
	}
	if v := os.Getenv("HTTP_ADDR"); v != "" {
		cfg.httpAddr = v
	}
	if v := os.Getenv("WEB_DIR"); v != "" {
		cfg.webDir = v
	}
	cfg.tlsCertFile = os.Getenv("TLS_CERT_FILE")
	cfg.tlsKeyFile = os.Getenv("TLS_KEY_FILE")
	if v := os.Getenv("ALLOWED_ORIGINS"); v != "" {
		for _, origin := range strings.Split(v, ",") {
			if origin = strings.TrimSpace(origin); origin != "" {
				cfg.allowedOrigins = append(cfg.allowedOrigins, origin)
			}
		}
	}
	if v := os.Getenv("MAX_PLAYERS_PER_ROOM"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.maxPlayersPerRoom = n
		} else {
			log.Fatalf("MAX_PLAYERS_PER_ROOM inválido: %v", err)
		}
	}

	return cfg
}

func main() {
	cfg := loadConfig()

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
	// (tablero) salvo que se pise por env. El cupo específico por rol (2
	// barras exactas) lo aplica tacaTacaState.attachTo arriba.
	server := networkcore.NewServer(roomFactory, networkcore.ServerOptions{
		MaxPlayersPerRoom: cfg.maxPlayersPerRoom,
	})

	// Transporte UDP — clientes nativos (ej. Unity).
	if err := server.StartUDP(cfg.udpPort); err != nil {
		log.Fatalf("error arrancando UDP: %v", err)
	}

	// Transporte WebTransport — clientes de navegador. Mismo Server, así
	// que un cliente UDP y uno de navegador pueden terminar en la misma
	// sala (ver TestUDPAndWebTransportSamePlayerSpace en networkcore).
	//
	// Con TLS_CERT_FILE/TLS_KEY_FILE seteados (certificado real, ej. de
	// Let's Encrypt) se usa ese; si no, cae al self-signed de desarrollo
	// (ver GenerateDevCertificate) — no apto para producción.
	wtOpts := networkcore.WebTransportOptions{
		Addr:           cfg.webTransportAddr,
		AllowedOrigins: cfg.allowedOrigins,
	}
	var certHash string
	if cfg.tlsCertFile != "" && cfg.tlsKeyFile != "" {
		tlsConf, err := networkcore.LoadTLSCertificate(cfg.tlsCertFile, cfg.tlsKeyFile)
		if err != nil {
			log.Fatalf("error cargando certificado TLS: %v", err)
		}
		wtOpts.TLSConfig = tlsConf
		certHash = "(certificado real — sin hash de pinning, el browser ya confía en la CA)"
	}
	devCert, err := server.StartWebTransport(wtOpts)
	if err != nil {
		log.Fatalf("error arrancando WebTransport: %v", err)
	}
	if devCert != nil {
		certHash = devCert.HashBase64
	}
	log.Printf("🔑 Certificate hash (sha-256, base64): %s", certHash)

	// Servidor HTTP aparte, solo para servir la página de prueba del
	// navegador (forgenet/web/) y exponer el hash del certificado dev sin
	// tener que copiarlo a mano.
	httpServer := startBrowserPageServer(cfg.httpAddr, cfg.webDir, certHash)

	waitForShutdownSignal()
	log.Println("apagando: cerrando sala(s) y transportes...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpServer.Shutdown(shutdownCtx)
	server.Stop()
	log.Println("apagado limpio.")
}

// waitForShutdownSignal bloquea hasta SIGINT/SIGTERM — para que un
// `docker stop` o un redeploy le den al server la chance de drenar salas
// en vez de matarlo en seco.
func waitForShutdownSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
}

func startBrowserPageServer(addr, webDir, certHash string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/certhash", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprint(w, certHash)
	})

	if _, err := os.Stat(webDir); err == nil {
		mux.Handle("/", http.FileServer(http.Dir(webDir)))
	} else {
		log.Printf("aviso: WEB_DIR %q no existe, no se sirve la página de prueba (solo /certhash)", webDir)
	}

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("🌐 Página de prueba en http://localhost%s/", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("servidor de página de prueba detenido: %v", err)
		}
	}()
	return srv
}
