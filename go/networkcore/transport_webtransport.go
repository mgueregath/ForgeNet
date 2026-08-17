package networkcore

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

// wtPeer implementa Peer para una sesión WebTransport (ej. un cliente de
// navegador). Cada datagrama WebTransport se mapea 1:1 a un paquete de
// nuestro protocolo — el core no lo sabe, para él es un Peer más.
type wtPeer struct {
	sess *webtransport.Session
	key  string
}

func (p *wtPeer) Send(data []byte) {
	_ = p.sess.SendDatagram(data)
}

func (p *wtPeer) Key() string {
	return p.key
}

// WebTransportOptions configura el listener WebTransport.
type WebTransportOptions struct {
	// Addr, ej. ":9443". El navegador se conecta a wss/https en ese puerto.
	Addr string
	// Path del endpoint WebTransport, ej. "/webtransport". Default: "/webtransport".
	Path string
	// TLSConfig propio (para producción, con un certificado real de una
	// CA). Si es nil, se genera un certificado self-signed de desarrollo
	// automáticamente (ver GenerateDevCertificate) — NO usar eso en
	// producción.
	TLSConfig *tls.Config
}

// DevCertificate es el resultado de generar un certificado de desarrollo:
// el TLS config listo para usar, y el hash que el cliente browser necesita
// pasarle a `new WebTransport(url, { serverCertificateHashes: [...] })`.
type DevCertificate struct {
	TLSConfig  *tls.Config
	HashBase64 string // sha-256 del cert DER, base64 sin padding
}

// GenerateDevCertificate crea un certificado self-signed ECDSA P-256,
// válido ~13 días (el máximo que acepta serverCertificateHashes del
// navegador son 14). Clave nueva en cada llamada — no determinístico a
// propósito: para pruebas automatizadas conviene leer el hash del
// resultado en vez de hardcodear uno, y para desarrollo manual alcanza con
// volver a copiar el hash si el proceso se reinicia.
//
// NO usar esto en producción — ahí hace falta un certificado real de una
// CA (Let's Encrypt, etc.), el navegador no acepta serverCertificateHashes
// para eso.
func GenerateDevCertificate() (*DevCertificate, error) {
	certKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generando clave: %w", err)
	}

	now := time.Now().UTC()
	validFrom := now.Add(-time.Hour) // margen para relojes desincronizados
	validUntil := validFrom.Add(13 * 24 * time.Hour)

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return nil, fmt.Errorf("generando serial: %w", err)
	}

	template := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             validFrom,
		NotAfter:              validUntil,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.ParseIP("::1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &certKey.PublicKey, certKey)
	if err != nil {
		return nil, fmt.Errorf("creando certificado: %w", err)
	}

	hash := sha256.Sum256(certDER)

	return &DevCertificate{
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{{
				Certificate: [][]byte{certDER},
				PrivateKey:  certKey,
			}},
		},
		HashBase64: base64.RawStdEncoding.EncodeToString(hash[:]),
	}, nil
}

// StartWebTransport arranca un listener WebTransport (QUIC/HTTP-3) además
// de (o en vez de) StartUDP — se pueden usar los dos a la vez en el mismo
// Server: un cliente Unity (UDP) y un cliente de navegador (WebTransport)
// pueden terminar jugando la misma partida, porque ambos transportes
// alimentan el mismo Server.HandlePacket.
//
// Si opts.TLSConfig es nil, genera y devuelve un certificado de desarrollo
// (ver GenerateDevCertificate) — el llamador necesita su HashBase64 para
// configurar el cliente browser.
func (s *Server) StartWebTransport(opts WebTransportOptions) (*DevCertificate, error) {
	path := opts.Path
	if path == "" {
		path = "/webtransport"
	}

	var devCert *DevCertificate
	tlsConf := opts.TLSConfig
	if tlsConf == nil {
		var err error
		devCert, err = GenerateDevCertificate()
		if err != nil {
			return nil, err
		}
		tlsConf = devCert.TLSConfig
	}

	h3Server := &http3.Server{
		Addr:      opts.Addr,
		TLSConfig: http3.ConfigureTLSConfig(tlsConf),
		QUICConfig: &quic.Config{
			EnableDatagrams: true,
		},
	}

	wtServer := &webtransport.Server{
		H3: h3Server,
		// Dev: aceptar cualquier origen. En producción esto debería
		// restringirse al dominio real donde vive la página.
		CheckOrigin: func(*http.Request) bool { return true },
	}
	webtransport.ConfigureHTTP3Server(h3Server)

	mux := http.NewServeMux()
	h3Server.Handler = mux
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		sess, err := wtServer.Upgrade(w, r)
		if err != nil {
			log.Printf("webtransport: upgrade falló: %v", err)
			w.WriteHeader(500)
			return
		}
		go s.webtransportSessionLoop(sess)
	})

	s.registerCloser(func() { wtServer.Close() })

	go func() {
		if err := wtServer.ListenAndServe(); err != nil {
			log.Printf("webtransport: servidor detenido: %v", err)
		}
	}()

	log.Printf("🌐 Server (WebTransport) escuchando en %s%s", opts.Addr, path)
	return devCert, nil
}

func (s *Server) webtransportSessionLoop(sess *webtransport.Session) {
	peer := &wtPeer{sess: sess, key: fmt.Sprintf("wt:%p", sess)}
	ctx := sess.Context()

	for {
		data, err := sess.ReceiveDatagram(ctx)
		if err != nil {
			return // sesión cerrada por el cliente o por timeout
		}
		s.HandlePacket(data, peer)
	}
}
