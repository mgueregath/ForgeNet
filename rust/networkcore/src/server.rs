//! `Server` multiplexa muchas salas (partidas) concurrentes sobre el mismo
//! transporte UDP compartido — genérico, no sabe qué juego corre en cada
//! una. Un cliente que manda `PACKET_HANDSHAKE` con `HANDSHAKE_MODE_CREATE`
//! arranca una sala nueva y recibe un código; con `HANDSHAKE_MODE_JOIN` se
//! une a la sala de ese código. Todo lo demás (input, ack, ping) se rutea a
//! la sala del cliente que lo mandó.
//!
//! Antes, `NetworkHost` ligaba su propio socket y admitía a cualquier
//! cliente sin noción de sala — eso ahora vive acá, para poder correr un
//! solo proceso siempre encendido que hospeda muchas partidas a la vez, en
//! vez de un proceso por partida LAN.

use crate::host::{NetworkHost, NetworkHostHandle};
use crate::protocol::*;
use dashmap::DashMap;
use rand::Rng;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;

/// Crea un `NetworkHost` nuevo, con sus propios hooks, cada vez que se crea
/// una sala — cada juego decide qué enganchar (ver ejemplo-tacataca), el
/// `Server` no sabe nada de eso.
pub type RoomFactory = Box<dyn Fn() -> NetworkHost + Send + Sync>;

/// Configura el `Server`. Todo acá es genérico — no hay nada específico de
/// ningún juego.
pub struct ServerOptions {
    /// Longitud de los códigos de sala generados. Default: 6.
    pub room_code_length: usize,
    /// Tope de clientes admitidos por sala (0 = sin tope). El `Server` solo
    /// cuenta clientes — no sabe qué rol tiene cada uno, así que un límite
    /// que dependa del rol (ej. "2 barras + 1 tablero" en taca-taca) es
    /// responsabilidad del juego, no de acá.
    pub max_players_per_room: usize,
    /// Máximo de paquetes de handshake aceptados desde un mismo peer (misma
    /// IP:puerto UDP) por `handshake_rate_limit_window` — el resto se
    /// descarta en silencio. Default: 10 intentos / 10s. Esto NO protege
    /// contra un atacante que rota de origen en cada intento — sí frena un
    /// cliente roto reintentando en loop o un flood simple desde una única
    /// conexión.
    pub handshake_rate_limit: usize,
    pub handshake_rate_limit_window: Duration,
    /// Cuánto se espera, desde que una sala que ya tuvo jugadores queda en 0
    /// conectados, antes de destruirla — no instantáneo, para darle tiempo a
    /// `admit_player` de reconocer una reconexión (ver el comentario grande
    /// ahí) antes de que la sala misma deje de existir. Default: 30s.
    pub empty_room_grace_period: Duration,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            room_code_length: 6,
            max_players_per_room: 0,
            handshake_rate_limit: 10,
            handshake_rate_limit_window: Duration::from_secs(10),
            empty_room_grace_period: Duration::from_secs(30),
        }
    }
}

impl ServerOptions {
    fn normalized(mut self) -> Self {
        if self.room_code_length == 0 {
            self.room_code_length = 6;
        }
        if self.handshake_rate_limit == 0 {
            self.handshake_rate_limit = 10;
        }
        if self.handshake_rate_limit_window.is_zero() {
            self.handshake_rate_limit_window = Duration::from_secs(10);
        }
        if self.empty_room_grace_period.is_zero() {
            self.empty_room_grace_period = Duration::from_secs(30);
        }
        self
    }
}

struct Room {
    code: String,
    host: NetworkHostHandle,
    /// Una sala recién creada que todavía no tuvo ningún cliente no se
    /// destruye aunque tenga cero conectados (ver `sweep_empty_rooms`). Una
    /// sala creada directamente vía `Server::create_room` (modo "host
    /// embebido") arranca en `false` igual que cualquier otra — no cuenta
    /// como "tuvo jugador" solo por haber sido creada.
    had_player: AtomicBool,
    /// Desde cuándo `connected_player_count()` está en 0 — `None` mientras
    /// la sala tiene al menos un cliente conectado. Ver `sweep_empty_rooms`.
    empty_since: std::sync::Mutex<Option<Instant>>,
}

/// Recuerda a qué sala pertenece cada peer y guarda el último ack de
/// handshake mandado, para poder responder handshakes reintentados (ack
/// perdido) sin volver a admitir al cliente ni crear una sala nueva.
struct PeerInfo {
    room_code: String,
    ack: Vec<u8>,
}

struct HandshakeWindow {
    start: Instant,
    count: usize,
}

pub struct Server {
    opts: ServerOptions,
    room_factory: RoomFactory,

    rooms: DashMap<String, Arc<Room>>,
    peers: DashMap<SocketAddr, PeerInfo>,
    handshake_attempts: DashMap<SocketAddr, HandshakeWindow>,

    socket: OnceLock<Arc<UdpSocket>>,
    running: AtomicBool,
}

/// roomCodeAlphabet excluye 0/O y 1/I — ambiguos al leerlos a mano si el QR
/// falla y alguien tiene que tipear el código.
const ROOM_CODE_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

impl Server {
    /// Crea un `Server` sin arrancar ningún transporte todavía — llamar
    /// `start_udp` después.
    pub fn new(factory: RoomFactory, opts: ServerOptions) -> Arc<Server> {
        let server = Arc::new(Server {
            opts: opts.normalized(),
            room_factory: factory,
            rooms: DashMap::new(),
            peers: DashMap::new(),
            handshake_attempts: DashMap::new(),
            socket: OnceLock::new(),
            running: AtomicBool::new(true),
        });

        tokio::spawn(janitor_loop(server.clone()));
        server
    }

    /// Liga un socket UDP y arranca su loop de recepción.
    ///
    /// `port=0` le pide al SO cualquier puerto disponible — útil para un
    /// deployment que no quiere depender de que un puerto fijo esté libre.
    /// El puerto real que asignó el SO queda disponible en `udp_port()`.
    pub async fn start_udp(self: &Arc<Self>, port: u16) -> std::io::Result<()> {
        let socket = Arc::new(UdpSocket::bind(("0.0.0.0", port)).await?);
        let real_port = socket.local_addr()?.port();
        // Solo puede haber un socket UDP por Server — set() falla en
        // silencio si ya se llamó start_udp antes (no debería pasar).
        let _ = self.socket.set(socket.clone());

        println!("🎮 Server (UDP) escuchando en :{} (60Hz)", real_port);
        tokio::spawn(udp_receive_loop(self.clone(), socket));
        Ok(())
    }

    /// Devuelve el puerto UDP real en el que quedó escuchando el server — el
    /// mismo que se pidió en `start_udp`, salvo que se haya pedido 0
    /// (cualquiera disponible), en cuyo caso es el que el SO asignó de
    /// verdad. 0 si `start_udp` nunca se llamó (o falló).
    pub fn udp_port(&self) -> u16 {
        self.socket
            .get()
            .and_then(|s| s.local_addr().ok())
            .map(|a| a.port())
            .unwrap_or(0)
    }

    /// Cierra el server: detiene el loop de janitor, todas las salas
    /// activas, y limpia el estado de peers/salas.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        for r in self.rooms.iter() {
            r.host.stop();
        }
        self.rooms.clear();
        self.peers.clear();
    }

    /// Punto de entrada común para cualquier transporte — igual que antes
    /// lo era `NetworkHost::handle_packet`, pero a nivel `Server`, porque el
    /// `Server` es quien sabe a qué sala pertenece cada paquete.
    pub async fn handle_packet(self: &Arc<Self>, data: &[u8], addr: SocketAddr) {
        let Some((seq, _ack, packet_type, _flags)) = parse_header(data) else {
            return;
        };

        if packet_type == PACKET_HANDSHAKE {
            self.handle_handshake(seq, addr, &data[HEADER_SIZE..]).await;
            return;
        }

        let room = self
            .peers
            .get(&addr)
            .and_then(|info| self.rooms.get(&info.room_code).map(|r| r.clone()));

        if let Some(room) = room {
            room.host.handle_packet(data, addr).await;
        }
    }

    async fn handle_handshake(self: &Arc<Self>, seq: u32, addr: SocketAddr, payload: &[u8]) {
        if !self.allow_handshake_attempt(addr) {
            return;
        }

        // Reintento de un handshake ya admitido (el ack anterior se
        // perdió): no crear una sala nueva ni volver a admitir, solo
        // reenviar el ack.
        if let Some(info) = self.peers.get(&addr) {
            let ack = info.ack.clone();
            drop(info);
            self.send(ack, addr).await;
            return;
        }

        let Some((mode, role, room_code)) = decode_join_payload(payload) else {
            return;
        };

        let room = match mode {
            HANDSHAKE_MODE_CREATE => self.new_room(),
            HANDSHAKE_MODE_JOIN => match self.rooms.get(&room_code).map(|r| r.clone()) {
                Some(r) => r,
                None => {
                    let mut pkt = build_header(seq, 0, PACKET_DISCONNECT, 0);
                    pkt.push(REASON_ROOM_NOT_FOUND);
                    self.send(pkt, addr).await;
                    return;
                }
            },
            _ => return,
        };

        if self.opts.max_players_per_room > 0
            && room.host.connected_player_count() >= self.opts.max_players_per_room
        {
            let mut pkt = build_header(seq, 0, PACKET_DISCONNECT, 0);
            pkt.push(REASON_ROOM_FULL);
            self.send(pkt, addr).await;
            return;
        }

        let (player_id, _reconnected) = room.host.admit_player(addr, role);
        room.had_player.store(true, Ordering::SeqCst);

        let mut ack = build_header(seq, 0, PACKET_HANDSHAKE, 0);
        ack.extend_from_slice(&encode_handshake_ack(player_id, &room.code));
        self.send(ack.clone(), addr).await;

        self.peers.insert(
            addr,
            PeerInfo {
                room_code: room.code.clone(),
                ack,
            },
        );
    }

    /// Aplica el rate limit de `handshake_rate_limit` por peer — ventana
    /// fija, no token bucket: simple y suficiente para lo que protege (ver
    /// `ServerOptions::handshake_rate_limit`).
    fn allow_handshake_attempt(&self, addr: SocketAddr) -> bool {
        let now = Instant::now();
        let mut window = self
            .handshake_attempts
            .entry(addr)
            .or_insert_with(|| HandshakeWindow { start: now, count: 0 });

        if now.duration_since(window.start) > self.opts.handshake_rate_limit_window {
            window.start = now;
            window.count = 1;
            return true;
        }
        if window.count >= self.opts.handshake_rate_limit {
            return false;
        }
        window.count += 1;
        true
    }

    fn sweep_handshake_attempts(&self) {
        let now = Instant::now();
        self.handshake_attempts
            .retain(|_, w| now.duration_since(w.start) <= self.opts.handshake_rate_limit_window);
    }

    /// Arranca una sala directamente, sin pasar por un handshake de red — a
    /// diferencia de una sala creada porque un cliente mandó
    /// `HANDSHAKE_MODE_CREATE`, acá no hay ningún cliente admitido todavía.
    ///
    /// Pensado para el modo "host embebido" (ej. un tablero en LAN, que es
    /// el mismo proceso que corre el `Server`): la app llama a esto una vez
    /// al arrancar y ya tiene una sala fija a la que los mandos se unen (vía
    /// handshake con `HANDSHAKE_MODE_JOIN`), sin que el propio tablero tenga
    /// que hacerse pasar por cliente de sí mismo para "crear" la sala. La
    /// sala recién creada NO cuenta como "vacía" para el janitor (ver
    /// `sweep_empty_rooms`/`had_player`) hasta que admite a su primer
    /// cliente de verdad, así que puede esperar indefinidamente a que
    /// alguien se una sin que `empty_room_grace_period` la destruya.
    pub fn create_room(self: &Arc<Self>) -> String {
        self.new_room().code.clone()
    }

    fn new_room(self: &Arc<Self>) -> Arc<Room> {
        let code = self.generate_unique_room_code();
        let host = (self.room_factory)();
        let socket = self
            .socket
            .get()
            .expect("start_udp debe llamarse antes de crear salas")
            .clone();
        let handle = host.start(socket);

        let room = Arc::new(Room {
            code: code.clone(),
            host: handle,
            had_player: AtomicBool::new(false),
            empty_since: std::sync::Mutex::new(None),
        });
        self.rooms.insert(code, room.clone());
        room
    }

    fn generate_unique_room_code(&self) -> String {
        loop {
            let code = random_room_code(self.opts.room_code_length);
            if !self.rooms.contains_key(&code) {
                return code;
            }
        }
    }

    async fn send(&self, data: Vec<u8>, addr: SocketAddr) {
        if let Some(socket) = self.socket.get() {
            let _ = socket.send_to(&data, addr).await;
        }
    }

    /// Destruye salas que se quedaron sin ningún cliente conectado durante
    /// más de `empty_room_grace_period` — genérico, no sabe qué juego corre
    /// en ellas. Una sala recién creada que todavía no tuvo ningún cliente
    /// no se toca (`had_player`).
    fn sweep_empty_rooms(&self) {
        let now = Instant::now();
        let to_remove: Vec<String> = self
            .rooms
            .iter()
            .filter_map(|r| {
                if !r.had_player.load(Ordering::SeqCst) {
                    return None;
                }
                if r.host.connected_player_count() > 0 {
                    *r.empty_since.lock().unwrap() = None;
                    return None;
                }
                let mut empty_since = r.empty_since.lock().unwrap();
                let since = *empty_since.get_or_insert(now);
                if now.duration_since(since) < self.opts.empty_room_grace_period {
                    return None;
                }
                Some(r.code.clone())
            })
            .collect();

        for code in to_remove {
            if let Some((_, room)) = self.rooms.remove(&code) {
                room.host.stop();
            }
            self.peers.retain(|_, info| info.room_code != code);
        }
    }
}

fn random_room_code(length: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..length)
        .map(|_| ROOM_CODE_ALPHABET[rng.gen_range(0..ROOM_CODE_ALPHABET.len())] as char)
        .collect()
}

// Procesa los paquetes uno por uno, en el orden en que llegan — igual que
// Go (que llama HandlePacket directo en su loop de recepción, sin
// goroutine por paquete). Despachar cada paquete a una tarea aparte
// rompería el orden de llegada de, por ejemplo, dos inputs consecutivos del
// mismo cliente.
async fn udp_receive_loop(server: Arc<Server>, socket: Arc<UdpSocket>) {
    let mut buf = vec![0u8; 2048];
    loop {
        if !server.running.load(Ordering::SeqCst) {
            return;
        }
        let (n, addr) = match socket.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(_) => continue,
        };

        server.handle_packet(&buf[..n], addr).await;
    }
}

async fn janitor_loop(server: Arc<Server>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    loop {
        ticker.tick().await;
        if !server.running.load(Ordering::SeqCst) {
            return;
        }
        server.sweep_empty_rooms();
        server.sweep_handshake_attempts();
    }
}
