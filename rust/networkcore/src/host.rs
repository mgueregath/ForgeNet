//! Rol "servidor" del protocolo para UNA sala/partida. No sabe nada del
//! juego que corre encima — se engancha vía los campos de callback de
//! `NetworkHost`, igual que `NetworkHost` en los cores de Go y C#. Tampoco
//! sabe de qué transporte vienen los paquetes ni a qué sala pertenece: eso
//! lo decide `Server`, que crea una instancia de `NetworkHost` por sala,
//! le da el socket compartido y le rutea sus paquetes (ver server.rs).

use crate::protocol::*;
use crate::types::*;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

const TICK_RATE: Duration = Duration::from_millis(16); // 60Hz
const RETRY_INTERVAL: Duration = Duration::from_millis(100);
const HEARTBEAT_EVERY: Duration = Duration::from_secs(1);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RETRIES: u8 = 3;

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

struct ReliableMessage {
    seq: u32,
    data: Vec<u8>,
    sent: Instant,
    retries: u8,
}

/// Estado de sesión de un cliente. No tiene ningún campo de estado de juego
/// — eso vive en el juego, no acá. `role` es opaco: el core lo guarda y lo
/// expone (ver `NetworkHostHandle::get_client_role`) pero nunca lo
/// interpreta — cada juego define sus propios valores y qué significan.
struct ClientConnection {
    player_id: u16,
    addr: SocketAddr,
    role: u8,
    last_heartbeat: Instant,
    #[allow(dead_code)]
    ping: i32,
    connected: bool,
    reliable_queue: Vec<ReliableMessage>,
}

type ConnHook = Box<dyn Fn(u16) + Send + Sync + 'static>;
/// El core no interpreta `role`: solo lo pasa tal cual llegó del handshake.
type RoleConnHook = Box<dyn Fn(u16, u8) + Send + Sync + 'static>;
type InputHook = Box<dyn Fn(PlayerInput) + Send + Sync + 'static>;
type TickHook = Box<dyn Fn(u64) + Send + Sync + 'static>;
type StateHook = Box<dyn Fn() -> Vec<u8> + Send + Sync + 'static>;

/// Cola de eventos compartible: se obtiene con `NetworkHost::events()`
/// *antes* de llamar a `start()`, para poder clonarla y moverla dentro de
/// closures como `on_input` (que se configuran antes de arrancar, pero
/// pueden necesitar encolar un evento — ej. GOAL — en el momento). Sigue
/// siendo válida después de `start()`.
#[derive(Clone)]
pub struct EventQueue(Arc<std::sync::Mutex<Vec<GameEvent>>>);

impl EventQueue {
    fn new() -> Self {
        Self(Arc::new(std::sync::Mutex::new(Vec::new())))
    }

    pub fn push(&self, evt: GameEvent) {
        self.0.lock().unwrap().push(evt);
    }

    fn take(&self) -> Vec<GameEvent> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

struct Inner {
    socket: Arc<UdpSocket>,
    clients: DashMap<u16, ClientConnection>,
    clients_by_addr: DashMap<SocketAddr, u16>,
    next_player_id: AtomicU32,
    sequence_counter: AtomicU32,
    tick: AtomicU64,
    pending_events: EventQueue,
    input_tx: mpsc::UnboundedSender<PlayerInput>,
    running: AtomicBool,

    on_player_connected: Option<RoleConnHook>,
    on_player_disconnected: Option<ConnHook>,
    on_input: Option<InputHook>,
    on_tick: Option<TickHook>,
    state_provider: Option<StateHook>,
}

/// Rol "servidor" del protocolo para UNA sala/partida. Se configuran los
/// hooks (`on_input`, etc.) y después se llama a `start(socket)` — el socket
/// lo posee y comparte `Server`, que es quien decide a qué sala pertenece
/// cada paquete (ver server.rs) — que consume el host y devuelve un
/// `NetworkHostHandle` para poder llamar `stop`/`admit_player`/etc. mientras
/// los loops corren en background. Para encolar eventos desde un hook, usar
/// `events()` (ver `EventQueue`).
pub struct NetworkHost {
    pub on_player_connected: Option<RoleConnHook>,
    pub on_player_disconnected: Option<ConnHook>,
    pub on_input: Option<InputHook>,
    pub on_tick: Option<TickHook>,
    pub state_provider: Option<StateHook>,
    events: EventQueue,
}

impl Default for NetworkHost {
    fn default() -> Self {
        Self {
            on_player_connected: None,
            on_player_disconnected: None,
            on_input: None,
            on_tick: None,
            state_provider: None,
            events: EventQueue::new(),
        }
    }
}

#[derive(Clone)]
pub struct NetworkHostHandle {
    inner: Arc<Inner>,
}

impl NetworkHost {
    pub fn new() -> Self {
        Self::default()
    }

    /// Cola de eventos, disponible desde antes de `start()` — clonarla y
    /// moverla dentro de un hook (`on_input`, etc.) para poder encolar
    /// eventos del juego (ej. GOAL) desde ahí.
    pub fn events(&self) -> EventQueue {
        self.events.clone()
    }

    /// Arranca los loops de fondo de esta sala (tick, retransmisión,
    /// heartbeat) usando el socket compartido que le pasa `Server`. No liga
    /// ningún socket propio ni recibe paquetes directamente — eso es
    /// responsabilidad de `Server` (ver `Server::handle_packet`, que rutea
    /// los paquetes de esta sala a `NetworkHostHandle::handle_packet`).
    pub fn start(self, socket: Arc<UdpSocket>) -> NetworkHostHandle {
        let (input_tx, input_rx) = mpsc::unbounded_channel();

        let inner = Arc::new(Inner {
            socket,
            clients: DashMap::new(),
            clients_by_addr: DashMap::new(),
            next_player_id: AtomicU32::new(1),
            sequence_counter: AtomicU32::new(0),
            tick: AtomicU64::new(0),
            pending_events: self.events,
            input_tx,
            running: AtomicBool::new(true),
            on_player_connected: self.on_player_connected,
            on_player_disconnected: self.on_player_disconnected,
            on_input: self.on_input,
            on_tick: self.on_tick,
            state_provider: self.state_provider,
        });

        tokio::spawn(game_loop(inner.clone(), input_rx));
        tokio::spawn(send_loop(inner.clone()));
        tokio::spawn(heartbeat_loop(inner.clone()));

        NetworkHostHandle { inner }
    }
}

impl NetworkHostHandle {
    pub fn stop(&self) {
        self.inner.running.store(false, Ordering::SeqCst);
    }

    /// Equivalente a `NetworkHost::events()`, disponible también después de
    /// `start()` por si hace falta encolar eventos desde fuera de un hook
    /// (ej. un timer).
    pub fn events(&self) -> EventQueue {
        self.inner.pending_events.clone()
    }

    pub fn connected_player_count(&self) -> usize {
        self.inner.clients.len()
    }

    /// Registra un cliente nuevo en esta sala y dispara
    /// `on_player_connected(player_id, role)`. La llama `Server`, después de
    /// decidir (crear/unirse) a qué sala pertenece el cliente. `role` es
    /// opaco para el core — cada juego define sus propios valores y qué
    /// hacer con ellos (ej. no crear una "barra" de juego para el rol que
    /// representa al tablero). Idempotente: un cliente que ya está admitido
    /// devuelve su player_id actual sin duplicar estado (cubre el reintento
    /// de un handshake cuyo ack se perdió).
    pub fn admit_player(&self, addr: SocketAddr, role: u8) -> u16 {
        if let Some(existing) = self.inner.clients_by_addr.get(&addr) {
            return *existing;
        }

        let player_id = self.inner.next_player_id.fetch_add(1, Ordering::Relaxed) as u16;
        self.inner.clients_by_addr.insert(addr, player_id);
        self.inner.clients.insert(
            player_id,
            ClientConnection {
                player_id,
                addr,
                role,
                last_heartbeat: Instant::now(),
                ping: 0,
                connected: true,
                reliable_queue: Vec::new(),
            },
        );

        if let Some(cb) = &self.inner.on_player_connected {
            cb(player_id, role);
        }
        player_id
    }

    /// Rol opaco con el que se admitió a un jugador (ver `admit_player`).
    /// `None` si el jugador no existe (ya desconectado, o id inválido).
    pub fn get_client_role(&self, player_id: u16) -> Option<u8> {
        self.inner.clients.get(&player_id).map(|c| c.role)
    }

    /// Procesa un paquete crudo recibido de un cliente que YA pertenece a
    /// esta sala. El handshake no pasa por acá: es `Server` quien lo
    /// intercepta primero para decidir a qué sala corresponde y recién ahí
    /// llama a `admit_player` (ver server.rs).
    pub async fn handle_packet(&self, data: &[u8], addr: SocketAddr) {
        dispatch_packet(&self.inner, data, addr).await;
    }

    /// Hace lo mismo que `events().push(evt)` (el evento sale en el próximo
    /// snapshot de todos modos) pero además lo manda ya mismo, a cada
    /// cliente conectado, como un paquete aparte marcado `FLAG_RELIABLE` —
    /// el core lo reintenta (mismo mecanismo que ya existía para reliable
    /// input, ver `send_loop`) hasta que el cliente lo confirma con
    /// `PACKET_ACK` o se agotan los reintentos. Pensado para eventos que el
    /// juego no puede permitirse perder en una red con pérdida real (ej.
    /// GOAL en un server público).
    ///
    /// La entrega es "al menos una vez", no "exactamente una vez": el
    /// cliente puede recibir el mismo evento acá y de nuevo en el snapshot
    /// normal — el juego debe tratarlo como una notificación idempotente
    /// (ej. "hubo un gol, resincronizá con state_payload"), no como algo que
    /// suma un contador él mismo.
    pub fn queue_reliable_event(&self, evt: GameEvent) {
        self.inner.pending_events.push(evt.clone());

        let tick = self.inner.tick.load(Ordering::SeqCst);
        let snap = GameSnapshot {
            tick,
            timestamp: now_unix_millis(),
            state_payload: Vec::new(),
            events: vec![evt],
        };
        let payload = snap.encode();

        let addrs: Vec<(u16, SocketAddr)> =
            self.inner.clients.iter().map(|c| (c.player_id, c.addr)).collect();

        for (player_id, addr) in addrs {
            let seq = self.inner.sequence_counter.fetch_add(1, Ordering::Relaxed);
            let mut packet = build_header(seq, 0, PACKET_SNAPSHOT, FLAG_RELIABLE);
            packet.extend_from_slice(&payload);

            if let Some(mut client) = self.inner.clients.get_mut(&player_id) {
                client.reliable_queue.push(ReliableMessage {
                    seq,
                    data: packet.clone(),
                    sent: Instant::now(),
                    retries: 0,
                });
            }

            let socket = self.inner.socket.clone();
            tokio::spawn(async move {
                let _ = socket.send_to(&packet, addr).await;
            });
        }
    }

    /// Cuántos paquetes reliable (ver `queue_reliable_event`) todavía no
    /// fueron confirmados por este cliente. Diagnóstico genérico — útil para
    /// cualquier juego que quiera saber si un cliente se está quedando
    /// atrás, no es específico de ningún evento en particular.
    pub fn pending_reliable_count(&self, player_id: u16) -> usize {
        self.inner
            .clients
            .get(&player_id)
            .map(|c| c.reliable_queue.len())
            .unwrap_or(0)
    }
}

async fn dispatch_packet(inner: &Arc<Inner>, data: &[u8], addr: SocketAddr) {
    let Some((seq, _ack, packet_type, flags)) = parse_header(data) else {
        return;
    };
    let payload = &data[HEADER_SIZE..];

    match packet_type {
        PACKET_INPUT => handle_input(inner, seq, addr, flags, payload).await,
        PACKET_ACK => handle_ack(inner, seq, addr).await,
        PACKET_PING => handle_ping(inner, seq, addr, payload).await,
        _ => {}
    }
}

async fn handle_input(inner: &Arc<Inner>, seq: u32, addr: SocketAddr, flags: u8, payload: &[u8]) {
    let Some(player_id) = inner.clients_by_addr.get(&addr).map(|r| *r) else {
        return;
    };
    let Some((delta_x, delta_y, rotation, actions)) = decode_input_payload(payload) else {
        return;
    };

    if let Some(mut client) = inner.clients.get_mut(&player_id) {
        client.last_heartbeat = Instant::now();
    }

    if flags & FLAG_RELIABLE != 0 {
        let ack = build_header(seq, seq, PACKET_ACK, 0);
        let _ = inner.socket.send_to(&ack, addr).await;
    }

    let input = PlayerInput {
        player_id,
        seq,
        timestamp: now_unix_millis(),
        delta_x,
        delta_y,
        rotation,
        actions,
    };
    let _ = inner.input_tx.send(input);
}

async fn handle_ack(inner: &Arc<Inner>, seq: u32, addr: SocketAddr) {
    let Some(player_id) = inner.clients_by_addr.get(&addr).map(|r| *r) else {
        return;
    };
    if let Some(mut client) = inner.clients.get_mut(&player_id) {
        client.last_heartbeat = Instant::now();
        client.reliable_queue.retain(|m| m.seq > seq);
    }
}

async fn handle_ping(inner: &Arc<Inner>, seq: u32, addr: SocketAddr, payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let Some(player_id) = inner.clients_by_addr.get(&addr).map(|r| *r) else {
        return;
    };

    let client_time = i64::from_be_bytes(payload[0..8].try_into().unwrap());
    let server_time = now_unix_millis();

    if let Some(mut client) = inner.clients.get_mut(&player_id) {
        client.ping = ((server_time - client_time) / 2) as i32;
        client.last_heartbeat = Instant::now();
    }

    let mut response = build_header(seq, 0, PACKET_PING, 0);
    response.extend_from_slice(&server_time.to_be_bytes());
    let _ = inner.socket.send_to(&response, addr).await;
}

async fn game_loop(inner: Arc<Inner>, mut input_rx: mpsc::UnboundedReceiver<PlayerInput>) {
    let mut ticker = tokio::time::interval(TICK_RATE);
    while inner.running.load(Ordering::SeqCst) {
        ticker.tick().await;
        if !inner.running.load(Ordering::SeqCst) {
            break;
        }

        // El core solo reenvía los inputs al juego, en orden — no
        // interpreta delta_x/delta_y/rotation/actions de ninguna forma.
        while let Ok(input) = input_rx.try_recv() {
            if let Some(cb) = &inner.on_input {
                cb(input);
            }
        }

        let tick = inner.tick.load(Ordering::SeqCst);
        if let Some(cb) = &inner.on_tick {
            cb(tick);
        }

        broadcast_snapshot(&inner, tick).await;
        inner.tick.store(tick + 1, Ordering::SeqCst);
    }
}

async fn broadcast_snapshot(inner: &Arc<Inner>, tick: u64) {
    let state_payload = match &inner.state_provider {
        Some(provider) => provider(),
        None => Vec::new(),
    };

    let events = inner.pending_events.take();

    let snapshot = GameSnapshot {
        tick,
        timestamp: now_unix_millis(),
        state_payload,
        events,
    };
    let payload = snapshot.encode();
    let seq = inner.sequence_counter.fetch_add(1, Ordering::Relaxed);

    let mut packet = build_header(seq, 0, PACKET_SNAPSHOT, 0);
    packet.extend_from_slice(&payload);

    for client in inner.clients.iter() {
        if client.connected {
            let _ = inner.socket.send_to(&packet, client.addr).await;
        }
    }
}

async fn send_loop(inner: Arc<Inner>) {
    let mut ticker = tokio::time::interval(Duration::from_millis(50));
    while inner.running.load(Ordering::SeqCst) {
        ticker.tick().await;
        if !inner.running.load(Ordering::SeqCst) {
            break;
        }

        let now = Instant::now();
        let addrs_and_msgs: Vec<(SocketAddr, Vec<u8>)> = {
            let mut to_resend = Vec::new();
            for mut client in inner.clients.iter_mut() {
                let addr = client.addr;
                for msg in client.reliable_queue.iter_mut() {
                    if now.duration_since(msg.sent) > RETRY_INTERVAL && msg.retries < MAX_RETRIES {
                        to_resend.push((addr, msg.data.clone()));
                        msg.sent = now;
                        msg.retries += 1;
                    }
                }
            }
            to_resend
        };

        for (addr, data) in addrs_and_msgs {
            let _ = inner.socket.send_to(&data, addr).await;
        }
    }
}

async fn heartbeat_loop(inner: Arc<Inner>) {
    let mut ticker = tokio::time::interval(HEARTBEAT_EVERY);
    while inner.running.load(Ordering::SeqCst) {
        ticker.tick().await;
        if !inner.running.load(Ordering::SeqCst) {
            break;
        }

        let now = Instant::now();
        let mut to_remove = Vec::new();
        for client in inner.clients.iter() {
            if now.duration_since(client.last_heartbeat) > HEARTBEAT_TIMEOUT {
                to_remove.push((client.player_id, client.addr));
            }
        }

        for (id, addr) in &to_remove {
            inner.clients.remove(id);
            inner.clients_by_addr.remove(addr);
        }

        for (id, _) in &to_remove {
            if let Some(cb) = &inner.on_player_disconnected {
                cb(*id);
            }
        }
    }
}
