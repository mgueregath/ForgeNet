//! Rol "servidor" del protocolo. No sabe nada del juego que corre encima —
//! se engancha vía los campos de callback de `NetworkHost`, igual que
//! `NetworkHost` en los cores de Go y C#.

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

struct ClientConnection {
    player_id: u16,
    addr: SocketAddr,
    last_heartbeat: Instant,
    #[allow(dead_code)]
    ping: i32,
    connected: bool,
    reliable_queue: Vec<ReliableMessage>,
}

type ConnHook = Box<dyn Fn(u16) + Send + Sync + 'static>;
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
    socket: UdpSocket,
    clients: DashMap<u16, ClientConnection>,
    clients_by_addr: DashMap<SocketAddr, u16>,
    next_player_id: AtomicU32,
    sequence_counter: AtomicU32,
    tick: AtomicU64,
    pending_events: EventQueue,
    input_tx: mpsc::UnboundedSender<PlayerInput>,
    running: AtomicBool,

    on_player_connected: Option<ConnHook>,
    on_player_disconnected: Option<ConnHook>,
    on_input: Option<InputHook>,
    on_tick: Option<TickHook>,
    state_provider: Option<StateHook>,
}

/// Rol "servidor". Se configuran los hooks (`on_input`, etc.) y después se
/// llama a `start()`, que consume el host y devuelve un `NetworkHostHandle`
/// para poder llamar `stop`/`connected_player_count` mientras los loops
/// corren en background. Para encolar eventos desde un hook, usar
/// `events()` (ver `EventQueue`).
pub struct NetworkHost {
    pub on_player_connected: Option<ConnHook>,
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

    pub async fn start(self, port: u16) -> std::io::Result<NetworkHostHandle> {
        let socket = UdpSocket::bind(("0.0.0.0", port)).await?;
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

        println!("🎮 NetworkHost escuchando en :{} (60Hz)", port);

        tokio::spawn(receive_loop(inner.clone()));
        tokio::spawn(game_loop(inner.clone(), input_rx));
        tokio::spawn(send_loop(inner.clone()));
        tokio::spawn(heartbeat_loop(inner.clone()));

        Ok(NetworkHostHandle { inner })
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
}

async fn receive_loop(inner: Arc<Inner>) {
    let mut buf = vec![0u8; 2048];
    while inner.running.load(Ordering::SeqCst) {
        let (n, addr) = match inner.socket.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        handle_packet(&inner, &buf[..n], addr).await;
    }
}

async fn handle_packet(inner: &Arc<Inner>, data: &[u8], addr: SocketAddr) {
    let Some((seq, _ack, packet_type, flags)) = parse_header(data) else {
        return;
    };
    let payload = &data[HEADER_SIZE..];

    match packet_type {
        PACKET_HANDSHAKE => handle_handshake(inner, seq, addr).await,
        PACKET_INPUT => handle_input(inner, seq, addr, flags, payload).await,
        PACKET_ACK => handle_ack(inner, seq, addr).await,
        PACKET_PING => handle_ping(inner, seq, addr, payload).await,
        _ => {}
    }
}

async fn handle_handshake(inner: &Arc<Inner>, seq: u32, addr: SocketAddr) {
    if inner.clients_by_addr.contains_key(&addr) {
        return;
    }

    let player_id = inner.next_player_id.fetch_add(1, Ordering::Relaxed) as u16;
    inner.clients_by_addr.insert(addr, player_id);
    inner.clients.insert(
        player_id,
        ClientConnection {
            player_id,
            addr,
            last_heartbeat: Instant::now(),
            ping: 0,
            connected: true,
            reliable_queue: Vec::new(),
        },
    );

    let mut response = build_header(seq, 0, PACKET_HANDSHAKE, 0);
    response.extend_from_slice(&player_id.to_be_bytes());
    let _ = inner.socket.send_to(&response, addr).await;

    if let Some(cb) = &inner.on_player_connected {
        cb(player_id);
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
