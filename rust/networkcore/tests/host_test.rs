use networkcore::{
    encode_input_payload, encode_join_payload, parse_header, GameEvent, GameSnapshot,
    NetworkHost, PlayerInput, RoomFactory, Server, ServerOptions, FLAG_RELIABLE,
    HANDSHAKE_MODE_CREATE, HANDSHAKE_MODE_JOIN, PACKET_ACK, PACKET_DISCONNECT, PACKET_HANDSHAKE,
    PACKET_INPUT, PACKET_PING, PACKET_SNAPSHOT,
};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// Cliente UDP mínimo, bloqueante, solo para estos tests — este proyecto
// nunca tuvo un "cliente Rust" público.
struct TestClient {
    socket: UdpSocket,
    player_id: u16,
    room_code: String,
    seq: u32,
}

impl TestClient {
    fn connect(port: u16) -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.connect(("127.0.0.1", port)).unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        Self { socket, player_id: 0, room_code: String::new(), seq: 0 }
    }

    /// Crea una sala nueva (HANDSHAKE_MODE_CREATE). `role` es opaco para el
    /// core — estos tests usan valores arbitrarios, ningún significado.
    fn handshake(&mut self, role: u8) {
        self.join_or_create(HANDSHAKE_MODE_CREATE, role, "");
    }

    /// Se une a una sala existente por código (HANDSHAKE_MODE_JOIN).
    fn join(&mut self, role: u8, room_code: &str) {
        self.join_or_create(HANDSHAKE_MODE_JOIN, role, room_code);
    }

    fn join_or_create(&mut self, mode: u8, role: u8, room_code: &str) {
        self.socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let payload = encode_join_payload(mode, role, room_code);
        let mut req = networkcore::build_header(0, 0, PACKET_HANDSHAKE, 0);
        req.extend_from_slice(&payload);
        self.socket.send(&req).unwrap();

        let mut buf = [0u8; 64];
        let n = self.socket.recv(&mut buf).expect("handshake sin respuesta");
        let (_, _, packet_type, _) =
            parse_header(&buf[..n]).expect("respuesta de handshake malformada");
        if packet_type == PACKET_DISCONNECT {
            panic!("handshake rechazado, reason={}", buf[networkcore::HEADER_SIZE]);
        }
        assert!(n >= networkcore::HEADER_SIZE + 3, "respuesta de handshake muy corta");
        let base = networkcore::HEADER_SIZE;
        self.player_id = u16::from_be_bytes([buf[base], buf[base + 1]]);
        let code_len = buf[base + 2] as usize;
        self.room_code = String::from_utf8_lossy(&buf[base + 3..base + 3 + code_len]).into_owned();
    }

    /// Intenta un handshake que se espera que el server rechace (ej. unirse
    /// a un código inexistente), y devuelve el motivo.
    fn handshake_expect_reject(&mut self, mode: u8, role: u8, room_code: &str) -> u8 {
        self.socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let payload = encode_join_payload(mode, role, room_code);
        let mut req = networkcore::build_header(0, 0, PACKET_HANDSHAKE, 0);
        req.extend_from_slice(&payload);
        self.socket.send(&req).unwrap();

        let mut buf = [0u8; 64];
        let n = self.socket.recv(&mut buf).expect("sin respuesta");
        let (_, _, packet_type, _) = parse_header(&buf[..n]).expect("respuesta malformada");
        assert_eq!(packet_type, PACKET_DISCONNECT, "esperaba PACKET_DISCONNECT");
        buf[networkcore::HEADER_SIZE]
    }

    fn send_input(&mut self, dx: i16, dy: i16, rot: u16, actions: u32, reliable: bool) {
        self.seq += 1;
        let flags = if reliable { FLAG_RELIABLE } else { 0 };
        let mut packet = networkcore::build_header(self.seq, 0, PACKET_INPUT, flags);
        packet.extend_from_slice(&encode_input_payload(dx, dy, rot, actions));
        self.socket.send(&packet).unwrap();
    }

    fn send_ping(&mut self) {
        self.seq += 1;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        let mut packet = networkcore::build_header(self.seq, 0, PACKET_PING, 0);
        packet.extend_from_slice(&now.to_be_bytes());
        self.socket.send(&packet).unwrap();
    }

    /// El campo `seq` del header de un `PACKET_ACK` es, por convención de
    /// este protocolo, el número de secuencia que se está confirmando (ver
    /// `NetworkHostHandle::handle_packet` -> `handle_ack`) — no un seq
    /// propio del cliente.
    fn send_ack(&mut self, acked_seq: u32) {
        let packet = networkcore::build_header(acked_seq, acked_seq, PACKET_ACK, 0);
        self.socket.send(&packet).unwrap();
    }

    fn read_until(&self, timeout: Duration, matches: impl FnMut(u8, &[u8]) -> bool) -> Option<Vec<u8>> {
        self.read_until_with_seq(timeout, matches).map(|(_, p)| p)
    }

    fn read_until_with_seq(
        &self,
        timeout: Duration,
        mut matches: impl FnMut(u8, &[u8]) -> bool,
    ) -> Option<(u32, Vec<u8>)> {
        let deadline = Instant::now() + timeout;
        let mut buf = [0u8; 4096];
        while Instant::now() < deadline {
            self.socket.set_read_timeout(Some(Duration::from_millis(200))).unwrap();
            let n = match self.socket.recv(&mut buf) {
                Ok(n) => n,
                Err(_) => continue,
            };
            let Some((seq, _, packet_type, _)) = parse_header(&buf[..n]) else { continue };
            let payload = buf[networkcore::HEADER_SIZE..n].to_vec();
            if matches(packet_type, &payload) {
                return Some((seq, payload));
            }
        }
        None
    }
}

/// Envuelve un `NetworkHost` detrás de un `Option` compartido para poder
/// usarlo como `RoomFactory`: `Server::new` toma dueño del closure, pero
/// estos tests arman el host *antes* de crear el `Server` (para configurar
/// hooks) y necesitan que la factory lo entregue una sola vez (un solo
/// `HANDSHAKE_MODE_CREATE` por test).
fn single_room_factory(host: NetworkHost) -> RoomFactory {
    let slot = Arc::new(Mutex::new(Some(host)));
    Box::new(move || slot.lock().unwrap().take().expect("factory llamada más de una vez"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_handshake_input_snapshot_event_ping() {
    let mut host = NetworkHost::new();
    let fake_state = vec![10u8, 20, 30, 40, 50];
    let fake_state_clone = fake_state.clone();
    host.state_provider = Some(Box::new(move || fake_state_clone.clone()));

    let received: Arc<Mutex<Vec<PlayerInput>>> = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();
    host.on_input = Some(Box::new(move |input| {
        received_clone.lock().unwrap().push(input);
    }));

    let events = host.events();
    let server = Server::new(single_room_factory(host), ServerOptions::default());
    server.start_udp(39999).await.expect("start_udp falló");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = TestClient::connect(39999);
    client.handshake(1);
    assert!(client.player_id > 0, "player_id no asignado");
    assert!(!client.room_code.is_empty(), "no se recibió código de sala");

    client.send_input(15, -7, 90, 0, false);
    client.send_input(3, 3, 45, 0x99, true);

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if received.lock().unwrap().len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    {
        let r = received.lock().unwrap();
        assert_eq!(r.len(), 2, "esperaba 2 inputs");
        assert_eq!(r[0].delta_x, 15);
        assert_eq!(r[0].rotation, 90);
        assert_eq!(r[1].delta_x, 3);
        assert_eq!(r[1].rotation, 45);
        assert_eq!(r[1].actions, 0x99);
    }

    let snap_payload = client
        .read_until(Duration::from_secs(2), |pt, _| pt == PACKET_SNAPSHOT)
        .expect("no llegó snapshot");
    let snap = GameSnapshot::decode(&snap_payload).expect("decode falló");
    assert_eq!(snap.state_payload, fake_state, "state_payload debe llegar byte a byte igual");

    // Evento custom vía events().push (equivalente a QueueEvent en Go).
    events.push(GameEvent { player_id: client.player_id, event_type: 42, data: vec![9, 9] });

    let evt_payload = client
        .read_until(Duration::from_secs(2), |pt, payload| {
            pt == PACKET_SNAPSHOT
                && GameSnapshot::decode(payload).map(|s| !s.events.is_empty()).unwrap_or(false)
        })
        .expect("no llegó snapshot con el evento custom");
    let snap2 = GameSnapshot::decode(&evt_payload).unwrap();
    assert_eq!(snap2.events[0].event_type, 42);
    assert_eq!(snap2.events[0].data, vec![9, 9]);

    client.send_ping();
    let pong = client.read_until(Duration::from_secs(2), |pt, _| pt == PACKET_PING);
    assert!(pong.is_some(), "no llegó pong");

    server.stop();
}

/// Análogo al de Go, pero verificado indirectamente: este core (a
/// diferencia del de Go) no expone `connected_player_count` a nivel
/// `Server`, así que en vez de eso confirmamos que el server sigue
/// respondiendo con normalidad después de 12s de actividad continua (si el
/// heartbeat no se refrescara con cada input, el cliente sería purgado a
/// los 10s y dejaría de recibir snapshots).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_heartbeat_survives_10_seconds() {
    let server = Server::new(Box::new(NetworkHost::new), ServerOptions::default());
    server.start_udp(39998).await.expect("start_udp falló");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = TestClient::connect(39998);
    client.handshake(1);

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(12) {
        client.send_input(1, 1, 0, 0, false);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let snap = client.read_until(Duration::from_secs(2), |pt, _| pt == PACKET_SNAPSHOT);
    assert!(snap.is_some(), "cliente dejó de recibir snapshots tras 12s de actividad (bug de heartbeat reintroducido)");

    server.stop();
}

/// Prueba el flujo genérico de salas: un cliente crea una sala (sin código
/// todavía) y recibe uno; otro cliente se une con ese código y termina en la
/// MISMA sala (mismo `NetworkHost`) que el primero — sin que el core sepa
/// nada de qué juego es ni de qué son los clientes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_and_join_room() {
    let created_count = Arc::new(Mutex::new(0usize));
    let created_count_clone = created_count.clone();
    let factory: RoomFactory = Box::new(move || {
        *created_count_clone.lock().unwrap() += 1;
        NetworkHost::new()
    });

    let server = Server::new(factory, ServerOptions::default());
    server.start_udp(39997).await.expect("start_udp falló");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut creator = TestClient::connect(39997);
    creator.handshake(1);
    assert!(!creator.room_code.is_empty(), "el creador no recibió código de sala");

    let mut joiner = TestClient::connect(39997);
    joiner.join(2, &creator.room_code);

    assert_eq!(joiner.room_code, creator.room_code, "joiner terminó en otra sala");
    assert_ne!(creator.player_id, joiner.player_id, "ambos clientes recibieron el mismo player_id");
    assert_eq!(
        *created_count.lock().unwrap(),
        1,
        "se creó más de una sala para un solo create + un solo join"
    );

    server.stop();
}

/// Unirse a un código que no existe debe rechazarse con
/// PACKET_DISCONNECT/REASON_ROOM_NOT_FOUND, sin crear nada.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_join_nonexistent_room_rejected() {
    let server = Server::new(Box::new(NetworkHost::new), ServerOptions::default());
    server.start_udp(39996).await.expect("start_udp falló");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = TestClient::connect(39996);
    let reason = client.handshake_expect_reject(HANDSHAKE_MODE_JOIN, 1, "ZZZZZZ");
    assert_eq!(reason, networkcore::REASON_ROOM_NOT_FOUND);

    server.stop();
}

/// El tope es puramente numérico — el Server no sabe qué rol tiene cada
/// cliente, solo cuenta.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_max_players_per_room_rejects_extra_joins() {
    let server = Server::new(
        Box::new(NetworkHost::new),
        ServerOptions { max_players_per_room: 2, ..ServerOptions::default() },
    );
    server.start_udp(39995).await.expect("start_udp falló");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut creator = TestClient::connect(39995);
    creator.handshake(1);

    let mut second = TestClient::connect(39995);
    second.join(1, &creator.room_code);

    let mut third = TestClient::connect(39995);
    let reason = third.handshake_expect_reject(HANDSHAKE_MODE_JOIN, 1, &creator.room_code);
    assert_eq!(reason, networkcore::REASON_ROOM_FULL);

    server.stop();
}

/// El core no interpreta `role`, pero lo guarda y lo pasa a
/// `on_player_connected` y a `get_client_role` — de ahí en más es 100%
/// decisión del juego qué hacer con ese valor. `get_client_role` se prueba
/// indirectamente acá: como el `NetworkHostHandle` real vive dentro del
/// `Server` (no es accesible desde afuera, igual que en Go donde vive
/// dentro de la sala), lo que se verifica end-to-end es lo que el juego SÍ
/// puede observar — el valor que le llega a `on_player_connected` — que es
/// exactamente el mismo valor que devolvería `get_client_role`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_role_is_opaque_but_exposed() {
    let got_roles: Arc<Mutex<std::collections::HashMap<u16, u8>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));
    let got_roles_clone = got_roles.clone();

    let mut host = NetworkHost::new();
    host.on_player_connected = Some(Box::new(move |id, role| {
        got_roles_clone.lock().unwrap().insert(id, role);
    }));

    let server = Server::new(single_room_factory(host), ServerOptions::default());
    server.start_udp(39994).await.expect("start_udp falló");
    tokio::time::sleep(Duration::from_millis(200)).await;

    const ROLE_A: u8 = 7;
    const ROLE_B: u8 = 42;
    let mut client_a = TestClient::connect(39994);
    client_a.handshake(ROLE_A);
    let mut client_b = TestClient::connect(39994);
    client_b.join(ROLE_B, &client_a.room_code);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let roles = got_roles.lock().unwrap();
    assert_eq!(roles.get(&client_a.player_id), Some(&ROLE_A), "rol de A incorrecto");
    assert_eq!(roles.get(&client_b.player_id), Some(&ROLE_B), "rol de B incorrecto");
    drop(roles);

    server.stop();
}

/// Un evento reliable llega, se retransmite mientras el cliente no lo
/// confirme, y deja de retransmitir (la cola queda vacía) apenas el cliente
/// manda el ack. `queue_reliable_event`/`pending_reliable_count` viven en
/// `NetworkHostHandle`, así que este test arranca el host directo (sin
/// pasar por `Server`) para poder llamarlos — el camino
/// handshake-vía-Server ya está cubierto por los tests de arriba.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queue_reliable_event_retransmits_until_acked() {
    let host = NetworkHost::new();
    let socket = Arc::new(
        tokio::net::UdpSocket::bind("127.0.0.1:39993")
            .await
            .expect("bind falló"),
    );
    let handle = host.start(socket.clone());

    // Al no pasar por Server, no hay nadie leyendo el socket — acá se hace
    // manualmente lo que Server::udp_receive_loop hace en el camino real
    // (recibir y despachar a handle_packet), para que el ack del cliente
    // efectivamente le llegue al host.
    {
        let handle = handle.clone();
        let socket = socket.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            loop {
                let Ok((n, addr)) = socket.recv_from(&mut buf).await else { continue };
                handle.handle_packet(&buf[..n], addr).await;
            }
        });
    }

    let mut client = TestClient::connect(39993);
    let client_addr = client.socket.local_addr().unwrap();
    let player_id = handle.admit_player(client_addr, 1);

    handle.queue_reliable_event(GameEvent { player_id, event_type: 9, data: vec![7] });

    let is_target_event = |pt: u8, payload: &[u8]| -> bool {
        pt == PACKET_SNAPSHOT
            && GameSnapshot::decode(payload)
                .map(|s| s.events.len() == 1 && s.events[0].event_type == 9)
                .unwrap_or(false)
    };

    let (seq, _) = client
        .read_until_with_seq(Duration::from_secs(1), is_target_event)
        .expect("no llegó el evento reliable");

    assert!(
        handle.pending_reliable_count(player_id) > 0,
        "el evento reliable no quedó en la cola de retransmisión"
    );

    // retry_interval es 100ms — sin ackear, debería seguir llegando.
    let retried = client.read_until_with_seq(Duration::from_millis(500), is_target_event);
    assert!(retried.is_some(), "el evento reliable no se retransmitió sin ack");

    client.send_ack(seq);

    let deadline = Instant::now() + Duration::from_secs(1);
    let mut cleared = false;
    while Instant::now() < deadline {
        if handle.pending_reliable_count(player_id) == 0 {
            cleared = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(cleared, "la cola de retransmisión no se vació después del ack");

    handle.stop();
}
