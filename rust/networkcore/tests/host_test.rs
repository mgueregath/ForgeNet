use networkcore::{
    encode_input_payload, parse_header, GameEvent, GameSnapshot, NetworkHost, PlayerInput,
    FLAG_RELIABLE, PACKET_HANDSHAKE, PACKET_INPUT, PACKET_PING, PACKET_SNAPSHOT,
};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// Cliente UDP mínimo, bloqueante, solo para estos tests — este proyecto
// nunca tuvo un "cliente Rust" público.
struct TestClient {
    socket: UdpSocket,
    player_id: u16,
    seq: u32,
}

impl TestClient {
    fn connect(port: u16) -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.connect(("127.0.0.1", port)).unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        Self { socket, player_id: 0, seq: 0 }
    }

    fn handshake(&mut self) {
        let req = networkcore::build_header(0, 0, PACKET_HANDSHAKE, 0);
        self.socket.send(&req).unwrap();

        let mut buf = [0u8; 64];
        let n = self.socket.recv(&mut buf).expect("handshake sin respuesta");
        assert!(n >= 11, "respuesta de handshake muy corta");
        self.player_id = u16::from_be_bytes([buf[9], buf[10]]);
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

    fn read_until(&self, timeout: Duration, mut matches: impl FnMut(u8, &[u8]) -> bool) -> Option<Vec<u8>> {
        let deadline = Instant::now() + timeout;
        let mut buf = [0u8; 4096];
        while Instant::now() < deadline {
            self.socket.set_read_timeout(Some(Duration::from_millis(200))).unwrap();
            let n = match self.socket.recv(&mut buf) {
                Ok(n) => n,
                Err(_) => continue,
            };
            let Some((_, _, packet_type, _)) = parse_header(&buf[..n]) else { continue };
            let payload = buf[networkcore::HEADER_SIZE..n].to_vec();
            if matches(packet_type, &payload) {
                return Some(payload);
            }
        }
        None
    }
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
    let handle = host.start(39999).await.expect("start falló");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = TestClient::connect(39999);
    client.handshake();
    assert!(client.player_id > 0, "playerId no asignado");

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
    assert_eq!(snap.state_payload, fake_state, "StatePayload debe llegar byte a byte igual");

    // Evento custom vía events().push (equivalente a QueueEvent).
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

    handle.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_heartbeat_survives_10_seconds() {
    let host = NetworkHost::new();
    let handle = host.start(39998).await.expect("start falló");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = TestClient::connect(39998);
    client.handshake();

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(12) {
        client.send_input(1, 1, 0, 0, false);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert_eq!(
        handle.connected_player_count(),
        1,
        "cliente se desconectó tras 12s de actividad (bug de heartbeat reintroducido)"
    );

    handle.stop();
}
