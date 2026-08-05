"""networkcore.py — Port a Python de nucleo-multiplayer/csharp/NetworkCore.

Transporte UDP genérico (handshake, heartbeat, input, snapshot, reliable,
ping) sin ningún esquema de juego hardcodeado. El juego se engancha vía
callbacks (on_input, on_tick, state_provider, queue_event) — igual patrón
que los cores en Go, Rust y C#. Misma spec de protocolo binario: comparten
framing con esos tres, y ahora también el formato de snapshot genérico
(Tick + StatePayload opaco + Events).

Incluye NetworkHost (rol servidor) y NetworkClient (rol cliente) — a
diferencia de Go/Rust/C++ (solo servidor) y TypeScript (solo cliente),
Python siempre tuvo ambos roles en este proyecto.
"""

import socket
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

# --- Protocolo ---

PACKET_SNAPSHOT = 0x01
PACKET_INPUT = 0x02
PACKET_ACK = 0x03
PACKET_PING = 0x04
PACKET_HANDSHAKE = 0x05
PACKET_DISCONNECT = 0x06

FLAG_COMPRESSED = 0x01
FLAG_RELIABLE = 0x02
FLAG_ORDERED = 0x04

HEADER_SIZE = 9

TICK_RATE = 1.0 / 60.0  # 60Hz
RETRY_INTERVAL = 0.1
HEARTBEAT_EVERY = 1.0
HEARTBEAT_TIMEOUT = 10.0
MAX_RETRIES = 3


def build_header(seq: int, ack: int, packet_type: int, flags: int = 0) -> bytes:
    return struct.pack(">IIB", seq, ack, (packet_type & 0x0F) | ((flags & 0x0F) << 4))


def parse_header(data: bytes) -> Optional[Tuple[int, int, int, int]]:
    if len(data) < HEADER_SIZE:
        return None
    seq, ack, type_and_flags = struct.unpack(">IIB", data[:HEADER_SIZE])
    return seq, ack, type_and_flags & 0x0F, (type_and_flags >> 4) & 0x0F


# --- Tipos genéricos ---


@dataclass
class GameEvent:
    """El core no sabe qué significa event_type ni qué hay en data — cada
    juego define su propia tabla de tipos y formato de payload."""

    player_id: int
    event_type: int
    data: bytes = b""


@dataclass
class GameSnapshot:
    """tick/timestamp son del core (sincronización); el estado del juego en
    sí viaja como bytes opacos en state_payload — el core nunca lo
    interpreta, solo lo transporta."""

    tick: int
    timestamp: int
    state_payload: bytes = b""
    events: List[GameEvent] = field(default_factory=list)

    def encode(self) -> bytes:
        buf = struct.pack(">Q", self.tick)
        buf += struct.pack(">I", len(self.state_payload))
        buf += self.state_payload
        buf += bytes([len(self.events)])
        for e in self.events:
            buf += struct.pack(">H", e.player_id)
            buf += bytes([e.event_type, len(e.data)])
            buf += e.data
        return buf

    @staticmethod
    def decode(payload: bytes) -> Optional["GameSnapshot"]:
        """Nunca lanza: un payload corrupto o truncado devuelve None en vez
        de indexar fuera de rango."""
        try:
            offset = 0
            if len(payload) < 8 + 4 + 1:
                return None

            tick = struct.unpack(">Q", payload[offset : offset + 8])[0]
            offset += 8

            state_len = struct.unpack(">I", payload[offset : offset + 4])[0]
            offset += 4
            if offset + state_len > len(payload):
                return None
            state_payload = payload[offset : offset + state_len]
            offset += state_len

            if offset >= len(payload):
                return None
            event_count = payload[offset]
            offset += 1

            events = []
            for _ in range(event_count):
                if offset + 4 > len(payload):
                    return None
                player_id = struct.unpack(">H", payload[offset : offset + 2])[0]
                offset += 2
                event_type = payload[offset]
                offset += 1
                data_len = payload[offset]
                offset += 1
                if offset + data_len > len(payload):
                    return None
                data = payload[offset : offset + data_len]
                offset += data_len
                events.append(GameEvent(player_id, event_type, data))

            return GameSnapshot(tick=tick, timestamp=0, state_payload=state_payload, events=events)
        except (struct.error, IndexError):
            return None


@dataclass
class PlayerInput:
    """Forma genérica (movimiento 2D + rotación + bitmask de acciones) — el
    core no interpreta estos valores, solo los transporta y se los pasa al
    juego vía NetworkHost.on_input."""

    player_id: int
    seq: int
    timestamp: int
    delta_x: int
    delta_y: int
    rotation: int
    actions: int


def encode_input_payload(delta_x: int, delta_y: int, rotation: int, actions: int) -> bytes:
    return struct.pack(">hhHI", delta_x, delta_y, rotation, actions)


def decode_input_payload(payload: bytes) -> Optional[Tuple[int, int, int, int]]:
    if len(payload) < 10:
        return None
    return struct.unpack(">hhHI", payload[:10])


# --- NetworkHost (rol servidor) ---


@dataclass
class _ReliableMessage:
    seq: int
    data: bytes
    sent: float
    retries: int = 0


@dataclass
class _ClientConnection:
    player_id: int
    addr: Tuple[str, int]
    last_seq: int
    last_heartbeat: float
    ping: int = 0
    connected: bool = True
    reliable_queue: List[_ReliableMessage] = field(default_factory=list)


class NetworkHost:
    """Rol "servidor" del protocolo. No sabe nada del juego que corre
    encima — se engancha vía los callbacks de abajo, igual que NetworkHost
    en los cores de Go/Rust/C#.
    """

    def __init__(self):
        self.on_player_connected: Optional[Callable[[int], None]] = None
        self.on_player_disconnected: Optional[Callable[[int], None]] = None
        self.on_input: Optional[Callable[[PlayerInput], None]] = None
        self.on_tick: Optional[Callable[[int], None]] = None
        self.state_provider: Optional[Callable[[], bytes]] = None

        self._sock: Optional[socket.socket] = None
        self._running = False
        self._clients: Dict[int, _ClientConnection] = {}
        self._clients_by_addr: Dict[Tuple[str, int], int] = {}
        self._lock = threading.RLock()
        self._next_player_id = 1
        self._sequence_counter = 0
        self._tick = 0
        self._pending_events: List[GameEvent] = []
        self._events_lock = threading.Lock()

    def start(self, port: int):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(("0.0.0.0", port))
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
        self._running = True

        threading.Thread(target=self._receive_loop, daemon=True).start()
        threading.Thread(target=self._game_loop, daemon=True).start()
        threading.Thread(target=self._send_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        print(f"🎮 NetworkHost escuchando en :{port} (60Hz)")

    def stop(self):
        self._running = False
        if self._sock:
            self._sock.close()

    def queue_event(self, evt: GameEvent):
        """El juego encola sus propios eventos (ej. GOAL) para que salgan
        en el próximo snapshot."""
        with self._events_lock:
            self._pending_events.append(evt)

    def connected_player_count(self) -> int:
        with self._lock:
            return len(self._clients)

    # --- Recepción ---

    def _receive_loop(self):
        while self._running:
            try:
                data, addr = self._sock.recvfrom(2048)
            except OSError:
                if self._running:
                    continue
                return
            self._handle_packet(data, addr)

    def _handle_packet(self, data: bytes, addr: Tuple[str, int]):
        parsed = parse_header(data)
        if parsed is None:
            return
        seq, _ack, packet_type, flags = parsed
        payload = data[HEADER_SIZE:]

        if packet_type == PACKET_HANDSHAKE:
            self._handle_handshake(seq, addr)
        elif packet_type == PACKET_INPUT:
            self._handle_input(seq, addr, flags, payload)
        elif packet_type == PACKET_ACK:
            self._handle_ack(seq, addr)
        elif packet_type == PACKET_PING:
            self._handle_ping(seq, addr, payload)

    def _handle_handshake(self, seq: int, addr: Tuple[str, int]):
        with self._lock:
            if addr in self._clients_by_addr:
                return
            player_id = self._next_player_id
            self._next_player_id += 1

            self._clients[player_id] = _ClientConnection(
                player_id=player_id, addr=addr, last_seq=seq, last_heartbeat=time.time()
            )
            self._clients_by_addr[addr] = player_id

        response = build_header(seq, 0, PACKET_HANDSHAKE) + struct.pack(">H", player_id)
        self._sock.sendto(response, addr)

        if self.on_player_connected:
            self.on_player_connected(player_id)

    def _handle_input(self, seq: int, addr: Tuple[str, int], flags: int, payload: bytes):
        with self._lock:
            player_id = self._clients_by_addr.get(addr)
            if player_id is None:
                return
            client = self._clients[player_id]
            client.last_heartbeat = time.time()

        decoded = decode_input_payload(payload)
        if decoded is None:
            return
        delta_x, delta_y, rotation, actions = decoded

        if flags & FLAG_RELIABLE:
            ack = build_header(seq, seq, PACKET_ACK)
            self._sock.sendto(ack, addr)

        if self.on_input:
            self.on_input(
                PlayerInput(
                    player_id=player_id,
                    seq=seq,
                    timestamp=int(time.time() * 1000),
                    delta_x=delta_x,
                    delta_y=delta_y,
                    rotation=rotation,
                    actions=actions,
                )
            )

    def _handle_ack(self, seq: int, addr: Tuple[str, int]):
        with self._lock:
            player_id = self._clients_by_addr.get(addr)
            if player_id is None:
                return
            client = self._clients[player_id]
            client.last_heartbeat = time.time()
            client.reliable_queue = [m for m in client.reliable_queue if m.seq > seq]

    def _handle_ping(self, seq: int, addr: Tuple[str, int], payload: bytes):
        if len(payload) < 8:
            return
        client_time = struct.unpack(">q", payload[:8])[0]
        server_time = int(time.time() * 1000)

        with self._lock:
            player_id = self._clients_by_addr.get(addr)
            if player_id is not None:
                client = self._clients[player_id]
                client.ping = (server_time - client_time) // 2
                client.last_heartbeat = time.time()

        response = build_header(seq, 0, PACKET_PING) + struct.pack(">q", server_time)
        self._sock.sendto(response, addr)

    # --- Tick de juego ---

    def _game_loop(self):
        while self._running:
            start = time.time()

            if self.on_tick:
                self.on_tick(self._tick)
            self._broadcast_snapshot()
            self._tick += 1

            elapsed = time.time() - start
            if elapsed < TICK_RATE:
                time.sleep(TICK_RATE - elapsed)

    def _broadcast_snapshot(self):
        state_payload = self.state_provider() if self.state_provider else b""

        with self._events_lock:
            events = self._pending_events
            self._pending_events = []

        snapshot = GameSnapshot(
            tick=self._tick, timestamp=int(time.time() * 1000), state_payload=state_payload, events=events
        )
        payload = snapshot.encode()

        with self._lock:
            self._sequence_counter += 1
            seq = self._sequence_counter
            clients = list(self._clients.values())

        packet = build_header(seq, 0, PACKET_SNAPSHOT) + payload
        for client in clients:
            if not client.connected:
                continue
            try:
                self._sock.sendto(packet, client.addr)
            except OSError:
                pass

    # --- Retransmisión de confiables ---

    def _send_loop(self):
        while self._running:
            time.sleep(0.05)
            now = time.time()

            with self._lock:
                clients = list(self._clients.values())

            for client in clients:
                for msg in client.reliable_queue:
                    if now - msg.sent > RETRY_INTERVAL and msg.retries < MAX_RETRIES:
                        try:
                            self._sock.sendto(msg.data, client.addr)
                        except OSError:
                            pass
                        msg.sent = now
                        msg.retries += 1

    # --- Heartbeat / desconexión ---

    def _heartbeat_loop(self):
        while self._running:
            time.sleep(HEARTBEAT_EVERY)
            now = time.time()
            to_remove = []

            with self._lock:
                for player_id, client in self._clients.items():
                    if now - client.last_heartbeat > HEARTBEAT_TIMEOUT:
                        to_remove.append(player_id)
                for player_id in to_remove:
                    client = self._clients.pop(player_id)
                    del self._clients_by_addr[client.addr]

            for player_id in to_remove:
                if self.on_player_disconnected:
                    self.on_player_disconnected(player_id)


# --- NetworkClient (rol cliente) ---


class NetworkClient:
    """Rol "cliente" del protocolo — usado tanto por el mando (siempre)
    como por cualquier otro cliente Python que hable con un NetworkHost."""

    def __init__(self):
        self.player_id: int = 0
        self.connected = False
        self.last_ping_ms: int = 0
        self.latest_snapshot: Optional[GameSnapshot] = None

        self.on_snapshot: Optional[Callable[[GameSnapshot], None]] = None
        self.on_pong: Optional[Callable[[int], None]] = None

        self._sock: Optional[socket.socket] = None
        self._server_addr: Optional[Tuple[str, int]] = None
        self._running = False
        self._seq = 0
        self._pending_pings: Dict[int, float] = {}
        self._ping_lock = threading.Lock()

    def connect(self, host: str, port: int, timeout: float = 3.0) -> bool:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.settimeout(timeout)
        self._server_addr = (host, port)

        self._sock.sendto(build_header(0, 0, PACKET_HANDSHAKE), self._server_addr)
        try:
            response, _ = self._sock.recvfrom(64)
        except socket.timeout:
            return False

        parsed = parse_header(response)
        if parsed is None or parsed[2] != PACKET_HANDSHAKE or len(response) < 11:
            return False

        self.player_id = struct.unpack(">H", response[9:11])[0]
        self.connected = True
        self._running = True

        self._sock.settimeout(0.5)  # timeout corto en vez de bloqueo indefinido, para poder cerrar limpio
        threading.Thread(target=self._receive_loop, daemon=True).start()
        return True

    def disconnect(self):
        self._running = False
        self.connected = False
        if self._sock:
            self._sock.close()

    def send_input(self, delta_x: int, delta_y: int, rotation: int, actions: int, reliable: bool = False):
        if not self.connected:
            return
        self._seq += 1
        flags = FLAG_RELIABLE if reliable else 0
        packet = build_header(self._seq, 0, PACKET_INPUT, flags) + encode_input_payload(
            delta_x, delta_y, rotation, actions
        )
        self._sock.sendto(packet, self._server_addr)

    def send_ping(self):
        if not self.connected:
            return
        self._seq += 1
        payload = struct.pack(">q", int(time.time() * 1000))
        packet = build_header(self._seq, 0, PACKET_PING) + payload
        with self._ping_lock:
            self._pending_pings[self._seq] = time.time()
        self._sock.sendto(packet, self._server_addr)

    def _receive_loop(self):
        while self._running:
            try:
                data, _ = self._sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                return
            self._handle_packet(data)

    def _handle_packet(self, data: bytes):
        parsed = parse_header(data)
        if parsed is None:
            return
        seq, _ack, packet_type, _flags = parsed
        payload = data[HEADER_SIZE:]

        if packet_type == PACKET_SNAPSHOT:
            snapshot = GameSnapshot.decode(payload)
            if snapshot is not None:
                self.latest_snapshot = snapshot
                if self.on_snapshot:
                    self.on_snapshot(snapshot)
        elif packet_type == PACKET_PING:
            with self._ping_lock:
                sent_at = self._pending_pings.pop(seq, None)
            if sent_at is not None:
                self.last_ping_ms = int((time.time() - sent_at) * 1000)
                if self.on_pong:
                    self.on_pong(self.last_ping_ms)
