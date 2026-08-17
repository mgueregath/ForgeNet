"""networkcore.py — Port a Python de nucleo-multiplayer/csharp/NetworkCore.

Transporte UDP genérico (handshake, heartbeat, input, snapshot, reliable,
ping) sin ningún esquema de juego hardcodeado. El juego se engancha vía
callbacks (on_input, on_tick, state_provider, queue_event) — igual patrón
que los cores en Go, Rust y C#. Misma spec de protocolo binario: comparten
framing con esos tres, y ahora también el formato de snapshot genérico
(Tick + StatePayload opaco + Events).

Incluye NetworkHost (rol servidor de UNA sala/partida), Server (multiplexa
muchas salas concurrentes sobre el mismo socket — ver server.go en Go) y
NetworkClient (rol cliente) — a diferencia de Go/Rust/C++ (solo servidor) y
TypeScript (solo cliente), Python siempre tuvo ambos roles en este
proyecto, así que acá vive el mirror completo del split Go host.go/server.go.
"""

import random
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

# --- Payload del handshake (join) ---
#
# El core no sabe qué juego corre encima, así que role viaja como un byte
# opaco: cada juego define sus propios valores y su propio significado (ej.
# taca-taca usa un valor para "barra" y otro para "tablero" — ver
# ejemplo_tacataca.py), igual que ya pasa con state_payload y
# GameEvent.event_type.

# Modos de handshake — quién crea una sala nueva vs. quién se une a una
# existente por código. Esto sí es genérico: cualquier juego con sesiones
# de partida necesita esta distinción.
HANDSHAKE_MODE_CREATE = 0x00
HANDSHAKE_MODE_JOIN = 0x01

# Motivos de rechazo del handshake, enviados en un PACKET_DISCONNECT.
REASON_ROOM_NOT_FOUND = 0x01
REASON_ROOM_FULL = 0x02


def build_header(seq: int, ack: int, packet_type: int, flags: int = 0) -> bytes:
    return struct.pack(">IIB", seq, ack, (packet_type & 0x0F) | ((flags & 0x0F) << 4))


def parse_header(data: bytes) -> Optional[Tuple[int, int, int, int]]:
    if len(data) < HEADER_SIZE:
        return None
    seq, ack, type_and_flags = struct.unpack(">IIB", data[:HEADER_SIZE])
    return seq, ack, type_and_flags & 0x0F, (type_and_flags >> 4) & 0x0F


def encode_join_payload(mode: int, role: int, room_code: str) -> bytes:
    """Mode(1) + Role(1) + RoomCodeLen(1) + RoomCode(N). Va después del
    header en un PACKET_HANDSHAKE mandado por el cliente. room_code se
    ignora en HANDSHAKE_MODE_CREATE (el server genera el código)."""
    room_bytes = room_code.encode("ascii")
    return bytes([mode, role, len(room_bytes)]) + room_bytes


def decode_join_payload(payload: bytes) -> Optional[Tuple[int, int, str]]:
    if len(payload) < 3:
        return None
    code_len = payload[2]
    if len(payload) < 3 + code_len:
        return None
    return payload[0], payload[1], payload[3 : 3 + code_len].decode("ascii")


def encode_handshake_ack(player_id: int, room_code: str) -> bytes:
    """PlayerID(2) + RoomCodeLen(1) + RoomCode(N). Va después del header en
    la respuesta del server — tanto quien crea la sala como quien se une
    reciben el código, para poder compartirlo (ej. un QR)."""
    room_bytes = room_code.encode("ascii")
    return struct.pack(">H", player_id) + bytes([len(room_bytes)]) + room_bytes


def decode_handshake_ack(payload: bytes) -> Optional[Tuple[int, str]]:
    if len(payload) < 3:
        return None
    player_id = struct.unpack(">H", payload[:2])[0]
    code_len = payload[2]
    if len(payload) < 3 + code_len:
        return None
    return player_id, payload[3 : 3 + code_len].decode("ascii")


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


# --- Peer: abstracción mínima de transporte ---


class Peer:
    """Cualquier cosa que pueda mandar bytes crudos y tenga una clave
    estable de peer (ver Peer en Go, host.go). Server crea una instancia
    por origen UDP; ni NetworkHost ni Server necesitan saber más que esto
    de qué hay del otro lado — es lo que permite que, igual que en Go, un
    mismo Server pueda algún día rutear paquetes de otros transportes sin
    tocar el resto del core.

    ip(): el protocolo no lleva ningún token de sesión, así que
    admit_player usa la IP (sin el puerto, que cambia en cada reconexión —
    nuevo socket UDP) como heurística para reconocer "es el mismo
    dispositivo reconectándose" — ver el comentario grande ahí."""

    def __init__(self, sock: socket.socket, addr: Tuple[str, int]):
        self._sock = sock
        self.addr = addr

    def send(self, data: bytes):
        try:
            self._sock.sendto(data, self.addr)
        except OSError:
            pass

    def key(self) -> Tuple[str, int]:
        return self.addr

    def ip(self) -> str:
        return self.addr[0]


# --- NetworkHost (rol servidor de UNA sala) ---


@dataclass
class _ReliableMessage:
    seq: int
    data: bytes
    sent: float
    retries: int = 0


@dataclass
class _ClientConnection:
    """Estado de sesión de un cliente. No tiene ningún campo de estado de
    juego — eso vive en el juego, no acá. role es opaco: el core lo guarda
    y lo expone (ver NetworkHost.get_client_role) pero nunca lo interpreta
    — cada juego define sus propios valores y qué significan."""

    player_id: int
    peer: Peer
    role: int
    last_seq: int
    last_heartbeat: float
    ping: int = 0
    connected: bool = True
    reliable_queue: List[_ReliableMessage] = field(default_factory=list)


class NetworkHost:
    """Rol "servidor" del protocolo para UNA sala/partida. No sabe nada del
    juego que corre encima — se engancha vía los callbacks de abajo, igual
    que NetworkHost en los cores de Go/Rust/C#. Tampoco sabe a qué sala
    pertenece ni posee ningún socket: eso lo decide y lo posee Server, que
    crea una instancia de NetworkHost por sala y le rutea sus paquetes (ver
    clase Server más abajo).
    """

    def __init__(self):
        # on_player_connected(player_id, role, reconnected): reconnected=True
        # si esto fue una reconexión reconocida por IP (ver admit_player) —
        # el juego puede usarlo para NO pisar el estado que ya tenía ese
        # jugador con uno vacío.
        self.on_player_connected: Optional[Callable[[int, int, bool], None]] = None
        self.on_player_disconnected: Optional[Callable[[int], None]] = None
        self.on_input: Optional[Callable[[PlayerInput], None]] = None
        self.on_tick: Optional[Callable[[int], None]] = None
        self.state_provider: Optional[Callable[[], bytes]] = None

        self._running = False
        self._clients: Dict[int, _ClientConnection] = {}
        self._clients_by_key: Dict[Tuple[str, int], int] = {}
        self._lock = threading.RLock()
        self._next_player_id = 1
        self._sequence_counter = 0
        self._tick = 0
        self._pending_events: List[GameEvent] = []
        self._events_lock = threading.Lock()

    def start(self):
        """Arranca el tick loop, la retransmisión de confiables y el
        heartbeat de esta sala. La llama Server exactamente una vez, al
        crear la sala — no abre ningún socket (eso es responsabilidad de
        Server, que es quien lo posee)."""
        self._running = True
        threading.Thread(target=self._game_loop, daemon=True).start()
        threading.Thread(target=self._send_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def stop(self):
        """Detiene los loops de fondo de esta sala. No toca ningún
        transporte — eso es responsabilidad de Server."""
        self._running = False

    def queue_event(self, evt: GameEvent):
        """El juego encola sus propios eventos (ej. GOAL) para que salgan
        en el próximo snapshot. Entrega "mejor esfuerzo": si el paquete se
        pierde, el evento no vuelve a mandarse — para eso ver
        queue_reliable_event."""
        with self._events_lock:
            self._pending_events.append(evt)

    def queue_reliable_event(self, evt: GameEvent):
        """Hace lo mismo que queue_event (el evento sale en el próximo
        snapshot de todos modos) pero además lo manda ya mismo, a cada
        cliente conectado, como un paquete aparte marcado FLAG_RELIABLE —
        el core lo reintenta (mismo mecanismo que ya existía para reliable
        input, ver _send_loop) hasta que el cliente lo confirma con
        PACKET_ACK o se agotan los reintentos.

        La entrega es "al menos una vez", no "exactamente una vez": el
        cliente puede recibir el mismo evento acá y de nuevo en el
        snapshot normal — el juego debe tratarlo como una notificación
        idempotente, no como algo que suma un contador él mismo."""
        self.queue_event(evt)

        snapshot = GameSnapshot(tick=self._tick, timestamp=int(time.time() * 1000), events=[evt])
        payload = snapshot.encode()

        with self._lock:
            clients = list(self._clients.values())

        for client in clients:
            with self._lock:
                self._sequence_counter += 1
                seq = self._sequence_counter
            packet = build_header(seq, 0, PACKET_SNAPSHOT, FLAG_RELIABLE) + payload

            with self._lock:
                client.reliable_queue.append(_ReliableMessage(seq=seq, data=packet, sent=time.time()))
            client.peer.send(packet)

    def pending_reliable_count(self, player_id: int) -> int:
        """Cuántos paquetes reliable (ver queue_reliable_event) todavía no
        fueron confirmados por este cliente. Diagnóstico genérico, no
        específico de ningún evento en particular."""
        with self._lock:
            client = self._clients.get(player_id)
            if client is None:
                return 0
            return len(client.reliable_queue)

    def connected_player_count(self) -> int:
        """Solo cuenta connected==True — self._clients también incluye
        clientes desconectados que quedaron "reclamables" para una
        reconexión (ver _heartbeat_loop/admit_player), así que
        len(self._clients) ya no alcanza."""
        with self._lock:
            return sum(1 for c in self._clients.values() if c.connected)

    # --- Admisión de clientes ---

    def admit_player(self, peer: Peer, role: int) -> Tuple[int, bool]:
        """Registra un cliente nuevo en esta sala, o reconoce una
        reconexión, y dispara on_player_connected(player_id, role,
        reconnected). La llama Server, después de decidir (crear/unirse) a
        qué sala pertenece el cliente. role es opaco para el core — cada
        juego define sus propios valores y qué hacer con ellos.

        Reconexión: si hay un cliente marcado desconectado (ver
        _heartbeat_loop) con la misma IP (Peer.ip()) que quien está
        haciendo el handshake, se le devuelve la MISMA identidad
        (player_id, y el role ORIGINAL — no el que venga en este
        handshake, para no perder de vista quién era) en vez de crear un
        jugador nuevo. Es una heurística por IP, no un token de sesión (el
        protocolo no lleva uno todavía): funciona bien en la LAN/datos
        móviles típica, donde cada dispositivo tiene su propia IP — no es
        a prueba de balas si dos jugadores comparten la misma IP pública
        (NAT compartido) y ambos están desconectados a la vez.

        Idempotente además en el sentido de siempre: un cliente que ya
        está admitido (misma Peer.key(), sin pasar por desconexión)
        devuelve su player_id actual sin duplicar estado ni disparar el
        hook de nuevo (cubre el reintento de un handshake cuyo ack se
        perdió). Devuelve (player_id, reconnected)."""
        key = peer.key()

        with self._lock:
            existing_id = self._clients_by_key.get(key)
            if existing_id is not None:
                return existing_id, False

            reclaimed = None
            ip = peer.ip()
            if ip:
                for c in self._clients.values():
                    if not c.connected and c.peer.ip() == ip:
                        reclaimed = c
                        break

            if reclaimed is not None:
                self._clients_by_key.pop(reclaimed.peer.key(), None)
                reclaimed.peer = peer
                reclaimed.connected = True
                reclaimed.last_heartbeat = time.time()
                self._clients_by_key[key] = reclaimed.player_id
                player_id = reclaimed.player_id
                role = reclaimed.role
                reconnected = True
            else:
                player_id = self._next_player_id
                self._next_player_id += 1

                self._clients[player_id] = _ClientConnection(
                    player_id=player_id, peer=peer, role=role, last_seq=0, last_heartbeat=time.time()
                )
                self._clients_by_key[key] = player_id
                reconnected = False

        if self.on_player_connected:
            self.on_player_connected(player_id, role, reconnected)
        return player_id, reconnected

    def get_client_role(self, player_id: int) -> Optional[int]:
        """Devuelve el rol opaco con el que se admitió a un jugador (ver
        admit_player). None si el jugador no existe (ya desconectado, o id
        inválido)."""
        with self._lock:
            client = self._clients.get(player_id)
            return client.role if client is not None else None

    # --- Recepción: punto de entrada común para cualquier transporte ---

    def handle_packet(self, data: bytes, peer: Peer):
        """Procesa un paquete crudo recibido de un Peer que YA pertenece a
        esta sala. El handshake no pasa por acá: es Server quien lo
        intercepta primero para decidir a qué sala corresponde y recién
        ahí llama a admit_player (ver clase Server)."""
        parsed = parse_header(data)
        if parsed is None:
            return
        seq, _ack, packet_type, flags = parsed
        payload = data[HEADER_SIZE:]

        if packet_type == PACKET_INPUT:
            self._handle_input(seq, peer, flags, payload)
        elif packet_type == PACKET_ACK:
            self._handle_ack(seq, peer)
        elif packet_type == PACKET_PING:
            self._handle_ping(seq, peer, payload)

    def _handle_input(self, seq: int, peer: Peer, flags: int, payload: bytes):
        with self._lock:
            player_id = self._clients_by_key.get(peer.key())
            if player_id is None:
                return
            client = self._clients[player_id]
            client.last_heartbeat = time.time()

        decoded = decode_input_payload(payload)
        if decoded is None:
            return
        delta_x, delta_y, rotation, actions = decoded

        if flags & FLAG_RELIABLE:
            client.peer.send(build_header(seq, seq, PACKET_ACK))

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

    def _handle_ack(self, seq: int, peer: Peer):
        with self._lock:
            player_id = self._clients_by_key.get(peer.key())
            if player_id is None:
                return
            client = self._clients[player_id]
            client.last_heartbeat = time.time()
            client.reliable_queue = [m for m in client.reliable_queue if m.seq > seq]

    def _handle_ping(self, seq: int, peer: Peer, payload: bytes):
        if len(payload) < 8:
            return
        client_time = struct.unpack(">q", payload[:8])[0]
        server_time = int(time.time() * 1000)

        with self._lock:
            player_id = self._clients_by_key.get(peer.key())
            if player_id is not None:
                client = self._clients[player_id]
                client.ping = (server_time - client_time) // 2
                client.last_heartbeat = time.time()

        response = build_header(seq, 0, PACKET_PING) + struct.pack(">q", server_time)
        peer.send(response)

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
            client.peer.send(packet)

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
                        client.peer.send(msg.data)
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
                # Solo se marca connected=False — a propósito NO se borra
                # de self._clients: se mantiene "reclamable" por
                # admit_player si el mismo dispositivo (misma IP) vuelve a
                # conectarse, así el juego puede restaurar su estado en vez
                # de tratarlo como uno nuevo. _clients_by_key sí se limpia:
                # esa key (ej. ip:puerto UDP viejo) ya no sirve para rutear
                # nada. El Server, por su lado, no destruye esta sala
                # instantáneamente al quedar en 0 conectados — ver
                # Server empty_room_grace_period.
                for player_id in to_remove:
                    client = self._clients[player_id]
                    client.connected = False
                    self._clients_by_key.pop(client.peer.key(), None)

            for player_id in to_remove:
                if self.on_player_disconnected:
                    self.on_player_disconnected(player_id)


# --- Server (multiplexa muchas salas concurrentes) ---


@dataclass
class _Room:
    code: str
    host: NetworkHost
    had_player: bool = False
    # empty_since: desde cuándo host.connected_player_count() está en 0 —
    # None mientras la sala tiene al menos un cliente conectado. Ver
    # Server._sweep_empty_rooms.
    empty_since: Optional[float] = None


@dataclass
class _PeerInfo:
    """Recuerda a qué sala pertenece cada Peer y guarda el último ack de
    handshake mandado, para poder responder handshakes reintentados (ack
    perdido) sin volver a admitir al cliente ni crear una sala nueva."""

    room_code: str
    ack: bytes


# room_code_alphabet excluye 0/O y 1/I — ambiguos al leerlos a mano si el
# QR falla y alguien tiene que tipear el código.
ROOM_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


class Server:
    """Multiplexa muchas salas (partidas) concurrentes sobre el mismo
    socket UDP compartido — genérico, no sabe qué juego corre en cada una.
    Un cliente que manda PACKET_HANDSHAKE con HANDSHAKE_MODE_CREATE arranca
    una sala nueva y recibe un código; con HANDSHAKE_MODE_JOIN se une a la
    sala de ese código. Todo lo demás (input, ack, ping) se rutea a la sala
    del cliente que lo mandó. Mismo diseño que Server en Go (server.go).

    room_factory crea un NetworkHost nuevo, con sus propios hooks, cada vez
    que se crea una sala — cada juego decide qué enganchar (ver
    ejemplo_tacataca.py), el Server no sabe nada de eso.
    """

    def __init__(
        self,
        room_factory: Callable[[], NetworkHost],
        room_code_length: int = 6,
        max_players_per_room: Optional[int] = None,
        handshake_rate_limit: int = 10,
        handshake_rate_limit_window: float = 10.0,
        empty_room_grace_period: float = 30.0,
    ):
        self._room_factory = room_factory
        self._room_code_length = room_code_length
        # max_players_per_room: tope de clientes admitidos por sala (None o
        # 0 = sin tope). El Server solo cuenta clientes — no sabe qué rol
        # tiene cada uno, así que un límite que dependa del rol (ej. "2
        # barras + 1 tablero" en taca-taca) es responsabilidad del juego,
        # no de acá (ver ejemplo_tacataca.py).
        self._max_players_per_room = max_players_per_room
        # handshake_rate_limit / _window: máximo de paquetes de handshake
        # aceptados desde un mismo Peer por ventana — el resto se descarta
        # en silencio. No protege contra un atacante que rota de origen en
        # cada intento; sí frena un cliente roto reintentando en loop o un
        # flood simple desde una única conexión.
        self._handshake_rate_limit = handshake_rate_limit
        self._handshake_rate_limit_window = handshake_rate_limit_window
        # empty_room_grace_period: cuánto se espera, desde que una sala que
        # ya tuvo jugadores queda en 0 conectados, antes de destruirla — no
        # instantáneo, para darle tiempo a admit_player de reconocer una
        # reconexión (ver el comentario grande ahí) antes de que la sala
        # misma deje de existir.
        self._empty_room_grace_period = empty_room_grace_period

        self._sock: Optional[socket.socket] = None
        self._udp_port = 0
        self._running = False
        self._rooms: Dict[str, _Room] = {}
        self._peers: Dict[Tuple[str, int], _PeerInfo] = {}
        self._lock = threading.RLock()

        self._handshake_lock = threading.Lock()
        self._handshake_attempts: Dict[Tuple[str, int], Tuple[float, int]] = {}

    def start_udp(self, port: int):
        """Liga un socket UDP y arranca su loop de recepción. port=0 le
        pide al SO cualquier puerto disponible — útil para un deployment
        que no quiere depender de que un puerto fijo esté libre. El puerto
        real que asignó el SO queda disponible en udp_port()."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(("0.0.0.0", port))
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
        self._udp_port = self._sock.getsockname()[1]
        self._running = True

        threading.Thread(target=self._receive_loop, daemon=True).start()
        threading.Thread(target=self._janitor_loop, daemon=True).start()

        print(f"🌐 Server escuchando en :{self._udp_port} (multi-sala, 60Hz por sala)")

    def udp_port(self) -> int:
        """Puerto UDP real en el que quedó escuchando el server — el mismo
        que se pidió en start_udp, salvo que se haya pedido 0 (cualquiera
        disponible), en cuyo caso es el que el SO asignó de verdad. 0 si
        start_udp nunca se llamó (o falló)."""
        return self._udp_port

    def stop(self):
        """Cierra el transporte y detiene todas las salas."""
        self._running = False
        if self._sock:
            self._sock.close()

        with self._lock:
            rooms = list(self._rooms.values())
            self._rooms = {}
            self._peers = {}
        for room in rooms:
            room.host.stop()

    # --- Recepción ---

    def _receive_loop(self):
        while self._running:
            try:
                data, addr = self._sock.recvfrom(2048)
            except OSError:
                if self._running:
                    continue
                return
            self.handle_packet(data, Peer(self._sock, addr))

    def handle_packet(self, data: bytes, peer: Peer):
        """Punto de entrada común para cualquier transporte — igual que
        NetworkHost.handle_packet, pero a nivel Server, porque el Server es
        quien sabe a qué sala pertenece cada paquete."""
        parsed = parse_header(data)
        if parsed is None:
            return
        seq, _ack, packet_type, _flags = parsed

        if packet_type == PACKET_HANDSHAKE:
            self._handle_handshake(seq, peer, data[HEADER_SIZE:])
            return

        with self._lock:
            info = self._peers.get(peer.key())
            room = self._rooms.get(info.room_code) if info is not None else None
        if room is None:
            return
        room.host.handle_packet(data, peer)

    def _handle_handshake(self, seq: int, peer: Peer, payload: bytes):
        key = peer.key()

        if not self._allow_handshake_attempt(key):
            return

        # Reintento de un handshake ya admitido (el ack anterior se
        # perdió): no crear una sala nueva ni volver a admitir, solo
        # reenviar el ack.
        with self._lock:
            info = self._peers.get(key)
        if info is not None:
            peer.send(info.ack)
            return

        decoded = decode_join_payload(payload)
        if decoded is None:
            return
        mode, role, room_code = decoded

        if mode == HANDSHAKE_MODE_CREATE:
            room = self._create_room()
        elif mode == HANDSHAKE_MODE_JOIN:
            with self._lock:
                room = self._rooms.get(room_code)
            if room is None:
                peer.send(build_header(seq, 0, PACKET_DISCONNECT) + bytes([REASON_ROOM_NOT_FOUND]))
                return
        else:
            return

        if self._max_players_per_room and room.host.connected_player_count() >= self._max_players_per_room:
            peer.send(build_header(seq, 0, PACKET_DISCONNECT) + bytes([REASON_ROOM_FULL]))
            return

        player_id, _reconnected = room.host.admit_player(peer, role)
        room.had_player = True

        ack = build_header(seq, 0, PACKET_HANDSHAKE) + encode_handshake_ack(player_id, room.code)
        peer.send(ack)

        with self._lock:
            self._peers[key] = _PeerInfo(room_code=room.code, ack=ack)

    def _allow_handshake_attempt(self, key: Tuple[str, int]) -> bool:
        """Ventana fija, no token bucket: simple y suficiente para lo que
        protege (ver handshake_rate_limit en __init__)."""
        with self._handshake_lock:
            now = time.time()
            window = self._handshake_attempts.get(key)
            if window is None or now - window[0] > self._handshake_rate_limit_window:
                self._handshake_attempts[key] = (now, 1)
                return True
            start, count = window
            if count >= self._handshake_rate_limit:
                return False
            self._handshake_attempts[key] = (start, count + 1)
            return True

    def _sweep_handshake_attempts(self):
        with self._handshake_lock:
            now = time.time()
            expired = [
                k for k, (start, _count) in self._handshake_attempts.items()
                if now - start > self._handshake_rate_limit_window
            ]
            for k in expired:
                del self._handshake_attempts[k]

    def create_room(self) -> str:
        """Arranca una sala directamente, sin pasar por un handshake de
        red — a diferencia de una sala creada porque un cliente mandó
        HANDSHAKE_MODE_CREATE, acá no hay ningún cliente admitido todavía.

        Pensado para el modo "host embebido" (ej. un tablero en LAN, que es
        el mismo proceso que corre el Server): la app llama a esto una vez
        al arrancar y ya tiene una sala fija a la que los mandos se unen
        con join_room, sin que el propio tablero tenga que hacerse pasar
        por cliente de sí mismo para "crear" la sala. La sala recién
        creada NO cuenta como "vacía" para el janitor (ver
        _sweep_empty_rooms/had_player) hasta que admite a su primer
        cliente de verdad, así que puede esperar indefinidamente a que
        alguien se una sin que empty_room_grace_period la destruya."""
        return self._create_room().code

    def _create_room(self) -> _Room:
        code = self._generate_unique_room_code()
        host = self._room_factory()
        host.start()

        room = _Room(code=code, host=host)
        with self._lock:
            self._rooms[code] = room
        return room

    def _generate_unique_room_code(self) -> str:
        while True:
            code = "".join(random.choice(ROOM_CODE_ALPHABET) for _ in range(self._room_code_length))
            with self._lock:
                if code not in self._rooms:
                    return code

    # --- Janitor: destruye salas vacías ---

    def _janitor_loop(self):
        while self._running:
            time.sleep(5.0)
            self._sweep_empty_rooms()
            self._sweep_handshake_attempts()

    def _sweep_empty_rooms(self):
        """Destruye salas que tuvieron al menos un jugador y llevan más de
        empty_room_grace_period sin ninguno conectado — no instantáneo, para
        darle tiempo a admit_player de reconocer una reconexión (ver el
        comentario grande ahí) antes de que la sala misma deje de existir.
        Una sala recién creada que todavía no tuvo ningún cliente no se
        toca (had_player)."""
        with self._lock:
            now = time.time()
            to_remove = []
            for code, room in self._rooms.items():
                if not room.had_player:
                    continue
                if room.host.connected_player_count() > 0:
                    room.empty_since = None
                    continue
                if room.empty_since is None:
                    room.empty_since = now
                    continue
                if now - room.empty_since < self._empty_room_grace_period:
                    continue
                to_remove.append(code)

            for code in to_remove:
                room = self._rooms.pop(code)
                room.host.stop()
                for peer_key, info in list(self._peers.items()):
                    if info.room_code == code:
                        del self._peers[peer_key]


# --- NetworkClient (rol cliente) ---


class NetworkClient:
    """Rol "cliente" del protocolo — usado tanto por el mando (siempre)
    como por cualquier otro cliente Python que hable con un Server/
    NetworkHost. connect() solo abre el socket; create_room()/join_room()
    hacen el handshake propiamente dicho, porque ahora el server necesita
    saber el modo (crear/unirse), el role opaco del cliente y, si
    corresponde, a qué código de sala unirse — ver encode_join_payload."""

    def __init__(self):
        self.player_id: int = 0
        self.room_code: str = ""
        self.connected = False
        self.last_ping_ms: int = 0
        self.latest_snapshot: Optional[GameSnapshot] = None
        # Motivo del último rechazo de handshake (REASON_ROOM_NOT_FOUND /
        # REASON_ROOM_FULL), o None si el último intento no fue rechazado.
        self.last_reject_reason: Optional[int] = None

        self.on_snapshot: Optional[Callable[[GameSnapshot], None]] = None
        self.on_pong: Optional[Callable[[int], None]] = None

        self._sock: Optional[socket.socket] = None
        self._server_addr: Optional[Tuple[str, int]] = None
        self._running = False
        self._seq = 0
        self._pending_pings: Dict[int, float] = {}
        self._ping_lock = threading.Lock()

    def connect(self, host: str, port: int, timeout: float = 3.0):
        """Abre el socket hacia el server, sin unirse a ninguna sala
        todavía — usar create_room()/join_room() para eso."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.settimeout(timeout)
        self._server_addr = (host, port)

    def create_room(self, role: int) -> bool:
        """Crea una sala nueva (HANDSHAKE_MODE_CREATE) y se une a ella.
        role es opaco para el core — su significado lo define el juego
        (ver ejemplo_tacataca.py). Tras un create_room exitoso, room_code
        queda seteado con el código que asignó el server (para compartir,
        ej. como QR)."""
        return self._handshake(HANDSHAKE_MODE_CREATE, role, "")

    def join_room(self, role: int, room_code: str) -> bool:
        """Se une a una sala existente por código (HANDSHAKE_MODE_JOIN)."""
        return self._handshake(HANDSHAKE_MODE_JOIN, role, room_code)

    def _handshake(self, mode: int, role: int, room_code: str) -> bool:
        if self._sock is None or self._server_addr is None:
            return False

        self.last_reject_reason = None
        payload = encode_join_payload(mode, role, room_code)
        request = build_header(0, 0, PACKET_HANDSHAKE) + payload
        self._sock.sendto(request, self._server_addr)

        try:
            response, _ = self._sock.recvfrom(64)
        except socket.timeout:
            return False

        parsed = parse_header(response)
        if parsed is None:
            return False
        _seq, _ack, packet_type, _flags = parsed

        if packet_type == PACKET_DISCONNECT:
            self.last_reject_reason = response[HEADER_SIZE] if len(response) > HEADER_SIZE else None
            return False
        if packet_type != PACKET_HANDSHAKE:
            return False

        decoded = decode_handshake_ack(response[HEADER_SIZE:])
        if decoded is None:
            return False
        self.player_id, self.room_code = decoded
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
        seq, _ack, packet_type, flags = parsed
        payload = data[HEADER_SIZE:]

        if packet_type == PACKET_SNAPSHOT:
            if flags & FLAG_RELIABLE:
                # Convención de este protocolo (ver NetworkHost.sendAck en
                # Go): el ack de un paquete reliable puntual lleva el seq
                # del paquete recibido en AMBOS campos del header, seq y
                # ack.
                self._sock.sendto(build_header(seq, seq, PACKET_ACK), self._server_addr)
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
