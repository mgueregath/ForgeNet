// networkcore.hpp — Port a C++ de nucleo-multiplayer/csharp/NetworkCore.
//
// Transporte UDP genérico (handshake, heartbeat, input, snapshot, reliable,
// ping) sin ningún esquema de juego hardcodeado. El juego se engancha vía
// callbacks (on_input, on_tick, state_provider, queue_event) — mismo patrón
// que los cores en Go, Rust, Python y C#. Misma spec de protocolo binario:
// comparten framing y ahora también el formato de snapshot genérico
// (Tick + StatePayload opaco + Events).
//
// NetworkHost es el rol "servidor" del protocolo para UNA sala/partida —
// ya no bindea su propio socket ni parsea el handshake: eso es de Server
// (ver más abajo), que multiplexa muchas salas concurrentes sobre un mismo
// transporte compartido, igual que networkcore.Server en el core Go. Role
// viaja como un byte opaco: el core lo guarda y lo expone, pero nunca lo
// interpreta — su significado (ej. "barra" vs "tablero" en taca-taca) vive
// solo en el ejemplo que lo usa.
//
// Header-only, C++17, sin dependencias externas (solo POSIX sockets + stdlib).
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace networkcore {

// --- Protocolo ---

constexpr uint8_t PACKET_SNAPSHOT = 0x01;
constexpr uint8_t PACKET_INPUT = 0x02;
constexpr uint8_t PACKET_ACK = 0x03;
constexpr uint8_t PACKET_PING = 0x04;
constexpr uint8_t PACKET_HANDSHAKE = 0x05;
constexpr uint8_t PACKET_DISCONNECT = 0x06;

constexpr uint8_t FLAG_RELIABLE = 0x02;

constexpr size_t HEADER_SIZE = 9;

// Modos de handshake — quién crea una sala nueva vs. quién se une a una
// existente por código. Genérico: cualquier juego con sesiones de partida
// necesita esta distinción, el core no interpreta nada más allá de esto.
constexpr uint8_t HANDSHAKE_MODE_CREATE = 0x00;
constexpr uint8_t HANDSHAKE_MODE_JOIN = 0x01;

// Motivos de rechazo del handshake, enviados en un PACKET_DISCONNECT.
constexpr uint8_t REASON_ROOM_NOT_FOUND = 0x01;
constexpr uint8_t REASON_ROOM_FULL = 0x02;

inline void write_u16(uint8_t* buf, uint16_t v) { buf[0] = (v >> 8) & 0xFF; buf[1] = v & 0xFF; }
inline void write_u32(uint8_t* buf, uint32_t v) {
    buf[0] = (v >> 24) & 0xFF; buf[1] = (v >> 16) & 0xFF;
    buf[2] = (v >> 8) & 0xFF; buf[3] = v & 0xFF;
}
inline void write_i32(uint8_t* buf, int32_t v) { write_u32(buf, (uint32_t)v); }
inline void write_u64(uint8_t* buf, uint64_t v) {
    for (int i = 0; i < 8; i++) buf[i] = (v >> (56 - i * 8)) & 0xFF;
}
inline void write_i64(uint8_t* buf, int64_t v) { write_u64(buf, (uint64_t)v); }

inline uint16_t read_u16(const uint8_t* buf) { return ((uint16_t)buf[0] << 8) | buf[1]; }
inline int16_t read_i16(const uint8_t* buf) { return (int16_t)read_u16(buf); }
inline uint32_t read_u32(const uint8_t* buf) {
    return ((uint32_t)buf[0] << 24) | ((uint32_t)buf[1] << 16) | ((uint32_t)buf[2] << 8) | buf[3];
}
inline int32_t read_i32(const uint8_t* buf) { return (int32_t)read_u32(buf); }
inline uint64_t read_u64(const uint8_t* buf) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v = (v << 8) | buf[i];
    return v;
}
inline int64_t read_i64(const uint8_t* buf) { return (int64_t)read_u64(buf); }

inline std::vector<uint8_t> build_header(uint32_t seq, uint32_t ack, uint8_t type, uint8_t flags = 0) {
    std::vector<uint8_t> buf(HEADER_SIZE);
    write_u32(buf.data(), seq);
    write_u32(buf.data() + 4, ack);
    buf[8] = (type & 0x0F) | ((flags & 0x0F) << 4);
    return buf;
}

struct Header {
    uint32_t seq, ack;
    uint8_t type, flags;
};

inline bool parse_header(const uint8_t* data, size_t len, Header& out) {
    if (len < HEADER_SIZE) return false;
    out.seq = read_u32(data);
    out.ack = read_u32(data + 4);
    out.type = data[8] & 0x0F;
    out.flags = (data[8] >> 4) & 0x0F;
    return true;
}

inline int64_t now_unix_millis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// --- Payload del handshake (join) ---
//
// El core no sabe qué juego corre encima, así que Role viaja como un byte
// opaco: cada juego define sus propios valores y su propio significado (ej.
// taca-taca usa un valor para "barra" y otro para "tablero" — ver su
// ejemplo), igual que ya pasa con StatePayload y GameEvent::event_type.

// encode_join_payload: Mode(1) + Role(1) + RoomCodeLen(1) + RoomCode(N). Va
// después del header en un PACKET_HANDSHAKE mandado por el cliente.
// room_code se ignora en HANDSHAKE_MODE_CREATE (el server genera el código).
inline std::vector<uint8_t> encode_join_payload(uint8_t mode, uint8_t role, const std::string& room_code) {
    std::vector<uint8_t> buf;
    buf.reserve(3 + room_code.size());
    buf.push_back(mode);
    buf.push_back(role);
    buf.push_back((uint8_t)room_code.size());
    buf.insert(buf.end(), room_code.begin(), room_code.end());
    return buf;
}

inline bool decode_join_payload(const uint8_t* payload, size_t len, uint8_t& mode, uint8_t& role, std::string& room_code) {
    if (len < 3) return false;
    uint8_t code_len = payload[2];
    if (len < (size_t)3 + code_len) return false;
    mode = payload[0];
    role = payload[1];
    room_code.assign(reinterpret_cast<const char*>(payload) + 3, code_len);
    return true;
}

// encode_handshake_ack: PlayerID(2) + RoomCodeLen(1) + RoomCode(N). Va
// después del header en la respuesta del server — tanto quien crea la sala
// como quien se une reciben el código, para poder compartirlo (ej. un QR).
inline std::vector<uint8_t> encode_handshake_ack(uint16_t player_id, const std::string& room_code) {
    std::vector<uint8_t> buf;
    buf.reserve(3 + room_code.size());
    uint8_t id_buf[2];
    write_u16(id_buf, player_id);
    buf.insert(buf.end(), id_buf, id_buf + 2);
    buf.push_back((uint8_t)room_code.size());
    buf.insert(buf.end(), room_code.begin(), room_code.end());
    return buf;
}

inline bool decode_handshake_ack(const uint8_t* payload, size_t len, uint16_t& player_id, std::string& room_code) {
    if (len < 3) return false;
    player_id = read_u16(payload);
    uint8_t code_len = payload[2];
    if (len < (size_t)3 + code_len) return false;
    room_code.assign(reinterpret_cast<const char*>(payload) + 3, code_len);
    return true;
}

// --- Tipos genéricos ---

// El core no sabe qué significa event_type ni qué hay en data — cada juego
// define su propia tabla de tipos y formato de payload.
struct GameEvent {
    uint16_t player_id;
    uint8_t event_type;
    std::vector<uint8_t> data;
};

// tick/timestamp son del core (sincronización); el estado del juego en sí
// viaja como bytes opacos en state_payload — el core nunca lo interpreta,
// solo lo transporta.
struct GameSnapshot {
    uint64_t tick = 0;
    int64_t timestamp = 0;
    std::vector<uint8_t> state_payload;
    std::vector<GameEvent> events;

    std::vector<uint8_t> encode() const {
        std::vector<uint8_t> buf;
        buf.reserve(HEADER_SIZE + state_payload.size() + 32);

        uint8_t tick_buf[8];
        write_u64(tick_buf, tick);
        buf.insert(buf.end(), tick_buf, tick_buf + 8);

        uint8_t len_buf[4];
        write_u32(len_buf, (uint32_t)state_payload.size());
        buf.insert(buf.end(), len_buf, len_buf + 4);
        buf.insert(buf.end(), state_payload.begin(), state_payload.end());

        buf.push_back((uint8_t)events.size());
        for (const auto& e : events) {
            uint8_t id_buf[2];
            write_u16(id_buf, e.player_id);
            buf.insert(buf.end(), id_buf, id_buf + 2);
            buf.push_back(e.event_type);
            buf.push_back((uint8_t)e.data.size());
            buf.insert(buf.end(), e.data.begin(), e.data.end());
        }

        return buf;
    }

    // Nunca lanza ni indexa fuera de rango: un payload corrupto o truncado
    // devuelve false.
    static bool decode(const uint8_t* payload, size_t len, GameSnapshot& out) {
        size_t offset = 0;
        if (len < 8 + 4 + 1) return false;

        out.tick = read_u64(payload + offset);
        offset += 8;

        uint32_t state_len = read_u32(payload + offset);
        offset += 4;
        if (offset + state_len > len) return false;
        out.state_payload.assign(payload + offset, payload + offset + state_len);
        offset += state_len;

        if (offset >= len) return false;
        uint8_t event_count = payload[offset++];

        for (int i = 0; i < event_count; i++) {
            if (offset + 4 > len) return false;
            GameEvent e;
            e.player_id = read_u16(payload + offset);
            offset += 2;
            e.event_type = payload[offset++];
            uint8_t data_len = payload[offset++];
            if (offset + data_len > len) return false;
            e.data.assign(payload + offset, payload + offset + data_len);
            offset += data_len;
            out.events.push_back(std::move(e));
        }

        return true;
    }
};

// Forma genérica (movimiento 2D + rotación + bitmask de acciones) — el core
// no interpreta estos valores, solo los transporta y se los pasa al juego
// vía NetworkHost::on_input.
struct PlayerInput {
    uint16_t player_id;
    uint32_t seq;
    int64_t timestamp;
    int16_t delta_x, delta_y;
    uint16_t rotation;
    uint32_t actions;
};

inline std::vector<uint8_t> encode_input_payload(int16_t dx, int16_t dy, uint16_t rot, uint32_t actions) {
    std::vector<uint8_t> buf(10);
    write_u16(buf.data(), (uint16_t)dx);
    write_u16(buf.data() + 2, (uint16_t)dy);
    write_u16(buf.data() + 4, rot);
    write_u32(buf.data() + 6, actions);
    return buf;
}

inline bool decode_input_payload(const uint8_t* payload, size_t len, int16_t& dx, int16_t& dy, uint16_t& rot, uint32_t& actions) {
    if (len < 10) return false;
    dx = read_i16(payload);
    dy = read_i16(payload + 2);
    rot = read_u16(payload + 4);
    actions = read_u32(payload + 6);
    return true;
}

// --- Peer ---
//
// Peer: identidad de un cliente a nivel transporte. El core Go tiene esto
// como una interfaz (UDP crudo o sesión WebTransport) para que un mismo
// NetworkHost pueda servir clientes de ambos transportes a la vez; este
// proyecto en C++ solo tiene transporte UDP (POSIX sockets, ver README),
// así que Peer es una única clase concreta en vez de una interfaz — mismo
// rol (mandar bytes + clave estable), sin necesidad de la abstracción extra
// mientras no haya un segundo transporte que portar.
class Peer {
public:
    Peer() = default;
    Peer(int fd, const struct sockaddr_in& addr) : fd_(fd), addr_(addr) {}

    void send(const std::vector<uint8_t>& data) const {
        if (fd_ < 0) return;
        sendto(fd_, data.data(), data.size(), 0, (const struct sockaddr*)&addr_, sizeof(addr_));
    }

    // Clave estable IP:puerto — análoga a Peer.Key() en el core Go.
    std::string key() const {
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &addr_.sin_addr, ip, sizeof(ip));
        return std::string(ip) + ":" + std::to_string(ntohs(addr_.sin_port));
    }

    const struct sockaddr_in& addr() const { return addr_; }

private:
    int fd_ = -1;
    struct sockaddr_in addr_{};
};

// --- NetworkHost (rol servidor de UNA sala/partida) ---

struct ReliableMessage {
    uint32_t seq;
    std::vector<uint8_t> data;
    std::chrono::steady_clock::time_point sent;
    uint8_t retries = 0;
};

// ClientConnection: estado de sesión de un cliente. No tiene ningún campo
// de estado de juego — eso vive en el juego, no acá. role es opaco: el
// core lo guarda y lo expone (ver get_client_role) pero nunca lo
// interpreta.
struct ClientConnection {
    uint16_t player_id = 0;
    Peer peer;
    uint8_t role = 0;
    uint32_t last_seq = 0;
    std::chrono::steady_clock::time_point last_heartbeat;
    int32_t ping = 0;
    bool connected = true;
    std::vector<ReliableMessage> reliable_queue;
};

// Rol "servidor" del protocolo para UNA sala/partida. No sabe nada del
// juego que corre encima — se engancha vía los callbacks públicos, igual
// que NetworkHost en los cores de Go/Rust/Python/C#. Tampoco bindea ningún
// socket ni sabe a qué sala pertenece: eso es responsabilidad de Server,
// que crea una instancia de NetworkHost por sala y le rutea sus paquetes
// (ver más abajo).
class NetworkHost {
public:
    std::function<void(uint16_t, uint8_t)> on_player_connected;
    std::function<void(uint16_t)> on_player_disconnected;
    std::function<void(const PlayerInput&)> on_input;
    std::function<void(uint64_t)> on_tick;
    std::function<std::vector<uint8_t>()> state_provider;

    NetworkHost() = default;
    ~NetworkHost() { stop(); }

    // Arranca el tick loop, la retransmisión de confiables y el heartbeat
    // de esta sala. La llama Server exactamente una vez, al crear la sala.
    void start() {
        if (running_) return;
        running_ = true;
        threads_.emplace_back(&NetworkHost::game_loop, this);
        threads_.emplace_back(&NetworkHost::send_loop, this);
        threads_.emplace_back(&NetworkHost::heartbeat_loop, this);
    }

    // Detiene los loops de fondo de esta sala. No toca ningún transporte —
    // eso es responsabilidad de Server, que es quien lo posee.
    void stop() {
        if (!running_) return;
        running_ = false;
        for (auto& t : threads_) {
            if (t.joinable()) t.join();
        }
        threads_.clear();
    }

    // Procesa un paquete crudo recibido de un Peer que YA pertenece a esta
    // sala. El handshake no pasa por acá: es Server quien lo intercepta
    // primero para decidir a qué sala corresponde y recién ahí llama a
    // admit_player (ver Server::handle_packet).
    void handle_packet(const uint8_t* data, size_t len, const Peer& peer) {
        Header h;
        if (!parse_header(data, len, h)) return;
        const uint8_t* payload = data + HEADER_SIZE;
        size_t payload_len = len - HEADER_SIZE;

        switch (h.type) {
            case PACKET_INPUT: handle_input(h.seq, peer, h.flags, payload, payload_len); break;
            case PACKET_ACK: handle_ack(h.seq, peer); break;
            case PACKET_PING: handle_ping(h.seq, peer, payload, payload_len); break;
        }
    }

    // Registra un cliente nuevo en esta sala y dispara
    // on_player_connected(player_id, role). La llama Server, después de
    // decidir (crear/unirse) a qué sala pertenece el cliente. Idempotente:
    // un cliente que ya está admitido devuelve su player_id actual sin
    // duplicar estado (cubre el reintento de un handshake cuyo ack se
    // perdió).
    uint16_t admit_player(const Peer& peer, uint8_t role) {
        std::string key = peer.key();
        uint16_t player_id;
        bool is_new = false;

        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            auto it = clients_by_key_.find(key);
            if (it != clients_by_key_.end()) {
                player_id = it->second;
            } else {
                player_id = next_player_id_.fetch_add(1);
                ClientConnection client;
                client.player_id = player_id;
                client.peer = peer;
                client.role = role;
                client.last_heartbeat = std::chrono::steady_clock::now();
                client.connected = true;
                clients_[player_id] = std::move(client);
                clients_by_key_[key] = player_id;
                is_new = true;
            }
        }

        if (is_new && on_player_connected) on_player_connected(player_id, role);
        return player_id;
    }

    // Devuelve el rol opaco con el que se admitió a un jugador (ver
    // admit_player). std::nullopt si el jugador no existe (ya
    // desconectado, o id inválido).
    std::optional<uint8_t> get_client_role(uint16_t player_id) {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        auto it = clients_.find(player_id);
        if (it == clients_.end()) return std::nullopt;
        return it->second.role;
    }

    // El juego encola sus propios eventos (ej. GOAL) para que salgan en el
    // próximo snapshot. Entrega "mejor esfuerzo" — ver queue_reliable_event
    // para eventos que el juego no puede permitirse perder.
    void queue_event(GameEvent evt) {
        std::lock_guard<std::mutex> lock(events_mutex_);
        pending_events_.push_back(std::move(evt));
    }

    // Hace lo mismo que queue_event (el evento sale en el próximo snapshot
    // de todos modos) pero además lo manda ya mismo, a cada cliente
    // conectado, como un paquete aparte marcado FLAG_RELIABLE — se
    // reintenta (mismo mecanismo que ya existía para reliable input, ver
    // send_loop) hasta que el cliente lo confirma con PACKET_ACK o se
    // agotan los reintentos. Pensado para eventos que el juego no puede
    // permitirse perder en una red con pérdida real (ej. GOAL en un server
    // público).
    //
    // La entrega es "al menos una vez", no "exactamente una vez": el
    // cliente puede recibir el mismo evento acá y de nuevo en el snapshot
    // normal — el juego debe tratarlo como una notificación idempotente,
    // no como algo que suma un contador él mismo.
    void queue_reliable_event(GameEvent evt) {
        GameSnapshot snap;
        snap.tick = tick_.load();
        snap.timestamp = now_unix_millis();
        snap.events.push_back(evt);
        auto payload = snap.encode();

        queue_event(std::move(evt));

        std::lock_guard<std::mutex> lock(clients_mutex_);
        for (auto& [id, client] : clients_) {
            uint32_t seq = sequence_counter_.fetch_add(1);
            auto packet = build_header(seq, 0, PACKET_SNAPSHOT, FLAG_RELIABLE);
            packet.insert(packet.end(), payload.begin(), payload.end());

            client.reliable_queue.push_back(ReliableMessage{seq, packet, std::chrono::steady_clock::now(), 0});
            client.peer.send(packet);
        }
    }

    // Cuántos paquetes reliable (ver queue_reliable_event) todavía no
    // fueron confirmados por este cliente. Diagnóstico genérico.
    size_t pending_reliable_count(uint16_t player_id) {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        auto it = clients_.find(player_id);
        if (it == clients_.end()) return 0;
        return it->second.reliable_queue.size();
    }

    size_t connected_player_count() {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        return clients_.size();
    }

private:
    std::atomic<bool> running_{false};
    std::vector<std::thread> threads_;

    std::map<uint16_t, ClientConnection> clients_;
    std::map<std::string, uint16_t> clients_by_key_; // key = Peer::key()
    std::mutex clients_mutex_;

    std::atomic<uint16_t> next_player_id_{1};
    std::atomic<uint32_t> sequence_counter_{0};
    std::atomic<uint64_t> tick_{0};

    std::vector<GameEvent> pending_events_;
    std::mutex events_mutex_;

    // Requiere clients_mutex_ tomado por el caller.
    ClientConnection* find_client_by_key_locked(const std::string& key) {
        auto it = clients_by_key_.find(key);
        if (it == clients_by_key_.end()) return nullptr;
        return &clients_[it->second];
    }

    void handle_input(uint32_t seq, const Peer& peer, uint8_t flags, const uint8_t* payload, size_t len) {
        int16_t dx, dy;
        uint16_t rot;
        uint32_t actions;
        if (!decode_input_payload(payload, len, dx, dy, rot, actions)) return;

        uint16_t player_id;
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            auto* client = find_client_by_key_locked(peer.key());
            if (!client) return;
            client->last_heartbeat = std::chrono::steady_clock::now();
            client->last_seq = seq;
            player_id = client->player_id;
        }

        if (flags & FLAG_RELIABLE) {
            auto ack = build_header(seq, seq, PACKET_ACK);
            peer.send(ack);
        }

        if (on_input) {
            PlayerInput input{player_id, seq, now_unix_millis(), dx, dy, rot, actions};
            on_input(input);
        }
    }

    void handle_ack(uint32_t seq, const Peer& peer) {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        auto* client = find_client_by_key_locked(peer.key());
        if (!client) return;
        client->last_heartbeat = std::chrono::steady_clock::now();
        auto& q = client->reliable_queue;
        q.erase(std::remove_if(q.begin(), q.end(), [seq](const ReliableMessage& m) { return m.seq <= seq; }), q.end());
    }

    void handle_ping(uint32_t seq, const Peer& peer, const uint8_t* payload, size_t len) {
        if (len < 8) return;
        int64_t client_time = read_i64(payload);
        int64_t server_time = now_unix_millis();

        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            auto* client = find_client_by_key_locked(peer.key());
            if (client) {
                client->ping = (int32_t)((server_time - client_time) / 2);
                client->last_heartbeat = std::chrono::steady_clock::now();
            }
        }

        auto response = build_header(seq, 0, PACKET_PING);
        uint8_t ts_buf[8];
        write_i64(ts_buf, server_time);
        response.insert(response.end(), ts_buf, ts_buf + 8);
        peer.send(response);
    }

    void game_loop() {
        while (running_) {
            auto start = std::chrono::steady_clock::now();

            uint64_t tick = tick_.load();
            if (on_tick) on_tick(tick);
            broadcast_snapshot(tick);
            tick_.store(tick + 1);

            auto elapsed = std::chrono::steady_clock::now() - start;
            auto sleep_for = std::chrono::milliseconds(16) - elapsed;
            if (sleep_for.count() > 0) std::this_thread::sleep_for(sleep_for);
        }
    }

    void broadcast_snapshot(uint64_t tick) {
        std::vector<uint8_t> state_payload;
        if (state_provider) state_payload = state_provider();

        std::vector<GameEvent> events;
        {
            std::lock_guard<std::mutex> lock(events_mutex_);
            events = std::move(pending_events_);
            pending_events_.clear();
        }

        GameSnapshot snapshot;
        snapshot.tick = tick;
        snapshot.timestamp = now_unix_millis();
        snapshot.state_payload = std::move(state_payload);
        snapshot.events = std::move(events);

        auto payload = snapshot.encode();
        uint32_t seq = sequence_counter_.fetch_add(1);

        auto packet = build_header(seq, 0, PACKET_SNAPSHOT);
        packet.insert(packet.end(), payload.begin(), payload.end());

        std::lock_guard<std::mutex> lock(clients_mutex_);
        for (auto& [id, client] : clients_) {
            if (!client.connected) continue;
            client.peer.send(packet);
        }
    }

    void send_loop() {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));

            auto now = std::chrono::steady_clock::now();
            std::vector<std::pair<Peer, std::vector<uint8_t>>> to_resend;

            {
                std::lock_guard<std::mutex> lock(clients_mutex_);
                for (auto& [id, client] : clients_) {
                    for (auto& msg : client.reliable_queue) {
                        if (now - msg.sent > std::chrono::milliseconds(100) && msg.retries < 3) {
                            to_resend.emplace_back(client.peer, msg.data);
                            msg.sent = now;
                            msg.retries++;
                        }
                    }
                }
            }

            for (auto& [peer, data] : to_resend) peer.send(data);
        }
    }

    void heartbeat_loop() {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            auto now = std::chrono::steady_clock::now();
            std::vector<uint16_t> to_remove;

            {
                std::lock_guard<std::mutex> lock(clients_mutex_);
                for (auto& [id, client] : clients_) {
                    if (std::chrono::duration_cast<std::chrono::seconds>(now - client.last_heartbeat).count() > 10) {
                        to_remove.push_back(id);
                    }
                }
                for (auto id : to_remove) {
                    clients_by_key_.erase(clients_[id].peer.key());
                    clients_.erase(id);
                }
            }

            for (auto id : to_remove) {
                if (on_player_disconnected) on_player_disconnected(id);
            }
        }
    }
};

// --- Server (multiplexado de salas) ---

// RoomFactory crea un NetworkHost nuevo, con sus propios hooks, cada vez
// que se crea una sala — cada juego decide qué enganchar (ver
// ejemplo-tacataca), Server no sabe nada de eso. Devuelve shared_ptr (no
// unique_ptr): Server::handle_packet resuelve la sala de un peer bajo lock
// y necesita poder copiar una referencia al host y soltar el lock antes de
// rutearle el paquete, sin arriesgarse a que el janitor la destruya en el
// medio (ver sweep_empty_rooms) — un unique_ptr no permitiría esa copia
// segura.
using RoomFactory = std::function<std::shared_ptr<NetworkHost>()>;

// ServerOptions configura el Server. Todo acá es genérico — no hay nada
// específico de ningún juego.
struct ServerOptions {
    // Longitud de los códigos de sala generados. Default: 6.
    int room_code_length = 6;
    // Tope de clientes admitidos por sala (0 = sin tope). El Server solo
    // cuenta clientes — no sabe qué rol tiene cada uno, así que un límite
    // que dependa del rol (ej. "2 barras + 1 tablero" en taca-taca) es
    // responsabilidad del juego, no de acá.
    int max_players_per_room = 0;
    // Máximo de paquetes de handshake aceptados desde un mismo Peer por
    // handshake_rate_limit_window — el resto se descarta en silencio.
    // Default: 10 intentos / 10s. No protege contra un atacante que rota
    // de origen en cada intento — sí frena un cliente roto reintentando en
    // loop o un flood simple desde una única conexión.
    int handshake_rate_limit = 10;
    std::chrono::milliseconds handshake_rate_limit_window{10000};
};

// roomCodeAlphabet excluye 0/O y 1/I — ambiguos al leerlos a mano si el QR
// falla y alguien tiene que tipear el código.
inline const char* room_code_alphabet() { return "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; }

inline std::string random_room_code(int length) {
    static thread_local std::mt19937 rng(std::random_device{}());
    const char* alphabet = room_code_alphabet();
    size_t alphabet_len = std::strlen(alphabet);
    std::uniform_int_distribution<size_t> dist(0, alphabet_len - 1);

    std::string code(length > 0 ? (size_t)length : 0, ' ');
    for (auto& c : code) c = alphabet[dist(rng)];
    return code;
}

// Server multiplexa muchas salas (partidas) concurrentes sobre el mismo
// transporte UDP compartido — genérico, no sabe qué juego corre en cada
// una. Un cliente que manda PACKET_HANDSHAKE con HANDSHAKE_MODE_CREATE
// arranca una sala nueva y recibe un código; con HANDSHAKE_MODE_JOIN se
// une a la sala de ese código. Todo lo demás (input, ack, ping) se rutea a
// la sala del cliente que lo mandó. Análogo a networkcore.Server en el
// core Go, adaptado a los idiomas de C++ (std::thread/std::mutex, ya
// usados en NetworkHost).
class Server {
public:
    Server(RoomFactory factory, ServerOptions opts = {})
        : opts_(opts), room_factory_(std::move(factory)) {}

    ~Server() { stop(); }

    // Arranca el transporte UDP y el janitor de fondo. No hay
    // StartWebTransport en este puerto — este proyecto en C++ solo
    // implementa el transporte UDP (ver README: "Dedicated Server" POSIX).
    bool start_udp(uint16_t port) {
        socket_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
        if (socket_fd_ < 0) return false;

        int buf_size = 1 << 20;
        setsockopt(socket_fd_, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
        setsockopt(socket_fd_, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));

        struct timeval tv{0, 500 * 1000}; // 500ms — evita bloqueo indefinido al cerrar
        setsockopt(socket_fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(port);
        if (bind(socket_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) return false;

        running_ = true;
        threads_.emplace_back(&Server::receive_loop, this);
        threads_.emplace_back(&Server::janitor_loop, this);

        std::cout << "🌐 Server escuchando en :" << port << " (multi-sala)" << std::endl;
        return true;
    }

    // Cierra el transporte y detiene todas las salas.
    void stop() {
        if (!running_) return;
        running_ = false;
        for (auto& t : threads_) {
            if (t.joinable()) t.join();
        }
        threads_.clear();
        if (socket_fd_ >= 0) {
            close(socket_fd_);
            socket_fd_ = -1;
        }

        std::map<std::string, Room> rooms_copy;
        {
            std::lock_guard<std::mutex> lock(mu_);
            rooms_copy = std::move(rooms_);
            rooms_.clear();
            peers_.clear();
        }
        for (auto& [code, r] : rooms_copy) r.host->stop();
    }

    // Punto de entrada común para el paquete crudo recibido — igual que
    // antes lo era NetworkHost::handle_packet, pero a nivel Server, porque
    // Server es quien sabe a qué sala pertenece cada paquete.
    void handle_packet(const uint8_t* data, size_t len, const Peer& peer) {
        Header h;
        if (!parse_header(data, len, h)) return;

        if (h.type == PACKET_HANDSHAKE) {
            handle_handshake(h.seq, peer, data + HEADER_SIZE, len - HEADER_SIZE);
            return;
        }

        std::shared_ptr<NetworkHost> host;
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = peers_.find(peer.key());
            if (it != peers_.end()) {
                auto rit = rooms_.find(it->second.room_code);
                if (rit != rooms_.end()) host = rit->second.host;
            }
        }
        if (!host) return;
        host->handle_packet(data, len, peer);
    }

private:
    struct Room {
        std::string code;
        std::shared_ptr<NetworkHost> host;
        bool had_player = false;
    };

    // Recuerda a qué sala pertenece cada Peer y guarda el último ack de
    // handshake mandado, para poder responder handshakes reintentados
    // (ack perdido) sin volver a admitir al cliente ni crear una sala
    // nueva.
    struct PeerInfo {
        std::string room_code;
        std::vector<uint8_t> ack;
    };

    struct HandshakeWindow {
        std::chrono::steady_clock::time_point start;
        int count = 0;
    };

    ServerOptions opts_;
    RoomFactory room_factory_;

    int socket_fd_ = -1;
    std::atomic<bool> running_{false};
    std::vector<std::thread> threads_;

    std::mutex mu_;
    std::map<std::string, Room> rooms_;
    std::map<std::string, PeerInfo> peers_; // key = Peer::key()

    std::mutex handshake_mu_;
    std::map<std::string, HandshakeWindow> handshake_attempts_;

    void receive_loop() {
        uint8_t buffer[2048];
        struct sockaddr_in addr{};
        socklen_t addr_len = sizeof(addr);

        while (running_) {
            int n = recvfrom(socket_fd_, buffer, sizeof(buffer), 0, (struct sockaddr*)&addr, &addr_len);
            if (n <= 0) continue; // timeout (SO_RCVTIMEO) o error — reintenta y chequea running_

            Peer peer(socket_fd_, addr);
            handle_packet(buffer, (size_t)n, peer);
        }
    }

    void handle_handshake(uint32_t seq, const Peer& peer, const uint8_t* payload, size_t len) {
        std::string key = peer.key();

        if (!allow_handshake_attempt(key)) return;

        // Reintento de un handshake ya admitido (el ack anterior se
        // perdió): no crear una sala nueva ni volver a admitir, solo
        // reenviar el ack.
        {
            std::vector<uint8_t> cached_ack;
            bool has_cached = false;
            {
                std::lock_guard<std::mutex> lock(mu_);
                auto it = peers_.find(key);
                if (it != peers_.end()) {
                    cached_ack = it->second.ack;
                    has_cached = true;
                }
            }
            if (has_cached) {
                peer.send(cached_ack);
                return;
            }
        }

        uint8_t mode, role;
        std::string room_code;
        if (!decode_join_payload(payload, len, mode, role, room_code)) return;

        std::shared_ptr<NetworkHost> host;
        std::string final_code;

        if (mode == HANDSHAKE_MODE_CREATE) {
            std::tie(final_code, host) = create_room();
        } else if (mode == HANDSHAKE_MODE_JOIN) {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = rooms_.find(room_code);
            if (it != rooms_.end()) {
                host = it->second.host;
                final_code = room_code;
            }
        } else {
            return;
        }

        if (!host) {
            auto reject = build_header(seq, 0, PACKET_DISCONNECT, 0);
            reject.push_back(REASON_ROOM_NOT_FOUND);
            peer.send(reject);
            return;
        }

        if (opts_.max_players_per_room > 0 &&
            (int)host->connected_player_count() >= opts_.max_players_per_room) {
            auto reject = build_header(seq, 0, PACKET_DISCONNECT, 0);
            reject.push_back(REASON_ROOM_FULL);
            peer.send(reject);
            return;
        }

        uint16_t player_id = host->admit_player(peer, role);

        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = rooms_.find(final_code);
            if (it != rooms_.end()) it->second.had_player = true;
        }

        auto ack = build_header(seq, 0, PACKET_HANDSHAKE, 0);
        auto ack_payload = encode_handshake_ack(player_id, final_code);
        ack.insert(ack.end(), ack_payload.begin(), ack_payload.end());
        peer.send(ack);

        {
            std::lock_guard<std::mutex> lock(mu_);
            peers_[key] = PeerInfo{final_code, ack};
        }
    }

    // allow_handshake_attempt aplica el rate limit de
    // opts_.handshake_rate_limit por Peer — ventana fija, no token bucket:
    // simple y suficiente para lo que protege (ver ServerOptions).
    bool allow_handshake_attempt(const std::string& key) {
        std::lock_guard<std::mutex> lock(handshake_mu_);
        auto now = std::chrono::steady_clock::now();
        auto it = handshake_attempts_.find(key);
        if (it == handshake_attempts_.end() || now - it->second.start > opts_.handshake_rate_limit_window) {
            handshake_attempts_[key] = HandshakeWindow{now, 1};
            return true;
        }
        if (it->second.count >= opts_.handshake_rate_limit) return false;
        it->second.count++;
        return true;
    }

    void sweep_handshake_attempts() {
        std::lock_guard<std::mutex> lock(handshake_mu_);
        auto now = std::chrono::steady_clock::now();
        for (auto it = handshake_attempts_.begin(); it != handshake_attempts_.end();) {
            if (now - it->second.start > opts_.handshake_rate_limit_window) it = handshake_attempts_.erase(it);
            else ++it;
        }
    }

    std::pair<std::string, std::shared_ptr<NetworkHost>> create_room() {
        std::string code = generate_unique_room_code();
        auto host = room_factory_();
        host->start();

        std::lock_guard<std::mutex> lock(mu_);
        rooms_[code] = Room{code, host, false};
        return {code, host};
    }

    std::string generate_unique_room_code() {
        while (true) {
            std::string code = random_room_code(opts_.room_code_length);
            std::lock_guard<std::mutex> lock(mu_);
            if (rooms_.find(code) == rooms_.end()) return code;
        }
    }

    // janitor_loop destruye salas que se quedaron sin ningún cliente
    // conectado — genérico, no sabe qué juego corre en ellas. Una sala
    // recién creada que todavía no tuvo ningún cliente no se toca
    // (had_player). Duerme en tramos cortos (en vez de un solo sleep de
    // 5s) para que stop() no tenga que esperar hasta 5s a que este thread
    // se una.
    void janitor_loop() {
        auto last_sweep = std::chrono::steady_clock::now();
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (!running_) break;
            if (std::chrono::steady_clock::now() - last_sweep >= std::chrono::seconds(5)) {
                sweep_empty_rooms();
                sweep_handshake_attempts();
                last_sweep = std::chrono::steady_clock::now();
            }
        }
    }

    void sweep_empty_rooms() {
        std::vector<std::pair<std::string, std::shared_ptr<NetworkHost>>> to_remove;
        {
            std::lock_guard<std::mutex> lock(mu_);
            for (auto& [code, r] : rooms_) {
                if (r.had_player && r.host->connected_player_count() == 0) {
                    to_remove.emplace_back(code, r.host);
                }
            }
            for (auto& [code, host] : to_remove) {
                rooms_.erase(code);
                for (auto pit = peers_.begin(); pit != peers_.end();) {
                    if (pit->second.room_code == code) pit = peers_.erase(pit);
                    else ++pit;
                }
            }
        }
        for (auto& [code, host] : to_remove) host->stop();
    }
};

} // namespace networkcore
