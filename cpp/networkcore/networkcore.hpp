// networkcore.hpp — Port a C++ de nucleo-multiplayer/csharp/NetworkCore.
//
// Transporte UDP genérico (handshake, heartbeat, input, snapshot, reliable,
// ping) sin ningún esquema de juego hardcodeado. El juego se engancha vía
// callbacks (on_input, on_tick, state_provider, queue_event) — mismo patrón
// que los cores en Go, Rust, Python y C#. Misma spec de protocolo binario:
// comparten framing y ahora también el formato de snapshot genérico
// (Tick + StatePayload opaco + Events).
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
#include <mutex>
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

constexpr uint8_t FLAG_RELIABLE = 0x02;

constexpr size_t HEADER_SIZE = 9;

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

// --- NetworkHost (rol servidor) ---

struct ReliableMessage {
    uint32_t seq;
    std::vector<uint8_t> data;
    std::chrono::steady_clock::time_point sent;
    uint8_t retries = 0;
};

struct ClientConnection {
    uint16_t player_id;
    struct sockaddr_in addr;
    uint32_t last_seq;
    std::chrono::steady_clock::time_point last_heartbeat;
    int32_t ping = 0;
    bool connected = true;
    std::vector<ReliableMessage> reliable_queue;
};

// Clave de dirección: IP + puerto (sockaddr_in no es directamente
// comparable/hasheable, así que se arma una clave simple).
inline uint64_t addr_key(const struct sockaddr_in& a) {
    return ((uint64_t)a.sin_addr.s_addr << 16) | ntohs(a.sin_port);
}

// Rol "servidor" del protocolo. No sabe nada del juego que corre encima —
// se engancha vía los callbacks públicos, igual que NetworkHost en los
// cores de Go/Rust/Python/C#.
class NetworkHost {
public:
    std::function<void(uint16_t)> on_player_connected;
    std::function<void(uint16_t)> on_player_disconnected;
    std::function<void(const PlayerInput&)> on_input;
    std::function<void(uint64_t)> on_tick;
    std::function<std::vector<uint8_t>()> state_provider;

    explicit NetworkHost(uint16_t port) : port_(port) {}

    ~NetworkHost() { stop(); }

    bool start() {
        socket_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
        if (socket_fd_ < 0) return false;

        int buf_size = 1 << 20;
        setsockopt(socket_fd_, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
        setsockopt(socket_fd_, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));

        struct timeval tv{0, 500 * 1000}; // 500ms — igual razón que en C#: evita
        setsockopt(socket_fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)); // bloqueo indefinido al cerrar

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(port_);
        if (bind(socket_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) return false;

        running_ = true;
        threads_.emplace_back(&NetworkHost::receive_loop, this);
        threads_.emplace_back(&NetworkHost::game_loop, this);
        threads_.emplace_back(&NetworkHost::send_loop, this);
        threads_.emplace_back(&NetworkHost::heartbeat_loop, this);

        std::cout << "🎮 NetworkHost escuchando en :" << port_ << " (60Hz)" << std::endl;
        return true;
    }

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
    }

    // El juego encola sus propios eventos (ej. GOAL) para que salgan en el
    // próximo snapshot.
    void queue_event(GameEvent evt) {
        std::lock_guard<std::mutex> lock(events_mutex_);
        pending_events_.push_back(std::move(evt));
    }

    size_t connected_player_count() {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        return clients_.size();
    }

private:
    uint16_t port_;
    int socket_fd_ = -1;
    std::atomic<bool> running_{false};
    std::vector<std::thread> threads_;

    std::map<uint16_t, ClientConnection> clients_;
    std::map<uint64_t, uint16_t> clients_by_addr_;
    std::mutex clients_mutex_;

    std::atomic<uint16_t> next_player_id_{1};
    std::atomic<uint32_t> sequence_counter_{0};
    std::atomic<uint64_t> tick_{0};

    std::vector<GameEvent> pending_events_;
    std::mutex events_mutex_;

    void send_to(const std::vector<uint8_t>& data, const struct sockaddr_in& addr) {
        sendto(socket_fd_, data.data(), data.size(), 0, (struct sockaddr*)&addr, sizeof(addr));
    }

    void receive_loop() {
        uint8_t buffer[2048];
        struct sockaddr_in addr{};
        socklen_t addr_len = sizeof(addr);

        while (running_) {
            int n = recvfrom(socket_fd_, buffer, sizeof(buffer), 0, (struct sockaddr*)&addr, &addr_len);
            if (n <= 0) continue; // timeout (SO_RCVTIMEO) o error — reintenta y chequea running_

            Header h;
            if (!parse_header(buffer, (size_t)n, h)) continue;
            const uint8_t* payload = buffer + HEADER_SIZE;
            size_t payload_len = (size_t)n - HEADER_SIZE;

            switch (h.type) {
                case PACKET_HANDSHAKE: handle_handshake(h.seq, addr); break;
                case PACKET_INPUT: handle_input(h.seq, addr, h.flags, payload, payload_len); break;
                case PACKET_ACK: handle_ack(h.seq, addr); break;
                case PACKET_PING: handle_ping(h.seq, addr, payload, payload_len); break;
            }
        }
    }

    void handle_handshake(uint32_t seq, const struct sockaddr_in& addr) {
        uint16_t player_id;
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            uint64_t key = addr_key(addr);
            if (clients_by_addr_.count(key)) return;

            player_id = next_player_id_.fetch_add(1);
            ClientConnection client;
            client.player_id = player_id;
            client.addr = addr;
            client.last_seq = seq;
            client.last_heartbeat = std::chrono::steady_clock::now();
            clients_[player_id] = client;
            clients_by_addr_[key] = player_id;
        }

        auto response = build_header(seq, 0, PACKET_HANDSHAKE);
        uint8_t id_buf[2];
        write_u16(id_buf, player_id);
        response.insert(response.end(), id_buf, id_buf + 2);
        send_to(response, addr);

        if (on_player_connected) on_player_connected(player_id);
    }

    ClientConnection* find_client_locked(const struct sockaddr_in& addr) {
        auto it = clients_by_addr_.find(addr_key(addr));
        if (it == clients_by_addr_.end()) return nullptr;
        return &clients_[it->second];
    }

    void handle_input(uint32_t seq, const struct sockaddr_in& addr, uint8_t flags, const uint8_t* payload, size_t len) {
        int16_t dx, dy;
        uint16_t rot;
        uint32_t actions;
        if (!decode_input_payload(payload, len, dx, dy, rot, actions)) return;

        uint16_t player_id;
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            auto* client = find_client_locked(addr);
            if (!client) return;
            client->last_heartbeat = std::chrono::steady_clock::now();
            player_id = client->player_id;
        }

        if (flags & FLAG_RELIABLE) {
            auto ack = build_header(seq, seq, PACKET_ACK);
            send_to(ack, addr);
        }

        if (on_input) {
            PlayerInput input{player_id, seq, now_unix_millis(), dx, dy, rot, actions};
            on_input(input);
        }
    }

    void handle_ack(uint32_t seq, const struct sockaddr_in& addr) {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        auto* client = find_client_locked(addr);
        if (!client) return;
        client->last_heartbeat = std::chrono::steady_clock::now();
        auto& q = client->reliable_queue;
        q.erase(std::remove_if(q.begin(), q.end(), [seq](const ReliableMessage& m) { return m.seq <= seq; }), q.end());
    }

    void handle_ping(uint32_t seq, const struct sockaddr_in& addr, const uint8_t* payload, size_t len) {
        if (len < 8) return;
        int64_t client_time = read_i64(payload);
        int64_t server_time = now_unix_millis();

        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            auto* client = find_client_locked(addr);
            if (client) {
                client->ping = (int32_t)((server_time - client_time) / 2);
                client->last_heartbeat = std::chrono::steady_clock::now();
            }
        }

        auto response = build_header(seq, 0, PACKET_PING);
        uint8_t ts_buf[8];
        write_i64(ts_buf, server_time);
        response.insert(response.end(), ts_buf, ts_buf + 8);
        send_to(response, addr);
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
            send_to(packet, client.addr);
        }
    }

    void send_loop() {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));

            auto now = std::chrono::steady_clock::now();
            std::vector<std::pair<struct sockaddr_in, std::vector<uint8_t>>> to_resend;

            {
                std::lock_guard<std::mutex> lock(clients_mutex_);
                for (auto& [id, client] : clients_) {
                    for (auto& msg : client.reliable_queue) {
                        if (now - msg.sent > std::chrono::milliseconds(100) && msg.retries < 3) {
                            to_resend.emplace_back(client.addr, msg.data);
                            msg.sent = now;
                            msg.retries++;
                        }
                    }
                }
            }

            for (auto& [addr, data] : to_resend) send_to(data, addr);
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
                    clients_by_addr_.erase(addr_key(clients_[id].addr));
                    clients_.erase(id);
                }
            }

            for (auto id : to_remove) {
                if (on_player_disconnected) on_player_disconnected(id);
            }
        }
    }
};

} // namespace networkcore
