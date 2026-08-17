// Harness de validación de networkcore.hpp — análogo a los tests de
// Go/Rust/Python/C#. Cliente UDP mínimo bloqueante, solo para este test
// (este proyecto nunca tuvo un "cliente C++" público).
#include "../networkcore/networkcore.hpp"
#include <cassert>
#include <cstring>
#include <iostream>
#include <mutex>
#include <vector>

using namespace networkcore;

static int failures = 0;

void check(const std::string& name, bool condition) {
    if (condition) {
        std::cout << "  PASS  " << name << std::endl;
    } else {
        std::cout << "  FAIL  " << name << std::endl;
        failures++;
    }
}

class TestClient {
public:
    TestClient(uint16_t port) {
        fd_ = socket(AF_INET, SOCK_DGRAM, 0);
        memset(&server_addr_, 0, sizeof(server_addr_));
        server_addr_.sin_family = AF_INET;
        server_addr_.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &server_addr_.sin_addr);

        struct timeval tv{2, 0};
        setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }

    ~TestClient() { close(fd_); }

    // handshake crea una sala nueva (HANDSHAKE_MODE_CREATE). role es
    // opaco para el core — estos tests usan valores arbitrarios, ningún
    // significado.
    bool handshake(uint8_t role) {
        return join_or_create(HANDSHAKE_MODE_CREATE, role, "");
    }

    // join se une a una sala existente por código (HANDSHAKE_MODE_JOIN).
    bool join(uint8_t role, const std::string& room_code) {
        return join_or_create(HANDSHAKE_MODE_JOIN, role, room_code);
    }

    bool join_or_create(uint8_t mode, uint8_t role, const std::string& room_code) {
        auto payload = encode_join_payload(mode, role, room_code);
        auto req = build_header(0, 0, PACKET_HANDSHAKE);
        req.insert(req.end(), payload.begin(), payload.end());
        sendto(fd_, req.data(), req.size(), 0, (struct sockaddr*)&server_addr_, sizeof(server_addr_));

        uint8_t buf[64];
        int n = recv(fd_, buf, sizeof(buf), 0);
        if (n < (int)HEADER_SIZE) return false;

        Header h;
        if (!parse_header(buf, (size_t)n, h)) return false;
        if (h.type == PACKET_DISCONNECT) return false;

        uint16_t player_id;
        std::string got_room_code;
        if (!decode_handshake_ack(buf + HEADER_SIZE, (size_t)n - HEADER_SIZE, player_id, got_room_code)) return false;

        player_id_ = player_id;
        room_code_ = got_room_code;
        return true;
    }

    // handshake_expect_reject intenta un handshake que se espera que el
    // server rechace (ej. unirse a un código inexistente), y devuelve el
    // motivo (0 si no llegó un PACKET_DISCONNECT).
    uint8_t handshake_expect_reject(uint8_t mode, uint8_t role, const std::string& room_code) {
        auto payload = encode_join_payload(mode, role, room_code);
        auto req = build_header(0, 0, PACKET_HANDSHAKE);
        req.insert(req.end(), payload.begin(), payload.end());
        sendto(fd_, req.data(), req.size(), 0, (struct sockaddr*)&server_addr_, sizeof(server_addr_));

        uint8_t buf[64];
        int n = recv(fd_, buf, sizeof(buf), 0);
        if (n <= 0) return 0;

        Header h;
        if (!parse_header(buf, (size_t)n, h) || h.type != PACKET_DISCONNECT) return 0;
        if ((size_t)n < HEADER_SIZE + 1) return 0;
        return buf[HEADER_SIZE];
    }

    void send_input(int16_t dx, int16_t dy, uint16_t rot, uint32_t actions, bool reliable) {
        seq_++;
        auto header = build_header(seq_, 0, PACKET_INPUT, reliable ? FLAG_RELIABLE : 0);
        auto payload = encode_input_payload(dx, dy, rot, actions);
        header.insert(header.end(), payload.begin(), payload.end());
        sendto(fd_, header.data(), header.size(), 0, (struct sockaddr*)&server_addr_, sizeof(server_addr_));
    }

    void send_ping() {
        seq_++;
        auto header = build_header(seq_, 0, PACKET_PING);
        uint8_t ts[8];
        write_i64(ts, now_unix_millis());
        header.insert(header.end(), ts, ts + 8);
        sendto(fd_, header.data(), header.size(), 0, (struct sockaddr*)&server_addr_, sizeof(server_addr_));
    }

    // Confirma un paquete reliable del server. El campo "seq" del header
    // de un PACKET_ACK es, por convención de este protocolo, el número de
    // secuencia que se está confirmando (ver NetworkHost::handle_ack) —
    // no un seq propio del cliente.
    void send_ack(uint32_t acked_seq) {
        auto packet = build_header(acked_seq, acked_seq, PACKET_ACK);
        sendto(fd_, packet.data(), packet.size(), 0, (struct sockaddr*)&server_addr_, sizeof(server_addr_));
    }

    // Lee paquetes hasta que `matches` de true o pase el timeout, descartando
    // lo que no matchea.
    bool read_until(int timeout_ms, uint8_t& out_type, std::vector<uint8_t>& out_payload,
                     std::function<bool(uint8_t, const std::vector<uint8_t>&)> matches) {
        uint32_t seq;
        return read_until_with_seq(timeout_ms, seq, out_type, out_payload, matches);
    }

    // Igual que read_until, pero también devuelve el seq del header — hace
    // falta para poder ackear un paquete reliable puntual (ver send_ack)
    // en vez de solo inspeccionar su payload.
    bool read_until_with_seq(int timeout_ms, uint32_t& out_seq, uint8_t& out_type, std::vector<uint8_t>& out_payload,
                              std::function<bool(uint8_t, const std::vector<uint8_t>&)> matches) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        uint8_t buf[4096];

        while (std::chrono::steady_clock::now() < deadline) {
            struct timeval tv{0, 200 * 1000};
            setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

            int n = recv(fd_, buf, sizeof(buf), 0);
            if (n <= 0) continue;

            Header h;
            if (!parse_header(buf, (size_t)n, h)) continue;
            std::vector<uint8_t> payload(buf + HEADER_SIZE, buf + n);
            if (matches(h.type, payload)) {
                out_seq = h.seq;
                out_type = h.type;
                out_payload = payload;
                return true;
            }
        }
        return false;
    }

    uint16_t player_id_ = 0;
    std::string room_code_;

private:
    int fd_;
    struct sockaddr_in server_addr_;
    uint32_t seq_ = 0;
};

void test_generic_mechanisms() {
    std::cout << "== Test 1: Server <-> TestClient (mecanismos genéricos) ==" << std::endl;

    auto host = std::make_shared<NetworkHost>();
    std::vector<uint8_t> fake_state = {10, 20, 30, 40, 50};
    host->state_provider = [&]() { return fake_state; };

    std::mutex inputs_mutex;
    std::vector<PlayerInput> received_inputs;
    host->on_input = [&](const PlayerInput& in) {
        std::lock_guard<std::mutex> lock(inputs_mutex);
        received_inputs.push_back(in);
    };

    Server server([&]() { return host; }, ServerOptions{});
    check("server arrancó", server.start_udp(49999));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    TestClient client(49999);
    check("handshake conecta", client.handshake(1));
    check("playerId asignado (>0)", client.player_id_ > 0);
    check("se recibió código de sala", !client.room_code_.empty());

    client.send_input(15, -7, 90, 0, false);
    client.send_input(3, 3, 45, 0x99, true);

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        size_t count;
        {
            std::lock_guard<std::mutex> lock(inputs_mutex);
            count = received_inputs.size();
        }
        if (count >= 2) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    {
        std::lock_guard<std::mutex> lock(inputs_mutex);
        check("OnInput recibió los 2 inputs", received_inputs.size() == 2);
        if (received_inputs.size() == 2) {
            check("1er input crudo sin interpretar (15/90)",
                  received_inputs[0].delta_x == 15 && received_inputs[0].rotation == 90);
            check("2do input crudo sin interpretar (3/45/0x99)",
                  received_inputs[1].delta_x == 3 && received_inputs[1].rotation == 45 &&
                      received_inputs[1].actions == 0x99);
        }
    }

    uint8_t type;
    std::vector<uint8_t> payload;
    bool got_snapshot = client.read_until(2000, type, payload,
                                           [](uint8_t t, const std::vector<uint8_t>&) { return t == PACKET_SNAPSHOT; });
    check("snapshot recibido", got_snapshot);
    if (got_snapshot) {
        GameSnapshot snap;
        bool decoded = GameSnapshot::decode(payload.data(), payload.size(), snap);
        check("snapshot decodificado", decoded);
        if (decoded) {
            check("StatePayload llega byte a byte igual al que provee el juego", snap.state_payload == fake_state);
        }
    }

    host->queue_event(GameEvent{client.player_id_, 42, {9, 9}});
    bool got_event = client.read_until(2000, type, payload, [](uint8_t t, const std::vector<uint8_t>& p) {
        if (t != PACKET_SNAPSHOT) return false;
        GameSnapshot s;
        return GameSnapshot::decode(p.data(), p.size(), s) && !s.events.empty();
    });
    check("evento custom (queue_event) llega al cliente", got_event);
    if (got_event) {
        GameSnapshot snap;
        GameSnapshot::decode(payload.data(), payload.size(), snap);
        check("EventType arbitrario preservado (42)", snap.events[0].event_type == 42);
        check("Data arbitraria preservada",
              snap.events[0].data.size() == 2 && snap.events[0].data[0] == 9 && snap.events[0].data[1] == 9);
    }

    client.send_ping();
    bool got_pong = client.read_until(2000, type, payload,
                                       [](uint8_t t, const std::vector<uint8_t>&) { return t == PACKET_PING; });
    check("ping/pong responde", got_pong);

    server.stop();
}

void test_heartbeat_survives() {
    std::cout << std::endl << "== Test 2: heartbeat sobrevive >10s con actividad ==" << std::endl;

    auto host = std::make_shared<NetworkHost>();
    Server server([&]() { return host; }, ServerOptions{});
    check("server arrancó", server.start_udp(49998));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    TestClient client(49998);
    client.handshake(1);

    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < std::chrono::seconds(12)) {
        client.send_input(1, 1, 0, 0, false);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    check("cliente sigue conectado en el host tras 12s", host->connected_player_count() == 1);
    server.stop();
}

// Test 3: flujo genérico de salas — un cliente crea una sala (sin código
// todavía) y recibe uno; otro cliente se une con ese código y termina en
// la MISMA sala (mismo NetworkHost) que el primero — sin que el core sepa
// nada de qué juego es ni de qué son los clientes.
void test_create_and_join_room() {
    std::cout << std::endl << "== Test 3: crear sala y unirse por código ==" << std::endl;

    std::vector<std::shared_ptr<NetworkHost>> created;
    RoomFactory factory = [&]() {
        auto h = std::make_shared<NetworkHost>();
        created.push_back(h);
        return h;
    };

    Server server(factory, ServerOptions{});
    check("server arrancó", server.start_udp(49997));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    TestClient creator(49997);
    check("el creador conecta", creator.handshake(1));
    check("el creador recibió código de sala", !creator.room_code_.empty());

    TestClient joiner(49997);
    check("el joiner se une por código", joiner.join(2, creator.room_code_));

    check("joiner termina en la misma sala", joiner.room_code_ == creator.room_code_);
    check("ambos clientes recibieron player_id distinto", creator.player_id_ != joiner.player_id_);
    check("se creó una sola sala para un Create + un Join", created.size() == 1);
    if (created.size() == 1) {
        check("la sala tiene 2 clientes conectados", created[0]->connected_player_count() == 2);
    }

    server.stop();
}

// Test 4: unirse a un código que no existe debe rechazarse con
// PACKET_DISCONNECT/REASON_ROOM_NOT_FOUND, sin crear nada.
void test_join_nonexistent_room_rejected() {
    std::cout << std::endl << "== Test 4: join a sala inexistente es rechazado ==" << std::endl;

    Server server([]() { return std::make_shared<NetworkHost>(); }, ServerOptions{});
    check("server arrancó", server.start_udp(49996));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    TestClient client(49996);
    uint8_t reason = client.handshake_expect_reject(HANDSHAKE_MODE_JOIN, 1, "ZZZZZZ");
    check("rechazo con REASON_ROOM_NOT_FOUND", reason == REASON_ROOM_NOT_FOUND);

    server.stop();
}

// Test 5: un cliente que se cae (deja de mandar heartbeat) y vuelve a
// conectarse desde la misma IP a la misma sala recibe el mismo player_id y
// el role ORIGINAL — no el nuevo que mande en el segundo handshake — ver
// el comentario grande en NetworkHost::admit_player. Mirror de
// TestReconnectionByIPPreservesIdentity (Go).
void test_reconnection_by_ip_preserves_identity() {
    std::cout << std::endl << "== Test 5: reconexión por IP preserva identidad y role original ==" << std::endl;

    auto host = std::make_shared<NetworkHost>();
    std::mutex cb_mutex;
    std::vector<uint16_t> player_ids;
    std::vector<bool> reconnected_flags;
    host->on_player_connected = [&](uint16_t id, uint8_t, bool reconnected) {
        std::lock_guard<std::mutex> lock(cb_mutex);
        player_ids.push_back(id);
        reconnected_flags.push_back(reconnected);
    };

    Server server([&]() { return host; }, ServerOptions{});
    check("server arrancó", server.start_udp(49995));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    uint16_t first_id = 0;
    std::string room_code;
    {
        TestClient first(49995);
        check("primer cliente conecta con role=7", first.handshake(7));
        first_id = first.player_id_;
        room_code = first.room_code_;
    } // el socket se cierra acá — simula la caída: deja de mandar heartbeat

    // Esperar más que heartbeat_timeout (10s hardcodeado en heartbeat_loop,
    // igual que heartbeatTimeout en el core Go) para que el server marque
    // al primer cliente como desconectado (pero reclamable). Margen extra
    // (2s) porque heartbeat_loop solo chequea una vez por segundo.
    std::this_thread::sleep_for(std::chrono::milliseconds(12000));

    TestClient second(49995);
    check("segundo cliente se reconecta con role distinto (3, debe ignorarse)", second.join(3, room_code));
    check("reconexión preservó el player_id", second.player_id_ == first_id);

    {
        std::lock_guard<std::mutex> lock(cb_mutex);
        check("hubo 2 llamadas a on_player_connected", reconnected_flags.size() == 2);
        if (reconnected_flags.size() == 2) {
            check("la primera conexión no se marcó reconnected", !reconnected_flags[0]);
            check("la segunda conexión se reconoció como reconnected", reconnected_flags[1]);
        }
    }

    auto role = host->get_client_role(first_id);
    check("el role tras reconexión es el ORIGINAL (7), no el de la reconexión (3)",
          role.has_value() && *role == 7);

    server.stop();
}

// Test 6: pedir el puerto 0 le deja al SO elegir uno libre, y udp_port()
// debe devolver cuál asignó de verdad (no 0). Mirror de TestUDPPortEphemeral
// (Go).
void test_udp_port_ephemeral() {
    std::cout << std::endl << "== Test 6: puerto UDP efímero (port=0) ==" << std::endl;

    Server server([]() { return std::make_shared<NetworkHost>(); }, ServerOptions{});
    check("server arrancó con port=0", server.start_udp(0));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    check("udp_port() devolvió un puerto real (!= 0)", server.udp_port() != 0);

    TestClient client(server.udp_port());
    check("handshake contra el puerto efímero funciona", client.handshake(1));
    check("playerId asignado", client.player_id_ > 0);

    server.stop();
}

// Test 7: modo "host embebido" — la sala existe de entrada, vía
// Server::create_room(), sin ningún cliente todavía, y un cliente se une
// después por HANDSHAKE_MODE_JOIN, igual que a cualquier otra sala. Mirror
// de TestEmbeddedHostMode (Go).
void test_embedded_host_mode() {
    std::cout << std::endl << "== Test 7: modo host embebido (Server::create_room) ==" << std::endl;

    std::vector<uint8_t> fake_state = {9, 9, 9};
    RoomFactory factory = [&]() {
        auto h = std::make_shared<NetworkHost>();
        h->state_provider = [&]() { return fake_state; };
        return h;
    };

    Server server(factory, ServerOptions{});
    check("server arrancó", server.start_udp(49994));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::string room_code = server.create_room();
    check("create_room() devolvió un código de sala", !room_code.empty());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    TestClient mando(49994);
    check("el mando se une a la sala pre-creada", mando.join(1, room_code));
    check("playerId asignado", mando.player_id_ > 0);
    check("el mando terminó en la sala correcta", mando.room_code_ == room_code);

    server.stop();
}

// Test 8: una sala pre-creada (modo host embebido) que todavía no admitió a
// nadie no debe destruirse nunca por el janitor, sin importar cuánto pase
// — el tablero puede quedarse esperando en el lobby indefinidamente. Solo
// empieza a correr empty_room_grace_period una vez que la sala tuvo al
// menos un cliente (ver had_player). Mirror de
// TestEmbeddedHostRoomSurvivesEmptyGracePeriod (Go).
void test_embedded_host_room_survives_empty_grace_period() {
    std::cout << std::endl << "== Test 8: sala host-embebido sobrevive vacía más allá del grace period ==" << std::endl;

    ServerOptions opts;
    opts.empty_room_grace_period = std::chrono::seconds(1); // corto a propósito

    Server server([]() { return std::make_shared<NetworkHost>(); }, opts);
    check("server arrancó", server.start_udp(49993));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::string room_code = server.create_room();

    // Esperar bastante más que el grace period (1s) Y que un ciclo del
    // janitor (5s, fijo en janitor_loop) — si la sala recién creada, sin
    // ningún cliente aún, fuera tratada igual que una vacía-tras-tener-
    // gente, el janitor ya la habría destruido acá.
    std::this_thread::sleep_for(std::chrono::seconds(7));

    TestClient client(49993);
    check("la sala pre-creada sobrevivió la espera", client.join(1, room_code));

    server.stop();
}

int main() {
    test_generic_mechanisms();
    test_heartbeat_survives();
    test_create_and_join_room();
    test_join_nonexistent_room_rejected();
    test_reconnection_by_ip_preserves_identity();
    test_udp_port_ephemeral();
    test_embedded_host_mode();
    test_embedded_host_room_survives_empty_grace_period();

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << "TODO OK" << std::endl;
        return 0;
    }
    std::cout << failures << " FALLO(S)" << std::endl;
    return 1;
}
