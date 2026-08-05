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

    bool handshake() {
        auto req = build_header(0, 0, PACKET_HANDSHAKE);
        sendto(fd_, req.data(), req.size(), 0, (struct sockaddr*)&server_addr_, sizeof(server_addr_));

        uint8_t buf[64];
        int n = recv(fd_, buf, sizeof(buf), 0);
        if (n < 11) return false;
        player_id_ = read_u16(buf + 9);
        return true;
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

    // Lee paquetes hasta que `matches` de true o pase el timeout, descartando
    // lo que no matchea.
    bool read_until(int timeout_ms, uint8_t& out_type, std::vector<uint8_t>& out_payload,
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
                out_type = h.type;
                out_payload = payload;
                return true;
            }
        }
        return false;
    }

    uint16_t player_id_ = 0;

private:
    int fd_;
    struct sockaddr_in server_addr_;
    uint32_t seq_ = 0;
};

void test_generic_mechanisms() {
    std::cout << "== Test 1: NetworkHost <-> TestClient (mecanismos genéricos) ==" << std::endl;

    NetworkHost host(49999);
    std::vector<uint8_t> fake_state = {10, 20, 30, 40, 50};
    host.state_provider = [&]() { return fake_state; };

    std::mutex inputs_mutex;
    std::vector<PlayerInput> received_inputs;
    host.on_input = [&](const PlayerInput& in) {
        std::lock_guard<std::mutex> lock(inputs_mutex);
        received_inputs.push_back(in);
    };

    check("host arrancó", host.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    TestClient client(49999);
    check("handshake conecta", client.handshake());
    check("playerId asignado (>0)", client.player_id_ > 0);

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

    host.queue_event(GameEvent{client.player_id_, 42, {9, 9}});
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

    host.stop();
}

void test_heartbeat_survives() {
    std::cout << std::endl << "== Test 2: heartbeat sobrevive >10s con actividad ==" << std::endl;

    NetworkHost host(49998);
    check("host arrancó", host.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    TestClient client(49998);
    client.handshake();

    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < std::chrono::seconds(12)) {
        client.send_input(1, 1, 0, 0, false);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    check("cliente sigue conectado en el host tras 12s", host.connected_player_count() == 1);
    host.stop();
}

int main() {
    test_generic_mechanisms();
    test_heartbeat_survives();

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << "TODO OK" << std::endl;
        return 0;
    }
    std::cout << failures << " FALLO(S)" << std::endl;
    return 1;
}
