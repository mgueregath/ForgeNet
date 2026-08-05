// Ejemplo de "juego" construido ENCIMA de networkcore.hpp, no dentro de él.
// Prueba que el core es de verdad genérico: NetworkHost no sabe qué es una
// "barra" ni una "pelota" — este binario es el que define ese esquema y lo
// conecta a los hooks genéricos (on_input/on_tick/state_provider/queue_event).
// Análogo a los ejemplos de Go/Rust/Python/C#.
#include "../networkcore/networkcore.hpp"
#include <mutex>
#include <vector>

using namespace networkcore;

constexpr uint8_t EVENT_TYPE_GOAL = 1;

struct RodState {
    uint16_t player_id;
    int32_t position = 0;
    uint16_t rotation = 0;
};

struct TacaTacaState {
    int32_t ball_x = 0, ball_y = 0;
    uint8_t score_team_a = 0, score_team_b = 0;
    std::vector<RodState> rods;

    // Formato propio de este ejemplo (no es parte del protocolo core — vive
    // dentro de state_payload, que el core trata como bytes opacos):
    // BallX(4) + BallY(4) + ScoreA(1) + ScoreB(1) + RodCount(1) + por
    // barra: PlayerID(2) + Position(4) + Rotation(2)
    std::vector<uint8_t> encode() const {
        std::vector<uint8_t> buf;
        buf.reserve(10 + rods.size() * 8);

        uint8_t tmp4[4];
        write_i32(tmp4, ball_x);
        buf.insert(buf.end(), tmp4, tmp4 + 4);
        write_i32(tmp4, ball_y);
        buf.insert(buf.end(), tmp4, tmp4 + 4);
        buf.push_back(score_team_a);
        buf.push_back(score_team_b);
        buf.push_back((uint8_t)rods.size());

        for (const auto& r : rods) {
            uint8_t tmp2[2];
            write_u16(tmp2, r.player_id);
            buf.insert(buf.end(), tmp2, tmp2 + 2);
            write_i32(tmp4, r.position);
            buf.insert(buf.end(), tmp4, tmp4 + 4);
            write_u16(tmp2, r.rotation);
            buf.insert(buf.end(), tmp2, tmp2 + 2);
        }

        return buf;
    }
};

class TacaTacaGame {
public:
    void attach_to(NetworkHost& host) {
        host.on_player_connected = [this](uint16_t id) {
            std::lock_guard<std::mutex> lock(mutex_);
            state_.rods.push_back(RodState{id});
        };

        host.on_player_disconnected = [this](uint16_t id) {
            std::lock_guard<std::mutex> lock(mutex_);
            auto& rods = state_.rods;
            rods.erase(std::remove_if(rods.begin(), rods.end(),
                                       [id](const RodState& r) { return r.player_id == id; }),
                       rods.end());
        };

        host.on_input = [this, &host](const PlayerInput& input) {
            bool goal = false;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto it = std::find_if(state_.rods.begin(), state_.rods.end(),
                                        [&](const RodState& r) { return r.player_id == input.player_id; });
                if (it == state_.rods.end()) return;

                it->position += input.delta_x; // el juego decide qué significa delta_x
                it->rotation = input.rotation;

                if (input.actions & 0x01) { // bit 0 = "gol" (definido acá, no por el core)
                    state_.score_team_a++;
                    goal = true;
                }
            }

            if (goal) {
                host.queue_event(GameEvent{input.player_id, EVENT_TYPE_GOAL, {}});
            }
        };

        host.state_provider = [this]() {
            std::lock_guard<std::mutex> lock(mutex_);
            return state_.encode();
        };
    }

private:
    TacaTacaState state_;
    std::mutex mutex_;
};

int main() {
    NetworkHost host(9999);
    TacaTacaGame game;
    game.attach_to(host);

    if (!host.start()) {
        std::cerr << "No se pudo arrancar el host" << std::endl;
        return 1;
    }

    while (true) std::this_thread::sleep_for(std::chrono::seconds(1));
}
