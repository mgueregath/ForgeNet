// Ejemplo de "juego" construido ENCIMA de networkcore.hpp, no dentro de él.
// Prueba que el core es de verdad genérico: ni NetworkHost ni Server saben
// qué es una "barra", una "pelota" ni un "tablero" — este binario es el
// que define ese esquema y lo conecta a los hooks genéricos
// (on_input/on_tick/state_provider/queue_event), incluido el significado
// de Role, que el core solo transporta sin interpretar.
// Análogo a los ejemplos de Go/Rust/Python/C#.
#include "../networkcore/networkcore.hpp"
#include <mutex>
#include <vector>

using namespace networkcore;

constexpr uint8_t EVENT_TYPE_GOAL = 1;

// Roles de taca-taca — opacos para el core, definidos y usados solo acá.
// ROLE_BOARD es el tablero (recibe el mismo snapshot que todos, pero no
// controla ninguna barra); ROLE_PADDLE es un mando/jugador. ROLE_BOARD no
// aparece en ninguna rama de código (solo el "else" implícito de "no es
// ROLE_PADDLE" importa acá) pero se deja definida para que un cliente que
// arma el handshake sepa qué valor mandar para conectarse como tablero.
[[maybe_unused]] constexpr uint8_t ROLE_BOARD = 0x01;
constexpr uint8_t ROLE_PADDLE = 0x02;

// MAX_PADDLES: cupo de mandos por partida — regla de taca-taca, no del
// core (Server::ServerOptions::max_players_per_room es un tope numérico
// genérico que no distingue roles; este chequeo sí lo hace).
constexpr size_t MAX_PADDLES = 2;

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
        // Role es opaco para el core: acá es donde taca-taca decide qué
        // hacer con cada valor. El tablero (ROLE_BOARD) recibe el mismo
        // snapshot que todos (ve la partida completa) pero no controla
        // ninguna barra. Un mando (ROLE_PADDLE) más allá del cupo tampoco
        // recibe barra — se queda conectado, pero sin efecto en el juego
        // (rechazarlo requeriría que el core soporte desconexión
        // post-admisión, que hoy no tiene).
        host.on_player_connected = [this](uint16_t id, uint8_t role) {
            if (role != ROLE_PADDLE) return;
            std::lock_guard<std::mutex> lock(mutex_);
            if (state_.rods.size() >= MAX_PADDLES) return;
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
    // room_factory: se llama una vez por sala creada (Server con
    // HANDSHAKE_MODE_CREATE) — cada partida de taca-taca es independiente,
    // con su propio estado (pelota, score, barras). El Server no sabe nada
    // de esto: solo llama a la factory y le rutea paquetes a lo que
    // devuelve.
    //
    // El TacaTacaGame se guarda dentro de la lambda (shared_ptr implícito
    // vía captura por valor de un shared_ptr) para que viva tanto como el
    // NetworkHost al que está enganchado.
    RoomFactory room_factory = []() {
        auto host = std::make_shared<NetworkHost>();
        auto game = std::make_shared<TacaTacaGame>();
        game->attach_to(*host);
        // Mantiene vivo a `game` mientras `host` exista: el propio host no
        // referencia a game directamente, pero sus callbacks (lambdas de
        // attach_to) sí capturan `this` de game — así que lo prendemos a
        // la vida de host con una captura extra en cualquiera de sus
        // callbacks. on_tick no se usa en este ejemplo, así que lo usamos
        // solo para extender el lifetime.
        host->on_tick = [game](uint64_t) { (void)game; };
        return host;
    };

    // max_players_per_room es el tope genérico del Server (cuenta
    // clientes, sin distinguir roles) — para taca-taca da MAX_PADDLES + 1
    // (tablero). El cupo específico por rol (2 barras exactas) lo aplica
    // TacaTacaGame::attach_to arriba.
    ServerOptions opts;
    opts.max_players_per_room = MAX_PADDLES + 1;

    Server server(room_factory, opts);

    if (!server.start_udp(9999)) {
        std::cerr << "No se pudo arrancar el server" << std::endl;
        return 1;
    }

    while (true) std::this_thread::sleep_for(std::chrono::seconds(1));
}
