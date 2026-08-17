//! Ejemplo de "juego" construido ENCIMA de networkcore, no dentro de él.
//! Prueba que el core es de verdad genérico: ni `NetworkHost` ni `Server`
//! saben qué es una "barra", una "pelota" ni un "tablero" — este binario es
//! el que define ese esquema y lo conecta a los hooks genéricos
//! (on_input/on_tick/state_provider/events), incluido el significado de
//! `role`, que el core solo transporta sin interpretar.
//! Análogo a nucleo-multiplayer/csharp/NetworkCore.Tests/TacaTacaExample.cs
//! y nucleo-multiplayer/go/ejemplo-tacataca/main.go.

use networkcore::{GameEvent, NetworkHost, PlayerInput, RoomFactory, Server, ServerOptions};
use std::sync::{Arc, Mutex};

const EVENT_TYPE_GOAL: u8 = 1;

// Roles de taca-taca — opacos para el core, definidos y usados solo acá.
// ROLE_BOARD es el tablero (recibe el mismo snapshot que todos, pero no
// controla ninguna barra); ROLE_PADDLE es un mando/jugador. ROLE_BOARD no se
// compara explícitamente en ningún lado (attach_to solo reacciona a
// ROLE_PADDLE) — cualquier valor distinto de ROLE_PADDLE se comporta como
// tablero, pero el nombre queda documentado acá para quien conecte un
// cliente.
#[allow(dead_code)]
const ROLE_BOARD: u8 = 0x01;
const ROLE_PADDLE: u8 = 0x02;

// MAX_PADDLES: cupo de mandos por partida — regla de taca-taca, no del core
// (ServerOptions::max_players_per_room es un tope numérico genérico que no
// distingue roles; este chequeo sí lo hace).
const MAX_PADDLES: usize = 2;

struct RodState {
    player_id: u16,
    position: i32,
    rotation: u16,
}

#[derive(Default)]
struct TacaTacaState {
    ball_x: i32,
    ball_y: i32,
    score_team_a: u8,
    score_team_b: u8,
    rods: Vec<RodState>,
}

impl TacaTacaState {
    /// Formato propio de este ejemplo (no es parte del protocolo core —
    /// vive dentro de StatePayload, que el core trata como bytes opacos):
    /// BallX(4) + BallY(4) + ScoreA(1) + ScoreB(1) + RodCount(1) + por
    /// barra: PlayerId(2) + Position(4) + Rotation(2)
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(10 + self.rods.len() * 8);
        buf.extend_from_slice(&self.ball_x.to_be_bytes());
        buf.extend_from_slice(&self.ball_y.to_be_bytes());
        buf.push(self.score_team_a);
        buf.push(self.score_team_b);
        buf.push(self.rods.len() as u8);
        for r in &self.rods {
            buf.extend_from_slice(&r.player_id.to_be_bytes());
            buf.extend_from_slice(&r.position.to_be_bytes());
            buf.extend_from_slice(&r.rotation.to_be_bytes());
        }
        buf
    }
}

/// Role es opaco para el core: acá es donde taca-taca decide qué hacer con
/// cada valor. El tablero (ROLE_BOARD) recibe el mismo snapshot que todos
/// (ve la partida completa) pero no controla ninguna barra. Un mando
/// (ROLE_PADDLE) más allá del cupo tampoco recibe barra — se queda
/// conectado, pero sin efecto en el juego (rechazarlo requeriría que el
/// core soporte desconexión post-admisión, que hoy no tiene).
fn attach_to(state: Arc<Mutex<TacaTacaState>>, host: &mut NetworkHost) {
    let events = host.events();

    let s = state.clone();
    host.on_player_connected = Some(Box::new(move |id, role, _reconnected| {
        if role != ROLE_PADDLE {
            return;
        }
        let mut st = s.lock().unwrap();
        if st.rods.len() >= MAX_PADDLES {
            return;
        }
        st.rods.push(RodState { player_id: id, position: 0, rotation: 0 });
    }));

    let s = state.clone();
    host.on_player_disconnected = Some(Box::new(move |id| {
        s.lock().unwrap().rods.retain(|r| r.player_id != id);
    }));

    let s = state.clone();
    host.on_input = Some(Box::new(move |input: PlayerInput| {
        let mut st = s.lock().unwrap();
        let Some(rod) = st.rods.iter_mut().find(|r| r.player_id == input.player_id) else {
            return;
        };

        rod.position += input.delta_x as i32; // el juego decide qué significa delta_x
        rod.rotation = input.rotation;

        let goal = input.actions & 0x01 != 0; // bit 0 = "gol" (definido acá, no por el core)
        if goal {
            st.score_team_a += 1;
        }
        drop(st);

        if goal {
            events.push(GameEvent { player_id: input.player_id, event_type: EVENT_TYPE_GOAL, data: vec![] });
        }
    }));

    let s = state.clone();
    host.state_provider = Some(Box::new(move || s.lock().unwrap().encode()));
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // room_factory: se llama una vez por sala creada (HANDSHAKE_MODE_CREATE)
    // — cada partida de taca-taca es independiente, con su propio estado
    // (pelota, score, barras). El Server no sabe nada de esto: solo llama a
    // la factory y le rutea paquetes a lo que devuelve.
    let room_factory: RoomFactory = Box::new(|| {
        let mut host = NetworkHost::new();
        let state = Arc::new(Mutex::new(TacaTacaState::default()));
        attach_to(state, &mut host);
        host
    });

    // max_players_per_room es el tope genérico del Server (cuenta clientes,
    // sin distinguir roles) — da igual a MAX_PADDLES+1 (tablero). El cupo
    // específico por rol (2 barras exactas) lo aplica attach_to arriba.
    let server = Server::new(
        room_factory,
        ServerOptions { max_players_per_room: MAX_PADDLES + 1, ..ServerOptions::default() },
    );

    server.start_udp(9999).await?;

    // Mantener corriendo.
    std::future::pending::<()>().await;
    Ok(())
}
