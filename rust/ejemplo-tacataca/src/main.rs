//! Ejemplo de "juego" construido ENCIMA de networkcore, no dentro de él.
//! Prueba que el core es de verdad genérico: NetworkHost no sabe qué es una
//! "barra" ni una "pelota" — este binario es el que define ese esquema y lo
//! conecta a los hooks genéricos (on_input/on_tick/state_provider/events).
//! Análogo a nucleo-multiplayer/csharp/NetworkCore.Tests/TacaTacaExample.cs
//! y nucleo-multiplayer/go/ejemplo-tacataca/main.go.

use networkcore::{GameEvent, NetworkHost, PlayerInput};
use std::sync::{Arc, Mutex};

const EVENT_TYPE_GOAL: u8 = 1;

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

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let state = Arc::new(Mutex::new(TacaTacaState::default()));
    let mut host = NetworkHost::new();
    let events = host.events();

    let s = state.clone();
    host.on_player_connected = Some(Box::new(move |id| {
        s.lock().unwrap().rods.push(RodState { player_id: id, position: 0, rotation: 0 });
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

    let _handle = host.start(9999).await?;

    // Mantener corriendo.
    std::future::pending::<()>().await;
    Ok(())
}
