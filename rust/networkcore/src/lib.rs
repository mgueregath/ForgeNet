//! Port a Rust de nucleo-multiplayer/csharp/NetworkCore: transporte UDP
//! genérico (handshake, heartbeat, input, snapshot, reliable, ping) sin
//! ningún esquema de juego hardcodeado. El juego se engancha vía callbacks
//! (`on_input`, `on_tick`, `state_provider`, `queue_event`).

mod host;
mod protocol;
mod types;

pub use host::{EventQueue, NetworkHost, NetworkHostHandle};
pub use protocol::{
    build_header, parse_header, FLAG_ORDERED, FLAG_RELIABLE, HEADER_SIZE, PACKET_ACK,
    PACKET_HANDSHAKE, PACKET_INPUT, PACKET_PING, PACKET_SNAPSHOT,
};
pub use types::{decode_input_payload, encode_input_payload, GameEvent, GameSnapshot, PlayerInput};
