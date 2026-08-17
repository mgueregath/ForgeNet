//! Port a Rust de nucleo-multiplayer/csharp/NetworkCore: transporte UDP
//! genérico (handshake, heartbeat, input, snapshot, reliable, ping) sin
//! ningún esquema de juego hardcodeado. El juego se engancha vía callbacks
//! (`on_input`, `on_tick`, `state_provider`, `queue_event`).

mod host;
mod protocol;
mod server;
mod types;

pub use host::{EventQueue, NetworkHost, NetworkHostHandle};
pub use protocol::{
    build_header, decode_join_payload, encode_handshake_ack, encode_join_payload, parse_header,
    FLAG_ORDERED, FLAG_RELIABLE, HANDSHAKE_MODE_CREATE, HANDSHAKE_MODE_JOIN, HEADER_SIZE,
    PACKET_ACK, PACKET_DISCONNECT, PACKET_HANDSHAKE, PACKET_INPUT, PACKET_PING, PACKET_SNAPSHOT,
    REASON_ROOM_FULL, REASON_ROOM_NOT_FOUND,
};
pub use server::{RoomFactory, Server, ServerOptions};
pub use types::{decode_input_payload, encode_input_payload, GameEvent, GameSnapshot, PlayerInput};
