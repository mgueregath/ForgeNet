//! Constantes y helpers de bajo nivel del protocolo binario. Misma spec que
//! nucleo-multiplayer/go/networkcore y nucleo-multiplayer/csharp/NetworkCore
//! (mismo framing: header, handshake, input, ack, ping, y ahora también el
//! mismo formato de snapshot genérico con StatePayload opaco).

pub const PACKET_SNAPSHOT: u8 = 0x01;
pub const PACKET_INPUT: u8 = 0x02;
pub const PACKET_ACK: u8 = 0x03;
pub const PACKET_PING: u8 = 0x04;
pub const PACKET_HANDSHAKE: u8 = 0x05;
pub const PACKET_DISCONNECT: u8 = 0x06;

#[allow(dead_code)]
pub const FLAG_COMPRESSED: u8 = 0x01;
pub const FLAG_RELIABLE: u8 = 0x02;
#[allow(dead_code)]
pub const FLAG_ORDERED: u8 = 0x04;

/// seq(4) + ack(4) + typeAndFlags(1)
pub const HEADER_SIZE: usize = 9;

pub fn build_header(seq: u32, ack: u32, packet_type: u8, flags: u8) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER_SIZE);
    buf.extend_from_slice(&seq.to_be_bytes());
    buf.extend_from_slice(&ack.to_be_bytes());
    buf.push((packet_type & 0x0F) | ((flags & 0x0F) << 4));
    buf
}

pub fn parse_header(data: &[u8]) -> Option<(u32, u32, u8, u8)> {
    if data.len() < HEADER_SIZE {
        return None;
    }
    let seq = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let ack = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    let type_and_flags = data[8];
    Some((seq, ack, type_and_flags & 0x0F, (type_and_flags >> 4) & 0x0F))
}

// --- Payload del handshake (join) ---
//
// El core no sabe qué juego corre encima, así que Role viaja como un byte
// opaco: cada juego define sus propios valores y su propio significado (ej.
// taca-taca usa un valor para "barra" y otro para "tablero" — ver su
// ejemplo), igual que ya pasa con StatePayload y GameEvent::event_type.

/// Modos de handshake — quién crea una sala nueva vs. quién se une a una
/// existente por código. Esto sí es genérico: cualquier juego con sesiones
/// de partida necesita esta distinción.
pub const HANDSHAKE_MODE_CREATE: u8 = 0x00;
pub const HANDSHAKE_MODE_JOIN: u8 = 0x01;

/// Motivos de rechazo del handshake, enviados en un PACKET_DISCONNECT.
pub const REASON_ROOM_NOT_FOUND: u8 = 0x01;
pub const REASON_ROOM_FULL: u8 = 0x02;

/// Mode(1) + Role(1) + RoomCodeLen(1) + RoomCode(N). Va después del header
/// en un PACKET_HANDSHAKE mandado por el cliente. RoomCode se ignora en
/// HANDSHAKE_MODE_CREATE (el server genera el código).
pub fn encode_join_payload(mode: u8, role: u8, room_code: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(3 + room_code.len());
    buf.push(mode);
    buf.push(role);
    buf.push(room_code.len() as u8);
    buf.extend_from_slice(room_code.as_bytes());
    buf
}

pub fn decode_join_payload(payload: &[u8]) -> Option<(u8, u8, String)> {
    if payload.len() < 3 {
        return None;
    }
    let code_len = payload[2] as usize;
    if payload.len() < 3 + code_len {
        return None;
    }
    let room_code = String::from_utf8_lossy(&payload[3..3 + code_len]).into_owned();
    Some((payload[0], payload[1], room_code))
}

/// encode_handshake_ack: PlayerID(2) + RoomCodeLen(1) + RoomCode(N). Va
/// después del header en la respuesta del server — tanto quien crea la sala
/// como quien se une reciben el código, para poder compartirlo (ej. un QR).
pub fn encode_handshake_ack(player_id: u16, room_code: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(3 + room_code.len());
    buf.extend_from_slice(&player_id.to_be_bytes());
    buf.push(room_code.len() as u8);
    buf.extend_from_slice(room_code.as_bytes());
    buf
}
