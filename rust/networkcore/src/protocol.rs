//! Constantes y helpers de bajo nivel del protocolo binario. Misma spec que
//! nucleo-multiplayer/go/networkcore y nucleo-multiplayer/csharp/NetworkCore
//! (mismo framing: header, handshake, input, ack, ping, y ahora también el
//! mismo formato de snapshot genérico con StatePayload opaco).

pub const PACKET_SNAPSHOT: u8 = 0x01;
pub const PACKET_INPUT: u8 = 0x02;
pub const PACKET_ACK: u8 = 0x03;
pub const PACKET_PING: u8 = 0x04;
pub const PACKET_HANDSHAKE: u8 = 0x05;
#[allow(dead_code)]
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
