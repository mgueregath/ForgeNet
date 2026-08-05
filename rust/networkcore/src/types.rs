use crate::protocol::HEADER_SIZE;

/// Genérico: el core no sabe qué significa `event_type` ni qué hay en
/// `data` — cada juego define su propia tabla de tipos y formato.
#[derive(Clone, Debug)]
pub struct GameEvent {
    pub player_id: u16,
    pub event_type: u8,
    pub data: Vec<u8>,
}

/// `tick`/`timestamp` son del core (sincronización); el estado del juego en
/// sí viaja como bytes opacos en `state_payload` — el core nunca lo
/// interpreta, solo lo transporta.
#[derive(Clone, Debug, Default)]
pub struct GameSnapshot {
    pub tick: u64,
    pub timestamp: i64,
    pub state_payload: Vec<u8>,
    pub events: Vec<GameEvent>,
}

impl GameSnapshot {
    /// Tick(8) + StatePayloadLen(4) + StatePayload(N) + EventCount(1) +
    /// Events(PlayerId(2)+EventType(1)+DataLen(1)+Data).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + self.state_payload.len() + 32);

        buf.extend_from_slice(&self.tick.to_be_bytes());
        buf.extend_from_slice(&(self.state_payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.state_payload);

        buf.push(self.events.len() as u8);
        for e in &self.events {
            buf.extend_from_slice(&e.player_id.to_be_bytes());
            buf.push(e.event_type);
            buf.push(e.data.len() as u8);
            buf.extend_from_slice(&e.data);
        }

        buf
    }

    /// Nunca hace panic: un payload corrupto o truncado devuelve `None` en
    /// vez de indexar fuera de rango.
    pub fn decode(payload: &[u8]) -> Option<GameSnapshot> {
        let mut offset = 0usize;
        if payload.len() < 8 + 4 + 1 {
            return None;
        }

        let tick = u64::from_be_bytes(payload[0..8].try_into().ok()?);
        offset += 8;

        let state_len = u32::from_be_bytes(payload[offset..offset + 4].try_into().ok()?) as usize;
        offset += 4;
        if offset + state_len > payload.len() {
            return None;
        }
        let state_payload = payload[offset..offset + state_len].to_vec();
        offset += state_len;

        if offset >= payload.len() {
            return None;
        }
        let event_count = payload[offset] as usize;
        offset += 1;

        let mut events = Vec::with_capacity(event_count);
        for _ in 0..event_count {
            if offset + 4 > payload.len() {
                return None;
            }
            let player_id = u16::from_be_bytes([payload[offset], payload[offset + 1]]);
            offset += 2;
            let event_type = payload[offset];
            offset += 1;
            let data_len = payload[offset] as usize;
            offset += 1;
            if offset + data_len > payload.len() {
                return None;
            }
            let data = payload[offset..offset + data_len].to_vec();
            offset += data_len;
            events.push(GameEvent { player_id, event_type, data });
        }

        Some(GameSnapshot {
            tick,
            timestamp: 0,
            state_payload,
            events,
        })
    }
}

/// Forma genérica (movimiento 2D + rotación + bitmask de acciones) — el
/// core no interpreta estos valores, solo los transporta y se los pasa al
/// juego vía `NetworkHost::on_input`.
#[derive(Clone, Copy, Debug)]
pub struct PlayerInput {
    pub player_id: u16,
    pub seq: u32,
    pub timestamp: i64,
    pub delta_x: i16,
    pub delta_y: i16,
    pub rotation: u16,
    pub actions: u32,
}

/// Payload de 10 bytes que va después del header en un `PACKET_INPUT`.
pub fn encode_input_payload(delta_x: i16, delta_y: i16, rotation: u16, actions: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10);
    buf.extend_from_slice(&delta_x.to_be_bytes());
    buf.extend_from_slice(&delta_y.to_be_bytes());
    buf.extend_from_slice(&rotation.to_be_bytes());
    buf.extend_from_slice(&actions.to_be_bytes());
    buf
}

pub fn decode_input_payload(payload: &[u8]) -> Option<(i16, i16, u16, u32)> {
    if payload.len() < 10 {
        return None;
    }
    let delta_x = i16::from_be_bytes([payload[0], payload[1]]);
    let delta_y = i16::from_be_bytes([payload[2], payload[3]]);
    let rotation = u16::from_be_bytes([payload[4], payload[5]]);
    let actions = u32::from_be_bytes([payload[6], payload[7], payload[8], payload[9]]);
    Some((delta_x, delta_y, rotation, actions))
}
