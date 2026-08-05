// networkcore.ts — Port a TypeScript de nucleo-multiplayer/csharp/NetworkCore.
//
// Transporte UDP genérico (handshake, ping, input, snapshot, reliable) sin
// ningún esquema de juego hardcodeado. Misma spec de protocolo binario que
// los cores en Go/Rust/Python/C# — comparten framing y el formato de
// snapshot genérico (Tick + StatePayload opaco + Events).
//
// Solo implementa el rol "cliente" (NetworkClient) — igual que en el
// prototipo original, TypeScript nunca tuvo un servidor en este proyecto.
import * as dgram from "dgram";

// --- Protocolo ---

export const PACKET_SNAPSHOT = 0x01;
export const PACKET_INPUT = 0x02;
export const PACKET_ACK = 0x03;
export const PACKET_PING = 0x04;
export const PACKET_HANDSHAKE = 0x05;

export const FLAG_RELIABLE = 0x02;
export const FLAG_ORDERED = 0x04;

export const HEADER_SIZE = 9;

export function buildHeader(seq: number, ack: number, type: number, flags = 0): Buffer {
  const buf = Buffer.alloc(HEADER_SIZE);
  buf.writeUInt32BE(seq, 0);
  buf.writeUInt32BE(ack, 4);
  buf[8] = (type & 0x0f) | ((flags & 0x0f) << 4);
  return buf;
}

export interface Header {
  seq: number;
  ack: number;
  type: number;
  flags: number;
}

export function parseHeader(data: Buffer): Header | null {
  if (data.length < HEADER_SIZE) return null;
  const typeAndFlags = data[8];
  return {
    seq: data.readUInt32BE(0),
    ack: data.readUInt32BE(4),
    type: typeAndFlags & 0x0f,
    flags: (typeAndFlags >> 4) & 0x0f,
  };
}

// --- Tipos genéricos ---

// El core no sabe qué significa eventType ni qué hay en data — cada juego
// define su propia tabla de tipos y formato de payload.
export interface GameEvent {
  playerId: number;
  eventType: number;
  data: Buffer;
}

// tick/timestamp son del core (sincronización); el estado del juego en sí
// viaja como bytes opacos en statePayload — el core nunca lo interpreta,
// solo lo transporta.
export interface GameSnapshot {
  tick: bigint;
  timestamp: number;
  statePayload: Buffer;
  events: GameEvent[];
}

// Nunca lanza: un payload corrupto o truncado devuelve null en vez de
// indexar fuera de rango.
export function decodeSnapshot(payload: Buffer): GameSnapshot | null {
  try {
    let offset = 0;
    if (payload.length < 8 + 4 + 1) return null;

    const tick = payload.readBigUInt64BE(offset);
    offset += 8;

    const stateLen = payload.readUInt32BE(offset);
    offset += 4;
    if (offset + stateLen > payload.length) return null;
    const statePayload = payload.subarray(offset, offset + stateLen);
    offset += stateLen;

    if (offset >= payload.length) return null;
    const eventCount = payload[offset];
    offset += 1;

    const events: GameEvent[] = [];
    for (let i = 0; i < eventCount; i++) {
      if (offset + 4 > payload.length) return null;
      const playerId = payload.readUInt16BE(offset);
      offset += 2;
      const eventType = payload[offset];
      offset += 1;
      const dataLen = payload[offset];
      offset += 1;
      if (offset + dataLen > payload.length) return null;
      const data = payload.subarray(offset, offset + dataLen);
      offset += dataLen;
      events.push({ playerId, eventType, data });
    }

    return { tick, timestamp: 0, statePayload, events };
  } catch {
    return null;
  }
}

// Forma genérica (movimiento 2D + rotación + bitmask de acciones) — el core
// no interpreta estos valores, solo los transporta.
export interface PlayerInput {
  deltaX: number;
  deltaY: number;
  rotation: number;
  actions: number;
}

export function encodeInputPayload(input: PlayerInput): Buffer {
  const buf = Buffer.alloc(10);
  buf.writeInt16BE(Math.round(input.deltaX), 0);
  buf.writeInt16BE(Math.round(input.deltaY), 2);
  buf.writeUInt16BE(Math.round(input.rotation), 4);
  buf.writeUInt32BE(input.actions >>> 0, 6);
  return buf;
}

// --- NetworkClient (rol cliente) ---

export class NetworkClient {
  private socket: dgram.Socket;
  private serverAddr: { host: string; port: number } | null = null;
  private sequence = 0;
  private pendingPings = new Map<number, number>();
  private running = false;

  playerId = 0;
  connected = false;
  lastPingMs = 0;
  latestSnapshot: GameSnapshot | null = null;

  onSnapshot: ((snapshot: GameSnapshot) => void) | null = null;
  onPong: ((ms: number) => void) | null = null;

  constructor() {
    this.socket = dgram.createSocket("udp4");
    this.socket.on("message", (msg) => this.handlePacket(msg));
  }

  connect(host: string, port: number, timeoutMs = 3000): Promise<boolean> {
    this.serverAddr = { host, port };

    return new Promise((resolve) => {
      const onMessage = (msg: Buffer) => {
        const header = parseHeader(msg);
        if (!header || header.type !== PACKET_HANDSHAKE || msg.length < 11) return;

        this.playerId = msg.readUInt16BE(9);
        this.connected = true;
        this.running = true;
        this.socket.removeListener("message", onMessage);
        clearTimeout(timer);
        resolve(true);
      };

      const timer = setTimeout(() => {
        this.socket.removeListener("message", onMessage);
        resolve(false);
      }, timeoutMs);

      this.socket.on("message", onMessage);

      const handshake = buildHeader(0, 0, PACKET_HANDSHAKE);
      this.socket.send(handshake, port, host);
    });
  }

  disconnect(): void {
    this.running = false;
    this.connected = false;
    this.socket.close();
  }

  sendInput(deltaX: number, deltaY: number, rotation: number, actions: number, reliable = false): void {
    if (!this.connected || !this.serverAddr) return;

    this.sequence++;
    const flags = reliable ? FLAG_RELIABLE : FLAG_ORDERED;
    const header = buildHeader(this.sequence, 0, PACKET_INPUT, flags);
    const payload = encodeInputPayload({ deltaX, deltaY, rotation, actions });
    const packet = Buffer.concat([header, payload]);

    this.socket.send(packet, this.serverAddr.port, this.serverAddr.host);
  }

  sendPing(): void {
    if (!this.connected || !this.serverAddr) return;

    this.sequence++;
    const header = buildHeader(this.sequence, 0, PACKET_PING);
    const payload = Buffer.alloc(8);
    payload.writeBigInt64BE(BigInt(Date.now()), 0);
    const packet = Buffer.concat([header, payload]);

    this.pendingPings.set(this.sequence, Date.now());
    this.socket.send(packet, this.serverAddr.port, this.serverAddr.host);
  }

  private handlePacket(data: Buffer): void {
    if (!this.running) return;
    const header = parseHeader(data);
    if (!header) return;

    const payload = data.subarray(HEADER_SIZE);

    switch (header.type) {
      case PACKET_SNAPSHOT: {
        const snapshot = decodeSnapshot(payload);
        if (snapshot) {
          this.latestSnapshot = snapshot;
          this.onSnapshot?.(snapshot);
        }
        break;
      }
      case PACKET_PING: {
        const sentAt = this.pendingPings.get(header.seq);
        if (sentAt !== undefined) {
          this.pendingPings.delete(header.seq);
          this.lastPingMs = Date.now() - sentAt;
          this.onPong?.(this.lastPingMs);
        }
        break;
      }
    }
  }
}
