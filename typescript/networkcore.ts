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
export const PACKET_DISCONNECT = 0x06;

export const FLAG_RELIABLE = 0x02;
export const FLAG_ORDERED = 0x04;

export const HEADER_SIZE = 9;

// --- Handshake (join) ---
//
// El core no sabe qué juego corre encima, así que Role viaja como un byte
// opaco: cada juego define sus propios valores y su propio significado (ver
// ejemplo-tacataca.ts), igual que ya pasa con StatePayload y GameEvent.eventType.

// Modos de handshake — quién crea una sala nueva vs. quién se une a una
// existente por código. Esto sí es genérico: cualquier juego con sesiones
// de partida necesita esta distinción.
export const HANDSHAKE_MODE_CREATE = 0x00;
export const HANDSHAKE_MODE_JOIN = 0x01;

// Motivos de rechazo del handshake, recibidos en un PACKET_DISCONNECT.
export const REASON_ROOM_NOT_FOUND = 0x01;
export const REASON_ROOM_FULL = 0x02;

// Lanzada por createRoom/joinRoom cuando el server rechaza el handshake
// (ej. código de sala inexistente, o sala llena) en vez de admitir.
export class HandshakeRejectedError extends Error {
  constructor(public readonly reason: number) {
    super(`handshake rechazado por el server (reason=${reason})`);
  }
}

// encodeJoinPayload: Mode(1) + Role(1) + RoomCodeLen(1) + RoomCode(N). Va
// después del header en un PACKET_HANDSHAKE enviado por el cliente.
// RoomCode se ignora en HANDSHAKE_MODE_CREATE (el server genera el código).
export function encodeJoinPayload(mode: number, role: number, roomCode: string): Buffer {
  const codeBuf = Buffer.from(roomCode, "ascii");
  const buf = Buffer.alloc(3 + codeBuf.length);
  buf[0] = mode;
  buf[1] = role;
  buf[2] = codeBuf.length;
  codeBuf.copy(buf, 3);
  return buf;
}

// decodeHandshakeAck: PlayerID(2) + RoomCodeLen(1) + RoomCode(N) — respuesta
// del server tanto a un Create como a un Join.
export function decodeHandshakeAck(payload: Buffer): { playerId: number; roomCode: string } | null {
  if (payload.length < 3) return null;
  const playerId = payload.readUInt16BE(0);
  const codeLen = payload[2];
  if (payload.length < 3 + codeLen) return null;
  const roomCode = payload.subarray(3, 3 + codeLen).toString("ascii");
  return { playerId, roomCode };
}

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
  roomCode = "";
  connected = false;
  lastPingMs = 0;
  latestSnapshot: GameSnapshot | null = null;

  onSnapshot: ((snapshot: GameSnapshot) => void) | null = null;
  onPong: ((ms: number) => void) | null = null;

  constructor() {
    this.socket = dgram.createSocket("udp4");
    this.socket.on("message", (msg) => this.handlePacket(msg));
  }

  // createRoom hace un handshake HANDSHAKE_MODE_CREATE: arranca una sala
  // nueva en el server y recibe su código (ej. para mostrarlo/QR-earlo y
  // que otros clientes se unan con joinRoom). role es opaco para el core —
  // ver ejemplo-tacataca.ts para su significado en este juego.
  createRoom(host: string, port: number, role: number, timeoutMs = 3000): Promise<{ playerId: number; roomCode: string }> {
    return this.handshake(host, port, HANDSHAKE_MODE_CREATE, role, "", timeoutMs);
  }

  // joinRoom se une a una sala existente por código (HANDSHAKE_MODE_JOIN).
  // Rechaza con HandshakeRejectedError si el código no existe o la sala
  // está llena (ver REASON_ROOM_NOT_FOUND / REASON_ROOM_FULL).
  joinRoom(host: string, port: number, role: number, roomCode: string, timeoutMs = 3000): Promise<{ playerId: number; roomCode: string }> {
    return this.handshake(host, port, HANDSHAKE_MODE_JOIN, role, roomCode, timeoutMs);
  }

  private handshake(
    host: string,
    port: number,
    mode: number,
    role: number,
    roomCode: string,
    timeoutMs: number,
  ): Promise<{ playerId: number; roomCode: string }> {
    this.serverAddr = { host, port };

    return new Promise((resolve, reject) => {
      const onMessage = (msg: Buffer) => {
        const header = parseHeader(msg);
        if (!header) return;

        if (header.type === PACKET_DISCONNECT) {
          const reason = msg.length > HEADER_SIZE ? msg[HEADER_SIZE] : 0;
          this.socket.removeListener("message", onMessage);
          clearTimeout(timer);
          reject(new HandshakeRejectedError(reason));
          return;
        }

        if (header.type !== PACKET_HANDSHAKE) return;
        const ack = decodeHandshakeAck(msg.subarray(HEADER_SIZE));
        if (!ack) return;

        this.playerId = ack.playerId;
        this.roomCode = ack.roomCode;
        this.connected = true;
        this.running = true;
        this.socket.removeListener("message", onMessage);
        clearTimeout(timer);
        resolve(ack);
      };

      const timer = setTimeout(() => {
        this.socket.removeListener("message", onMessage);
        reject(new Error("handshake: timeout esperando respuesta del server"));
      }, timeoutMs);

      this.socket.on("message", onMessage);

      const payload = encodeJoinPayload(mode, role, roomCode);
      const handshake = Buffer.concat([buildHeader(0, 0, PACKET_HANDSHAKE), payload]);
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
        // Un snapshot FLAG_RELIABLE es un evento puntual que el server
        // reintenta hasta confirmarse (ver NetworkHost.QueueReliableEvent
        // en Go) — hay que ackearlo, si no el server lo reenvía sin parar.
        // Convención de este protocolo: el ack lleva seq=ack=el seq del
        // paquete que se confirma, no un seq propio del cliente.
        if (header.flags & FLAG_RELIABLE) {
          this.sendAck(header.seq);
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

  private sendAck(ackedSeq: number): void {
    if (!this.serverAddr) return;
    const packet = buildHeader(ackedSeq, ackedSeq, PACKET_ACK);
    this.socket.send(packet, this.serverAddr.port, this.serverAddr.host);
  }
}
