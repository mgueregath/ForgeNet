// networkcore.ts — port a navegador (WebTransport) de
// nucleo-multiplayer/*/networkcore. Misma spec de protocolo binario que
// los cores en Go/Rust/Python/C++/C#/TypeScript-Node — comparten framing y
// el envelope de snapshot genérico (Tick + StatePayload opaco + Events).
//
// A diferencia de typescript/networkcore.ts (que usa el módulo `dgram` de
// Node, solo disponible fuera del navegador), este archivo corre 100% en
// el navegador: usa la API WebTransport (QUIC) en vez de UDP crudo, porque
// un navegador no puede abrir sockets UDP. Cada datagrama WebTransport
// mapea 1:1 a un paquete de nuestro protocolo — ver
// nucleo-multiplayer/go/networkcore/transport_webtransport.go, el lado
// servidor de este puente.

export const PACKET_SNAPSHOT = 0x01;
export const PACKET_INPUT = 0x02;
export const PACKET_ACK = 0x03;
export const PACKET_PING = 0x04;
export const PACKET_HANDSHAKE = 0x05;

export const FLAG_RELIABLE = 0x02;
export const FLAG_ORDERED = 0x04;

export const HEADER_SIZE = 9;

export function buildHeader(seq: number, ack: number, type: number, flags = 0): Uint8Array {
  const buf = new Uint8Array(HEADER_SIZE);
  const view = new DataView(buf.buffer);
  view.setUint32(0, seq, false);
  view.setUint32(4, ack, false);
  buf[8] = (type & 0x0f) | ((flags & 0x0f) << 4);
  return buf;
}

export interface Header {
  seq: number;
  ack: number;
  type: number;
  flags: number;
}

export function parseHeader(data: Uint8Array): Header | null {
  if (data.length < HEADER_SIZE) return null;
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const typeAndFlags = data[8];
  return {
    seq: view.getUint32(0, false),
    ack: view.getUint32(4, false),
    type: typeAndFlags & 0x0f,
    flags: (typeAndFlags >> 4) & 0x0f,
  };
}

// --- Tipos genéricos ---

export interface GameEvent {
  playerId: number;
  eventType: number;
  data: Uint8Array;
}

export interface GameSnapshot {
  tick: bigint;
  timestamp: number;
  statePayload: Uint8Array;
  events: GameEvent[];
}

// Nunca lanza: un payload corrupto o truncado devuelve null en vez de
// indexar fuera de rango.
export function decodeSnapshot(payload: Uint8Array): GameSnapshot | null {
  try {
    const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
    let offset = 0;
    if (payload.length < 8 + 4 + 1) return null;

    const tick = view.getBigUint64(offset, false);
    offset += 8;

    const stateLen = view.getUint32(offset, false);
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
      const playerId = view.getUint16(offset, false);
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

export interface PlayerInput {
  deltaX: number;
  deltaY: number;
  rotation: number;
  actions: number;
}

export function encodeInputPayload(input: PlayerInput): Uint8Array {
  const buf = new Uint8Array(10);
  const view = new DataView(buf.buffer);
  view.setInt16(0, Math.round(input.deltaX), false);
  view.setInt16(2, Math.round(input.deltaY), false);
  view.setUint16(4, Math.round(input.rotation), false);
  view.setUint32(6, input.actions >>> 0, false);
  return buf;
}

function base64ToBytes(base64: string): Uint8Array<ArrayBuffer> {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

export interface ConnectOptions {
  // Hash SHA-256 (base64, sin padding) del certificado self-signed de
  // desarrollo del servidor — ver GenerateDevCertificate en el core Go.
  // NO se usa en producción (ahí el certificado es de una CA real y el
  // navegador lo valida solo).
  certificateHashBase64?: string;
}

// --- NetworkClient (rol cliente, navegador) ---

export class NetworkClient {
  private transport: WebTransport | null = null;
  private writer: WritableStreamDefaultWriter<Uint8Array> | null = null;
  private sequence = 0;
  private pendingPings = new Map<number, number>();
  private handshakeResolve: ((playerId: number | null) => void) | null = null;

  playerId = 0;
  connected = false;
  lastPingMs = 0;
  latestSnapshot: GameSnapshot | null = null;

  onSnapshot: ((snapshot: GameSnapshot) => void) | null = null;
  onPong: ((ms: number) => void) | null = null;

  async connect(url: string, opts: ConnectOptions = {}, timeoutMs = 5000): Promise<boolean> {
    const wtOptions: WebTransportOptions = {};
    if (opts.certificateHashBase64) {
      wtOptions.serverCertificateHashes = [
        { algorithm: "sha-256", value: base64ToBytes(opts.certificateHashBase64) },
      ];
    }

    this.transport = new WebTransport(url, wtOptions);
    await this.transport.ready;

    this.writer = this.transport.datagrams.writable.getWriter();
    this.readLoop(); // corre en background, no se espera

    const handshakeDone = new Promise<number | null>((resolve) => {
      this.handshakeResolve = resolve;
    });
    const timeout = new Promise<null>((resolve) => setTimeout(() => resolve(null), timeoutMs));

    await this.sendRaw(buildHeader(this.sequence, 0, PACKET_HANDSHAKE));

    const playerId = await Promise.race([handshakeDone, timeout]);
    this.handshakeResolve = null;

    if (playerId === null) return false;

    this.playerId = playerId;
    this.connected = true;
    return true;
  }

  disconnect(): void {
    this.connected = false;
    this.transport?.close();
  }

  async sendInput(deltaX: number, deltaY: number, rotation: number, actions: number, reliable = false): Promise<void> {
    if (!this.connected) return;
    this.sequence++;
    const flags = reliable ? FLAG_RELIABLE : FLAG_ORDERED;
    const header = buildHeader(this.sequence, 0, PACKET_INPUT, flags);
    const payload = encodeInputPayload({ deltaX, deltaY, rotation, actions });
    await this.sendRaw(concat(header, payload));
  }

  async sendPing(): Promise<void> {
    if (!this.connected) return;
    this.sequence++;
    const header = buildHeader(this.sequence, 0, PACKET_PING);
    const payload = new Uint8Array(8);
    new DataView(payload.buffer).setBigInt64(0, BigInt(Date.now()), false);
    this.pendingPings.set(this.sequence, Date.now());
    await this.sendRaw(concat(header, payload));
  }

  private async sendRaw(data: Uint8Array): Promise<void> {
    if (!this.writer) return;
    await this.writer.ready;
    await this.writer.write(data);
  }

  private async readLoop(): Promise<void> {
    if (!this.transport) return;
    const reader = this.transport.datagrams.readable.getReader();
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        if (value) this.handlePacket(value);
      }
    } catch {
      // sesión cerrada — nada que hacer, connect()/sendX ya no deberían usarse
    }
  }

  private handlePacket(data: Uint8Array): void {
    const header = parseHeader(data);
    if (!header) return;
    const payload = data.subarray(HEADER_SIZE);

    switch (header.type) {
      case PACKET_HANDSHAKE: {
        if (payload.length < 2) return;
        const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
        const playerId = view.getUint16(0, false);
        this.handshakeResolve?.(playerId);
        break;
      }
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

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}
