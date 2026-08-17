// Decodificador del esquema de estado de taca-taca (Ball/Rod/Score) para
// el lado navegador — análogo a nucleo-multiplayer/typescript/ejemplo-tacataca.ts,
// pero con Uint8Array/DataView en vez de Buffer (Buffer no existe en el
// navegador).

// Roles de taca-taca — opacos para el core (viajan como el byte Role del
// handshake, ver networkcore.ts), definidos y usados solo acá. Deben
// coincidir con roleBoard/rolePaddle en go/ejemplo-tacataca/main.go.
export const ROLE_BOARD = 0x01;
export const ROLE_PADDLE = 0x02;

export interface RodState {
  playerId: number;
  position: number;
  rotation: number;
}

export interface TacaTacaState {
  ballX: number;
  ballY: number;
  scoreTeamA: number;
  scoreTeamB: number;
  rods: RodState[];
}

// Formato: BallX(4) + BallY(4) + ScoreA(1) + ScoreB(1) + RodCount(1) + por
// barra: PlayerID(2) + Position(4) + Rotation(2) — definido por el juego,
// no por el core (vive dentro de StatePayload, que el core trata como
// bytes opacos).
export function decodeTacaTacaState(data: Uint8Array): TacaTacaState {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const ballX = view.getInt32(0, false);
  const ballY = view.getInt32(4, false);
  const scoreTeamA = data[8];
  const scoreTeamB = data[9];
  const rodCount = data[10];

  const rods: RodState[] = [];
  let offset = 11;
  for (let i = 0; i < rodCount; i++) {
    const playerId = view.getUint16(offset, false);
    const position = view.getInt32(offset + 2, false);
    const rotation = view.getUint16(offset + 6, false);
    rods.push({ playerId, position, rotation });
    offset += 8;
  }

  return { ballX, ballY, scoreTeamA, scoreTeamB, rods };
}
