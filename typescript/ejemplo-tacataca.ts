// Ejemplo de esquema de juego para el lado cliente — decodifica el
// StatePayload de taca-taca (Ball/Rod/Score) sin que networkcore.ts sepa
// nada de esto. Análogo (solo lectura, porque TS acá es cliente) a
// TacaTacaState en los ejemplos de Go/Rust/Python/C#.
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
export function decodeTacaTacaState(data: Buffer): TacaTacaState {
  const ballX = data.readInt32BE(0);
  const ballY = data.readInt32BE(4);
  const scoreTeamA = data[8];
  const scoreTeamB = data[9];
  const rodCount = data[10];

  const rods: RodState[] = [];
  let offset = 11;
  for (let i = 0; i < rodCount; i++) {
    const playerId = data.readUInt16BE(offset);
    const position = data.readInt32BE(offset + 2);
    const rotation = data.readUInt16BE(offset + 6);
    rods.push({ playerId, position, rotation });
    offset += 8;
  }

  return { ballX, ballY, scoreTeamA, scoreTeamB, rods };
}
