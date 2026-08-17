// Harness de validación de networkcore.ts — a diferencia de los otros
// lenguajes (que tienen host propio para self-test), TypeScript solo
// implementa el rol cliente en este proyecto. La validación entonces es
// un cross-test real contra el ejemplo taca-taca en Go (ya probado):
// confirma compatibilidad real de protocolo entre los dos lenguajes, no
// solo autoconsistencia.
//
// Este script NO levanta el servidor Go (spawnear subprocesos desde Node
// no es confiable en todos los entornos) — asume que ya está corriendo en
// 127.0.0.1:9999. Ver README para el comando exacto.
import { NetworkClient, HandshakeRejectedError } from "./networkcore";
import { decodeTacaTacaState, ROLE_PADDLE } from "./ejemplo-tacataca";

let failures = 0;

function check(name: string, condition: boolean): void {
  if (condition) {
    console.log(`  PASS  ${name}`);
  } else {
    console.log(`  FAIL  ${name}`);
    failures++;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  console.log("== Test: NetworkClient (TypeScript) <-> ejemplo taca-taca (Go real) ==");
  console.log("   (requiere que el servidor Go ya esté corriendo en :9999)");

  const client = new NetworkClient();
  let connected = true;
  try {
    // rolePaddle: este cliente se registra como mando, así el server crea
    // una barra para él (necesario para que el resto del test — posición,
    // rotación, gol — tenga algo que verificar). Crea una sala nueva
    // (HANDSHAKE_MODE_CREATE) porque este test no depende de ninguna sala
    // preexistente.
    await client.createRoom("127.0.0.1", 9999, ROLE_PADDLE);
  } catch (e) {
    connected = false;
    if (e instanceof HandshakeRejectedError) {
      console.log(`\nHandshake rechazado por el server, reason=${e.reason}`);
    } else {
      console.log(`\nNo se pudo conectar — ¿está corriendo el servidor Go en :9999? (${e})`);
    }
  }
  check("handshake contra ejemplo taca-taca (Go)", connected);
  if (!connected) {
    process.exit(1);
  }
  check("playerId asignado", client.playerId > 0);
  check("roomCode asignado", client.roomCode.length > 0);

  client.sendInput(25, 0, 30, 0);
  client.sendInput(0, 0, 30, 0x01, true); // "meter un gol"

  // Nota: el servidor Go es un proceso de larga duración que puede tener
  // jugadores de corridas anteriores todavía en su estado (no hay reinicio
  // entre corridas de este test) — buscar la barra propia por playerId en
  // vez de asumir que es la única conectada.
  let myRod: ReturnType<typeof decodeTacaTacaState>["rods"][number] | null = null;
  let lastDecoded: ReturnType<typeof decodeTacaTacaState> | null = null;
  const deadline = Date.now() + 3000;
  while (Date.now() < deadline) {
    const snap = client.latestSnapshot;
    if (snap && snap.statePayload.length > 0) {
      const candidate = decodeTacaTacaState(snap.statePayload);
      lastDecoded = candidate;
      const rod = candidate.rods.find((r) => r.playerId === client.playerId);
      if (rod && rod.position === 25) {
        myRod = rod;
        break;
      }
    }
    await sleep(50);
  }

  check("estado de taca-taca decodificado desde StatePayload", myRod !== null);
  if (myRod) {
    check(`posición de barra aplicada por Go (25 -> ${myRod.position})`, myRod.position === 25);
    check(`rotación de barra aplicada por Go (30 -> ${myRod.rotation})`, myRod.rotation === 30);
    check(`gol contabilizado por Go (>=1 -> ${lastDecoded?.scoreTeamA})`, (lastDecoded?.scoreTeamA ?? 0) >= 1);
  }

  let pongMs: number | null = null;
  client.onPong = (ms) => (pongMs = ms);
  client.sendPing();
  const pingDeadline = Date.now() + 2000;
  while (Date.now() < pingDeadline && pongMs === null) {
    await sleep(20);
  }
  check("ping/pong contra Go real", pongMs !== null);

  client.disconnect();

  console.log();
  if (failures === 0) {
    console.log("TODO OK");
    process.exit(0);
  } else {
    console.log(`${failures} FALLO(S)`);
    process.exit(1);
  }
}

main();
