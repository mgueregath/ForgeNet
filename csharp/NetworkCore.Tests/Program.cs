using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using NetworkCore;
using NetworkCore.Tests;

var stdout = new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true };
Console.SetOut(stdout);

int failures = 0;

void Check(string name, bool condition)
{
    if (condition)
    {
        Console.WriteLine($"  PASS  {name}");
    }
    else
    {
        Console.WriteLine($"  FAIL  {name}");
        failures++;
    }
}

// ---------------------------------------------------------------------
// Test 1: mecanismos GENÉRICOS de NetworkHost <-> NetworkClient — sin
// ningún esquema de juego de por medio. Prueba que StatePayload viaja como
// bytes opacos, que OnInput recibe los valores crudos sin interpretarlos,
// y que QueueEvent llega intacto al cliente.
// ---------------------------------------------------------------------
Console.WriteLine("== Test 1: NetworkHost <-> NetworkClient (mecanismos genéricos) ==");
{
    var host = new NetworkHost();
    byte[] fakeState = { 10, 20, 30, 40, 50 };
    host.StateProvider = () => fakeState;

    // Lista, no una sola variable: se mandan 2 inputs seguidos y ambos
    // llegan casi al mismo tiempo — guardar solo "el último" es una carrera.
    var receivedInputs = new System.Collections.Generic.List<PlayerInput>();
    var inputsLock = new object();
    host.OnInput += input => { lock (inputsLock) receivedInputs.Add(input); };

    // Server posee el socket y multiplexa salas; la factory devuelve
    // siempre el mismo host ya configurado arriba, así que el primer
    // handshake (CreateRoom) crea UNA sala sobre ese host — igual que hacían
    // los tests en Go (ver TestHandshakeInputSnapshotPing en host_test.go).
    var server = new Server(() => host, new ServerOptions());
    server.StartUdp(19999);
    Thread.Sleep(300);

    var client = new NetworkClient();

    GameSnapshot? snapshotWithEvent = null;
    var eventSeen = new ManualResetEventSlim(false);
    client.OnSnapshot += snap =>
    {
        if (snap.Events.Count > 0 && snapshotWithEvent == null)
        {
            snapshotWithEvent = snap;
            eventSeen.Set();
        }
    };

    // Role es opaco para el core — acá se usa un valor arbitrario, sin
    // ningún significado (mecanismo genérico, no hay esquema de juego).
    const byte genericRole = 1;
    bool connected = client.CreateRoom("127.0.0.1", 19999, genericRole);
    Check("handshake conecta", connected);
    Check("playerId asignado (>0)", client.PlayerId > 0);
    Check("código de sala asignado", client.RoomCode.Length > 0);

    client.SendInput(deltaX: 15, deltaY: -7, rotation: 90, actions: 0);
    // Actions=0x99 es un valor arbitrario — el core no debe interpretarlo,
    // solo transportarlo tal cual.
    client.SendInput(deltaX: 3, deltaY: 3, rotation: 45, actions: 0x99, reliable: true);

    var deadline = DateTime.UtcNow.AddSeconds(2);
    while (DateTime.UtcNow < deadline && receivedInputs.Count < 2) Thread.Sleep(20);
    Check($"OnInput recibió los 2 inputs (llegaron {receivedInputs.Count})", receivedInputs.Count == 2);
    lock (inputsLock)
    {
        bool firstOk = receivedInputs.Count > 0 && receivedInputs[0].DeltaX == 15 && receivedInputs[0].Rotation == 90;
        bool secondOk = receivedInputs.Count > 1 && receivedInputs[1].DeltaX == 3 && receivedInputs[1].Rotation == 45 && receivedInputs[1].Actions == 0x99;
        Check("DeltaX/Rotation del 1er input crudos sin interpretar (15/90)", firstOk);
        Check("DeltaX/Rotation/Actions del 2do input crudos sin interpretar (3/45/0x99)", secondOk);
    }

    deadline = DateTime.UtcNow.AddSeconds(2);
    while (DateTime.UtcNow < deadline && (client.LatestSnapshot == null || client.LatestSnapshot.StatePayload.Length == 0))
        Thread.Sleep(20);

    Check("snapshot recibido", client.LatestSnapshot != null);
    if (client.LatestSnapshot != null)
    {
        bool payloadMatches = client.LatestSnapshot.StatePayload.Length == fakeState.Length;
        if (payloadMatches)
        {
            for (int i = 0; i < fakeState.Length; i++)
                payloadMatches &= client.LatestSnapshot.StatePayload[i] == fakeState[i];
        }
        Check("StatePayload llega byte-a-byte igual al que provee el juego (core no lo toca)", payloadMatches);
    }

    host.QueueEvent(new GameEvent { PlayerId = client.PlayerId, EventType = 42, Data = new byte[] { 9, 9 } });
    eventSeen.Wait(TimeSpan.FromSeconds(2));
    Check("evento custom (QueueEvent) llega al cliente", snapshotWithEvent != null && snapshotWithEvent.Events.Count > 0);
    if (snapshotWithEvent != null && snapshotWithEvent.Events.Count > 0)
    {
        var e = snapshotWithEvent.Events[0];
        Check($"EventType arbitrario preservado (42 -> {e.EventType})", e.EventType == 42);
        Check("Data arbitraria preservada", e.Data.Length == 2 && e.Data[0] == 9 && e.Data[1] == 9);
    }

    client.SendPing();
    int? pong = null;
    client.OnPong += ms => pong = ms;
    deadline = DateTime.UtcNow.AddSeconds(2);
    while (DateTime.UtcNow < deadline && pong == null) Thread.Sleep(20);
    Check("ping/pong responde", pong != null);

    // QueueReliableEvent: manda un evento crítico ya mismo (fuera del
    // snapshot normal) y lo reintenta hasta que el cliente lo confirme — acá
    // solo validamos que el cliente lo recibe y confirma automáticamente
    // (ver NetworkClient.HandlePacket) y que la cola de retransmisión del
    // host se vacía.
    host.QueueReliableEvent(new GameEvent { PlayerId = client.PlayerId, EventType = 7, Data = new byte[] { 1 } });
    deadline = DateTime.UtcNow.AddSeconds(2);
    while (DateTime.UtcNow < deadline && host.PendingReliableCount(client.PlayerId) > 0) Thread.Sleep(20);
    Check("evento reliable confirmado (cola de retransmisión vacía)", host.PendingReliableCount(client.PlayerId) == 0);

    client.Disconnect();
    server.Stop();
}

// ---------------------------------------------------------------------
// Test 2: el heartbeat sobrevive más de 10s de actividad (regresión del
// bug ya corregido en server.go — acá validamos que el port a C# no lo
// reintroduce).
// ---------------------------------------------------------------------
Console.WriteLine();
Console.WriteLine("== Test 2: heartbeat sobrevive >10s con actividad ==");
{
    var host = new NetworkHost();
    var server = new Server(() => host, new ServerOptions());
    server.StartUdp(19998);
    Thread.Sleep(300);

    var client = new NetworkClient();
    client.CreateRoom("127.0.0.1", 19998, role: 1);

    var start = DateTime.UtcNow;
    while ((DateTime.UtcNow - start).TotalSeconds < 12)
    {
        client.SendInput(1, 1, 0, 0);
        Thread.Sleep(500);
    }

    Check("cliente sigue conectado en el host tras 12s", host.ConnectedPlayerCount == 1);

    client.Disconnect();
    server.Stop();
}

// ---------------------------------------------------------------------
// Test 3: cross-test contra el ejemplo taca-taca en Go REAL (nucleo-
// multiplayer/go/ejemplo-tacataca). Ahora que el core en Go también se
// generalizó (mismo StatePayload opaco que C#), esto valida paridad
// completa de protocolo entre los dos lenguajes — no solo el framing
// (handshake/ping) sino también el envelope de snapshot completo.
// ---------------------------------------------------------------------
Console.WriteLine();
Console.WriteLine("== Test 3: NetworkClient (C#) <-> ejemplo taca-taca (Go real) ==");
{
    string repoRoot = FindRepoRoot();
    string serversDir = Path.Combine(repoRoot, "go", "ejemplo-tacataca");

    var psi = new ProcessStartInfo
    {
        FileName = "go",
        Arguments = "run .",
        WorkingDirectory = serversDir,
        RedirectStandardOutput = true,
        RedirectStandardError = true,
        UseShellExecute = false,
    };

    Process? goServer = null;
    try
    {
        goServer = Process.Start(psi);
        Check("proceso Go (ejemplo taca-taca) arrancó", goServer != null);
        Thread.Sleep(2000); // esperar a que "go run" compile y levante el listener

        var client = new NetworkClient();
        // Contra el ejemplo taca-taca real: un rol distinto de
        // TacaTacaRoles.Paddle no recibe barra (ver taca-taca/main.go), así
        // que usamos Paddle acá para que el server arranque el gameplay
        // igual que con un mando real.
        bool connected = client.CreateRoom("127.0.0.1", 9999, TacaTacaRoles.Paddle);
        Check("handshake contra Go real", connected);
        Check("playerId asignado por Go", client.PlayerId > 0);
        Check("código de sala asignado por Go", client.RoomCode.Length > 0);

        client.SendPing();
        int? pong = null;
        client.OnPong += ms => pong = ms;
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (DateTime.UtcNow < deadline && pong == null) Thread.Sleep(20);
        Check("ping/pong contra Go real", pong != null);

        // Ambos cores son genéricos ahora: el envelope de snapshot
        // (Tick + StatePayloadLen + StatePayload + Events) debe decodificar
        // sin error, aunque el esquema DENTRO de StatePayload (Ball/Rod/Score)
        // sea específico de taca-taca y este cliente genérico no lo interprete.
        deadline = DateTime.UtcNow.AddSeconds(2);
        while (DateTime.UtcNow < deadline && client.LatestSnapshot == null) Thread.Sleep(20);
        Check("snapshot de Go decodificado con el mismo envelope genérico (TryDecode)", client.LatestSnapshot != null);
        if (client.LatestSnapshot != null)
            Check("StatePayload de Go llega no vacío (esquema taca-taca)", client.LatestSnapshot.StatePayload.Length > 0);

        client.Disconnect();
    }
    finally
    {
        if (goServer != null && !goServer.HasExited)
        {
            goServer.Kill(entireProcessTree: true);
            goServer.WaitForExit(2000);
        }
    }
}

// ---------------------------------------------------------------------
// Test 4: ejemplo end-to-end de un juego (taca-taca) construido ENCIMA del
// core genérico, sin tocar NetworkCore. Prueba que el esquema de estado
// específico del juego (pelota/barras/marcador) viaja correctamente a
// través del transporte genérico.
// ---------------------------------------------------------------------
Console.WriteLine();
Console.WriteLine("== Test 4: ejemplo taca-taca sobre el core genérico ==");
{
    var host = new NetworkHost();
    var game = new TacaTacaGame();
    game.AttachTo(host);
    var server = new Server(() => host, new ServerOptions());
    server.StartUdp(19997);
    Thread.Sleep(300);

    var client = new NetworkClient();
    bool connected = client.CreateRoom("127.0.0.1", 19997, TacaTacaRoles.Paddle);
    Check("handshake conecta (taca-taca)", connected);

    // Mover la barra propia.
    client.SendInput(deltaX: 25, deltaY: 0, rotation: 30, actions: 0);
    // "Meter un gol" (bit 0 de Actions, definido por TacaTacaGame, no por el core).
    client.SendInput(deltaX: 0, deltaY: 0, rotation: 30, actions: 0x01, reliable: true);

    TacaTacaState? decoded = null;
    var deadline = DateTime.UtcNow.AddSeconds(3);
    while (DateTime.UtcNow < deadline)
    {
        var snap = client.LatestSnapshot;
        if (snap != null && snap.StatePayload.Length > 0)
        {
            var candidate = TacaTacaState.Decode(snap.StatePayload);
            if (candidate.Rods.Count == 1 && candidate.Rods[0].Position == 25)
            {
                decoded = candidate;
                break;
            }
        }
        Thread.Sleep(50);
    }

    Check("estado de taca-taca decodificado desde StatePayload", decoded != null);
    if (decoded != null)
    {
        Check($"posición de barra aplicada por el juego (25 -> {decoded.Rods[0].Position})", decoded.Rods[0].Position == 25);
        Check($"rotación de barra aplicada (30 -> {decoded.Rods[0].Rotation})", decoded.Rods[0].Rotation == 30);
        Check($"gol contabilizado por el juego (1 -> {decoded.ScoreTeamA})", decoded.ScoreTeamA == 1);
    }

    client.Disconnect();
    server.Stop();
}

// ---------------------------------------------------------------------
// Test 5: reconexión por IP. Simula un cliente que se cae sin avisar (el
// socket se cierra pero nunca manda un disconnect explícito) y vuelve a
// conectarse desde la misma IP antes de que la sala lo purgue — debe
// reconocerse como el MISMO jugador (mismo PlayerId, Role original, no el
// de la reconexión) en vez de crear uno nuevo. Espejo de
// TestReconnectionByIPPreservesIdentity en host_test.go. HeartbeatTimeout
// es una constante interna de 10s (igual que en Go), así que este test
// tarda ~10s+.
// ---------------------------------------------------------------------
Console.WriteLine();
Console.WriteLine("== Test 5: reconexión por IP preserva identidad y Role original ==");
{
    var host = new NetworkHost();
    var playerIds = new System.Collections.Generic.List<ushort>();
    var reconnectedFlags = new System.Collections.Generic.List<bool>();
    var connLock = new object();
    host.OnPlayerConnected += (id, role, reconnected) =>
    {
        lock (connLock)
        {
            playerIds.Add(id);
            reconnectedFlags.Add(reconnected);
        }
    };

    var server = new Server(() => host, new ServerOptions());
    server.StartUdp(19996);
    Thread.Sleep(300);

    var first = new NetworkClient();
    bool firstConnected = first.CreateRoom("127.0.0.1", 19996, role: 7);
    Check("primer cliente conecta", firstConnected);
    ushort firstId = first.PlayerId;
    string roomCode = first.RoomCode;
    first.Disconnect(); // simula la caída: deja de mandar heartbeat, sin avisar

    Thread.Sleep(TimeSpan.FromSeconds(10.5)); // > HeartbeatTimeout (10s)

    var second = new NetworkClient();
    // role distinto a propósito: debe ignorarse, el Role original (7) gana.
    bool secondConnected = second.JoinRoom("127.0.0.1", 19996, role: 3, roomCode);
    Check("segundo cliente (reconexión) conecta", secondConnected);
    Check("reconexión preservó el PlayerId", second.PlayerId == firstId);

    lock (connLock)
    {
        Check($"2 llamadas a OnPlayerConnected (hubo {reconnectedFlags.Count})", reconnectedFlags.Count == 2);
        if (reconnectedFlags.Count == 2)
        {
            Check("la primera conexión no se marca reconnected", !reconnectedFlags[0]);
            Check("la segunda conexión se reconoce como reconnected", reconnectedFlags[1]);
        }
    }

    bool roleOk = host.GetClientRole(firstId, out byte roleAfter) && roleAfter == 7;
    Check($"Role tras reconexión = el original (7), no el de la reconexión (3): got {roleAfter}", roleOk);

    second.Disconnect();
    server.Stop();
}

// ---------------------------------------------------------------------
// Test 6: StartUdp(0) le pide al SO cualquier puerto libre; UDPPort() debe
// devolver el que asignó de verdad (no 0) y ese puerto debe ser usable.
// Espejo de TestUDPPortEphemeral en host_test.go.
// ---------------------------------------------------------------------
Console.WriteLine();
Console.WriteLine("== Test 6: StartUdp(0) + UDPPort() (puerto efímero) ==");
{
    var server = new Server(() => new NetworkHost(), new ServerOptions());
    server.StartUdp(0);
    Check("UDPPort() no es 0 tras StartUdp(0)", server.UDPPort() != 0);
    Thread.Sleep(200);

    var client = new NetworkClient();
    bool connected = client.CreateRoom("127.0.0.1", server.UDPPort(), role: 1);
    Check("handshake contra el puerto efímero funciona", connected);
    Check("playerId asignado (puerto efímero)", client.PlayerId > 0);

    client.Disconnect();
    server.Stop();
}

// ---------------------------------------------------------------------
// Test 7: modo "host embebido" — Server.CreateRoom() arranca una sala sin
// ningún cliente ni handshake de red, y un cliente normal se une después
// con JoinRoom, igual que a cualquier otra sala. Espejo de
// TestEmbeddedHostMode en client_test.go.
// ---------------------------------------------------------------------
Console.WriteLine();
Console.WriteLine("== Test 7: modo host embebido (Server.CreateRoom) ==");
{
    var fakeState = new byte[] { 9, 9, 9 };
    var server = new Server(() =>
    {
        var h = new NetworkHost();
        h.StateProvider = () => fakeState;
        return h;
    }, new ServerOptions());
    server.StartUdp(19995);
    Thread.Sleep(200);

    string roomCode = server.CreateRoom();
    Check("CreateRoom() devolvió código de sala", roomCode.Length > 0);

    var mando = new NetworkClient();
    bool joined = mando.JoinRoom("127.0.0.1", 19995, role: 1, roomCode);
    Check("JoinRoom contra la sala pre-creada", joined);
    Check("playerId asignado", mando.PlayerId > 0);
    Check("el mando terminó en la sala correcta", mando.RoomCode == roomCode);

    mando.Disconnect();
    server.Stop();
}

// ---------------------------------------------------------------------
// Test 8: una sala pre-creada (host embebido) que todavía no admitió a
// NADIE no debe destruirse nunca por el janitor, sin importar cuánto
// tiempo pase — el tablero puede quedarse esperando en el lobby
// indefinidamente. Solo empieza a correr el EmptyRoomGracePeriod una vez
// que la sala tuvo al menos un cliente (Room.HadPlayer). Espejo de
// TestEmbeddedHostRoomSurvivesEmptyGracePeriod en client_test.go.
// ---------------------------------------------------------------------
Console.WriteLine();
Console.WriteLine("== Test 8: sala pre-creada sobrevive sin nadie más allá de EmptyRoomGracePeriod ==");
{
    var server = new Server(() => new NetworkHost(), new ServerOptions { EmptyRoomGracePeriod = TimeSpan.FromMilliseconds(300) });
    server.StartUdp(19994);
    Thread.Sleep(200);

    string roomCode = server.CreateRoom();

    // Esperar bastante más que el (muy corto, a propósito) grace period —
    // si la sala vacía-sin-nadie-nunca fuera tratada igual que una vacía-
    // tras-tener-gente, el janitor ya la habría destruido acá.
    Thread.Sleep(1000);

    var client = new NetworkClient();
    bool joined = client.JoinRoom("127.0.0.1", 19994, role: 1, roomCode);
    Check("la sala pre-creada sobrevivió la espera sin ser destruida", joined);

    client.Disconnect();
    server.Stop();
}

Console.WriteLine();
if (failures == 0)
{
    Console.WriteLine("TODO OK");
    return 0;
}
else
{
    Console.WriteLine($"{failures} FALLO(S)");
    return 1;
}

static string FindRepoRoot()
{
    var dir = new DirectoryInfo(AppContext.BaseDirectory);
    while (dir != null && !Directory.Exists(Path.Combine(dir.FullName, "go", "networkcore")))
        dir = dir.Parent;

    if (dir == null)
        throw new DirectoryNotFoundException("No se encontró la raíz del repo (carpeta 'go/networkcore') subiendo desde " + AppContext.BaseDirectory);

    return dir.FullName;
}
