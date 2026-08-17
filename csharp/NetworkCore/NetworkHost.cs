using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace NetworkCore
{
    // Peer es la identidad de un cliente a nivel transporte — hoy solo UDP
    // crudo (ver UdpPeer en Server.cs), pero la abstracción existe para que
    // NetworkHost nunca dependa de un socket concreto: cualquier cosa que
    // pueda mandar bytes y tenga una clave estable (Key()) sirve. Espejo de
    // la interfaz Peer en go/networkcore/host.go.
    //
    // IP(): el protocolo no lleva ningún token de sesión, así que
    // AdmitPlayer usa la IP (sin el puerto, que cambia en cada reconexión —
    // nuevo socket UDP) como heurística para reconocer "es el mismo
    // dispositivo reconectándose" — ver el comentario grande ahí.
    public interface IPeer
    {
        void Send(byte[] data);
        string Key();
        string IP();
    }

    // Rol "servidor" del protocolo. Es una clase C# plana (sin MonoBehaviour,
    // sin dependencias de UnityEngine) para poder correr en dos contextos:
    //  - Embebido dentro de un cliente Unity que también renderiza (modalidad
    //    Host Embebido, ej. taca-taca: un teléfono es la mesa).
    //  - Un futuro build headless de Unity ("Dedicated Server" build target)
    //    si algún juego necesitara la topología clásica en C# en vez de Go.
    //
    // Genérico por diseño: NetworkHost solo maneja sesión/conexión (handshake
    // de admisión, heartbeat, reliable, ping, tick loop) para UNA sala/
    // partida — NO sabe nada sobre el juego en sí (no hay "Health", "Ammo",
    // "Ball" ni nada específico acá), y tampoco sabe de qué transporte vienen
    // los paquetes ni a qué sala pertenece: eso lo decide Server, que crea
    // una instancia de NetworkHost por sala y le rutea sus paquetes (ver
    // Server.cs). El juego que use NetworkHost se engancha vía OnInput/
    // OnTick/StateProvider.
    public class ClientConnection
    {
        public ushort PlayerId;
        public IPeer Peer = null!;
        // Role es opaco: el core lo guarda y lo expone (ver
        // NetworkHost.GetClientRole) pero nunca lo interpreta — cada juego
        // define sus propios valores y qué significan.
        public byte Role;
        public uint LastSeq;
        public DateTime LastHeartbeat;
        public int Ping;
        public bool Connected = true;
        public readonly List<ReliableMessage> ReliableQueue = new List<ReliableMessage>();
        public readonly object QueueLock = new object();
    }

    public class ReliableMessage
    {
        public uint Seq;
        public byte[] Data = Array.Empty<byte>();
        public DateTime Sent;
        public byte Retries;
    }

    public class NetworkHost : IDisposable
    {
        private static readonly TimeSpan TickRate = TimeSpan.FromMilliseconds(16); // 60Hz, igual que server.go
        private static readonly TimeSpan RetryInterval = TimeSpan.FromMilliseconds(100);
        private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan HeartbeatTimeout = TimeSpan.FromSeconds(10);
        private const int MaxRetries = 3;

        private volatile bool _running;
        private int _started; // 0/1 usado con Interlocked, equivalente a sync.Once en host.go

        private readonly Dictionary<string, ClientConnection> _clientsByKey = new(); // key = Peer.Key()
        private readonly Dictionary<ushort, ClientConnection> _clientsById = new();
        private readonly object _clientsLock = new();

        private ulong _tick;
        private readonly List<GameEvent> _pendingEvents = new();
        private readonly object _eventsLock = new();

        private ushort _nextPlayerId = 1;
        private uint _sequenceCounter;
        private readonly object _seqLock = new();

        private readonly ConcurrentQueue<PlayerInput> _inputQueue = new();
        private const int InputQueueMax = 1024;

        // --- Hooks genéricos: acá es donde el juego se engancha ---

        // Se dispara cuando un jugador nuevo termina el handshake (ver
        // AdmitPlayer). Role es opaco para el core — cada juego define sus
        // propios valores y qué hacer con ellos (ej. no crear una "barra" de
        // juego para el rol que representa al tablero). reconnected=true si
        // esto fue una reconexión reconocida por IP (ver AdmitPlayer) — el
        // juego puede usarlo para NO pisar el estado que ya tenía ese
        // jugador con uno vacío.
        public event Action<ushort, byte, bool>? OnPlayerConnected;
        // Se dispara cuando un jugador se desconecta (timeout de heartbeat).
        public event Action<ushort>? OnPlayerDisconnected;
        // Se dispara para cada input recibido, en orden, dentro del tick loop.
        // El juego decide qué hacer con DeltaX/DeltaY/Rotation/Actions.
        public event Action<PlayerInput>? OnInput;
        // Se dispara una vez por tick, antes de pedir el snapshot — el juego
        // actualiza acá su simulación (física, IA, lo que sea).
        public event Action<ulong>? OnTick;
        // El juego provee los bytes de su estado actual para el próximo
        // snapshot. Si es null, se manda un StatePayload vacío.
        public Func<byte[]>? StateProvider;

        // Start arranca el tick loop, la retransmisión de confiables y el
        // heartbeat de esta sala. La llama Server exactamente una vez, al
        // crear la sala (ver Server.CreateRoom) — el chequeo con
        // Interlocked es solo una salvaguarda, análogo a sync.Once en
        // host.go. NetworkHost ya NO liga ningún socket: eso es
        // responsabilidad de Server, que es quien lo posee.
        internal void Start()
        {
            if (Interlocked.Exchange(ref _started, 1) != 0) return;

            _running = true;
            new Thread(GameLoop) { IsBackground = true, Name = "NetworkHost.Tick" }.Start();
            new Thread(SendLoop) { IsBackground = true, Name = "NetworkHost.Send" }.Start();
            new Thread(HeartbeatLoop) { IsBackground = true, Name = "NetworkHost.Heartbeat" }.Start();
        }

        // Stop detiene los loops de fondo de esta sala (tick, retransmisión,
        // heartbeat). No toca ningún transporte — eso es responsabilidad de
        // Server, que es quien los posee.
        public void Stop()
        {
            _running = false;
        }

        public void Dispose() => Stop();

        // El juego puede encolar eventos propios (ej. GOAL) para que salgan
        // en el próximo snapshot. Entrega "mejor esfuerzo": si el paquete se
        // pierde, el evento no vuelve a mandarse. Para eventos que el juego
        // no puede permitirse perder ver QueueReliableEvent.
        public void QueueEvent(GameEvent evt)
        {
            lock (_eventsLock) _pendingEvents.Add(evt);
        }

        // QueueReliableEvent hace lo mismo que QueueEvent (el evento sale en
        // el próximo snapshot de todos modos) pero además lo manda ya mismo,
        // a cada cliente conectado, como un paquete aparte marcado
        // PacketFlag.Reliable — el core lo reintenta (mismo mecanismo que ya
        // existía para reliable input, ver SendLoop) hasta que el cliente lo
        // confirma con PacketAck o se agotan los reintentos. Pensado para
        // eventos que el juego no puede permitirse perder en una red con
        // pérdida real (ej. GOAL en un server público).
        //
        // La entrega es "al menos una vez", no "exactamente una vez": el
        // cliente puede recibir el mismo evento acá y de nuevo en el
        // snapshot normal — el juego debe tratarlo como una notificación
        // idempotente (ej. "hubo un gol, resincronizá con StatePayload"), no
        // como algo que suma un contador él mismo.
        public void QueueReliableEvent(GameEvent evt)
        {
            QueueEvent(evt);

            var snap = new GameSnapshot
            {
                Tick = _tick,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Events = new List<GameEvent> { evt }
            };
            var payload = snap.Encode();

            List<ClientConnection> clients;
            lock (_clientsLock) clients = new List<ClientConnection>(_clientsById.Values);

            foreach (var client in clients)
            {
                uint seq = NextSeqNumber();
                var header = PacketHeader.Build(seq, 0, PacketType.Snapshot, PacketFlag.Reliable);
                var packet = Concat(header, payload);

                lock (client.QueueLock)
                    client.ReliableQueue.Add(new ReliableMessage { Seq = seq, Data = packet, Sent = DateTime.UtcNow });

                client.Peer.Send(packet);
            }
        }

        // PendingReliableCount: cuántos paquetes reliable (ver
        // QueueReliableEvent) todavía no fueron confirmados por este
        // cliente. Diagnóstico genérico — útil para cualquier juego que
        // quiera saber si un cliente se está quedando atrás, no es
        // específico de ningún evento en particular.
        public int PendingReliableCount(ushort playerId)
        {
            ClientConnection? client;
            lock (_clientsLock) _clientsById.TryGetValue(playerId, out client);
            if (client == null) return 0;

            lock (client.QueueLock) return client.ReliableQueue.Count;
        }

        // Solo cuenta Connected==true — _clientsById también incluye
        // clientes desconectados que quedaron "reclamables" para una
        // reconexión (ver HeartbeatLoop/AdmitPlayer), así que Count ya no
        // alcanza.
        public int ConnectedPlayerCount
        {
            get
            {
                lock (_clientsLock)
                {
                    int n = 0;
                    foreach (var c in _clientsById.Values)
                        if (c.Connected) n++;
                    return n;
                }
            }
        }

        // --- Recepción: punto de entrada común para cualquier transporte ---

        // HandlePacket procesa un paquete crudo recibido de un Peer que YA
        // pertenece a esta sala. El handshake no pasa por acá: es Server
        // quien lo intercepta primero para decidir a qué sala corresponde
        // (crear una nueva o unirse a una existente) y recién ahí llama a
        // AdmitPlayer — ver Server.cs.
        public void HandlePacket(byte[] data, IPeer peer)
        {
            if (!PacketHeader.TryParse(data, out uint seq, out _, out byte type, out byte flags))
                return;

            switch (type)
            {
                case PacketType.Input:
                    HandleInput(seq, peer, flags, SubArray(data, PacketHeader.Size));
                    break;
                case PacketType.Ack:
                    HandleAck(seq, peer);
                    break;
                case PacketType.Ping:
                    HandlePing(seq, peer, SubArray(data, PacketHeader.Size));
                    break;
            }
        }

        // AdmitPlayer registra un cliente nuevo en esta sala, o reconoce una
        // reconexión, y dispara OnPlayerConnected(playerId, role,
        // reconnected). La llama Server, después de decidir (crear/unirse)
        // a qué sala pertenece el cliente. Role es opaco para el core — cada
        // juego define sus propios valores y qué hacer con ellos (ej. no
        // crear una "barra" de juego para el rol que representa al
        // tablero).
        //
        // Reconexión: si hay un cliente marcado desconectado (ver
        // HeartbeatLoop) con la misma IP (Peer.IP()) que quien está haciendo
        // el handshake, se le devuelve la MISMA identidad (PlayerId, y el
        // Role ORIGINAL — no el que venga en este handshake, para no perder
        // de vista quién era) en vez de crear un jugador nuevo. Es una
        // heurística por IP, no un token de sesión (el protocolo no lleva
        // uno todavía): funciona bien en la LAN/datos móviles típica, donde
        // cada dispositivo tiene su propia IP — no es a prueba de balas si
        // dos jugadores comparten la misma IP pública (NAT compartido) y
        // ambos están desconectados a la vez.
        //
        // Idempotente además en el sentido de siempre: un cliente que ya
        // está admitido (mismo Peer.Key(), sin pasar por desconexión)
        // devuelve su PlayerId actual sin duplicar estado ni disparar el
        // hook de nuevo (cubre el reintento de un handshake cuyo ack se
        // perdió).
        public ushort AdmitPlayer(IPeer peer, byte role, out bool reconnected)
        {
            string key = peer.Key();
            ushort playerId;
            reconnected = false;

            lock (_clientsLock)
            {
                if (_clientsByKey.TryGetValue(key, out var existing))
                    return existing.PlayerId;

                ClientConnection? reclaimed = null;
                string ip = peer.IP();
                if (!string.IsNullOrEmpty(ip))
                {
                    foreach (var c in _clientsById.Values)
                    {
                        if (!c.Connected && c.Peer.IP() == ip)
                        {
                            reclaimed = c;
                            break;
                        }
                    }
                }

                if (reclaimed != null)
                {
                    _clientsByKey.Remove(reclaimed.Peer.Key());
                    reclaimed.Peer = peer;
                    reclaimed.Connected = true;
                    reclaimed.LastHeartbeat = DateTime.UtcNow;
                    _clientsByKey[key] = reclaimed;
                    playerId = reclaimed.PlayerId;
                    role = reclaimed.Role;
                    reconnected = true;
                }
                else
                {
                    playerId = _nextPlayerId++;
                    var client = new ClientConnection
                    {
                        PlayerId = playerId,
                        Peer = peer,
                        Role = role,
                        LastHeartbeat = DateTime.UtcNow,
                        Connected = true
                    };
                    _clientsById[playerId] = client;
                    _clientsByKey[key] = client;
                }
            }

            OnPlayerConnected?.Invoke(playerId, role, reconnected);
            return playerId;
        }

        // GetClientRole devuelve el rol opaco con el que se admitió a un
        // jugador (ver AdmitPlayer). Devuelve false si el jugador no existe
        // (ya desconectado, o ID inválido).
        public bool GetClientRole(ushort playerId, out byte role)
        {
            lock (_clientsLock)
            {
                if (_clientsById.TryGetValue(playerId, out var client))
                {
                    role = client.Role;
                    return true;
                }
            }
            role = 0;
            return false;
        }

        private void HandleInput(uint seq, IPeer peer, byte flags, byte[] payload)
        {
            var client = FindClientByKey(peer.Key());
            if (client == null) return;
            if (!PlayerInput.TryDecodePayload(payload, out short dx, out short dy, out ushort rot, out uint actions))
                return;

            var input = new PlayerInput
            {
                PlayerId = client.PlayerId,
                Seq = seq,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DeltaX = dx,
                DeltaY = dy,
                Rotation = rot,
                Actions = actions
            };

            if ((flags & PacketFlag.Reliable) != 0)
                SendAck(client, seq);

            if (_inputQueue.Count < InputQueueMax)
                _inputQueue.Enqueue(input);

            client.LastSeq = seq;
            client.LastHeartbeat = DateTime.UtcNow;
        }

        private void HandleAck(uint seq, IPeer peer)
        {
            var client = FindClientByKey(peer.Key());
            if (client == null) return;

            client.LastHeartbeat = DateTime.UtcNow;

            lock (client.QueueLock)
            {
                client.ReliableQueue.RemoveAll(m => m.Seq <= seq);
            }
        }

        private void HandlePing(uint seq, IPeer peer, byte[] payload)
        {
            var client = FindClientByKey(peer.Key());
            if (client == null) return;
            if (payload.Length < 8) return;

            long clientTime = BigEndian.ReadInt64(payload, 0);
            long serverTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            client.Ping = (int)((serverTime - clientTime) / 2);
            client.LastHeartbeat = DateTime.UtcNow;

            var response = new byte[17];
            BigEndian.WriteUInt32(response, 0, seq);
            BigEndian.WriteUInt32(response, 4, 0);
            response[8] = PacketType.Ping;
            BigEndian.WriteInt64(response, 9, serverTime);
            peer.Send(response);
        }

        private void SendAck(ClientConnection client, uint seq)
        {
            var response = new byte[9];
            BigEndian.WriteUInt32(response, 0, seq);
            BigEndian.WriteUInt32(response, 4, seq);
            response[8] = PacketType.Ack;
            client.Peer.Send(response);
        }

        // --- Tick de juego ---

        private void GameLoop()
        {
            while (_running)
            {
                Thread.Sleep(TickRate);
                if (!_running) break;

                DispatchInputs();
                OnTick?.Invoke(_tick);
                BroadcastSnapshot();

                _tick++;
            }
        }

        // El core solo desencola y reenvía los inputs al juego, en orden —
        // no interpreta DeltaX/DeltaY/Rotation/Actions de ninguna forma.
        private void DispatchInputs()
        {
            while (_inputQueue.TryDequeue(out var input))
                OnInput?.Invoke(input);
        }

        private void BroadcastSnapshot()
        {
            byte[] statePayload = StateProvider?.Invoke() ?? Array.Empty<byte>();

            List<GameEvent> events;
            lock (_eventsLock)
            {
                events = new List<GameEvent>(_pendingEvents);
                _pendingEvents.Clear();
            }

            var snapshot = new GameSnapshot
            {
                Tick = _tick,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                StatePayload = statePayload,
                Events = events
            };

            var payload = snapshot.Encode();
            uint seq = NextSeqNumber();

            List<ClientConnection> clients;
            lock (_clientsLock)
                clients = new List<ClientConnection>(_clientsById.Values);

            var header = PacketHeader.Build(seq, 0, PacketType.Snapshot);
            var packet = Concat(header, payload);

            foreach (var client in clients)
            {
                if (!client.Connected) continue;
                client.Peer.Send(packet);
            }
        }

        // --- Retransmisión de confiables ---

        private void SendLoop()
        {
            while (_running)
            {
                Thread.Sleep(50);
                if (!_running) break;

                List<ClientConnection> clients;
                lock (_clientsLock)
                    clients = new List<ClientConnection>(_clientsById.Values);

                var now = DateTime.UtcNow;
                foreach (var client in clients)
                {
                    lock (client.QueueLock)
                    {
                        foreach (var msg in client.ReliableQueue)
                        {
                            if (now - msg.Sent > RetryInterval && msg.Retries < MaxRetries)
                            {
                                client.Peer.Send(msg.Data);
                                msg.Sent = now;
                                msg.Retries++;
                            }
                        }
                    }
                }
            }
        }

        // --- Heartbeat / desconexión ---

        private void HeartbeatLoop()
        {
            while (_running)
            {
                Thread.Sleep(HeartbeatInterval);
                if (!_running) break;

                var now = DateTime.UtcNow;
                var toRemove = new List<ushort>();

                lock (_clientsLock)
                {
                    foreach (var kv in _clientsById)
                    {
                        if (now - kv.Value.LastHeartbeat > HeartbeatTimeout)
                            toRemove.Add(kv.Key);
                    }

                    // Solo se marca Connected=false — a propósito NO se
                    // borra de _clientsById: se mantiene "reclamable" por
                    // AdmitPlayer si el mismo dispositivo (misma IP) vuelve
                    // a conectarse, así el juego puede restaurar su estado
                    // en vez de tratarlo como uno nuevo. _clientsByKey sí se
                    // limpia: esa key (ej. ip:puerto UDP viejo) ya no sirve
                    // para rutear nada. El Server, por su lado, no destruye
                    // esta sala instantáneamente al quedar en 0 conectados —
                    // ver ServerOptions.EmptyRoomGracePeriod.
                    foreach (var id in toRemove)
                    {
                        var client = _clientsById[id];
                        client.Connected = false;
                        _clientsByKey.Remove(client.Peer.Key());
                    }
                }

                foreach (var id in toRemove)
                    OnPlayerDisconnected?.Invoke(id);
            }
        }

        // --- Helpers ---

        private ClientConnection? FindClientByKey(string key)
        {
            lock (_clientsLock)
                return _clientsByKey.TryGetValue(key, out var c) ? c : null;
        }

        private uint NextSeqNumber()
        {
            lock (_seqLock) return ++_sequenceCounter;
        }

        private static byte[] SubArray(byte[] data, int offset)
        {
            if (offset >= data.Length) return Array.Empty<byte>();
            var result = new byte[data.Length - offset];
            Array.Copy(data, offset, result, 0, result.Length);
            return result;
        }

        private static byte[] Concat(byte[] a, byte[] b)
        {
            var result = new byte[a.Length + b.Length];
            Array.Copy(a, 0, result, 0, a.Length);
            Array.Copy(b, 0, result, a.Length, b.Length);
            return result;
        }
    }
}
