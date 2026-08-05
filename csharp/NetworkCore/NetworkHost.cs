using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace NetworkCore
{
    // Rol "servidor" del protocolo. Es una clase C# plana (sin MonoBehaviour,
    // sin dependencias de UnityEngine) para poder correr en dos contextos:
    //  - Embebido dentro de un cliente Unity que también renderiza (modalidad
    //    Host Embebido, ej. taca-taca: un teléfono es la mesa).
    //  - Un futuro build headless de Unity ("Dedicated Server" build target)
    //    si algún juego necesitara la topología clásica en C# en vez de Go.
    //
    // Genérico por diseño: NetworkHost solo maneja sesión/conexión/transporte
    // (handshake, heartbeat, reliable, ping, tick loop) — NO sabe nada sobre
    // el juego en sí (no hay "Health", "Ammo", "Ball" ni nada específico acá).
    // El juego que lo use se engancha vía OnInput/OnTick/StateProvider.
    public class ClientConnection
    {
        public ushort PlayerId;
        public IPEndPoint EndPoint = null!;
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
        public const int MaxPlayers = 64;
        private static readonly TimeSpan TickRate = TimeSpan.FromMilliseconds(16); // 60Hz, igual que server.go
        private static readonly TimeSpan RetryInterval = TimeSpan.FromMilliseconds(100);
        private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan HeartbeatTimeout = TimeSpan.FromSeconds(10);
        private const int MaxRetries = 3;

        private UdpClient? _udp;
        private volatile bool _running;

        private readonly Dictionary<IPEndPoint, ClientConnection> _clientsByEndPoint = new();
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

        // Se dispara cuando un jugador nuevo termina el handshake.
        public event Action<ushort>? OnPlayerConnected;
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

        public void Start(int port)
        {
            _udp = new UdpClient(port);
            _udp.Client.ReceiveBufferSize = 1 << 20;
            _udp.Client.SendBufferSize = 1 << 20;
            // Timeout corto en vez de bloqueo indefinido: en algunas
            // plataformas UdpClient.Close() no interrumpe de forma fiable un
            // Receive() síncrono bloqueado en otro thread (puede colgar
            // Stop() esperando a que ese thread termine). Con esto el loop
            // se despierta solo cada 500ms a chequear _running.
            _udp.Client.ReceiveTimeout = 500;
            _running = true;

            new Thread(ReceiveLoop) { IsBackground = true, Name = "NetworkHost.Receive" }.Start();
            new Thread(GameLoop) { IsBackground = true, Name = "NetworkHost.Tick" }.Start();
            new Thread(SendLoop) { IsBackground = true, Name = "NetworkHost.Send" }.Start();
            new Thread(HeartbeatLoop) { IsBackground = true, Name = "NetworkHost.Heartbeat" }.Start();
        }

        public void Stop()
        {
            _running = false;
            _udp?.Close();
        }

        public void Dispose() => Stop();

        // El juego puede encolar eventos propios (ej. GOAL) para que salgan
        // en el próximo snapshot, además de los que dispara el propio core
        // (hoy el core no genera ninguno automáticamente — antes lo hacía
        // como parte de la lógica de "Fire" del prototipo shooter, ahora esa
        // decisión es 100% del juego).
        public void QueueEvent(GameEvent evt)
        {
            lock (_eventsLock) _pendingEvents.Add(evt);
        }

        // --- Recepción ---

        private void ReceiveLoop()
        {
            var anyEndPoint = new IPEndPoint(IPAddress.Any, 0);
            while (_running)
            {
                byte[] data;
                IPEndPoint remote;
                try
                {
                    data = _udp!.Receive(ref anyEndPoint);
                    remote = new IPEndPoint(anyEndPoint.Address, anyEndPoint.Port);
                }
                catch (SocketException)
                {
                    if (!_running) return;
                    continue;
                }
                catch (ObjectDisposedException)
                {
                    return;
                }

                HandlePacket(data, remote);
            }
        }

        private void HandlePacket(byte[] data, IPEndPoint addr)
        {
            if (!PacketHeader.TryParse(data, out uint seq, out _, out byte type, out byte flags))
                return;

            switch (type)
            {
                case PacketType.Handshake:
                    HandleHandshake(seq, addr);
                    break;
                case PacketType.Input:
                    HandleInput(seq, addr, flags, SubArray(data, PacketHeader.Size));
                    break;
                case PacketType.Ack:
                    HandleAck(seq, addr);
                    break;
                case PacketType.Ping:
                    HandlePing(seq, addr, SubArray(data, PacketHeader.Size));
                    break;
            }
        }

        private static byte[] SubArray(byte[] data, int offset)
        {
            if (offset >= data.Length) return Array.Empty<byte>();
            var result = new byte[data.Length - offset];
            Array.Copy(data, offset, result, 0, result.Length);
            return result;
        }

        private void HandleHandshake(uint seq, IPEndPoint addr)
        {
            ushort playerId;
            lock (_clientsLock)
            {
                if (_clientsByEndPoint.ContainsKey(addr))
                    return;

                playerId = _nextPlayerId++;
                var client = new ClientConnection
                {
                    PlayerId = playerId,
                    EndPoint = addr,
                    LastSeq = seq,
                    LastHeartbeat = DateTime.UtcNow,
                    Connected = true
                };
                _clientsByEndPoint[addr] = client;
                _clientsById[playerId] = client;
            }

            var response = new byte[11];
            BigEndian.WriteUInt32(response, 0, seq);
            BigEndian.WriteUInt32(response, 4, 0);
            response[8] = PacketType.Handshake;
            BigEndian.WriteUInt16(response, 9, playerId);
            SendTo(response, addr);

            OnPlayerConnected?.Invoke(playerId);
        }

        private void HandleInput(uint seq, IPEndPoint addr, byte flags, byte[] payload)
        {
            var client = FindClientByEndPoint(addr);
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

        private void HandleAck(uint seq, IPEndPoint addr)
        {
            var client = FindClientByEndPoint(addr);
            if (client == null) return;

            client.LastHeartbeat = DateTime.UtcNow;

            lock (client.QueueLock)
            {
                client.ReliableQueue.RemoveAll(m => m.Seq <= seq);
            }
        }

        private void HandlePing(uint seq, IPEndPoint addr, byte[] payload)
        {
            var client = FindClientByEndPoint(addr);
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
            SendTo(response, addr);
        }

        private void SendAck(ClientConnection client, uint seq)
        {
            var response = new byte[9];
            BigEndian.WriteUInt32(response, 0, seq);
            BigEndian.WriteUInt32(response, 4, seq);
            response[8] = PacketType.Ack;
            SendTo(response, client.EndPoint);
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

            foreach (var client in clients)
            {
                if (!client.Connected) continue;

                var packet = new byte[payload.Length + PacketHeader.Size];
                BigEndian.WriteUInt32(packet, 0, seq);
                BigEndian.WriteUInt32(packet, 4, 0);
                packet[8] = PacketType.Snapshot;
                Array.Copy(payload, 0, packet, PacketHeader.Size, payload.Length);

                SendTo(packet, client.EndPoint);
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
                                SendTo(msg.Data, client.EndPoint);
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

                    foreach (var id in toRemove)
                    {
                        var client = _clientsById[id];
                        client.Connected = false;
                        _clientsById.Remove(id);
                        _clientsByEndPoint.Remove(client.EndPoint);
                    }
                }

                foreach (var id in toRemove)
                    OnPlayerDisconnected?.Invoke(id);
            }
        }

        // --- Helpers ---

        private ClientConnection? FindClientByEndPoint(IPEndPoint addr)
        {
            lock (_clientsLock)
                return _clientsByEndPoint.TryGetValue(addr, out var c) ? c : null;
        }

        private uint NextSeqNumber()
        {
            lock (_seqLock) return ++_sequenceCounter;
        }

        private void SendTo(byte[] data, IPEndPoint addr)
        {
            try { _udp?.Send(data, data.Length, addr); }
            catch (ObjectDisposedException) { /* servidor detenido durante el envío */ }
            catch (SocketException) { /* paquete perdido, esperable en UDP */ }
        }

        public int ConnectedPlayerCount
        {
            get { lock (_clientsLock) return _clientsById.Count; }
        }
    }
}
