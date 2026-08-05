using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace NetworkCore
{
    // Rol "cliente" del protocolo — usado tanto por el mando (siempre) como
    // por cualquier otro cliente Unity que hable con un NetworkHost (embebido
    // o dedicado). Clase C# plana, sin dependencias de UnityEngine.
    public class NetworkClient : IDisposable
    {
        private static readonly TimeSpan RetryInterval = TimeSpan.FromMilliseconds(100);
        private const int MaxRetries = 3;

        private UdpClient? _udp;
        private IPEndPoint? _serverEndPoint;
        private volatile bool _running;
        private uint _sequenceCounter;

        private readonly Dictionary<uint, DateTime> _pendingPings = new();
        private readonly object _pingLock = new();

        private readonly List<ReliableMessage> _reliableQueue = new();
        private readonly object _reliableLock = new();

        public ushort PlayerId { get; private set; }
        public bool Connected { get; private set; }
        public int LastPingMs { get; private set; }
        public GameSnapshot? LatestSnapshot { get; private set; }

        public event Action<GameSnapshot>? OnSnapshot;
        public event Action<int>? OnPong; // ms de RTT

        // Handshake bloqueante con timeout — simple y suficiente para
        // conectar contra el host al arrancar. No corre en background.
        public bool Connect(string host, int port, int timeoutMs = 3000)
        {
            _udp = new UdpClient();
            _udp.Client.ReceiveTimeout = timeoutMs;
            _serverEndPoint = new IPEndPoint(IPAddress.Parse(host), port);

            var handshake = PacketHeader.Build(0, 0, PacketType.Handshake);
            _udp.Send(handshake, handshake.Length, _serverEndPoint);

            IPEndPoint remote = new IPEndPoint(IPAddress.Any, 0);
            byte[] response;
            try
            {
                response = _udp.Receive(ref remote);
            }
            catch (SocketException)
            {
                return false;
            }

            if (!PacketHeader.TryParse(response, out _, out _, out byte type, out _) || type != PacketType.Handshake)
                return false;
            if (response.Length < 11)
                return false;

            PlayerId = BigEndian.ReadUInt16(response, 9);
            Connected = true;
            _running = true;

            // Igual que en NetworkHost: timeout corto en vez de 0 (bloqueo
            // indefinido) para que Disconnect() no dependa de que Close()
            // interrumpa un Receive() bloqueado en otro thread.
            _udp.Client.ReceiveTimeout = 500;
            new Thread(ReceiveLoop) { IsBackground = true, Name = "NetworkClient.Receive" }.Start();
            new Thread(RetryLoop) { IsBackground = true, Name = "NetworkClient.Retry" }.Start();

            return true;
        }

        public void Disconnect()
        {
            _running = false;
            Connected = false;
            _udp?.Close();
        }

        public void Dispose() => Disconnect();

        public void SendInput(short deltaX, short deltaY, ushort rotation, uint actions, bool reliable = false)
        {
            if (!Connected) return;

            uint seq = ++_sequenceCounter;
            var input = new PlayerInput { DeltaX = deltaX, DeltaY = deltaY, Rotation = rotation, Actions = actions };
            byte flags = reliable ? PacketFlag.Reliable : (byte)0;

            var header = PacketHeader.Build(seq, 0, PacketType.Input, flags);
            var payload = input.EncodePayload();
            var packet = Concat(header, payload);

            SendRaw(packet);

            if (reliable)
            {
                lock (_reliableLock)
                {
                    _reliableQueue.Add(new ReliableMessage { Seq = seq, Data = packet, Sent = DateTime.UtcNow, Retries = 0 });
                }
            }
        }

        public void SendPing()
        {
            if (!Connected) return;

            uint seq = ++_sequenceCounter;
            var header = PacketHeader.Build(seq, 0, PacketType.Ping);
            var payload = new byte[8];
            BigEndian.WriteInt64(payload, 0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            var packet = Concat(header, payload);

            lock (_pingLock) _pendingPings[seq] = DateTime.UtcNow;
            SendRaw(packet);
        }

        private void ReceiveLoop()
        {
            var anyEndPoint = new IPEndPoint(IPAddress.Any, 0);
            while (_running)
            {
                byte[] data;
                try
                {
                    data = _udp!.Receive(ref anyEndPoint);
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

                HandlePacket(data);
            }
        }

        private void HandlePacket(byte[] data)
        {
            if (!PacketHeader.TryParse(data, out uint seq, out _, out byte type, out _))
                return;

            switch (type)
            {
                case PacketType.Snapshot:
                    var payload = new byte[data.Length - PacketHeader.Size];
                    Array.Copy(data, PacketHeader.Size, payload, 0, payload.Length);
                    // Nunca confiar ciegamente en el payload: un paquete
                    // corrupto, de otra versión de protocolo, o (como en el
                    // cross-test) de un servidor con un esquema de snapshot
                    // distinto, no debe poder tirar abajo el thread de
                    // recepción — en .NET una excepción no capturada en un
                    // thread de background mata el proceso entero.
                    if (GameSnapshot.TryDecode(payload, out var snapshot))
                    {
                        LatestSnapshot = snapshot;
                        OnSnapshot?.Invoke(snapshot!);
                    }
                    break;

                case PacketType.Ping:
                    lock (_pingLock)
                    {
                        if (_pendingPings.TryGetValue(seq, out var sentAt))
                        {
                            LastPingMs = (int)(DateTime.UtcNow - sentAt).TotalMilliseconds;
                            _pendingPings.Remove(seq);
                            OnPong?.Invoke(LastPingMs);
                        }
                    }
                    break;

                case PacketType.Ack:
                    lock (_reliableLock)
                        _reliableQueue.RemoveAll(m => m.Seq <= seq);
                    break;
            }
        }

        private void RetryLoop()
        {
            while (_running)
            {
                Thread.Sleep(50);
                if (!_running) break;

                var now = DateTime.UtcNow;
                lock (_reliableLock)
                {
                    foreach (var msg in _reliableQueue)
                    {
                        if (now - msg.Sent > RetryInterval && msg.Retries < MaxRetries)
                        {
                            SendRaw(msg.Data);
                            msg.Sent = now;
                            msg.Retries++;
                        }
                    }
                }
            }
        }

        private static byte[] Concat(byte[] a, byte[] b)
        {
            var result = new byte[a.Length + b.Length];
            Array.Copy(a, 0, result, 0, a.Length);
            Array.Copy(b, 0, result, a.Length, b.Length);
            return result;
        }

        private void SendRaw(byte[] packet)
        {
            try { _udp?.Send(packet, packet.Length, _serverEndPoint); }
            catch (ObjectDisposedException) { }
            catch (SocketException) { }
        }
    }
}
