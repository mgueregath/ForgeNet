using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;

namespace NetworkCore
{
    // RoomFactory crea un NetworkHost nuevo, con sus propios hooks, cada vez
    // que se crea una sala — cada juego decide qué enganchar (ver
    // NetworkCore.Tests.TacaTacaExample), el Server no sabe nada de eso.
    public delegate NetworkHost RoomFactory();

    // ServerOptions configura el Server. Todo acá es genérico — no hay nada
    // específico de ningún juego.
    public class ServerOptions
    {
        // Longitud de los códigos de sala generados.
        public int RoomCodeLength = 6;

        // Tope de clientes admitidos por sala (0 = sin tope). El Server solo
        // cuenta clientes — no sabe qué rol tiene cada uno, así que un
        // límite que dependa del rol (ej. "2 barras + 1 tablero" en
        // taca-taca) es responsabilidad del juego, no de acá.
        public int MaxPlayersPerRoom = 0;

        // Máximo de paquetes de handshake aceptados desde un mismo Peer
        // (misma IP:puerto UDP) por HandshakeRateLimitWindow — el resto se
        // descarta en silencio. Esto NO protege contra un atacante que rota
        // de origen en cada intento (el core no ve la IP real, solo
        // Peer.Key()) — sí frena un cliente roto reintentando en loop o un
        // flood simple desde una única conexión.
        public int HandshakeRateLimit = 10;
        public TimeSpan HandshakeRateLimitWindow = TimeSpan.FromSeconds(10);

        // Cuánto se espera, desde que una sala que ya tuvo jugadores queda
        // en 0 conectados, antes de destruirla — no instantáneo, para
        // darle tiempo a AdmitPlayer de reconocer una reconexión (ver el
        // comentario grande ahí) antes de que la sala misma deje de
        // existir. Default: 30s.
        public TimeSpan EmptyRoomGracePeriod = TimeSpan.FromSeconds(30);
    }

    internal class Room
    {
        public string Code = "";
        public NetworkHost Host = null!;
        public bool HadPlayer;
        // Desde cuándo Host.ConnectedPlayerCount está en 0 — null mientras
        // la sala tiene al menos un cliente conectado. Ver SweepEmptyRooms.
        public DateTime? EmptySince;
    }

    // PeerInfo recuerda a qué sala pertenece cada Peer y guarda el último
    // ack de handshake mandado, para poder responder handshakes
    // reintentados (ack perdido) sin volver a admitir al cliente ni crear
    // una sala nueva.
    internal class PeerInfo
    {
        public string RoomCode = "";
        public byte[] Ack = Array.Empty<byte>();
    }

    internal class HandshakeWindow
    {
        public DateTime Start;
        public int Count;
    }

    // UdpPeer implementa IPeer para un cliente UDP crudo (ej. un cliente
    // Unity). Espejo de udpPeer en go/networkcore/transport_udp.go.
    internal class UdpPeer : IPeer
    {
        private readonly UdpClient _udp;
        private readonly IPEndPoint _endPoint;

        public UdpPeer(UdpClient udp, IPEndPoint endPoint)
        {
            _udp = udp;
            _endPoint = endPoint;
        }

        public void Send(byte[] data)
        {
            try { _udp.Send(data, data.Length, _endPoint); }
            catch (ObjectDisposedException) { /* servidor detenido durante el envío */ }
            catch (SocketException) { /* paquete perdido, esperable en UDP */ }
        }

        public string Key() => "udp:" + _endPoint;

        public string IP() => _endPoint.Address.ToString();
    }

    // Server multiplexa muchas salas (partidas) concurrentes sobre el mismo
    // transporte — genérico, no sabe qué juego corre en cada una. Un cliente
    // que manda PacketHandshake con HandshakeMode.Create arranca una sala
    // nueva y recibe un código; con HandshakeMode.Join se une a la sala de
    // ese código. Todo lo demás (input, ack, ping) se rutea a la sala del
    // cliente que lo mandó. Espejo de go/networkcore/server.go.
    public class Server : IDisposable
    {
        // roomCodeAlphabet excluye 0/O y 1/I — ambiguos al leerlos a mano si
        // el QR falla y alguien tiene que tipear el código.
        private const string RoomCodeAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
        private static readonly TimeSpan JanitorInterval = TimeSpan.FromSeconds(5);

        private readonly ServerOptions _opts;
        private readonly RoomFactory _roomFactory;

        private readonly Dictionary<string, Room> _rooms = new();
        private readonly Dictionary<string, PeerInfo> _peers = new(); // key = Peer.Key()
        private readonly object _lock = new();

        private readonly Dictionary<string, HandshakeWindow> _handshakeAttempts = new();
        private readonly object _handshakeLock = new();

        private UdpClient? _udp;
        private volatile bool _running;
        private ushort _udpPort;

        public Server(RoomFactory roomFactory, ServerOptions? options = null)
        {
            _roomFactory = roomFactory;
            _opts = options ?? new ServerOptions();
            if (_opts.RoomCodeLength <= 0) _opts.RoomCodeLength = 6;
            if (_opts.HandshakeRateLimit <= 0) _opts.HandshakeRateLimit = 10;
            if (_opts.HandshakeRateLimitWindow <= TimeSpan.Zero) _opts.HandshakeRateLimitWindow = TimeSpan.FromSeconds(10);
            if (_opts.EmptyRoomGracePeriod <= TimeSpan.Zero) _opts.EmptyRoomGracePeriod = TimeSpan.FromSeconds(30);
        }

        // StartUdp liga un socket UDP y arranca su loop de recepción, más el
        // janitor que destruye salas vacías. No arranca ninguna sala
        // todavía — las salas se crean bajo demanda con el primer handshake
        // HandshakeMode.Create (o directamente con CreateRoom(), para el
        // modo "host embebido").
        //
        // port=0 le pide al SO cualquier puerto disponible — útil para un
        // deployment que no quiere depender de que un puerto fijo esté
        // libre. El puerto real que asignó el SO queda disponible en
        // UDPPort().
        public void StartUdp(int port)
        {
            _udp = new UdpClient(port);
            _udp.Client.ReceiveBufferSize = 1 << 20;
            _udp.Client.SendBufferSize = 1 << 20;
            // Timeout corto en vez de bloqueo indefinido: ver la misma nota
            // en NetworkHost (versión previa a este cambio) — con esto el
            // loop de recepción se despierta solo cada 500ms a chequear
            // _running en vez de depender de que Close() interrumpa un
            // Receive() bloqueado en otro thread.
            _udp.Client.ReceiveTimeout = 500;
            _udpPort = (ushort)((IPEndPoint)_udp.Client.LocalEndPoint!).Port;
            _running = true;

            new Thread(UdpReceiveLoop) { IsBackground = true, Name = "Server.UdpReceive" }.Start();
            new Thread(JanitorLoop) { IsBackground = true, Name = "Server.Janitor" }.Start();
        }

        // UDPPort devuelve el puerto UDP real en el que quedó escuchando el
        // server — el mismo que se pidió en StartUdp, salvo que se haya
        // pedido 0 (cualquiera disponible), en cuyo caso es el que el SO
        // asignó de verdad. 0 si StartUdp nunca se llamó.
        public ushort UDPPort() => _udpPort;

        // Stop cierra el transporte y detiene todas las salas.
        public void Stop()
        {
            _running = false;
            _udp?.Close();

            List<Room> rooms;
            lock (_lock)
            {
                rooms = new List<Room>(_rooms.Values);
                _rooms.Clear();
                _peers.Clear();
            }
            foreach (var room in rooms) room.Host.Stop();
        }

        public void Dispose() => Stop();

        private void UdpReceiveLoop()
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

                HandlePacket(data, new UdpPeer(_udp!, remote));
            }
        }

        // HandlePacket es el punto de entrada común para cualquier
        // transporte — igual que antes lo era NetworkHost.HandlePacket, pero
        // a nivel Server, porque el Server es quien sabe a qué sala
        // pertenece cada paquete.
        public void HandlePacket(byte[] data, IPeer peer)
        {
            if (!PacketHeader.TryParse(data, out uint seq, out _, out byte type, out _))
                return;

            if (type == PacketType.Handshake)
            {
                HandleHandshake(seq, peer, SubArray(data, PacketHeader.Size));
                return;
            }

            Room? room;
            lock (_lock)
            {
                room = _peers.TryGetValue(peer.Key(), out var info) && _rooms.TryGetValue(info.RoomCode, out var r)
                    ? r
                    : null;
            }

            room?.Host.HandlePacket(data, peer);
        }

        private void HandleHandshake(uint seq, IPeer peer, byte[] payload)
        {
            string key = peer.Key();

            if (!AllowHandshakeAttempt(key)) return;

            // Reintento de un handshake ya admitido (el ack anterior se
            // perdió): no crear una sala nueva ni volver a admitir, solo
            // reenviar el ack.
            lock (_lock)
            {
                if (_peers.TryGetValue(key, out var existingInfo))
                {
                    peer.Send(existingInfo.Ack);
                    return;
                }
            }

            if (!JoinPayload.TryDecode(payload, out byte mode, out byte role, out string roomCode))
                return;

            Room? room;
            switch (mode)
            {
                case HandshakeMode.Create:
                    room = CreateRoomInternal();
                    break;

                case HandshakeMode.Join:
                    lock (_lock) _rooms.TryGetValue(roomCode, out room);
                    if (room == null)
                    {
                        peer.Send(Concat(PacketHeader.Build(seq, 0, PacketType.Disconnect), new[] { DisconnectReason.RoomNotFound }));
                        return;
                    }
                    break;

                default:
                    return;
            }

            if (_opts.MaxPlayersPerRoom > 0 && room.Host.ConnectedPlayerCount >= _opts.MaxPlayersPerRoom)
            {
                peer.Send(Concat(PacketHeader.Build(seq, 0, PacketType.Disconnect), new[] { DisconnectReason.RoomFull }));
                return;
            }

            ushort playerId = room.Host.AdmitPlayer(peer, role, out _);
            room.HadPlayer = true;

            var ack = Concat(PacketHeader.Build(seq, 0, PacketType.Handshake), HandshakeAck.Encode(playerId, room.Code));
            peer.Send(ack);

            lock (_lock) _peers[key] = new PeerInfo { RoomCode = room.Code, Ack = ack };
        }

        // CreateRoom arranca una sala directamente, sin pasar por un
        // handshake de red — a diferencia de una sala creada porque un
        // cliente mandó HandshakeMode.Create, acá no hay ningún cliente
        // admitido todavía.
        //
        // Pensado para el modo "host embebido" (ej. un tablero en LAN, que
        // es el mismo proceso que corre el Server): la app llama a esto una
        // vez al arrancar y ya tiene una sala fija a la que los mandos se
        // unen con JoinRoom, sin que el propio tablero tenga que hacerse
        // pasar por cliente de sí mismo para "crear" la sala. La sala recién
        // creada NO cuenta como "vacía" para el janitor (ver
        // SweepEmptyRooms/Room.HadPlayer) hasta que admite a su primer
        // cliente de verdad, así que puede esperar indefinidamente a que
        // alguien se una sin que EmptyRoomGracePeriod la destruya.
        public string CreateRoom() => CreateRoomInternal().Code;

        // AllowHandshakeAttempt aplica el rate limit de
        // ServerOptions.HandshakeRateLimit por Peer — ventana fija, no token
        // bucket: simple y suficiente para lo que protege.
        private bool AllowHandshakeAttempt(string key)
        {
            lock (_handshakeLock)
            {
                var now = DateTime.UtcNow;
                if (!_handshakeAttempts.TryGetValue(key, out var window) || now - window.Start > _opts.HandshakeRateLimitWindow)
                {
                    _handshakeAttempts[key] = new HandshakeWindow { Start = now, Count = 1 };
                    return true;
                }
                if (window.Count >= _opts.HandshakeRateLimit) return false;
                window.Count++;
                return true;
            }
        }

        private Room CreateRoomInternal()
        {
            string code = GenerateUniqueRoomCode();
            var host = _roomFactory();
            host.Start();

            var room = new Room { Code = code, Host = host };
            lock (_lock) _rooms[code] = room;
            return room;
        }

        private string GenerateUniqueRoomCode()
        {
            while (true)
            {
                string code = RandomRoomCode(_opts.RoomCodeLength);
                lock (_lock)
                {
                    if (!_rooms.ContainsKey(code)) return code;
                }
            }
        }

        private static string RandomRoomCode(int length)
        {
            var raw = new byte[length];
            RandomNumberGenerator.Fill(raw);
            var chars = new char[length];
            for (int i = 0; i < length; i++)
                chars[i] = RoomCodeAlphabet[raw[i] % RoomCodeAlphabet.Length];
            return new string(chars);
        }

        // JanitorLoop destruye salas que se quedaron sin ningún cliente
        // conectado durante más de EmptyRoomGracePeriod — genérico, no sabe
        // qué juego corre en ellas. Una sala recién creada que todavía no
        // tuvo ningún cliente no se toca (Room.HadPlayer).
        private void JanitorLoop()
        {
            while (_running)
            {
                Thread.Sleep(JanitorInterval);
                if (!_running) break;

                SweepEmptyRooms();
                SweepHandshakeAttempts();
            }
        }

        private void SweepEmptyRooms()
        {
            lock (_lock)
            {
                var now = DateTime.UtcNow;
                var toRemove = new List<string>();
                foreach (var kv in _rooms)
                {
                    var room = kv.Value;
                    if (!room.HadPlayer) continue;

                    if (room.Host.ConnectedPlayerCount > 0)
                    {
                        room.EmptySince = null;
                        continue;
                    }

                    if (room.EmptySince == null)
                    {
                        room.EmptySince = now;
                        continue;
                    }

                    if (now - room.EmptySince.Value < _opts.EmptyRoomGracePeriod) continue;

                    room.Host.Stop();
                    toRemove.Add(kv.Key);
                }

                foreach (var code in toRemove)
                {
                    _rooms.Remove(code);

                    var peerKeysToRemove = new List<string>();
                    foreach (var kv in _peers)
                        if (kv.Value.RoomCode == code) peerKeysToRemove.Add(kv.Key);
                    foreach (var peerKey in peerKeysToRemove) _peers.Remove(peerKey);
                }
            }
        }

        private void SweepHandshakeAttempts()
        {
            lock (_handshakeLock)
            {
                var now = DateTime.UtcNow;
                var toRemove = new List<string>();
                foreach (var kv in _handshakeAttempts)
                    if (now - kv.Value.Start > _opts.HandshakeRateLimitWindow) toRemove.Add(kv.Key);
                foreach (var handshakeKey in toRemove) _handshakeAttempts.Remove(handshakeKey);
            }
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
