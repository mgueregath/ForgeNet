using System;

namespace NetworkCore
{
    // Constantes y helpers de bajo nivel del protocolo binario.
    // Espejo exacto (byte a byte) de implementaciones/servidores/server.go —
    // cualquier cambio acá debe reflejarse ahí y viceversa, son la misma spec
    // de wire en dos lenguajes.
    public static class PacketType
    {
        public const byte Snapshot = 0x01;
        public const byte Input = 0x02;
        public const byte Ack = 0x03;
        public const byte Ping = 0x04;
        public const byte Handshake = 0x05;
        public const byte Disconnect = 0x06;
    }

    public static class PacketFlag
    {
        public const byte Compressed = 0x01;
        public const byte Reliable = 0x02;
        public const byte Ordered = 0x04;
    }

    // Modos de handshake — quién crea una sala nueva vs. quién se une a una
    // existente por código. Esto sí es genérico: cualquier juego con
    // sesiones de partida necesita esta distinción. Espejo de
    // HandshakeModeCreate/HandshakeModeJoin en protocol.go.
    public static class HandshakeMode
    {
        public const byte Create = 0x00;
        public const byte Join = 0x01;
    }

    // Motivos de rechazo del handshake, enviados en un PacketDisconnect.
    // Espejo de ReasonRoomNotFound/ReasonRoomFull en protocol.go.
    public static class DisconnectReason
    {
        public const byte RoomNotFound = 0x01;
        public const byte RoomFull = 0x02;
    }

    // Payload del handshake (join): Mode(1) + Role(1) + RoomCodeLen(1) +
    // RoomCode(N bytes ASCII). Va después del header en un PacketHandshake
    // mandado por el cliente. RoomCode se ignora en HandshakeMode.Create (el
    // server genera el código). El core no sabe qué juego corre encima, así
    // que Role viaja como un byte opaco: cada juego define sus propios
    // valores y su propio significado (ej. taca-taca usa un valor para
    // "barra" y otro para "tablero" — ver su ejemplo), igual que ya pasa con
    // StatePayload y GameEvent.EventType.
    public static class JoinPayload
    {
        public static byte[] Encode(byte mode, byte role, string roomCode)
        {
            var codeBytes = System.Text.Encoding.ASCII.GetBytes(roomCode ?? "");
            var buf = new byte[3 + codeBytes.Length];
            buf[0] = mode;
            buf[1] = role;
            buf[2] = (byte)codeBytes.Length;
            Array.Copy(codeBytes, 0, buf, 3, codeBytes.Length);
            return buf;
        }

        public static bool TryDecode(byte[] payload, out byte mode, out byte role, out string roomCode)
        {
            mode = 0; role = 0; roomCode = "";
            if (payload.Length < 3) return false;

            int codeLen = payload[2];
            if (payload.Length < 3 + codeLen) return false;

            mode = payload[0];
            role = payload[1];
            roomCode = System.Text.Encoding.ASCII.GetString(payload, 3, codeLen);
            return true;
        }
    }

    // Payload de la respuesta/ack del handshake: PlayerID(2 BE) +
    // RoomCodeLen(1) + RoomCode(N). Tanto quien crea la sala como quien se
    // une reciben el código, para poder compartirlo (ej. un QR).
    public static class HandshakeAck
    {
        public static byte[] Encode(ushort playerId, string roomCode)
        {
            var codeBytes = System.Text.Encoding.ASCII.GetBytes(roomCode ?? "");
            var buf = new byte[3 + codeBytes.Length];
            BigEndian.WriteUInt16(buf, 0, playerId);
            buf[2] = (byte)codeBytes.Length;
            Array.Copy(codeBytes, 0, buf, 3, codeBytes.Length);
            return buf;
        }

        public static bool TryDecode(byte[] payload, out ushort playerId, out string roomCode)
        {
            playerId = 0; roomCode = "";
            if (payload.Length < 3) return false;

            int codeLen = payload[2];
            if (payload.Length < 3 + codeLen) return false;

            playerId = BigEndian.ReadUInt16(payload, 0);
            roomCode = System.Text.Encoding.ASCII.GetString(payload, 3, codeLen);
            return true;
        }
    }

    // Go usa binary.BigEndian en todo el protocolo. .NET (BitConverter) es
    // little-endian en las plataformas donde corre Unity (x86/x64/ARM), así
    // que estos helpers empaquetan/desempaquetan a mano para no depender de
    // la endianness del host y matchear a Go exactamente.
    public static class BigEndian
    {
        public static void WriteUInt16(byte[] buf, int offset, ushort value)
        {
            buf[offset] = (byte)(value >> 8);
            buf[offset + 1] = (byte)value;
        }

        public static void WriteInt16(byte[] buf, int offset, short value)
            => WriteUInt16(buf, offset, unchecked((ushort)value));

        public static void WriteUInt32(byte[] buf, int offset, uint value)
        {
            buf[offset] = (byte)(value >> 24);
            buf[offset + 1] = (byte)(value >> 16);
            buf[offset + 2] = (byte)(value >> 8);
            buf[offset + 3] = (byte)value;
        }

        public static void WriteInt32(byte[] buf, int offset, int value)
            => WriteUInt32(buf, offset, unchecked((uint)value));

        public static void WriteUInt64(byte[] buf, int offset, ulong value)
        {
            buf[offset] = (byte)(value >> 56);
            buf[offset + 1] = (byte)(value >> 48);
            buf[offset + 2] = (byte)(value >> 40);
            buf[offset + 3] = (byte)(value >> 32);
            buf[offset + 4] = (byte)(value >> 24);
            buf[offset + 5] = (byte)(value >> 16);
            buf[offset + 6] = (byte)(value >> 8);
            buf[offset + 7] = (byte)value;
        }

        public static void WriteInt64(byte[] buf, int offset, long value)
            => WriteUInt64(buf, offset, unchecked((ulong)value));

        public static ushort ReadUInt16(byte[] buf, int offset)
            => (ushort)((buf[offset] << 8) | buf[offset + 1]);

        public static short ReadInt16(byte[] buf, int offset)
            => unchecked((short)ReadUInt16(buf, offset));

        public static uint ReadUInt32(byte[] buf, int offset)
            => ((uint)buf[offset] << 24) | ((uint)buf[offset + 1] << 16) |
               ((uint)buf[offset + 2] << 8) | buf[offset + 3];

        public static int ReadInt32(byte[] buf, int offset)
            => unchecked((int)ReadUInt32(buf, offset));

        public static ulong ReadUInt64(byte[] buf, int offset)
        {
            ulong hi = ReadUInt32(buf, offset);
            ulong lo = ReadUInt32(buf, offset + 4);
            return (hi << 32) | lo;
        }

        public static long ReadInt64(byte[] buf, int offset)
            => unchecked((long)ReadUInt64(buf, offset));
    }

    // Header común a todos los paquetes: seq(4) + ack(4) + typeAndFlags(1) = 9 bytes.
    public static class PacketHeader
    {
        public const int Size = 9;

        public static byte[] Build(uint seq, uint ack, byte type, byte flags = 0)
        {
            var buf = new byte[Size];
            BigEndian.WriteUInt32(buf, 0, seq);
            BigEndian.WriteUInt32(buf, 4, ack);
            buf[8] = (byte)((type & 0x0F) | ((flags & 0x0F) << 4));
            return buf;
        }

        public static bool TryParse(byte[] data, out uint seq, out uint ack, out byte type, out byte flags)
        {
            seq = 0; ack = 0; type = 0; flags = 0;
            if (data.Length < Size) return false;

            seq = BigEndian.ReadUInt32(data, 0);
            ack = BigEndian.ReadUInt32(data, 4);
            byte typeAndFlags = data[8];
            type = (byte)(typeAndFlags & 0x0F);
            flags = (byte)((typeAndFlags >> 4) & 0x0F);
            return true;
        }
    }
}
