using System;
using System.Collections.Generic;

namespace NetworkCore
{
    // Evento genérico: el core no sabe qué significa EventType ni qué hay en
    // Data — cada juego define su propia tabla de tipos (ej. taca-taca:
    // 1=GOAL, 2=MATCH_START, 3=MATCH_END) y su propio formato de payload.
    public class GameEvent
    {
        public ushort PlayerId;
        public byte EventType;
        public byte[] Data = Array.Empty<byte>();
    }

    // Snapshot genérico: Tick/Timestamp son del core (sincronización), pero
    // el estado del juego en sí (posición de la pelota, de las palancas, del
    // marcador, o lo que sea que tenga el juego que use esta librería) viaja
    // como bytes opacos en StatePayload — el core nunca lo interpreta, solo
    // lo transporta. Quien lo define y decodifica es el juego (ver
    // NetworkHost.StateProvider del lado host, y GameSnapshot.StatePayload
    // del lado cliente).
    public class GameSnapshot
    {
        public ulong Tick;
        public long Timestamp;
        public byte[] StatePayload = Array.Empty<byte>();
        public List<GameEvent> Events = new List<GameEvent>();

        // Wire format: Tick(8) + StatePayloadLen(4) + StatePayload(N) +
        // EventCount(1) + Events(PlayerId(2)+EventType(1)+DataLen(1)+Data).
        public byte[] Encode()
        {
            var buf = new List<byte>(9 + StatePayload.Length + 32);

            var tickBuf = new byte[8];
            BigEndian.WriteUInt64(tickBuf, 0, Tick);
            buf.AddRange(tickBuf);

            var lenBuf = new byte[4];
            BigEndian.WriteUInt32(lenBuf, 0, (uint)StatePayload.Length);
            buf.AddRange(lenBuf);
            buf.AddRange(StatePayload);

            buf.Add((byte)Events.Count);

            foreach (var e in Events)
            {
                buf.Add((byte)(e.PlayerId >> 8));
                buf.Add((byte)e.PlayerId);
                buf.Add(e.EventType);
                buf.Add((byte)e.Data.Length);
                buf.AddRange(e.Data);
            }

            return buf.ToArray();
        }

        // Inverso de Encode(): usado del lado cliente para leer el payload
        // que viene después del header de 9 bytes en un paquete PACKET_SNAPSHOT.
        // StatePayload queda sin interpretar — el juego lo decodifica con su
        // propio esquema (ver ejemplos/).
        //
        // TryDecode nunca lanza: un paquete corrupto, truncado, o de un
        // emisor con un formato de snapshot distinto (ej. server.go, que
        // sigue su propio esquema fijo) no debe poder tirar abajo el thread
        // de recepción del cliente. Valida bounds a mano en vez de confiar
        // en los valores de longitud que trae el propio payload.
        public static bool TryDecode(byte[] payload, out GameSnapshot? snapshot)
        {
            snapshot = null;
            try
            {
                int offset = 0;
                if (payload.Length < 8 + 4 + 1) return false;

                var s = new GameSnapshot { Tick = BigEndian.ReadUInt64(payload, offset) };
                offset += 8;

                uint stateLen = BigEndian.ReadUInt32(payload, offset);
                offset += 4;
                if (stateLen > int.MaxValue || offset + (int)stateLen > payload.Length) return false;

                s.StatePayload = new byte[stateLen];
                Array.Copy(payload, offset, s.StatePayload, 0, (int)stateLen);
                offset += (int)stateLen;

                if (offset >= payload.Length) return false;
                int eventCount = payload[offset++];

                for (int i = 0; i < eventCount; i++)
                {
                    if (offset + 4 > payload.Length) return false; // PlayerId(2)+EventType(1)+DataLen(1) mínimo

                    var e = new GameEvent { PlayerId = BigEndian.ReadUInt16(payload, offset) };
                    offset += 2;
                    e.EventType = payload[offset++];
                    byte dataLen = payload[offset++];
                    if (offset + dataLen > payload.Length) return false;

                    e.Data = new byte[dataLen];
                    Array.Copy(payload, offset, e.Data, 0, dataLen);
                    offset += dataLen;
                    s.Events.Add(e);
                }

                snapshot = s;
                return true;
            }
            catch
            {
                return false;
            }
        }
    }

    // Input del jugador: forma genérica (movimiento 2D + rotación + bitmask
    // de acciones) razonable para la mayoría de juegos arcade/tiempo real —
    // en taca-taca, DeltaX/DeltaY pueden mapear a desplazamiento de barra y
    // Rotation al giro; en otro juego pueden significar otra cosa. El core
    // no interpreta estos valores, solo los transporta y se los pasa al
    // juego vía NetworkHost.OnInput.
    public class PlayerInput
    {
        public ushort PlayerId;
        public uint Seq;
        public long Timestamp;
        public short DeltaX;
        public short DeltaY;
        public ushort Rotation;
        public uint Actions;

        // Payload de 10 bytes que va después del header en un PACKET_INPUT.
        public byte[] EncodePayload()
        {
            var buf = new byte[10];
            BigEndian.WriteInt16(buf, 0, DeltaX);
            BigEndian.WriteInt16(buf, 2, DeltaY);
            BigEndian.WriteUInt16(buf, 4, Rotation);
            BigEndian.WriteUInt32(buf, 6, Actions);
            return buf;
        }

        public static bool TryDecodePayload(byte[] payload, out short deltaX, out short deltaY, out ushort rotation, out uint actions)
        {
            deltaX = 0; deltaY = 0; rotation = 0; actions = 0;
            if (payload.Length < 10) return false;

            deltaX = BigEndian.ReadInt16(payload, 0);
            deltaY = BigEndian.ReadInt16(payload, 2);
            rotation = BigEndian.ReadUInt16(payload, 4);
            actions = BigEndian.ReadUInt32(payload, 6);
            return true;
        }
    }
}
