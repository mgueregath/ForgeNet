using System;
using System.Collections.Generic;
using NetworkCore;

namespace NetworkCore.Tests
{
    // Ejemplo de "juego" construido ENCIMA de NetworkCore, no dentro de él.
    // Existe para probar que el core es de verdad genérico: NetworkHost no
    // sabe qué es una "barra" ni una "pelota" — esta clase es la que define
    // ese esquema y lo conecta a los hooks genéricos (OnInput/OnTick/
    // StateProvider/QueueEvent). Si mañana hay otro juego con otro esquema,
    // se escribe una clase análoga a esta, sin tocar NetworkCore.
    public enum TacaTacaEventType : byte
    {
        Goal = 1,
    }

    // Roles de taca-taca — opacos para el core, definidos y usados solo acá.
    // RoleBoard es el tablero (recibe el mismo snapshot que todos, pero no
    // controla ninguna barra); RolePaddle es un mando/jugador. Análogo a
    // roleBoard/rolePaddle en go/ejemplo-tacataca/main.go.
    public static class TacaTacaRoles
    {
        public const byte Board = 0x01;
        public const byte Paddle = 0x02;
    }

    public class RodState
    {
        public ushort PlayerId;
        public int Position; // desplazamiento lateral de la barra
        public ushort Rotation; // ángulo de giro
    }

    public class TacaTacaState
    {
        public int BallX;
        public int BallY;
        public byte ScoreTeamA;
        public byte ScoreTeamB;
        public List<RodState> Rods = new List<RodState>();

        // Formato propio del ejemplo (no es parte del protocolo core —
        // vive dentro de StatePayload, que el core trata como bytes opacos):
        // BallX(4) + BallY(4) + ScoreA(1) + ScoreB(1) + RodCount(1) +
        // por barra: PlayerId(2) + Position(4) + Rotation(2)
        public byte[] Encode()
        {
            var buf = new List<byte>(10 + Rods.Count * 8);
            var tmp4 = new byte[4];

            BigEndian.WriteInt32(tmp4, 0, BallX); buf.AddRange(tmp4);
            BigEndian.WriteInt32(tmp4, 0, BallY); buf.AddRange(tmp4);
            buf.Add(ScoreTeamA);
            buf.Add(ScoreTeamB);
            buf.Add((byte)Rods.Count);

            foreach (var r in Rods)
            {
                var tmp2 = new byte[2];
                BigEndian.WriteUInt16(tmp2, 0, r.PlayerId); buf.AddRange(tmp2);
                BigEndian.WriteInt32(tmp4, 0, r.Position); buf.AddRange(tmp4);
                BigEndian.WriteUInt16(tmp2, 0, r.Rotation); buf.AddRange(tmp2);
            }

            return buf.ToArray();
        }

        public static TacaTacaState Decode(byte[] data)
        {
            int offset = 0;
            var state = new TacaTacaState
            {
                BallX = BigEndian.ReadInt32(data, offset)
            };
            offset += 4;
            state.BallY = BigEndian.ReadInt32(data, offset);
            offset += 4;
            state.ScoreTeamA = data[offset++];
            state.ScoreTeamB = data[offset++];
            byte rodCount = data[offset++];

            for (int i = 0; i < rodCount; i++)
            {
                var rod = new RodState
                {
                    PlayerId = BigEndian.ReadUInt16(data, offset)
                };
                offset += 2;
                rod.Position = BigEndian.ReadInt32(data, offset);
                offset += 4;
                rod.Rotation = BigEndian.ReadUInt16(data, offset);
                offset += 2;
                state.Rods.Add(rod);
            }

            return state;
        }
    }

    // Conecta TacaTacaState a un NetworkHost genérico vía sus hooks.
    // NetworkHost no conoce esta clase — es este lado el que se suscribe.
    public class TacaTacaGame
    {
        // Cupo de mandos por partida — regla de taca-taca, no del core
        // (NetworkHost/Server no distinguen roles, así que un límite que
        // dependa del rol es responsabilidad del juego, no de acá).
        private const int MaxPaddles = 2;

        private readonly TacaTacaState _state = new TacaTacaState();
        private readonly object _lock = new object();

        public void AttachTo(NetworkHost host)
        {
            // Role es opaco para el core: acá es donde taca-taca decide qué
            // hacer con cada valor. El tablero (TacaTacaRoles.Board) recibe
            // el mismo snapshot que todos (ve la partida completa) pero no
            // controla ninguna barra. Un mando (TacaTacaRoles.Paddle) más
            // allá del cupo tampoco recibe barra — se queda conectado, pero
            // sin efecto en el juego.
            host.OnPlayerConnected += (id, role) =>
            {
                if (role != TacaTacaRoles.Paddle) return;
                lock (_lock)
                {
                    if (_state.Rods.Count >= MaxPaddles) return;
                    _state.Rods.Add(new RodState { PlayerId = id, Position = 0, Rotation = 0 });
                }
            };
            host.OnPlayerDisconnected += id =>
            {
                lock (_lock) _state.Rods.RemoveAll(r => r.PlayerId == id);
            };
            host.OnInput += input =>
            {
                lock (_lock)
                {
                    foreach (var rod in _state.Rods)
                    {
                        if (rod.PlayerId != input.PlayerId) continue;
                        rod.Position += input.DeltaX; // el juego decide qué significa DeltaX
                        rod.Rotation = input.Rotation;

                        if ((input.Actions & 0x01) != 0) // bit 0 = "gol" (para el test)
                        {
                            _state.ScoreTeamA++;
                            host.QueueEvent(new GameEvent
                            {
                                PlayerId = input.PlayerId,
                                EventType = (byte)TacaTacaEventType.Goal,
                                Data = Array.Empty<byte>()
                            });
                        }
                        break;
                    }
                }
            };
            host.StateProvider = () =>
            {
                lock (_lock) return _state.Encode();
            };
        }
    }
}
