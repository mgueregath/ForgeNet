#!/usr/bin/env python3
"""Ejemplo de "juego" construido ENCIMA de networkcore.py, no dentro de él.

Prueba que el core es de verdad genérico: ni NetworkHost ni Server saben
qué es una "barra", una "pelota" ni un "tablero" — este módulo es el que
define ese esquema y lo conecta a los hooks genéricos
(on_input/on_tick/state_provider/queue_event), incluido el significado de
role, que el core solo transporta sin interpretar.
Análogo a nucleo-multiplayer/csharp/NetworkCore.Tests/TacaTacaExample.cs y
nucleo-multiplayer/go/ejemplo-tacataca/main.go.
"""

import struct
import threading
from dataclasses import dataclass, field
from typing import List

from networkcore import GameEvent, NetworkHost, PlayerInput, Server

EVENT_TYPE_GOAL = 1

# Roles de taca-taca — opacos para el core, definidos y usados solo acá
# (mismo diseño que go/ejemplo-tacataca/main.go). ROLE_BOARD es el tablero
# (recibe el mismo snapshot que todos, pero no controla ninguna barra);
# ROLE_PADDLE es un mando/jugador.
ROLE_BOARD = 0x01
ROLE_PADDLE = 0x02

# MAX_PADDLES: cupo de mandos por partida — regla de taca-taca, no del core
# (Server.max_players_per_room es un tope numérico genérico que no
# distingue roles; este chequeo sí lo hace).
MAX_PADDLES = 2


@dataclass
class RodState:
    player_id: int
    position: int = 0
    rotation: int = 0


@dataclass
class TacaTacaState:
    ball_x: int = 0
    ball_y: int = 0
    score_team_a: int = 0
    score_team_b: int = 0
    rods: List[RodState] = field(default_factory=list)

    def encode(self) -> bytes:
        """Formato propio de este ejemplo (no es parte del protocolo core —
        vive dentro de state_payload, que el core trata como bytes
        opacos): BallX(4) + BallY(4) + ScoreA(1) + ScoreB(1) + RodCount(1)
        + por barra: PlayerID(2) + Position(4) + Rotation(2)."""
        buf = struct.pack(">iiBBB", self.ball_x, self.ball_y, self.score_team_a, self.score_team_b, len(self.rods))
        for r in self.rods:
            buf += struct.pack(">HiH", r.player_id, r.position, r.rotation)
        return buf

    @staticmethod
    def decode(data: bytes) -> "TacaTacaState":
        ball_x, ball_y, score_a, score_b, rod_count = struct.unpack(">iiBBB", data[:11])
        rods = []
        offset = 11
        for _ in range(rod_count):
            player_id, position, rotation = struct.unpack(">HiH", data[offset : offset + 8])
            rods.append(RodState(player_id, position, rotation))
            offset += 8
        return TacaTacaState(ball_x, ball_y, score_a, score_b, rods)


class TacaTacaGame:
    """Conecta TacaTacaState a un NetworkHost genérico vía sus hooks.
    NetworkHost no conoce esta clase — es este lado el que se suscribe."""

    def __init__(self):
        self.state = TacaTacaState()
        self._lock = threading.Lock()

    def attach_to(self, host: NetworkHost):
        host.on_player_connected = self._on_connected
        host.on_player_disconnected = self._on_disconnected
        host.on_input = lambda input: self._on_input(host, input)
        host.state_provider = self._encode_state

    def _on_connected(self, player_id: int, role: int, reconnected: bool):
        # role es opaco para el core: acá es donde taca-taca decide qué
        # hacer con cada valor. El tablero (ROLE_BOARD) recibe el mismo
        # snapshot que todos (ve la partida completa) pero no controla
        # ninguna barra. Un mando (ROLE_PADDLE) más allá del cupo tampoco
        # recibe barra — se queda conectado, pero sin efecto en el juego.
        #
        # reconnected (reconexión reconocida por IP, ver
        # NetworkHost.admit_player) no se usa acá: este ejemplo no
        # conserva posición/rotación entre reconexiones — _on_disconnected
        # ya borra la barra, así que reconectar arma una nueva desde cero.
        # Un juego real que quiera conservar estado chequearía este flag
        # para NO pisar el estado existente.
        if role != ROLE_PADDLE:
            return
        with self._lock:
            if len(self.state.rods) >= MAX_PADDLES:
                return
            self.state.rods.append(RodState(player_id=player_id))

    def _on_disconnected(self, player_id: int):
        with self._lock:
            self.state.rods = [r for r in self.state.rods if r.player_id != player_id]

    def _on_input(self, host: NetworkHost, input: PlayerInput):
        goal = False
        with self._lock:
            rod = next((r for r in self.state.rods if r.player_id == input.player_id), None)
            if rod is None:
                return

            rod.position += input.delta_x  # el juego decide qué significa delta_x
            rod.rotation = input.rotation

            if input.actions & 0x01:  # bit 0 = "gol" (definido acá, no por el core)
                self.state.score_team_a += 1
                goal = True

        if goal:
            host.queue_event(GameEvent(player_id=input.player_id, event_type=EVENT_TYPE_GOAL, data=b""))

    def _encode_state(self) -> bytes:
        with self._lock:
            return self.state.encode()


def main():
    # room_factory: se llama una vez por sala creada (HANDSHAKE_MODE_CREATE)
    # — cada partida de taca-taca es independiente, con su propio estado
    # (pelota, score, barras). El Server no sabe nada de esto: solo llama a
    # la factory y le rutea paquetes a lo que devuelve.
    def room_factory() -> NetworkHost:
        host = NetworkHost()
        game = TacaTacaGame()
        game.attach_to(host)
        return host

    # max_players_per_room es el tope genérico del Server (cuenta clientes,
    # sin distinguir roles) — para taca-taca da igual a MAX_PADDLES+1
    # (tablero). El cupo específico por rol (2 barras exactas) lo aplica
    # TacaTacaGame._on_connected arriba.
    server = Server(room_factory, max_players_per_room=MAX_PADDLES + 1)
    server.start_udp(9999)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
