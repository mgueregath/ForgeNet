#!/usr/bin/env python3
"""Ejemplo de "juego" construido ENCIMA de networkcore.py, no dentro de él.

Prueba que el core es de verdad genérico: NetworkHost no sabe qué es una
"barra" ni una "pelota" — este módulo es el que define ese esquema y lo
conecta a los hooks genéricos (on_input/on_tick/state_provider/queue_event).
Análogo a nucleo-multiplayer/csharp/NetworkCore.Tests/TacaTacaExample.cs y
nucleo-multiplayer/go/ejemplo-tacataca/main.go.
"""

import struct
import threading
from dataclasses import dataclass, field
from typing import List

from networkcore import GameEvent, NetworkHost, PlayerInput

EVENT_TYPE_GOAL = 1


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

    def _on_connected(self, player_id: int):
        with self._lock:
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
    host = NetworkHost()
    game = TacaTacaGame()
    game.attach_to(host)
    host.start(9999)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        host.stop()


if __name__ == "__main__":
    main()
