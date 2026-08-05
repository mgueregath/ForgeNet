#!/usr/bin/env python3
"""Harness de validación de networkcore.py — análogo a
nucleo-multiplayer/go/networkcore/host_test.go y
nucleo-multiplayer/rust/networkcore/tests/host_test.rs.

Ejecutar: python3 test_networkcore.py
"""

import sys
import time

from networkcore import GameEvent, GameSnapshot, NetworkClient, NetworkHost
from ejemplo_tacataca import TacaTacaGame, TacaTacaState

failures = 0


def check(name: str, condition: bool):
    global failures
    if condition:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name}")
        failures += 1


def test_generic_mechanisms():
    print("== Test 1: NetworkHost <-> NetworkClient (mecanismos genéricos) ==")

    host = NetworkHost()
    fake_state = bytes([10, 20, 30, 40, 50])
    host.state_provider = lambda: fake_state

    received_inputs = []
    host.on_input = lambda inp: received_inputs.append(inp)

    host.start(29999)
    time.sleep(0.3)

    client = NetworkClient()

    snapshot_with_event = {"value": None}
    client.on_snapshot = lambda snap: (
        snapshot_with_event.__setitem__("value", snap) if snap.events and snapshot_with_event["value"] is None else None
    )

    connected = client.connect("127.0.0.1", 29999)
    check("handshake conecta", connected)
    check("playerId asignado (>0)", client.player_id > 0)

    client.send_input(15, -7, 90, 0)
    client.send_input(3, 3, 45, 0x99, reliable=True)

    deadline = time.time() + 2
    while time.time() < deadline and len(received_inputs) < 2:
        time.sleep(0.02)

    check(f"OnInput recibió los 2 inputs (llegaron {len(received_inputs)})", len(received_inputs) == 2)
    if len(received_inputs) == 2:
        first_ok = received_inputs[0].delta_x == 15 and received_inputs[0].rotation == 90
        second_ok = received_inputs[1].delta_x == 3 and received_inputs[1].rotation == 45 and received_inputs[1].actions == 0x99
        check("1er input crudo sin interpretar (15/90)", first_ok)
        check("2do input crudo sin interpretar (3/45/0x99)", second_ok)

    deadline = time.time() + 2
    while time.time() < deadline and (client.latest_snapshot is None or not client.latest_snapshot.state_payload):
        time.sleep(0.02)

    check("snapshot recibido", client.latest_snapshot is not None)
    if client.latest_snapshot is not None:
        check(
            "StatePayload llega byte a byte igual al que provee el juego",
            client.latest_snapshot.state_payload == fake_state,
        )

    host.queue_event(GameEvent(player_id=client.player_id, event_type=42, data=bytes([9, 9])))
    deadline = time.time() + 2
    while time.time() < deadline and snapshot_with_event["value"] is None:
        time.sleep(0.02)

    check("evento custom (queue_event) llega al cliente", snapshot_with_event["value"] is not None)
    if snapshot_with_event["value"] is not None:
        evt = snapshot_with_event["value"].events[0]
        check(f"EventType arbitrario preservado (42 -> {evt.event_type})", evt.event_type == 42)
        check("Data arbitraria preservada", evt.data == bytes([9, 9]))

    pong = {"ms": None}
    client.on_pong = lambda ms: pong.__setitem__("ms", ms)
    client.send_ping()
    deadline = time.time() + 2
    while time.time() < deadline and pong["ms"] is None:
        time.sleep(0.02)
    check("ping/pong responde", pong["ms"] is not None)

    client.disconnect()
    host.stop()


def test_heartbeat_survives():
    print()
    print("== Test 2: heartbeat sobrevive >10s con actividad ==")

    host = NetworkHost()
    host.start(29998)
    time.sleep(0.3)

    client = NetworkClient()
    client.connect("127.0.0.1", 29998)

    start = time.time()
    while time.time() - start < 12:
        client.send_input(1, 1, 0, 0)
        time.sleep(0.5)

    check("cliente sigue conectado en el host tras 12s", host.connected_player_count() == 1)

    client.disconnect()
    host.stop()


def test_tacataca_example():
    print()
    print("== Test 3: ejemplo taca-taca sobre el core genérico ==")

    host = NetworkHost()
    game = TacaTacaGame()
    game.attach_to(host)
    host.start(29997)
    time.sleep(0.3)

    client = NetworkClient()
    connected = client.connect("127.0.0.1", 29997)
    check("handshake conecta (taca-taca)", connected)

    client.send_input(25, 0, 30, 0)
    client.send_input(0, 0, 30, 0x01, reliable=True)  # "meter un gol"

    decoded = None
    deadline = time.time() + 3
    while time.time() < deadline:
        snap = client.latest_snapshot
        if snap is not None and snap.state_payload:
            candidate = TacaTacaState.decode(snap.state_payload)
            if len(candidate.rods) == 1 and candidate.rods[0].position == 25:
                decoded = candidate
                break
        time.sleep(0.05)

    check("estado de taca-taca decodificado desde StatePayload", decoded is not None)
    if decoded is not None:
        check(f"posición de barra aplicada (25 -> {decoded.rods[0].position})", decoded.rods[0].position == 25)
        check(f"rotación de barra aplicada (30 -> {decoded.rods[0].rotation})", decoded.rods[0].rotation == 30)
        check(f"gol contabilizado (1 -> {decoded.score_team_a})", decoded.score_team_a == 1)

    client.disconnect()
    host.stop()


if __name__ == "__main__":
    test_generic_mechanisms()
    test_heartbeat_survives()
    test_tacataca_example()

    print()
    if failures == 0:
        print("TODO OK")
        sys.exit(0)
    else:
        print(f"{failures} FALLO(S)")
        sys.exit(1)
