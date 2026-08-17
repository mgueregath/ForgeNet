#!/usr/bin/env python3
"""Harness de validación de networkcore.py — análogo a
nucleo-multiplayer/go/networkcore/host_test.go y
nucleo-multiplayer/rust/networkcore/tests/host_test.rs.

Ejecutar: python3 test_networkcore.py
"""

import sys
import time

from networkcore import (
    REASON_ROOM_FULL,
    REASON_ROOM_NOT_FOUND,
    GameEvent,
    NetworkClient,
    NetworkHost,
    Server,
)
from ejemplo_tacataca import MAX_PADDLES, ROLE_BOARD, ROLE_PADDLE, TacaTacaGame, TacaTacaState

failures = 0


def check(name: str, condition: bool):
    global failures
    if condition:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name}")
        failures += 1


def test_generic_mechanisms():
    print("== Test 1: NetworkHost <-> NetworkClient via Server (mecanismos genéricos) ==")

    fake_state = bytes([10, 20, 30, 40, 50])
    received_inputs = []
    hosts = []

    def room_factory():
        host = NetworkHost()
        host.state_provider = lambda: fake_state
        host.on_input = lambda inp: received_inputs.append(inp)
        hosts.append(host)
        return host

    server = Server(room_factory)
    server.start_udp(29999)
    time.sleep(0.3)

    client = NetworkClient()

    snapshot_with_event = {"value": None}
    client.on_snapshot = lambda snap: (
        snapshot_with_event.__setitem__("value", snap) if snap.events and snapshot_with_event["value"] is None else None
    )

    client.connect("127.0.0.1", 29999)
    connected = client.create_room(role=1)
    check("handshake conecta (create_room)", connected)
    check("playerId asignado (>0)", client.player_id > 0)
    check("room_code recibido del server", bool(client.room_code))

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

    host = hosts[0]
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
    server.stop()


def test_heartbeat_survives():
    print()
    print("== Test 2: heartbeat sobrevive >10s con actividad ==")

    hosts = []

    def room_factory():
        host = NetworkHost()
        hosts.append(host)
        return host

    server = Server(room_factory)
    server.start_udp(29998)
    time.sleep(0.3)

    client = NetworkClient()
    client.connect("127.0.0.1", 29998)
    client.create_room(role=1)

    start = time.time()
    while time.time() - start < 12:
        client.send_input(1, 1, 0, 0)
        time.sleep(0.5)

    check("cliente sigue conectado en el host tras 12s", hosts[0].connected_player_count() == 1)

    client.disconnect()
    server.stop()


def test_tacataca_example():
    print()
    print("== Test 3: ejemplo taca-taca sobre el core genérico (via Server, con role) ==")

    def room_factory():
        host = NetworkHost()
        game = TacaTacaGame()
        game.attach_to(host)
        return host

    server = Server(room_factory, max_players_per_room=MAX_PADDLES + 1)
    server.start_udp(29997)
    time.sleep(0.3)

    client = NetworkClient()
    client.connect("127.0.0.1", 29997)
    connected = client.create_room(role=ROLE_PADDLE)
    check("handshake conecta (taca-taca, ROLE_PADDLE)", connected)

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

    # Un segundo cliente con ROLE_BOARD se une a la misma sala: debe poder
    # conectarse (ve la partida completa) pero no ocupar cupo de barra.
    board = NetworkClient()
    board.connect("127.0.0.1", 29997)
    board_connected = board.join_room(role=ROLE_BOARD, room_code=client.room_code)
    check("tablero (ROLE_BOARD) se une a la misma sala", board_connected and board.room_code == client.room_code)

    time.sleep(0.3)
    deadline = time.time() + 2
    still_one_rod = False
    while time.time() < deadline:
        snap = board.latest_snapshot
        if snap is not None and snap.state_payload:
            candidate = TacaTacaState.decode(snap.state_payload)
            still_one_rod = len(candidate.rods) == 1
            if still_one_rod:
                break
        time.sleep(0.05)
    check("el tablero no ocupa cupo de barra (rods sigue en 1)", still_one_rod)

    board.disconnect()
    client.disconnect()
    server.stop()


def test_create_and_join_room():
    print()
    print("== Test 4: crear sala + unirse por código termina en la misma sala ==")

    created_hosts = []

    def room_factory():
        host = NetworkHost()
        created_hosts.append(host)
        return host

    server = Server(room_factory)
    server.start_udp(29996)
    time.sleep(0.3)

    creator = NetworkClient()
    creator.connect("127.0.0.1", 29996)
    check("creador conecta (create_room)", creator.create_room(role=1))
    check("creador recibió un código de sala", bool(creator.room_code))

    joiner = NetworkClient()
    joiner.connect("127.0.0.1", 29996)
    check("joiner se une con el código del creador", joiner.join_room(role=2, room_code=creator.room_code))
    check("joiner terminó en la misma sala", joiner.room_code == creator.room_code)
    check("PlayerIDs distintos", creator.player_id != joiner.player_id)
    check(f"se creó una sola sala ({len(created_hosts)})", len(created_hosts) == 1)
    if created_hosts:
        check(
            f"2 clientes conectados en la sala ({created_hosts[0].connected_player_count()})",
            created_hosts[0].connected_player_count() == 2,
        )

    creator.disconnect()
    joiner.disconnect()
    server.stop()


def test_join_nonexistent_room_rejected():
    print()
    print("== Test 5: unirse a una sala inexistente es rechazado ==")

    server = Server(lambda: NetworkHost())
    server.start_udp(29995)
    time.sleep(0.3)

    client = NetworkClient()
    client.connect("127.0.0.1", 29995)
    ok = client.join_room(role=1, room_code="ZZZZZZ")
    check("join a sala inexistente falla", not ok)
    check("motivo de rechazo es ReasonRoomNotFound", client.last_reject_reason == REASON_ROOM_NOT_FOUND)

    server.stop()


def test_max_players_per_room_rejects_extra_joins():
    print()
    print("== Test 6: max_players_per_room rechaza joins de más (tope numérico) ==")

    server = Server(lambda: NetworkHost(), max_players_per_room=2)
    server.start_udp(29994)
    time.sleep(0.3)

    creator = NetworkClient()
    creator.connect("127.0.0.1", 29994)
    creator.create_room(role=1)

    second = NetworkClient()
    second.connect("127.0.0.1", 29994)
    check("segundo cliente entra dentro del tope", second.join_room(role=1, room_code=creator.room_code))

    third = NetworkClient()
    third.connect("127.0.0.1", 29994)
    ok = third.join_room(role=1, room_code=creator.room_code)
    check("tercer cliente rechazado por tope", not ok)
    check("motivo de rechazo es ReasonRoomFull", third.last_reject_reason == REASON_ROOM_FULL)

    creator.disconnect()
    second.disconnect()
    server.stop()


def test_reliable_event_retransmits_until_acked():
    print()
    print("== Test 7: evento reliable se retransmite hasta ser confirmado ==")

    hosts = []

    def room_factory():
        host = NetworkHost()
        hosts.append(host)
        return host

    server = Server(room_factory)
    server.start_udp(29993)
    time.sleep(0.3)

    client = NetworkClient()
    client.connect("127.0.0.1", 29993)
    client.create_room(role=1)

    received = {"event": None}

    def on_snapshot(snap):
        if snap.events and received["event"] is None:
            received["event"] = snap.events[0]

    client.on_snapshot = on_snapshot

    host = hosts[0]
    host.queue_reliable_event(GameEvent(player_id=client.player_id, event_type=9, data=bytes([7])))

    deadline = time.time() + 2
    while time.time() < deadline and received["event"] is None:
        time.sleep(0.02)
    check("evento reliable llega al cliente", received["event"] is not None)
    if received["event"] is not None:
        check("event_type preservado (9)", received["event"].event_type == 9)
        check("data preservada ([7])", received["event"].data == bytes([7]))

    # El cliente confirma automáticamente al recibir un PacketSnapshot con
    # FLAG_RELIABLE (ver NetworkClient._handle_packet) — la cola de
    # retransmisión del host debe vaciarse solita, sin que el test tenga
    # que ackear a mano.
    deadline = time.time() + 1
    while time.time() < deadline and host.pending_reliable_count(client.player_id) != 0:
        time.sleep(0.02)
    check(
        "la cola de retransmisión se vació tras el ack del cliente",
        host.pending_reliable_count(client.player_id) == 0,
    )

    client.disconnect()
    server.stop()


if __name__ == "__main__":
    test_generic_mechanisms()
    test_heartbeat_survives()
    test_tacataca_example()
    test_create_and_join_room()
    test_join_nonexistent_room_rejected()
    test_max_players_per_room_rejects_extra_joins()
    test_reliable_event_retransmits_until_acked()

    print()
    if failures == 0:
        print("TODO OK")
        sys.exit(0)
    else:
        print(f"{failures} FALLO(S)")
        sys.exit(1)
