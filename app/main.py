from __future__ import annotations

import argparse
import base64
import dataclasses
import datetime
import functools
import itertools
import logging
import os
import random
import select
import socket
from typing import Protocol

from app import loop
from app import models
from app import serialization
from app import store

EMPTY_RDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="


def ping_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    if replication_connection:
        return
    client.send(b"+PONG\r\n")  # TODO: implement serialization


def echo_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    word = command.args[0]
    if replication_connection:
        return
    # TODO: implement serialization
    resp = b"$" + str(len(word)).encode() + b"\r\n" + word + b"\r\n"
    client.send(resp)


def get_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    key = command.args[0]
    result = app.store.get(key)
    if replication_connection:
        return
    response = result.serialize()
    client.send(response)


def set_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    key = command.args[0]
    value = command.args[1]
    expire = None

    if len(command.args) > 2:
        assert command.args[2].upper() == b"PX"

        ms = int(command.args[3])
        expire = datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(
            milliseconds=ms
        )

    result = app.store.set(key, value, expire)
    if replication_connection:
        return
    response = result.serialize()
    client.send(response)

    propagate = command.resp3().serialize()
    if app.params.master_replicas:
        print("Propagation to replicas:", propagate)
    for replica in app.params.master_replicas:
        replica.send(propagate)


def info_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    key = command.args[0]
    assert key.upper() == b"REPLICATION"
    if replication_connection:
        return
    role = "master" if app.params.master else "slave"
    result = models.BulkString(
        b=f"""\
# Replication
role:{role}
master_replid:{app.params.master_replid}
master_repl_offset:{app.params.master_repl_offset}
""".encode()
    )
    response = result.serialize()
    client.send(response)


def replconf_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    value = command.args[1]
    method = command.args[0].upper()

    if method == b"GETACK":
        result = models.Array(
            a=[
                models.BulkString(b=b"REPLCONF"),
                models.BulkString(b=b"ACK"),
                models.BulkString(b=str(app.params.replica_offset).encode()),
            ]
        )
        response = result.serialize()
        client.send(response)
        return
    if method == b"ACK":
        remote_offset = int(value)
        client.last_offset = remote_offset
        print("REMOTE OFFSET", remote_offset)
        return

    elif method in [b"LISTENING-PORT", b"CAPA"]:
        result = models.SimpleString(s="OK")
        response = result.serialize()
        client.send(response)
        return

    print("replconf_command", method)
    raise NotImplementedError


def psync_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    repl_id = command.args[0]
    repl_offset = command.args[1]
    print("REPLICA: PSYNC", repl_id, repl_offset)
    assert repl_id == b"?"
    assert int(repl_offset) == -1

    if replication_connection:
        return

    print(app.params.master_repl_offset)
    result = models.SimpleString(
        s=f"FULLRESYNC {app.params.master_replid} {app.params.master_repl_offset}"
    )
    response = result.serialize()
    client.send(response)

    # TODO: construct properly
    result = models.BulkString(b=base64.b64decode(EMPTY_RDB))
    response = result.serialize(trailing=False)
    client.send(response)
    client.replica = True
    client.expected_offset = 0
    app.params.master_replicas.append(client)  # TODO: remove on disconnect


def select_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    # TODO: implement
    pass


def wait_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    replicas = int(command.args[0])
    wait_ms = int(command.args[1])

    # print("MASTER WAIT", replicas, wait_ms)
    ready = sum(
        1
        for replica in app.params.master_replicas
        if replica.expected_offset == replica.last_offset
    )
    if ready >= min(replicas, len(app.params.master_replicas)):
        result = models.Integer(n=ready)
        response = result.serialize()
        client.send(response)
        return

    now = datetime.datetime.now(tz=datetime.UTC)
    end = now + datetime.timedelta(milliseconds=wait_ms) if wait_ms != 0 else None

    for replica in app.params.master_replicas:
        req = models.Array(
            a=[
                models.BulkString(b=b"REPLCONF"),
                models.BulkString(b=b"GETACK"),
                models.BulkString(b=b"*"),
            ]
        )
        replica.send(req.serialize())

    # print("SCHED")
    app.loop.add(
        functools.partial(wait_command_cont, client=client, end=end, replicas=replicas),
        later=datetime.timedelta(milliseconds=100),
    )


def wait_command_cont(
    app: models.App,
    *,
    client: models.Client,
    end: datetime.datetime | None,
    replicas: int,
):
    now = datetime.datetime.now(tz=datetime.UTC)
    ready = sum(
        1
        for replica in app.params.master_replicas
        if replica.expected_offset == replica.last_offset
    )
    # for replica in params.master_replicas:
    #     print("EXP, LAST, ", replica.expected_offset, replica.last_offset)
    # print("READY", ready)

    if ready >= replicas or (end is not None and now >= end):
        result = models.Integer(n=ready)
        response = result.serialize()
        client.send(response)
        return

    # print("SCHED")
    app.loop.add(
        functools.partial(wait_command_cont, client=client, end=end, replicas=replicas),
        later=datetime.timedelta(milliseconds=100),
    )


def config_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    method = command.args[0]
    item = command.args[1]
    assert method.upper() == b"GET"
    if item == b"dir":
        response = models.Array(
            a=[
                models.BulkString(b=b"dir"),
                models.BulkString(b=app.params.dir.encode()),
            ]
        )
        client.send(response.serialize())
        return
    if item == b"dbfilename":
        response = models.Array(
            a=[
                models.BulkString(b=b"dbfilename"),
                models.BulkString(b=app.params.dbfilename.encode()),
            ]
        )
        client.send(response.serialize())
        return

    print(item)
    raise NotImplementedError


def keys_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    mask = command.args[0]
    assert mask == b"*"
    response = app.store.keys()
    client.send(response.serialize())


def type_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    key = command.args[0]
    response = app.store.type(key)
    client.send(response.serialize())


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class Command:
    command: bytes
    args: list[bytes]

    def resp3(self):
        arr = [models.BulkString(b=self.command)] + [
            models.BulkString(b=arg) for arg in self.args
        ]
        return models.Array(a=arr)


def parse_command(item: models.ProtocolItem) -> Command:
    assert isinstance(item, models.Array)

    command = b""
    args: list[bytes] = []
    for i in item.a:
        assert isinstance(i, models.BulkString)
        if not command:
            command = i.b
            continue
        args.append(i.b)

    return Command(command=command, args=args)


def batched(lst, n):
    it = iter(lst)
    return iter(lambda: tuple(itertools.islice(it, n)), ())


def xadd_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    key = command.args[0]
    id_raw = command.args[1]

    # TODO 3.12 batched
    tuples = list(batched(command.args[2:], n=2))
    response = app.store.xadd(key, id_raw, tuples)
    client.send(response.serialize())


def xrange_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    key = command.args[0]
    start = command.args[1]
    end = command.args[2]

    response = app.store.xrange(key, start, end)
    client.send(response.serialize())


def xread_command(
    app: models.App,
    client: models.Client,
    command: Command,
    replication_connection: bool,
):
    args = command.args[:]
    first_arg = args[0]
    block_ms = None
    if first_arg.upper() == b"BLOCK":
        block_ms = int(command.args[1])
        args = args[2:]

    streams = args[0]
    assert streams.upper() == b"STREAMS"
    rest = args[1:]
    assert len(rest) % 2 == 0
    keys = rest[: len(rest) // 2]
    raw_ids = rest[len(rest) // 2 :]
    last_xids: dict[bytes, models.XID] = {}
    print("XREAD", block_ms, keys, raw_ids)

    response = app.store.xread(keys, raw_ids, last_xids)
    if response is not None:
        client.send(serialization.encode_array(response).serialize())
        return
    if block_ms is None:
        client.send(models.NullBulkString().serialize())
        return
    now = datetime.datetime.now(tz=datetime.UTC)
    end = now + datetime.timedelta(milliseconds=block_ms) if block_ms else None
    # print("SCHED")
    app.loop.add(
        functools.partial(
            xread_command_cont,
            keys=keys,
            raw_ids=raw_ids,
            last_xids=last_xids,
            client=client,
            end=end,
        ),
        later=datetime.timedelta(milliseconds=100),
    )


def xread_command_cont(
    app: models.App,
    *,
    keys: list[bytes],
    raw_ids: list[bytes],
    last_xids: dict[bytes, models.XID],
    client: models.Client,
    end: datetime.datetime | None,
):
    response = app.store.xread(keys, raw_ids, last_xids)
    if response is not None:
        client.send(serialization.encode_array(response).serialize())
        return
    now = datetime.datetime.now(tz=datetime.UTC)
    if end is not None and now >= end:
        client.send(models.NullBulkString().serialize())
        return
    # print("SCHED")
    app.loop.add(
        functools.partial(
            xread_command_cont,
            keys=keys,
            raw_ids=raw_ids,
            last_xids=last_xids,
            client=client,
            end=end,
        ),
        later=datetime.timedelta(milliseconds=100),
    )


class CommandProtocol(Protocol):
    def __call__(
        self,
        app: models.App,
        client: models.Client,
        command: Command,
        replication_connection: bool,
    ):
        ...


f_mapping: dict[bytes, CommandProtocol] = {
    b"PING": ping_command,
    b"ECHO": echo_command,
    b"GET": get_command,
    b"SET": set_command,
    b"INFO": info_command,
    b"REPLCONF": replconf_command,
    b"PSYNC": psync_command,
    b"SELECT": select_command,
    b"WAIT": wait_command,
    b"CONFIG": config_command,
    b"KEYS": keys_command,
    b"TYPE": type_command,
    b"XADD": xadd_command,
    b"XRANGE": xrange_command,
    b"XREAD": xread_command,
}


def serve_client(app: models.App, client: models.Client):
    while True:
        if not client.data:
            break

        result = serialization.read_next_value(client.data)
        if result is None:
            break

        item, client.data, parsed = result
        command = parse_command(item)
        f = f_mapping.get(command.command.upper())
        if f is None:
            raise NotImplementedError

        f(app, client, command, replication_connection=False)
        # params.master_repl_offset += parsed


def serve_master(app: models.App, client: models.Client):
    while True:
        if not client.data:
            break

        if app.params.replica_flow == 4:
            result = serialization.read_next_value_rdb(client.data)
        else:
            result = serialization.read_next_value(client.data)
        if result is None:
            break

        item, client.data, parsed = result
        if app.params.replica_flow < 4:
            assert isinstance(item, models.SimpleString)
        elif app.params.replica_flow == 4:
            # TODO: apply rdb from master
            assert isinstance(item, models.BulkString)
            app.params.replica_offset = 0
        else:  # REPLICATION is ESTABLISHED PARSE AS CLIENT BUT DO NOT RESPOND
            # TODO: code duplication
            command = parse_command(item)
            f = f_mapping.get(command.command.upper())
            if f is None:
                raise NotImplementedError

            print(
                "REPLICATION:",
                app.params.replica_offset,
                parsed,
                app.params.replica_offset + parsed,
                item,
            )
            f(app, client, command, replication_connection=True)
            app.params.replica_offset += parsed
            # client.send(Array(a=[BulkString(b=b"PING")]).serialize())
            continue

        app.params.replica_flow += 1
        # print("FROM MASTER:", params.replica_flow, item)
        if app.params.replica_flow == 1:
            data = models.Array(
                a=[
                    models.BulkString(b=b"REPLCONF"),
                    models.BulkString(b=b"listening-port"),
                    models.BulkString(b=str(app.params.port).encode()),
                ]
            )
            payload = data.serialize()
            client.send(payload)
        if app.params.replica_flow == 2:
            data = models.Array(
                a=[
                    models.BulkString(b=b"REPLCONF"),
                    models.BulkString(b=b"capa"),
                    models.BulkString(b=b"psync2"),
                ]
            )
            payload = data.serialize()
            client.send(payload)
        if app.params.replica_flow == 3:
            app.params.replica_offset = -1
            data = models.Array(
                a=[
                    models.BulkString(b=b"PSYNC"),
                    models.BulkString(b=b"?"),
                    models.BulkString(b=str(app.params.replica_offset).encode()),
                ]
            )
            payload = data.serialize()
            client.send(payload)


def main():
    with open("dump.rdb", "wb") as file:
        file.write(base64.b64decode(EMPTY_RDB))
    parser = argparse.ArgumentParser(
        prog="CodeCrafters Redis Python",
        description="Custom Redis Implementation",
        epilog="2024 @ Serhii Charykov",
    )
    parser.add_argument("--port", default=6379, type=int)
    parser.add_argument("--replicaof", nargs="+", default=[])
    parser.add_argument("--dir", default=".")
    parser.add_argument("--dbfilename", default="dump.rdb")
    args = parser.parse_args()
    params = models.Params(
        master_replid=random.randbytes(20).hex(),
        port=args.port,
        dir=args.dir,
        dbfilename=args.dbfilename,
    )

    if args.replicaof:
        params.master = False
        params.master_host = args.replicaof[0]
        params.master_port = int(args.replicaof[1])

    print(f"Params : {params}")
    print("Starting Redis on port {}".format(params.port))
    os.chdir(params.dir)

    replica = None
    if not params.master:
        replica_socket = socket.create_connection(
            (params.master_host, params.master_port)
        )
        replica_socket.setblocking(False)
        ping = models.Array(a=[models.BulkString(b=b"PING")])
        payload = ping.serialize()
        replica = models.Client(socket=replica_socket)
        replica.send(payload)

    server_socket = socket.create_server(("localhost", params.port), reuse_port=True)
    server_socket.setblocking(False)
    client_sockets: dict[socket.socket, models.Client] = {}

    event_loop = loop.EventLoop()
    storage = store.read_rdb(params)
    app = models.App(loop=event_loop, params=params, store=storage)
    event_loop.register(app)

    while True:
        read_sockets = list(client_sockets)
        read_sockets.append(server_socket)
        if not params.master and params.replica_active:
            read_sockets.append(replica.socket)

        timeout = event_loop.get_timeout()
        read, _, _ = select.select(read_sockets, [], [], timeout)
        for sock in read:
            if sock == server_socket:
                client, address = sock.accept()
                client_sockets[client] = models.Client(socket=client)
                continue

            if replica and sock == replica.socket:
                recv = sock.recv(4096)
                if not recv:
                    logging.warning(f"Master connection closed")
                    params.replica_active = False
                    continue
                replica.data += recv
                serve_master(app, replica)
                continue

            recv = sock.recv(4096)
            if not recv:
                sock.close()
                del client_sockets[sock]
                continue

            client = client_sockets[sock]
            client.data += recv
            serve_client(app, client)

        event_loop.run_cycle()


if __name__ == "__main__":
    main()
