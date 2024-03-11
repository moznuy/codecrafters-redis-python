from __future__ import annotations

import argparse
import base64
import dataclasses
import datetime
import functools
import logging
import random
import select
import socket
import time
from typing import Callable
from typing import Protocol


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class ProtocolItem:
    def serialize(self) -> bytes:
        raise NotImplementedError


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class SimpleString(ProtocolItem):
    s: str

    def serialize(self) -> bytes:
        return b"+" + self.s.encode() + b"\r\n"


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class Integer(ProtocolItem):
    n: int

    def serialize(self) -> bytes:
        return b":" + str(self.n).encode() + b"\r\n"


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class BulkString(ProtocolItem):
    b: bytes

    def serialize(self, trailing=True) -> bytes:
        result = b"$" + str(len(self.b)).encode() + b"\r\n" + self.b
        if trailing:
            result += b"\r\n"
        return result


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class NullBulkString(ProtocolItem):
    def serialize(self) -> bytes:
        return b"$-1\r\n"


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class Array(ProtocolItem):
    a: list[ProtocolItem]

    def serialize(self) -> bytes:
        result = b"*" + str(len(self.a)).encode() + b"\r\n"
        for item in self.a:
            result += item.serialize()
        return result


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class StorageMeta:
    expire: datetime.datetime | None = None


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class StorageValue:
    pass


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class Simple(StorageValue):
    b: bytes


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class StorageItem:
    meta: StorageMeta
    value: StorageValue


class Storage:
    def __init__(self):
        self.storage: dict[bytes, StorageItem] = {}

    def get(self, key: bytes) -> ProtocolItem:
        if key not in self.storage:
            return NullBulkString()
        item = self.storage[key]
        if item.meta.expire:
            now = datetime.datetime.now(tz=datetime.UTC)
            if now > item.meta.expire:  # TODO: passive deletion? or not?
                del self.storage[key]
                return NullBulkString()
        assert isinstance(item.value, Simple)
        return BulkString(b=item.value.b)

    def set(self, key: bytes, value: bytes, expire: datetime.datetime | None = None):
        self.storage[key] = StorageItem(
            meta=StorageMeta(expire=expire), value=Simple(b=value)
        )
        return SimpleString(s="OK")


def read_next_value(data: bytes) -> tuple[ProtocolItem, bytes, int] | None:
    assert data

    byte, data = data[:1], data[1:]
    parsed = 1
    if byte == b"+":  # Simple String
        index = data.find(b"\r\n")
        if index == -1:  # Not enough data
            return None
        raw_string, data = data[:index], data[index + 2 :]
        parsed += index + 2
        return SimpleString(s=raw_string.decode()), data, parsed

    if byte == b"$":  # Bulk String
        index = data.find(b"\r\n")
        if index == -1:  # Not enough data
            return None
        length = int(data[:index])  # TODO: protocol error
        index2 = data.find(b"\r\n", index + 2)
        if index2 == -1:  # Not enough data
            return None
        s = data[index + 2 : index2]
        data = data[index2 + 2 :]
        parsed += index2 + 2
        assert length == len(s)  # TODO: protocol error
        return BulkString(b=s), data, parsed

    if byte == b"*":  # Array
        index = data.find(b"\r\n")
        if index == -1:  # Not enough data
            return None
        length = int(data[:index])  # TODO: protocol error
        res = Array(a=[])
        data = data[index + 2 :]
        parsed += index + 2
        for _ in range(length):
            result = read_next_value(data)
            if result is None:  # Not enough data
                return None
            item, data, tmp_parsed = result
            parsed += tmp_parsed
            res.a.append(item)
        return res, data, parsed

    raise NotImplementedError


def read_next_value_rdb(data: bytes) -> tuple[ProtocolItem, bytes, int] | None:
    assert data

    # TODO: duplicate code
    byte, data = data[:1], data[1:]
    parsed = 1
    if byte == b"$":  # Bulk String
        index = data.find(b"\r\n")
        if index == -1:  # Not enough data
            return None
        length = int(data[:index])  # TODO: protocol error
        data = data[index + 2 :]
        parsed += index + 2
        s, data = data[:length], data[length:]
        if len(s) != length:  # Not enough data
            return None
        return BulkString(b=s), data, parsed

    # TODO: determine why sometimes this fails with actual redis
    # Something missing in packets concat/divide
    print("TODO" * 5, byte + data)
    raise NotImplementedError


@dataclasses.dataclass(kw_only=True, slots=True)
class Client:
    socket: socket.socket
    data: bytes = b""
    replica: bool = False
    expected_offset: int = 0
    last_offset: int = 0

    def send(self, data: bytes):
        self.socket.sendall(data)
        if self.replica:
            self.expected_offset += len(data)


def ping_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    item: ProtocolItem,
    replication_connection: bool,
):
    assert isinstance(item, Array)
    if replication_connection:
        return
    client.send(b"+PONG\r\n")  # TODO: implement serialization


def echo_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    item: ProtocolItem,
    replication_connection: bool,
):
    assert isinstance(item, Array)
    word = item.a[1]
    assert isinstance(word, BulkString)
    if replication_connection:
        return
    # TODO: implement serialization
    resp = b"$" + str(len(word.b)).encode() + b"\r\n" + word.b + b"\r\n"
    client.send(resp)


def get_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    item: ProtocolItem,
    replication_connection: bool,
):
    assert isinstance(item, Array)
    key = item.a[1]
    assert isinstance(key, BulkString)
    result = store.get(key.b)
    if replication_connection:
        return
    response = result.serialize()
    client.send(response)


def set_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    item: ProtocolItem,
    replication_connection: bool,
):
    assert isinstance(item, Array)
    key = item.a[1]
    assert isinstance(key, BulkString)
    value = item.a[2]
    assert isinstance(value, BulkString)
    expire = None

    if len(item.a) > 3:
        assert isinstance(item.a[3], BulkString)
        assert item.a[3].b.upper() == b"PX"

        assert isinstance(item.a[4], BulkString)
        ms = int(item.a[4].b)
        expire = datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(
            milliseconds=ms
        )

    result = store.set(key.b, value.b, expire)
    if replication_connection:
        return
    response = result.serialize()
    client.send(response)

    propagate = item.serialize()
    if params.master_replicas:
        print("Propagation to replicas:", propagate)
    for replica in params.master_replicas:
        replica.send(propagate)


def info_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    item: ProtocolItem,
    replication_connection: bool,
):
    assert isinstance(item, Array)
    key = item.a[1]
    assert isinstance(key, BulkString)
    assert key.b.upper() == b"REPLICATION"
    if replication_connection:
        return
    role = "master" if params.master else "slave"
    result = BulkString(
        b=f"""\
# Replication
role:{role}
master_replid:{params.master_replid}
master_repl_offset:{params.master_repl_offset}
""".encode()
    )
    response = result.serialize()
    client.send(response)


def replconf_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    item: ProtocolItem,
    replication_connection: bool,
):
    assert isinstance(item, Array)
    name = item.a[1]
    value = item.a[2]
    assert isinstance(name, BulkString)
    assert isinstance(value, BulkString)
    method = name.b.upper()

    if method == b"GETACK":
        result = Array(
            a=[
                BulkString(b=b"REPLCONF"),
                BulkString(b=b"ACK"),
                BulkString(b=str(params.replica_offset).encode()),
            ]
        )
        response = result.serialize()
        client.send(response)
        return
    if method == b"ACK":
        remote_offset = int(value.b)
        client.last_offset = remote_offset
        print("REMOTE OFFSET", remote_offset)
        return

    elif method in [b"LISTENING-PORT", b"CAPA"]:
        result = SimpleString(s="OK")
        response = result.serialize()
        client.send(response)
        return

    print("replconf_command", method)
    raise NotImplementedError


def psync_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    item: ProtocolItem,
    replication_connection: bool,
):
    assert isinstance(item, Array)
    repl_id = item.a[1]
    assert isinstance(repl_id, BulkString)
    repl_offset = item.a[2]
    assert isinstance(repl_offset, BulkString)
    print("REPLICA: PSYNC", repl_id.b, repl_offset.b)
    assert repl_id.b.decode() == "?"
    assert int(repl_offset.b) == -1

    if replication_connection:
        return

    print(params.master_repl_offset)
    result = SimpleString(
        s=f"FULLRESYNC {params.master_replid} {params.master_repl_offset}"
    )
    response = result.serialize()
    client.send(response)

    # TODO: construct properly
    empty_rdb = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
    result = BulkString(b=base64.b64decode(empty_rdb))
    response = result.serialize(trailing=False)
    client.send(response)
    client.replica = True
    client.expected_offset = 0
    params.master_replicas.append(client)  # TODO: remove on disconnect


def select_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    item: ProtocolItem,
    replication_connection: bool,
):
    # TODO: implement
    pass


def wait_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    item: ProtocolItem,
    replication_connection: bool,
):
    assert isinstance(item, Array)
    replicas_raw = item.a[1]
    wait_ms_raw = item.a[2]
    assert isinstance(replicas_raw, BulkString)
    assert isinstance(wait_ms_raw, BulkString)
    replicas = int(replicas_raw.b)
    wait_ms = int(wait_ms_raw.b)

    # print("MASTER WAIT", replicas, wait_ms)
    ready = sum(
        1
        for replica in params.master_replicas
        if replica.expected_offset == replica.last_offset
    )
    if ready >= min(replicas, len(params.master_replicas)):
        result = Integer(n=ready)
        response = result.serialize()
        client.send(response)
        return

    now = datetime.datetime.now(tz=datetime.UTC)
    end = now + datetime.timedelta(milliseconds=wait_ms) if wait_ms != 0 else None

    for replica in params.master_replicas:
        req = Array(
            a=[
                BulkString(b=b"REPLCONF"),
                BulkString(b=b"GETACK"),
                BulkString(b=b"*"),
            ]
        )
        replica.send(req.serialize())

    # print("SCHED")
    loop.append(
        functools.partial(wait_command_cont, client=client, end=end, replicas=replicas)
    )


def wait_command_cont(
    loop: list,
    store: Storage,
    params: Params,
    *,
    client: Client,
    end: datetime.datetime | None,
    replicas: int,
):
    now = datetime.datetime.now(tz=datetime.UTC)
    ready = sum(
        1
        for replica in params.master_replicas
        if replica.expected_offset == replica.last_offset
    )
    # for replica in params.master_replicas:
    #     print("EXP, LAST, ", replica.expected_offset, replica.last_offset)
    # print("READY", ready)

    if ready >= replicas or (end is not None and now >= end):
        result = Integer(n=ready)
        response = result.serialize()
        client.send(response)
        return

    # print("SCHED")
    loop.append(
        functools.partial(wait_command_cont, client=client, end=end, replicas=replicas)
    )


class CommandProtocol(Protocol):
    def __call__(
        self,
        loop: list,
        store: Storage,
        params: Params,
        client: Client,
        item: ProtocolItem,
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
}


def serve_client(loop: list, store: Storage, params: Params, client: Client):
    while True:
        if not client.data:
            break

        result = read_next_value(client.data)
        if result is None:
            break

        item, client.data, parsed = result
        assert isinstance(item, Array)
        command = item.a[0]
        assert isinstance(command, BulkString)
        com = command.b.upper()

        f = f_mapping.get(com)
        if f is None:
            raise NotImplementedError

        f(loop, store, params, client, item, replication_connection=False)
        # params.master_repl_offset += parsed


def serve_master(loop: list, store: Storage, params: Params, client: Client):
    while True:
        if not client.data:
            break

        if params.replica_flow == 4:
            result = read_next_value_rdb(client.data)
        else:
            result = read_next_value(client.data)
        if result is None:
            break

        item, client.data, parsed = result
        if params.replica_flow < 4:
            assert isinstance(item, SimpleString)
        elif params.replica_flow == 4:
            # TODO: apply rdb from master
            assert isinstance(item, BulkString)
            params.replica_offset = 0
        else:  # REPLICATION is ESTABLISHED PARSE AS CLIENT BUT DO NOT RESPOND
            # TODO: code duplication
            assert isinstance(item, Array)
            command = item.a[0]
            assert isinstance(command, BulkString)
            com = command.b.upper()

            f = f_mapping.get(com)
            if f is None:
                raise NotImplementedError

            print(
                "REPLICATION:",
                params.replica_offset,
                parsed,
                params.replica_offset + parsed,
                item,
            )
            f(loop, store, params, client, item, replication_connection=True)
            params.replica_offset += parsed
            # client.send(Array(a=[BulkString(b=b"PING")]).serialize())
            continue

        params.replica_flow += 1
        print("FROM MASTER:", params.replica_flow, item)
        if params.replica_flow == 1:
            data = Array(
                a=[
                    BulkString(b=b"REPLCONF"),
                    BulkString(b=b"listening-port"),
                    BulkString(b=str(params.port).encode()),
                ]
            )
            payload = data.serialize()
            client.send(payload)
        if params.replica_flow == 2:
            data = Array(
                a=[
                    BulkString(b=b"REPLCONF"),
                    BulkString(b=b"capa"),
                    BulkString(b=b"psync2"),
                ]
            )
            payload = data.serialize()
            client.send(payload)
        if params.replica_flow == 3:
            params.replica_offset = -1
            data = Array(
                a=[
                    BulkString(b=b"PSYNC"),
                    BulkString(b=b"?"),
                    BulkString(b=str(params.replica_offset).encode()),
                ]
            )
            payload = data.serialize()
            client.send(payload)


@dataclasses.dataclass(kw_only=True, slots=True)
class Params:
    port: int = 0

    master: bool = True
    master_replid: str = ""
    master_repl_offset: int = 0
    master_replicas: list[Client] = dataclasses.field(default_factory=list)

    master_host: str = ""
    master_port: int = 0
    replica_flow: int = 0
    replica_offset: int = -1
    replica_active: bool = True


def main():
    parser = argparse.ArgumentParser(
        prog="CodeCrafters Redis Python",
        description="Custom Redis Implementation",
        epilog="2024 @ Serhii Chaykov",
    )
    parser.add_argument("--port", default=6379, type=int)
    parser.add_argument("--replicaof", nargs="+", default=[])
    args = parser.parse_args()
    params = Params(master_replid=random.randbytes(20).hex(), port=args.port)
    loop: list[Callable] = []

    if args.replicaof:
        params.master = False
        params.master_host = args.replicaof[0]
        params.master_port = int(args.replicaof[1])

    print(f"Params : {params}")
    print("Starting Redis on port {}".format(params.port))

    replica = None
    if not params.master:
        replica_socket = socket.create_connection(
            (params.master_host, params.master_port)
        )
        replica_socket.setblocking(False)
        ping = Array(a=[BulkString(b=b"PING")])
        payload = ping.serialize()
        replica = Client(socket=replica_socket)
        replica.send(payload)

    server_socket = socket.create_server(("localhost", params.port), reuse_port=True)
    server_socket.setblocking(False)
    client_sockets: dict[socket.socket, Client] = {}
    store = Storage()

    while True:
        read_sockets = list(client_sockets)
        read_sockets.append(server_socket)
        if not params.master and params.replica_active:
            read_sockets.append(replica.socket)

        timeout = 0.1 if loop else None
        read, _, _ = select.select(read_sockets, [], [], timeout)
        for sock in read:
            if sock == server_socket:
                client, address = sock.accept()
                client_sockets[client] = Client(socket=client)
                continue

            if replica and sock == replica.socket:
                recv = sock.recv(4096)
                if not recv:
                    logging.warning(f"Master connection closed")
                    params.replica_active = False
                    continue
                replica.data += recv
                serve_master(loop, store, params, replica)
                continue

            recv = sock.recv(4096)
            if not recv:
                sock.close()
                del client_sockets[sock]
                continue

            client = client_sockets[sock]
            client.data += recv
            serve_client(loop, store, params, client)

        next_loop: list[Callable] = []
        while loop:
            f = loop.pop(0)
            f(next_loop, store, params)
        loop = next_loop


if __name__ == "__main__":
    main()
