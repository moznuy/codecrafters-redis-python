from __future__ import annotations

import argparse
import base64
import collections
import contextlib
import dataclasses
import datetime
import functools
import itertools
import logging
import os
import random
import select
import socket
from typing import Callable
from typing import Protocol


EMPTY_RDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="


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
class SimpleError(ProtocolItem):
    e: str = ""
    m: str = ""

    def serialize(self) -> bytes:
        err = (self.e + " " + self.m) if self.e else self.m
        return b"-" + err.encode() + b"\r\n"


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
    @staticmethod
    def type():
        return SimpleString(s="none")


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class Simple(StorageValue):
    b: bytes

    @staticmethod
    def type():
        return SimpleString(s="string")


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class Stream(StorageValue):
    s: list[tuple[XID, list[tuple[bytes, bytes]]]]

    @staticmethod
    def type():
        return SimpleString(s="stream")


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class StorageItem:
    meta: StorageMeta
    value: StorageValue


XID = collections.namedtuple("XID", ["time", "seq"])


class Storage:
    def __init__(self):
        self.storage: dict[bytes, StorageItem] = {}

    def get(self, key: bytes) -> ProtocolItem:
        if key not in self.storage:
            return NullBulkString()

        if not self._check_exp(key):
            return NullBulkString()

        item = self.storage[key]
        assert isinstance(item.value, Simple)
        return BulkString(b=item.value.b)

    def _check_exp(self, key: bytes) -> bool:
        item = self.storage[key]
        if item.meta.expire:
            now = datetime.datetime.now(tz=datetime.UTC)
            if now > item.meta.expire:  # TODO: passive deletion? or not?
                del self.storage[key]
                return False
        return True

    def set(self, key: bytes, value: bytes, expire: datetime.datetime | None = None):
        now = datetime.datetime.now(tz=datetime.UTC)
        if expire and expire < now:
            print("KEY EXPIRED (probably read from rdb)", key)
            return SimpleString(s="OK")
        self.storage[key] = StorageItem(
            meta=StorageMeta(expire=expire), value=Simple(b=value)
        )
        return SimpleString(s="OK")

    def keys(self):
        keys = list(self.storage.keys())
        return Array(a=[BulkString(b=key) for key in keys])

    def type(self, key: bytes):
        if key not in self.storage:
            return StorageValue.type()

        if not self._check_exp(key):
            return StorageValue.type()

        item = self.storage[key]
        return item.value.type()

    @staticmethod
    def x_validate_id(last_xid: XID, new_xid: XID) -> SimpleError | None:
        if new_xid <= XID(0, 0):
            return SimpleError(
                e="ERR",
                m="The ID specified in XADD must be greater than 0-0",
            )

        if last_xid is not None and new_xid <= last_xid:
            return SimpleError(
                e="ERR",
                m="The ID specified in XADD is equal or smaller than the target stream top item",
            )
        return None

    @staticmethod
    def gen_seq(last_xid: XID | None, time: int):
        if last_xid is None:
            return 0 if time > 0 else 1
        elif last_xid.time == time:
            return last_xid.seq + 1
        return 0

    def xadd(self, key: bytes, id_raw: bytes, tuples: list):
        value = self.storage.setdefault(
            key, StorageItem(meta=StorageMeta(), value=Stream(s=[]))
        ).value
        assert isinstance(value, Stream)  # TODO: wrong type

        try:
            last_xid = value.s[-1][0]
        except IndexError:
            last_xid = None

        if id_raw == b"*":
            time = int(datetime.datetime.now(tz=datetime.UTC).timestamp() * 1000)
            seq = self.gen_seq(last_xid, time)
            new_xid = XID(time, seq)
        else:
            time_raw, seq_raw = id_raw.split(b"-")
            if seq_raw == b"*":
                time = int(time_raw)
                seq = self.gen_seq(last_xid, time)
                new_xid = XID(time, seq)
            else:
                time, seq = int(time_raw), int(seq_raw)
                new_xid = XID(time, seq)

        err = self.x_validate_id(last_xid, new_xid)
        if err is not None:
            return err

        value.s.append((new_xid, tuples))
        return BulkString(b=f"{new_xid.time}-{new_xid.seq}".encode())

    @staticmethod
    def _get_seq(raw: bytes, default: int):
        warmer = raw.split(b"-")
        if len(warmer) == 1:
            xid = XID(int(warmer[0]), default)
        else:
            xid = XID(int(warmer[0]), int(warmer[1]))
        return xid

    def xrange(self, key: bytes, start_raw: bytes, end_raw: bytes):
        start = self._get_seq(start_raw, -1) if start_raw != b"-" else XID(0, 0)
        end = (
            self._get_seq(end_raw, 2**64 - 1)
            if end_raw != b"+"
            else XID(2**64 - 1, 2**64 - 1)
        )

        value = self.storage.setdefault(
            key, StorageItem(meta=StorageMeta(), value=Stream(s=[]))
        ).value
        assert isinstance(value, Stream)  # TODO: wrong type
        result = []
        for xid, items in value.s:
            if start <= xid <= end:  # TODO: binary search
                result.append(Array(a=[encode_xid(xid), encode_array(items)]))
        return Array(a=result)

    def xread(self, keys: list[bytes], raw_ids: list[bytes]):
        key: bytes
        raw_ids: bytes

        result = []
        for key, raw_id in zip(keys, raw_ids, strict=True):
            start = self._get_seq(raw_id, 0)

            val = self.storage.get(key)
            if val is None:
                continue
            stream = val.value
            assert isinstance(stream, Stream)

            result_inner = []
            for xid, items in stream.s:
                if start < xid:  # TODO: binary search
                    result_inner.append([encode_xid(xid), items])
            if result_inner:
                result.append([key, result_inner])
        if not result:
            return None
        return result


# TODO: use everywhere
def encode_int(n: int) -> BulkString:
    assert isinstance(n, int)
    return BulkString(b=str(n).encode())


# TODO: use everywhere
def encode_xid(n: XID) -> BulkString:
    assert isinstance(n, XID)
    return BulkString(b=f"{n.time}-{n.seq}".encode())


# TODO: use everywhere
def encode_array(a: list) -> Array:
    assert isinstance(a, list)
    res = []
    for x in a:
        if isinstance(x, BulkString):
            res.append(x)
            continue
        if isinstance(x, bytes):
            res.append(BulkString(b=x))
            continue
        if isinstance(x, tuple):
            for i in x:
                assert isinstance(i, bytes)
                res.append(BulkString(b=i))
            continue
        if isinstance(x, list):
            res.append(encode_array(x))
            continue
        print(x)
        raise NotImplementedError
    return Array(a=res)


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
    command: Command,
    replication_connection: bool,
):
    if replication_connection:
        return
    client.send(b"+PONG\r\n")  # TODO: implement serialization


def echo_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
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
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    command: Command,
    replication_connection: bool,
):
    key = command.args[0]
    result = store.get(key)
    if replication_connection:
        return
    response = result.serialize()
    client.send(response)


def set_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
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

    result = store.set(key, value, expire)
    if replication_connection:
        return
    response = result.serialize()
    client.send(response)

    propagate = command.resp3().serialize()
    if params.master_replicas:
        print("Propagation to replicas:", propagate)
    for replica in params.master_replicas:
        replica.send(propagate)


def info_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    command: Command,
    replication_connection: bool,
):
    key = command.args[0]
    assert key.upper() == b"REPLICATION"
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
    command: Command,
    replication_connection: bool,
):
    value = command.args[1]
    method = command.args[0].upper()

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
        remote_offset = int(value)
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

    print(params.master_repl_offset)
    result = SimpleString(
        s=f"FULLRESYNC {params.master_replid} {params.master_repl_offset}"
    )
    response = result.serialize()
    client.send(response)

    # TODO: construct properly
    result = BulkString(b=base64.b64decode(EMPTY_RDB))
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
    command: Command,
    replication_connection: bool,
):
    # TODO: implement
    pass


def wait_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    command: Command,
    replication_connection: bool,
):
    replicas = int(command.args[0])
    wait_ms = int(command.args[1])

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


def config_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    command: Command,
    replication_connection: bool,
):
    method = command.args[0]
    item = command.args[1]
    assert method.upper() == b"GET"
    if item == b"dir":
        response = Array(
            a=[
                BulkString(b=b"dir"),
                BulkString(b=params.dir.encode()),
            ]
        )
        client.send(response.serialize())
        return
    if item == b"dbfilename":
        response = Array(
            a=[
                BulkString(b=b"dbfilename"),
                BulkString(b=params.dbfilename.encode()),
            ]
        )
        client.send(response.serialize())
        return

    print(item)
    raise NotImplementedError


def keys_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    command: Command,
    replication_connection: bool,
):
    mask = command.args[0]
    assert mask == b"*"
    response = store.keys()
    client.send(response.serialize())


def type_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    command: Command,
    replication_connection: bool,
):
    key = command.args[0]
    response = store.type(key)
    client.send(response.serialize())


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class Command:
    command: bytes
    args: list[bytes]

    def resp3(self):
        arr = [BulkString(b=self.command)] + [BulkString(b=arg) for arg in self.args]
        return Array(a=arr)


def parse_command(item: ProtocolItem) -> Command:
    assert isinstance(item, Array)

    command = b""
    args: list[bytes] = []
    for i in item.a:
        assert isinstance(i, BulkString)
        if not command:
            command = i.b
            continue
        args.append(i.b)

    return Command(command=command, args=args)


def batched(lst, n):
    it = iter(lst)
    return iter(lambda: tuple(itertools.islice(it, n)), ())


def xadd_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    command: Command,
    replication_connection: bool,
):
    key = command.args[0]
    id_raw = command.args[1]

    # TODO 3.12 batched
    tuples = list(batched(command.args[2:], n=2))
    response = store.xadd(key, id_raw, tuples)
    client.send(response.serialize())


def xrange_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
    command: Command,
    replication_connection: bool,
):
    key = command.args[0]
    start = command.args[1]
    end = command.args[2]

    response = store.xrange(key, start, end)
    client.send(response.serialize())


def xread_command(
    loop: list,
    store: Storage,
    params: Params,
    client: Client,
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
    print("XREAD", block_ms, keys, raw_ids)

    response = store.xread(keys, raw_ids)
    if response is not None:
        client.send(encode_array(response).serialize())
        return
    if block_ms is None:
        client.send(NullBulkString().serialize())
        return
    now = datetime.datetime.now(tz=datetime.UTC)
    end = now + datetime.timedelta(milliseconds=block_ms) if block_ms else None
    loop.append(
        functools.partial(
            xread_command_cont, keys=keys, raw_ids=raw_ids, client=client, end=end
        )
    )
    # print("SCHED")


def xread_command_cont(
    loop: list,
    store: Storage,
    params: Params,
    *,
    keys: list[bytes],
    raw_ids: list[bytes],
    client: Client,
    end: datetime.datetime | None,
):
    response = store.xread(keys, raw_ids)
    if response is not None:
        client.send(encode_array(response).serialize())
        return
    now = datetime.datetime.now(tz=datetime.UTC)
    if end is not None and now >= end:
        client.send(NullBulkString().serialize())
        return
    loop.append(
        functools.partial(
            xread_command_cont, keys=keys, raw_ids=raw_ids, client=client, end=end
        )
    )
    # print("SCHED")


class CommandProtocol(Protocol):
    def __call__(
        self,
        loop: list,
        store: Storage,
        params: Params,
        client: Client,
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


def serve_client(loop: list, store: Storage, params: Params, client: Client):
    while True:
        if not client.data:
            break

        result = read_next_value(client.data)
        if result is None:
            break

        item, client.data, parsed = result
        command = parse_command(item)
        f = f_mapping.get(command.command.upper())
        if f is None:
            raise NotImplementedError

        f(loop, store, params, client, command, replication_connection=False)
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
            command = parse_command(item)
            f = f_mapping.get(command.command.upper())
            if f is None:
                raise NotImplementedError

            print(
                "REPLICATION:",
                params.replica_offset,
                parsed,
                params.replica_offset + parsed,
                item,
            )
            f(loop, store, params, client, command, replication_connection=True)
            params.replica_offset += parsed
            # client.send(Array(a=[BulkString(b=b"PING")]).serialize())
            continue

        params.replica_flow += 1
        # print("FROM MASTER:", params.replica_flow, item)
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
    dir: str = ""
    dbfilename: str = ""

    master: bool = True
    master_replid: str = ""
    master_repl_offset: int = 0
    master_replicas: list[Client] = dataclasses.field(default_factory=list)

    master_host: str = ""
    master_port: int = 0
    replica_flow: int = 0
    replica_offset: int = -1
    replica_active: bool = True


def _rdb_read_length(data: bytes) -> tuple[int, bytes]:
    byte, data = data[0], data[1:]

    ms2 = (byte & 0b11_00_0000) >> 6
    if ms2 == 0:
        return byte & 0b00_11_1111, data
    if ms2 == 1:
        length = ((byte & 0b00_11_1111) << 6) | data[0]
        data = data[1:]
        return length, data
    if ms2 == 2:
        length_raw, data = data[:4], data[4:]
        length = int.from_bytes(length_raw, "little")
        return length, data
    if ms2 == 3:
        special_format = byte & 0b00_11_1111
        if special_format == 0:  # 8  bit integer
            return -1, data
        if special_format == 1:  # 16 bit integer
            return -2, data
        if special_format == 2:  # 32 bit integer
            return -4, data
        if special_format == 3:  # LZF compressed string
            return -10, data
    raise NotImplementedError


def _rdb_read_str(data: bytes) -> tuple[bytes, bytes]:
    l, data = _rdb_read_length(data)
    if l > 0:
        s, data = data[:l], data[l:]
        return s, data
    elif abs(l) in [1, 2, 4]:
        l = abs(l)
        s, data = data[:l], data[l:]
        return str(int.from_bytes(s, "little")).encode(), data
    elif l == -10:  # LZF
        len_compressed, data = _rdb_read_length(data)
        len_uncompressed, data = _rdb_read_length(data)
        compressed, data = data[:len_compressed], data[:len_compressed]
        raise NotImplementedError  # TODO: LZF
        return b"", data
    raise NotImplementedError


def read_rdb(params: Params) -> Storage:
    rdb_store = Storage()
    data = None
    with contextlib.suppress(FileNotFoundError):
        with open(params.dbfilename, "rb") as file:
            data = file.read()
    if not data:
        return rdb_store

    magic, data = data[:5], data[5:]
    assert magic == b"REDIS"
    version, data = data[:4], data[4:]
    expiry = None

    while data:
        byte, data = data[0], data[1:]
        if byte == 0xFA:  # AUX
            key, data = _rdb_read_str(data)
            value, data = _rdb_read_str(data)
            # print(key, value)
            continue
        if byte == 0xFF:  # END
            break
        if byte == 0xFE:  # DATABASE Selector
            db, data = _rdb_read_length(data)
            assert db == 0
            continue
        if byte == 0xFB:  # RESIZEDB
            # We do not optimize here :)
            _hash_table_size, data = _rdb_read_length(data)
            _expiry_table_size, data = _rdb_read_length(data)
            continue
        if byte == 0xFC:  # EXPIRETIMEMS
            ms_raw, data = data[:8], data[8:]
            ms = int.from_bytes(ms_raw, "little")
            expiry = datetime.datetime.fromtimestamp(ms / 1000.0, tz=datetime.UTC)
            continue
        if 0 <= byte <= 14:  # VALUE TYPE:
            value_type = byte
            key, data = _rdb_read_str(data)
            if value_type == 0:  # String Encoding
                value, data = _rdb_read_str(data)
                rdb_store.set(key, value, expiry)
                expiry = None
                continue

            print(f"{value_type=}")
            raise NotImplementedError

        print(f"{byte:02x}")
        raise NotImplementedError

    print(f"{len(rdb_store.storage)=}")
    return rdb_store


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
    params = Params(
        master_replid=random.randbytes(20).hex(),
        port=args.port,
        dir=args.dir,
        dbfilename=args.dbfilename,
    )
    loop: list[Callable] = []

    if args.replicaof:
        params.master = False
        params.master_host = args.replicaof[0]
        params.master_port = int(args.replicaof[1])

    print(f"Params : {params}")
    print("Starting Redis on port {}".format(params.port))
    os.chdir(params.dir)
    store = read_rdb(params)

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
