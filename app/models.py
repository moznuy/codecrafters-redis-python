from __future__ import annotations

import collections
import dataclasses
import datetime
import socket

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app import loop
    from app import store


@dataclasses.dataclass(slots=True, kw_only=True)
class App:
    loop: loop.EventLoop
    params: Params
    store: store.Storage


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


XID = collections.namedtuple("XID", ["time", "seq"])


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
