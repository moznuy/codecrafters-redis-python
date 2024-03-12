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
class Command:
    command: bytes
    args: list[bytes]


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class StorageMeta:
    expire: datetime.datetime | None = None


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class StorageValue:
    @staticmethod
    def type():
        raise NotImplementedError


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class Simple(StorageValue):
    b: bytes

    @staticmethod
    def type():
        return "string"


XID = collections.namedtuple("XID", ["time", "seq"])


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class Stream(StorageValue):
    s: list[tuple[XID, list[tuple[bytes, bytes]]]]

    @staticmethod
    def type():
        return "stream"


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class StorageItem:
    meta: StorageMeta
    value: StorageValue
