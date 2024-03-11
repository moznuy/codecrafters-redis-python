from __future__ import annotations

import dataclasses
import select
import socket


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class ProtocolItem:
    def serialize(self) -> bytes:
        raise NotImplementedError


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class SimpleString(ProtocolItem):
    s: str

    def serialize(self) -> bytes:
        return b'+' + self.s.encode() + b'\r\n'


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class BulkString(ProtocolItem):
    b: bytes

    def serialize(self) -> bytes:
        return b"$" + str(len(self.b)).encode() + b"\r\n" + self.b + b"\r\n"


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class NullBulkString(ProtocolItem):
    def serialize(self) -> bytes:
        return b"$-1\r\n"


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class Array(ProtocolItem):
    a: list[ProtocolItem]


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class StorageMeta:
    pass


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
        assert isinstance(item.value, Simple)
        return BulkString(b=item.value.b)

    def set(self, key: bytes, value: bytes):
        self.storage[key] = StorageItem(meta=StorageMeta(), value=Simple(b=value))
        return SimpleString(s='OK')


def read_next_value(data: bytes) -> tuple[ProtocolItem, bytes] | None:
    assert data

    byte, data = data[:1], data[1:]
    if byte == b'+':  # Simple String
        index = data.find(b'\r\n')
        if index == -1:  # Not enough data
            return None
        raw_string, data = data[:index], data[index+2:]
        return SimpleString(s=raw_string.decode()), data

    if byte == b'$':  # Bulk String
        index = data.find(b'\r\n')
        if index == -1:  # Not enough data
            return None
        length = int(data[:index])  # TODO: protocol error
        index2 = data.find(b'\r\n', index + 2)
        if index2 == -1:  # Not enough data
            return None
        s = data[index+2: index2]
        data = data[index2+2:]
        assert length == len(s)  # TODO: protocol error
        return BulkString(b=s), data

    if byte == b'*':  # Array
        index = data.find(b'\r\n')
        if index == -1:  # Not enough data
            return None
        length = int(data[:index])  # TODO: protocol error
        res = Array(a=[])
        data = data[index+2:]
        for _ in range(length):
            result = read_next_value(data)
            if result is None:  # Not enough data
                return None
            item, data = result
            res.a.append(item)
        return res, data

    raise NotImplementedError


@dataclasses.dataclass(kw_only=True, slots=True)
class Client:
    socket: socket.socket
    data: bytes


def ping_command(store: Storage, client: Client, item: ProtocolItem):
    assert isinstance(item, Array)
    client.socket.sendall(b"+PONG\r\n")  # TODO: implement serialization


def echo_command(store: Storage, client: Client, item: ProtocolItem):
    assert isinstance(item, Array)
    word = item.a[1]
    assert isinstance(word, BulkString)
    # TODO: implement serialization
    resp = b"$" + str(len(word.b)).encode() + b"\r\n" + word.b + b"\r\n"
    client.socket.sendall(resp)


def get_command(store: Storage, client: Client, item: ProtocolItem):
    assert isinstance(item, Array)
    key = item.a[1]
    assert isinstance(key, BulkString)
    result = store.get(key.b)
    response = result.serialize()
    client.socket.sendall(response)


def set_command(store: Storage, client: Client, item: ProtocolItem):
    assert isinstance(item, Array)
    key = item.a[1]
    assert isinstance(key, BulkString)
    value = item.a[2]
    assert isinstance(value, BulkString)
    result = store.set(key.b, value.b)
    response = result.serialize()
    client.socket.sendall(response)


f_mapping = {
    b'PING': ping_command,
    b'ECHO': echo_command,
    b'GET': get_command,
    b'SET': set_command,
}


def serve_client(store: Storage, client: Client):
    while True:
        if not client.data:
            break

        result = read_next_value(client.data)
        if result is None:
            break

        item, client.data = result
        assert isinstance(item, Array)
        command = item.a[0]
        assert isinstance(command, BulkString)
        com = command.b.upper()

        f = f_mapping.get(com)
        if f is None:
            raise NotImplementedError

        f(store, client, item)


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    client_sockets: dict[socket.socket, Client] = {}
    store = Storage()

    while True:
        read_sockets = [server_socket] + list(client_sockets)
        read, _, _ = select.select(read_sockets, [], [], None)
        for sock in read:
            if sock == server_socket:
                client, address = server_socket.accept()
                client_sockets[client] = Client(socket=client, data=b'')
                continue

            recv = sock.recv(4096)
            if not recv:
                sock.close()
                del client_sockets[sock]
                continue

            client = client_sockets[sock]
            client.data += recv
            serve_client(store, client)


if __name__ == "__main__":
    main()
