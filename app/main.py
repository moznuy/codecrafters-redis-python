from __future__ import annotations

import dataclasses
import select
import socket


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class ProtocolItem:
    pass


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class SimpleString(ProtocolItem):
    s: str


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class BulkString(ProtocolItem):
    b: bytes


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class Array(ProtocolItem):
    a: list[ProtocolItem]


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

    raise NotImplemented


@dataclasses.dataclass(kw_only=True, slots=True)
class Client:
    socket: socket.socket
    data: bytes


def ping_command(client: Client, item: ProtocolItem):
    assert isinstance(item, Array)
    client.socket.sendall(b"+PONG\r\n")  # TODO: implement serialization


def echo_command(client: Client, item: ProtocolItem):
    assert isinstance(item, Array)
    word = item.a[1]
    assert isinstance(word, BulkString)
    # TODO: implement serialization
    resp = b"$" + str(len(word.b)).encode() + b"\r\n" + word.b + b"\r\n"
    client.socket.sendall(resp)


def serve_client(client: Client):
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

        if command.b.upper() == b'PING':
            ping_command(client, item)
            continue

        if command.b.upper() == b'ECHO':
            echo_command(client, item)
            continue

        raise NotImplemented


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    client_sockets: dict[socket.socket, Client] = {}

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
            serve_client(client)


if __name__ == "__main__":
    main()
