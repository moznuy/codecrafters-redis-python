# TODO: use everywhere
from __future__ import annotations

from typing import Any

from app import models
from app import store


def ser_xid(n: models.XID):
    assert isinstance(n, models.XID)
    return ser_bulk_string(f"{n.time}-{n.seq}")


def ser_number_raw(n: int):
    assert isinstance(n, int)
    return str(n).encode()


def ser_number(n: int):
    assert isinstance(n, int)
    return ser_bulk_string(str(n))


def ser_integer(n: int):
    assert isinstance(n, int)
    return b":" + str(n).encode() + b"\r\n"


def ser_simple_string(s: str):
    assert isinstance(s, str)
    assert "\r" not in s and "\n" not in s

    return b"+" + s.encode() + b"\r\n"


def ser_simple_error(m: str, e: str | None = None):
    assert isinstance(m, str)
    assert "\r" not in m and "\n" not in m
    if e is not None:
        assert isinstance(e, str)
        assert "\r" not in e and "\n" not in e

        err = e + " " + m
    else:
        err = m

    return b"-" + err.encode() + b"\r\n"


def ser_bulk_string(data: str | bytes, trailing=True):
    assert isinstance(data, (bytes, str))
    if isinstance(data, str):
        data = data.encode()

    result = b"$" + ser_number_raw(len(data)) + b"\r\n" + data
    if trailing:
        result += b"\r\n"
    return result


def ser_list(data: list | tuple):
    assert isinstance(data, (tuple, list))

    result = b"*" + ser_number_raw(len(data)) + b"\r\n"
    for item in data:
        result += serialize(item)
    return result


def ser_command(cmd: models.Command):
    assert isinstance(cmd, models.Command)

    array = [cmd.command] + cmd.args
    return ser_list(array)


NullBulkString = object()


def serialize(item: Any) -> bytes:
    if item is NullBulkString:
        return b"$-1\r\n"
    if isinstance(item, models.XID):
        return ser_xid(item)
    if isinstance(item, (bytes, str)):
        return ser_bulk_string(item)
    if isinstance(item, store.Err):
        return ser_simple_error(item.message, item.error)
    if isinstance(item, int):
        return ser_number(item)
    if isinstance(item, models.Command):
        return ser_command(item)
    if isinstance(item, (tuple, list)):
        return ser_list(item)

    print(type(item), item)
    raise NotImplementedError


def parse_command(item: list) -> models.Command:
    assert isinstance(item, list)

    command = b""
    args: list[bytes] = []
    i: bytes
    for i in item:
        assert isinstance(i, bytes)

        if not command:
            command = i
            continue
        args.append(i)

    return models.Command(command=command, args=args)


def read_next_value(data: bytes) -> tuple[Any, bytes, int] | None:
    assert data

    byte, data = data[:1], data[1:]
    parsed = 1
    if byte == b"+":  # Simple String
        index = data.find(b"\r\n")
        if index == -1:  # Not enough data
            return None
        raw_string, data = data[:index], data[index + 2 :]
        parsed += index + 2
        return raw_string.decode(), data, parsed

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
        return s, data, parsed

    if byte == b"*":  # Array
        index = data.find(b"\r\n")
        if index == -1:  # Not enough data
            return None
        length = int(data[:index])  # TODO: protocol error
        res = []
        data = data[index + 2 :]
        parsed += index + 2
        for _ in range(length):
            result = read_next_value(data)
            if result is None:  # Not enough data
                return None
            item, data, tmp_parsed = result
            parsed += tmp_parsed
            res.append(item)
        return res, data, parsed

    raise NotImplementedError


def read_next_value_rdb(data: bytes) -> tuple[Any, bytes, int] | None:
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
        return s, data, parsed

    # TODO: determine why sometimes this fails with actual redis
    # Something missing in packets concat/divide
    print("TODO" * 5, byte + data)
    raise NotImplementedError
