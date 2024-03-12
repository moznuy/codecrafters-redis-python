# TODO: use everywhere
from __future__ import annotations

from app import models


def encode_int(n: int) -> models.BulkString:
    assert isinstance(n, int)
    return models.BulkString(b=str(n).encode())


# TODO: use everywhere
def encode_xid(n: models.XID) -> models.BulkString:
    assert isinstance(n, models.XID)
    return models.BulkString(b=f"{n.time}-{n.seq}".encode())


# TODO: use everywhere
def encode_array(a: list) -> models.Array:
    assert isinstance(a, list)
    res = []
    for x in a:
        if isinstance(x, models.BulkString):
            res.append(x)
            continue
        if isinstance(x, bytes):
            res.append(models.BulkString(b=x))
            continue
        if isinstance(x, tuple):
            for i in x:
                assert isinstance(i, bytes)
                res.append(models.BulkString(b=i))
            continue
        if isinstance(x, list):
            res.append(encode_array(x))
            continue
        print(x)
        raise NotImplementedError
    return models.Array(a=res)


def read_next_value(data: bytes) -> tuple[models.ProtocolItem, bytes, int] | None:
    assert data

    byte, data = data[:1], data[1:]
    parsed = 1
    if byte == b"+":  # Simple String
        index = data.find(b"\r\n")
        if index == -1:  # Not enough data
            return None
        raw_string, data = data[:index], data[index + 2 :]
        parsed += index + 2
        return models.SimpleString(s=raw_string.decode()), data, parsed

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
        return models.BulkString(b=s), data, parsed

    if byte == b"*":  # Array
        index = data.find(b"\r\n")
        if index == -1:  # Not enough data
            return None
        length = int(data[:index])  # TODO: protocol error
        res = models.Array(a=[])
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


def read_next_value_rdb(data: bytes) -> tuple[models.ProtocolItem, bytes, int] | None:
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
        return models.BulkString(b=s), data, parsed

    # TODO: determine why sometimes this fails with actual redis
    # Something missing in packets concat/divide
    print("TODO" * 5, byte + data)
    raise NotImplementedError
