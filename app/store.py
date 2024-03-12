from __future__ import annotations

import contextlib
import datetime

from app import models
from app import serialization


class Err(RuntimeError):
    error = "ERR"

    def __init__(self, message: str):
        self.message = message


class Storage:
    def __init__(self):
        self.storage: dict[bytes, models.StorageItem] = {}

    def get(self, key: bytes):
        if key not in self.storage:
            return serialization.NullBulkString

        if not self._check_exp(key):
            return serialization.NullBulkString

        item = self.storage[key]
        assert isinstance(item.value, models.Simple)
        return item.value.b

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
            return "OK"
        self.storage[key] = models.StorageItem(
            meta=models.StorageMeta(expire=expire), value=models.Simple(b=value)
        )
        return "OK"

    def keys(self):
        keys = list(self.storage.keys())
        return keys

    def type(self, key: bytes):
        if key not in self.storage:
            return "none"

        if not self._check_exp(key):
            return "none"

        item = self.storage[key]
        return item.value.type()

    @staticmethod
    def x_validate_id(last_xid: models.XID, new_xid: models.XID):
        if new_xid <= models.XID(0, 0):
            raise Err("The ID specified in XADD must be greater than 0-0")

        if last_xid is not None and new_xid <= last_xid:
            raise Err(
                "The ID specified in XADD is equal or smaller than the target stream top item"
            )
        return None

    @staticmethod
    def gen_seq(last_xid: models.XID | None, time: int):
        if last_xid is None:
            return 0 if time > 0 else 1
        elif last_xid.time == time:
            return last_xid.seq + 1
        return 0

    def xadd(self, key: bytes, id_raw: bytes, tuples: list):
        value = self.storage.setdefault(
            key,
            models.StorageItem(meta=models.StorageMeta(), value=models.Stream(s=[])),
        ).value
        assert isinstance(value, models.Stream)  # TODO: wrong type

        try:
            last_xid = value.s[-1][0]
        except IndexError:
            last_xid = None

        if id_raw == b"*":
            time = int(datetime.datetime.now(tz=datetime.UTC).timestamp() * 1000)
            seq = self.gen_seq(last_xid, time)
            new_xid = models.XID(time, seq)
        else:
            time_raw, seq_raw = id_raw.split(b"-")
            if seq_raw == b"*":
                time = int(time_raw)
                seq = self.gen_seq(last_xid, time)
                new_xid = models.XID(time, seq)
            else:
                time, seq = int(time_raw), int(seq_raw)
                new_xid = models.XID(time, seq)

        self.x_validate_id(last_xid, new_xid)

        value.s.append((new_xid, tuples))
        return f"{new_xid.time}-{new_xid.seq}"

    @staticmethod
    def _get_seq(raw: bytes, default: int):
        warmer = raw.split(b"-")
        if len(warmer) == 1:
            xid = models.XID(int(warmer[0]), default)
        else:
            xid = models.XID(int(warmer[0]), int(warmer[1]))
        return xid

    def xrange(self, key: bytes, start_raw: bytes, end_raw: bytes):
        start = self._get_seq(start_raw, -1) if start_raw != b"-" else models.XID(0, 0)
        end = (
            self._get_seq(end_raw, 2**64 - 1)
            if end_raw != b"+"
            else models.XID(2**64 - 1, 2**64 - 1)
        )

        value = self.storage.setdefault(
            key,
            models.StorageItem(meta=models.StorageMeta(), value=models.Stream(s=[])),
        ).value
        assert isinstance(value, models.Stream)  # TODO: wrong type
        result = []
        for xid, items in value.s:
            if start <= xid <= end:  # TODO: binary search
                result.append([xid, [i for item in items for i in item]])
        return result

    def xread(
        self,
        keys: list[bytes],
        raw_ids: list[bytes],
        last_xids: dict[bytes, models.XID],
    ):
        key: bytes
        raw_ids: bytes

        result = []
        for key, raw_id in zip(keys, raw_ids, strict=True):
            if raw_id == b"$":
                start = None
            else:
                start = self._get_seq(raw_id, 0)

            val = self.storage.get(key)
            if val is None:
                continue
            stream = val.value
            assert isinstance(stream, models.Stream)

            if start is None:
                start = last_xids.get(key)

            if start is None:
                with contextlib.suppress(IndexError):
                    last_xid = stream.s[-1][0]
                    last_xids[key] = start = last_xid

            result_inner = []
            for xid, items in stream.s:
                if start < xid:  # TODO: binary search
                    result_inner.append([xid, [i for item in items for i in item]])
            if result_inner:
                result.append([key, result_inner])
        if not result:
            return None
        return result


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


def read_rdb(params: models.Params) -> Storage:
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
