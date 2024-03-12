from __future__ import annotations

import base64
import datetime
import functools
import itertools
from typing import Protocol

from app import models
from app import serialization
from app import store


def ping_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
    replication_connection: bool,
):
    if replication_connection:
        return
    client.send(serialization.ser_simple_string("PONG"))


def echo_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
    replication_connection: bool,
):
    word = command.args[0]
    if replication_connection:
        return
    client.send(serialization.serialize(word))


def get_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
    replication_connection: bool,
):
    key = command.args[0]
    result = app.store.get(key)
    if replication_connection:
        return
    client.send(serialization.serialize(result))


def set_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
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
    client.send(serialization.serialize(result))

    propagate = serialization.serialize(command)
    if app.params.master_replicas:
        print("Propagation to replicas:", propagate)
    for replica in app.params.master_replicas:
        replica.send(propagate)


def info_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
    replication_connection: bool,
):
    key = command.args[0]
    assert key.upper() == b"REPLICATION"
    if replication_connection:
        return
    role = "master" if app.params.master else "slave"
    result = f"""\
# Replication
role:{role}
master_replid:{app.params.master_replid}
master_repl_offset:{app.params.master_repl_offset}
"""
    client.send(serialization.serialize(result))


def replconf_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
    replication_connection: bool,
):
    value = command.args[1]
    method = command.args[0].upper()

    if method == b"GETACK":
        result = ["REPLCONF", "ACK", app.params.replica_offset]
        client.send(serialization.serialize(result))
        return
    if method == b"ACK":
        remote_offset = int(value)
        client.last_offset = remote_offset
        print("REMOTE OFFSET", remote_offset)
        return
    elif method in [b"LISTENING-PORT", b"CAPA"]:
        client.send(serialization.ser_simple_string("OK"))
        return

    print("replconf_command", method)
    raise NotImplementedError


EMPTY_RDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="


def psync_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
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
    result = f"FULLRESYNC {app.params.master_replid} {app.params.master_repl_offset}"
    client.send(serialization.ser_simple_string(result))

    # TODO: construct properly
    result = base64.b64decode(EMPTY_RDB)
    response = serialization.ser_bulk_string(result, trailing=False)
    client.send(response)
    client.replica = True
    client.expected_offset = 0
    app.params.master_replicas.append(client)  # TODO: remove on disconnect


def select_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
    replication_connection: bool,
):
    # TODO: implement
    pass


def wait_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
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
        client.send(serialization.ser_integer(ready))
        return

    now = datetime.datetime.now(tz=datetime.UTC)
    end = now + datetime.timedelta(milliseconds=wait_ms) if wait_ms != 0 else None

    for replica in app.params.master_replicas:
        req = ["REPLCONF", "GETACK", "*"]
        replica.send(serialization.serialize(req))

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
    # TODO: same code?
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
        client.send(serialization.ser_integer(ready))
        return

    # print("SCHED")
    app.loop.add(
        functools.partial(wait_command_cont, client=client, end=end, replicas=replicas),
        later=datetime.timedelta(milliseconds=100),
    )


def config_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
    replication_connection: bool,
):
    method = command.args[0]
    item = command.args[1]
    assert method.upper() == b"GET"
    if item == b"dir":
        result = ["dir", app.params.dir]
        client.send(serialization.serialize(result))
        return
    if item == b"dbfilename":
        result = ["dbfilename", app.params.dbfilename]
        client.send(serialization.serialize(result))
        return

    print(item)
    raise NotImplementedError


def keys_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
    replication_connection: bool,
):
    mask = command.args[0]
    assert mask == b"*"
    result = app.store.keys()
    client.send(serialization.serialize(result))


def type_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
    replication_connection: bool,
):
    key = command.args[0]
    result = app.store.type(key)
    client.send(serialization.ser_simple_string(result))


def batched(lst, n):
    it = iter(lst)
    return iter(lambda: tuple(itertools.islice(it, n)), ())


def xadd_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
    replication_connection: bool,
):
    key = command.args[0]
    id_raw = command.args[1]

    # TODO 3.12 batched
    tuples = list(batched(command.args[2:], n=2))
    try:
        result = app.store.xadd(key, id_raw, tuples)
    except store.Err as err:
        client.send(serialization.serialize(err))
        return
    client.send(serialization.serialize(result))


def xrange_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
    replication_connection: bool,
):
    key = command.args[0]
    start = command.args[1]
    end = command.args[2]

    result = app.store.xrange(key, start, end)
    client.send(serialization.serialize(result))


def xread_command(
    app: models.App,
    client: models.Client,
    command: models.Command,
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

    result = app.store.xread(keys, raw_ids, last_xids)
    if result is not None:
        client.send(serialization.serialize(result))
        return
    if block_ms is None:
        client.send(serialization.serialize(serialization.NullBulkString))
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
    # TODO: same code
    result = app.store.xread(keys, raw_ids, last_xids)
    if result is not None:
        client.send(serialization.serialize(result))
        return
    now = datetime.datetime.now(tz=datetime.UTC)
    if end is not None and now >= end:
        client.send(serialization.serialize(serialization.NullBulkString))
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
        command: models.Command,
        replication_connection: bool,
    ):
        ...


COMMAND_MAPPING: dict[bytes, CommandProtocol] = {
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
