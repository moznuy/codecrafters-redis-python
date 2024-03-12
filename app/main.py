from __future__ import annotations

import argparse
import datetime
import functools
import logging
import os
import random
import select
import socket

from app import commands
from app import loop
from app import models
from app import serialization
from app import store


def serve_client(app: models.App, client: models.Client):
    while True:
        if not client.data:
            break

        result = serialization.read_next_value(client.data)
        if result is None:
            break

        item, client.data, parsed = result
        command = serialization.parse_command(item)
        f = commands.COMMAND_MAPPING.get(command.command.upper())
        if f is None:
            raise NotImplementedError

        f(app, client, command, replication_connection=False)
        # params.master_repl_offset += parsed


TEST_ENV = os.environ.get("TEST_ENV", "1") == "1"


def notify_master_task(app: models.App, *, client: models.Client):
    if TEST_ENV:
        return
    result = ["REPLCONF", "ACK", app.params.replica_offset]
    client.send(serialization.serialize(result))

    app.loop.add(
        functools.partial(notify_master_task, client=client),
        later=datetime.timedelta(seconds=1),
    )


def serve_master(app: models.App, client: models.Client):
    while True:
        if not client.data:
            break

        if app.params.replica_flow == 4:
            result = serialization.read_next_value_rdb(client.data)
        else:
            result = serialization.read_next_value(client.data)
        if result is None:
            break

        item, client.data, parsed = result
        if app.params.replica_flow < 4:
            # TODO: check item
            print("Replica flow", app.params.replica_flow, item)
        elif app.params.replica_flow == 4:
            # TODO: check item
            # TODO: apply rdb from master
            print("Replica flow", app.params.replica_flow, item)
            app.params.replica_offset = 0
            notify_master_task(app, client=client)
        else:  # REPLICATION is ESTABLISHED PARSE AS CLIENT BUT DO NOT RESPOND
            # TODO: code duplication
            command = serialization.parse_command(item)
            f = commands.COMMAND_MAPPING.get(command.command.upper())
            if f is None:
                raise NotImplementedError

            print(
                "REPLICATION:",
                app.params.replica_offset,
                parsed,
                app.params.replica_offset + parsed,
                item,
            )
            f(app, client, command, replication_connection=True)
            app.params.replica_offset += parsed
            # client.send(Array(a=[BulkString(b=b"PING")]).serialize())
            continue

        app.params.replica_flow += 1
        # print("FROM MASTER:", params.replica_flow, item)
        if app.params.replica_flow == 1:
            result = ["REPLCONF", "listening-port", app.params.port]
            client.send(serialization.serialize(result))
        if app.params.replica_flow == 2:
            result = ["REPLCONF", "capa", "psync2"]
            client.send(serialization.serialize(result))
        if app.params.replica_flow == 3:
            app.params.replica_offset = -1
            result = ["PSYNC", "?", app.params.replica_offset]
            client.send(serialization.serialize(result))


def main():
    # with open("dump.rdb", "wb") as file:
    #     file.write(base64.b64decode(commands.EMPTY_RDB))
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
    params = models.Params(
        master_replid=random.randbytes(20).hex(),
        port=args.port,
        dir=args.dir,
        dbfilename=args.dbfilename,
    )

    if args.replicaof:
        params.master = False
        params.master_host = args.replicaof[0]
        params.master_port = int(args.replicaof[1])

    print(f"Params : {params}")
    print("Starting Redis on port {}".format(params.port))
    os.chdir(params.dir)

    replica = None
    if not params.master:
        replica_socket = socket.create_connection(
            (params.master_host, params.master_port)
        )
        replica_socket.setblocking(False)
        replica = models.Client(socket=replica_socket)
        replica.send(serialization.serialize(["PING"]))

    server_socket = socket.create_server(("localhost", params.port), reuse_port=True)
    server_socket.setblocking(False)
    client_sockets: dict[socket.socket, models.Client] = {}

    event_loop = loop.EventLoop()
    storage = store.read_rdb(params)
    app = models.App(loop=event_loop, params=params, store=storage)
    event_loop.register(app)

    while True:
        read_sockets = list(client_sockets)
        read_sockets.append(server_socket)
        if not params.master and params.replica_active:
            read_sockets.append(replica.socket)

        timeout = event_loop.get_timeout()
        read, _, _ = select.select(read_sockets, [], [], timeout)
        for sock in read:
            if sock == server_socket:
                client, address = sock.accept()
                client_sockets[client] = models.Client(socket=client)
                continue

            if replica and sock == replica.socket:
                recv = sock.recv(4096)
                if not recv:
                    logging.warning(f"Master connection closed")
                    params.replica_active = False
                    continue
                replica.data += recv
                serve_master(app, replica)
                continue

            recv = sock.recv(4096)
            if not recv:
                sock.close()
                del client_sockets[sock]
                continue

            client = client_sockets[sock]
            client.data += recv
            serve_client(app, client)

        event_loop.run_cycle()


if __name__ == "__main__":
    main()
