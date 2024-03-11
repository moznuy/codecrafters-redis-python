import socket
from typing import Any


def serve_client(client_tuple: tuple[socket.socket, Any]):
    client, address = client_tuple
    while True:
        data = client.recv(4096)
        if not data:
            break
        strings = data.split(b'\r\n')
        pings = sum(1 for string in strings if string == b'ping')
        for _ in range(pings):
            client.sendall(b"+PONG\r\n")
    client.close()


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client_tuple = server_socket.accept()
    serve_client(client_tuple)


if __name__ == "__main__":
    main()
