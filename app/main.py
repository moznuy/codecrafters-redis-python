import socket
from typing import Any


def serve_client(client_tuple: tuple[socket.socket, Any]):
    client, address = client_tuple
    data = client.recv(4096)
    strings = data.split(b'\n')
    for _ in strings:
        client.sendall(b"+PONG\r\n")
    client.close()


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client_tuple = server_socket.accept()
    serve_client(client_tuple)


if __name__ == "__main__":
    main()
