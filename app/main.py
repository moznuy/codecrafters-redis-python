import select
import socket


def serve_client_once(client: socket.socket) -> bool:
    data = client.recv(4096)
    if not data:
        return False
    strings = data.split(b'\r\n')
    pings = sum(1 for string in strings if string == b'ping')
    for _ in range(pings):
        client.sendall(b"+PONG\r\n")
    return True


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    client_sockets: list[socket.socket] = []

    while True:
        read_sockets = [server_socket] + client_sockets
        read, _, _ = select.select(read_sockets, [], [], None)
        for sock in read:
            if sock == server_socket:
                client, address = server_socket.accept()
                client_sockets.append(client)
                continue

            valid = serve_client_once(sock)
            if valid:
                continue
            sock.close()
            client_sockets.remove(sock)


if __name__ == "__main__":
    main()
