import socket


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client, address = server_socket.accept()
    client.sendall(b"+PONG\r\n")
    client.close()


if __name__ == "__main__":
    main()
