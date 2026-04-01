import socket  # noqa: F401

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        (skt, _) = server_socket.accept() # wait for client

        try:
            chunk = receive(skt)
            if not chunk:
                continue

            sent = send(skt, b"+PONG\r\n")
            if sent == -1:
                print("Client disconnected while sending.")
        finally:
            skt.close()


def receive(skt) -> bytes | None:
    # TCP messages end with \n.
    reply = b""
    while True:
        r = skt.recv(256)
        if not r:
            print("Client disconnected while receving.")
            return None
        reply += r
        if reply.endswith(b"\n"):
            return reply.strip()
         

def send(skt, msg: bytes) -> int:
    sent = 0
    while sent < len(msg):
        if (ns := skt.send(msg[sent:])) == 0:
            return -1
        sent += ns
    return sent


if __name__ == "__main__":
    main()
