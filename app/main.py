from threading import Thread, current_thread
import socket  # noqa: F401

# There is an async task accepting connections. For each connection, creates a task to process the commands from
# that connection. The task is alive as long as the connection is alive.
#
# The connection task sits waiting for incoming messages and exeuctes them as soon as they appear.

def connection(skt):
    print("serving connections on thread", current_thread())
    try:
        while True:
            if not (chunk := receive(skt)):
                break
            if (sent := send(skt, b"+PONG\r\n")) == -1:
                break
    finally:
        skt.close()


connections = []


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        print("Waiting for commands...")
        (skt, _) = server_socket.accept() # wait for client
        # Launch thread using this socket.
        th = Thread(target=connection, name=f"socket {skt}", args=(skt,))
        connections.append(th)
        th.start()


def receive(skt) -> bytes | None:
    # TCP messages end with \n.
    reply = b""
    while not reply.endswith(b"\n"):
        r = skt.recv(256)
        if not r:
            print("Client disconnected while receving.")
            return None
        reply += r
    
    return reply.strip()
         

def send(skt, msg: bytes) -> int:
    sent = 0
    while sent < len(msg):
        if (ns := skt.send(msg[sent:])) == 0:
            print("Client disconnected while sending.")
            return -1
        sent += ns

    return sent


if __name__ == "__main__":
    main()
