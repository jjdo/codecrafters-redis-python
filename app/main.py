from threading import Thread, current_thread
from app.resp import RESP, RESPSocket, RESPEot, RESPError, RESPTypeKind
from app.cmd import execute
import socket  # noqa: F401


def connection(skt):
    print("serving connections on thread '%s'" % current_thread().name)
    resp = RESP()
    try:
        while True:
            rt = resp.parse(RESPSocket(skt))
            print("+++ RECEIVED", rt)
            if rt.type == RESPTypeKind.ARRAY:
                reply = execute(rt)
                print("+++ SENDING", reply)
                if send(skt, reply) == -1:
                    break
            else:
                print("+++ NOT A COMMAND", rt)
                continue
    except RESPError as e:
        print("Error in RESP : %s" % e)
    except RESPEot:
        print("Client disconnected while receving.")
    finally:
        skt.close()


connections = []


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        print("Waiting for commands...")
        (skt, _) = server_socket.accept()  # wait for client
        # Launch thread using this socket.
        th = Thread(target=connection, name=f"socket {skt.fileno()}", args=(skt,))
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
