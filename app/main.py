from threading import Thread, current_thread
import socket  # noqa: F401
from app.resp import dump, parse, RESPSocket, RESPEot, RESPError, RESPTypeKind
from app.cmd import execute


def connection(skt):
    print("serving connections on thread '%s'" % current_thread().name)
    try:
        while True:
            rt = parse(RESPSocket(skt))
            print("+++ RECEIVED", rt)
            if rt.type == RESPTypeKind.ARRAY:
                reply = dump(execute(rt))
                print("+++ SENDING", reply)
                if send(skt, reply) == -1:
                    break
            else:
                print("+++ NOT A COMMAND", rt)
    except RESPEot:
        print("Client disconnected while receving.")
    except RESPError as e:
        print("Error in RESP : %s" % e)
    finally:
        skt.close()


connections = []


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Serving on", server_socket.getsockname())

    try:
        while True:
            print("Waiting for commands...")
            (skt, _) = server_socket.accept()  # wait for client
            # Launch thread using this socket.
            th = Thread(target=connection, name=f"socket {skt.fileno()}", args=(skt,))
            connections.append(th)
            th.start()
    finally:
        server_socket.close()


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
