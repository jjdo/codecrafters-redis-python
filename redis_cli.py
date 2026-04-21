import threading
import socket


def send_cmd(th: str, cmd: bytes):
    print("Sending command %s from thread %s" % (cmd, th))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", 6379))
        s.sendall(cmd)
        reply = s.recv(1024)
        print("Received %s" % reply)


def main():
    # Create 3 threads each sending one command.
    commands = [
        ("PING", b"*1\r\n$4\r\nPING\r\n"),
        ("ECHO 1", b"*2\r\n$4\r\nECHO\r\n$6\r\nHello!\r\n"),
        ("ECHO 2", b"*2\r\n$4\r\nECHO\r\n$4\r\nBye!\r\n"),
    ]
    threads = [
        threading.Thread(
            target=send_cmd,
            args=(th, cmd),
        )
        for (th, cmd) in commands
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
