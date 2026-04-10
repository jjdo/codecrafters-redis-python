import asyncio
from app.resp import dump, parse, RESPStream, RESPEot, RESPError, RESPTypeKind
from app.cmd import execute


async def handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Handles a new connection from the client.

    Enters an infinite loop reading commands until the client connection is closed.
    """
    skt_fd = writer.get_extra_info("socket").fileno()
    try:
        while True:
            rt = await parse(RESPStream(reader))
            print("+++ RECEIVED", rt)
            if rt.type == RESPTypeKind.ARRAY:
                reply = dump(execute(rt))
                print("+++ SENDING", reply)
                await writer.write(reply)
            else:
                print("+++ NOT A COMMAND", rt)
                continue
    except RESPEot:
        print("Client disconnected while receving.")
    except RESPError as e:
        print("Error in RESP : %s" % e)
    finally:
        writer.close()
        await writer.wait_closed()
        print("+++ Closed handler connection %s" % skt_fd)


async def main():
    server = await asyncio.start_server(
        handle_connection, "localhost", 6379, reuse_port=True
    )

    addrs = ", ".join(
        f"({sock.getsockname()}, {sock.fileno()})" for sock in server.sockets
    )
    print("+++ Serving on %s" % addrs)

    async with server:
        try:
            await server.serve_forever()
        except Exception as e:
            print("+++ exception", e)
        finally:
            server.close()
            await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
