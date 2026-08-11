#!/usr/bin/env python3

import argparse
import asyncio
import base64
import signal
import ssl

USERNAME = "ms-mqtt-client"
PASSWORD = "network-tests"
EXPECTED_AUTHORIZATION = "Basic " + base64.b64encode(
    f"{USERNAME}:{PASSWORD}".encode()
).decode()


async def relay(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(64 * 1024):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass


async def handle_connect(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    target_writer = None
    try:
        request_line = (await reader.readline()).decode("ascii").strip()
        headers = {}
        while line := await reader.readline():
            if line in (b"\r\n", b"\n"):
                break
            name, value = line.decode("ascii").split(":", 1)
            headers[name.lower()] = value.strip()

        if headers.get("proxy-authorization") != EXPECTED_AUTHORIZATION:
            writer.write(
                b"HTTP/1.1 407 Proxy Authentication Required\r\n"
                b'Proxy-Authenticate: Basic realm="ms-mqtt-network-tests"\r\n'
                b"Content-Length: 0\r\n\r\n"
            )
            await writer.drain()
            return

        method, authority, version = request_line.split(" ", 2)
        if method != "CONNECT" or version not in ("HTTP/1.0", "HTTP/1.1"):
            writer.write(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            return

        hostname, separator, port = authority.rpartition(":")
        if not separator or not hostname:
            raise ValueError("CONNECT target must be host:port")
        hostname = hostname.strip("[]")
        target_reader, target_writer = await asyncio.open_connection(hostname, int(port))

        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await writer.drain()

        tasks = {
            asyncio.create_task(relay(reader, target_writer)),
            asyncio.create_task(relay(target_reader, writer)),
        }
        _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    except (ConnectionError, UnicodeError, ValueError):
        if not writer.is_closing():
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
            try:
                await writer.drain()
            except ConnectionError:
                pass
    finally:
        if target_writer is not None:
            target_writer.close()
            await target_writer.wait_closed()
        writer.close()
        await writer.wait_closed()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--http-port", type=int, required=True)
    parser.add_argument("--https-port", type=int, required=True)
    parser.add_argument("--certificate", required=True)
    parser.add_argument("--private-key", required=True)
    args = parser.parse_args()

    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls_context.load_cert_chain(args.certificate, args.private_key)

    http_server = await asyncio.start_server(handle_connect, "127.0.0.1", args.http_port)
    https_server = await asyncio.start_server(
        handle_connect, "127.0.0.1", args.https_port, ssl=tls_context
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)

    async with http_server, https_server:
        await stop.wait()


if __name__ == "__main__":
    asyncio.run(main())
