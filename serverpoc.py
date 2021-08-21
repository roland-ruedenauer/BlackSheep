import asyncio
import logging
import os
import warnings
from asyncio import Event
from dataclasses import dataclass
from email.utils import formatdate
from multiprocessing import Process
from socket import IPPROTO_TCP, SHUT_RDWR, SO_REUSEPORT, SOL_SOCKET, TCP_NODELAY, socket
from threading import Thread
from time import sleep, time
from typing import Any, List, Optional

import httptools
import uvloop
from httptools.parser.errors import (
    HttpParserCallbackError,
    HttpParserError,
    HttpParserUpgrade,
)

from blacksheep.baseapp import BaseApplication  # TODO: replace in cimport
from blacksheep.messages import Request, Response
from blacksheep.scribe import py_is_small_response as is_small_response
from blacksheep.scribe import py_write_small_response as write_small_response
from blacksheep.scribe import write_response

# from blacksheep.connection import ServerConnection


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


__all__ = ("HTTPServer",)


server_logger = logging.getLogger("blacksheep.server")

MAX_REQUEST_HEADERS_COUNT = 80
MAX_REQUEST_HEADER_SIZE = 8192  # 8kb


def get_current_timestamp():
    return formatdate(usegmt=True).encode()


@dataclass
class HTTPServerOptions:
    host: str
    port: int
    keep_alive_timeout: int
    processes_count: int
    ssl_context: Any
    backlog: int
    no_delay: bool

    @classmethod
    def default(cls):
        # TODO: support environment variables
        return cls(
            host="localhost",
            port=8080,
            keep_alive_timeout=60,
            processes_count=1,
            ssl_context=None,
            backlog=100,
            no_delay=True,
        )


class HTTPServer:
    def __init__(
        self,
        app: BaseApplication,
        options: Optional[HTTPServerOptions] = None,
    ):
        self._app = app
        self.running = False
        self.current_timestamp = get_current_timestamp()
        self.connections = set()
        self.options = options or HTTPServerOptions.default()
        self.loop: asyncio.AbstractEventLoop
        self.debug = False

    def on_connection_lost(self):
        pass

    def close(self):
        pass

    def on_stop(self):
        self.loop.run_until_complete(asyncio.wait_for(self._app.stop(), timeout=20))


async def tick(server: HTTPServer, loop):
    while server.running:
        server.current_timestamp = get_current_timestamp()
        await asyncio.sleep(1, loop=loop)


async def monitor_connections(server: HTTPServer, loop):
    while server.running:
        current_time = time()

        for connection in server.connections.copy():
            inactive_for = current_time - connection.time_of_last_activity
            if inactive_for > server.options.keep_alive_timeout:
                server_logger.debug(
                    f"[*] Closing idle connection, inactive for: {inactive_for}."
                )

                if not connection.closed:
                    connection.close()
                try:
                    server.connections.remove(connection)
                except ValueError:
                    pass

            if connection.closed:
                try:
                    server.connections.remove(connection)
                except ValueError:
                    pass

        await asyncio.sleep(1, loop=loop)


def monitor_processes(server: HTTPServer, processes: List[Process]):
    while server.running:
        sleep(5)
        if not server.running:
            return

        dead_processes = [p for p in processes if not p.is_alive()]
        for dead_process in dead_processes:
            server_logger.warning(
                f"Process {dead_process.pid} died; removing and spawning a new one."
            )

            try:
                processes.remove(dead_process)
            except ValueError:
                # this means that the process was not anymore inside processes list
                pass
            else:
                p = Process(target=spawn_server, args=(server,))
                p.start()
                processes.append(p)

                server_logger.warning(
                    f"Spawned a new process {p.pid}, to replace "
                    f"dead process {dead_process.pid}."
                )


class ConnectionProtocol:
    def __init__(self, handler, loop) -> None:
        self.url: bytes
        self.method: str
        self.handler = handler
        self.headers = []
        self.request: Optional[Request] = None
        self.transport = None
        self.parser = httptools.HttpRequestParser(self)  # type: ignore
        self.loop = loop
        self.ignore_more_body = False
        self.closed = False
        self.reading_paused = False
        self.writing_paused = False
        self.request_complete = Event()

    def on_url(self, url: bytes):
        self.url = url
        self.method = self.parser.get_method().decode()

    def on_header(self, name: bytes, value: bytes):
        self.headers.append((name, value))

        if (
            len(self.headers) > MAX_REQUEST_HEADERS_COUNT
            or len(value) > MAX_REQUEST_HEADER_SIZE
        ):
            self.transport.write(write_small_response(Response(413)))
            self.dispose()

    def on_message_complete(self):
        # TODO: come gestire request body?
        print("EOF")

    def on_headers_complete(self):
        # cdef Request request
        request = Request(self.method, self.url, self.headers)
        self.request = request
        self.loop.create_task(self.handle_request(request))

        if self.method in {"GET", "HEAD", "TRACE"}:
            self.request_complete.set()  # methods without body

    def connection_made(self, transport):
        self.time_of_last_activity = time()
        self.transport = transport

    def connection_lost(self, exc):
        self.dispose()

    def close(self):
        self.dispose()

    def dispose(self):
        pass

    async def handle_request(self, request):
        try:
            response = await self.handler(request)
        except Exception:
            server_logger.exception("Unhandled request handler exception.")
            response = Response(500)

        # the request was handled: ignore any more body the client might be sending
        # for example, if the client tried to upload a file to a non-existing endpoint;
        # we return immediately 404 even though the client is still writing the content
        # in the socket
        self.ignore_more_body = True

        # connection might get closed while the application is handling a request
        if self.closed:
            return

        if is_small_response(response):
            self.transport.write(write_small_response(response))
        else:
            async for chunk in write_response(response):

                if self.closed:
                    return

                # if self.writing_paused:
                #     await self.writable.wait()
                self.transport.write(chunk)

        await self.reset_when_request_complete()

    async def reset_when_request_complete(self):
        # we need to wait for the client to send the message is sending,
        # before resetting this connection; because otherwise we cannot handle cleanly
        # situations where we send a response before getting the full request
        await self.request_complete.wait()

        if not self.parser.should_keep_alive():
            self.dispose()
        self.reset()

    def reset(self):
        self.request = None
        self.parser = httptools.HttpRequestParser(self)
        self.reading_paused = False
        self.writing_paused = False
        self.url = b""
        self.method = ""
        self.headers = []
        self.ignore_more_body = False

    def pause_reading(self):
        self.reading_paused = True
        self.transport.pause_reading()

    def resume_reading(self):
        self.reading_paused = False
        self.transport.resume_reading()

    def data_received(self, data: bytes):
        self.time_of_last_activity = time()

        try:
            self.parser.feed_data(data)
        except AttributeError:
            if self.transport:
                pass
            else:
                raise
        except HttpParserCallbackError:
            self.dispose()
            raise
        except HttpParserError:
            # TODO: support logging this event
            self.dispose()
        except HttpParserUpgrade:
            self.handle_upgrade()

    def handle_upgrade(self):
        print("TODO: upgrade")


def spawn_server(server: HTTPServer):
    loop = asyncio.new_event_loop()
    server.loop = loop

    if server.debug:
        loop.set_debug(True)
    asyncio.set_event_loop(loop)

    options = server.options

    if server.debug:
        loop.set_debug(True)

    s = socket()
    s.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1)

    if options.no_delay:
        try:
            s.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        except (OSError, NameError):
            warnings.warn(
                "ServerOptions set for NODELAY, but this option is not supported "
                "(caused OSError or NameError on this host).",
                RuntimeWarning,
            )

    s.bind((options.host, options.port))

    # TODO: Cythonize this, too
    def protocol_factory() -> ConnectionProtocol:
        return ConnectionProtocol(handler=server._app.handle, loop=loop)

    asyncio_server = loop.create_server(  # type: ignore
        protocol_factory,
        sock=s,
        reuse_port=options.processes_count > 1,
        backlog=options.backlog,
        # ssl=options.ssl_context,
    )
    loop.create_task(tick(server, loop))
    loop.create_task(monitor_connections(server, loop))

    def on_stop():
        loop.stop()
        server.on_stop()

    # TODO: run app on_start!!!
    loop.run_until_complete(server._app.start())  # type: ignore
    # if server.on_start:
    #     loop.run_until_complete(server.on_start.fire())

    process_id = os.getpid()
    listening_on = "".join(
        [
            "https://" if options.ssl_context else "http://",
            options.host or "localhost",
            ":",
            str(options.port),
        ]
    )
    print(f"[*] Process {process_id} is listening on {listening_on}")
    loop.run_until_complete(asyncio_server)

    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        server.running = False
        pending = asyncio.all_tasks()
        try:
            if pending:
                print(f"[*] Process {process_id} is completing pending tasks")
                try:
                    loop.run_until_complete(
                        asyncio.wait_for(
                            asyncio.gather(*pending), timeout=20, loop=loop
                        )
                    )
                except asyncio.TimeoutError:
                    pass

            server.on_stop()

        except KeyboardInterrupt:
            pass

        on_stop()
        try:
            s.shutdown(SHUT_RDWR)
        except OSError:
            pass
        s.close()
        server.close()
        loop.close()


def run_server(server: HTTPServer):
    multi_process = server.options.processes_count > 1

    if multi_process:
        print(f"[*] Using multiprocessing ({server.options.processes_count})")
        print(f"[*] Master process id: {os.getpid()}")
        processes = []
        for _ in range(server.options.processes_count):
            p = Process(target=spawn_server, args=(server,))
            p.start()
            processes.append(p)

        monitor_thread = None
        if not server.debug:
            monitor_thread = Thread(
                target=monitor_processes, args=(server, processes), daemon=True
            )
            monitor_thread.start()

        for p in processes:
            try:
                p.join()
            except (KeyboardInterrupt, SystemExit):
                server.running = False

        if monitor_thread:
            try:
                monitor_thread.join(30)
            except (KeyboardInterrupt, SystemExit):
                pass
    else:
        print(
            f"[*] Using single process. To enable multiprocessing, "
            "use `processes_count` in ServerOptions"
        )
        try:
            spawn_server(server)
        except (KeyboardInterrupt, SystemExit):
            print("[*] Exit")


if __name__ == "__main__":

    from blacksheep.server import Application

    app = Application()

    @app.router.get("/")
    def home():
        return "Hello, World"

    run_server(
        HTTPServer(
            app, options=HTTPServerOptions("localhost", 44666, 60, 1, None, 100, True)
        )
    )
