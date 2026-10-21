"""A controllable TCP proxy used to partition individual workers.

Modes:

``pass``
    forward traffic.
``blackhole``
    keep connections open but stop delivering bytes in both directions until
    healed, like a partition shorter than TCP's retransmission timeout; new
    connections hang instead of being refused.  Bytes are delayed, never
    dropped, because TCP itself never silently drops bytes from a stream.
``reset``
    abort every connection with a TCP RST and refuse new ones.
"""
import asyncio
import socket
import struct
import threading


class Proxy:

    def __init__(self, listen_port, target_host, target_port):
        self.listen_port = listen_port
        self.target = (target_host, target_port)
        self.mode = 'pass'
        self._conns = set()
        self._loop = asyncio.new_event_loop()
        self._open = None
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        self._ready.wait()

    def _run(self):
        asyncio.set_event_loop(self._loop)
        self._open = asyncio.Event()
        self._open.set()
        self._server = self._loop.run_until_complete(
            asyncio.start_server(self._handle, '127.0.0.1', self.listen_port, reuse_address=True))
        self._ready.set()
        self._loop.run_forever()

    async def _handle(self, reader, writer):
        await self._open.wait()
        if self.mode == 'reset':
            _rst(writer)
            return
        try:
            up_reader, up_writer = await asyncio.open_connection(*self.target)
        except OSError:
            _rst(writer)
            return
        conn = (writer, up_writer)
        self._conns.add(conn)
        try:
            await asyncio.gather(self._pump(reader, up_writer), self._pump(up_reader, writer))
        finally:
            self._conns.discard(conn)
            for w in conn:
                w.close()

    async def _pump(self, reader, writer):
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                await self._open.wait()
                if self.mode == 'reset':
                    break
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            try:
                writer.close()
            except Exception:  # pylint: disable=broad-except
                pass

    def set_mode(self, mode):
        assert mode in ('pass', 'blackhole', 'reset'), mode
        done = threading.Event()

        def apply():
            self.mode = mode
            if mode == 'blackhole':
                self._open.clear()
            else:
                self._open.set()
            if mode == 'reset':
                for conn in list(self._conns):
                    for w in conn:
                        _rst(w)
            done.set()
        self._loop.call_soon_threadsafe(apply)
        done.wait()

    def close(self):
        self.set_mode('reset')
        self._loop.call_soon_threadsafe(self._server.close)
        self._loop.call_soon_threadsafe(self._loop.stop)


def _rst(writer):
    sock = writer.get_extra_info('socket')
    if sock is not None:
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        except OSError:
            pass
    writer.transport.abort()
