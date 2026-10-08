"""TLS/H2 Gateway peer: scripted Hrana responses or a disposable App relay.

Synthetic TLS fixtures copied from sdk-python b6307ce3; no production keys.
"""

import asyncio
import json
import ssl
from contextlib import suppress
from pathlib import Path

import h11
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import DataReceived, RequestReceived, StreamEnded, StreamReset
from h2.exceptions import H2Error
from tiana_sdk import Client

FIXTURES = Path(__file__).parent / "fixtures"
ENDPOINT = "ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.service.internal.tiana.com"
TOKEN = "tia_" + "A" * 43


def result(baton="baton-secret", autocommit=True, closing=False, values=None):
    rows = [] if values is None else [values]
    cols = (
        [] if values is None else [{"name": f"c{i}", "decltype": None} for i in range(len(values))]
    )
    replies = [
        {
            "type": "ok",
            "response": {
                "type": "execute",
                "result": {
                    "cols": cols,
                    "rows": rows,
                    "affected_row_count": 1,
                    "last_insert_rowid": str(2**63 - 1),
                },
            },
        },
        {"type": "ok", "response": {"type": "get_autocommit", "is_autocommit": autocommit}},
    ]
    if closing:
        replies.append({"type": "ok", "response": {"type": "close"}})
    return {"baton": baton, "base_url": None, "results": replies}


def closed():
    return {"baton": None, "results": [{"type": "ok", "response": {"type": "close"}}]}


def http(body: bytes, *, status=200, headers=b"", chunked=False):
    start = f"HTTP/1.1 {status} response\r\n".encode()
    if chunked:
        return (
            start
            + b"Transfer-Encoding: chunked\r\n"
            + headers
            + b"\r\n"
            + f"{len(body):x}\r\n".encode()
            + body
            + b"\r\n0\r\n\r\n"
        )
    return start + f"Content-Length: {len(body)}\r\n".encode() + headers + b"\r\n" + body


class Peer:
    def __init__(self, replies=(), *, backend=None):
        self.replies = replies
        self.backend = backend
        self.requests = []
        self.connects = 0
        self.errors = []
        self.tasks = set()
        self.writers = set()
        self.observed = asyncio.Queue()
        self.disconnected = asyncio.Event()

    async def __aenter__(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = context.maximum_version = ssl.TLSVersion.TLSv1_3
        context.set_alpn_protocols(["h2"])
        context.load_cert_chain(FIXTURES / "gateway.pem", FIXTURES / "gateway-key.pem")
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0, ssl=context)
        self.address = self.server.sockets[0].getsockname()[:2]
        self.client = Client(
            ENDPOINT,
            token=TOKEN,
            gateway_address=self.address,
            ca_file=FIXTURES / "gateway.pem",
            use_system_roots=False,
        )
        return self

    async def __aexit__(self, *_):
        await self.client.aclose()
        self.server.close()
        await self.server.wait_closed()
        for writer in tuple(self.writers):
            writer.close()
        tasks = tuple(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        assert not self.errors, self.errors

    async def handle(self, reader, writer):
        current = asyncio.current_task()
        self.tasks.add(current)
        self.writers.add(writer)
        h2 = H2Connection(H2Configuration(client_side=False))
        h2.initiate_connection()
        incoming = asyncio.Queue(maxsize=8)
        window = asyncio.Event()
        worker = None

        async def send(data):
            view = memoryview(data)
            while view:
                count = min(len(view), h2.local_flow_control_window(1), h2.max_outbound_frame_size)
                if not count:
                    window.clear()
                    await window.wait()
                    continue
                h2.send_data(1, view[:count].tobytes())
                view = view[count:]
                writer.write(h2.data_to_send())
                await writer.drain()

        try:
            writer.write(h2.data_to_send())
            while data := await reader.read(16384):
                for event in h2.receive_data(data):
                    if isinstance(event, RequestReceived):
                        self.connects += 1
                        headers = dict(event.headers)
                        assert headers[b":method"] == b"CONNECT"
                        assert headers[b"tiana-database-protocol"] == b"hrana-http"
                        assert headers[b"proxy-authorization"] == b"Bearer " + TOKEN.encode()
                        h2.send_headers(
                            1,
                            [
                                (b":status", b"200"),
                                (b"tiana-tunnel-version", b"1"),
                                (b"tiana-auth-mode", b"TOKEN_REQUIRED"),
                                (b"tiana-request-id", headers[b"tiana-request-id"]),
                            ],
                        )
                        worker = asyncio.create_task(
                            self.relay(incoming, send)
                            if self.backend
                            else self.script(incoming, send, h2, writer)
                        )
                    elif isinstance(event, DataReceived):
                        await incoming.put(event.data)
                        h2.acknowledge_received_data(event.flow_controlled_length, 1)
                    elif isinstance(event, (StreamEnded, StreamReset)):
                        return
                window.set()
                writer.write(h2.data_to_send())
                await writer.drain()
        except (OSError, H2Error):
            pass
        except Exception as error:
            self.errors.append(error)
        finally:
            if worker is not None:
                worker.cancel()
                outcome = await asyncio.gather(worker, return_exceptions=True)
                if isinstance(outcome[0], Exception) and not isinstance(
                    outcome[0], (OSError, H2Error)
                ):
                    self.errors.append(outcome[0])
            writer.close()
            with suppress(OSError, TimeoutError):
                async with asyncio.timeout(1):
                    await writer.wait_closed()
            self.tasks.discard(current)
            self.writers.discard(writer)
            self.disconnected.set()

    async def script(self, incoming, send, h2, writer):
        parser = h11.Connection(h11.SERVER)
        payload = bytearray()
        index = 0
        while True:
            event = parser.next_event()
            if event is h11.NEED_DATA:
                parser.receive_data(await incoming.get())
            elif isinstance(event, h11.Request):
                assert event.method == b"POST" and event.target == b"/v3/pipeline"
                assert not any(b"authorization" in name for name, _ in event.headers)
                assert TOKEN.encode() not in repr(event.headers).encode()
            elif isinstance(event, h11.Data):
                payload.extend(event.data)
            elif isinstance(event, h11.EndOfMessage):
                request = json.loads(payload)
                self.requests.append(request)
                await self.observed.put(request)
                assert index < len(self.replies), "unexpected extra SQL request"
                reply = self.replies[index]
                index += 1
                if reply is None:
                    await asyncio.Future()
                elif reply == "reset":
                    h2.reset_stream(1)
                    writer.write(h2.data_to_send())
                    await writer.drain()
                    await asyncio.Future()
                else:
                    encoded = (
                        reply if isinstance(reply, bytes) else http(json.dumps(reply).encode())
                    )
                    await send(encoded)
                # Advance only the fixture parser; canned wire response is independent.
                parser.send(h11.Response(status_code=200, headers=[(b"content-length", b"0")]))
                parser.send(h11.EndOfMessage())
                parser.start_next_cycle()
                payload.clear()
            else:
                raise AssertionError("unexpected request event")

    async def relay(self, incoming, send):
        reader, writer = await asyncio.open_connection(*self.backend)

        async def upload():
            while True:
                writer.write(await incoming.get())
                await writer.drain()

        async def download():
            while data := await reader.read(16384):
                await send(data)

        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(upload())
                group.create_task(download())
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()
