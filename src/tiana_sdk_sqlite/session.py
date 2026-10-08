"""Exclusive SQLite sessions over a generic Tiana channel, with no SQL replay."""

import asyncio
import math
from enum import Enum
from typing import Self

import h11
from tiana_sdk import Client, ConnectError, Tunnel

from ._wire import MAX_BODY_BYTES, Parameters, Result, decode_pipeline, dumps, loads, statement
from .errors import Error, invalid_response

HRANA_HTTP_PROTOCOL = "hrana-http"
MAX_HEADER_BYTES = 32 * 1024


class TransactionMode(Enum):
    DEFERRED = "BEGIN DEFERRED"
    IMMEDIATE = "BEGIN IMMEDIATE"
    EXCLUSIVE = "BEGIN EXCLUSIVE"


class Session:
    """A lazy dedicated session. Concurrent operations fail with SESSION_BUSY.

    Use async context management or aclose(). Task cancellation invalidates this
    session. Transport loss does not establish rollback or permit SQL replay.
    The supplied Client remains caller-owned.
    """

    def __init__(self, client: Client, *, request_timeout: float = 30.0):
        try:
            valid_timeout = (
                type(request_timeout) in (int, float)
                and math.isfinite(request_timeout)
                and request_timeout > 0
            )
        except OverflowError:
            valid_timeout = False
        if not valid_timeout:
            raise Error("INVALID_TIMEOUT")
        self._client = client
        self._timeout = request_timeout
        self._state = "fresh"
        self._tunnel: Tunnel | None = None
        self._http: h11.Connection | None = None
        self._baton: str | None = None
        self._autocommit: bool | None = True
        self._request_id: str | None = None
        self._busy = False
        self._task: asyncio.Task | None = None

    @property
    def autocommit(self) -> bool | None:
        return self._autocommit

    @property
    def request_id(self) -> str | None:
        return self._request_id

    @property
    def is_usable(self) -> bool:
        return self._state in ("fresh", "ready") and not self._busy

    def _error(self, code: str, unknown: bool = False) -> Error:
        return Error(code, outcome_unknown=unknown, request_id=self.request_id)

    def _check(self):
        if self._busy:
            raise self._error("SESSION_BUSY")
        if self._state == "closed":
            raise self._error("SESSION_CLOSED")
        if self._state == "unusable":
            raise self._error("SESSION_UNUSABLE", True)

    async def execute(self, sql: str, parameters: Parameters = ()) -> Result:
        """Execute one statement, returning metadata without rows."""
        return await self._run(sql, parameters, rows=False, closing=False)

    async def query(self, sql: str, parameters: Parameters = ()) -> Result:
        """Execute one statement and buffer typed rows within the response limit."""
        return await self._run(sql, parameters, rows=True, closing=False)

    async def execute_and_close(self, sql: str, parameters: Parameters = ()) -> Result:
        """Execute/get_autocommit/close in one pipeline; close does not commit."""
        return await self._run(sql, parameters, rows=False, closing=True)

    async def _run(self, sql, parameters, *, rows, closing):
        self._check()
        try:
            stmt = statement(sql, parameters, rows)
        except Error as error:
            raise self._error(error.code, error.outcome_unknown) from None
        requests = [{"type": "execute", "stmt": stmt}, {"type": "get_autocommit"}]
        if closing:
            requests.append({"type": "close"})
        return await self._pipeline(requests, closing, self._timeout)

    async def begin(self, mode: TransactionMode = TransactionMode.DEFERRED) -> None:
        self._check()
        if not isinstance(mode, TransactionMode):
            raise self._error("INVALID_ARGUMENT")
        await self._transaction(mode.value, True)

    async def commit(self) -> None:
        await self._transaction("COMMIT", False)

    async def rollback(self) -> None:
        await self._transaction("ROLLBACK", False)

    async def _transaction(self, sql: str, before: bool):
        self._check()
        if self.autocommit is not before:
            raise self._error("TRANSACTION_STATE")
        await self.execute(sql)
        if self.autocommit is not (not before):
            self.abort()
            raise self._error("INVALID_RESPONSE", True)

    async def aclose(self) -> None:
        """Release a known server stream, then the local channel. Never commit."""
        if self._state == "closed" and not self._busy:
            return
        self._check()
        if self._state == "fresh":
            self._state, self._autocommit = "closed", None
            return
        await self._pipeline([{"type": "close"}], True, min(3.0, self._timeout))

    def abort(self) -> None:
        """Release local I/O only; server rollback/cleanup may wait for its TTL."""
        if self._state != "closed":
            self._state = "unusable"
        self._autocommit = None
        self._baton = None
        if self._tunnel is not None:
            self._tunnel.close()
        if self._task is not None and self._task is not asyncio.current_task():
            self._task.cancel()

    async def __aenter__(self) -> Self:
        self._check()
        return self

    async def __aexit__(self, exc_type, _exc, _tb):
        try:
            await self.aclose()
        except Error:
            if exc_type is None:
                raise
        finally:
            if self._tunnel is not None:
                self.abort()
                await self._tunnel.wait_closed()
                self._tunnel = None

    async def _pipeline(self, requests: list[dict], closing: bool, timeout: float):
        self._check()
        try:
            body = dumps({"baton": self._baton, "requests": requests})
        except Error as error:
            raise self._error(error.code, error.outcome_unknown) from None
        self._busy, self._task = True, asyncio.current_task()
        self._state, self._autocommit = "unusable", None
        sent = False
        try:
            async with asyncio.timeout(timeout):
                if self._tunnel is None:
                    self._tunnel = await self._client.connect(HRANA_HTTP_PROTOCOL)
                    self._request_id = self._tunnel.request_id
                    self._http = h11.Connection(
                        h11.CLIENT, max_incomplete_event_size=MAX_HEADER_BYTES
                    )
                # From this point onwards a partial write may have reached SQLite.
                sent = True
                data, disconnect = await self._exchange(body)
                baton, result, error, autocommit = decode_pipeline(data, requests, closing)
                if error is not None and error.outcome_unknown:
                    raise error
                if closing:
                    self._state = "closed"
                elif not disconnect:
                    self._http.start_next_cycle()
                    self._state, self._autocommit, self._baton = "ready", autocommit, baton
        except asyncio.CancelledError:
            self.abort()
            raise
        except TimeoutError:
            self.abort()
            raise self._error("TIMEOUT", sent) from None
        except Error as error:
            self.abort()
            raise self._error(error.code, error.outcome_unknown) from None
        except (ConnectError, OSError, h11.ProtocolError):
            self.abort()
            raise self._error("TRANSPORT_ERROR" if sent else "CONNECT_FAILED", sent) from None
        except BaseException:
            self.abort()
            raise
        finally:
            try:
                if self._state in ("unusable", "closed") and self._tunnel is not None:
                    tunnel, self._tunnel = self._tunnel, None
                    self._http, self._baton = None, None
                    tunnel.close()
                    await tunnel.wait_closed()
            finally:
                self._busy, self._task = False, None
        if error is not None:
            raise self._error(error.code, error.outcome_unknown)
        return result

    async def _exchange(self, body: bytes):
        request = h11.Request(
            method=b"POST",
            target=b"/v3/pipeline",
            headers=[
                (b"host", b"localhost"),
                (b"content-type", b"application/json"),
                (b"accept", b"application/json"),
                (b"content-length", str(len(body)).encode("ascii")),
            ],
        )
        await self._tunnel.write(self._http.send(request))
        view = memoryview(body)
        for start in range(0, len(body), 16 * 1024):
            await self._tunnel.write(
                self._http.send(h11.Data(data=view[start : start + 16 * 1024]))
            )
        await self._tunnel.write(self._http.send(h11.EndOfMessage()))
        response, header = None, bytearray()
        collected = bytearray()
        while True:
            event = self._http.next_event()
            if event is h11.NEED_DATA:
                data = await self._tunnel.read(16 * 1024)
                if response is None:
                    header.extend(data)
                    end = header.find(b"\r\n\r\n")
                    if (len(header) if end < 0 else end + 4) > MAX_HEADER_BYTES:
                        raise self._error("INVALID_RESPONSE", True)
                self._http.receive_data(data)
            elif isinstance(event, h11.Response):
                if response is not None:
                    raise invalid_response()
                response = event
                headers = dict(event.headers)
                if b"content-encoding" in headers:
                    raise invalid_response()
                length = headers.get(b"content-length")
                if length is not None and (
                    not length.isdigit() or len(length) > 20 or int(length) > MAX_BODY_BYTES
                ):
                    raise self._error("RESPONSE_TOO_LARGE", True)
                header.clear()
            elif isinstance(event, h11.Data):
                if response is None:
                    raise invalid_response()
                if len(collected) + len(event.data) > MAX_BODY_BYTES:
                    raise self._error("RESPONSE_TOO_LARGE", True)
                collected.extend(event.data)
            elif isinstance(event, h11.EndOfMessage):
                if response is None or event.headers or self._http.trailing_data[0]:
                    raise invalid_response()
                if response.status_code != 200:
                    try:
                        rejection = loads(collected)
                    except Error:
                        rejection = None
                    code = rejection.get("code") if isinstance(rejection, dict) else None
                    if code == "BATON_INVALID":
                        raise self._error("BATON_INVALID")
                    if isinstance(code, str) and code in {
                        "STREAM_EXPIRED",
                        "STREAM_NOT_FOUND",
                        "STREAM_LIMIT",
                        "SERVICE_STOPPING",
                        "REQUEST_TOO_LARGE",
                    }:
                        raise self._error(code, True)
                    raise self._error("HTTP_REJECTED", True)
                disconnect = (
                    self._http.their_state is h11.MUST_CLOSE
                    or self._http.our_state is h11.MUST_CLOSE
                )
                return collected, disconnect
            else:
                # Informational/upgrade responses, unsolicited events and EOF.
                raise invalid_response()
