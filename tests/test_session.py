import asyncio
import json
import math
import os
import unittest

from tiana_sdk import Client

from tiana_sdk_sqlite import MAX_BODY_BYTES, Error, Session, TransactionMode

from .peer import ENDPOINT, Peer, closed, http, result


class SessionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.deadline = asyncio.timeout(10)
        await self.deadline.__aenter__()

    async def asyncTearDown(self):
        await self.deadline.__aexit__(None, None, None)

    async def test_configuration_rejects_invalid_timeouts(self):
        async with Peer() as peer:
            for timeout in [0, -1, True, "5", math.inf, math.nan, 10**1000]:
                with self.assertRaises(Error) as caught:
                    Session(peer.client, request_timeout=timeout)
                self.assertEqual(caught.exception.code, "INVALID_TIMEOUT")
            self.assertEqual(peer.connects, 0)

    async def test_typed_values_batons_and_explicit_close(self):
        wire = [
            {"type": "null"},
            {"type": "integer", "value": str(-(2**63))},
            {"type": "float", "value": 1.25},
            {"type": "text", "value": "中文\0"},
            {"type": "blob", "base64": "AP8"},
        ]
        async with Peer([result(baton="1", values=wire), result(baton="2"), closed()]) as peer:
            session = Session(peer.client)
            self.assertTrue(session.autocommit)
            values = (None, -(2**63), 1.25, "中文\0", b"\0\xff")
            output = await session.query("SELECT ?,?,?,?,?", values)
            self.assertEqual(output.rows, (values,))
            self.assertEqual(output.last_insert_rowid, 2**63 - 1)
            self.assertEqual(output.affected_row_count, 1)
            self.assertNotIn("中文", repr(output))
            self.assertTrue(session.request_id.startswith("req-"))
            await session.execute("SELECT :v", {"v": True})
            await session.aclose()
            await session.aclose()
            self.assertFalse(peer.client._tunnels)
            self.assertFalse(session.is_usable)
            self.assertIsNone(session.autocommit)
            self.assertEqual(peer.connects, 1)
            self.assertEqual([r["baton"] for r in peer.requests], [None, "1", "2"])
            self.assertEqual(peer.requests[0]["requests"][0]["stmt"]["args"], wire)
            self.assertTrue(peer.requests[0]["requests"][0]["stmt"]["want_rows"])
            self.assertEqual(
                peer.requests[1]["requests"][0]["stmt"]["named_args"],
                [{"name": "v", "value": {"type": "integer", "value": "1"}}],
            )

    async def test_transactions_failed_commit_and_raw_savepoints(self):
        busy = result(autocommit=False)
        busy["results"][0] = {
            "type": "error",
            "error": {"code": "SQLITE_BUSY", "message": "private SQL"},
        }
        async with Peer(
            [
                result(autocommit=False),
                result(autocommit=False),
                busy,
                result(),
                result(autocommit=False),
                result(),
                closed(),
            ]
        ) as peer:
            async with Session(peer.client) as session:
                with self.assertRaises(Error):
                    await session.commit()
                await session.begin(TransactionMode.IMMEDIATE)
                with self.assertRaises(Error):
                    await session.begin()
                await session.execute("SAVEPOINT inner")
                with self.assertRaises(Error) as caught:
                    await session.commit()
                self.assertEqual(caught.exception.code, "SQLITE_BUSY")
                self.assertFalse(caught.exception.outcome_unknown)
                self.assertNotIn("private", str(caught.exception))
                self.assertFalse(session.autocommit)
                await session.rollback()
                await session.begin(TransactionMode.EXCLUSIVE)
                await session.commit()
            sql = [r["requests"][0]["stmt"]["sql"] for r in peer.requests[:-1]]
            self.assertEqual(
                sql,
                [
                    "BEGIN IMMEDIATE",
                    "SAVEPOINT inner",
                    "COMMIT",
                    "ROLLBACK",
                    "BEGIN EXCLUSIVE",
                    "COMMIT",
                ],
            )

    async def test_invalid_inputs_preserve_fresh_and_active_sessions(self):
        async with Peer([result(autocommit=False), closed()]) as peer:
            async with Session(peer.client) as session:
                invalid = [
                    (None, ()),
                    ("x" * (MAX_BODY_BYTES + 1), ()),
                    ("\0" * (MAX_BODY_BYTES // 4), ()),
                    ("select", "string"),
                    ("select", [2**63]),
                    ("select", [-(2**63) - 1]),
                    ("select", [math.nan]),
                    ("select", [math.inf]),
                    ("select", ["\ud800"]),
                    ("select", {"": 1}),
                    ("select", [b"x" * MAX_BODY_BYTES]),
                ]
                for sql, params in invalid:
                    with self.assertRaises(Error) as caught:
                        await session.execute(sql, params)
                    self.assertFalse(caught.exception.outcome_unknown)
                    self.assertTrue(session.is_usable)
                self.assertEqual(peer.connects, 0)
                await session.begin()
                with self.assertRaises(Error):
                    await session.query("select ?", [math.nan])
                self.assertFalse(session.autocommit)
                self.assertTrue(session.is_usable)
        async with Peer() as peer:
            session = Session(peer.client)
            await session.aclose()
            self.assertEqual(peer.connects, 0)

    async def test_cancel_and_timeout_poison_without_replay(self):
        for timeout in [False, True]:
            async with Peer([None]) as peer:
                session = Session(peer.client, request_timeout=0.1 if timeout else 3)
                task = asyncio.create_task(session.execute("INSERT INTO t VALUES ('private')"))
                await peer.observed.get()
                if timeout:
                    with self.assertRaises(Error) as caught:
                        await task
                    self.assertEqual(caught.exception.code, "TIMEOUT")
                    self.assertTrue(caught.exception.outcome_unknown)
                else:
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                self.assertFalse(session.is_usable)
                self.assertIsNone(session.autocommit)
                await peer.disconnected.wait()
                for operation in [session.aclose(), session.execute("SELECT 1")]:
                    with self.assertRaises(Error) as caught:
                        await operation
                    self.assertEqual(caught.exception.code, "SESSION_UNUSABLE")
                self.assertEqual(len(peer.requests), 1)
                self.assertFalse(peer.client._tunnels)

    async def test_busy_is_rejected_without_disrupting_owner(self):
        async with Peer([None]) as peer:
            session = Session(peer.client)
            task = asyncio.create_task(session.execute("INSERT INTO t VALUES(1)"))
            await peer.observed.get()
            with self.assertRaises(Error) as caught:
                await session.query("SELECT 1")
            self.assertEqual(caught.exception.code, "SESSION_BUSY")
            self.assertFalse(task.done())
            session.abort()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(len(peer.requests), 1)

    async def test_reset_and_unknown_sql_errors_are_terminal(self):
        for code in [
            "SQLITE_IOERR",
            "RESULT_TOO_LARGE",
            "RESPONSE_TOO_LARGE",
            "UNTRUSTED_private",
            "reset",
        ]:
            reply = result()
            reply["results"][0] = {"type": "error", "error": {"code": code, "message": "private"}}
            async with Peer(["reset" if code == "reset" else reply]) as peer:
                session = Session(peer.client)
                with self.assertRaises(Error) as caught:
                    await session.execute("INSERT INTO t VALUES (1)")
                self.assertTrue(caught.exception.outcome_unknown)
                self.assertNotIn("private", repr(caught.exception))
                self.assertFalse(session.is_usable)
                with self.assertRaises(Error):
                    await session.execute("SELECT 1")
                self.assertEqual(peer.connects, 1)

    async def test_known_sql_error_reuse_and_context_does_not_mask_error(self):
        reply = result()
        reply["results"][0] = {"type": "error", "error": {"code": "SQLITE_CONSTRAINT"}}
        async with Peer([reply, closed()]) as peer:
            with self.assertRaises(Error) as caught:
                async with Session(peer.client) as session:
                    await session.execute("INSERT INTO t VALUES(1)")
            self.assertEqual(caught.exception.code, "SQLITE_CONSTRAINT")
            self.assertEqual(len(peer.requests), 2)
        async with Peer(["reset"]) as peer:
            with self.assertRaises(Error) as caught:
                async with Session(peer.client) as session:
                    await session.execute("INSERT INTO t VALUES(1)")
            self.assertEqual(caught.exception.code, "TRANSPORT_ERROR")

    async def test_http_rejections_and_limits(self):
        for body, headers, status, code, unknown in [
            (b'{"code":"BATON_INVALID"}', b"", 400, "BATON_INVALID", False),
            (b'{"code":"STREAM_EXPIRED"}', b"", 503, "STREAM_EXPIRED", True),
            (b"", b"Location: https://example.com/\r\n", 302, "HTTP_REJECTED", True),
            (b"", b"Content-Encoding: gzip\r\n", 200, "INVALID_RESPONSE", True),
            (b"x" * (MAX_BODY_BYTES + 1), b"", 200, "RESPONSE_TOO_LARGE", True),
            (b"", b"X-Large: " + b"x" * 32768 + b"\r\n", 200, "INVALID_RESPONSE", True),
        ]:
            async with Peer([http(body, status=status, headers=headers)]) as peer:
                session = Session(peer.client)
                with self.assertRaises(Error) as caught:
                    await session.execute("select 1")
                self.assertEqual(caught.exception.code, code)
                self.assertEqual(caught.exception.outcome_unknown, unknown)
                self.assertFalse(session.is_usable)

    async def test_chunked_and_connection_close(self):
        async with Peer(
            [http(json.dumps(result(None, closing=True)).encode(), chunked=True)]
        ) as peer:
            session = Session(peer.client)
            await session.execute_and_close("select 1")
            await session.aclose()
            self.assertFalse(session.is_usable)
        async with Peer([http(b"x" * (MAX_BODY_BYTES + 1), chunked=True)]) as peer:
            with self.assertRaises(Error) as caught:
                await Session(peer.client).execute("select 1")
            self.assertEqual(caught.exception.code, "RESPONSE_TOO_LARGE")
        async with Peer(
            [http(json.dumps(result()).encode(), headers=b"Connection: close\r\n")]
        ) as peer:
            session = Session(peer.client)
            self.assertEqual((await session.execute("select 1")).affected_row_count, 1)
            self.assertFalse(session.is_usable)
            self.assertIsNone(session.autocommit)

    async def test_malformed_shapes_types_and_json(self):
        bad = []
        v = result()
        v["base_url"] = "https://private.example"
        bad.append(v)
        for baton in [None, "", "x" * 4097, "\ud800"]:
            v = result(baton)
            bad.append(v)
        for field, value in [
            ("cols", None),
            ("rows", [[{"type": "null"}]]),
            ("affected_row_count", True),
            ("affected_row_count", 2**64),
            ("last_insert_rowid", str(2**63)),
        ]:
            v = result()
            v["results"][0]["response"]["result"][field] = value
            bad.append(v)
        v = result()
        v["results"][0]["error"] = {}
        bad.append(v)
        v = result()
        v["results"][1]["response"]["is_autocommit"] = 1
        bad.append(v)
        v = result()
        v["results"].pop()
        bad.append(v)
        for value in [
            None,
            {"type": "null", "value": 1},
            {"type": "integer", "value": 7},
            {"type": "integer", "value": str(2**63)},
            {"type": "float", "value": True},
            {"type": "blob", "base64": "AP8="},
            {"type": "blob", "base64": "AP9"},
            {"type": "text", "value": "\ud800"},
            {"type": "unknown"},
        ]:
            bad.append(result(values=[value]))
        bad.extend(
            [
                http(b"\xff"),
                http(b'{"results":[],"results":[]}'),
                http(b'{"value":NaN}'),
                http(json.dumps(result()).encode("utf-16")),
            ]
        )
        for index, response in enumerate(bad):
            with self.subTest(case=index):
                async with Peer([response]) as peer:
                    session = Session(peer.client)
                    with self.assertRaises(Error) as caught:
                        await session.query("select 'private'")
                    self.assertEqual(caught.exception.code, "INVALID_RESPONSE")
                    self.assertTrue(caught.exception.outcome_unknown)
                    self.assertFalse(session.is_usable)

    async def test_transaction_ack_and_close_ack_are_validated(self):
        async with Peer([result()]) as peer:
            session = Session(peer.client)
            with self.assertRaises(Error) as caught:
                await session.begin()
            self.assertTrue(caught.exception.outcome_unknown)
            self.assertFalse(session.is_usable)
        async with Peer([result(closing=True)]) as peer:
            with self.assertRaises(Error):
                await Session(peer.client).execute_and_close("select 1")

    async def test_connect_failure_is_before_sql(self):
        server = await asyncio.start_server(lambda r, w: w.close(), "127.0.0.1", 0)
        address = server.sockets[0].getsockname()[:2]
        server.close()
        await server.wait_closed()
        async with Client(ENDPOINT, gateway_address=address) as client:
            session = Session(client)
            with self.assertRaises(Error) as caught:
                await session.execute("INSERT INTO t VALUES(1)")
            self.assertEqual(caught.exception.code, "CONNECT_FAILED")
            self.assertFalse(caught.exception.outcome_unknown)
            self.assertFalse(session.is_usable)

    @unittest.skipUnless(
        os.environ.get("TIANA_SQLITE_TEST_ADDRESS"), "requires disposable loopback App SQLite"
    )
    async def test_real_app_sqlite(self):
        import ipaddress

        host, port = os.environ["TIANA_SQLITE_TEST_ADDRESS"].rsplit(":", 1)
        self.assertTrue(ipaddress.ip_address(host).is_loopback)
        async with Peer(backend=(host, int(port))) as peer:
            async with Session(peer.client) as a, Session(peer.client) as b:
                await a.execute(
                    "CREATE TABLE python_sdk_test(id INTEGER PRIMARY KEY, name TEXT, data BLOB)"
                )
                await a.begin(TransactionMode.IMMEDIATE)
                await a.execute("INSERT INTO python_sdk_test VALUES (1,'discard',NULL)")
                self.assertEqual(
                    (await b.query("SELECT count(*) FROM python_sdk_test")).rows, ((0,),)
                )
                await a.rollback()
                await a.begin()
                await a.execute(
                    "INSERT INTO python_sdk_test VALUES (:id,:name,:data)",
                    {"id": 2**63 - 1, "name": "中文\0", "data": b"\0\xff"},
                )
                await a.execute("SAVEPOINT nested")
                await a.execute("INSERT INTO python_sdk_test VALUES (2,'discard',NULL)")
                await a.execute("ROLLBACK TO nested")
                await a.execute("RELEASE nested")
                await a.commit()
                self.assertEqual(
                    (await b.query("SELECT * FROM python_sdk_test")).rows,
                    ((2**63 - 1, "中文\0", b"\0\xff"),),
                )
                with self.assertRaises(Error) as caught:
                    await a.execute("INSERT INTO python_sdk_test VALUES (?,NULL,NULL)", [2**63 - 1])
                self.assertEqual(caught.exception.code, "SQLITE_CONSTRAINT")
                self.assertTrue(a.is_usable)
                await a.begin()
                await a.execute("INSERT INTO python_sdk_test VALUES (3,'discard',NULL)")
                await a.aclose()
                self.assertEqual(
                    (await b.query("SELECT count(*) FROM python_sdk_test")).rows, ((1,),)
                )
            async with Session(peer.client) as reopened:
                self.assertEqual(
                    (await reopened.query("SELECT count(*) FROM python_sdk_test")).rows, ((1,),)
                )
