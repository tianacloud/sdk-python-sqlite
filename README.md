# Tiana Python SQLite SDK

`tiana-sdk-sqlite` provides async SQLite sessions through Tiana Gateway. It depends
on `tiana-sdk` for TLS/H2 CONNECT channels; this package owns the `hrana-http`
profile, Hrana HTTP v3, SQL parameters/results, baton lifecycle and transactions.
Requires Python 3.11 or later. No local engine or helper process is involved.

## Install and use

The transport dependency is fetched from GitHub at immutable commit
`a2e08b958b3f8b43d72369035c756abc5715d1e7` (v1.0.0 release commit).
The core SDK is fetched over HTTPS. If GitHub requires authentication, configure
HTTPS credentials, then install this adapter from its checkout:

```sh
python -m pip install .
```

Only this adapter's source is local; its declared `tiana-sdk` dependency resolves
from `git+https://github.com/tianacloud/sdk-python.git@a2e08b958b3f8b43d72369035c756abc5715d1e7`.
No companion checkout, editable core install, local wheel or PYTHONPATH override
is required. Built wheels retain the same remote dependency. Future upgrades
require changing the pinned commit explicitly; no PyPI core release is required.

```python
import asyncio
import os
from tiana_sdk import Client
from tiana_sdk_sqlite import Session, TransactionMode


async def main():
    async with Client(
        os.environ["TIANA_ENDPOINT"],
        token=os.environ.get("TIANA_TOKEN"),
        ca_file=os.environ.get("TIANA_CA_FILE"),
    ) as client:
        async with Session(client) as session:
            await session.execute(
                "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)"
            )
            await session.begin(TransactionMode.IMMEDIATE)
            await session.execute("INSERT INTO items (name) VALUES (?)", ["example"])
            await session.commit()
            result = await session.query(
                "SELECT id, name FROM items WHERE name = :name", {"name": "example"}
            )
            print(result.rows)


asyncio.run(main())
```

Session is lazy: construction does not connect. The supplied Client remains
caller-owned, and separate Sessions use independent tunnels. A session permits
one outstanding operation; overlapping calls immediately raise `SESSION_BUSY`
without cancelling the owner. This API is native asyncio, not Python DB-API 2.0,
a pool, ORM integration or synchronous adapter.

## Runnable example

See [examples/README.md](examples/README.md) and run `python examples/sqlite.py`
after installing this package. Set `TIANA_ENDPOINT` to your actual hostname;
optional settings are `TIANA_TOKEN`, `TIANA_CA_FILE` (PEM), and
`TIANA_GATEWAY_ADDRESS`. `python examples/sqlite.py --help` shows usage.

The example demonstrates positional/named parameters, int64/text/blob values,
commit, rollback and bounded session cleanup using a connection-local TEMP table.
It can be rerun without creating or changing persistent application tables.
Examples are included in the source distribution.

## SQL API

- `await execute(sql, parameters=())`: metadata without rows.
- `await query(sql, parameters=())`: metadata and buffered rows.
- `await execute_and_close(sql, parameters=())`: execute/get_autocommit/close in
  one pipeline. Closing never implicitly commits a transaction.
- `await begin(mode=TransactionMode.DEFERRED)`, `commit()`, `rollback()`:
  confirmed-state transaction operations. IMMEDIATE and EXCLUSIVE are supported.
- Raw transaction SQL and savepoints are supported. Every statement pipeline also
  fetches `get_autocommit`; `session.autocommit` is a bool when known, otherwise None.
- `await aclose()`: bounded explicit server close and transport cleanup. Closing a
  fresh session does no I/O. Async context exit closes without committing.
  `abort()` releases local I/O and cancels a running operation, without a server
  close request. It makes the session unusable.

Parameters are a sequence for positional binds or a mapping for named binds.
Values are None, bool (integer 0/1), signed 64-bit int, finite float, UTF-8 str
(including NUL), or bytes. Out-of-range ints, nonfinite floats, lone Unicode
surrogates and invalid parameter collections are rejected before network I/O.
Blob encoding is strict unpadded base64; int64 values travel as decimal strings.

Result contains `columns`, `rows`, `affected_row_count` and optional
`last_insert_rowid`. Columns expose optional `name` and `decltype`; rows are
immutable tuples. Result repr omits row data. Query results are fully buffered;
use bounded result sets for large tables. There are no streaming cursors,
server-side prepared statements or WebSocket support in this initial API.

## Failure and resource semantics

There is no automatic retry, reconnect, baton recovery or SQL replay. Timeout,
cancellation, malformed responses, unknown SQL errors and transport loss invalidate
the session. Task cancellation propagates asyncio.CancelledError. Discarded
sessions cannot send cleanup using a stale baton. Create a fresh Session only after
resolving prior uncertain outcomes. A known SQL error may leave partial effects or
an active transaction; a failed COMMIT may still require explicit ROLLBACK.

`Error` exposes a bounded allowlisted `code`, `outcome_unknown` and the tunnel
`request_id` when established. Errors omit SQL, parameters, tokens, batons and remote
messages. Unknown outcome is not permission to retry. BATON_INVALID rejects the
current request before execution but does not restore an earlier session.

Request/response wire bodies are limited to 8 MiB, response headers to 32 KiB,
and batons to 4096 UTF-8 bytes. HTTP parsing uses h11, without redirects, automatic
retries, compression or inner credential forwarding. JSON rejects duplicate keys,
nonfinite values, invalid UTF-8, ambiguous outcomes and malformed result shapes.
The default operation deadline is 30 seconds; `Session(client, request_timeout=5)`
changes it. Explicit close uses at most min(request_timeout, 3) seconds for network
work; local transport cleanup can additionally take the transport's bounded close
interval. Decoded Python objects consume more memory than wire bytes.

Use async context managers or explicitly close both Session and Client. Python
object collection is not async cleanup; merely dropping the Session does not
release a Client-owned tunnel. Abort/transport loss cannot prove rollback: the
server can retain a transaction and locks until stream TTL cleanup. Storage
atomicity, durability and crash recovery remain server-owned. The SDK does not
acquire or refresh tokens; configure existing credentials in Client.

## Validation

```sh
python -m unittest discover -v
python -m build
ruff check .
ruff format --check .
```

Local TLS/H2 peers cover values, state, cancellation, concurrent-call rejection,
limits, malformed replies, redaction and no replay. The separate real-App test is
skipped unless explicitly configured with a disposable loopback App SQLite behind
a TCP-to-Unix bridge:

```sh
TIANA_SQLITE_TEST_ADDRESS=127.0.0.1:18080 python -m unittest discover -k real_app -v
```

It creates `python_sdk_test` in that disposable database and checks real reads,
writes, transaction isolation, rollback, savepoints, commit, constraint errors,
close rollback and committed rows from a fresh session. Do not point it at an
existing or production database. Synthetic test certificates originate from
sdk-python b6307ce3; no production credentials are included.
