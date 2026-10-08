# Runnable SQLite example

Requires Python 3.11+. From the repository root, install the adapter and its
pinned remote transport dependency (see the root README for GitHub access):

```sh
python -m pip install .
export TIANA_ENDPOINT='ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test'
# Replace this placeholder with your actual Endpoint hostname.
python examples/sqlite.py
```

Configure these environment variables before running:

| Variable | Meaning |
| --- | --- |
| `TIANA_ENDPOINT` | Required Endpoint hostname; determines TLS verification, SNI and CONNECT identity. |
| `TIANA_TOKEN` | Optional existing connection token; omit only when anonymous access is allowed. An explicitly empty token fails. |
| `TIANA_CA_FILE` | Optional PEM CA file, added to the system trust roots. Certificate verification stays enabled. |
| `TIANA_GATEWAY_ADDRESS` | Optional physical TCP destination, `host:port` or `[IPv6]:port`; does not change Endpoint identity. |

The example does not log in, refresh credentials or read a token file. It imports
installed `tiana_sdk` and `tiana_sdk_sqlite` packages without modifying `sys.path`
or requiring a sibling checkout. `--help` prints usage without connecting.

It creates a connection-local TEMP table, inserts bound int64/text/blob values,
commits and queries with named parameters, then verifies rollback of a second
insert. No persistent application table is created or modified. Repeated runs
start with a new TEMP table. Expected output:

```text
committed row: id=9223372036854775807, label=hello Tiana, blob_bytes=2
rollback verified: rows=1
session closed
```

Async context managers close the Session before the Client, including on errors.
Errors exit nonzero; no operation is retried or replayed. If the connection fails,
explicit close may be impossible and server cleanup may wait for its session TTL.
Connection failure or cancellation does not prove rollback. The example reports
bounded SDK error codes rather than credentials or remote error messages.
