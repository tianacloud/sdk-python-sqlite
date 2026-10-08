"""Run a repeatable TEMP-table transaction demo; see README.md for configuration."""

import argparse
import asyncio
import ipaddress
import os
import re
import sys

from tiana_sdk import Client, ConnectError

from tiana_sdk_sqlite import Error, Session, TransactionMode


def gateway_address(value: str | None) -> tuple[str, int] | None:
    """Parse the optional physical destination, preserving Endpoint TLS identity."""
    if value is None:
        return None
    match = re.fullmatch(r"(?:\[([^\[\]]+)\]|([A-Za-z0-9.-]+)):([1-9][0-9]{0,4})", value)
    if match is None or int(match[3]) > 65535:
        raise ValueError("TIANA_GATEWAY_ADDRESS must be host:port or [IPv6]:port")
    host = match[1] or match[2]
    if match[1]:
        try:
            ipaddress.IPv6Address(host)
        except ValueError:
            raise ValueError("TIANA_GATEWAY_ADDRESS contains invalid IPv6") from None
    return host, int(match[3])


async def main() -> None:
    endpoint = os.environ.get("TIANA_ENDPOINT")
    if not endpoint:
        raise ValueError("TIANA_ENDPOINT must be set to your Endpoint hostname")
    async with Client(
        endpoint,
        token=os.environ.get("TIANA_TOKEN"),
        ca_file=os.environ.get("TIANA_CA_FILE"),
        gateway_address=gateway_address(os.environ.get("TIANA_GATEWAY_ADDRESS")),
    ) as client:
        async with Session(client) as session:
            # TEMP isolates this demo from persistent tables and subsequent runs.
            await session.execute(
                "CREATE TEMP TABLE tiana_sdk_demo (id INTEGER PRIMARY KEY, label TEXT, data BLOB)"
            )
            await session.begin(TransactionMode.IMMEDIATE)
            await session.execute(
                "INSERT INTO temp.tiana_sdk_demo VALUES (?, ?, ?)",
                [2**63 - 1, "hello Tiana", b"\x00\xff"],
            )
            await session.commit()
            result = await session.query(
                "SELECT id, label, data FROM temp.tiana_sdk_demo WHERE label = :label",
                {"label": "hello Tiana"},
            )
            if result.rows != ((2**63 - 1, "hello Tiana", b"\x00\xff"),):
                raise ValueError("demo typed-value verification failed")
            row = result.rows[0]
            print(f"committed row: id={row[0]}, label={row[1]}, blob_bytes={len(row[2])}")

            await session.begin()
            await session.execute(
                "INSERT INTO temp.tiana_sdk_demo VALUES (?, ?, ?)", [1, "discard", None]
            )
            await session.rollback()
            if (await session.query("SELECT count(*) FROM temp.tiana_sdk_demo")).rows != ((1,),):
                raise ValueError("demo rollback verification failed")
            print("rollback verified: rows=1")
        print("session closed")


if __name__ == "__main__":
    argparse.ArgumentParser(
        description="Demonstrate SQLite parameters, transactions and explicit session cleanup.",
        epilog=(
            "Required: TIANA_ENDPOINT. Optional: TIANA_TOKEN, TIANA_CA_FILE (PEM), "
            "TIANA_GATEWAY_ADDRESS (host:port or [IPv6]:port)."
        ),
    ).parse_args()
    try:
        asyncio.run(main())
    except (ConnectError, Error) as error:
        print(
            f"{error.code}" + (": outcome unknown; do not replay" if error.outcome_unknown else ""),
            file=sys.stderr,
        )
        raise SystemExit(1) from None
    except ValueError as error:
        print(error, file=sys.stderr)
        raise SystemExit(1) from None
    except KeyboardInterrupt:
        raise SystemExit(130) from None
