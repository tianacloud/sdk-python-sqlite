"""Hrana v3 codecs with strict values, shape validation and bounded JSON."""

import base64
import binascii
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TypeAlias

from .errors import Error, invalid_response, sql_error

MAX_BODY_BYTES = 8 * 1024 * 1024
SqlValue: TypeAlias = None | bool | int | float | str | bytes
Parameters: TypeAlias = Sequence[SqlValue] | Mapping[str, SqlValue]


@dataclass(frozen=True)
class Column:
    name: str | None
    decltype: str | None


@dataclass(frozen=True, repr=False)
class Result:
    columns: tuple[Column, ...]
    rows: tuple[tuple[SqlValue, ...], ...]
    affected_row_count: int
    last_insert_rowid: int | None


def _utf8(value: str) -> int:
    if len(value) > MAX_BODY_BYTES:
        raise Error("REQUEST_TOO_LARGE")
    try:
        return len(value.encode("utf-8"))
    except UnicodeError:
        raise Error("INVALID_ARGUMENT") from None


def statement(sql: str, parameters: Parameters, want_rows: bool) -> dict:
    if not isinstance(sql, str):
        raise Error("INVALID_ARGUMENT")
    size = _utf8(sql)
    if size > MAX_BODY_BYTES:
        raise Error("REQUEST_TOO_LARGE")
    named = isinstance(parameters, Mapping)
    if not named and (
        not isinstance(parameters, Sequence)
        or isinstance(parameters, (str, bytes, bytearray, memoryview))
    ):
        raise Error("INVALID_ARGUMENT")
    values = []
    names = set()
    # Bound raw inputs and base64 expansion before encoding/copying each value.
    for name, value in parameters.items() if named else enumerate(parameters):
        if named:
            if not isinstance(name, str) or not name or "\0" in name or name in names:
                raise Error("INVALID_ARGUMENT")
            names.add(name)
            size += _utf8(name)
        if type(value) is str:
            length = _utf8(value)
        elif type(value) is bytes:
            length = (len(value) * 4 + 2) // 3
        else:
            length = 32
        size += length + 64
        if size > MAX_BODY_BYTES:
            raise Error("REQUEST_TOO_LARGE")
        wire = encode_value(value)
        values.append({"name": name, "value": wire} if named else wire)
    return {"sql": sql, "named_args" if named else "args": values, "want_rows": want_rows}


def encode_value(value: SqlValue) -> dict:
    if value is None:
        return {"type": "null"}
    if type(value) is bool:
        value = int(value)
    if type(value) is int and -(2**63) <= value < 2**63:
        return {"type": "integer", "value": str(value)}
    if type(value) is float and math.isfinite(value):
        return {"type": "float", "value": value}
    if type(value) is str:
        return {"type": "text", "value": value}
    if type(value) is bytes:
        return {"type": "blob", "base64": base64.b64encode(value).rstrip(b"=").decode("ascii")}
    raise Error("INVALID_ARGUMENT")


def dumps(value: dict) -> bytes:
    encoded = bytearray()
    try:
        for chunk in json.JSONEncoder(
            ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).iterencode(value):
            chunk = chunk.encode("utf-8")
            if len(encoded) + len(chunk) > MAX_BODY_BYTES:
                raise Error("REQUEST_TOO_LARGE")
            encoded.extend(chunk)
    except (ValueError, UnicodeError):
        raise Error("INVALID_ARGUMENT") from None
    return bytes(encoded)


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate field")
        value[key] = item
    return value


def _constant(_):
    raise ValueError("nonfinite number")


def _float(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("nonfinite number")
    return number


def loads(data: bytes | bytearray):
    try:
        # Explicit UTF-8: json.loads(bytes) otherwise also accepts UTF-16/32.
        return json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_object,
            parse_constant=_constant,
            parse_float=_float,
        )
    except (ValueError, UnicodeError, RecursionError):
        raise invalid_response() from None


def _integer(value) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"-?[0-9]{1,20}", value):
        raise invalid_response()
    number = int(value)
    if not -(2**63) <= number < 2**63:
        raise invalid_response()
    return number


def decode_value(value) -> SqlValue:
    if not isinstance(value, dict):
        raise invalid_response()
    kind = value.get("type")
    field = "base64" if kind == "blob" else "value"
    if set(value) != ({"type"} if kind == "null" else {"type", field}):
        raise invalid_response()
    if kind == "null":
        return None
    v = value.get(field)
    if kind == "integer":
        return _integer(v)
    if kind == "float" and type(v) in (int, float):
        try:
            v = float(v)
        except OverflowError:
            raise invalid_response() from None
        if math.isfinite(v):
            return v
    if kind == "text" and isinstance(v, str):
        try:
            v.encode("utf-8")
        except UnicodeError:
            raise invalid_response() from None
        return v
    if kind == "blob" and isinstance(v, str):
        try:
            if "=" in v:
                raise ValueError("noncanonical base64")
            decoded = base64.b64decode(v + "=" * (-len(v) % 4), validate=True)
            if base64.b64encode(decoded).rstrip(b"=").decode("ascii") != v:
                raise ValueError("noncanonical base64")
            return decoded
        except (ValueError, binascii.Error):
            raise invalid_response() from None
    raise invalid_response()


def _nullable_text(value):
    if value is not None:
        if not isinstance(value, str):
            raise invalid_response()
        try:
            value.encode("utf-8")
        except UnicodeError:
            raise invalid_response() from None
    return value


def decode_result(result) -> Result:
    if not isinstance(result, dict):
        raise invalid_response()
    cols, rows, affected = (result.get(k) for k in ("cols", "rows", "affected_row_count"))
    if (
        not isinstance(cols, list)
        or not isinstance(rows, list)
        or type(affected) is not int
        or not 0 <= affected < 2**64
    ):
        raise invalid_response()
    columns = []
    for col in cols:
        if not isinstance(col, dict):
            raise invalid_response()
        columns.append(Column(_nullable_text(col.get("name")), _nullable_text(col.get("decltype"))))
    decoded = []
    for row in rows:
        if not isinstance(row, list) or len(row) != len(columns):
            raise invalid_response()
        decoded.append(tuple(decode_value(v) for v in row))
    rowid = result.get("last_insert_rowid")
    return Result(
        tuple(columns), tuple(decoded), affected, _integer(rowid) if rowid is not None else None
    )


def decode_pipeline(data: bytearray, requests: list[dict], closing: bool):
    envelope = loads(data)
    if not isinstance(envelope, dict) or envelope.get("base_url") is not None:
        raise invalid_response()
    baton, results = envelope.get("baton"), envelope.get("results")
    if (
        not isinstance(results, list)
        or len(results) != len(requests)
        or (closing and baton is not None)
        or (not closing and (not isinstance(baton, str) or not baton))
    ):
        raise invalid_response()
    if baton is not None:
        try:
            if len(baton.encode("utf-8")) > 4096:
                raise invalid_response()
        except UnicodeError:
            raise invalid_response() from None
    result, error, autocommit = None, None, None
    for request, reply in zip(requests, results):
        if not isinstance(reply, dict):
            raise invalid_response()
        if reply.get("type") == "error":
            if set(reply) != {"type", "error"} or request["type"] != "execute":
                raise invalid_response()
            if not isinstance(reply["error"], dict) or not isinstance(
                reply["error"].get("code"), str
            ):
                raise invalid_response()
            error = sql_error(reply["error"]["code"])
        elif reply.get("type") == "ok" and set(reply) == {"type", "response"}:
            response = reply["response"]
            if not isinstance(response, dict) or response.get("type") != request["type"]:
                raise invalid_response()
            if request["type"] == "execute":
                if set(response) != {"type", "result"}:
                    raise invalid_response()
                result = decode_result(response["result"])
            elif request["type"] == "get_autocommit":
                if (
                    set(response) != {"type", "is_autocommit"}
                    or type(response["is_autocommit"]) is not bool
                ):
                    raise invalid_response()
                autocommit = response["is_autocommit"]
            elif set(response) != {"type"}:
                raise invalid_response()
        else:
            raise invalid_response()
    return baton, result, error, autocommit
