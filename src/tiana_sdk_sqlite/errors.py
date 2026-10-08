"""Bounded diagnostic errors; no SQL, parameters, batons or remote messages."""


class Error(Exception):
    """An error is never permission to replay. Known SQL errors may have effects."""

    def __init__(self, code: str, *, outcome_unknown: bool = False, request_id: str | None = None):
        self.code = code
        self.outcome_unknown = outcome_unknown
        self.request_id = request_id
        super().__init__(
            f"{code} (outcome_unknown={outcome_unknown})"
            + (f" [request_id={request_id}]" if request_id else "")
        )


KNOWN_SQL_ERRORS = frozenset(
    {
        "SQLITE_ERROR",
        "SQLITE_UNKNOWN",
        "SQLITE_BUSY",
        "SQLITE_LOCKED",
        "SQLITE_CONSTRAINT",
        "SQLITE_READONLY",
        "SQLITE_MISMATCH",
        "SQLITE_RANGE",
        "SQLITE_TOOBIG",
        "SQLITE_FULL",
        "SQLITE_ABORT",
        "SQLITE_INTERRUPT",
        "SQLITE_AUTH",
        "SQLITE_PERM",
        "ARGS_INVALID",
        "ARGS_BOTH_POSITIONAL_AND_NAMED",
        "SQL_NO_STATEMENT",
        "SQL_MANY_STATEMENTS",
    }
)
UNCERTAIN_SQL_ERRORS = frozenset({"RESULT_TOO_LARGE", "RESPONSE_TOO_LARGE", "SQLITE_IOERR"})


def sql_error(code: str) -> Error:
    if code in KNOWN_SQL_ERRORS:
        return Error(code)
    return Error(code if code in UNCERTAIN_SQL_ERRORS else "SQL_ERROR", outcome_unknown=True)


def invalid_response() -> Error:
    return Error("INVALID_RESPONSE", outcome_unknown=True)
