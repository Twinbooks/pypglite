from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any, Iterable, Mapping, Optional

from ._native import PGlite
from ._native import PGliteBackendError
from ._native import PGliteError
from ._native import QueryResult


apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"


class Error(PGliteError):
    pass


class DatabaseError(Error):
    pass


class InterfaceError(Error):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


def connect(
    dsn: str | bytes | object,
    *,
    lib_path: str | None = None,
    bootstrap_mode: str | None = None,
    initdb_path: str | None = None,
    initdb_if_missing: bool = False,
) -> "Connection":
    return Connection(
        dsn,
        lib_path=lib_path,
        bootstrap_mode=bootstrap_mode,
        initdb_path=initdb_path,
        initdb_if_missing=initdb_if_missing,
    )


class Connection:
    def __init__(
        self,
        dsn: str | bytes | object,
        *,
        lib_path: str | None = None,
        bootstrap_mode: str | None = None,
        initdb_path: str | None = None,
        initdb_if_missing: bool = False,
    ) -> None:
        try:
            self._db = PGlite(
                dsn,
                lib_path=lib_path,
                bootstrap_mode=bootstrap_mode,
                initdb_path=initdb_path,
                initdb_if_missing=initdb_if_missing,
            )
        except PGliteError as exc:
            raise OperationalError(str(exc)) from exc

        self.autocommit = False
        self.closed = False
        self._in_transaction = False

    def cursor(self) -> "Cursor":
        self._check_open()
        return Cursor(self)

    def commit(self) -> None:
        self._check_open()
        self._sync_transaction_state_from_backend()
        if self._in_transaction:
            try:
                self._db.query("COMMIT")
            except PGliteBackendError as exc:
                raise DatabaseError(str(exc)) from exc
            finally:
                if not self._sync_transaction_state_from_backend():
                    self._in_transaction = False

    def rollback(self) -> None:
        self._check_open()
        self._sync_transaction_state_from_backend()
        if self._in_transaction:
            try:
                self._db.query("ROLLBACK")
            except PGliteBackendError as exc:
                raise DatabaseError(str(exc)) from exc
            finally:
                if not self._sync_transaction_state_from_backend():
                    self._in_transaction = False

    def close(self) -> None:
        if self.closed:
            return
        self._db.close()
        self.closed = True

    def __enter__(self) -> "Connection":
        self._check_open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None and not self.autocommit:
            self.commit()
        elif exc_type is not None and not self.autocommit:
            self.rollback()
        self.close()

    def _execute(self, query: str, params: Any = None) -> QueryResult:
        self._check_open()
        sql = _format_query(query, params)
        command = _leading_keyword(sql)
        self._sync_transaction_state_from_backend()
        if not self.autocommit and not self._in_transaction and command not in {
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "START",
        }:
            try:
                self._db.query("BEGIN")
            except PGliteBackendError as exc:
                raise DatabaseError(str(exc)) from exc
            if not self._sync_transaction_state_from_backend():
                self._in_transaction = True

        try:
            result = self._db.query(sql)
        except PGliteBackendError as exc:
            raise DatabaseError(str(exc)) from exc

        if not self._sync_transaction_state_from_backend():
            if command in {"COMMIT", "ROLLBACK"}:
                self._in_transaction = False
            elif command in {"BEGIN", "START"}:
                self._in_transaction = True
        if isinstance(result, list):
            return result[-1] if result else QueryResult(command_tag="EMPTY")
        return result

    def _sync_transaction_state_from_backend(self) -> bool:
        getter = getattr(self._db, "logical_transaction_status", None)
        if not callable(getter):
            return False
        status = getter()
        self._in_transaction = status in {"T", "E"}
        return True

    def _check_open(self) -> None:
        if self.closed:
            raise InterfaceError("connection already closed")


class Cursor:
    arraysize = 1

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.closed = False
        self.description: Optional[list[tuple[Any, ...]]] = None
        self.rowcount = -1
        self._results: list[tuple[Any, ...]] = []
        self._result_index = 0

    def close(self) -> None:
        self.closed = True
        self._results = []
        self.description = None
        self.rowcount = -1
        self._result_index = 0

    def execute(self, query: str, params: Any = None) -> "Cursor":
        self._check_open()
        result = self.connection._execute(query, params)
        self._load_result(result)
        return self

    def executemany(self, query: str, param_seq: Iterable[Any]) -> "Cursor":
        self._check_open()
        total_rowcount = 0
        for params in param_seq:
            result = self.connection._execute(query, params)
            self._load_result(result)
            if self.rowcount > 0:
                total_rowcount += self.rowcount
        self.rowcount = total_rowcount
        return self

    def fetchone(self) -> Optional[tuple[Any, ...]]:
        self._check_open()
        if self._result_index >= len(self._results):
            return None
        row = self._results[self._result_index]
        self._result_index += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        self._check_open()
        if size is None:
            size = self.arraysize
        rows = self._results[self._result_index : self._result_index + size]
        self._result_index += len(rows)
        return rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        self._check_open()
        rows = self._results[self._result_index :]
        self._result_index = len(self._results)
        return rows

    def mogrify(self, query: str, params: Any = None) -> bytes:
        self._check_open()
        return _format_query(query, params).encode("utf-8")

    def __enter__(self) -> "Cursor":
        self._check_open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _load_result(self, result: QueryResult) -> None:
        columns = result.columns
        column_types = list(result.column_types or [])
        if len(column_types) < len(columns):
            column_types.extend([None] * (len(columns) - len(column_types)))
        self.description = (
            [
                (column, column_types[index], None, None, None, None, None)
                for index, column in enumerate(columns)
            ]
            if columns
            else None
        )
        self._results = [
            tuple(row[index] if index < len(row) else None for index, _ in enumerate(columns))
            for row in result.rows
        ]
        self._result_index = 0
        self.rowcount = len(self._results) if columns else _parse_rowcount(result.command_tag)

    def _check_open(self) -> None:
        if self.closed:
            raise InterfaceError("cursor already closed")


def _leading_keyword(sql: str) -> str:
    return sql.lstrip().split(None, 1)[0].rstrip(";").upper()


def _parse_rowcount(command_tag: str) -> int:
    for part in reversed(command_tag.split()):
        if part.isdigit():
            return int(part)
    return -1


def _format_query(query: str, params: Any = None) -> str:
    if params is None:
        return query

    try:
        if isinstance(params, Mapping):
            return query % {key: _adapt_value(value) for key, value in params.items()}

        if not isinstance(params, (list, tuple)):
            params = (params,)
        return query % tuple(_adapt_value(value) for value in params)
    except (KeyError, TypeError, ValueError) as exc:
        raise ProgrammingError(f"failed to interpolate query parameters: {exc}") from exc


def _adapt_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "'\\x" + bytes(value).hex() + "'::bytea"
    if isinstance(value, dt.datetime):
        return "'" + value.isoformat(sep=" ", timespec="microseconds") + "'"
    if isinstance(value, dt.date):
        return "'" + value.isoformat() + "'"
    if isinstance(value, dt.time):
        return "'" + value.isoformat(timespec="microseconds") + "'"
    if isinstance(value, list):
        if not value:
            return "'{}'"
        return "ARRAY[" + ", ".join(_adapt_value(item) for item in value) + "]"
    if isinstance(value, tuple):
        return "(" + ", ".join(_adapt_value(item) for item in value) + ")"
    return "'" + str(value).replace("'", "''") + "'"


__all__ = [
    "Connection",
    "Cursor",
    "DatabaseError",
    "Error",
    "InterfaceError",
    "OperationalError",
    "ProgrammingError",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]
