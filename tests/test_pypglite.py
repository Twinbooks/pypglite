from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock
import unittest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pypglite
from pypglite import dbapi2
from pypglite import _native


class DummyPGlite:
    def __init__(self, data_dir: str | Path, **_: object) -> None:
        self.data_dir = Path(data_dir)
        self.closed = False
        self._transaction_status = "I"
        self._rows: list[tuple[str, str]] = []

    def close(self) -> None:
        self.closed = True

    def logical_transaction_status(self) -> str:
        return self._transaction_status

    def query(self, sql: str):
        lowered = sql.strip().lower()
        if lowered == "begin":
            self._transaction_status = "T"
            return _native.QueryResult(command_tag="BEGIN")
        if lowered in {"commit", "rollback"}:
            self._transaction_status = "I"
            return _native.QueryResult(command_tag=lowered.upper())
        if lowered.startswith("create table"):
            return _native.QueryResult(command_tag="CREATE TABLE")
        if lowered.startswith("insert into demo values"):
            self._rows.append(("1", "TRUE"))
            return _native.QueryResult(command_tag="INSERT 0 1")
        if lowered == "select id, flag from demo":
            return _native.QueryResult(
                command_tag="SELECT 1",
                columns=["id", "flag"],
                column_types=[23, 16],
                rows=list(self._rows),
            )
        raise AssertionError(f"unsupported test SQL: {sql}")


class PyPGlitePackageTests(unittest.TestCase):
    def test_public_api_keeps_native_runtime_and_dbapi_separate(self) -> None:
        self.assertIs(pypglite.PGlite, _native.PGlite)
        self.assertIs(pypglite.QueryResult, _native.QueryResult)
        self.assertIs(pypglite.connect, dbapi2.connect)
        self.assertFalse(hasattr(_native, "connect"))
        self.assertFalse(hasattr(_native, "Connection"))

    def test_dbapi2_module_reexports_dbapi_surface(self) -> None:
        self.assertIs(pypglite.Connection, dbapi2.Connection)
        self.assertIs(pypglite.Cursor, dbapi2.Cursor)
        self.assertIs(dbapi2.connect, pypglite.connect)
        self.assertEqual(dbapi2.apilevel, "2.0")
        self.assertEqual(dbapi2.paramstyle, "pyformat")
        self.assertEqual(dbapi2.threadsafety, 1)

    def test_version_is_defined(self) -> None:
        self.assertTrue(pypglite.__version__)

    def test_dbapi2_execute_and_adaptation_work_without_native_driver_logic(self) -> None:
        with mock.patch("pypglite.dbapi2.PGlite", DummyPGlite):
            with dbapi2.connect("memory://") as conn:
                with conn.cursor() as cur:
                    cur.execute("create table demo (id int, flag bool)")
                    cur.execute("insert into demo values (%s, %s)", (1, True))
                    cur.execute("select id, flag from demo")
                    self.assertEqual(cur.description[0][1], 23)
                    self.assertEqual(cur.fetchall(), [("1", "TRUE")])
                    self.assertEqual(cur.mogrify("select %s = any(%s)", ("a", [])), b"select 'a' = any('{}')")


if __name__ == "__main__":
    unittest.main()
