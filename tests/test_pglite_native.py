from __future__ import annotations

from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pypglite._native import NamedRow
from pypglite._native import QueryResult


class QueryResultTests(unittest.TestCase):
    def test_named_rows_preserve_unique_columns(self) -> None:
        result = QueryResult(
            command_tag="SELECT 1",
            columns=["id", "name"],
            rows=[("1", "alpha")],
        )

        self.assertEqual(result.named_rows, [{"id": "1", "name": "alpha"}])
        self.assertIsInstance(result.named_rows[0], NamedRow)
        self.assertEqual(result.named_rows[0]["id"], "1")

    def test_named_rows_preserve_duplicate_columns(self) -> None:
        result = QueryResult(
            command_tag="SELECT 1",
            columns=["coalesce", "coalesce"],
            rows=[("0.0", "f")],
        )

        row = result.named_rows[0]
        self.assertEqual(row["coalesce"], ("0.0", "f"))
        self.assertEqual(row.getall("coalesce"), ("0.0", "f"))
        self.assertEqual(
            row.items(),
            (("coalesce", "0.0"), ("coalesce", "f")),
        )
        self.assertEqual(row.as_dict(), {"coalesce": ("0.0", "f")})


if __name__ == "__main__":
    unittest.main()
