from __future__ import annotations

from pathlib import Path
import tempfile

from pypglite import PGlite


def main() -> int:
    data_dir = Path(tempfile.mkdtemp(prefix="pypglite-cibw-")) / "pgdata"
    with PGlite(data_dir) as db:
        result = db.query("select 1 as value")

    assert result.rows == [("1",)]
    assert result.named_rows[0]["value"] == "1"
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
