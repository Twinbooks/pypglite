# Python Server Example

This directory provides a server-based Python example for PGlite.

Important: this is **not** a native, non-WASM embedding of PostgreSQL. It starts `pglite-server` as a private local process and speaks the PostgreSQL wire protocol from Python.

If you need the true native embedded engine, use the main `pypglite` package at
the repository root and start with the top-level [README](../../README.md).

## What is included

- `pglite.py`:
  A small Python wrapper that starts `pglite-server` on a local port and runs simple queries without extra Python dependencies.
- `example.py`:
  A tiny end-to-end demo.

## Prerequisites

You need the `pglite-server` CLI available on your `PATH`.

One way to get it is:

```bash
npm install -g @electric-sql/pglite-socket
```

## Usage

```python
from pglite import PGliteServer

with PGliteServer(db="memory://") as db:
    db.query("create table demo (id int primary key, name text)")
    db.query("insert into demo values (1, 'hello')")
    result = db.query("select * from demo")
    print(result.rows)
```

Run the bundled example with:

```bash
cd examples/python
python3 example.py
```

If you want real `psycopg2` compatibility, use the root-level
`psycopg2-pglite` build path instead:

```bash
make -C ../../postgres-pglite/pglite/libpq-pglite wheel-upstream-psycopg2-pglite
python3 -m pip install ../../postgres-pglite/pglite/out/upstream-psycopg2/wheelhouse/psycopg2_pglite-*.whl
```

## Notes

- `QueryResult.rows` is returned as a list of dictionaries.
- Values are decoded as text. This wrapper intentionally stays small and does not implement PostgreSQL type decoding.
- `PGliteServer.query()` opens a short-lived connection for each query. Use `connect()` directly if you want to keep a session open.
- For the maintained `psycopg2` path, use upstream `psycopg2` linked against
  `libpq-pglite` from the repository root.
