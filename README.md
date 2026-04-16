# PyPGlite

`pypglite` is an embedded PostgreSQL runtime package for Python.

It packages a native `libpglite` runtime and exposes a direct Python API as
`pypglite`.

The intended API split is:

- `pypglite.PGlite` for the direct embedded runtime
- `pypglite.dbapi2` and top-level `pypglite.connect()` for a small DB-API layer
- upstream `psycopg2` built against `libpq-pglite` for compatibility-driven integrations

## Repository Split

- `pypglite`: this repository. Python packaging, docs, tests, examples, and
  the Python-facing APIs.
- [`postgres-pglite`](https://github.com/Twinbooks/postgres-pglite): the
  engine fork. Patched PostgreSQL sources, `libpglite`, `libpq-pglite`, and
  the native runtime build.

The engine lives in the `postgres-pglite` submodule, and this repository tracks
the Python-side integration work that builds on top of it.

## Install

Clone with submodules and install from the repo root:

```bash
git clone --recurse-submodules https://github.com/Twinbooks/pypglite.git
cd pypglite
python3 -m pip install .
```

To build the native engine bundle first:

```bash
cd postgres-pglite
bash build-libpglite.sh
cd ..
```

To build a wheel instead of installing:

```bash
python3 -m pip wheel . -w dist --no-deps
```

To build platform wheels with `cibuildwheel`:

```bash
WHEELHOUSE_DIR=wheelhouse bash scripts/build-pypglite-wheels.sh --platform macos
```

The repository also includes a GitHub Actions workflow in
`.github/workflows/wheels.yml` that builds Linux and macOS `pypglite` wheels.

To build the compatibility wheel from this repo:

```bash
bash scripts/build-psycopg2-pglite-wheel.sh
python3 -m pip install postgres-pglite/pglite/out/upstream-psycopg2/wheelhouse/psycopg2_pglite-*.whl
```

The repository also includes `.github/workflows/psycopg2-pglite-wheels.yml`
to build `psycopg2-pglite` wheels in GitHub Actions across supported Python
versions.

## Naming

The names are intentionally split by role:

- Repository: `pypglite`
- Engine repository: `postgres-pglite`
- Canonical Python distribution: `pypglite`
- Compatibility distribution built from this repo: `psycopg2-pglite`
- Python import provided by this package: `pypglite`

The primary package is `pypglite`. For `psycopg2` compatibility, this repo
keeps an explicit `psycopg2-pglite` build path based on upstream `psycopg2`
linked against `libpq-pglite`, not a bundled Python shim.

## Status

Current release line: `0.0.1`

This is an early release, but the package layout is now the real one for this
repository: the code, tests, packaging metadata, and examples live at the repo
root rather than under `experimental/`.

What works today:

- in-process native query execution through `libpglite`
- first-run bootstrap handled inside the native runtime via embedded `initdb`
- multiple logical connections to the same embedded database in one process
- upstream `psycopg2` smoke and reconnect/concurrency stress through
  `libpq-pglite`

Known limits:

- one active embedded data directory per process today
- `PQsocket()` currently returns `-1`, so selector-based integrations such as
  bus/websocket polling are not supported yet
- the upstream `libpq-pglite` route is still incomplete in areas such as async
  fd integration, COPY, and large objects

## Use It

Direct API smoke:

```bash
python3 - <<'PY'
from pathlib import Path
from pypglite import PGlite

with PGlite(Path("/tmp/pypglite-demo")) as db:
    print(db.query("select 1 as value").named_rows)
PY
```

Upstream `psycopg2 + libpq-pglite` smoke:

```bash
make -C postgres-pglite/pglite/libpq-pglite upstream-psycopg2-smoke
```

Bundled examples:

```bash
python3 examples/pypglite.py
```

## Validation Commands

Native engine and driver checks:

```bash
make -C postgres-pglite/pglite/libpq-pglite smoke
make -C postgres-pglite/pglite/libpq-pglite wheel-upstream-psycopg2-pglite
make -C postgres-pglite/pglite/libpq-pglite upstream-psycopg2-smoke
make -C postgres-pglite/pglite/libpq-pglite stress-upstream-psycopg2
```

Python compatibility checks:

```bash
python3 runtests.py
PGLITE_RUN_NATIVE_TESTS=1 python3 runtests.py
```

## Additional Example

- [Server-Based Python Example](./examples/python/README.md)

## Upstream

This fork is based on the upstream PGlite project from ElectricSQL:

- Upstream PGlite: https://github.com/electric-sql/pglite
- Upstream engine fork lineage: https://github.com/electric-sql/postgres-pglite

If you are looking for the original WASM/browser/TypeScript project, use the
upstream repository. This fork is organized around the Python/native embedding
path instead.
