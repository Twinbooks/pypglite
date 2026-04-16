from __future__ import annotations

from dataclasses import dataclass, field
import ctypes
import functools
import hashlib
import importlib.metadata
import importlib.util
import itertools
import os
from pathlib import Path
import shutil
import struct
import sys
import threading
from typing import Any, Mapping, Optional


class PGliteError(RuntimeError):
    pass


class PGliteBackendError(PGliteError):
    def __init__(self, fields: Mapping[str, str]) -> None:
        self.fields = dict(fields)
        self.sqlstate = self.fields.get("C")
        self.message_primary = self.fields.get("M", "unknown PostgreSQL error")
        self.message_detail = self.fields.get("D")
        self.table_name = self.fields.get("t")
        message = self.message_primary
        if self.sqlstate:
            message = f"{self.sqlstate}: {message}"
        super().__init__(message)


@dataclass(frozen=True)
class NamedRow:
    pairs: tuple[tuple[str, Optional[str]], ...]

    def __len__(self) -> int:
        return len(self.pairs)

    def __iter__(self):
        return iter(self.pairs)

    def __getitem__(self, key: str) -> Optional[str] | tuple[Optional[str], ...]:
        values = self.getall(key)
        if not values:
            raise KeyError(key)
        return values[0] if len(values) == 1 else values

    def __eq__(self, other: object) -> bool:
        if isinstance(other, dict):
            return self.as_dict() == other
        if isinstance(other, (list, tuple)):
            return tuple(self.pairs) == tuple(other)
        return NotImplemented

    def items(self) -> tuple[tuple[str, Optional[str]], ...]:
        return self.pairs

    def keys(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.pairs)

    def values(self) -> tuple[Optional[str], ...]:
        return tuple(value for _, value in self.pairs)

    def get(self, key: str, default: Any = None) -> Any:
        values = self.getall(key)
        if not values:
            return default
        return values[0] if len(values) == 1 else values

    def getall(self, key: str) -> tuple[Optional[str], ...]:
        return tuple(value for name, value in self.pairs if name == key)

    def as_dict(self) -> dict[str, Optional[str] | tuple[Optional[str], ...]]:
        grouped: dict[str, list[Optional[str]]] = {}
        for name, value in self.pairs:
            grouped.setdefault(name, []).append(value)

        return {
            name: values[0] if len(values) == 1 else tuple(values)
            for name, values in grouped.items()
        }


@dataclass
class QueryResult:
    command_tag: str
    columns: list[str] = field(default_factory=list)
    column_types: list[int | None] = field(default_factory=list)
    rows: list[tuple[Optional[str], ...]] = field(default_factory=list)

    @property
    def named_rows(self) -> list[NamedRow]:
        return [
            NamedRow(
                tuple(
                    (
                        self.columns[index] if index < len(self.columns) else f"column_{index}",
                        value,
                    )
                    for index, value in enumerate(row)
                )
            )
            for row in self.rows
        ]


def _read_cstring(payload: bytes, offset: int) -> tuple[str, int]:
    end = payload.index(b"\x00", offset)
    return payload[offset:end].decode("utf-8"), end + 1


def _parse_error_fields(payload: bytes) -> dict[str, str]:
    fields: dict[str, str] = {}
    offset = 0

    while offset < len(payload) and payload[offset] != 0:
        field_code = chr(payload[offset])
        field_value, offset = _read_cstring(payload, offset + 1)
        fields[field_code] = field_value

    return fields


def _parse_error(payload: bytes) -> PGliteBackendError:
    return PGliteBackendError(_parse_error_fields(payload))


def parse_simple_query_response(data: bytes) -> QueryResult | list[QueryResult]:
    results: list[QueryResult] = []
    columns: list[str] = []
    column_types: list[int | None] = []
    rows: list[tuple[Optional[str], ...]] = []
    offset = 0

    while offset < len(data):
        message_type = data[offset : offset + 1]
        length = struct.unpack("!I", data[offset + 1 : offset + 5])[0]
        payload = data[offset + 5 : offset + 1 + length]
        offset += 1 + length

        if message_type == b"T":
            field_count = struct.unpack("!H", payload[:2])[0]
            payload_offset = 2
            columns = []
            column_types = []
            for _ in range(field_count):
                column_name, payload_offset = _read_cstring(payload, payload_offset)
                columns.append(column_name)
                _table_oid = struct.unpack("!I", payload[payload_offset : payload_offset + 4])[0]
                payload_offset += 4
                _column_attr = struct.unpack("!H", payload[payload_offset : payload_offset + 2])[0]
                payload_offset += 2
                type_oid = struct.unpack("!I", payload[payload_offset : payload_offset + 4])[0]
                payload_offset += 4
                _type_size = struct.unpack("!h", payload[payload_offset : payload_offset + 2])[0]
                payload_offset += 2
                _type_modifier = struct.unpack("!i", payload[payload_offset : payload_offset + 4])[0]
                payload_offset += 4
                _format_code = struct.unpack("!H", payload[payload_offset : payload_offset + 2])[0]
                payload_offset += 2
                column_types.append(type_oid)
            rows = []
            continue

        if message_type == b"D":
            field_count = struct.unpack("!H", payload[:2])[0]
            payload_offset = 2
            row: list[Optional[str]] = []
            for _ in range(field_count):
                field_length = struct.unpack("!i", payload[payload_offset : payload_offset + 4])[0]
                payload_offset += 4
                if field_length == -1:
                    row.append(None)
                    continue
                value = payload[payload_offset : payload_offset + field_length]
                row.append(value.decode("utf-8"))
                payload_offset += field_length
            rows.append(tuple(row))
            continue

        if message_type == b"C":
            command_tag = payload[:-1].decode("utf-8")
            results.append(
                QueryResult(
                    command_tag=command_tag,
                    columns=columns,
                    column_types=column_types,
                    rows=rows,
                )
            )
            columns = []
            column_types = []
            rows = []
            continue

        if message_type == b"I":
            results.append(QueryResult(command_tag="EMPTY"))
            continue

        if message_type == b"E":
            raise _parse_error(payload)

        if message_type == b"Z":
            if len(results) == 1:
                return results[0]
            return results

        if message_type in {b"1", b"2", b"3", b"K", b"N", b"S", b"n", b"s", b"t"}:
            continue

        raise PGliteError(f"unsupported PostgreSQL message type {message_type!r}")

    if len(results) == 1:
        return results[0]
    return results


def _parse_ready_for_query_status(data: bytes) -> str | None:
    offset = 0
    status: str | None = None

    while offset + 5 <= len(data):
        length = struct.unpack("!I", data[offset + 1 : offset + 5])[0]
        if length < 4 or offset + 1 + length > len(data):
            break

        message_type = data[offset : offset + 1]
        payload = data[offset + 5 : offset + 1 + length]
        if message_type == b"Z" and payload:
            status = payload[:1].decode("ascii", "ignore") or None

        offset += 1 + length

    return status


_ENGINE_REGISTRY_LOCK = threading.RLock()
_ENGINE_REGISTRY: dict[str, "_SharedEngine"] = {}
_CLIENT_ID_SEQUENCE = itertools.count(1)


def _normalize_bootstrap_mode(value: str | None) -> str:
    normalized = (value or "auto").lower()
    if normalized in {"auto", "embedded"}:
        return "embedded"
    return normalized


class _SharedEngine:
    def __init__(
        self,
        data_dir: str,
        *,
        lib_path: str | os.PathLike[str] | None = None,
        bootstrap_mode: str | None = None,
    ) -> None:
        self.data_dir = data_dir
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._wait_queue: list[object] = []
        self._refcount = 0
        self._closing = False
        self._closed = False
        self._transaction_owner: int | None = None
        self._transaction_status = "I"

        self._api = _LibPGlite(lib_path)
        self.lib_path = os.fspath(self._api.path)
        self.bootstrap_mode = _normalize_bootstrap_mode(bootstrap_mode)

        handle = ctypes.c_void_p()
        if bootstrap_mode is not None and not self._api._supports_open_with_options:
            raise PGliteError("this libpglite build does not support bootstrap_mode")
        if self._api._supports_open_with_options:
            encoded_bootstrap_mode = None
            if bootstrap_mode is not None:
                encoded_bootstrap_mode = bootstrap_mode.encode("utf-8")
            rc = self._api.lib.pglite_open_with_options(
                os.fsencode(self.data_dir),
                encoded_bootstrap_mode,
                ctypes.byref(handle),
            )
        else:
            rc = self._api.lib.pglite_open(
                os.fsencode(self.data_dir),
                ctypes.byref(handle),
            )
        if rc != 0:
            message = self._api.lib.pglite_global_error()
            raise PGliteError(message.decode("utf-8") if message else "pglite_open failed")
        self._handle = handle

    def is_compatible(
        self,
        *,
        lib_path: str | os.PathLike[str] | None = None,
        bootstrap_mode: str | None = None,
    ) -> bool:
        normalized_bootstrap_mode = _normalize_bootstrap_mode(bootstrap_mode)
        if normalized_bootstrap_mode != self.bootstrap_mode:
            return False
        if lib_path is None:
            return True
        return os.fspath(Path(lib_path).expanduser().resolve()) == self.lib_path

    def attach(self) -> None:
        with self._condition:
            if self._closing or self._closed:
                raise PGliteError("shared embedded PGlite engine is closing")
            self._refcount += 1

    def close_client(self, client_id: int) -> bool:
        with self._condition:
            self._check_open_locked()
            if self._transaction_owner == client_id and self._transaction_status in {"T", "E"}:
                try:
                    rollback_data = self._exec_sql_locked("ROLLBACK")
                except PGliteError:
                    # Best effort: a closed logical client must not leave the
                    # shared engine permanently pinned to a dead owner.
                    self._transaction_status = "I"
                else:
                    self._update_transaction_state_locked(client_id, rollback_data)
                finally:
                    if self._transaction_owner == client_id:
                        self._transaction_owner = None
                        self._transaction_status = "I"

            self._refcount -= 1
            if self._refcount == 0:
                self._closing = True
                return True

            self._condition.notify_all()
            return False

    def close_underlying(self) -> None:
        with self._condition:
            if self._closed:
                return
            rc = self._api.lib.pglite_close(self._handle)
            if rc != 0:
                raise PGliteError(self._error_message())
            self._closed = True
            self._condition.notify_all()

    def exec_raw(self, client_id: int, sql: str) -> bytes:
        with self._condition:
            self._check_open_locked()
            self._acquire_turn_locked(client_id)
            try:
                data = self._exec_sql_locked(sql)
            except Exception:
                self._condition.notify_all()
                raise
            self._update_transaction_state_locked(client_id, data)
            self._condition.notify_all()
            return data

    def exec_protocol(self, client_id: int, message: bytes) -> bytes:
        with self._condition:
            self._check_open_locked()
            self._acquire_turn_locked(client_id)
            try:
                data = self._exec_protocol_locked(message)
            except Exception:
                self._condition.notify_all()
                raise
            self._update_transaction_state_locked(client_id, data)
            self._condition.notify_all()
            return data

    def error_message(self) -> str:
        with self._lock:
            return self._error_message()

    def logical_transaction_status(self, client_id: int) -> str:
        with self._condition:
            self._check_open_locked()
            if self._transaction_owner == client_id:
                return self._transaction_status
            return "I"

    def _acquire_turn_locked(self, client_id: int) -> None:
        if self._transaction_owner == client_id:
            return

        ticket = object()
        self._wait_queue.append(ticket)
        try:
            while True:
                owner_available = self._transaction_owner is None
                at_front = bool(self._wait_queue) and self._wait_queue[0] is ticket
                if owner_available and at_front:
                    self._wait_queue.pop(0)
                    return
                self._condition.wait()
        except BaseException:
            if ticket in self._wait_queue:
                self._wait_queue.remove(ticket)
                self._condition.notify_all()
            raise

    def _update_transaction_state_locked(self, client_id: int, data: bytes) -> None:
        status = _parse_ready_for_query_status(data)
        if status is None:
            return
        self._transaction_status = status
        if status in {"T", "E"}:
            self._transaction_owner = client_id
        else:
            self._transaction_owner = None

    def _exec_sql_locked(self, sql: str) -> bytes:
        out_ptr = ctypes.c_void_p()
        out_len = ctypes.c_size_t()
        rc = self._api.lib.pglite_exec(
            self._handle,
            sql.encode("utf-8"),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if rc != 0:
            raise PGliteError(self._error_message())
        return self._take_bytes(out_ptr, out_len.value)

    def _exec_protocol_locked(self, message: bytes) -> bytes:
        buffer = ctypes.create_string_buffer(message)
        out_ptr = ctypes.c_void_p()
        out_len = ctypes.c_size_t()
        rc = self._api.lib.pglite_exec_protocol(
            self._handle,
            ctypes.cast(buffer, ctypes.c_void_p),
            len(message),
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        if rc != 0:
            raise PGliteError(self._error_message())
        return self._take_bytes(out_ptr, out_len.value)

    def _take_bytes(self, out_ptr: ctypes.c_void_p, length: int) -> bytes:
        if not out_ptr.value or length == 0:
            return b""
        try:
            return ctypes.string_at(out_ptr.value, length)
        finally:
            self._api.lib.pglite_free(out_ptr)

    def _error_message(self) -> str:
        raw = self._api.lib.pglite_error(self._handle)
        return raw.decode("utf-8") if raw else "unknown native pglite error"

    def _check_open_locked(self) -> None:
        if self._closed or self._closing:
            raise PGliteError("pglite handle is closed")


def _shared_engine_key(data_dir: str | os.PathLike[str]) -> str:
    return os.fspath(Path(data_dir).expanduser().resolve())


def _acquire_shared_engine(
    data_dir: str | os.PathLike[str],
    *,
    lib_path: str | os.PathLike[str] | None = None,
    bootstrap_mode: str | None = None,
) -> tuple[str, _SharedEngine, int]:
    key = _shared_engine_key(data_dir)
    with _ENGINE_REGISTRY_LOCK:
        engine = _ENGINE_REGISTRY.get(key)
        if engine is not None and not engine.is_compatible(
            lib_path=lib_path,
            bootstrap_mode=bootstrap_mode,
        ):
            raise PGliteError(
                "an embedded PGlite engine is already open for this data directory "
                "with different lib_path/bootstrap_mode options"
            )

        if engine is None:
            engine = _SharedEngine(
                key,
                lib_path=lib_path,
                bootstrap_mode=bootstrap_mode,
            )
            _ENGINE_REGISTRY[key] = engine

        engine.attach()
        return key, engine, next(_CLIENT_ID_SEQUENCE)


def _release_shared_engine(key: str, engine: _SharedEngine, client_id: int) -> None:
    with _ENGINE_REGISTRY_LOCK:
        should_close = engine.close_client(client_id)
        if not should_close:
            return
        if _ENGINE_REGISTRY.get(key) is engine:
            del _ENGINE_REGISTRY[key]
        engine.close_underlying()


class _LibPGlite:
    def __init__(self, lib_path: str | os.PathLike[str] | None = None) -> None:
        resolved = Path(lib_path) if lib_path is not None else self._find_library()
        self.path = resolved
        mode = ctypes.DEFAULT_MODE | getattr(ctypes, "RTLD_GLOBAL", 0)
        self.lib = ctypes.CDLL(str(resolved), mode=mode)

        self._supports_open_with_options = hasattr(self.lib, "pglite_open_with_options")
        self.lib.pglite_open.argtypes = [
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        self.lib.pglite_open.restype = ctypes.c_int
        if self._supports_open_with_options:
            self.lib.pglite_open_with_options.argtypes = [
                ctypes.c_char_p,
                ctypes.c_char_p,
                ctypes.POINTER(ctypes.c_void_p),
            ]
            self.lib.pglite_open_with_options.restype = ctypes.c_int

        self.lib.pglite_exec.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        self.lib.pglite_exec.restype = ctypes.c_int

        self.lib.pglite_exec_protocol.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        self.lib.pglite_exec_protocol.restype = ctypes.c_int

        self.lib.pglite_close.argtypes = [ctypes.c_void_p]
        self.lib.pglite_close.restype = ctypes.c_int

        self.lib.pglite_error.argtypes = [ctypes.c_void_p]
        self.lib.pglite_error.restype = ctypes.c_char_p

        self.lib.pglite_global_error.argtypes = []
        self.lib.pglite_global_error.restype = ctypes.c_char_p

        self.lib.pglite_free.argtypes = [ctypes.c_void_p]
        self.lib.pglite_free.restype = None

    @staticmethod
    def _find_library() -> Path:
        env_path = os.environ.get("PGLITE_LIB_PATH")
        if env_path:
            candidate = Path(env_path).expanduser().resolve()
            if candidate.exists():
                return candidate

        for candidate in _LibPGlite._repo_library_candidates():
            if candidate.exists():
                return candidate

        bundle_root = _LibPGlite._find_bundle_root()
        if bundle_root is not None:
            for candidate in _LibPGlite._bundle_library_candidates(bundle_root):
                if candidate.exists():
                    return candidate

        raise PGliteError(
            "could not find libpglite; install a bundled wheel, set PGLITE_LIB_PATH, or build it with postgres-pglite/build-libpglite.sh"
        )

    @staticmethod
    def _bundle_library_candidates(bundle_root: Path) -> list[Path]:
        return [
            bundle_root / "lib" / "libpglite.0.dylib",
            bundle_root / "lib" / "libpglite.so.0.1",
        ]

    @staticmethod
    def _repo_library_candidates() -> list[Path]:
        repo_root = Path(__file__).resolve().parents[1]
        candidates = _LibPGlite._bundle_library_candidates(repo_root / "postgres-pglite" / "pglite" / "out" / "native")
        candidates.extend(
            [
                repo_root / "postgres-pglite" / "pglite" / "native" / "libpglite.0.dylib",
                repo_root / "postgres-pglite" / "pglite" / "native" / "libpglite.so.0.1",
            ]
        )
        return candidates

    @staticmethod
    def _find_bundle_root() -> Path | None:
        env_bundle = os.environ.get("PGLITE_BUNDLE_PATH")
        if env_bundle:
            candidate = Path(env_bundle).expanduser().resolve()
            if candidate.exists():
                return candidate

        spec = importlib.util.find_spec("pglite_native_runtime")
        if spec is None or not spec.submodule_search_locations:
            return None

        package_dir = Path(next(iter(spec.submodule_search_locations))).resolve()
        bundle_root = package_dir / "bundle"
        if bundle_root.exists():
            return _LibPGlite._materialize_packaged_bundle(package_dir, bundle_root)
        return None

    @staticmethod
    def _materialize_packaged_bundle(package_dir: Path, bundle_root: Path) -> Path:
        cache_root = _LibPGlite._bundle_cache_root(package_dir, bundle_root)
        ready_marker = cache_root / ".pglite-materialized"
        if ready_marker.exists() and all(
            candidate.exists() for candidate in _LibPGlite._bundle_library_candidates(cache_root)
        ):
            return cache_root

        manifest_path = package_dir / "bundle-empty-dirs.txt"
        temp_root = cache_root.with_name(f"{cache_root.name}.tmp-{os.getpid()}")
        if temp_root.exists():
            shutil.rmtree(temp_root)
        shutil.copytree(bundle_root, temp_root)
        if manifest_path.exists():
            for relative_dir in manifest_path.read_text(encoding="utf-8").splitlines():
                if relative_dir:
                    (temp_root / relative_dir).mkdir(parents=True, exist_ok=True)
        ready_marker.parent.mkdir(parents=True, exist_ok=True)
        (temp_root / ".pglite-materialized").write_text("ok\n", encoding="utf-8")

        try:
            if cache_root.exists():
                shutil.rmtree(cache_root)
            temp_root.replace(cache_root)
        except OSError:
            if temp_root.exists():
                shutil.rmtree(temp_root, ignore_errors=True)
            if ready_marker.exists() and all(
                candidate.exists() for candidate in _LibPGlite._bundle_library_candidates(cache_root)
            ):
                return cache_root
            raise
        return cache_root

    @staticmethod
    def _bundle_cache_root(package_dir: Path, bundle_root: Path) -> Path:
        env_cache = os.environ.get("PGLITE_BUNDLE_CACHE")
        if env_cache:
            cache_base = Path(env_cache).expanduser().resolve()
        elif os.name == "posix" and sys.platform == "darwin":
            cache_base = Path.home() / "Library" / "Caches"
        else:
            cache_base = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")).expanduser().resolve()

        try:
            version = importlib.metadata.version("pypglite")
        except importlib.metadata.PackageNotFoundError:
            version = "0.0.1"

        digest = _LibPGlite._bundle_cache_digest(package_dir, bundle_root)
        cache_root = cache_base / "pypglite" / version / digest / "bundle"
        cache_root.parent.mkdir(parents=True, exist_ok=True)
        return cache_root

    @staticmethod
    @functools.lru_cache(maxsize=8)
    def _bundle_cache_digest(package_dir: Path, bundle_root: Path) -> str:
        manifest_path = package_dir / "bundle-empty-dirs.txt"
        digest = hashlib.sha256()
        digest.update(str(package_dir).encode("utf-8"))
        digest.update(b"\0")

        if manifest_path.exists():
            digest.update(manifest_path.read_bytes())
            stat = manifest_path.stat()
            digest.update(str(stat.st_size).encode("ascii"))
            digest.update(b":")
            digest.update(str(stat.st_mtime_ns).encode("ascii"))
            digest.update(b"\0")

        for path in sorted(bundle_root.rglob("*")):
            relative = path.relative_to(bundle_root).as_posix().encode("utf-8")
            digest.update(relative)
            digest.update(b"\0")
            if path.is_symlink():
                digest.update(b"l\0")
                digest.update(os.readlink(path).encode("utf-8"))
                digest.update(b"\0")
                continue

            stat = path.stat()
            if path.is_dir():
                digest.update(b"d\0")
            else:
                digest.update(b"f\0")
                digest.update(str(stat.st_size).encode("ascii"))
                digest.update(b":")
            digest.update(str(stat.st_mtime_ns).encode("ascii"))
            digest.update(b"\0")

        return digest.hexdigest()[:12]


class PGlite:
    def __init__(
        self,
        data_dir: str | os.PathLike[str],
        *,
        lib_path: str | os.PathLike[str] | None = None,
        bootstrap_mode: str | None = None,
        initdb_path: str | os.PathLike[str] | None = None,
        initdb_if_missing: bool = False,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.closed = False

        # Native bootstrap now lives in libpglite itself. The legacy initdb
        # kwargs remain accepted for compatibility with earlier experiments.
        _ = initdb_path, initdb_if_missing

        self._registry_key, self._engine, self._client_id = _acquire_shared_engine(
            self.data_dir,
            lib_path=lib_path,
            bootstrap_mode=bootstrap_mode,
        )

    def close(self) -> None:
        if self.closed:
            return
        _release_shared_engine(self._registry_key, self._engine, self._client_id)
        self.closed = True

    def __enter__(self) -> PGlite:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def exec_raw(self, sql: str) -> bytes:
        self._check_open()
        return self._engine.exec_raw(self._client_id, sql)

    def exec_protocol(self, message: bytes) -> bytes:
        self._check_open()
        if message[:1] == b"X":
            self.close()
            return b""
        return self._engine.exec_protocol(self._client_id, message)

    def query(self, sql: str) -> QueryResult | list[QueryResult]:
        return parse_simple_query_response(self.exec_raw(sql))

    def logical_transaction_status(self) -> str:
        self._check_open()
        return self._engine.logical_transaction_status(self._client_id)

    def _error_message(self) -> str:
        return self._engine.error_message()

    def _check_open(self) -> None:
        if self.closed:
            raise PGliteError("pglite handle is closed")
