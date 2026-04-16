from __future__ import annotations

from dataclasses import dataclass, field
import shlex
import socket
import struct
import subprocess
import time
from typing import Optional, Sequence


DEFAULT_USER = "postgres"
DEFAULT_PASSWORD = "postgres"
DEFAULT_DATABASE = "postgres"


class PGliteError(RuntimeError):
    pass


@dataclass
class QueryResult:
    command_tag: str
    columns: list[str] = field(default_factory=list)
    rows: list[dict[str, Optional[str]]] = field(default_factory=list)


def _normalize_command(command: str | Sequence[str]) -> list[str]:
    if isinstance(command, str):
        return shlex.split(command)
    return list(command)


def _encode_cstring(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def _read_cstring(payload: bytes, offset: int) -> tuple[str, int]:
    end = payload.index(b"\x00", offset)
    return payload[offset:end].decode("utf-8"), end + 1


def _pick_free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


class PGWireClient:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        user: str = DEFAULT_USER,
        password: str = DEFAULT_PASSWORD,
        database: str = DEFAULT_DATABASE,
        connect_timeout: float = 5.0,
    ) -> None:
        self._sock = socket.create_connection((host, port), timeout=connect_timeout)
        self._sock.settimeout(connect_timeout)
        self._password = password
        self._startup(user=user, database=database)

    def close(self) -> None:
        try:
            self._sock.sendall(b"X" + struct.pack("!I", 4))
        except OSError:
            pass
        self._sock.close()

    def __enter__(self) -> PGWireClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def query(self, sql: str) -> QueryResult | list[QueryResult]:
        payload = sql.encode("utf-8") + b"\x00"
        self._sock.sendall(b"Q" + struct.pack("!I", len(payload) + 4) + payload)

        results: list[QueryResult] = []
        columns: list[str] = []
        rows: list[dict[str, Optional[str]]] = []

        while True:
            message_type, payload = self._recv_message()

            if message_type == b"T":
                columns = self._parse_row_description(payload)
                rows = []
                continue

            if message_type == b"D":
                rows.append(self._parse_data_row(payload, columns))
                continue

            if message_type == b"C":
                command_tag = payload[:-1].decode("utf-8")
                results.append(
                    QueryResult(command_tag=command_tag, columns=columns, rows=rows)
                )
                columns = []
                rows = []
                continue

            if message_type == b"I":
                results.append(QueryResult(command_tag="EMPTY"))
                continue

            if message_type == b"E":
                raise self._parse_error(payload)

            if message_type == b"Z":
                if len(results) == 1:
                    return results[0]
                return results

            if message_type in {
                b"1",
                b"2",
                b"3",
                b"A",
                b"K",
                b"N",
                b"S",
                b"n",
                b"s",
                b"t",
            }:
                continue

            raise PGliteError(
                f"unsupported PostgreSQL message type {message_type!r} while executing query"
            )

    def _startup(self, *, user: str, database: str) -> None:
        body = b"".join(
            [
                struct.pack("!I", 196608),
                _encode_cstring("user"),
                _encode_cstring(user),
                _encode_cstring("database"),
                _encode_cstring(database),
                _encode_cstring("client_encoding"),
                _encode_cstring("UTF8"),
                b"\x00",
            ]
        )
        self._sock.sendall(struct.pack("!I", len(body) + 4) + body)

        while True:
            message_type, payload = self._recv_message()

            if message_type == b"R":
                auth_code = struct.unpack("!I", payload[:4])[0]
                if auth_code == 0:
                    continue
                if auth_code == 3:
                    self._send_password()
                    continue
                raise PGliteError(f"unsupported authentication method {auth_code}")

            if message_type in {b"K", b"N", b"S"}:
                continue

            if message_type == b"E":
                raise self._parse_error(payload)

            if message_type == b"Z":
                return

            raise PGliteError(
                f"unsupported PostgreSQL message type {message_type!r} during startup"
            )

    def _send_password(self) -> None:
        payload = self._password.encode("utf-8") + b"\x00"
        self._sock.sendall(b"p" + struct.pack("!I", len(payload) + 4) + payload)

    def _recv_exact(self, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = self._sock.recv(size - len(chunks))
            if not chunk:
                raise PGliteError("connection closed while reading PostgreSQL message")
            chunks.extend(chunk)
        return bytes(chunks)

    def _recv_message(self) -> tuple[bytes, bytes]:
        message_type = self._recv_exact(1)
        length = struct.unpack("!I", self._recv_exact(4))[0]
        return message_type, self._recv_exact(length - 4)

    def _parse_row_description(self, payload: bytes) -> list[str]:
        field_count = struct.unpack("!H", payload[:2])[0]
        offset = 2
        columns: list[str] = []

        for _ in range(field_count):
            column_name, offset = _read_cstring(payload, offset)
            columns.append(column_name)
            offset += 18

        return columns

    def _parse_data_row(
        self, payload: bytes, columns: Sequence[str]
    ) -> dict[str, Optional[str]]:
        field_count = struct.unpack("!H", payload[:2])[0]
        offset = 2
        row: dict[str, Optional[str]] = {}

        for index in range(field_count):
            field_length = struct.unpack("!i", payload[offset : offset + 4])[0]
            offset += 4
            key = columns[index] if index < len(columns) else f"column_{index}"
            if field_length == -1:
                row[key] = None
                continue
            value = payload[offset : offset + field_length]
            row[key] = value.decode("utf-8")
            offset += field_length

        return row

    def _parse_error(self, payload: bytes) -> PGliteError:
        fields: dict[str, str] = {}
        offset = 0

        while offset < len(payload) and payload[offset] != 0:
            field_code = chr(payload[offset])
            field_value, offset = _read_cstring(payload, offset + 1)
            fields[field_code] = field_value

        message = fields.get("M", "unknown PostgreSQL error")
        sqlstate = fields.get("C")
        if sqlstate:
            return PGliteError(f"{sqlstate}: {message}")
        return PGliteError(message)


class PGliteServer:
    def __init__(
        self,
        *,
        command: str | Sequence[str] = ("pglite-server",),
        db: str = "memory://",
        host: str = "127.0.0.1",
        port: Optional[int] = None,
        user: str = DEFAULT_USER,
        password: str = DEFAULT_PASSWORD,
        database: str = DEFAULT_DATABASE,
        debug: int = 0,
        extensions: Optional[Sequence[str]] = None,
        startup_timeout: float = 10.0,
        max_connections: int = 1,
    ) -> None:
        self.command = _normalize_command(command)
        self.db = db
        self.host = host
        self.port = port or _pick_free_port(host)
        self.user = user
        self.password = password
        self.database = database
        self.debug = debug
        self.extensions = list(extensions or [])
        self.startup_timeout = startup_timeout
        self.max_connections = max_connections
        self._process: Optional[subprocess.Popen[str]] = None

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )

    def start(self) -> PGliteServer:
        if self._process is not None:
            return self

        args = [
            *self.command,
            f"--db={self.db}",
            f"--host={self.host}",
            f"--port={self.port}",
            f"--debug={self.debug}",
            f"--max-connections={self.max_connections}",
        ]
        if self.extensions:
            args.append(f"--extensions={','.join(self.extensions)}")

        self._process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        deadline = time.monotonic() + self.startup_timeout
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                raise PGliteError(self._build_startup_error())
            try:
                with socket.create_connection((self.host, self.port), timeout=0.2):
                    return self
            except OSError:
                time.sleep(0.05)

        message = self._build_startup_error("timed out waiting for pglite-server")
        self.close()
        raise PGliteError(message)

    def connect(self) -> PGWireClient:
        if self._process is None:
            self.start()
        return PGWireClient(
            self.host,
            self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    def query(self, sql: str) -> QueryResult | list[QueryResult]:
        with self.connect() as client:
            return client.query(sql)

    def close(self) -> None:
        if self._process is None:
            return

        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)

        if self._process.stdout is not None:
            self._process.stdout.close()

        self._process = None

    def __enter__(self) -> PGliteServer:
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _build_startup_error(self, prefix: str = "failed to start pglite-server") -> str:
        if self._process is None:
            return prefix

        output = ""
        exit_code = self._process.poll()
        if self._process.stdout is not None:
            if exit_code is not None:
                try:
                    output = self._process.stdout.read()
                except OSError:
                    output = ""

        if exit_code is not None:
            prefix = f"{prefix}; process exited with code {exit_code}"

        output = output.strip()
        if output:
            return f"{prefix}\n{output}"
        return prefix
