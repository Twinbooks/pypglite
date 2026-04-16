from __future__ import annotations

import importlib.metadata

from . import dbapi2
from ._native import NamedRow
from ._native import PGlite
from ._native import PGliteBackendError
from ._native import PGliteError
from ._native import QueryResult
from .dbapi2 import Connection
from .dbapi2 import Cursor
from .dbapi2 import DatabaseError
from .dbapi2 import Error
from .dbapi2 import InterfaceError
from .dbapi2 import OperationalError
from .dbapi2 import ProgrammingError
from .dbapi2 import apilevel
from .dbapi2 import connect
from .dbapi2 import paramstyle
from .dbapi2 import threadsafety

_SOURCE_VERSION = "0.0.1"


def _package_version() -> str:
    try:
        return importlib.metadata.version("pypglite")
    except importlib.metadata.PackageNotFoundError:
        return _SOURCE_VERSION


__version__ = _package_version()

__all__ = [
    "Connection",
    "Cursor",
    "DatabaseError",
    "Error",
    "InterfaceError",
    "NamedRow",
    "OperationalError",
    "PGlite",
    "PGliteBackendError",
    "PGliteError",
    "ProgrammingError",
    "QueryResult",
    "__version__",
    "apilevel",
    "dbapi2",
    "connect",
    "paramstyle",
    "threadsafety",
]
