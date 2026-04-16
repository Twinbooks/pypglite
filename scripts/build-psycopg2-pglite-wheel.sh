#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

PYTHON_BIN=${PYTHON_BIN:-python3}
MAKE_BIN=${MAKE:-$(command -v gmake || command -v make)}

if [ -z "${PGLITE_BUILD_JOBS:-}" ]; then
  case "$(uname -s)" in
    Darwin) export PGLITE_BUILD_JOBS=2 ;;
    *) export PGLITE_BUILD_JOBS=8 ;;
  esac
fi

export PYTHON_BIN
export MAKE="$MAKE_BIN"

bash postgres-pglite/build-libpglite.sh
"$MAKE_BIN" -C postgres-pglite/pglite/libpq-pglite wheel-upstream-psycopg2-pglite
