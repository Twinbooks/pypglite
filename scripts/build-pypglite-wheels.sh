#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

PYTHON_BIN=${PYTHON_BIN:-python3}
WHEELHOUSE_DIR=${WHEELHOUSE_DIR:-wheelhouse}
TOOL_VENV_DIR=${TOOL_VENV_DIR:-"$REPO_ROOT/.ci-tools/cibuildwheel-venv"}

if [ -z "${CIBW_PLATFORM:-}" ]; then
  case "$(uname -s)" in
    Darwin) export CIBW_PLATFORM=macos ;;
    Linux) export CIBW_PLATFORM=linux ;;
    *)
      echo "unsupported local platform for cibuildwheel: $(uname -s)" >&2
      exit 1
      ;;
  esac
fi

if [ -z "${PGLITE_BUILD_JOBS:-}" ]; then
  case "$(uname -s)" in
    Darwin) export PGLITE_BUILD_JOBS=2 ;;
    *) export PGLITE_BUILD_JOBS=8 ;;
  esac
fi

if [ ! -x "$TOOL_VENV_DIR/bin/python" ]; then
  "$PYTHON_BIN" -m venv "$TOOL_VENV_DIR"
fi

"$TOOL_VENV_DIR/bin/python" -m pip install --upgrade pip cibuildwheel==2.23.3
"$TOOL_VENV_DIR/bin/python" -m cibuildwheel "$@" --output-dir "$WHEELHOUSE_DIR"
