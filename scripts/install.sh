#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-.venv}"
INSTALL_SBERT="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sbert)
      INSTALL_SBERT="true"
      shift
      ;;
    --python)
      PYTHON_BIN="$2"
      shift 2
      ;;
    --venv)
      VENV_DIR="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: scripts/install.sh [--sbert] [--python <python_bin>] [--venv <venv_dir>]"
      exit 1
      ;;
  esac
done

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python interpreter not found: $PYTHON_BIN"
  exit 1
fi

"$PYTHON_BIN" - <<'PY'
import sys
if sys.version_info < (3, 10):
    raise SystemExit("Python 3.10+ is required")
print(f"Using Python {sys.version.split()[0]}")
PY

"$PYTHON_BIN" -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install --upgrade pip setuptools wheel

if [[ "$INSTALL_SBERT" == "true" ]]; then
  "$VENV_DIR/bin/pip" install -e ".[sbert]"
else
  "$VENV_DIR/bin/pip" install -e .
fi

echo ""
echo "Install complete."
echo "Interpreter: $ROOT_DIR/$VENV_DIR/bin/python"
echo "Try:"
echo "  $ROOT_DIR/$VENV_DIR/bin/python -m customer_dedupe run-test --size 300 --output-dir data/cli_output"
