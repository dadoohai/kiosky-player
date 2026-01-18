#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${CONFIG_PATH:-$ROOT_DIR/config.json}"
EXAMPLE_PATH="$ROOT_DIR/config.example.json"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ ! -f "$CONFIG_PATH" ]]; then
  if [[ -f "$EXAMPLE_PATH" ]]; then
    cp "$EXAMPLE_PATH" "$CONFIG_PATH"
    echo "[run] config.json criado a partir de config.example.json"
  fi
  echo "[run] Edite $CONFIG_PATH e preencha api_key e environment_id"
  exit 1
fi

if [[ ! -x "$ROOT_DIR/scripts/install/deps.sh" ]]; then
  echo "[run] deps.sh nao encontrado" >&2
  exit 1
fi

PYTHON_BIN="$PYTHON_BIN" VENV_DIR="$VENV_DIR" bash "$ROOT_DIR/scripts/install/deps.sh"

PYTHON_EXEC="$VENV_DIR/bin/python"
if [[ ! -x "$PYTHON_EXEC" ]]; then
  echo "[run] Python do venv nao encontrado em $PYTHON_EXEC" >&2
  exit 1
fi

exec "$PYTHON_EXEC" "$ROOT_DIR/kiosk.py" --config "$CONFIG_PATH"
