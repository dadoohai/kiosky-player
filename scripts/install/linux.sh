#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVICE_DIR="$HOME/.config/systemd/user"
SERVICE_PATH="$SERVICE_DIR/kiosky.service"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
CONFIG_PATH="${CONFIG_PATH:-$ROOT_DIR/config.json}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Python nao encontrado em: $PYTHON_BIN" >&2
  exit 1
fi
if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config nao encontrada em: $CONFIG_PATH" >&2
  exit 1
fi

mkdir -p "$SERVICE_DIR"
cat > "$SERVICE_PATH" <<SERVICE
[Unit]
Description=Kiosky MPV Player
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$ROOT_DIR
ExecStart=$PYTHON_BIN $ROOT_DIR/kiosk.py --config $CONFIG_PATH
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
SERVICE

systemctl --user daemon-reload
systemctl --user enable --now kiosky.service

echo "Servico instalado: $SERVICE_PATH"
