#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PLIST_PATH="$HOME/Library/LaunchAgents/com.kiosky.player.plist"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
CONFIG_PATH="${CONFIG_PATH:-$ROOT_DIR/config.json}"
LOG_DIR="$ROOT_DIR/logs"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Python nao encontrado em: $PYTHON_BIN" >&2
  exit 1
fi
if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config nao encontrada em: $CONFIG_PATH" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"
cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.kiosky.player</string>
    <key>ProgramArguments</key>
    <array>
      <string>$PYTHON_BIN</string>
      <string>$ROOT_DIR/kiosk.py</string>
      <string>--config</string>
      <string>$CONFIG_PATH</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$ROOT_DIR</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$LOG_DIR/kiosky.out.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/kiosky.err.log</string>
  </dict>
</plist>
PLIST

launchctl unload "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl load "$PLIST_PATH"

echo "LaunchAgent instalado: $PLIST_PATH"
