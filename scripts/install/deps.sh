#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"
REQUIREMENTS_FILE="$ROOT_DIR/requirements.txt"

log() { echo "[deps] $*"; }

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  log "Python 3 nao encontrado. Instale o Python 3.9+ e rode novamente."
  exit 1
fi

log "Criando venv em $VENV_DIR"
"$PYTHON_BIN" -m venv "$VENV_DIR"
# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"

if [[ -f "$REQUIREMENTS_FILE" ]]; then
  log "Instalando dependencias Python"
  pip install -r "$REQUIREMENTS_FILE"
else
  log "requirements.txt nao encontrado"
fi

if command -v mpv >/dev/null 2>&1; then
  log "MPV ja instalado"
  exit 0
fi

log "MPV nao encontrado. Tentando instalar..."
OS_NAME="$(uname -s)"
if [[ "$OS_NAME" == "Darwin" ]]; then
  if command -v brew >/dev/null 2>&1; then
    brew install mpv
  else
    log "Homebrew nao encontrado. Instale com: https://brew.sh"
    exit 1
  fi
elif [[ "$OS_NAME" == "Linux" ]]; then
  if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update
    sudo apt-get install -y mpv
  elif command -v apt >/dev/null 2>&1; then
    sudo apt update
    sudo apt install -y mpv
  elif command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y mpv
  elif command -v yum >/dev/null 2>&1; then
    sudo yum install -y mpv
  elif command -v pacman >/dev/null 2>&1; then
    sudo pacman -S --noconfirm mpv
  else
    log "Gerenciador de pacotes nao identificado. Instale o MPV manualmente."
    exit 1
  fi
else
  log "Sistema nao suportado neste script. Instale o MPV manualmente."
  exit 1
fi

log "Dependencias instaladas"
