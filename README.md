# Kiosky MPV Player

Este projeto cria um player kiosk que busca midias em uma API, faz polling para atualizacoes e exibe imagens/videos em tela cheia usando o MPV. O foco e estabilidade 24/7.

## Requisitos

- Python 3.9+
- MPV instalado no sistema
  - macOS: `brew install mpv`
  - Windows: baixe do site oficial ou use `choco install mpv`
  - Linux/Orange Pi: `sudo apt install mpv`

## Configuracao

1. Copie o arquivo de exemplo e edite:
   - `cp config.example.json config.json`
2. Preencha `api_key` e `environment_id`.
3. (Opcional) ajuste `poll_interval_sec` (padrao 1800s), `mute`, `mpv_path`, `log_file`, `watchdog_interval_sec` e `preload_next`.
4. `ipc_path` vazio usa um padrao automatico (socket no Linux/macOS, named pipe no Windows).
5. Para status/monitoramento, defina `status_file` (JSON) e `status_interval_sec`.

## Rodar localmente

```
python3 -m venv .venv
. .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python kiosk.py --config config.json
```

O player abre em tela cheia. Para encerrar, finalize o processo.

## Inicializacao simples (faz tudo)

Linux / macOS:
```
bash scripts/run.sh
```

Windows (PowerShell):
```
powershell -ExecutionPolicy Bypass -File scripts/run.ps1
```

## Configuracao via Web UI (Ctrl+S)

- Com o player aberto, pressione `Ctrl+S` para abrir o navegador em `http://127.0.0.1:8765`.
- Altere o `environment_id` e a `rotacao` e clique em **Salvar**.
- O player aplica a rotacao na hora e atualiza a playlist.

Opcional no `config.json`:
- `rotation_deg`: 0, 90, 180, 270
- `hotkeys_enabled`: true/false
- `config_ui_enabled`: true/false
- `config_ui_bind` e `config_ui_port`
- `low_resource_mode`: true/false (modo economia)
- `telemetry_enabled`: true/false
- `telemetry_interval_sec`: 300 (5 min)
- `telemetry_url` e `station_id`

## Instalacao rapida de dependencias

Linux / macOS:
```
bash scripts/install/deps.sh
```

Windows (PowerShell):
```
powershell -ExecutionPolicy Bypass -File scripts/install/deps.ps1
```

## Como funciona

- Faz POST na API e coleta `media_urls` + `exposure_time_ms`.
- Baixa midias para `media_cache/` e reutiliza cache se falhar.
- Mantem um unico processo do MPV via IPC (menos flicker).
- Pre-carrega o proximo item via playlist do MPV quando `preload_next=true`.
- Reproduz cada item por `exposure_time_ms` em loop.
- Recarrega a playlist quando a API muda.
- Watchdog reinicia o MPV se travar ou perder o IPC.
- Limpa midias antigas do cache a cada `cleanup_interval_sec` (padrao 30 min).
- Telemetria enviada a cada 5 min (healthcheck), com evento de startup e playlist update.

## Auto start (24/7)

### Instalacao automatica

- Linux/Orange Pi: `bash scripts/install/linux.sh`
- macOS: `bash scripts/install/macos.sh`
- Windows (PowerShell como admin): `powershell -ExecutionPolicy Bypass -File scripts/install/windows.ps1`
  - Variaveis opcionais: `PYTHON_BIN` e `CONFIG_PATH` (Linux/macOS) ou `-Python`/`-Config` (Windows).

### Linux / Orange Pi (systemd)

1. Copie `scripts/linux/systemd/kiosky.service` para `~/.config/systemd/user/`.
2. Ajuste `ExecStart` e `WorkingDirectory`.
3. Ative:

```
systemctl --user daemon-reload
systemctl --user enable --now kiosky.service
```

### macOS (launchd)

1. Copie `scripts/macos/launchd/com.kiosky.player.plist` para `~/Library/LaunchAgents/`.
2. Ajuste caminhos em `ProgramArguments` e `WorkingDirectory`.
3. Ative:

```
launchctl load ~/Library/LaunchAgents/com.kiosky.player.plist
```

### Windows (Task Scheduler)

1. Edite `scripts/windows/start-kiosk.bat` para apontar para seu Python e `config.json`.
2. Crie uma tarefa no Task Scheduler para "At log on" e aponte para o `.bat`.

## Dicas de robustez

- Use `log_file` no `config.json` para salvar logs (rotacao via `log_max_bytes`/`log_backup_count`).
- Use `status_file` para gerar um JSON com status (uptime, item atual, ultimo polling).
- Para liberar espaco, `cleanup_interval_sec` remove arquivos fora da playlist atual.
- Garanta que o PC nao durma e desative screen saver.
- Se o MPV estiver fora do PATH, ajuste `mpv_path`.
- No Windows, se personalizar o IPC, use formato `\\\\.\\pipe\\nome`.
