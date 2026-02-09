# Instrucoes de uso (Kiosky)

## Inicializacao simples

Linux / macOS:
```
bash scripts/run.sh
```

Windows (PowerShell):
```
powershell -ExecutionPolicy Bypass -File scripts/run.ps1
```

## Menu de configuracao (rotacao e ambiente)

- Com o player aberto, pressione **Ctrl+S**.
- O navegador abre em `http://127.0.0.1:8765`.
- Altere **environment_id** e **rotacao** e clique em **Salvar**.
- O navegador fecha automaticamente e o player aplica as mudancas.

Se preferir abrir manualmente, use o link acima.

## Modo economia de recursos (Orange Pi Zero 3)

Ative no `config.json`:
```
"low_resource_mode": true
```

E mantenha:
```
"hwdec": "auto"
```

Recomendacoes de midia:
- Video: H.264 (AVC), 720p (max 1080p leve), bitrate baixo (2–4 Mbps).
- Imagens: JPG/PNG.

## Polling e limpeza de cache

- API de midias: a cada **30 min** (`poll_interval_sec = 1800`).
- Se nao mudar, continua tocando normalmente.
- Se sair da lista, o arquivo e removido do cache.
- Se entrar, baixa e entra no carrossel.
- Limpeza extra do cache a cada 30 min (`cleanup_interval_sec`).

## Modo offline

- O player salva a ultima playlist localmente e tenta tocar offline no proximo start.
- Se o estado offline sumir, ele tenta montar playlist usando os arquivos existentes no cache.
- Se quiser limitar idade do offline: `offline_max_age_hours`.
- Se a API estiver fora no boot, pode ignorar a idade offline com `offline_ignore_max_age_when_no_network=true`.
- Para impedir troca para vazio quando API responder sem itens: `allow_empty_playlist_from_api` (recomendado `false`).
- Para evitar limpar cache sem internet: `disable_cleanup_when_offline`.

## Reinicio 00:00 / PREP

- `sync_prep_mode=play_then_resync` (padrao): evita tela preta na janela `23:58-00:05 UTC`.
- `sync_prep_mode=wait_until_anchor`: aguarda `00:05 UTC` para iniciar.
- Midia com falha de carga entra em cooldown (`media_load_retry_cooldown_sec`) e o player avanca para a proxima.

## Telemetria (a cada 5 min)

- Envia healthcheck para `https://api.dadooh.ai/api/v1/interact/telemetry`.
- No `config.json`:
  - `telemetry_enabled`: true/false
  - `telemetry_interval_sec`: 300
  - `station_id`: identificacao do totem (opcional)

## Encerrar o player

- Use **Ctrl+C** no terminal para fechar tudo.
