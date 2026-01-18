param(
  [string]$Root = "",
  [string]$Python = "",
  [string]$Config = ""
)

if ([string]::IsNullOrWhiteSpace($Root)) {
  $Root = (Resolve-Path "$PSScriptRoot\..\..").Path
}
if ([string]::IsNullOrWhiteSpace($Python)) {
  $Python = Join-Path $Root ".venv\Scripts\python.exe"
}
if ([string]::IsNullOrWhiteSpace($Config)) {
  $Config = Join-Path $Root "config.json"
}

if (-not (Test-Path $Python)) {
  Write-Error "Python nao encontrado em: $Python"
  exit 1
}
if (-not (Test-Path $Config)) {
  Write-Error "Config nao encontrada em: $Config"
  exit 1
}

$taskName = "KioskyPlayer"
$cmd = "cmd /c \"cd /d \"" + $Root + "\" && \"" + $Python + "\" \"" + (Join-Path $Root "kiosk.py") + "\" --config \"" + $Config + "\"\""

schtasks /Create /F /SC ONLOGON /RL HIGHEST /TN $taskName /TR $cmd | Out-Null
Write-Host "Tarefa criada: $taskName"
