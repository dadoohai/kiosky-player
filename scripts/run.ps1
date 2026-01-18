param(
  [string]$Root = "",
  [string]$Config = "",
  [string]$Python = ""
)

if ([string]::IsNullOrWhiteSpace($Root)) {
  $Root = (Resolve-Path "$PSScriptRoot\..\").Path
}
if ([string]::IsNullOrWhiteSpace($Config)) {
  $Config = Join-Path $Root "config.json"
}
if ([string]::IsNullOrWhiteSpace($Python)) {
  $Python = "python"
}

$example = Join-Path $Root "config.example.json"
if (-not (Test-Path $Config)) {
  if (Test-Path $example) {
    Copy-Item $example $Config -Force
    Write-Host "[run] config.json criado a partir de config.example.json"
  }
  Write-Host "[run] Edite $Config e preencha api_key e environment_id"
  exit 1
}

$deps = Join-Path $Root "scripts\install\deps.ps1"
if (-not (Test-Path $deps)) {
  Write-Error "deps.ps1 nao encontrado"
  exit 1
}

& powershell -ExecutionPolicy Bypass -File $deps -Root $Root -Python $Python

$venvPython = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
  Write-Error "Python do venv nao encontrado em $venvPython"
  exit 1
}

& $venvPython (Join-Path $Root "kiosk.py") --config $Config
