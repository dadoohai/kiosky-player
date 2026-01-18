param(
  [string]$Root = "",
  [string]$Python = ""
)

if ([string]::IsNullOrWhiteSpace($Root)) {
  $Root = (Resolve-Path "$PSScriptRoot\..\..").Path
}
if ([string]::IsNullOrWhiteSpace($Python)) {
  $Python = "python"
}

function Log($msg) { Write-Host "[deps] $msg" }

$pythonCmd = Get-Command $Python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
  Write-Error "Python nao encontrado. Instale Python 3.9+ e rode novamente."
  exit 1
}

$venvDir = Join-Path $Root ".venv"
Log "Criando venv em $venvDir"
& $Python -m venv $venvDir

$activate = Join-Path $venvDir "Scripts\Activate.ps1"
. $activate

$requirements = Join-Path $Root "requirements.txt"
if (Test-Path $requirements) {
  Log "Instalando dependencias Python"
  pip install -r $requirements
} else {
  Log "requirements.txt nao encontrado"
}

$mpv = Get-Command mpv -ErrorAction SilentlyContinue
if ($mpv) {
  Log "MPV ja instalado"
  exit 0
}

Log "MPV nao encontrado. Tentando instalar..."

if (Get-Command winget -ErrorAction SilentlyContinue) {
  winget install --id mpv.mpv -e
} elseif (Get-Command choco -ErrorAction SilentlyContinue) {
  choco install mpv -y
} elseif (Get-Command scoop -ErrorAction SilentlyContinue) {
  scoop install mpv
} else {
  Write-Error "Nenhum gerenciador (winget/choco/scoop) encontrado. Instale o MPV manualmente."
  exit 1
}

Log "Dependencias instaladas"
