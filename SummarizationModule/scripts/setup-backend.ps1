$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$venvPath = Join-Path $root ".venv"
$pythonExe = Join-Path $venvPath "Scripts\\python.exe"
$requirementsPath = Join-Path $root "backend\\requirements.txt"
$stampPath = Join-Path $venvPath ".requirements.sha256"
$tmpPath = Join-Path $root ".tmp"

New-Item -ItemType Directory -Force $tmpPath | Out-Null
$env:TEMP = $tmpPath
$env:TMP = $tmpPath
$env:PIP_CACHE_DIR = Join-Path $root ".pip-cache"

if (-not (Test-Path $pythonExe)) {
  py -3.14 -m venv $venvPath
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to create the backend virtual environment."
  }
}

$requirementsHash = (Get-FileHash $requirementsPath -Algorithm SHA256).Hash
$installedHash = if (Test-Path $stampPath) { (Get-Content $stampPath -Raw).Trim() } else { "" }

if ($requirementsHash -ne $installedHash) {
  py -3.14 -m pip --python $pythonExe install --upgrade pip
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to upgrade pip in the local virtual environment."
  }

  py -3.14 -m pip --python $pythonExe install -r $requirementsPath
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to install backend requirements into the local virtual environment."
  }

  Set-Content -Path $stampPath -Value $requirementsHash -NoNewline
}
