$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $root ".venv\\Scripts\\python.exe"
$backendDir = Join-Path $root "backend"
$tmpPath = Join-Path $root ".tmp"

if (-not (Test-Path $venvPython)) {
  throw "Backend virtual environment is missing. Run 'npm run setup' first."
}

New-Item -ItemType Directory -Force $tmpPath | Out-Null
$env:TEMP = $tmpPath
$env:TMP = $tmpPath

Push-Location $backendDir
try {
  & $venvPython app.py
}
finally {
  Pop-Location
}
