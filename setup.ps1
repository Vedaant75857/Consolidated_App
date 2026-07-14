$ErrorActionPreference = "Stop"

Write-Host "--- Installing node_modules for Frontends ---"

$frontends = @(
    ".\landing-page",
    ".\DataConsolidationAppV7\frontend",
    ".\ProcIP_Module2-main\frontend",
    ".\SummarizationModule\frontend"
)

foreach ($frontend in $frontends) {
    if (Test-Path $frontend) {
        Write-Host "Installing NPM dependencies for $frontend"
        Push-Location $frontend
        npm install
        Pop-Location
    } else {
        Write-Host "Path not found: $frontend"
    }
}


Write-Host "`n--- Initializing Python Environments for Backends ---"

$venvPath = ".\.venv"
$pythonExe = Join-Path $venvPath "Scripts\python.exe"

if (!(Test-Path $pythonExe)) {
    Write-Host "Creating repo root .venv"
    $venvCreated = $false

    if (Get-Command py -ErrorAction SilentlyContinue) {
        py -3.13 -m venv $venvPath
        $venvCreated = $LASTEXITCODE -eq 0 -and (Test-Path $pythonExe)
    }

    if (!$venvCreated) {
        $localPython313 = Join-Path $env:LOCALAPPDATA "Programs\Python\Python313\python.exe"
        if (Test-Path $localPython313) {
            $localPythonVersion = & $localPython313 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
            if ($LASTEXITCODE -eq 0 -and $localPythonVersion -eq "3.13") {
                & $localPython313 -m venv $venvPath
                $venvCreated = $LASTEXITCODE -eq 0 -and (Test-Path $pythonExe)
            }
        }
    }

    if (!$venvCreated) {
        $systemPythonVersion = & python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
        if ($LASTEXITCODE -eq 0 -and $systemPythonVersion -eq "3.13") {
            python -m venv $venvPath
            $venvCreated = $LASTEXITCODE -eq 0 -and (Test-Path $pythonExe)
        }
    }

    if (!$venvCreated) {
        throw "Python 3.13 is required to create the repo root .venv. Install Python 3.13 or make py -3.13 available."
    }
}

Write-Host "Using Python:"
& $pythonExe --version
$venvPythonVersion = & $pythonExe -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
if ($LASTEXITCODE -ne 0 -or $venvPythonVersion -ne "3.13") {
    throw "The repo root .venv must use Python 3.13. Recreate it with Python 3.13 before continuing."
}

Write-Host "Upgrading pip in repo root .venv"
& $pythonExe -m pip install --upgrade pip

Write-Host "Installing backend runtime and dev requirements"
& $pythonExe -m pip install -r .\requirements-dev.txt

Write-Host "`n--- Setup Complete ---"
