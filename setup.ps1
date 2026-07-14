$ErrorActionPreference = "Stop"

Write-Host "--- Installing repository root node_modules ---"
npm install
if ($LASTEXITCODE -ne 0) {
    throw "Root NPM dependency installation failed with exit code $LASTEXITCODE."
}

Write-Host "`n--- Installing node_modules for Frontends ---"

$frontends = @(
    ".\frontend\landing",
    ".\frontend\module1",
    ".\frontend\module2",
    ".\frontend\module3"
)

foreach ($frontend in $frontends) {
    if (Test-Path $frontend) {
        Write-Host "Installing NPM dependencies for $frontend"
        Push-Location $frontend
        try {
            if ($frontend -eq ".\frontend\module1") {
                # Module 1 currently uses React 19 while glide-data-grid's peer
                # declaration only advertises support through React 18.
                npm install --legacy-peer-deps
            } else {
                npm install
            }
            if ($LASTEXITCODE -ne 0) {
                throw "NPM dependency installation failed for $frontend with exit code $LASTEXITCODE."
            }
        } finally {
            Pop-Location
        }
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

    $portablePython313 = Join-Path $PSScriptRoot "bin\python-portable\python.exe"
    if (Test-Path $portablePython313) {
        $portablePythonVersion = & $portablePython313 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
        if ($LASTEXITCODE -eq 0 -and $portablePythonVersion -eq "3.13") {
            & $portablePython313 -m venv $venvPath
            $venvCreated = $LASTEXITCODE -eq 0 -and (Test-Path $pythonExe)
        }
    }

    if (!$venvCreated -and (Get-Command py -ErrorAction SilentlyContinue)) {
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
if ($LASTEXITCODE -ne 0) {
    throw "pip upgrade failed with exit code $LASTEXITCODE."
}

Write-Host "Installing backend runtime and dev requirements"
& $pythonExe -m pip install -r .\requirements-dev.txt
if ($LASTEXITCODE -ne 0) {
    throw "Backend dependency installation failed with exit code $LASTEXITCODE."
}

Write-Host "`n--- Setup Complete ---"
