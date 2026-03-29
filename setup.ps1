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

$backends = @(
    ".\DataConsolidationAppV7\backend",
    ".\ProcIP_Module2-main\backend",
    ".\SummarizationModule\backend"
)

foreach ($backend in $backends) {
    if (Test-Path $backend) {
        Write-Host "Setting up Python Environment for $backend"
        Push-Location $backend
        
        # Create virtual environment if it doesn't exist
        if (!(Test-Path ".venv")) {
            Write-Host "Creating .venv in $backend"
            python -m venv .venv
        }
        
        # Install requirements
        if (Test-Path "requirements.txt") {
            Write-Host "Installing requirements in $backend"
            .\.venv\Scripts\python -m pip install --upgrade pip
            .\.venv\Scripts\pip install -r requirements.txt
        }
        
        Pop-Location
    } else {
        Write-Host "Path not found: $backend"
    }
}

Write-Host "`n--- Setup Complete ---"
