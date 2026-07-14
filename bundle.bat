@echo off
setlocal
pushd "%~dp0"
if errorlevel 1 (
    echo Failed to enter the ProcIP Suite directory.
    endlocal & exit /b 1
)

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference = 'Stop';" ^
  "$required = @('backend', 'frontend', 'bin', 'requirements-backend.txt', 'requirements-dev.txt', 'package.json', 'package-lock.json', 'setup.ps1', 'start.bat', 'ENVIRONMENT.md', 'README.md');" ^
  "$excludedDirectories = @('.agent', '.agents', '.codex', 'logs', 'node_modules', '.venv');" ^
  "function Copy-FilteredTree([string]$Source, [string]$Destination) {" ^
  "  New-Item -ItemType Directory -Path $Destination -Force | Out-Null;" ^
  "  Get-ChildItem -LiteralPath $Source -Force | ForEach-Object {" ^
  "    $target = Join-Path $Destination $_.Name;" ^
  "    if ($_.PSIsContainer) {" ^
  "      if ($excludedDirectories -notcontains $_.Name) { Copy-FilteredTree $_.FullName $target }" ^
  "    } else { Copy-Item -LiteralPath $_.FullName -Destination $target -Force }" ^
  "  };" ^
  "};" ^
  "$missing = @($required | Where-Object { -not (Test-Path -LiteralPath $_) });" ^
  "if ($missing.Count -gt 0) { throw ('Missing required bundle input(s): ' + ($missing -join ', ')) };" ^
  "$share = Join-Path (Get-Location).Path 'share';" ^
  "New-Item -ItemType Directory -Path $share -Force | Out-Null;" ^
  "$stage = Join-Path $share ('.bundle-staging-' + [guid]::NewGuid().ToString('N'));" ^
  "$archive = Join-Path $share 'ProcIP_Suite.zip';" ^
  "$temporaryArchive = Join-Path $share ('.ProcIP_Suite-' + [guid]::NewGuid().ToString('N') + '.zip');" ^
  "try {" ^
  "  New-Item -ItemType Directory -Path $stage -Force | Out-Null;" ^
  "  Copy-FilteredTree 'backend' (Join-Path $stage 'backend');" ^
  "  Copy-FilteredTree 'frontend' (Join-Path $stage 'frontend');" ^
  "  Copy-Item -LiteralPath 'bin' -Destination (Join-Path $stage 'bin') -Recurse -Force;" ^
  "  foreach ($file in $required[3..($required.Count - 1)]) { Copy-Item -LiteralPath $file -Destination (Join-Path $stage $file) -Force };" ^
  "  $archiveInputs = @($required | ForEach-Object { Join-Path $stage $_ });" ^
  "  Compress-Archive -LiteralPath $archiveInputs -DestinationPath $temporaryArchive -CompressionLevel Optimal;" ^
  "  Move-Item -LiteralPath $temporaryArchive -Destination $archive -Force;" ^
  "  Write-Host ('Created ' + $archive);" ^
  "} finally {" ^
  "  if (Test-Path -LiteralPath $temporaryArchive) { Remove-Item -LiteralPath $temporaryArchive -Force };" ^
  "  if (Test-Path -LiteralPath $stage) { Remove-Item -LiteralPath $stage -Recurse -Force };" ^
  "}"

set "EXIT_CODE=%ERRORLEVEL%"
popd
endlocal & exit /b %EXIT_CODE%
