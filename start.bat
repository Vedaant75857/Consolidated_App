@echo off
setlocal
pushd "%~dp0"
if errorlevel 1 (
    echo Failed to enter the ProcIP Suite directory.
    endlocal & exit /b 1
)

set "PATH=%~dp0bin\node-portable;%~dp0bin\python-portable;%PATH%"

if not exist "bin\node-portable\node.exe" (
    echo Missing portable Node.js: bin\node-portable\node.exe
    set "EXIT_CODE=1"
    goto :cleanup
)
if not exist "bin\node-portable\npm.cmd" (
    echo Missing portable npm: bin\node-portable\npm.cmd
    set "EXIT_CODE=1"
    goto :cleanup
)
if not exist "bin\python-portable\python.exe" (
    echo Missing portable Python: bin\python-portable\python.exe
    set "EXIT_CODE=1"
    goto :cleanup
)

if not exist "node_modules\" goto :setup
if not exist ".venv\" goto :setup
goto :dev

:setup
call npm run setup
if errorlevel 1 goto :setup_failed

:dev
call npm --ignore-scripts run dev
set "EXIT_CODE=%ERRORLEVEL%"
goto :cleanup

:setup_failed
set "EXIT_CODE=%ERRORLEVEL%"

:cleanup
popd
endlocal & exit /b %EXIT_CODE%
