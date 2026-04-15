@echo off
setlocal enabledelayedexpansion

echo.
echo ===================================================
echo   DataScopingTool — Full Build Script
echo ===================================================
echo.

:: -------------------------------------------------------------------
:: Step 0 — Pre-flight checks
:: -------------------------------------------------------------------
where node >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Node.js is not installed or not on PATH.
    echo        Install it from https://nodejs.org/ and try again.
    pause
    exit /b 1
)
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not installed or not on PATH.
    echo        Install it from https://python.org/ and try again.
    pause
    exit /b 1
)

:: Verify Python version >= 3.11
for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set PY_VER=%%v
for /f "tokens=1,2 delims=." %%a in ("!PY_VER!") do (
    set PY_MAJOR=%%a
    set PY_MINOR=%%b
)
if !PY_MAJOR! lss 3 (
    echo ERROR: Python 3.11+ required, found !PY_VER!.
    pause & exit /b 1
)
if !PY_MAJOR! equ 3 if !PY_MINOR! lss 11 (
    echo ERROR: Python 3.11+ required, found !PY_VER!.
    pause & exit /b 1
)
echo   Python !PY_VER! OK

:: Verify Node version >= 18
for /f "tokens=1 delims=." %%v in ('node --version 2^>^&1') do set NODE_RAW=%%v
set NODE_MAJOR=!NODE_RAW:v=!
if !NODE_MAJOR! lss 18 (
    echo ERROR: Node.js 18+ required, found !NODE_RAW!.
    pause & exit /b 1
)
echo   Node !NODE_RAW! OK

:: Verify 64-bit Python
python -c "import struct; exit(0 if struct.calcsize('P')*8==64 else 1)" 2>nul
if %errorlevel% neq 0 (
    echo ERROR: 64-bit Python required. Current Python appears to be 32-bit.
    pause & exit /b 1
)
echo   64-bit architecture OK

:: Verify lockfiles exist for all four frontends
set LOCK_OK=1
for %%d in (
    "landing-page"
    "DataConsolidationAppV7\frontend"
    "ProcIP_Module2-main\frontend"
    "SummarizationModule\frontend"
) do (
    if not exist "%%~d\package-lock.json" (
        echo ERROR: Missing package-lock.json in %%~d
        set LOCK_OK=0
    )
)
if !LOCK_OK! equ 0 (
    echo        Run "npm install" in each frontend to generate lockfiles, then retry.
    pause & exit /b 1
)
echo   All frontend lockfiles present

:: -------------------------------------------------------------------
:: Step 1 — Install Python build dependencies
:: -------------------------------------------------------------------
echo.
echo [1/9] Installing Python build dependencies ...
pip install -r requirements-build.txt
if %errorlevel% neq 0 (
    echo ERROR: pip install failed.
    pause
    exit /b 1
)

:: -------------------------------------------------------------------
:: Step 2 — Build Landing Page frontend
:: -------------------------------------------------------------------
echo.
echo [2/9] Building Landing Page frontend ...
cd landing-page
call npm ci
if %errorlevel% neq 0 (
    echo ERROR: npm ci failed for Landing Page.
    pause & exit /b 1
)
set VITE_HOME_URL=http://localhost:3000
set VITE_STITCHER_FE=http://localhost:3001
set VITE_NORMALIZER_FE=http://localhost:5000
set VITE_ANALYZER_FE=http://localhost:3005
call npx vite build
if %errorlevel% neq 0 (
    echo ERROR: Landing page build failed.
    pause
    exit /b 1
)
cd ..

:: -------------------------------------------------------------------
:: Step 3-5 — Build Module frontends
:: -------------------------------------------------------------------
echo.
echo [3/9] Building Module 1 (Data Stitcher) frontend ...
cd DataConsolidationAppV7\frontend
call npm ci
if %errorlevel% neq 0 (
    echo ERROR: npm ci failed for Module 1.
    pause & exit /b 1
)
set VITE_HOME_URL=http://localhost:3000
set VITE_STITCHER_FE=http://localhost:3001
set VITE_NORMALIZER_FE=http://localhost:5000
set VITE_ANALYZER_FE=http://localhost:3005
call npx vite build
if %errorlevel% neq 0 (
    echo ERROR: Module 1 frontend build failed.
    pause
    exit /b 1
)
cd ..\..

echo.
echo [4/9] Building Module 2 (Data Normalizer) frontend ...
cd ProcIP_Module2-main\frontend
call npm ci
if %errorlevel% neq 0 (
    echo ERROR: npm ci failed for Module 2.
    pause & exit /b 1
)
set VITE_HOME_URL=http://localhost:3000
set VITE_STITCHER_FE=http://localhost:3001
set VITE_NORMALIZER_FE=http://localhost:5000
set VITE_ANALYZER_FE=http://localhost:3005
call npx vite build
if %errorlevel% neq 0 (
    echo ERROR: Module 2 frontend build failed.
    pause
    exit /b 1
)
cd ..\..

echo.
echo [5/9] Building Module 3 (Spend Summarizer) frontend ...
cd SummarizationModule\frontend
call npm ci
if %errorlevel% neq 0 (
    echo ERROR: npm ci failed for Module 3.
    pause & exit /b 1
)
set VITE_HOME_URL=http://localhost:3000
set VITE_STITCHER_FE=http://localhost:3001
set VITE_NORMALIZER_FE=http://localhost:5000
set VITE_ANALYZER_FE=http://localhost:3005
call npx vite build
if %errorlevel% neq 0 (
    echo ERROR: Module 3 frontend build failed.
    pause
    exit /b 1
)
cd ..\..

:: -------------------------------------------------------------------
:: Step 6 — Run PyInstaller (one-folder mode)
:: -------------------------------------------------------------------
echo.
echo [6/9] Packaging with PyInstaller (this may take several minutes) ...
python -m PyInstaller --clean --noconfirm DataScopingTool.spec
if %errorlevel% neq 0 (
    echo ERROR: PyInstaller build failed.
    pause
    exit /b 1
)

:: -------------------------------------------------------------------
:: Step 7 — Write build metadata + copy README into distribution folder
:: -------------------------------------------------------------------
echo.
echo [7/9] Writing build metadata ...

for /f "tokens=*" %%g in ('git rev-parse --short HEAD 2^>nul') do set GIT_SHA=%%g
if "!GIT_SHA!"=="" set GIT_SHA=unknown

for /f "tokens=*" %%t in ('python -c "from datetime import datetime,timezone;print(datetime.now(timezone.utc).isoformat())"') do set BUILD_TIME=%%t

for /f "tokens=*" %%n in ('node --version 2^>^&1') do set NODE_FULL=%%n

(
echo {
echo   "built_at": "!BUILD_TIME!",
echo   "python_version": "!PY_VER!",
echo   "node_version": "!NODE_FULL!",
echo   "git_sha": "!GIT_SHA!"
echo }
) > "dist\DataScopingTool\build-info.json"

if exist README.txt (
    copy /y README.txt "dist\DataScopingTool\README.txt" >nul
    echo   README.txt copied into distribution folder.
)

:: -------------------------------------------------------------------
:: Step 8 — Smoke test the built app
:: -------------------------------------------------------------------
echo.
echo [8/9] Smoke-testing the built app ...

where curl >nul 2>&1
if %errorlevel% neq 0 (
    echo WARNING: curl not found on PATH — skipping smoke test.
    echo          Install curl or verify manually that the app works.
    goto :smoke_done
)

start "" "dist\DataScopingTool\DataScopingTool.exe"
echo   Waiting up to 90 seconds for services to start ...

set WAIT=0
:wait_loop
if !WAIT! geq 90 goto :wait_done
curl -sf http://127.0.0.1:3000/ >nul 2>&1
if !errorlevel! equ 0 goto :wait_done
timeout /t 1 /nobreak >nul
set /a WAIT+=1
goto :wait_loop
:wait_done

timeout /t 5 /nobreak >nul

set SMOKE_OK=1

:: Health endpoint checks
curl -sf http://127.0.0.1:3000/ >nul 2>&1
if !errorlevel! neq 0 (
    echo   FAIL: Landing Page [port 3000] — health endpoint did not respond.
    set SMOKE_OK=0
) else (
    echo   OK:   Landing Page [port 3000] — health endpoint responded.
)

curl -sf http://127.0.0.1:3001/api/health >nul 2>&1
if !errorlevel! neq 0 (
    echo   FAIL: Module 1 — Data Stitcher [port 3001] — /api/health did not respond.
    set SMOKE_OK=0
) else (
    echo   OK:   Module 1 — Data Stitcher [port 3001] — /api/health responded.
)

curl -sf http://127.0.0.1:5000/api/health >nul 2>&1
if !errorlevel! neq 0 (
    echo   FAIL: Module 2 — Data Normalizer [port 5000] — /api/health did not respond.
    set SMOKE_OK=0
) else (
    echo   OK:   Module 2 — Data Normalizer [port 5000] — /api/health responded.
)

curl -sf http://127.0.0.1:3005/api/health >nul 2>&1
if !errorlevel! neq 0 (
    echo   FAIL: Module 3 — Spend Summarizer [port 3005] — /api/health did not respond.
    set SMOKE_OK=0
) else (
    echo   OK:   Module 3 — Spend Summarizer [port 3005] — /api/health responded.
)

:: Landing page HTML marker check
curl -sf http://127.0.0.1:3000/ | findstr /i "id=\"root\"" >nul 2>&1
if !errorlevel! neq 0 (
    echo   FAIL: Landing Page HTML missing expected root element.
    set SMOKE_OK=0
) else (
    echo   OK:   Landing Page HTML contains root element.
)

:: Verify a JS asset is fetchable from each module (not returning index.html fallback)
for %%p in (3000 3001 5000 3005) do (
    set ASSET_OK=0
    for /f "tokens=*" %%u in ('curl -sf http://127.0.0.1:%%p/ 2^>nul ^| findstr /r "src=.*\.js"') do (
        set ASSET_OK=1
    )
    if !ASSET_OK! equ 0 (
        echo   WARN: Port %%p — could not find JS asset reference in index.html.
    )
)

:: Config endpoint check (verifies runtime config is served)
curl -sf http://127.0.0.1:3000/config.json >nul 2>&1
if !errorlevel! neq 0 (
    echo   WARN: /config.json not served from Landing Page.
) else (
    echo   OK:   /config.json served from Landing Page.
)

taskkill /f /im DataScopingTool.exe >nul 2>&1

if !SMOKE_OK! equ 0 (
    echo.
    echo ERROR: Smoke test failed. The app is broken — do not ship it.
    pause
    exit /b 1
)
echo   Smoke test passed.

:smoke_done

:: -------------------------------------------------------------------
:: Step 9 — Package into a distributable ZIP
:: -------------------------------------------------------------------
echo.
echo [9/9] Creating distributable ZIP ...
if exist "dist\DataScopingTool.zip" del "dist\DataScopingTool.zip"
powershell -Command "Compress-Archive -Path 'dist\DataScopingTool\*' -DestinationPath 'dist\DataScopingTool.zip' -Force"
if %errorlevel% neq 0 (
    echo WARNING: ZIP creation failed. You can still share the dist\DataScopingTool folder directly.
) else (
    echo   ZIP created successfully.
)

echo.
echo ===================================================
echo   BUILD COMPLETE
echo.
echo   Portable app folder: dist\DataScopingTool\
echo   Distributable ZIP:   dist\DataScopingTool.zip
echo.
echo   Share the ZIP with users.
echo   They unzip it and double-click DataScopingTool.exe.
echo ===================================================
echo.
pause
