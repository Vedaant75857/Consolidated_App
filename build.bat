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

:: -------------------------------------------------------------------
:: Step 1 — Install Python build dependencies
:: -------------------------------------------------------------------
echo [1/5] Installing Python build dependencies ...
pip install -r requirements-build.txt
if %errorlevel% neq 0 (
    echo ERROR: pip install failed.
    pause
    exit /b 1
)

:: -------------------------------------------------------------------
:: Step 2 — Build Landing Page frontend
::          (with env vars pointing to backend ports)
:: -------------------------------------------------------------------
echo.
echo [2/5] Building Landing Page frontend ...
cd landing-page
call npm install
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
:: Step 3 — Build Module frontends
::          (same env vars so cross-module links resolve correctly)
:: -------------------------------------------------------------------
echo.
echo [3/5] Building Module 1 (Data Stitcher) frontend ...
cd DataConsolidationAppV7\frontend
call npm install
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
echo [4/5] Building Module 2 (Data Normalizer) frontend ...
cd ProcIP_Module2-main\frontend
call npm install
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
echo [5/6] Building Module 3 (Spend Summarizer) frontend ...
cd SummarizationModule\frontend
call npm install
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
:: Step 6 — Run PyInstaller
:: -------------------------------------------------------------------
echo.
echo [6/6] Packaging with PyInstaller (this may take several minutes) ...
pyinstaller --clean --noconfirm DataScopingTool.spec
if %errorlevel% neq 0 (
    echo ERROR: PyInstaller build failed.
    pause
    exit /b 1
)

echo.
echo ===================================================
echo   BUILD COMPLETE
echo.
echo   Your EXE is at:  dist\DataScopingTool.exe
echo.
echo   Share this single file with users.
echo   They just double-click it to run the app.
echo ===================================================
echo.
pause
