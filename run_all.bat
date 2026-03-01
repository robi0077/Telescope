@echo off
setlocal enabledelayedexpansion

:: Get the directory of this script
cd /d "%~dp0"

echo ===================================================
echo   TELESCOPE ALL-IN-ONE LAUNCHER
echo ===================================================

:: -----------------------------------------------------------------------------
:: 1. REDIS CHECK & START
:: -----------------------------------------------------------------------------
echo [1/3] Checking Redis...
docker ps | findstr "telescope_redis" >nul
if %errorlevel% equ 0 (
    echo    - Redis is running.
) else (
    echo    - Starting Redis...
    docker start telescope_redis >nul 2>&1
    if !errorlevel! neq 0 (
        echo    - Container not found, creating new one...
        docker run -d -p 6379:6379 --name telescope_redis redis:7-alpine >nul
    )
)

:: -----------------------------------------------------------------------------
:: 2. LAUNCH BACKEND (API + WORKER)
:: -----------------------------------------------------------------------------
echo [2/3] Launching Backend System...
:: We use 'start' so this script continues immediately
:: run_system.bat itself spawns the Worker in a NEW window and keeps API in ITS window.
:: So we start run_system.bat in a NEW window so IT can hold the API.
start "Telescope Backend" run_system.bat

:: Give it a moment to initialize
timeout /t 5 >nul

:: -----------------------------------------------------------------------------
:: 3. LAUNCH FRONTEND
:: -----------------------------------------------------------------------------
echo [3/3] Launching Frontend...
start "Telescope Frontend" run_frontend.bat

echo.
echo ===================================================
echo   SYSTEM LAUNCHED!
echo ===================================================
echo.
echo Backend API: http://localhost:8000
echo Frontend UI: http://localhost:5173
echo.
echo You can close this launcher window now (the other windows will stay open).
pause
