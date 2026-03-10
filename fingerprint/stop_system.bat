@echo off
setlocal

echo ===================================================
echo   TELESCOPE SYSTEM SHUTDOWN
echo ===================================================

:: 1. Stop Redis Container
echo [1/3] Stopping Redis Container...
docker stop telescope_redis
if %errorlevel% equ 0 (
    echo    - Redis stopped.
) else (
    echo    - Redis was not running or failed to stop.
)

:: 2. Stop Python Processes (API & Worker)
echo [2/3] Stopping Python Processes...
:: This kills all python processes executed by the user. 
:: In a dev environment this is usually acceptable to stop 'uvicorn' and 'celery'.
taskkill /F /IM python.exe /T >nul 2>&1
if %errorlevel% equ 0 (
    echo    - Stopped Python processes.
) else (
    echo    - No Python processes found.
)

echo.
echo ===================================================
echo   SYSTEM SHUTDOWN COMPLETE
echo ===================================================
echo.
pause
