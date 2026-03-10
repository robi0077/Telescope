@echo off
setlocal enabledelayedexpansion

:: Get the directory of this script
cd /d "%~dp0"

echo ===================================================
echo   TELESCOPE FINGERPRINT CONSUMER
echo ===================================================

:: Ensure python finds the 'src' package
set PYTHONPATH=%cd%\src;%PYTHONPATH%

echo.
echo Activating Virtual Environment...
call .\venv\Scripts\activate.bat

echo Launching standalone consumer to listen to Scraper Queue...
.\venv\Scripts\python.exe -m telescope.consumer

pause
