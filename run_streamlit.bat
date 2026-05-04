@echo off
setlocal

cd /d "%~dp0"
chcp 65001 >nul

if not exist "logs" mkdir "logs"

for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%I"
set "LOG_FILE=logs\run_%TS%.log"

echo [%date% %time%] Starting Streamlit app... > "%LOG_FILE%"
echo [%date% %time%] Log file: %LOG_FILE% >> "%LOG_FILE%"

streamlit run app.py --logger.level=debug >> "%LOG_FILE%" 2>&1

echo [%date% %time%] Streamlit process ended with exit code %ERRORLEVEL% >> "%LOG_FILE%"
endlocal
