@echo off
REM Start WKennel7 Waitlist Backend Server
REM Keep this window open while using the extension

title WKennel7 Waitlist Backend Server

echo ====================================================
echo WKennel7 Waitlist Backend Server
echo ====================================================
echo.
echo Starting server on port 8000...
echo Extension should connect to: http://localhost:8000/api/waitlist
echo.
echo Keep this window open while using the extension!
echo Press Ctrl+C to stop the server
echo ====================================================
echo.

cd /d "%~dp0"
python backend_server.py 8000

pause
