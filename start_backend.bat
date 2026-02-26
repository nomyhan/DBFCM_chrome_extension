@echo off
REM Start DBFCM Extension Backend Server
REM Keep this window open while using the extension

title DBFCM Extension Backend Server

echo ====================================================
echo DBFCM Extension Backend Server
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
