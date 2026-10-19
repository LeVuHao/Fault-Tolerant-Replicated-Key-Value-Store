@echo off
echo ======================================================================
echo    KHOI DONG TOAN BO CUM PHAN TAN (PRIMARY + 2 BACKUP + DASHBOARD)
echo ======================================================================
start "PRIMARY-8001" cmd /c "%~dp0run_primary.bat"
timeout /t 2 /nobreak >nul
start "BACKUP1-8002" cmd /c "%~dp0run_backup1.bat"
start "BACKUP2-8003" cmd /c "%~dp0run_backup2.bat"
timeout /t 2 /nobreak >nul
start "" "%~dp0dashboard\index.html"
echo Toan bo cum da duoc khoi dong thanh cong!
