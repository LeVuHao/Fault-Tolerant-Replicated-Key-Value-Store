@echo off
title BACKUP REPLICA 2 (PORT 8003) - DEMO CATCH-UP RECOVERY
color 0E
echo ======================================================================
echo    KHOI DONG BACKUP REPLICA 2 (PORT 8003) - DEMO CATCH-UP
echo ======================================================================
mvn exec:java -Dexec.mainClass="com.phantan.backup.BackupServer" -Dexec.args="8003 backup-2 http://localhost:8001"
pause
