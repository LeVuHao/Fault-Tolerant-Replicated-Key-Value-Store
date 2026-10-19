@echo off
title BACKUP REPLICA 1 (PORT 8002)
color 0A
echo ======================================================================
echo    KHOI DONG BACKUP REPLICA 1 (PORT 8002)
echo ======================================================================
mvn exec:java -Dexec.mainClass="com.phantan.backup.BackupServer" -Dexec.args="8002 backup-1 http://localhost:8001"
pause
