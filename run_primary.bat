@echo off
title PRIMARY NODE (PORT 8001)
color 0B
echo ======================================================================
echo    KHOI DONG PRIMARY NODE (PORT 8001)
echo ======================================================================
mvn exec:java -Dexec.mainClass="com.phantan.mock.MockPrimaryServer" -Dexec.args="8001"
pause
