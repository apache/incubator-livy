@ECHO OFF

set arg1=%1

PowerShell -NoProfile -ExecutionPolicy Bypass -Command "& '%~dp0\livy-server2.ps1'" %arg1%