@echo off
cd /d "%~dp0"
".\.venv\Scripts\python.exe" ".\fx.py"
exit /b %ERRORLEVEL%