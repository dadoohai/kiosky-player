@echo off
set BASE_DIR=C:\path\to\kioskyLocal
set PYTHON_EXE=%BASE_DIR%\.venv\Scripts\python.exe
set CONFIG=%BASE_DIR%\config.json

cd /d %BASE_DIR%
"%PYTHON_EXE%" "%BASE_DIR%\kiosk.py" --config "%CONFIG%"
