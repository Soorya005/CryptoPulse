@echo off
cd /d "%~dp0"
if exist .venv\Scripts\activate.bat call .venv\Scripts\activate.bat
python -m pytest detector\test_detector.py api\test_api.py -v --tb=short
