@echo off
cd /d "%~dp0"
echo Starting your Stock Dashboard...
python -m streamlit run app.py
pause