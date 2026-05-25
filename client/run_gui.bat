@echo off
title TrackLab GUI
color 0A

echo.
echo ====================================
echo  TrackLab v0.0.2.0
echo  GUI Launcher
echo ====================================
echo.

REM Controlla Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERRORE: Python non trovato nel PATH
    echo.
    echo Installa Python da: https://www.python.org/downloads/
    echo Assicurati di spuntare "Add Python to PATH"
    echo.
    pause
    exit /b 1
)

echo Python trovato, avvio GUI...
echo.

REM Avvia GUI
python run_gui.py

REM Pausa se errore
if errorlevel 1 (
    echo.
    echo Errore durante l'esecuzione.
    pause
)