@echo off
:: MP3 Cataloger - Script di avvio per Windows
:: Questo script installa le dipendenze e avvia il catalogatore MP3

echo ================================================
echo           MP3 Cataloger - Avvio
echo ================================================
echo.

:: Verifica se Python è installato
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERRORE: Python non è installato o non è nel PATH
    echo Scarica Python da: https://python.org/downloads
    pause
    exit /b 1
)

:: Verifica se pip è disponibile
pip --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERRORE: pip non è disponibile
    pause
    exit /b 1
)

:: Installa le dipendenze se non sono presenti
echo Verifico dipendenze...
python -c "import eyed3" 2>nul
if %errorlevel% neq 0 (
    echo Installo eyed3...
    pip install eyed3
)

python -c "import mutagen" 2>nul
if %errorlevel% neq 0 (
    echo Installo mutagen...
    pip install mutagen
)

echo Dipendenze OK!
echo.

:: Richiedi il percorso se non fornito come argomento
if "%~1"=="" (
    set /p "MP3_PATH=Inserisci il percorso della cartella con i file MP3: "
) else (
    set "MP3_PATH=%~1"
)

:: Verifica che il percorso esista
if not exist "%MP3_PATH%" (
    echo ERRORE: Il percorso "%MP3_PATH%" non esiste
    pause
    exit /b 1
)

echo.
echo Avvio catalogazione in: %MP3_PATH%
echo.

:: Esegui il programma
python mp3_cataloger_v0141.py "%MP3_PATH%" -v

echo.
echo Catalogazione completata!
echo Controlla i file di log per i dettagli.
echo.
pause