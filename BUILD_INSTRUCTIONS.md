# 🔨 Build Instructions — Music Cataloger Advanced v1072

## Windows — EXE Installer

### Prerequisiti
```
pip install pyinstaller
pip install customtkinter eyed3 mutagen musicbrainzngs requests Pillow
```

### Build (directory — consigliato, avvio più veloce)
```bat
cd "C:\Users\pmarquesf\OneDrive - Indra\Desktop\Pedro\Music Cataloger"
pyinstaller music_cataloger.spec --clean
```
Output: `dist\Music Cataloger Advanced\Music Cataloger Advanced.exe`

### Build Portable (file singolo)
```bat
pyinstaller --onefile --windowed --icon=icons\variant_1_musical_folder.ico ^
    --name "MusicCataloger" --add-data "config;config" ^
    --add-data "gui;gui" --add-data "icons;icons" run_gui.py
```
Output: `dist\MusicCataloger.exe`

### Installer con Inno Setup
1. Scarica Inno Setup: https://jrsoftware.org/isdl.php
2. Usa lo script `installer.iss` (da generare) che impacchetta la cartella `dist\`

---

## macOS — App Bundle

### Prerequisiti
```bash
pip install py2app customtkinter eyed3 mutagen requests Pillow
```

### setup.py per py2app
```python
from setuptools import setup
APP = ['run_gui.py']
DATA_FILES = [('', ['config', 'gui', 'icons'])]
OPTIONS = {
    'argv_emulation': True,
    'iconfile': 'icons/variant_1_musical_folder.ico',
    'packages': ['customtkinter', 'eyed3', 'mutagen', 'PIL'],
}
setup(app=APP, data_files=DATA_FILES, options={'py2app': OPTIONS}, setup_requires=['py2app'])
```

```bash
python setup.py py2app
```
Output: `dist/Music Cataloger Advanced.app`

---

## Linux — AppImage

### Metodo con appimage-builder
```bash
pip install pyinstaller
pyinstaller --onedir --windowed run_gui.py
# poi usa appimage-builder per creare l'AppImage
```

Oppure distribuzione via PyPI:
```bash
pip install music-cataloger-advanced
music-cataloger
```

---

## Note comuni

- **librosa/numpy**: opzionali (calcolo BPM). Se non inclusi, il BPM viene letto dal DB online.
- **Cartella data/**: NON viene inclusa nell'EXE — viene creata al primo avvio nella home utente.
- **genre_prefs.json**: viene creato al primo avvio nella cartella `data/`.
