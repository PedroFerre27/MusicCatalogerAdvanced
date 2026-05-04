# Build cross-platform — Music Cataloger Advanced

PyInstaller **non fa cross-compile**. Per ogni OS target serve una macchina
(o VM, o container) di quell'OS per buildare il binario nativo.

Pedro: per i tuoi amici Linux/macOS hai 3 opzioni, in ordine di facilità:

## Opzione A — Distribuire la versione Python (più facile, raccomandato)

Per il pilot privato con amici tecnici, distribuire il sorgente è MOLTO
più semplice. Loro:

```bash
# Linux/macOS
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run_gui.py
```

Niente build, niente firme, niente nascondersi dietro Gatekeeper.
Funziona su qualunque OS con Python 3.11+. Per renderlo più carino
preparo nel prossimo turno uno script `install.sh` che fa setup
in un comando.

## Opzione B — Build su Linux (per binario standalone)

Se vuoi un binario singolo che gli amici Linux possano scaricare ed
eseguire senza installare Python:

```bash
# Su una macchina Linux (Ubuntu 22.04 LTS raccomandato per max
# compatibilità glibc):
pip install pyinstaller customtkinter Pillow eyed3 mutagen requests pyjwt
pyinstaller music_cataloger_linux.spec --clean

# Output: dist/music-cataloger-advanced
chmod +x dist/music-cataloger-advanced
./dist/music-cataloger-advanced
```

Se non hai una macchina Linux:
- **WSL2 su Windows** (gratis, integrato): `wsl --install -d Ubuntu-22.04`
- **VM VirtualBox** con Ubuntu 22.04
- **Docker** (vedi `Dockerfile.build-linux`)

### Distribuzione su Linux

Per fare in modo che l'app appaia nel menu applicazioni di GNOME/KDE
serve creare un file `.desktop`:

```ini
# ~/.local/share/applications/music-cataloger.desktop
[Desktop Entry]
Type=Application
Name=Music Cataloger Advanced
Comment=Organize your Latin dance music library
Exec=/opt/music-cataloger/music-cataloger-advanced
Icon=/opt/music-cataloger/icons/app/taskbar_active.png
Terminal=false
Categories=AudioVideo;Audio;
```

## Opzione C — Build su macOS (richiede Mac)

Su un Mac (Intel o Apple Silicon):

```bash
pip install pyinstaller customtkinter Pillow eyed3 mutagen requests pyjwt
pyinstaller music_cataloger_macos.spec --clean

# Output:
# dist/Music Cataloger Advanced.app  (bundle, drag in /Applications)
```

### Importante per amici macOS

L'app non è firmata con un certificato Apple Developer ($99/anno).
**Al primo avvio** macOS dirà "App di sviluppatore non identificato"
e bloccherà l'apertura. Per aprirla la prima volta:

1. Click destro sull'app in Finder
2. "Apri"
3. Conferma "Apri" nel popup

Da lì in poi macOS si ricorda e l'app si apre normalmente con doppio
click.

### Conversione PNG → ICNS (icona macOS)

Se vuoi che l'icona dell'app sia "vera" su macOS serve un file `.icns`:

```bash
# Su macOS (sips e iconutil sono preinstallati)
mkdir music_cataloger.iconset
sips -z 16 16     icons/app/taskbar_active.png --out music_cataloger.iconset/icon_16x16.png
sips -z 32 32     icons/app/taskbar_active.png --out music_cataloger.iconset/icon_16x16@2x.png
sips -z 32 32     icons/app/taskbar_active.png --out music_cataloger.iconset/icon_32x32.png
sips -z 64 64     icons/app/taskbar_active.png --out music_cataloger.iconset/icon_32x32@2x.png
sips -z 128 128   icons/app/taskbar_active.png --out music_cataloger.iconset/icon_128x128.png
sips -z 256 256   icons/app/taskbar_active.png --out music_cataloger.iconset/icon_128x128@2x.png
sips -z 256 256   icons/app/taskbar_active.png --out music_cataloger.iconset/icon_256x256.png
sips -z 512 512   icons/app/taskbar_active.png --out music_cataloger.iconset/icon_256x256@2x.png
sips -z 512 512   icons/app/taskbar_active.png --out music_cataloger.iconset/icon_512x512.png
cp icons/app/taskbar_active.png                   music_cataloger.iconset/icon_512x512@2x.png
iconutil -c icns music_cataloger.iconset -o icons/music_cataloger.icns
```

## Auto-update cross-platform

L'auto-updater attuale è solo Windows (usa batch file per swap EXE).
Per Linux/macOS serve un meccanismo diverso (replace binary atomico,
restart). Per ora gli amici Linux/macOS dovranno scaricare il nuovo
binario manualmente. Lo faccio post-pilot 1.
