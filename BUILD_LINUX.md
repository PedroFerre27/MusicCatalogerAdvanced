# Music Cataloger — Build Linux (per il tester)

Sei il tester Linux di Pedro. Questa guida ti porta dal `git clone`
al binario funzionante in 10 minuti.

## Prerequisiti

Distribuzione raccomandata: **Ubuntu 22.04 LTS** o derivate (Debian 12,
Linux Mint 21+, Pop!_OS 22.04). Su Fedora/Arch/openSUSE i comandi
package manager cambiano ma il resto è uguale.

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install -y python3 python3-pip python3-tk python3-venv git \
    libgirepository1.0-dev libcairo2-dev pkg-config

# Fedora
sudo dnf install -y python3 python3-pip python3-tkinter git \
    gobject-introspection-devel cairo-devel

# Arch
sudo pacman -S --needed python python-pip tk git \
    gobject-introspection cairo
```

Verifica versione Python:
```bash
python3 --version    # deve essere 3.10 o superiore (testato su 3.13)
```

## 1. Clona il repo

```bash
cd ~
git clone https://github.com/PedroFerre27/MusicCatalogerAdvanced.git
cd MusicCatalogerAdvanced

# Vai al tag stabile pilot
git checkout v1086-stable
```

## 2. Setup virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip wheel

# Installa dipendenze runtime
pip install customtkinter Pillow eyed3 mutagen requests pyjwt \
    musicbrainzngs

# Installa PyInstaller per il build
pip install pyinstaller
```

## 3. Build del binario

```bash
# Pulisci cache PyInstaller
rm -rf ~/.cache/pyinstaller build/ dist/ 2>/dev/null

# Build (~3-5 min)
pyinstaller music_cataloger_linux.spec --clean --noconfirm
```

Output: `dist/music-cataloger-advanced` (~100-130 MB, file singolo).

## 4. Test del binario

```bash
# Lancia direttamente (scriverà data/ accanto al binario)
./dist/music-cataloger-advanced
```

Dovrebbe aprirsi la finestra di login.

Server di Pedro: `https://api.choros27.synology.me`
Le credenziali te le dà Pedro (nuovo utente Pro o Advanced).

## 5. Installazione "vera" (opzionale)

Per averlo come app integrata nel desktop (icona, lanciatore Activities):

```bash
# Sposta il binario in una location stabile
mkdir -p ~/.local/bin
cp dist/music-cataloger-advanced ~/.local/bin/

# Crea cartella applicazione + icona
mkdir -p ~/.local/share/icons/hicolor/256x256/apps
cp icons/app/taskbar_active.png \
   ~/.local/share/icons/hicolor/256x256/apps/music-cataloger.png

# Installa il .desktop file
mkdir -p ~/.local/share/applications
cp music-cataloger.desktop ~/.local/share/applications/

# Aggiorna database delle app
update-desktop-database ~/.local/share/applications/

# Aggiorna cache icone
gtk-update-icon-cache ~/.local/share/icons/hicolor/ 2>/dev/null || true
```

Dopo questo dovresti trovare "Music Cataloger" nelle Activities/menu app.

## 6. Aggiornamenti futuri

L'auto-update non funziona ancora su Linux (Pedro ci sta lavorando).
Per aggiornare manualmente:

```bash
cd ~/MusicCatalogerAdvanced
git fetch --tags
git checkout v1087-stable    # o qualunque tag Pedro pubblichi
source .venv/bin/activate
pyinstaller music_cataloger_linux.spec --clean --noconfirm
cp dist/music-cataloger-advanced ~/.local/bin/    # se hai installato
```

## Troubleshooting

### "ModuleNotFoundError: No module named '_tkinter'"
```bash
sudo apt install python3-tk     # Debian/Ubuntu
sudo dnf install python3-tkinter  # Fedora
```

### "Failed to load Python shared library"
La build su Ubuntu vecchia (18.04, 20.04) non gira su distro nuove
e viceversa. Builda sulla TUA distro.

### Lentezza al primo avvio
Normale. PyInstaller onefile estrae le dipendenze in `/tmp/_MEI*` al
primo avvio (3-5 secondi). Avvii successivi sono più veloci.

### "Error: cannot connect to server"
Verifica che il NAS di Pedro sia online:
```bash
curl https://api.choros27.synology.me/health
```
Risposta attesa: `{"status":"ok",...}`.

### Bug? Log al feedback Pedro

Da terminale:
```bash
./dist/music-cataloger-advanced 2>&1 | tee ~/music-cataloger.log
```

Riproduce il bug, poi mandagli a Pedro `~/music-cataloger.log` insieme
a screenshot del problema.
