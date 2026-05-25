# BUILD — TrackLab

Guida unica per buildare il programma su Windows, Linux e macOS.
Sostituisce i vecchi BUILD_INSTRUCTIONS.md, BUILD_CROSS_PLATFORM.md
e BUILD_LINUX.md (consolidati qui).

PyInstaller **non fa cross-compile**: per ogni OS target serve una
macchina di quell'OS. Per scelte di distribuzione e auto-update
cross-platform vedi la sezione finale.

---

## Indice

- 1. Windows (build EXE — il caso principale)
- 2. Linux (build binario)
- 3. macOS (build .app bundle)
- 4. Distribuire il sorgente Python (opzione semplice)
- 5. Pubblicare una release (workflow completo)
- 6. Note auto-update cross-platform

---

## 1. Windows — build EXE

### Prerequisiti
- Python 3.13 installato (https://www.python.org/downloads/)
- Pip aggiornato: `python -m pip install --upgrade pip`

```cmd
cd C:\dev\tracklab
pip install -r requirements.txt
pip install pyinstaller
```

### Build (raccomandato — directory mode)
```cmd
python build_ico.py
pyinstaller tracklab.spec --clean --noconfirm
```

Output: `dist\TrackLab\TrackLab.exe`

Avvio più veloce, ma è una cartella intera da distribuire.

### Build alternativa — file singolo portable
```cmd
pyinstaller --onefile --windowed ^
    --icon=icons\tracklab.ico ^
    --version-file=version_info.txt ^
    --name "TrackLab" ^
    --add-data "config;config" ^
    --add-data "gui;gui" ^
    --add-data "icons;icons" ^
    run_gui.py
```

Output: `dist\TrackLab.exe` (file singolo, ~100MB+).

Più lento al primo avvio (estrae in `/tmp`) ma più facile da
distribuire (1 solo file).

### Verifica versione nell'EXE
Dopo il build, click destro su `TrackLab.exe` →
Proprietà → Dettagli. Devi vedere la versione corrente (es.
`1.0.88.0` per v1088.0). Se non corrisponde, hai dimenticato di
aggiornare `version_info.txt` (vedi nota sotto).

⚠️ **Importante**: prima di buildare verifica SEMPRE che
`version.py` e `version_info.txt` siano allineati. Errore comune:
aggiornare `version.py` (es. `v1088.0`) ma lasciare
`version_info.txt` fermo (`1.0.86.6`). L'EXE prende la versione da
`version_info.txt`.

---

## 2. Linux — build binario

Pensato per: amici tester che usano Linux.

### Prerequisiti
Distribuzione raccomandata: **Ubuntu 22.04 LTS** (o derivate).

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

### Clone e setup
```bash
git clone https://github.com/PedroFerre27/TrackLab.git
cd TrackLab
git checkout vXXXX-stable    # ultimo tag stabile

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip wheel
pip install -r requirements.txt
pip install pyinstaller
```

### Build
```bash
rm -rf ~/.cache/pyinstaller build/ dist/ 2>/dev/null
pyinstaller tracklab_linux.spec --clean --noconfirm
```

Output: `dist/tracklab-advanced` (~100-130 MB, file singolo).

### Test
```bash
./dist/tracklab-advanced
```

Server (configurato di default): `https://api.choros27.synology.me`.
Le credenziali le fornisce Pedro.

### Installazione "vera" (opzionale)
```bash
mkdir -p ~/.local/bin
cp dist/tracklab-advanced ~/.local/bin/

mkdir -p ~/.local/share/icons/hicolor/256x256/apps
cp icons/app/taskbar_active.png \
   ~/.local/share/icons/hicolor/256x256/apps/tracklab.png

mkdir -p ~/.local/share/applications
cp tracklab.desktop ~/.local/share/applications/

update-desktop-database ~/.local/share/applications/
gtk-update-icon-cache ~/.local/share/icons/hicolor/ 2>/dev/null || true
```

Trovi "TrackLab" nelle Activities/menu app.

### Aggiornamenti
L'auto-update non funziona ancora su Linux. Aggiorna manualmente:
```bash
cd ~/TrackLab
git fetch --tags
git checkout vNUOVATAG-stable
source .venv/bin/activate
pyinstaller tracklab_linux.spec --clean --noconfirm
cp dist/tracklab-advanced ~/.local/bin/
```

---

## 3. macOS — build .app bundle

Pedro non ha un Mac fisico — vedi sezione 6 per le opzioni reali
(GitHub Actions, MacStadium, ecc.).

Se hai un Mac (Intel o Apple Silicon):

```bash
pip install pyinstaller customtkinter Pillow eyed3 mutagen \
    requests pyjwt musicbrainzngs
pyinstaller tracklab_macos.spec --clean
```

Output: `dist/TrackLab.app` (bundle macOS).

### Conversione PNG → ICNS (icona macOS)
Per avere l'icona "vera" su macOS serve un file `.icns`:

```bash
# Su macOS (sips e iconutil sono preinstallati)
mkdir tracklab.iconset
for size in 16 32 128 256 512; do
    sips -z $size $size icons/app/taskbar_active.png \
        --out tracklab.iconset/icon_${size}x${size}.png
    sips -z $((size*2)) $((size*2)) icons/app/taskbar_active.png \
        --out tracklab.iconset/icon_${size}x${size}@2x.png
done
cp icons/app/taskbar_active.png \
   tracklab.iconset/icon_512x512@2x.png
iconutil -c icns tracklab.iconset \
   -o icons/tracklab.icns
```

### Avvertenza distribuzione macOS
L'app non è firmata con un certificato Apple Developer
(richiede abbonamento $99/anno). Al primo avvio macOS dirà "App di
sviluppatore non identificato" e bloccherà l'apertura.

Istruzioni da dare all'utente:
1. Click destro sull'app in Finder
2. "Apri"
3. Conferma "Apri" nel popup

Dopo la prima volta macOS si ricorda e l'app si apre normalmente.

---

## 4. Distribuire il sorgente Python (opzione semplice)

Per il pilot tra amici tecnici, distribuire il sorgente è MOLTO più
semplice che buildare 3 binari. Loro fanno:

```bash
# Linux/macOS
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run_gui.py
```

```cmd
REM Windows
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python run_gui.py
```

Niente build, niente firme, niente Gatekeeper. Funziona su qualunque
OS con Python 3.11+. Per il pilot privato è la via più pragmatica.

---

## 5. Pubblicare una release (workflow completo)

Sequenza per pubblicare una versione stabile. Per i dettagli git
vedi VERSIONING.md.

### Step 1 — Bump versione (TUTTI i file in sincrono)
- `version.py` → `APP_VERSION = "v1088.0"`
- `version_info.txt` → `filevers=(1, 0, 88, 0)` + `FileVersion` +
  `ProductVersion` + commento iniziale
- `UPGRADES.md` → nuova sezione con changelog

Verifica:
```cmd
findstr APP_VERSION version.py
findstr filevers version_info.txt
```
DEVONO coincidere.

### Step 2 — Build EXE
```cmd
python build_ico.py
pyinstaller tracklab.spec --clean --noconfirm
```

### Step 3 — Verifica versione nell'EXE
Click destro su `dist\...\TrackLab.exe` → Proprietà
→ Dettagli. La FileVersion deve corrispondere.

### Step 4 — Calcola SHA256
```cmd
certutil -hashfile "dist\TrackLab\TrackLab.exe" SHA256
```
Copia l'hash (riga sotto al messaggio "SHA256 hash...").

### Step 5 — Carica l'EXE sul NAS
Carica l'EXE in `/volume1/docker/tracklab-server/data/releases/`
sul NAS (via DSM File Station, scp, o quello che preferisci).
Convenzione nome: `Music_Cataloger_vXXXX.exe`.

### Step 6 — Pubblica il manifest via API
```cmd
set SECRET=<la SECRET_KEY del server, presa dal .env del NAS>
curl -X POST https://api.choros27.synology.me/admin/version/publish ^
  -H "X-Admin-Token: %SECRET%" ^
  -H "Content-Type: application/json" ^
  -d "{\"version\":\"v1088.0\",\"filename\":\"Music_Cataloger_v1088.0.exe\",\"sha256\":\"<HASH-COPIATO>\",\"changelog\":\"- Chiusura branch security-audit\\n- Proxy lookup server-side\\n- README + SECURITY.md\",\"mandatory\":false}"
```

Verifica:
```cmd
curl -sk https://api.choros27.synology.me/version/latest
```
Deve mostrare la nuova versione.

### Step 7 — Git commit + tag
```cmd
git add .
git commit -m "v1088.0 - release stabile"
git tag v1088.0-stable -m "Pilot release v1088.0"
git push origin main
git push origin v1088.0-stable
```

### Step 8 — GitHub Release (opzionale ma utile)
Vai su https://github.com/PedroFerre27/TrackLab/releases/new
- Tag: `v1088.0-stable`
- Title: `TrackLab v1088.0`
- Description: changelog
- Attach: l'EXE buildato (trascinalo)
- Publish

Da qui il tuo tester Linux può scaricare il binario direttamente
da GitHub anziché passare dal NAS.

---

## 6. Note auto-update cross-platform

L'auto-updater attuale è **solo Windows** (usa un batch file per
swap EXE atomico). Per Linux/macOS gli utenti devono aggiornare
manualmente.

Estendere l'auto-update a Linux/macOS richiede un meccanismo
diverso: replace binario atomico + restart. È in roadmap come
**R7 — Auto-update Linux/macOS** (ROADMAP.md, P2).

### Build macOS senza un Mac fisico

Le opzioni reali, in ordine di praticità:

1. **GitHub Actions con runner macOS** (raccomandato per
   distribuzione seria): workflow CI che builda l'app `.app`/`.dmg`
   su un Mac Apple-hosted a ogni tag. Nessun Mac fisico
   necessario, legale, ripetibile.
2. **Servizi Mac in cloud** (MacStadium, MacInCloud): Mac reali a
   noleggio, a pagamento.
3. **Un amico/tester con un Mac** che builda da sorgente seguendo
   la sezione 3.
4. **Distribuire il sorgente** (sezione 4): per il pilot tra amici
   tecnici è la via più pragmatica.

**Avvertenza onesta**: VM macOS su hardware non-Apple (es.
OSX-KVM) funzionano tecnicamente ma **violano l'EULA Apple**. Non
è una via legale né affidabile per distribuire software. Lo
menziono solo per chiarezza: non è una raccomandazione.

Per il pilot: opzione 4 (sorgente) o 3 (tester con Mac).
Per distribuzione seria: opzione 1 (GitHub Actions) abbinata alla
firma EXE (R8 in roadmap).
