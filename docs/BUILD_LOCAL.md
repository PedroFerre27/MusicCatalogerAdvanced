# BUILD_LOCAL.md — Build locale di TrackLab (tutte le piattaforme)

Guida per buildare gli eseguibili TrackLab **in locale**, senza il
workflow GitHub Actions. Utile per iterazione veloce su bug specifici
di una piattaforma prima di pubblicare un tag.

> **Nota**: per i rilasci normali NON serve buildare in locale. Il
> push di un tag `client/vXXXX.X-stable` fa partire i workflow GitHub
> Actions che producono `.dmg` (macOS) e `.AppImage` (Linux)
> automaticamente, allegandoli alla Release. Vedi
> `.github/workflows/build-macos.yml` e `build-linux.yml`.

---

## Riepilogo formati per piattaforma

| OS | Formato | Spec PyInstaller | Build CI |
|----|---------|------------------|----------|
| Windows | `TrackLab.exe` (onefile) | `tracklab.spec` | locale (Pedro) |
| macOS | `TrackLab.app` → `.dmg` | `tracklab_macos.spec` | GitHub Actions |
| Linux | binario → `.AppImage` | `tracklab_linux.spec` | GitHub Actions |

---

## Windows (build locale — quello che usa Pedro)

Prerequisiti: Python 3.13 + dipendenze installate.

```bat
cd C:\dev\tracklab\client
python build_ico.py
pyinstaller tracklab.spec --clean --noconfirm
:: Output: dist\TrackLab.exe
```

Per pubblicare: carica `dist\TrackLab.exe` su NAS releases/ + aggiorna
version.json via endpoint admin (vedi server/app/api/updates.py).

---

## Linux — Opzione A: Docker (raccomandata, portabile)

Funziona su Windows (Docker Desktop), Mac, e NAS Synology con Docker.

### Setup una tantum

```bash
cd <repo>/client
docker build -f Dockerfile.linux-build -t tracklab-linux-builder .
```

### Build ogni volta (~2-3 min)

```bash
# Windows (PowerShell/CMD)
mkdir dist 2>nul
docker run --rm -v "%cd%:/src" -v "%cd%\dist:/out" tracklab-linux-builder

# Linux / Mac / NAS
mkdir -p dist
docker run --rm -v "$PWD:/src" -v "$PWD/dist:/out" tracklab-linux-builder
```

Output: `client/dist/TrackLab-<versione>-linux-x86_64.AppImage`

### Test (richiede Linux con GUI)

```bash
chmod +x TrackLab-*.AppImage
./TrackLab-*.AppImage
```

---

## Linux — Opzione B: WSL2 su Windows

Setup una tantum:

```powershell
# PowerShell ADMIN
wsl --install -d Ubuntu-22.04
# riavvia Windows
```

Dentro WSL2 Ubuntu:

```bash
sudo apt update && sudo apt install -y \
  python3.11 python3-pip python3-tk libfuse2 wget file
wget https://github.com/AppImage/AppImageKit/releases/download/continuous/appimagetool-x86_64.AppImage \
  -O ~/appimagetool && chmod +x ~/appimagetool

cd /mnt/c/dev/tracklab/client
pip install -r requirements.txt pyinstaller Pillow
pyinstaller tracklab_linux.spec --clean --noconfirm
# Poi gli step di AppDir staging (vedi build-linux.yml step "Build AppImage")
```

Più semplice: usa l'Opzione A (Docker) anche dentro WSL2.

---

## Linux — Opzione C: direttamente sul NAS Synology

Il NAS DS415+ ha Docker installato (stesso che fa girare TrackLab
Server). Si può buildare lì.

```bash
ssh root@<ip-nas>

# Clone repo in cartella separata dai dati live del server
mkdir -p /volume1/docker/tracklab-build
cd /volume1/docker/tracklab-build
git clone https://github.com/PedroFerre27/TrackLab.git .

cd client
docker build -f Dockerfile.linux-build -t tracklab-linux-builder .
mkdir -p dist
docker run --rm -v "$PWD:/src" -v "$PWD/dist:/out" tracklab-linux-builder

# Output in /volume1/docker/tracklab-build/client/dist/
# Scarica su PC via SMB/SFTP per il test
```

**Nota**: la CPU Atom del DS415+ è lenta, build ~10-15 min vs ~3 min
su un PC moderno. Funziona ma con pazienza.

---

## macOS — build locale

Serve un Mac fisico (Pedro non ne ha → usa GitHub Actions). Se hai
accesso temporaneo a un Mac:

```bash
cd <repo>/client
pip install -r requirements.txt pyinstaller Pillow
pyinstaller tracklab_macos.spec --clean --noconfirm
# Output: dist/TrackLab.app

# Crea .dmg
hdiutil create -volname "TrackLab" -srcfolder dist/TrackLab.app \
  -ov -format UDZO dist/TrackLab.dmg
```

L'app NON è firmata → primo avvio: click destro → Apri → conferma.

---

## Note tecniche comuni

- **UPGRADES.md**: è gitignored (doc interna privata). Gli spec lo
  bundlano SOLO se esiste sul filesystem (build locale di Pedro sì,
  CI cloud no). Il dialog Help → Changelog gestisce graceful il file
  mancante. Vedi commento in ciascuno spec.
- **Icona**: generata automaticamente dagli spec.
  - Windows: `tracklab.ico` da `build_ico.py` o inline nello spec
  - macOS: `tracklab.icns` generato via Pillow nello spec
  - Linux: `app_icon_256.png` usato direttamente nell'AppImage
- **jwt**: NON è un hidden import (il client decodifica i JWT
  manualmente con base64). Non aggiungerlo agli spec.
- **Architettura**: i build CI sono single-arch:
  - macOS: arm64 (Apple Silicon). Per Intel servirebbe job `macos-13`.
  - Linux: x86_64. Per arm64 servirebbe job `ubuntu-22.04-arm`.
