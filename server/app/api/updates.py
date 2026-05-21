"""
api/updates.py — Endpoint per auto-update del client EXE

GET  /version/latest             → {version, exe_url, sha256, changelog, mandatory}
GET  /downloads/{filename}       → serve il file EXE dal volume /srv/app/data/releases

DEPLOYMENT NAS:
- L'admin carica gli EXE in /volume1/docker/music-cataloger/data/releases/
- Aggiorna /volume1/docker/music-cataloger/data/version.json con i metadati
- Il client al boot fa check e si auto-aggiorna

version.json (formato):
{
  "version":   "v1085",
  "filename":  "Music_Cataloger_Advanced_v1085.exe",
  "sha256":    "abc123...",
  "changelog": "- Fix bug X\n- Nuova feature Y",
  "mandatory": false,
  "released_at": "2026-04-25T10:00:00Z"
}

Il file `version.json` è OPZIONALE: se manca, l'endpoint /version/latest
ritorna 404 e il client capisce "nessun update disponibile".
"""
import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel

from ..config import settings

router = APIRouter(tags=["updates"])

# Path dove l'admin sul NAS copia gli EXE rilasciati.
# Risolto in assoluto per evitare ambiguità rispetto al CWD del processo.
RELEASES_DIR = (settings.DATA_DIR / "releases").resolve()
VERSION_FILE = (settings.DATA_DIR / "version.json").resolve()


class VersionInfo(BaseModel):
    version:     str
    exe_url:     str  = ""    # URL completo da cui scaricare (deprecato in v0.2.0)
    filename:    str  = ""    # nome file Windows .exe (back-compat)
    # v0.2.0: filename per piattaforma (preferito a `filename` quando presente)
    filename_windows: str = ""
    filename_linux:   str = ""
    filename_macos:   str = ""
    sha256:           str  = ""
    sha256_windows:   str  = ""
    sha256_linux:     str  = ""
    sha256_macos:     str  = ""
    changelog:   str  = ""
    mandatory:   bool = False
    released_at: Optional[str] = None


@router.get("/version/latest", response_model=VersionInfo)
def latest_version():
    """
    Ritorna info sull'ultima release del client.
    404 se non c'è ancora nessuna release pubblicata, o se il file è
    illeggibile/corrotto. Quest'ultimo caso è trattato come "no release"
    perché il client non ha modo di gestire un 500 in modo utile e
    finirebbe per spammare l'errore al boot di ogni utente.
    """
    if not VERSION_FILE.exists():
        raise HTTPException(404, "Nessuna release pubblicata")
    if not VERSION_FILE.is_file():
        # Es. è una directory creata per errore con `mkdir -p`
        print(f"[updates] WARNING: {VERSION_FILE} non è un file regolare")
        raise HTTPException(404, "Nessuna release pubblicata (file non valido)")
    try:
        data = json.loads(VERSION_FILE.read_text(encoding="utf-8"))
    except (PermissionError, OSError) as e:
        # File esiste ma non leggibile (UID/GID o ACL)
        print(f"[updates] WARNING: impossibile leggere {VERSION_FILE}: {e}")
        raise HTTPException(404, "Nessuna release pubblicata (errore di lettura)")
    except json.JSONDecodeError as e:
        print(f"[updates] WARNING: {VERSION_FILE} JSON malformato: {e}")
        raise HTTPException(404, "Nessuna release pubblicata (file malformato)")

    # Compone exe_url assoluto se manca
    if "exe_url" not in data and data.get("filename"):
        # In produzione dietro reverse proxy, il client ha già il base URL
        # corretto. Qui ritorniamo un path relativo "/downloads/<file>"
        # che il client risolverà con il proprio server_url.
        data["exe_url"] = f"/downloads/{data['filename']}"

    return VersionInfo(**data)


@router.get("/downloads/{filename}")
def download_release(filename: str):
    """
    Serve il file EXE dal volume releases. Solo nomi file semplici
    (no path traversal con ../).
    """
    # Sanity: niente path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(400, "Nome file non valido")
    if not filename.endswith((".exe", ".zip", ".dmg", ".AppImage", ".tar.gz")):
        raise HTTPException(400, "Estensione non consentita")

    file_path = RELEASES_DIR / filename
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(404, f"File non trovato: {filename}")

    return FileResponse(
        path=str(file_path),
        media_type="application/octet-stream",
        filename=filename,
    )


# ── v0.2.0 — Admin: pubblica nuova release via API ──────────────
class PublishVersion(BaseModel):
    version:   str
    filename:  str
    sha256:    Optional[str] = None
    changelog: str = ""
    mandatory: bool = False


@router.post("/admin/version/publish")
def admin_publish_version(
    body: PublishVersion,
    request: Request,
):
    """
    Admin pubblica metadata nuova release. Risolve i problemi di
    permessi UID che si avevano quando Pedro creava version.json
    direttamente via DSM File Station (UID 0 = admin DSM, container
    gira come UID 1000 → PermissionError).

    Workflow corretto:
    1. Carica EXE in `data/releases/` (può essere UID admin, lì basta read)
    2. Calcola SHA256 (`certutil` su Win, `sha256sum` su Linux)
    3. Chiama questo endpoint → server scrive version.json con UID 1000
       (cioè scrittura interna al container = sempre permessa)

    NB: l'autenticazione qui è basic-token via header per semplificare
    chiamate da curl/script. In v0.2.0 accettiamo solo l'header
    `X-Admin-Token` con valore == settings.SECRET_KEY (il primo che
    riesce a leggere SECRET_KEY è già admin del sistema).
    """
    from fastapi import Header
    auth = request.headers.get("X-Admin-Token", "")
    if not auth or auth != settings.SECRET_KEY:
        raise HTTPException(401, "X-Admin-Token mancante o errato")

    # Verifica che l'EXE esista in releases/
    exe_path = (RELEASES_DIR / body.filename).resolve()
    if not exe_path.exists():
        raise HTTPException(
            422,
            f"EXE '{body.filename}' non trovato in {RELEASES_DIR}. "
            f"Caricalo prima di pubblicare il manifest.")
    if not str(exe_path).startswith(str(RELEASES_DIR)):
        raise HTTPException(422, "Path traversal rilevato")

    # Scrivi version.json (con UID del container = sempre OK)
    payload = {
        "version":   body.version,
        "filename":  body.filename,
        "sha256":    body.sha256 or "",
        "changelog": body.changelog,
        "mandatory": body.mandatory,
    }
    VERSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    VERSION_FILE.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8")
    return {"ok": True, "version": body.version,
            "path": str(VERSION_FILE)}
