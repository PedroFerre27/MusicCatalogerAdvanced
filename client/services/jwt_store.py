"""
services/jwt_store.py — Persistenza locale dei JWT token.

Salva access + refresh token in un file JSON nella cartella dati utente.
Il file viene scritto con permessi restrittivi (0600 su Unix). Su Windows
NTFS rispetta le ACL dell'utente corrente automaticamente.

NOTA SICUREZZA: i token sono memorizzati in chiaro sul disco utente.
Chi ha accesso al profilo Windows dell'utente può leggerli. Questo è
accettabile per il pilot (stesso livello di sicurezza di un cookie
"remember me" di un browser). Un domani — post-pilot — si può integrare
Windows Credential Manager via `keyring` per storage cifrato nativo.
"""
from __future__ import annotations
import json
import os
import stat
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _get_data_dir() -> Path:
    """Ritorna la cartella dati — stessa logica di altri moduli del progetto.

    v1085m: in PyInstaller bundle (onefile/onedir), `__file__` è dentro
    `_MEI<random>` che viene cancellata ad ogni avvio. Per persistenza
    della sessione, usiamo la dir dell'EXE quando in bundle.
    """
    import sys
    if getattr(sys, "frozen", False):
        base = Path(sys.executable).parent / "data"
    else:
        base = Path(__file__).parent.parent / "data"
    base.mkdir(parents=True, exist_ok=True)
    return base


TOKEN_FILE = _get_data_dir() / "session.json"


@dataclass
class StoredSession:
    """Una sessione utente persistita. `saved_at` serve per decidere se
    il refresh token è ancora verosimilmente valido prima di fare rete."""
    access_token:  str
    refresh_token: str
    server_url:    str            # es. "https://api.choros27.synology.me"
    user_email:    str
    user_plan:     str
    saved_at:      str            # ISO 8601 UTC


class JwtStore:
    """Singleton di convenienza per salvare/leggere la sessione corrente."""

    def __init__(self, path: Path = TOKEN_FILE):
        self._path = path
        self._cached: Optional[StoredSession] = None

    def save(self, session: StoredSession) -> None:
        """Serializza e scrive. Permessi 0600 su Unix."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data = asdict(session)
        data["saved_at"] = datetime.now(timezone.utc).isoformat()
        self._path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        # Permessi restrittivi (no-op su Windows ma non rompe)
        try:
            os.chmod(self._path, stat.S_IRUSR | stat.S_IWUSR)
        except Exception:
            pass
        self._cached = StoredSession(**data)

    def load(self) -> Optional[StoredSession]:
        if self._cached is not None:
            return self._cached
        if not self._path.exists():
            return None
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            self._cached = StoredSession(**data)
            return self._cached
        except Exception:
            # File corrotto o schema cambiato — scarta e forza login
            return None

    def clear(self) -> None:
        """Cancella la sessione (logout)."""
        self._cached = None
        try:
            if self._path.exists():
                self._path.unlink()
        except Exception:
            pass

    def update_access_token(self, new_access: str) -> None:
        """Aggiorna solo l'access token dopo un refresh riuscito."""
        s = self.load()
        if s is None:
            return
        s.access_token = new_access
        s.saved_at = datetime.now(timezone.utc).isoformat()
        self.save(s)


# Istanza globale
store = JwtStore()
