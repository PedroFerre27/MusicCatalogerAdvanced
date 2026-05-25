"""
services/spotify_store.py — Persistenza locale del token OAuth Spotify
dell'utente.

v1089.0 (R4 predisposizione): salva access + refresh token in un file
JSON nella cartella dati utente, stessa logica di `jwt_store.py`.

NOTA SICUREZZA: come `jwt_store.py`, il token e' salvato in chiaro sul
disco utente. Chi ha accesso al profilo Windows dell'utente puo'
leggerlo. Accettabile per il pilot. Quando in futuro si introdurra'
storage cifrato nativo (Windows Credential Manager via `keyring`)
entrambi i moduli passeranno alla stessa API.

Differenze sostanziali da jwt_store:
- Spotify access token scade in 3600s (1h) → serve scadenza esplicita
  e check di validita' prima di ogni chiamata API.
- Lo scope concesso dall'utente puo' essere meno di quello richiesto
  → salviamo lo scope effettivo restituito da Spotify.
"""
from __future__ import annotations
import json
import os
import stat
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional


def _get_data_dir() -> Path:
    """Stessa logica di jwt_store._get_data_dir (bundle-safe)."""
    import sys
    if getattr(sys, "frozen", False):
        base = Path(sys.executable).parent / "data"
    else:
        base = Path(__file__).parent.parent / "data"
    base.mkdir(parents=True, exist_ok=True)
    return base


TOKEN_FILE = _get_data_dir() / "spotify_token.json"


@dataclass
class SpotifyToken:
    """
    Token OAuth Spotify persistito.

    expires_at: ISO 8601 UTC. Calcolato come `now + expires_in` al
    momento del salvataggio. Permette di sapere se rifare il refresh
    senza chiamare l'API.

    user_*: dati di profilo recuperati al momento del primo collegamento
    (via /v1/me, scope user-read-private/email). Usati solo per
    visualizzazione nelle Impostazioni ("Collegato come ..."). Possono
    essere None se l'utente ha concesso scope diversi.
    """
    access_token:  str
    refresh_token: str
    expires_at:    str             # ISO 8601 UTC
    scope:         str             # space-separated string come da Spotify
    token_type:    str = "Bearer"
    user_id:       Optional[str] = None
    user_email:    Optional[str] = None
    user_display_name: Optional[str] = None
    saved_at:      str = ""        # ISO 8601 UTC, valorizzato in save()


class SpotifyStore:
    """Singleton di convenienza per salvare/leggere il token Spotify."""

    def __init__(self, path: Path = TOKEN_FILE):
        self._path = path
        self._cached: Optional[SpotifyToken] = None

    def save(self, token: SpotifyToken) -> None:
        """Serializza e scrive con permessi restrittivi (0600 su Unix)."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data = asdict(token)
        data["saved_at"] = datetime.now(timezone.utc).isoformat()
        self._path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        try:
            os.chmod(self._path, stat.S_IRUSR | stat.S_IWUSR)
        except Exception:
            pass
        self._cached = SpotifyToken(**data)

    def load(self) -> Optional[SpotifyToken]:
        if self._cached is not None:
            return self._cached
        if not self._path.exists():
            return None
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            self._cached = SpotifyToken(**data)
            return self._cached
        except Exception:
            # File corrotto o schema cambiato — scarta, l'utente
            # dovra' ricollegare l'account.
            return None

    def clear(self) -> None:
        """Cancella il token (disconnessione Spotify)."""
        self._cached = None
        try:
            if self._path.exists():
                self._path.unlink()
        except Exception:
            pass

    def is_expired(self, skew_seconds: int = 60) -> bool:
        """
        True se l'access token e' scaduto (o sta per scadere entro
        `skew_seconds`). Usato come trigger per il refresh.
        """
        tok = self.load()
        if tok is None:
            return True
        try:
            exp = datetime.fromisoformat(tok.expires_at)
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            return datetime.now(timezone.utc) + timedelta(
                seconds=skew_seconds) >= exp
        except Exception:
            return True

    def update_after_refresh(self, new_access: str, expires_in: int,
                             new_refresh: Optional[str] = None,
                             new_scope: Optional[str] = None) -> None:
        """
        Aggiorna i campi dopo un refresh riuscito.

        Spotify normalmente NON ritorna un nuovo refresh_token al
        refresh (ma puo' farlo, e in quel caso va aggiornato). Idem per
        lo scope: se l'utente ha downgrade-ato i permessi via Spotify
        web settings, lo scope nel refresh puo' essere ridotto.
        """
        tok = self.load()
        if tok is None:
            return
        tok.access_token = new_access
        tok.expires_at = (datetime.now(timezone.utc) +
                          timedelta(seconds=expires_in)).isoformat()
        if new_refresh:
            tok.refresh_token = new_refresh
        if new_scope:
            tok.scope = new_scope
        self.save(tok)

    def is_connected(self) -> bool:
        """True se esiste un token (anche se scaduto: il refresh
        decidera' se ancora utilizzabile)."""
        return self.load() is not None


# Istanza globale (usata dalla UI e da external_apis)
store = SpotifyStore()
