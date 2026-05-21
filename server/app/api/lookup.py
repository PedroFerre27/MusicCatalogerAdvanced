"""
api/lookup.py — Endpoint proxy lookup metadati (v0.2.3)

GET /api/v1/lookup?provider=<p>&artist=<a>&title=<t>

Sostituisce le chiamate dirette del client a Discogs/Last.fm/
Spotify/GetSong. Il client manda artist+title+provider, il server
fa la chiamata con i propri token (settings, da .env) e ritorna il
dict metadati normalizzato — STESSO formato che il client si
aspettava dalle chiamate dirette, così non cambia il parsing client.

Autenticazione: richiede JWT valido (get_current_user). Un utente
non loggato non puo' usare il proxy → niente abuso anonimo dei
token del server.

Rate limit: tecnico globale anti-abuso (120/min per IP). NON e' un
gating per-piano (decisione di scope: lookup illimitati per tutti i
piani; il gating "DB online" e' gia' applicato a livello di
catalogazione in catalog.py::_check_options_against_plan).
"""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Query

from ..models.db import User
from ..services.auth import get_current_user
from ..services.ratelimit import limiter
from ..services import music_lookup

router = APIRouter(prefix="/api/v1", tags=["lookup"])


@router.get("/lookup")
@limiter.limit("120/minute")
def lookup_metadata(
    request: Request,
    provider: str = Query(..., description="lastfm|spotify|discogs|getsong"),
    artist: str = Query(..., min_length=1, max_length=300),
    title: str = Query(..., min_length=1, max_length=300),
    user: User = Depends(get_current_user),
):
    """Proxy autenticato verso le API musicali con token server-side.

    Ritorna:
      - 200 + {found: true, data: {...}}  se il provider ha risultati
      - 200 + {found: false, data: null} se nessun risultato (NON 404:
        "nessun match" e' un esito normale, non un errore)
      - 400 se provider non supportato
      - 401 se JWT mancante/scaduto (via get_current_user)
      - 429 se rate limit superato
    """
    provider = (provider or "").strip().lower()
    if provider not in music_lookup.SUPPORTED_PROVIDERS:
        raise HTTPException(
            status_code=400,
            detail=(f"Provider '{provider}' non supportato. "
                    f"Validi: {', '.join(music_lookup.SUPPORTED_PROVIDERS)}"),
        )
    result = music_lookup.lookup(provider, artist.strip(), title.strip())
    return {
        "found": result is not None,
        "provider": provider,
        "data": result,
    }
