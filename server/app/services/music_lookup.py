"""
services/music_lookup.py — Proxy lookup metadati musicali (v0.2.3)

SCOPO (security-audit Fase 2): spostare server-side le chiamate alle
API musicali che richiedono token segreti. Prima il client chiamava
direttamente Discogs/Last.fm/Spotify/GetSong con i token hardcoded
nel suo `config/secrets.py` — estraibili da chiunque decompilasse
l'EXE. Ora il client chiama `/api/v1/lookup` e il SERVER fa la
chiamata con i propri token (in `.env`, mai esposti).

Questo modulo replica FEDELMENTE la logica di parsing/normalizzazione
che era in client `services/external_apis.py`, così il formato del
dict ritornato e' identico a prima → il client non deve cambiare il
modo in cui interpreta i risultati.

Provider supportati:
  - lastfm    → ws.audioscrobbler.com (genere, durata, playcount)
  - spotify   → OAuth client_credentials + /v1/search (cover, anno)
  - discogs   → api.discogs.com/database/search (genere/stile)
  - getsong   → api.getsong.co (BPM)

Token letti da settings (env). Se un token manca, la funzione
ritorna None (graceful) e il client fa fallback agli altri provider
pubblici (iTunes/MusicBrainz/Deezer) che NON richiedono auth.

NB: questo e' PASSTHROUGH puro. Nessuna cache server-side (decisione
di scope: la cache resta client-side in local_db.json; il community
DB centralizzato sara' un branch dedicato futuro).
"""
from __future__ import annotations

import base64
import logging
from typing import Optional, Dict, List

# v0.2.3 fix: import difensivo. Se per qualsiasi ragione `requests`
# non fosse disponibile nell'immagine, il server NON deve crashare
# all'avvio (auth/catalog/health devono restare su). Il proxy lookup
# semplicemente ritornera' None per ogni provider (il client fa
# fallback a iTunes/MusicBrainz/Deezer).
try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

from ..config import settings

logger = logging.getLogger("music_lookup")

# Timeout uniforme per tutte le chiamate esterne (secondi)
_HTTP_TIMEOUT = 10
_USER_AGENT = f"MusicCatalogerServer/{settings.APP_VERSION}"


# ─────────────────────────────────────────────────────────────────
#  Helper genere (porting da client _select_primary_genre)
# ─────────────────────────────────────────────────────────────────
def _select_primary_genre(genres: List[str]) -> Optional[str]:
    """Sceglie il genere principale da una lista. Porting semplificato
    della logica client: primo genere non generico."""
    GENERIC = {"music", "musica", "other", "unknown", "various", ""}
    for g in genres:
        if g and g.strip().lower() not in GENERIC:
            return g.strip()
    return genres[0].strip() if genres else None


# ─────────────────────────────────────────────────────────────────
#  LAST.FM
# ─────────────────────────────────────────────────────────────────
def _lookup_lastfm(artist: str, title: str) -> Optional[Dict]:
    api_key = settings.LASTFM_API_KEY
    if not api_key:
        logger.debug("Last.fm API key non configurata server-side")
        return None
    try:
        params = {
            "method": "track.getInfo",
            "artist": artist,
            "track": title,
            "api_key": api_key,
            "format": "json",
        }
        r = requests.get("https://ws.audioscrobbler.com/2.0/",
                          params=params, timeout=_HTTP_TIMEOUT)
        if r.status_code != 200:
            logger.debug(f"Last.fm HTTP {r.status_code}")
            return None
        data = r.json()
        if "track" not in data or "error" in data:
            return None
        track = data["track"]
        metadata: Dict = {
            "title": track.get("name"),
            "artist": (track.get("artist", {}).get("name")
                       if isinstance(track.get("artist"), dict) else artist),
        }
        if track.get("album"):
            metadata["album"] = track["album"].get("title")
        if track.get("duration"):
            try:
                d = int(track["duration"])
                if d > 0:
                    metadata["duration"] = d / 1000.0
            except (ValueError, TypeError):
                pass
        # Generi dai tag
        genres: List[str] = []
        toptags = track.get("toptags", {}).get("tag", [])
        if isinstance(toptags, list):
            genres = [t.get("name") for t in toptags if t.get("name")]
        if genres:
            uniq = list(dict.fromkeys(genres))
            primary = _select_primary_genre(uniq)
            if primary:
                metadata["genre"] = primary
            metadata["all_genres"] = uniq[:5]
        if track.get("playcount"):
            try:
                pc = int(track["playcount"])
                metadata["playcount"] = pc
                metadata["popularity"] = (
                    "high" if pc > 1_000_000 else
                    "medium" if pc > 100_000 else "low")
            except (ValueError, TypeError):
                pass
        if track.get("url"):
            metadata["lastfm_url"] = track["url"]
        cleaned = {k: v for k, v in metadata.items()
                   if v is not None and str(v).strip()}
        return cleaned or None
    except Exception as e:
        logger.debug(f"Last.fm error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────
#  SPOTIFY  (OAuth client_credentials)
# ─────────────────────────────────────────────────────────────────
def _lookup_spotify(artist: str, title: str) -> Optional[Dict]:
    client_id = settings.SPOTIFY_CLIENT_ID
    client_secret = settings.SPOTIFY_CLIENT_SECRET
    if not client_id or not client_secret:
        logger.debug("Credenziali Spotify non configurate server-side")
        return None
    try:
        auth_b64 = base64.b64encode(
            f"{client_id}:{client_secret}".encode()).decode()
        tok = requests.post(
            "https://accounts.spotify.com/api/token",
            headers={"Authorization": f"Basic {auth_b64}",
                     "Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "client_credentials"},
            timeout=_HTTP_TIMEOUT)
        if tok.status_code != 200:
            logger.debug(f"Spotify token error {tok.status_code}")
            return None
        access_token = tok.json().get("access_token")
        if not access_token:
            return None
        sr = requests.get(
            "https://api.spotify.com/v1/search",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"q": f'artist:"{artist}" track:"{title}"',
                    "type": "track", "limit": 1},
            timeout=_HTTP_TIMEOUT)
        if sr.status_code != 200:
            logger.debug(f"Spotify search error {sr.status_code}")
            return None
        tracks = sr.json().get("tracks", {}).get("items", [])
        if not tracks:
            return None
        t = tracks[0]
        metadata = {
            "title": t.get("name"),
            "artist": t["artists"][0]["name"] if t.get("artists") else None,
            "album": t["album"]["name"] if t.get("album") else None,
            "year": (t["album"]["release_date"][:4]
                     if t.get("album", {}).get("release_date") else None),
            "track_num": (str(t.get("track_number"))
                          if t.get("track_number") else None),
            "duration": (t.get("duration_ms", 0) / 1000.0
                         if t.get("duration_ms") else None),
            "popularity": t.get("popularity"),
            "spotify_url": t.get("external_urls", {}).get("spotify"),
        }
        imgs = t.get("album", {}).get("images") or []
        if imgs:
            metadata["cover_url"] = imgs[0].get("url")
        return {k: v for k, v in metadata.items()
                if v is not None and str(v).strip()} or None
    except Exception as e:
        logger.debug(f"Spotify error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────
#  DISCOGS
# ─────────────────────────────────────────────────────────────────
def _lookup_discogs(artist: str, title: str) -> Optional[Dict]:
    token = settings.DISCOGS_TOKEN
    if not token:
        logger.debug("Discogs token non configurato server-side")
        return None
    try:
        r = requests.get(
            "https://api.discogs.com/database/search",
            params={"artist": artist, "track": title,
                    "token": token, "per_page": 1, "type": "release"},
            headers={"User-Agent": _USER_AGENT},
            timeout=_HTTP_TIMEOUT)
        if r.status_code != 200:
            logger.debug(f"Discogs HTTP {r.status_code}")
            return None
        results = r.json().get("results", [])
        if not results:
            return None
        res = results[0]
        metadata: Dict = {"title": title, "artist": artist}
        if res.get("year"):
            metadata["year"] = str(res["year"])
        # Discogs genre/style
        genres: List[str] = []
        if res.get("genre"):
            genres.extend(res["genre"])
        if res.get("style"):
            genres.extend(res["style"])
        if genres:
            uniq = list(dict.fromkeys(genres))
            primary = _select_primary_genre(uniq)
            if primary:
                metadata["genre"] = primary
            metadata["all_genres"] = uniq[:5]
        if res.get("cover_image"):
            metadata["cover_url"] = res["cover_image"]
        return {k: v for k, v in metadata.items()
                if v is not None and str(v).strip()} or None
    except Exception as e:
        logger.debug(f"Discogs error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────
#  GETSONG  (BPM)
# ─────────────────────────────────────────────────────────────────
def _lookup_getsong(artist: str, title: str) -> Optional[Dict]:
    api_key = settings.GETSONG_API_KEY
    if not api_key:
        logger.debug("GetSong API key non configurata server-side")
        return None
    try:
        r = requests.get(
            "https://api.getsong.co/search/",
            params={"api_key": api_key, "type": "both",
                    "lookup": f"artist:{artist} song:{title}"},
            headers={"User-Agent": _USER_AGENT},
            timeout=_HTTP_TIMEOUT)
        if r.status_code == 403:
            logger.warning("GetSong API: accesso negato (403)")
            return None
        if r.status_code != 200:
            logger.debug(f"GetSong HTTP {r.status_code}")
            return None
        data = r.json()
        bpm = None
        if isinstance(data, dict):
            if data.get("search") and len(data["search"]) > 0:
                res = data["search"][0]
                if res.get("tempo"):
                    bpm = int(float(res["tempo"]))
            elif data.get("tempo"):
                bpm = int(float(data["tempo"]))
        if bpm and 40 <= bpm <= 250:   # validazione range plausibile
            return {"bpm": bpm, "artist": artist, "title": title}
        return None
    except Exception as e:
        logger.debug(f"GetSong error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────
#  Dispatcher
# ─────────────────────────────────────────────────────────────────
_PROVIDERS = {
    "lastfm":  _lookup_lastfm,
    "spotify": _lookup_spotify,
    "discogs": _lookup_discogs,
    "getsong": _lookup_getsong,
}

SUPPORTED_PROVIDERS = tuple(_PROVIDERS.keys())


def lookup(provider: str, artist: str, title: str) -> Optional[Dict]:
    """Esegue il lookup per il provider richiesto. Ritorna il dict
    metadati normalizzato (stesso formato del vecchio client) o None
    se il provider non ha risultati / token mancante / errore /
    libreria requests non disponibile."""
    if requests is None:
        logger.warning(
            "Libreria 'requests' non disponibile — proxy lookup disabilitato. "
            "Aggiungi requests a requirements.txt e ricostruisci l'immagine.")
        return None
    fn = _PROVIDERS.get(provider)
    if fn is None:
        return None
    return fn(artist, title)
