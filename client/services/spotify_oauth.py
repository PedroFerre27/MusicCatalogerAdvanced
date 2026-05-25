"""
services/spotify_oauth.py — Flusso OAuth Authorization Code + PKCE per
Spotify user-side.

v1089.0 (R4 predisposizione): implementa il flusso completo end-to-end.
Inerte se `settings.spotify_oauth.client_id` e' vuoto (la UI non
chiama mai questo modulo in quel caso). Quando Pedro avra' l'app
Spotify Developer pronta, basta valorizzare CLIENT_ID e tutto si
attiva senza modifiche al codice.

Flusso (RFC 7636 - PKCE):
  1. genera code_verifier (43-128 char) + code_challenge (S256)
  2. apre browser su https://accounts.spotify.com/authorize?...
  3. avvia mini server HTTP su 127.0.0.1:<porta>
  4. utente acconsente → Spotify redireziona a 127.0.0.1/callback?code=...
  5. il server cattura il code, lo scambia con POST a /api/token
  6. ritorna access_token + refresh_token

PKCE invece di Client Secret: il client desktop non puo' custodire
secret (sarebbe estratto dall'EXE), PKCE risolve dimostrando che chi
scambia il code e' la stessa entita' che ha iniziato il flusso.

Sicurezza:
- `state` random per prevenire CSRF (controllato al callback)
- timeout configurabile sul callback (default 120s)
- niente refresh_token in URL/log; access_token redatto nei log
"""
from __future__ import annotations

import base64
import hashlib
import http.server
import json
import logging
import secrets
import socket
import threading
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

from config.settings import settings
from services.spotify_store import SpotifyToken, store as spotify_store

logger = logging.getLogger(__name__)


AUTHORIZE_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL     = "https://accounts.spotify.com/api/token"
ME_URL        = "https://api.spotify.com/v1/me"


# ── Eccezioni dedicate ──────────────────────────────────────────────

class SpotifyOAuthError(Exception):
    """Errore generico nel flusso OAuth Spotify."""

class SpotifyNotConfigured(SpotifyOAuthError):
    """`client_id` non configurato. Vuol dire che la feature non e'
    ancora stata attivata (predisposizione v1089.0)."""

class SpotifyOAuthCancelled(SpotifyOAuthError):
    """L'utente ha negato il consenso o ha chiuso il browser."""

class SpotifyOAuthTimeout(SpotifyOAuthError):
    """Callback non ricevuto entro `callback_timeout` secondi."""


# ── PKCE helpers ────────────────────────────────────────────────────

def _generate_pkce_pair() -> Tuple[str, str]:
    """
    Genera (code_verifier, code_challenge) per PKCE S256.

    code_verifier: 64 char URL-safe random.
    code_challenge: base64url(sha256(verifier)) senza padding.
    """
    verifier = secrets.token_urlsafe(64)[:64]    # esattamente 64 char
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return verifier, challenge


def _build_authorize_url(client_id: str, redirect_uri: str, scope: str,
                        state: str, code_challenge: str) -> str:
    """Costruisce l'URL di autorizzazione Spotify."""
    params = {
        "client_id":             client_id,
        "response_type":         "code",
        "redirect_uri":          redirect_uri,
        "state":                 state,
        "scope":                 scope,
        "code_challenge_method": "S256",
        "code_challenge":        code_challenge,
        "show_dialog":           "false",
    }
    return f"{AUTHORIZE_URL}?{urllib.parse.urlencode(params)}"


# ── Loopback HTTP server per il callback ────────────────────────────

@dataclass
class _CallbackResult:
    """Esito del callback: code o error, mai entrambi."""
    code:  Optional[str] = None
    state: Optional[str] = None
    error: Optional[str] = None


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    """
    Handler che cattura UNA richiesta GET /callback?code=...

    Risponde con una pagina HTML che l'utente vede nel browser
    ("Puoi chiudere questa scheda"). Il code viene salvato nel
    `result_holder` condiviso col chiamante.
    """

    # Patchato dal server prima di servire
    result_holder: _CallbackResult = None  # type: ignore
    expected_state: str = ""

    def do_GET(self):  # noqa: N802 (BaseHTTPRequestHandler API)
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path not in ("/callback", "/"):
            self.send_error(404, "Not Found")
            return
        qs = urllib.parse.parse_qs(parsed.query)
        code  = qs.get("code", [None])[0]
        state = qs.get("state", [None])[0]
        error = qs.get("error", [None])[0]

        # Validazione CSRF: lo state deve combaciare
        if state != self.expected_state:
            self.result_holder.error = "state_mismatch"
            self._respond_html("Errore: state mismatch (possibile CSRF).",
                               ok=False)
            return

        if error:
            self.result_holder.error = error
            self._respond_html(f"Autorizzazione negata: {error}", ok=False)
            return

        if not code:
            self.result_holder.error = "no_code"
            self._respond_html("Risposta inattesa da Spotify.", ok=False)
            return

        self.result_holder.code = code
        self.result_holder.state = state
        self._respond_html(
            "Account Spotify collegato! Puoi chiudere questa scheda "
            "e tornare a Music Cataloger.", ok=True)

    def _respond_html(self, message: str, ok: bool):
        color = "#3b6fd4" if ok else "#d84545"
        html = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Music Cataloger - Spotify</title>
<style>body{{font-family:Segoe UI,sans-serif;background:#0f1419;color:#e8edf2;
display:flex;align-items:center;justify-content:center;height:100vh;margin:0;}}
.card{{background:#1e2533;padding:32px 40px;border-radius:12px;
border-left:4px solid {color};max-width:480px;text-align:center;}}
h1{{color:{color};margin:0 0 12px;font-size:18px;}}
p{{color:#7a8699;margin:0;line-height:1.5;}}</style></head>
<body><div class="card"><h1>Music Cataloger Advanced</h1>
<p>{message}</p></div></body></html>"""
        body = html.encode("utf-8")
        self.send_response(200 if ok else 400)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        # Silenzia il log stdout standard di BaseHTTPRequestHandler.
        logger.debug("[spotify_oauth_callback] " + fmt, *args)


def _pick_free_port(ports: list[int]) -> Optional[int]:
    """
    Trova la prima porta libera nella lista. Ritorna None se nessuna
    e' disponibile.

    Sondiamo con `bind` su SO_REUSEADDR=0 — la stessa porta verra' poi
    usata dal `HTTPServer` qualche ms dopo, in pratica il check e'
    sufficiente perche' nessun altro processo dovrebbe acquisirla
    nell'intervallo.
    """
    for p in ports:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", p))
                return p
        except OSError:
            continue
    return None


def _wait_for_callback(port: int, expected_state: str,
                       timeout: int) -> _CallbackResult:
    """
    Avvia il server, attende UNA richiesta, poi shutdown.

    Bloccante. Da chiamare in un thread se non si vuole bloccare la
    GUI principale.
    """
    result = _CallbackResult()
    _CallbackHandler.result_holder = result
    _CallbackHandler.expected_state = expected_state

    server = http.server.HTTPServer(("127.0.0.1", port), _CallbackHandler)
    server.timeout = 1   # poll ogni secondo per controllare il timeout

    elapsed = 0
    while result.code is None and result.error is None and elapsed < timeout:
        server.handle_request()
        elapsed += 1   # approssimazione: handle_request blocca al piu' `timeout`

    try:
        server.server_close()
    except Exception:
        pass

    if result.code is None and result.error is None:
        result.error = "timeout"
    return result


# ── Token exchange e refresh ────────────────────────────────────────

def _post_token(data: dict) -> dict:
    """POST a /api/token e parsing JSON. Solleva su HTTP != 200."""
    body = urllib.parse.urlencode(data).encode("ascii")
    req = urllib.request.Request(
        TOKEN_URL, data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return payload
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = "<no body>"
        raise SpotifyOAuthError(
            f"HTTP {e.code} da Spotify token endpoint: {err_body}") from e
    except urllib.error.URLError as e:
        raise SpotifyOAuthError(
            f"Errore di rete verso Spotify: {e.reason}") from e


def _exchange_code(client_id: str, code: str, redirect_uri: str,
                   verifier: str) -> dict:
    """Scambia authorization code per access/refresh token."""
    return _post_token({
        "grant_type":    "authorization_code",
        "code":          code,
        "redirect_uri":  redirect_uri,
        "client_id":     client_id,
        "code_verifier": verifier,
    })


def _refresh_token(client_id: str, refresh: str) -> dict:
    """Rigenera l'access token usando il refresh_token."""
    return _post_token({
        "grant_type":    "refresh_token",
        "refresh_token": refresh,
        "client_id":     client_id,
    })


def _fetch_user_profile(access_token: str) -> dict:
    """GET /v1/me — solo per visualizzazione in Impostazioni."""
    req = urllib.request.Request(
        ME_URL, headers={"Authorization": f"Bearer {access_token}"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning("[spotify_oauth] /me fallito (non bloccante): %s", e)
        return {}


# ── API pubblica ────────────────────────────────────────────────────

def is_configured() -> bool:
    """True se l'app Developer e' configurata (Pedro ha messo il
    Client ID). Quando False, la UI mostra il pulsante disabilitato."""
    return bool(settings.spotify_oauth.client_id.strip())


def start_connect_flow() -> SpotifyToken:
    """
    Avvia il flusso completo. Bloccante per il chiamante (~10-60s
    tipicamente, fino a `callback_timeout`).

    Va chiamato da un worker thread: apre un browser, attende il
    callback HTTP, scambia il code, salva il token e ritorna.

    Solleva:
      SpotifyNotConfigured  — se client_id e' vuoto
      SpotifyOAuthCancelled — se l'utente nega il consenso
      SpotifyOAuthTimeout   — se non riceviamo callback in tempo
      SpotifyOAuthError     — errori di rete o di scambio
    """
    cfg = settings.spotify_oauth
    if not cfg.client_id.strip():
        raise SpotifyNotConfigured(
            "Spotify Client ID non configurato. Feature in preparazione.")

    # 1. Pick porta libera tra quelle registrate
    port = _pick_free_port(cfg.callback_ports)
    if port is None:
        raise SpotifyOAuthError(
            f"Nessuna porta libera tra {cfg.callback_ports}. "
            "Chiudi eventuali altre istanze di Music Cataloger.")

    # 2. PKCE pair + state
    verifier, challenge = _generate_pkce_pair()
    state = secrets.token_urlsafe(24)

    # Costruiamo il redirect_uri con la porta effettiva. La porta DEVE
    # essere registrata sul dashboard Spotify (vedi callback_ports nei
    # settings).
    parsed = urllib.parse.urlparse(cfg.redirect_uri)
    redirect_uri = urllib.parse.urlunparse(
        parsed._replace(netloc=f"127.0.0.1:{port}"))

    auth_url = _build_authorize_url(
        client_id=cfg.client_id, redirect_uri=redirect_uri,
        scope=cfg.scope, state=state, code_challenge=challenge)

    # 3. Apri browser
    logger.info("[spotify_oauth] apro browser per consenso utente (port=%d)",
                port)
    webbrowser.open(auth_url, new=2)

    # 4. Attendi callback
    result = _wait_for_callback(port, state, cfg.callback_timeout)
    if result.error == "timeout":
        raise SpotifyOAuthTimeout(
            f"Nessuna risposta entro {cfg.callback_timeout}s.")
    if result.error in ("access_denied", "state_mismatch"):
        raise SpotifyOAuthCancelled(result.error)
    if result.error:
        raise SpotifyOAuthError(f"OAuth error: {result.error}")
    if not result.code:
        raise SpotifyOAuthError("Callback senza authorization code.")

    # 5. Scambia code per token
    logger.info("[spotify_oauth] code ricevuto, scambio per token")
    payload = _exchange_code(cfg.client_id, result.code, redirect_uri,
                             verifier)
    access  = payload.get("access_token")
    refresh = payload.get("refresh_token")
    expires_in = int(payload.get("expires_in", 3600))
    scope_granted = payload.get("scope", cfg.scope)
    if not access or not refresh:
        raise SpotifyOAuthError(f"Token response incompleta: {payload}")

    # 6. Profilo utente (best-effort, per UI)
    profile = _fetch_user_profile(access)

    token = SpotifyToken(
        access_token=access,
        refresh_token=refresh,
        expires_at=(datetime.now(timezone.utc) +
                    timedelta(seconds=expires_in)).isoformat(),
        scope=scope_granted,
        user_id=profile.get("id"),
        user_email=profile.get("email"),
        user_display_name=profile.get("display_name"),
    )
    spotify_store.save(token)
    logger.info("[spotify_oauth] token salvato. utente=%s scope=%s",
                token.user_email or token.user_id or "?", scope_granted)
    return token


def get_valid_access_token() -> Optional[str]:
    """
    Ritorna un access_token valido, refreshandolo se serve.

    None se:
      - utente non ha mai collegato Spotify
      - refresh fallito (il chiamante NON deve usare Spotify; spetta a
        external_apis cadere sul fallback proxy)
    """
    tok = spotify_store.load()
    if tok is None:
        return None
    if not spotify_store.is_expired():
        return tok.access_token

    # Tenta il refresh
    cfg = settings.spotify_oauth
    if not cfg.client_id.strip():
        # Edge case: l'utente si era collegato con un client_id che
        # poi e' stato rimosso. Senza client_id non possiamo refreshare
        # → invalidiamo il token e segnaliamo "non connesso".
        logger.warning(
            "[spotify_oauth] client_id mancante, impossibile refresh — "
            "il token e' marcato invalido")
        return None
    try:
        payload = _refresh_token(cfg.client_id, tok.refresh_token)
        new_access = payload.get("access_token")
        if not new_access:
            raise SpotifyOAuthError(f"refresh response incompleta: {payload}")
        spotify_store.update_after_refresh(
            new_access=new_access,
            expires_in=int(payload.get("expires_in", 3600)),
            new_refresh=payload.get("refresh_token"),   # opzionale
            new_scope=payload.get("scope"),
        )
        return new_access
    except SpotifyOAuthError as e:
        logger.warning("[spotify_oauth] refresh fallito: %s", e)
        return None


def disconnect() -> None:
    """Cancella il token locale. L'utente puo' ricollegare in
    qualsiasi momento. Non c'e' un endpoint Spotify per revocare
    server-side (l'utente puo' farlo da Spotify Account → Apps)."""
    spotify_store.clear()
    logger.info("[spotify_oauth] account scollegato (token locale cancellato)")


def get_connection_info() -> Optional[dict]:
    """
    Per la UI Impostazioni: ritorna info user-friendly sull'account
    collegato. None se non collegato.
    """
    tok = spotify_store.load()
    if tok is None:
        return None
    return {
        "email":        tok.user_email,
        "display_name": tok.user_display_name,
        "user_id":      tok.user_id,
        "scope":        tok.scope,
    }
