"""
services/api_client.py — Client HTTP verso il Music Cataloger Server.

Responsabilità:
- Wrapper su `requests` con autenticazione Bearer automatica
- Refresh trasparente dell'access token quando scade (401 → refresh → retry)
- Decodifica JWT lato client per leggere piano/features senza chiamate extra
- Rilevamento server offline e modalità fallback

Uso tipico:

    client = ApiClient(server_url)
    user = client.login(email, password)   # StoredSession salvata in jwt_store
    plans = client.list_plans()            # usa il token automaticamente
    me = client.me()

    # quando il server è unreachable
    try:
        client.me()
    except ServerUnreachableError:
        # passa a modalità offline con piano letto dal JWT locale
        features = client.get_features_from_stored_token()
"""
from __future__ import annotations
import base64
import json
from dataclasses import dataclass
from typing import Any, Optional

import requests

from .jwt_store import store, StoredSession


# Timeouts ragionevoli per NAS casalingo via HTTPS
CONNECT_TIMEOUT = 5    # secondi
READ_TIMEOUT    = 30


class ApiError(Exception):
    """Errore generico di chiamata API."""
    def __init__(self, status: int, detail: Any):
        super().__init__(f"HTTP {status}: {detail}")
        self.status = status
        self.detail = detail


class ServerUnreachableError(Exception):
    """Il server è irraggiungibile (timeout, DNS, proxy). Passa a offline."""


class AuthError(Exception):
    """Credenziali non valide o token irrecuperabilmente scaduto."""


# ── Utility JWT ───────────────────────────────────────────────────
def decode_jwt_payload(token: str) -> dict:
    """Decodifica la parte payload di un JWT senza verificare la firma.
    Il client si fida del payload solo per info non-sensibili come piano
    e username — il server comunque valida il token su ogni chiamata."""
    try:
        _, payload_b64, _ = token.split(".")
        # padding base64url
        padded = payload_b64 + "=" * (-len(payload_b64) % 4)
        data = base64.urlsafe_b64decode(padded.encode())
        return json.loads(data)
    except Exception:
        return {}


# ── Client ────────────────────────────────────────────────────────
@dataclass
class LoginResponse:
    access_token:  str
    refresh_token: str
    user:          dict   # contiene email, username, plan, features, is_admin


class ApiClient:

    def __init__(self, server_url: str):
        # Rimuove trailing slash
        self.server_url = server_url.rstrip("/")
        self._session = requests.Session()

    # ── Basso livello ─────────────────────────────────────────────
    def _url(self, path: str) -> str:
        return f"{self.server_url}{path}"

    def _auth_headers(self) -> dict:
        s = store.load()
        return {"Authorization": f"Bearer {s.access_token}"} if s else {}

    def _handle_error(self, resp: requests.Response) -> None:
        """Solleva l'eccezione appropriata in base al codice di risposta."""
        if resp.ok:
            return
        try:
            detail = resp.json().get("detail", resp.text)
        except Exception:
            detail = resp.text
        if resp.status_code == 401:
            raise AuthError(str(detail))
        raise ApiError(resp.status_code, detail)

    def _request(
        self, method: str, path: str,
        json_body: Optional[dict] = None,
        form_body: Optional[dict] = None,
        retry_on_401: bool = True,
        require_auth: bool = True,
    ) -> dict:
        """Wrapper centrale con refresh automatico del token."""
        headers = {}
        if require_auth:
            headers.update(self._auth_headers())
        try:
            resp = self._session.request(
                method, self._url(path),
                json=json_body, data=form_body,
                headers=headers,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
        except (requests.ConnectionError, requests.Timeout) as e:
            raise ServerUnreachableError(str(e)) from e

        # Refresh trasparente al primo 401
        if resp.status_code == 401 and retry_on_401 and require_auth:
            try:
                self._refresh_access_token()
            except Exception:
                raise AuthError("Sessione scaduta — effettua nuovamente il login")
            # Retry con nuovo access_token
            return self._request(
                method, path,
                json_body=json_body, form_body=form_body,
                retry_on_401=False, require_auth=require_auth,
            )

        self._handle_error(resp)
        if resp.content:
            return resp.json()
        return {}

    def _refresh_access_token(self) -> None:
        s = store.load()
        if not s:
            raise AuthError("Nessuna sessione salvata")
        try:
            resp = self._session.post(
                self._url("/auth/refresh"),
                json={"refresh_token": s.refresh_token},
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
        except (requests.ConnectionError, requests.Timeout) as e:
            raise ServerUnreachableError(str(e)) from e
        if resp.status_code != 200:
            # Refresh token scaduto o invalido → user deve rifare login
            store.clear()
            raise AuthError("Refresh token scaduto")
        new_access = resp.json()["access_token"]
        store.update_access_token(new_access)

    # ── High level ────────────────────────────────────────────────
    def ping(self) -> bool:
        """Test rapido raggiungibilità server. Ritorna False se offline."""
        try:
            resp = self._session.get(self._url("/health"),
                                     timeout=(CONNECT_TIMEOUT, 3))
            return resp.status_code == 200
        except Exception:
            return False

    def login(self, email: str, password: str) -> LoginResponse:
        """Autentica e salva entrambi i token in jwt_store."""
        try:
            resp = self._session.post(
                self._url("/auth/login"),
                data={"username": email, "password": password},
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
        except (requests.ConnectionError, requests.Timeout) as e:
            raise ServerUnreachableError(str(e)) from e

        if resp.status_code == 401:
            raise AuthError("Email o password errate")
        self._handle_error(resp)

        data = resp.json()
        session = StoredSession(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            server_url=self.server_url,
            user_email=data["user"]["email"],
            user_plan=data["user"]["plan"],
            saved_at="",  # save() compila
        )
        store.save(session)
        return LoginResponse(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            user=data["user"],
        )

    def logout(self) -> None:
        store.clear()

    def me(self) -> dict:
        return self._request("GET", "/auth/me")

    def lookup(self, provider: str, artist: str, title: str) -> Optional[dict]:
        """v1087.3 (security Fase 2): proxy lookup metadati via server.

        Sostituisce le chiamate dirette del client a Discogs/Last.fm/
        Spotify/GetSong (i cui token sono ora SOLO sul server).

        Ritorna il dict metadati normalizzato (stesso formato di prima)
        oppure None se: nessun risultato, provider non supportato,
        server irraggiungibile, o utente non autenticato. In tutti i
        casi None → il chiamante fa fallback ai provider pubblici
        (iTunes/MusicBrainz/Deezer) senza interrompere la catalogazione.
        """
        try:
            from urllib.parse import quote
            path = (f"/api/v1/lookup?provider={quote(provider)}"
                    f"&artist={quote(artist)}&title={quote(title)}")
            resp = self._request("GET", path)
            if resp and resp.get("found"):
                return resp.get("data")
            return None
        except ServerUnreachableError:
            # Server offline → fallback graceful (il client continua coi
            # provider pubblici). Non rilanciare: la catalogazione non
            # deve fermarsi se il proxy non risponde.
            return None
        except AuthError:
            # Sessione non valida → il lookup proxy non e' disponibile,
            # ma la catalogazione locale puo' proseguire coi provider
            # pubblici. Logghiamo a debug, non interrompiamo.
            return None
        except Exception:
            return None

    def list_plans(self) -> list:
        return self._request("GET", "/plans", require_auth=False)

    def my_plan(self) -> dict:
        return self._request("GET", "/plans/me")

    def request_upgrade(self, to_plan: str, message: str = "") -> dict:
        return self._request(
            "POST", "/plans/upgrade-request",
            json_body={"to_plan": to_plan, "message": message or None},
        )

    def my_upgrade_requests(self) -> list:
        return self._request("GET", "/plans/my-requests")

    # ── v0.0.2.3 — Change password + Register ─────────────────────
    def change_password(self, current_password: str, new_password: str) -> dict:
        """Cambia password dell'utente autenticato. Richiede l'access token
        attuale. I token esistenti NON vengono invalidati (scelta MVP)."""
        return self._request(
            "POST", "/auth/change-password",
            json_body={
                "current_password": current_password,
                "new_password":     new_password,
            },
        )

    def register(self, email: str, username: str, password: str) -> dict:
        """Self-service signup. Il nuovo utente parte SEMPRE con piano 'base'.
        Non richiede autenticazione. Dopo la registrazione, l'utente deve
        fare login separatamente."""
        return self._request(
            "POST", "/auth/register",
            json_body={
                "email":    email,
                "username": username,
                "password": password,
            },
            require_auth=False,
        )

    # ── v0.0.2.4 — Catalog tracking ───────────────────────────────
    def catalog_start(self, path: str, files_total: int,
                       options: Optional[dict] = None) -> dict:
        """Notifica server: sto iniziando una catalogazione.
        Il server applica le quote del piano:
          - 402 se files_total > max_files_per_run
          - 402 se runs giornaliere superate
          - 403 se opzioni non concesse al piano
        Ritorna {job_id, status, quota_remaining}.
        """
        return self._request(
            "POST", "/catalog/start",
            json_body={
                "path":        path[:500],
                "files_total": int(files_total),
                "options":     options or {},
            },
        )

    def catalog_progress(self, job_id: int, files_done: int,
                          progress_pct: int,
                          files_total: Optional[int] = None,
                          log_chunk: str = "", log_level: str = "INFO") -> dict:
        """Update di progresso. log_chunk può essere multi-riga (\\n)."""
        body = {
            "files_done":   int(files_done),
            "progress_pct": max(0, min(100, int(progress_pct))),
            "log_level":    log_level,
        }
        if files_total is not None:
            body["files_total"] = int(files_total)
        if log_chunk:
            body["log_chunk"] = log_chunk[:10000]
        return self._request(
            "POST", f"/catalog/{job_id}/progress", json_body=body,
        )

    def catalog_complete(self, job_id: int, files_done: int,
                          report: Optional[dict] = None) -> dict:
        """Notifica fine + invia report finale."""
        return self._request(
            "POST", f"/catalog/{job_id}/complete",
            json_body={
                "files_done": int(files_done),
                "report":     report or {},
            },
        )

    def catalog_fail(self, job_id: int, error_message: str) -> dict:
        return self._request(
            "POST", f"/catalog/{job_id}/fail",
            json_body={"error_message": str(error_message)[:2000]},
        )

    def catalog_cancel(self, job_id: int) -> dict:
        return self._request("POST", f"/catalog/{job_id}/cancel")

    def catalog_my_jobs(self, limit: int = 20) -> list:
        return self._request("GET", f"/catalog/my-jobs?limit={limit}")

    # ── v0.0.2.5 — Caribbean settings condivise ───────────────────
    def get_caribbean_defaults(self) -> dict:
        """Recupera i default caraibici pubblicati dall'admin sul server.
        Endpoint pubblico (no auth necessario), sempre 200."""
        return self._request("GET", "/caribbean-settings/defaults",
                              require_auth=False)

    def set_caribbean_defaults(self, settings_dict: dict) -> dict:
        """Admin: pubblica nuovi default caraibici per tutti gli utenti."""
        return self._request("POST", "/admin/caribbean-settings",
                              json_body=settings_dict)

    # ── v0.0.2.6 — Registration open/disable ──────────────────────
    def get_registration_status(self) -> dict:
        """Pubblico: ritorna {enabled, message}. Il client lo chiama
        nella login window per mostrare/nascondere il link Registrati."""
        return self._request("GET", "/auth/registration/status",
                              require_auth=False)

    def admin_disable_registration(self) -> dict:
        return self._request("POST", "/auth/admin/registration/disable")

    def admin_enable_registration(self) -> dict:
        return self._request("POST", "/auth/admin/registration/enable")

    # ── v0.0.2.7 — Admin: crea utente + statistiche ──────────────
    def admin_create_user(self, email: str, username: str, password: str,
                          plan: str = "base", is_admin: bool = False) -> dict:
        return self._request("POST", "/auth/admin/users", json_body={
            "email": email, "username": username, "password": password,
            "plan": plan, "is_admin": is_admin,
        })

    def get_admin_stats(self) -> dict:
        return self._request("GET", "/admin/stats")

    # ── Supporto offline ──────────────────────────────────────────
    def get_features_from_stored_token(self) -> dict:
        """Fallback offline: decodifica il JWT salvato per leggere le
        feature dell'utente senza chiamare il server. Da usare solo
        se il server è unreachable e l'utente ha un token locale valido.

        NOTA: il server NON ritorna `features` dentro il JWT access token
        (solo `plan`). Quindi qui usiamo `plan` e lookup nel dict locale
        di features — è una copia sincronizzata con il server.
        """
        s = store.load()
        if not s:
            return {}
        payload = decode_jwt_payload(s.access_token)
        plan = payload.get("plan", "base")
        # Import lazy per evitare circular
        try:
            from config.user_plans import PLAN_FEATURES
            return PLAN_FEATURES.get(plan, PLAN_FEATURES["base"])
        except ImportError:
            return {}

    def is_authenticated(self) -> bool:
        """True se abbiamo una sessione salvata (non garantisce validità
        del token — può essere scaduto)."""
        return store.load() is not None

    def get_stored_user_info(self) -> Optional[dict]:
        """Info utente dalla sessione salvata (per mostrare "DJ" nella GUI
        anche prima di una chiamata `/auth/me`).

        v1086.7 security-audit: `is_admin` viene SEMPRE letto come False
        in modalita' offline. Prima si fidava del JWT decodificato senza
        verifica firma → un attaccante poteva editare `session.json` con
        un token fake che dichiarava `is_admin=True` per vedere le tab
        admin (anche se le chiamate sarebbero state respinte dal server).
        Ora le tab admin si vedono SOLO con login online riuscito
        (autoritativo via `/auth/me`).
        """
        s = store.load()
        if not s:
            return None
        payload = decode_jwt_payload(s.access_token)
        return {
            "email":    s.user_email,
            "plan":     s.user_plan,
            "username": payload.get("username", s.user_email.split("@")[0]),
            # v1086.7: SEMPRE False in stored info; only `me()` puo' set True
            "is_admin": False,
        }
