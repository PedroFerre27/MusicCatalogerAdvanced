#!/usr/bin/env python3
"""
run_gui.py — Entry point GUI v0.0.2.2 (con autenticazione server)

Flusso all'avvio:
  1. Prova a leggere una sessione salvata (jwt_store)
  2. Se presente, prova /auth/me per validarla
     - successo       → avvia main window con info utente dal server
     - server offline → se offline_ok=True e token non vecchio → avvio offline
     - 401            → apre login window
  3. Se assente → apre login window
  4. Dopo login → avvia main window

Configurazione URL server: `data/client_config.json` (auto-creato).
"""
from __future__ import annotations
import sys
import io
from pathlib import Path
from typing import Optional

# v0.0.2.2 fix: forza UTF-8 su stdout/stderr su Windows (cp1252 default
# crasha su caratteri come '✓' '→' '⚠'). Su Linux/macOS è già UTF-8.
# Questo va fatto PRIMA di qualsiasi print o import che possa stampare.
if sys.platform == "win32":
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                      errors="replace", line_buffering=True)
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8",
                                      errors="replace", line_buffering=True)
    except Exception:
        # In alcuni contesti (PyInstaller --windowed, exe senza console)
        # stdout è None → fallback a StringIO dummy
        class _Null:
            def write(self, *a, **kw): pass
            def flush(self): pass
        if sys.stdout is None: sys.stdout = _Null()
        if sys.stderr is None: sys.stderr = _Null()

# Aggiungo la root del progetto al path (stesso comportamento del vecchio run_gui)
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# v1085e: modalità subprocess cataloger.
# Quando il client è EXE PyInstaller, _build_command costruisce un
# comando del tipo `[sys.executable, "--cataloger-mode", path, ...altri args]`.
# `sys.executable` è l'EXE GUI stesso, quindi al boot intercettiamo
# questo flag PRIMA di tutto e invochiamo il main del cataloger
# nello stesso processo, senza costruire la GUI.
if "--cataloger-mode" in sys.argv:
    sys.argv.remove("--cataloger-mode")
    try:
        from run_cataloger import main as cataloger_main
    except Exception:
        # Fallback: importa con path esplicito (PyInstaller bundle)
        import importlib.util
        meipass = getattr(sys, "_MEIPASS", str(PROJECT_ROOT))
        spec = importlib.util.spec_from_file_location(
            "run_cataloger", str(Path(meipass) / "run_cataloger.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        cataloger_main = mod.main
    rc = cataloger_main()
    sys.exit(rc if isinstance(rc, int) else 0)

# v1086.2: singleton lock — impedisci doppio avvio.
# DEVE stare DOPO il check `--cataloger-mode` (perche' il subprocess
# cataloger e' una seconda invocazione legittima dell'EXE) ma PRIMA
# di tutti gli import pesanti (ctk, ApiClient, etc.) cosi' il check
# di "gia' in esecuzione" sia istantaneo.
from services.singleton import (
    acquire as _singleton_acquire,
    bring_existing_to_front as _singleton_focus,
    show_already_running_dialog as _singleton_dialog,
)
if not _singleton_acquire():
    # Un'altra istanza e' gia' attiva. Porto la sua finestra in primo
    # piano. v1086.2 round 3 (Pedro feedback): se bring-to-front
    # riesce, NIENTE dialog — il porting in foreground e' gia' chiaro
    # all'utente. Mostro il dialog solo come fallback se la finestra
    # esistente non viene trovata (es. crash zombie o errore Win32).
    if not _singleton_focus():
        _singleton_dialog()
    sys.exit(0)

import customtkinter as ctk

from config.app_config import config as client_config
from services.api_client import (
    ApiClient, AuthError, ServerUnreachableError,
)
from services.jwt_store import store


APP_VERSION = "v0.0.2.2"


def _try_resume_session() -> tuple[Optional[ApiClient], Optional[dict], str]:
    """
    Tenta di usare una sessione salvata. Ritorna (client, user, mode):
      mode = "online"   → tutto ok, client autenticato
             "offline"  → server unreachable ma token locale ancora valido
             "expired"  → serve login
             "none"     → nessun token salvato
    """
    stored = store.load()
    if stored is None:
        return None, None, "none"

    client = ApiClient(stored.server_url)

    # Ping rapido
    if not client.ping():
        # Server offline — se policy lo consente, parte offline
        if client_config.offline_ok:
            info = client.get_stored_user_info()
            return client, info, "offline"
        return client, None, "expired"

    # Server online — prova /auth/me (gestisce refresh automatico)
    try:
        user = client.me()
        return client, user, "online"
    except AuthError:
        # Refresh scaduto → serve nuovo login
        store.clear()
        return None, None, "expired"
    except ServerUnreachableError:
        # Server caduto fra ping e me
        if client_config.offline_ok:
            info = client.get_stored_user_info()
            return client, info, "offline"
        return None, None, "expired"


def _show_login() -> tuple[Optional[ApiClient], Optional[dict]]:
    """Mostra la finestra di login. Ritorna (client, user) se riuscito."""
    # Import lazy per evitare di creare CTk prima del momento giusto
    from gui.login_window import LoginWindow
    w = LoginWindow()
    client, user = w.show()
    return client, user


def _start_main_window(api_client: ApiClient, user_info: dict, offline: bool):
    """Avvia la main window passando contesto server."""
    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("blue")

    # v1088.1 fix: aggiorna il piano locale con i dati ricevuti dal
    # server PRIMA di costruire la GUI. Senza questo, la titlebar e
    # tutti gli has_feature() leggevano il default "base" perché
    # set_plan_from_server (introdotta nell'audit security) non era
    # mai chiamata da nessuno. Effetto bug: utente Advanced vedeva
    # "TrackLab | Base" e tutte le feature pro-only nascoste.
    if user_info:
        try:
            from config.user_plans import set_plan_from_server
            set_plan_from_server(
                plan=user_info.get("plan", "base"),
                username=user_info.get("username", "") or "",
                email=user_info.get("email", "") or "",
            )
        except Exception as e:
            print(f"[run_gui] set_plan_from_server skip: {e}")

    root = ctk.CTk()
    # Import lazy della main window (è grossa)
    from gui.main_window import TrackLabGUI

    # v0.0.2.2: TrackLabGUI accetta api_client e user_info opzionali.
    # Per retrocompatibilità, se la main window corrente non li accetta,
    # fallback al costruttore vecchio.
    try:
        gui = TrackLabGUI(root, api_client=api_client, user_info=user_info)
    except TypeError:
        # Main window non ancora adattata → parte in modalità legacy
        print("[run_gui] Main window in modalità legacy (no server integration)")
        gui = TrackLabGUI(root)

    if offline:
        # Visual warning: banner giallo in cima
        try:
            root.title(root.title() + "  [MODALITÀ OFFLINE]")
        except Exception:
            pass

    # v0.0.2.4: Check auto-update (solo online + EXE PyInstaller)
    if not offline and api_client is not None:
        try:
            from services.updater import check_and_offer_update, cleanup_old_backup
            # v1085m: pulisci backup .exe.old del vecchio updater (no-op
            # se non esiste). Non bloccante se fallisce.
            try: cleanup_old_backup()
            except Exception: pass
            # Delay 1.5s per dare tempo alla main window di apparire
            root.after(1500, lambda: check_and_offer_update(
                api_client, parent_window=root, silent=True))
        except Exception as e:
            print(f"[run_gui] update check skip: {e}")

    root.mainloop()


def main():
    print("=" * 60)
    print(f"TrackLab GUI {APP_VERSION}")
    print("=" * 60)
    print(f"Server: {client_config.server_url}")

    client, user, mode = _try_resume_session()

    if mode == "online":
        print(f"✓ Sessione ripresa per {user.get('email')} (online)")
        _start_main_window(client, user, offline=False)
        return

    if mode == "offline":
        print(f"⚠ Server {client_config.server_url} irraggiungibile — avvio in modalità offline")
        _start_main_window(client, user, offline=True)
        return

    # mode in ("expired", "none") → login
    print("→ Login richiesto")
    client, user = _show_login()
    if client is None or user is None:
        print("Login annullato — uscita")
        return
    print(f"✓ Login riuscito per {user.get('email')}")
    _start_main_window(client, user, offline=False)


if __name__ == "__main__":
    main()
