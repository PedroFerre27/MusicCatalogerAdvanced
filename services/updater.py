"""
services/updater.py — Auto-update del client EXE (Windows)

Funzionamento:
1. Al boot dopo il login, il client chiama GET /version/latest
2. Se la versione remota è > corrente:
   - Mostra dialog "Aggiornamento disponibile" con changelog
   - Se l'utente accetta, scarica il nuovo EXE in cartella temp
   - Verifica sha256 (se fornito dal server)
   - Lancia un piccolo batch script "updater.bat" che:
     a) Aspetta che l'EXE corrente sia chiuso
     b) Sposta il nuovo EXE al posto del vecchio
     c) Riavvia l'app
   - Chiude l'app corrente

Il batch script è scritto al volo in temp e non viene committato.

Usage tipico nel client:

    from services.updater import check_and_offer_update
    check_and_offer_update(api_client, parent_window=root)

Il check è non bloccante: lancia un thread che fa rete, e mostra il dialog
solo se trova davvero un update.
"""
from __future__ import annotations
import hashlib
import os
import re
import sys
import tempfile
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional


def _log(msg: str) -> None:
    """v1085h: scrive su `data/updater.log` (oltre a stdout) così il
    debug è visibile anche quando l'EXE è in modalità windowed e
    sys.stdout è None. Pedro può aprire il file dopo il boot per
    capire cosa è successo nel check update.
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [updater] {msg}\n"
    # 1) stdout (visibile da CMD se EXE console mode)
    try:
        if sys.stdout is not None:
            print(line.rstrip(), flush=True)
    except Exception:
        pass
    # 2) file (sempre)
    try:
        # Cerco la dir 'data/' accanto all'EXE / script
        if getattr(sys, "frozen", False):
            base = Path(sys.executable).parent
        else:
            base = Path(__file__).parent.parent
        log_dir = base / "data"
        log_dir.mkdir(parents=True, exist_ok=True)
        with open(log_dir / "updater.log", "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def _parse_version(s: str) -> tuple:
    """
    'v1085' → (1085,)
    'v1.0.85' → (1, 0, 85)
    '0.1.4' → (0, 1, 4)
    Versioni non numeriche → (0,) per evitare crash.
    """
    s = s.strip().lstrip("v")
    parts = re.findall(r"\d+", s)
    if not parts:
        return (0,)
    return tuple(int(p) for p in parts)


def is_newer(remote_version: str, local_version: str) -> bool:
    """True se remote_version è strettamente più recente di local."""
    return _parse_version(remote_version) > _parse_version(local_version)


def get_local_version() -> str:
    """Legge APP_VERSION dal file version.py."""
    try:
        # Import dinamico per evitare dipendenze al boot
        from version import APP_VERSION
        return APP_VERSION
    except Exception:
        return "v0"


def is_running_as_exe() -> bool:
    """True se il client è in esecuzione come EXE PyInstaller."""
    return getattr(sys, "frozen", False)


def get_current_exe_path() -> Optional[Path]:
    """Path dell'EXE in esecuzione (None se siamo in modalità script)."""
    if is_running_as_exe():
        return Path(sys.executable)
    return None


def cleanup_old_backup() -> None:
    """v1085m: rimuove `<current_exe>.exe.old` lasciato dall'updater.

    Strategia rename-and-replace: l'updater rinomina il vecchio EXE
    in `*.exe.old`, poi sposta il nuovo. Quando il nuovo si avvia,
    chiama questa funzione per pulire il backup.

    Sicuro chiamarla anche quando .old non esiste — fa nothing.
    Catch broad: se per qualche motivo la cancellazione fallisce
    (lock antivirus), non blocchiamo l'app.
    """
    try:
        exe = get_current_exe_path()
        if exe is None:
            return
        old = exe.with_suffix(exe.suffix + ".old")
        if old.exists():
            old.unlink()
            _log(f"cleanup .exe.old completato: {old}")
    except Exception as e:
        _log(f"cleanup .exe.old skip (non critico): {e}")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _download_to_temp(url: str, expected_sha256: Optional[str] = None,
                      progress_cb=None) -> Path:
    """
    Scarica un file in cartella temp. Ritorna il Path del file scaricato.
    `progress_cb(downloaded_bytes, total_bytes)` chiamato periodicamente.
    Solleva eccezione se sha256 non corrisponde (se fornito).
    """
    import requests

    # Filename safe basato sul path
    fname = url.rsplit("/", 1)[-1] or "update.exe"
    fname = re.sub(r"[^A-Za-z0-9._-]", "_", fname)
    dest = Path(tempfile.gettempdir()) / "music_cataloger_update" / fname
    dest.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=(10, 300)) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        done = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                f.write(chunk)
                done += len(chunk)
                if progress_cb:
                    try:
                        progress_cb(done, total)
                    except Exception:
                        pass

    if expected_sha256:
        actual = _sha256_file(dest)
        if actual.lower() != expected_sha256.lower():
            try:
                dest.unlink()
            except Exception:
                pass
            raise ValueError(
                f"SHA256 mismatch: scaricato {actual[:12]}…, atteso {expected_sha256[:12]}…")

    return dest


def _make_windows_updater_script(current_exe: Path, new_exe: Path) -> Path:
    """
    Crea un batch script che aspetta che l'EXE corrente sia chiuso, poi
    lo sostituisce col nuovo e riavvia. Ritorna il path dello script.

    Il batch viene creato in temp con nome univoco. Il client lo lancia
    e poi termina — Windows continua a eseguire il batch dopo che l'EXE
    chiude i suoi handle.
    """
    if sys.platform != "win32":
        raise RuntimeError("Updater batch script disponibile solo su Windows")

    script_path = Path(tempfile.gettempdir()) / "music_cataloger_updater.bat"
    log_file = Path(tempfile.gettempdir()) / "music_cataloger_updater.log"
    # Path del backup del vecchio EXE — sarà cancellato al boot della
    # nuova versione (ved. updater.cleanup_old_backup)
    old_backup = current_exe.with_suffix(current_exe.suffix + ".old")
    # v1085m: strategia rename-and-replace invece di copy.
    # Causa del bug "Failed to load python313.dll" in v1085l:
    # con PyInstaller ONEFILE, il vecchio EXE in esecuzione tiene un
    # lock sul proprio file E sulla _MEIxxx temp dir. Fare `copy /Y`
    # mentre il processo si stava chiudendo creava una race condition:
    # a volte la copy avveniva con il bootloader ancora "agganciato",
    # producendo un EXE corrotto sui ~31 MB iniziali.
    #
    # Nuova strategia (atomica):
    #   1. Aspetta che il vecchio EXE NON sia in lock
    #   2. RINOMINA il vecchio in *.exe.old (operazione atomica filesystem)
    #   3. SPOSTA il nuovo in posizione del vecchio (atomico)
    #   4. Lancia il nuovo
    #   5. Al boot del nuovo, updater.cleanup_old_backup() rimuove .exe.old
    #
    # Perché funziona: rename è atomico e non legge/scrive il contenuto;
    # quindi non collide col bootloader PyInstaller in chiusura.
    content = f"""@echo off
REM Music Cataloger auto-updater (v1085m: rename-and-replace strategy)
REM Atteso da: {current_exe}
REM Nuovo file: {new_exe}

setlocal
set LOG="{log_file}"
echo. >> %LOG%
echo ============================================== >> %LOG%
echo [%DATE% %TIME%] Avvio updater v1085m >> %LOG%
echo   current_exe = {current_exe} >> %LOG%
echo   new_exe     = {new_exe} >> %LOG%
echo   old_backup  = {old_backup} >> %LOG%
echo Music Cataloger - aggiornamento in corso...
timeout /t 3 /nobreak >nul

REM Step 1: aspetto che il vecchio EXE non sia in lock.
REM Test del lock = provo a rinominarlo: se ci riesco, non è in uso.
set RETRIES=0
:WAIT_UNLOCK
REM Pulisco eventuale .old da update precedente fallito
if exist "{old_backup}" del /F /Q "{old_backup}" >> %LOG% 2>&1

REM Tento rename atomico — se il file è in lock fallisce
ren "{current_exe}" "{current_exe.name}.old" >> %LOG% 2>&1
if %errorlevel%==0 goto MOVE_NEW

set /a RETRIES+=1
echo [%DATE% %TIME%] Tentativo %RETRIES%: vecchio EXE ancora in lock >> %LOG%
if %RETRIES% GEQ 30 goto FAIL_LOCK
timeout /t 1 /nobreak >nul
goto WAIT_UNLOCK

:MOVE_NEW
echo [%DATE% %TIME%] Vecchio EXE rinominato in .old, sposto il nuovo >> %LOG%
move /Y "{new_exe}" "{current_exe}" >> %LOG% 2>&1
if %errorlevel% neq 0 goto FAIL_MOVE

echo [%DATE% %TIME%] SUCCESS - nuovo EXE in posizione, rilancio >> %LOG%
echo Aggiornamento completato. Riavvio in corso...
REM Aspetto un altro secondo per essere sicuri che il nuovo file sia
REM "stabile" sul filesystem (antivirus/OneDrive possono aver appena
REM fatto un'apertura per scansione)
timeout /t 1 /nobreak >nul
start "" "{current_exe}"
echo [%DATE% %TIME%] Updater terminato OK >> %LOG%
exit /b 0

:FAIL_LOCK
echo [%DATE% %TIME%] FAIL - vecchio EXE rimane in lock dopo 30 tentativi >> %LOG%
echo ERRORE: l'eseguibile vecchio sembra ancora in uso.
echo Chiudi manualmente Music Cataloger e riprova.
echo Log: %LOG%
pause
exit /b 1

:FAIL_MOVE
echo [%DATE% %TIME%] FAIL - move del nuovo EXE fallito >> %LOG%
REM Tento rollback: rinomino .old di nuovo a .exe
ren "{current_exe}.old" "{current_exe.name}" >> %LOG% 2>&1
echo ERRORE: impossibile installare la nuova versione.
echo Log: %LOG%
pause
exit /b 1
"""
    script_path.write_text(content, encoding="cp1252")
    return script_path


# ── High-level API: check + dialog ────────────────────────────────
def check_and_offer_update(api_client, parent_window=None, silent: bool = True):
    """
    Punto di ingresso principale. Da chiamare al boot dopo il login.

    - Esegue il check su un thread separato (non blocca GUI)
    - Se trova un update e siamo in modalità EXE, mostra il dialog
    - Se siamo in modalità script (sviluppo), logga ma NON propone update
    - `silent=True` → non mostra messaggi se "tutto è aggiornato" o se
      il server non ha pubblicato release
    """
    if api_client is None:
        return  # offline, niente check

    def _worker():
        local_ver = get_local_version()
        _log(f"check inizio — versione locale: {local_ver}, "
             f"frozen={is_running_as_exe()}")
        try:
            info = api_client._request("GET", "/version/latest", require_auth=False)
        except Exception as e:
            err_str = str(e)
            _log(f"check failed: {err_str}")
            # v1085h: se l'errore è 404 (= nessuna release pubblicata)
            # o ServerUnreachable, è normale — silenzio.
            # Per altri errori (500, 401, parse JSON…) mostra dialog
            # all'admin in modo che capisca cosa succede.
            from services.api_client import ApiError, ServerUnreachableError
            is_silent = (
                isinstance(e, ServerUnreachableError)
                or (isinstance(e, ApiError) and e.status == 404)
                or silent
            )
            if not is_silent and parent_window is not None:
                try:
                    from tkinter import messagebox
                    parent_window.after(0, lambda: messagebox.showwarning(
                        "Verifica aggiornamenti",
                        f"Impossibile contattare il server per verificare "
                        f"aggiornamenti:\n\n{err_str}\n\nL'app continuerà a "
                        f"funzionare normalmente."))
                except Exception:
                    pass
            return

        remote_ver = info.get("version", "")
        _log(f"versione remota: {remote_ver}, locale: {local_ver}")
        if not remote_ver:
            _log(f"/version/latest non ha restituito un campo 'version'")
            return
        if not is_newer(remote_ver, local_ver):
            _log(f"versione corrente {local_ver} è aggiornata "
                 f"(remota: {remote_ver})")
            return

        _log(f"nuova versione disponibile: {remote_ver} (corrente: {local_ver})")

        if not is_running_as_exe():
            _log("non in modalità EXE — skip auto-update "
                 "(rebuild manuale necessario in sviluppo)")
            return

        # Mostra dialog su main thread
        if parent_window is not None:
            try:
                parent_window.after(0, lambda: _show_update_dialog(
                    api_client, parent_window, info, local_ver))
            except Exception as e:
                _log(f"dialog open failed: {e}")

    threading.Thread(target=_worker, daemon=True).start()


def _show_update_dialog(api_client, parent_window, info: dict, local_ver: str):
    """Dialog interattivo: mostra changelog + offre update / posticipa."""
    import customtkinter as ctk
    from tkinter import messagebox

    PALETTE = {
        "bg":         "#0f1419",
        "surface":    "#1e2533",
        "text":       "#e8edf2",
        "text_dim":   "#7a8699",
        "primary":    "#3b6fd4",
        "primary_hover": "#2d5ab8",
        "border":     "#333a4a",
    }

    win = ctk.CTkToplevel(parent_window)
    win.title("Aggiornamento disponibile")
    win.geometry("500x460")
    win.resizable(False, False)
    win.transient(parent_window)
    try:
        win.grab_set()
    except Exception:
        pass
    win.configure(fg_color=PALETTE["bg"])
    try:
        from gui.app_icon import set_window_icon
        set_window_icon(win)
    except Exception:
        pass

    # Centra
    parent_window.update_idletasks()
    px = parent_window.winfo_x()
    py = parent_window.winfo_y()
    pw = parent_window.winfo_width()
    ph = parent_window.winfo_height()
    win.geometry(f"500x460+{px+(pw-500)//2}+{py+(ph-460)//2}")

    ctk.CTkLabel(win, text="⬆  Aggiornamento disponibile",
                 font=("Segoe UI", 16, "bold"),
                 text_color=PALETTE["text"]).pack(pady=(20, 4))
    ctk.CTkLabel(
        win,
        text=f"Versione corrente: {local_ver}\n"
             f"Versione disponibile: {info['version']}",
        font=("Segoe UI", 11),
        text_color=PALETTE["text_dim"]
    ).pack(pady=(0, 12))

    # Changelog box
    box_frame = ctk.CTkFrame(win, fg_color=PALETTE["surface"], corner_radius=8)
    box_frame.pack(fill="both", expand=True, padx=24, pady=(0, 12))
    ctk.CTkLabel(box_frame, text="Novità in questa versione:",
                 font=("Segoe UI", 10, "bold"),
                 text_color=PALETTE["text"], anchor="w"
                 ).pack(fill="x", padx=12, pady=(8, 4))
    txt = ctk.CTkTextbox(box_frame, fg_color=PALETTE["bg"],
                         text_color=PALETTE["text"],
                         border_color=PALETTE["border"], border_width=1,
                         font=("Consolas", 10), height=180, wrap="word")
    txt.pack(fill="both", expand=True, padx=12, pady=(0, 12))
    txt.insert("1.0", info.get("changelog") or "(nessun changelog fornito)")
    txt.configure(state="disabled")

    # Status line (durante download)
    status_var = ctk.StringVar(value="")
    status_lbl = ctk.CTkLabel(win, textvariable=status_var,
                              font=("Segoe UI", 10),
                              text_color=PALETTE["text_dim"])
    status_lbl.pack()

    # Buttons
    btn_row = ctk.CTkFrame(win, fg_color="transparent")
    btn_row.pack(fill="x", padx=24, pady=(8, 18))

    is_mandatory = bool(info.get("mandatory", False))

    def _do_update():
        btn_yes.configure(state="disabled", text="Scarico...")
        btn_no.configure(state="disabled")

        def _worker():
            try:
                # Risolvi URL completo se relativo
                exe_url = info.get("exe_url", "")
                if exe_url.startswith("/"):
                    exe_url = api_client.server_url.rstrip("/") + exe_url
                if not exe_url:
                    raise ValueError("exe_url vuoto nella response del server")

                def _on_progress(done, total):
                    if total > 0:
                        pct = int(done * 100 / total)
                        win.after(0, lambda: status_var.set(
                            f"Download {pct}% ({done//1024} / {total//1024} KB)"))
                    else:
                        win.after(0, lambda: status_var.set(
                            f"Download {done//1024} KB..."))

                new_exe = _download_to_temp(
                    exe_url,
                    expected_sha256=info.get("sha256"),
                    progress_cb=_on_progress,
                )
                win.after(0, lambda: status_var.set("Preparo l'aggiornamento..."))

                current_exe = get_current_exe_path()
                if current_exe is None:
                    raise RuntimeError("Modalità script — niente da aggiornare")

                script = _make_windows_updater_script(current_exe, new_exe)

                # Lancia il batch in background detached, poi chiudi l'app
                import subprocess
                subprocess.Popen(
                    ["cmd.exe", "/c", "start", "", "/MIN", str(script)],
                    creationflags=0x00000008,  # DETACHED_PROCESS
                    close_fds=True,
                )
                win.after(0, lambda: (
                    status_var.set("✓ Aggiornamento avviato. L'app si riavvierà..."),
                    parent_window.after(800, lambda: (
                        win.destroy(),
                        parent_window.quit(),
                        parent_window.destroy(),
                    )),
                ))
            except Exception as e:
                win.after(0, lambda: (
                    status_var.set(f"Errore: {e}"),
                    btn_yes.configure(state="normal", text="Riprova"),
                    btn_no.configure(state="normal"),
                ))

        threading.Thread(target=_worker, daemon=True).start()

    btn_no = ctk.CTkButton(
        btn_row, text="Più tardi" if not is_mandatory else "Annulla",
        width=120, height=34,
        fg_color="transparent", hover_color=PALETTE["surface"],
        text_color=PALETTE["text_dim"], font=("Segoe UI", 10),
        command=win.destroy,
    )
    btn_no.pack(side="right", padx=(4, 0))

    btn_yes = ctk.CTkButton(
        btn_row, text="Aggiorna ora", width=140, height=34,
        fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
        text_color="#ffffff", font=("Segoe UI", 10, "bold"),
        command=_do_update,
    )
    btn_yes.pack(side="right")

    if is_mandatory:
        # Mandatory: l'utente non può chiudere senza aggiornare
        ctk.CTkLabel(
            win, text="⚠  Questo aggiornamento è obbligatorio",
            font=("Segoe UI", 10, "bold"),
            text_color="#d84545"
        ).pack(side="bottom", pady=(0, 8))
        win.protocol("WM_DELETE_WINDOW", lambda: None)  # blocca X
