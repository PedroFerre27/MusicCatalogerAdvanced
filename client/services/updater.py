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
    dest = Path(tempfile.gettempdir()) / "tracklab_update" / fname
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

    v1085p: torno alla strategia v1085f (copy /Y semplice) + env vars
    PyInstaller cleanup. Le strategie rename-and-replace e i 50 righe
    di commenti REM Unicode introdotti in v1085m...o creavano problemi
    nuovi (cp1252 encoding error sulla freccia U+2192) senza risolvere
    il bug di base.

    Cosa cambia rispetto a v1085f:
    - Aggiunto env cleanup PyInstaller (causa root crash python313.dll)
    - Logging completo per debug
    - Solo ASCII nei commenti REM (cp1252 limit)
    """
    if sys.platform != "win32":
        raise RuntimeError("Updater batch script disponibile solo su Windows")

    script_path = Path(tempfile.gettempdir()) / "tracklab_updater.bat"
    log_file = Path(tempfile.gettempdir()) / "tracklab_updater.log"

    # Strategia: copy /Y semplice come v1085f con retry per gestire lock.
    # Niente rename, niente .old backup, niente roba "atomica":
    # PyInstaller onefile gestisce gia' il proprio lock automaticamente
    # perche' il vecchio EXE chiude i suoi handle prima che il batch
    # parta (il batch aspetta 2 sec per sicurezza).
    content = f"""@echo off
REM TrackLab auto-updater (v1085p: stile v1085f + env cleanup)

setlocal
set LOG="{log_file}"
echo. >> %LOG%
echo ============================================== >> %LOG%
echo [%DATE% %TIME%] Avvio updater v1085p >> %LOG%
echo   current_exe = {current_exe} >> %LOG%
echo   new_exe     = {new_exe} >> %LOG%

REM v1086.1: niente piu' echo verso stdout (la console e' nascosta con
REM CREATE_NO_WINDOW lato Python; tutto quel che serve va nel %LOG%).
REM v1086.2 fix critico: sostituito `timeout /t N /nobreak` con
REM `ping -n N+1 127.0.0.1`. Il comando `timeout` di Windows fallisce
REM IMMEDIATAMENTE con "il reindirizzamento dell'input non e'
REM supportato" quando stdin del batch e' ridiretto a NUL (cioe' nel
REM nostro caso con subprocess.DEVNULL). Risultato: i 30 retry copy
REM duravano in totale ~1 secondo invece che 30, e l'app non aveva
REM tempo di chiudere → copia falliva sempre.
REM `ping -n 3` fa 3 ping a 127.0.0.1 che durano ~2 secondi totali e
REM non ha problemi con stdin ridiretto. E' il workaround standard.
REM Aspetto ~2 sec che il vecchio processo abbia chiuso tutti i handle
ping -n 3 127.0.0.1 >nul

REM Retry copy fino a 30 sec (in caso di OneDrive/AV transient lock)
set RETRIES=0
:RETRY
echo [%DATE% %TIME%] Tentativo %RETRIES% copy >> %LOG%
copy /Y "{new_exe}" "{current_exe}" >> %LOG% 2>&1
if %errorlevel%==0 goto SUCCESS
set /a RETRIES+=1
if %RETRIES% GEQ 30 goto FAIL
REM ping -n 2 = ~1 sec di attesa fra retry
ping -n 2 127.0.0.1 >nul
goto RETRY

:SUCCESS
echo [%DATE% %TIME%] Copy OK >> %LOG%
del "{new_exe}" >nul 2>&1

REM ENV CLEANUP critico: il batch eredita env vars PyInstaller dal
REM processo Python che lo ha lanciato. Se le passa al nuovo EXE,
REM il bootloader si confonde e cerca DLL in path inesistenti.
REM Fix: clear tutte le env vars PyInstaller note.
set "_PYI_APPLICATION_HOME_DIR="
set "_MEIPASS2="
set "_PYI_ARCHIVE_FILE="
set "_PYIBOOT_USER_PYTHONPATH="
set "_PYI_SPLASH_IPC="
echo [%DATE% %TIME%] env vars PyInstaller cleared >> %LOG%

REM v1086.2 — Lancio del nuovo EXE in modo VISIBILE.
REM Problema: il batch e' partito con STARTUPINFO+SW_HIDE da Python,
REM quindi `start "" <exe>` eredita SW_HIDE → nuovo EXE invisibile.
REM Soluzione classica Windows: lanciare via explorer.exe, che
REM "stacca" il processo figlio dal contesto di hide del padre e
REM lo lancia con il window state default (visibile).
REM Vedi: https://stackoverflow.com/q/29903706
REM Fallback: se explorer.exe fallisce per qualche motivo, prova
REM start "" come backup (potrebbe non essere visibile, ma almeno
REM l'EXE gira e l'utente puo' Alt-Tabbarci sopra).
explorer.exe "{current_exe}"
if %errorlevel% NEQ 0 (
    echo [%DATE% %TIME%] explorer fallito errlvl=%errorlevel%, fallback start >> %LOG%
    start "" /D "{current_exe.parent}" "{current_exe}"
)
echo [%DATE% %TIME%] launch emesso, errorlevel=%errorlevel% >> %LOG%
echo [%DATE% %TIME%] Updater terminato OK >> %LOG%
exit /b 0

:FAIL
echo [%DATE% %TIME%] FAIL - copy non riuscita dopo 30 tentativi >> %LOG%
REM v1086.1: niente piu' pause — la console e' nascosta, l'utente non
REM vedrebbe il prompt e il batch resterebbe in stallo per sempre.
REM L'errore e' loggato in %LOG% per debug.
exit /b 1
"""
    # cp1252 funziona perche' ho rimosso tutti i caratteri non-ASCII
    # dai commenti. Ma se in futuro qualcuno aggiunge accenti nei path
    # (es. "C:\\Users\\Pedro Marqueš\\..."), cp1252 fallisce. Quindi
    # provo cp1252, poi fallback a utf-8 con BOM.
    try:
        script_path.write_text(content, encoding="cp1252")
    except UnicodeEncodeError:
        script_path.write_text("\ufeff" + content, encoding="utf-8")
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
            new_exe_path = None  # tracking per fallback manuale
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
                new_exe_path = new_exe   # salvato per fallback
                win.after(0, lambda: status_var.set("Preparo l'aggiornamento..."))

                current_exe = get_current_exe_path()
                if current_exe is None:
                    raise RuntimeError("Modalità script — niente da aggiornare")

                script = _make_windows_updater_script(current_exe, new_exe)

                # v1086.1 (revisione): la combo CREATE_NO_WINDOW |
                # DETACHED_PROCESS NON nasconde affidabilmente la console
                # di un .bat (DETACHED stacca dal padre, ma il batch crea
                # comunque la propria console). Su Windows il modo corretto
                # per nascondere completamente la finestra di un processo
                # figlio e' usare STARTUPINFO con wShowWindow=SW_HIDE.
                import subprocess
                CREATE_NO_WINDOW = 0x08000000
                startupinfo = subprocess.STARTUPINFO()
                # STARTF_USESHOWWINDOW = 0x00000001
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = subprocess.SW_HIDE
                subprocess.Popen(
                    [str(script)],
                    creationflags=CREATE_NO_WINDOW,
                    startupinfo=startupinfo,
                    close_fds=True,
                    shell=False,
                    # Reindirizzo I/O standard a NUL per evitare che il
                    # batch erediti handle del processo padre (che si sta
                    # chiudendo). Senza questo, su alcune configurazioni
                    # Windows il batch puo' fallire silenziosamente.
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
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
                _log(f"_do_update fallito: {e}")
                # v1085n: in caso di errore, mostra il path dell'EXE
                # scaricato (se c'è) così l'utente può sostituirlo a mano.
                # Questo è il fallback per uscire dal "loop circolare":
                # client rotto → updater rotto → client non si aggiorna.
                fallback_msg = f"Errore aggiornamento: {e}"
                if new_exe_path and new_exe_path.exists():
                    current = get_current_exe_path()
                    fallback_msg = (
                        f"L'aggiornamento automatico è fallito.\n\n"
                        f"Errore: {e}\n\n"
                        f"PROCEDURA MANUALE:\n"
                        f"1. Chiudi questa finestra\n"
                        f"2. Chiudi TrackLab\n"
                        f"3. Copia il file scaricato sopra quello corrente:\n"
                        f"   FROM: {new_exe_path}\n"
                        f"   TO:   {current}\n"
                        f"4. Riapri TrackLab"
                    )
                    win.after(0, lambda: _show_fallback_manual(
                        win, parent_window, new_exe_path, current, str(e)))
                else:
                    win.after(0, lambda: (
                        status_var.set(fallback_msg),
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


def _show_fallback_manual(parent_dialog, main_window,
                           new_exe_path: Path, current_exe: Path,
                           error_msg: str) -> None:
    """v1085n: dialog di fallback quando l'update automatico fallisce.

    Mostra path FROM/TO e apre cartella di entrambi al click. L'utente
    può sostituire il file a mano. Necessario per uscire dal loop
    "updater rotto → client non si aggiorna mai".
    """
    import customtkinter as ctk
    import subprocess as _sub

    # Distruggo il dialog di update (con il "Scarico..." in error state)
    try: parent_dialog.destroy()
    except Exception: pass

    fb = ctk.CTkToplevel(main_window)
    fb.title("Aggiornamento manuale richiesto")
    fb.geometry("640x420")
    fb.resizable(False, False)
    try:
        from gui.app_icon import set_window_icon
        set_window_icon(fb)
    except Exception: pass
    try:
        fb.lift(); fb.focus_force()
    except Exception: pass

    # Header
    ctk.CTkLabel(fb, text="⚠  Aggiornamento automatico fallito",
                 font=("Segoe UI", 14, "bold"),
                 text_color="#e8a62b").pack(pady=(20, 6))

    # Errore
    ctk.CTkLabel(fb, text=f"Errore: {error_msg}",
                 font=("Segoe UI", 9),
                 text_color="#999999",
                 wraplength=580).pack(pady=(0, 16))

    # Istruzioni
    instructions = (
        "Per applicare l'aggiornamento manualmente:\n\n"
        "  1. Chiudi TrackLab (questa finestra + finestra principale)\n"
        "  2. Apri la cartella 'File scaricato' (sotto)\n"
        "  3. Copia il file Music_Cataloger_*.exe\n"
        "  4. Apri la cartella 'App corrente' (sotto)\n"
        "  5. Incolla sostituendo il file esistente\n"
        "  6. Riapri TrackLab"
    )
    ctk.CTkLabel(fb, text=instructions, font=("Consolas", 9),
                 justify="left", anchor="w").pack(padx=24, pady=(0, 16),
                                                   fill="x")

    # Path display
    paths_frame = ctk.CTkFrame(fb, fg_color="#1a1a2e")
    paths_frame.pack(padx=24, pady=(0, 16), fill="x")
    ctk.CTkLabel(paths_frame, text=f"FROM: {new_exe_path}",
                 font=("Consolas", 8), text_color="#88c0d0",
                 wraplength=580).pack(padx=12, pady=(8, 4), anchor="w")
    ctk.CTkLabel(paths_frame, text=f"TO:   {current_exe}",
                 font=("Consolas", 8), text_color="#a3be8c",
                 wraplength=580).pack(padx=12, pady=(0, 8), anchor="w")

    # Btn row
    btn_row = ctk.CTkFrame(fb, fg_color="transparent")
    btn_row.pack(pady=(0, 16))

    def _open_folder(path: Path):
        try:
            _sub.Popen(["explorer", "/select,", str(path)])
        except Exception as e:
            _log(f"open_folder failed: {e}")

    ctk.CTkButton(btn_row, text="📂  Apri 'File scaricato'", width=200,
                  command=lambda: _open_folder(new_exe_path)
                  ).pack(side="left", padx=4)
    ctk.CTkButton(btn_row, text="📂  Apri 'App corrente'", width=200,
                  command=lambda: _open_folder(current_exe)
                  ).pack(side="left", padx=4)
    ctk.CTkButton(btn_row, text="Chiudi", width=100,
                  fg_color="transparent", text_color="#999",
                  command=fb.destroy).pack(side="left", padx=4)
