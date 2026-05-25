"""
singleton.py — single-instance lock per TrackLab.

v1086.2: implementa il "lock di istanza singola". Se l'utente prova ad
aprire l'EXE quando un'altra istanza e' gia' in esecuzione:
  - acquire() ritorna False
  - viene chiamata bring_existing_to_front() che porta in primo piano
    la finestra dell'istanza esistente (Windows: FindWindow + SetForeground)

Strategia (multi-piattaforma):
- Windows: socket TCP su 127.0.0.1:PORT. Se la porta e' gia' bound
  da un'altra istanza, sappiamo che e' in esecuzione. Vantaggi rispetto
  a un lock file: il SO rilascia automaticamente la porta quando il
  processo muore (anche se crash), nessun PID stale.
- Linux/macOS: stesso approccio (socket).

Il numero di porta e' deterministico ma alto e poco probabile collisione.
Se la porta e' occupata da un altro programma (raro), `acquire()` ritorna
False e l'utente vedra' un dialog "Already running" anche se non c'e'
nessuna istanza TrackLab. E' un edge case accettabile.
"""
from __future__ import annotations
import socket
import sys
from typing import Optional

# Porta TCP "magica" per il lock. Scelto un numero alto e specifico per
# minimizzare collisioni con altri servizi. Se in futuro qualche altro
# software usa questa porta, basta cambiarla qui.
SINGLETON_PORT = 47286
SINGLETON_HOST = "127.0.0.1"

# Variabile globale per tenere il socket vivo per tutta la durata del
# processo (se cade fuori scope, il SO rilascia la porta e un'altra
# istanza puo' partire).
_singleton_socket: Optional[socket.socket] = None


def acquire() -> bool:
    """
    Tenta di acquisire il lock di istanza singola.
    Returns:
        True  → questa e' l'unica istanza, possiamo procedere
        False → un'altra istanza e' gia' in esecuzione
    """
    global _singleton_socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # SO_REUSEADDR NON va settato qui: vogliamo proprio fallire se
        # un'altra istanza tiene la porta.
        s.bind((SINGLETON_HOST, SINGLETON_PORT))
        s.listen(1)
        _singleton_socket = s  # tenuta viva per tutta la sessione
        return True
    except OSError:
        # Porta gia' occupata → un'altra istanza e' attiva (o un altro
        # programma sta usando questa porta — edge case accettabile)
        return False


def bring_existing_to_front() -> bool:
    """
    Porta in primo piano la finestra dell'istanza TrackLab gia'
    in esecuzione. Solo Windows per ora — su Linux/macOS no-op.

    Returns:
        True se la finestra e' stata trovata e portata in foreground,
        False altrimenti.
    """
    if sys.platform != "win32":
        return False
    try:
        import ctypes
        from ctypes import wintypes

        # Cerca finestra per titolo. Il titolo della GUI e' "Music
        # Cataloger | <Plan>" (impostato in main_window.py). Uso
        # `EnumWindows` per scansionare e trovare il primo match.
        user32 = ctypes.WinDLL("user32", use_last_error=True)

        EnumWindows = user32.EnumWindows
        EnumWindowsProc = ctypes.WINFUNCTYPE(
            wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
        GetWindowTextW = user32.GetWindowTextW
        IsWindowVisible = user32.IsWindowVisible
        ShowWindow = user32.ShowWindow
        SetForegroundWindow = user32.SetForegroundWindow

        SW_RESTORE = 9
        target_hwnd = [None]

        def callback(hwnd, _lparam):
            if not IsWindowVisible(hwnd):
                return True
            length = user32.GetWindowTextLengthW(hwnd)
            if length == 0:
                return True
            buff = ctypes.create_unicode_buffer(length + 1)
            GetWindowTextW(hwnd, buff, length + 1)
            title = buff.value
            if title.startswith("TrackLab"):
                target_hwnd[0] = hwnd
                return False  # interrompo enum
            return True

        EnumWindows(EnumWindowsProc(callback), 0)
        if target_hwnd[0] is None:
            return False

        # Restore se minimizzata + bring to front
        ShowWindow(target_hwnd[0], SW_RESTORE)
        SetForegroundWindow(target_hwnd[0])
        return True
    except Exception:
        return False


def show_already_running_dialog():
    """
    Mostra un dialog nativo "TrackLab e' gia' in esecuzione".
    Usa MessageBox di Windows per evitare di dover importare Tk (che e'
    pesante e rallenterebbe il check di doppia istanza).
    """
    if sys.platform == "win32":
        try:
            import ctypes
            ctypes.windll.user32.MessageBoxW(
                0,
                "TrackLab e' gia' in esecuzione.\n\n"
                "L'istanza esistente verra' portata in primo piano.",
                "TrackLab",
                0x40,  # MB_ICONINFORMATION
            )
        except Exception:
            print("TrackLab e' gia' in esecuzione.")
    else:
        print("TrackLab e' gia' in esecuzione.")
