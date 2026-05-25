"""
gui/app_icon.py — Helper per impostare l'icona di finestre CTk.

Gestisce:
- Path resolution sia in sviluppo (__file__.parent) che dentro EXE
  PyInstaller (sys._MEIPASS — la cartella temporanea dove PyInstaller
  estrae i data files a runtime).
- Fallback intelligente: prima .ico (Windows nativo), poi .png via PIL.
- Mantenimento del riferimento PhotoImage (tk fa GC altrimenti l'icona
  sparisce).

Uso:
    from gui.app_icon import set_window_icon
    set_window_icon(self.root)   # per CTk principale
    set_window_icon(toplevel)    # per CTkToplevel / tk.Toplevel
"""
from __future__ import annotations
import sys
from pathlib import Path
from typing import Optional


def _resource_root() -> Path:
    """
    Ritorna la root dove cercare i file statici (icons/, data/).

    - In sviluppo: la cartella del progetto (2 livelli sopra questo file)
    - In PyInstaller onedir: sys._MEIPASS (cartella temp di estrazione)
    - In PyInstaller onefile: sys._MEIPASS (cartella temp di estrazione)
    """
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return Path(meipass)
    return Path(__file__).parent.parent


def find_icon_file() -> Optional[Path]:
    """
    Cerca un file icona utilizzabile, in ordine di preferenza.
    Su Windows .ico ha la priorità (nativo per iconbitmap).
    """
    root = _resource_root()
    candidates = [
        root / "icons" / "tracklab.ico",
        root / "icons" / "app" / "tracklab.ico",
        root / "icons" / "app" / "taskbar_active.png",
        root / "icons" / "app" / "app_icon_256.png",
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


def set_window_icon(window) -> bool:
    """
    Imposta l'icona di una finestra tk/ctk. Ritorna True se ha avuto successo.

    Il riferimento PhotoImage (per PNG) è salvato come attributo della finestra
    per evitare il garbage collection che fa sparire l'icona.

    v1085f: applica SIA iconbitmap (per la qualità nativa Windows
    in taskbar/titolo) SIA iconphoto con default=True (per propagare
    l'icona ai dialog messagebox figli, che altrimenti userebbero
    l'icona di tkinter di default).
    """
    icon_path = find_icon_file()
    if icon_path is None:
        return False

    success = False
    try:
        if icon_path.suffix.lower() == ".ico":
            # Windows: iconbitmap nativo per taskbar
            try:
                window.iconbitmap(str(icon_path))
                success = True
            except Exception:
                pass

        # In aggiunta: iconphoto con default=True per propagare ai messagebox
        try:
            from PIL import Image, ImageTk
        except ImportError:
            return success

        # Cerca il PNG corrispondente (alta qualità multi-resolution)
        # Preferenza: taskbar_active.png 256x256, poi convert l'.ico
        root = _resource_root()
        png_candidates = [
            root / "icons" / "app" / "taskbar_active.png",
            root / "icons" / "app" / "app_icon_256.png",
            root / "icons" / "app" / "app_icon.png",
        ]
        png_path = next((p for p in png_candidates if p.exists()), None)

        if png_path is not None:
            img = Image.open(str(png_path)).convert("RGBA")
        else:
            # Fallback: usa .ico
            img = Image.open(str(icon_path)).convert("RGBA")

        # 64x64 è un buon compromesso: nitido in titlebar e messagebox
        # senza essere enorme per gli usi piccoli
        if img.size != (64, 64):
            img = img.resize((64, 64), Image.LANCZOS)
        photo = ImageTk.PhotoImage(img)
        # default=True propaga a TUTTI i nuovi Toplevel del processo
        # inclusi i messagebox standard di tkinter
        window.iconphoto(True, photo)
        # Mantieni riferimento per evitare GC
        window._app_icon_ref = photo
        return True

    except Exception as e:
        # Log silenzioso — non è fatale se l'icona non si carica
        print(f"[app_icon] Could not set window icon: {e}")
        return success


def get_title_icon_photo(size: int = 40):
    """
    Ritorna un CTkImage/PhotoImage da usare come 'logo' accanto al titolo
    nella GUI (header TrackLab). Preferisce taskbar_active.png se
    presente — altrimenti fallback a tracklab.ico via PIL.
    """
    root = _resource_root()
    candidates = [
        root / "icons" / "app" / "taskbar_active.png",
        root / "icons" / "app" / "app_icon_256.png",
        root / "icons" / "tracklab.ico",
    ]
    icon_path = next((c for c in candidates if c.exists()), None)
    if icon_path is None:
        return None

    try:
        from PIL import Image
        import customtkinter as ctk
        img = Image.open(str(icon_path))
        return ctk.CTkImage(light_image=img, dark_image=img, size=(size, size))
    except Exception:
        return None
