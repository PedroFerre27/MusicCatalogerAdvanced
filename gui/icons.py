"""
icons.py — v1073: mappa icone custom completa (62 PNG)

v1085n: path resolution robusto che gestisce sia modalità script
che PyInstaller onefile (sys._MEIPASS).
"""
import sys
from pathlib import Path
from functools import lru_cache
import customtkinter as ctk


def _resolve_icon_dir() -> Path:
    """v1085n: in PyInstaller onefile, le risorse sono in sys._MEIPASS,
    non in `Path(__file__).parent.parent`. In modalità script normale
    invece sì."""
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return Path(meipass) / "icons" / "app"
    return Path(__file__).parent.parent / "icons" / "app"


_ICON_DIR = _resolve_icon_dir()

_ICON_MAP = {
    # ── Stat bar ─────────────────────────────────────────────────────────
    "processati":     "processati",
    "spostati":       "moved",
    "aggiornati":     "lapis",
    "cover_stat":     "cover",
    "non_cat":        "warning",
    # ── Tab bar ──────────────────────────────────────────────────────────
    "log":            "log",
    "db_locale":      "localdb",
    "generi":         "genres",
    "cache":          "cache",
    "qualita":        "quality_icon",
    "caraibica":      "caribbean_top",
    "avanzate":       "advanced",
    # ── Sidebar ──────────────────────────────────────────────────────────
    "directory":      "library",
    "folder":         "library",
    "opzioni":        "settings2",
    "gestione_dup":   "duplicates",
    "cover_album":    "cover",
    "title_icon":     "taskbar_active",
    # ── Pulsanti principali ───────────────────────────────────────────────
    "avvia":          "analyze2",
    "ferma":          "warning",
    "reload":         "reload",
    "reload2":        "reload2",
    # ── Priorità (su/giù) ────────────────────────────────────────────────
    "up":             "up",
    "down":           "down",
    # ── Manutenzione ─────────────────────────────────────────────────────
    "csv":            "csv",
    "find_dups":      "find_dups",
    "svuota_cache":   "clear_cache",
    "open_folder":    "data_folder",
    "m3u":            "m3u",
    "rinomina_b":     "batch_rename",
    "replaygain":     "replaygain",
    "integrity":      "integrity",
    # ── Caraibica ────────────────────────────────────────────────────────
    "classify":       "classify_priority",
    "bpm_range":      "analyze",
    "velocita_bpm":   "velocita_bpm",
    "artisti_noti":   "artisti_noti",
    "indicatori":     "indicatori_testuali",
    # ── Avanzate ─────────────────────────────────────────────────────────
    "settings":       "settings2",
    "metadata_icon":  "metadata",
    "online_db":      "online_db",
    "cover_icon":     "albums",
    "adv_maint":      "advanced2",
    "adv_library":    "library2",
    "adv_cover":      "albums",
    "adv_sources":    "online_db",
    "adv_classify":   "classify",
    "adv_rename":     "rename",
    # ── Misc ─────────────────────────────────────────────────────────────
    "albums":         "albums",
    "warning":        "warning",
    "simulation":     "simulation",
    "debug":          "debug",
    # ── Phosphor icons (v1077) — flyout profilo e funzioni correlate ─────
    "profile":        "ph-user",
    "settings_ph":    "ph-gear",
    "lang":           "ph-translate",
    "plans":          "ph-crown",
    "help_ph":        "ph-question",
    "logout":         "ph-sign-out",
}


@lru_cache(maxsize=256)
def get_icon(name: str, size: int = 32) -> "ctk.CTkImage | None":
    """v1085n: caricamento icone più robusto.

    Cambiamenti:
    - Verifica che il PNG abbia transparency (RGBA mode) prima di
      restituirlo. Se è in modalità RGB pura senza alpha → conversione
      esplicita aggiungendo canale alpha pieno (255) per evitare
      sfondi neri/bianchi su tema scuro.
    - Se PIL.Image.open() fallisce o il file è troncato/corrotto
      (capita con OneDrive sync), ritorna None invece di immagine
      nera. CustomTkinter mostrerà solo testo del bottone — meglio
      che icona corrotta.
    """
    from PIL import Image, UnidentifiedImageError
    png_name = _ICON_MAP.get(name, name)
    png_path = _ICON_DIR / f"{png_name}.png"
    if not png_path.exists():
        png_path = _ICON_DIR / f"{name}.png"
    if not png_path.exists():
        return None
    try:
        img = Image.open(str(png_path))
        # Forza conversione RGBA per garantire canale alpha.
        # Se il PNG è già RGBA, convert("RGBA") è no-op.
        # Se è RGB (no alpha) o L (grayscale), aggiunge alpha=255.
        if img.mode != "RGBA":
            img = img.convert("RGBA")
        img = img.resize((size, size), Image.LANCZOS)
        return ctk.CTkImage(light_image=img, dark_image=img, size=(size, size))
    except (UnidentifiedImageError, OSError, ValueError) as e:
        # File corrotto/incompleto (es. OneDrive sync interrotto durante build)
        print(f"[icons] Icona corrotta per '{name}' ({png_path}): {e}")
        return None
    except Exception as e:
        print(f"[icons] Errore caricamento '{name}': {e}")
        return None


def icon_button(parent, icon_name: str, text: str = "", size: int = 28, **kwargs):
    img = get_icon(icon_name, size)
    t = ("  " + text) if (img and text) else text
    return ctk.CTkButton(parent, image=img, text=t, compound="left", **kwargs)


def icon_label(parent, icon_name: str, text: str = "", size: int = 28, **kwargs):
    img = get_icon(icon_name, size)
    t = ("  " + text) if (img and text) else text
    return ctk.CTkLabel(parent, image=img, text=t, compound="left", **kwargs)
