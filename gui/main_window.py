"""
Music Cataloger Advanced — GUI
"""

import os
import queue
import re
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

import customtkinter as ctk
try:
    from gui.icons import get_icon as _get_icon
    _ICONS_AVAILABLE = True
except Exception:
    _ICONS_AVAILABLE = False
    def _get_icon(name, size=24): return None
from tkinter import filedialog, messagebox

# ─── VERSIONE — modifica qui per aggiornare titolo e About ───────────────────
# v1056: versione centralizzata in version.py
try:
    import sys as _sys, os as _os
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
    from version import APP_VERSION  # v1057
except ImportError:
    APP_VERSION = "v1073"  # fallback
# ─────────────────────────────────────────────────────────────────────────

def _get_data_dir() -> Path:
    """v1049: cartella dati centralizzata (data/ nella directory progetto)."""
    if hasattr(sys, '_MEIPASS'):
        sd = Path(sys.executable).parent
    else:
        sd = Path(__file__).parent.parent.absolute()
    d = sd / "data"
    d.mkdir(parents=True, exist_ok=True)
    return d

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

PALETTE = {
    "bg":            "#0A1520",   # quasi nero-blu, molto desaturato
    "surface":       "#0F1E2E",   # pannelli: grigio-blu molto scuro
    "surface2":      "#152438",   # sezioni interne
    "accent":        "#A03030",   # accento rosso (usato raramente)
    "primary":       "#3A6EA8",   # blu acciaio desaturato (non elettrico)
    "primary_hover": "#2E5E95",   # hover meno saturo
    "success":       "#3D8A58",   # verde muted
    "warning":       "#B07820",   # ambra muted
    "error":         "#9A3A3A",   # rosso muted
    "text":          "#B8CCDF",   # testo principale: bianco-blu molto soft
    "text_dim":      "#5A7A95",   # label secondarie: grigio-blu
    "border":        "#152030",   # bordi quasi invisibili
    "progress_bg":   "#101C2A",
    # Colori log per tipo riga
    # v1029: WARNING → ambra (era rosso, ora giallo/ambra come da standard logging)
    #        ERROR   → rosso muted (invariato)
    "log_info":      "#8AAABF",   # INFO: grigio-azzurro neutro
    "log_debug":     "#2A4255",   # DEBUG: quasi invisibile (non distrae)
    "log_warning":   "#C8922A",   # WARNING: ambra-arancio ben visibile (era #B08040 troppo scuro)
    "log_error":     "#C05050",   # ERROR: rosso muted (era #9A5050, alzato per contrasto)
    "log_progress":  "#3A8A68",   # PROGRESS X/Y: verde acqua desaturato
    "log_file":      "#4A7EA0",   # *** file ***: azzurro medio
    "log_tree":      "#3A6070",   # >-- step: azzurro grigio scuro
    "log_result":    "#3A7055",   # \\-- risultato: verde grigio
}

FONT_TITLE = ("Segoe UI", 20, "bold")
FONT_HEAD  = ("Segoe UI", 13, "bold")
FONT_BODY  = ("Segoe UI", 11)
FONT_SMALL = ("Segoe UI", 10)
FONT_MONO  = ("Consolas", 10)
BTN_H = 36


# ─── WIDGET: LabeledProgressBar ──────────────────────────────────────────────

class LabeledProgressBar(ctk.CTkFrame):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, fg_color="transparent", **kwargs)

        self._pct_var   = ctk.StringVar(value="0%")
        self._file_var  = ctk.StringVar(value="In attesa...")
        self._phase_var = ctk.StringVar(value="")
        self._eta_var   = ctk.StringVar(value="")  # v1045: ETA dedicata

        top = ctk.CTkFrame(self, fg_color="transparent")
        top.pack(fill="x", pady=(0, 2))

        self._phase_label = ctk.CTkLabel(
            top, textvariable=self._phase_var,
            font=("Segoe UI", 10, "bold"), text_color=PALETTE["warning"]
        )
        self._phase_label.pack(side="left")

        self._file_label = ctk.CTkLabel(
            top, textvariable=self._file_var,
            font=FONT_SMALL, text_color=PALETTE["text_dim"], anchor="w"
        )
        self._file_label.pack(side="left", fill="x", expand=True, padx=(8, 0))

        # v1051: ordine pack right: prima % (estrema destra), poi ETA (alla sua sinistra)
        # Con side="right" Tkinter impacca da destra: l'ultimo packato è il più a sinistra
        self._pct_label = ctk.CTkLabel(
            top, textvariable=self._pct_var,
            font=("Segoe UI", 11, "bold"),
            text_color=PALETTE["primary"], width=48, anchor="e"
        )
        self._pct_label.pack(side="right")

        # ETA: packato dopo % → si posiziona alla sua sinistra → "⏱ 5m  42%"
        self._eta_label = ctk.CTkLabel(
            top, textvariable=self._eta_var,
            font=("Segoe UI", 10), text_color=PALETTE["text_dim"],
            anchor="e",
        )
        self._eta_label.pack(side="right", padx=(0, 6))

        self._bar = ctk.CTkProgressBar(
            self, height=12,
            fg_color=PALETTE["progress_bg"],
            progress_color=PALETTE["primary"],
            corner_radius=6,
        )
        self._bar.pack(fill="x")
        self._bar.set(0)

    def set_eta(self, eta_str: str) -> None:
        """v1045: aggiorna la stima ETA visibile nella barra."""
        if eta_str:
            self._eta_var.set(f"⏱ {eta_str}")
        else:
            self._eta_var.set("")

    def update(self, current: int, total: int, filename: str = "", phase: str = ""):
        if total > 0:
            pct = min(1.0, current / total)
            self._bar.set(pct)
            self._pct_var.set(f"{int(pct * 100)}%")
        if filename:
            short = filename if len(filename) <= 50 else "..." + filename[-47:]
            self._file_var.set(short)
        if phase:
            labels = {
                'catalogazione': 'Fase 1/2 — Catalogazione',
                'classifica_salsa': 'Fase 2/2 — Classifica Salsa',
            }
            self._phase_var.set(labels.get(phase, phase))

    def reset(self):
        self._bar.set(0)
        self._pct_var.set("0%")
        self._file_var.set("In attesa...")
        self._phase_var.set("")
        self._eta_var.set("")

    def complete(self):
        self._bar.set(1.0)
        self._pct_var.set("100%")
        self._file_var.set("Completato ✓")
        self._phase_var.set("")
        self._eta_var.set("")


# ─── WIDGET: LogViewer ───────────────────────────────────────────────────────

class LogViewer(ctk.CTkTextbox):
    TAG_COLORS = {
        "ERROR":    PALETTE["error"],
        "WARNING":  PALETTE["warning"],
        "INFO":     PALETTE["log_info"],
        "DEBUG":    PALETTE["log_debug"],
        "SUCCESS":  PALETTE["success"],
        "FILE":     PALETTE["log_file"],     # *** file ***
        "TREE":     PALETTE["log_tree"],     # >-- step
        "RESULT":   PALETTE["log_result"],   # \-- risultato
        "PROGRESS": PALETTE["log_progress"], # PROGRESS X/Y
    }

    def __init__(self, parent, **kwargs):
        super().__init__(parent, font=FONT_MONO, wrap="word", state="disabled",
                         fg_color=PALETTE["surface"], text_color=PALETTE["text"], **kwargs)
        for tag, color in self.TAG_COLORS.items():
            self._textbox.tag_config(tag, foreground=color)

    def _detect_tag(self, text: str, level: str) -> str:
        """Rileva il tag colore in base al contenuto della riga."""
        if level.upper() == "DEBUG":
            return "DEBUG"
        stripped = text.strip()
        if "***" in stripped and ".mp3" in stripped.lower():
            return "FILE"
        if stripped.startswith(">--"):
            return "TREE"
        if stripped.startswith("\\--") or stripped.startswith("└--"):
            return "RESULT"
        if "PROGRESS:" in stripped:
            return "PROGRESS"
        if level.upper() == "ERROR":
            return "ERROR"
        if level.upper() == "WARNING":
            return "WARNING"
        if level.upper() in ("SUCCESS",):
            return "SUCCESS"
        return "INFO"

    def append(self, text: str, level: str = "INFO"):
        self.configure(state="normal")
        tag = self._detect_tag(text, level)
        self._textbox.insert("end", text + "\n", tag)
        self._textbox.see("end")
        self.configure(state="disabled")

    def clear(self):
        self.configure(state="normal")
        self.delete("1.0", "end")
        self.configure(state="disabled")


# ─── WIDGET: StatCard ─────────────────────────────────────────────────────────

class StatCard(ctk.CTkFrame):
    def __init__(self, parent, icon: str, label: str, value: str = "0",
                 icon_name: str = "", **kwargs):
        """icon = emoji fallback, icon_name = nome icona custom (caricata da icons.py)"""
        super().__init__(parent, fg_color=PALETTE["surface"], corner_radius=10,
                         border_width=1, border_color=PALETTE["border"], **kwargs)
        inner = ctk.CTkFrame(self, fg_color="transparent")
        inner.pack(padx=14, pady=10, fill="both", expand=True)
        # Usa icona custom se disponibile, altrimenti emoji
        _img = None
        if icon_name:
            try:
                from gui.icons import get_icon as _gi
                _img = _gi(icon_name, 32)
            except Exception:
                pass
        if _img:
            ctk.CTkLabel(inner, text="", image=_img).pack(anchor="w")
        else:
            ctk.CTkLabel(inner, text=icon, font=("Segoe UI", 22)).pack(anchor="w")
        self._val_var = ctk.StringVar(value=value)
        ctk.CTkLabel(inner, textvariable=self._val_var,
                     font=("Segoe UI", 26, "bold"), text_color=PALETTE["primary"]).pack(anchor="w")
        ctk.CTkLabel(inner, text=label,
                     font=FONT_SMALL, text_color=PALETTE["text_dim"]).pack(anchor="w")

    def set(self, value):
        self._val_var.set(str(value))

    def get(self) -> int:
        """v1085l: ritorna il valore corrente come int. Usato dal poll
        per il payload di /catalog/{id}/complete."""
        try:
            return int(self._val_var.get())
        except (ValueError, TypeError):
            return 0


# ─── FINESTRA PRINCIPALE ─────────────────────────────────────────────────────

class MusicCatalogerGUI:
    def __init__(self, root: ctk.CTk, api_client=None, user_info=None):
        # v0.0.2.2: api_client e user_info sono opzionali per retrocompatibilità.
        #   - api_client: istanza ApiClient o None (modalità locale-only)
        #   - user_info : dict con email/plan/features o None
        # Quando sono None, la GUI legge plan da config/user_plans.py come prima.
        self.api_client = api_client
        self.user_info  = user_info or {}
        self.root = root
        self.root.title(f"Music Cataloger Advanced  {APP_VERSION}")
        # Icona finestra e taskbar — helper gestisce PyInstaller bundle
        try:
            from gui.app_icon import set_window_icon
            set_window_icon(self.root)
        except Exception:
            pass
        self.root.geometry("1300x860")
        # v1081: range resize stretto. Il resize con CTkTabview+7 tab è
        # intrinsecamente lento (ricalcolo layout di TUTTE le tab ad ogni
        # Configure); l'utente ha optato per fissare un range piccolo di
        # min/max accettabile invece di inseguire ulteriori workaround che
        # risolvono solo parzialmente. Valori scelti:
        #   min 1100×720 → laptop 1366×768 (tutto visibile senza scroll)
        #   max 1500×960 → range limitato, il drag lento dura poco
        # Se serve più spazio si può sempre massimizzare a tutto schermo
        # via doppio-click sulla titlebar (il max Windows è separato).
        self.root.minsize(1100, 720)
        self.root.maxsize(1500, 960)

        self._selected_path = ctk.StringVar()
        self._is_running = False
        self._process: Optional[subprocess.Popen] = None
        self._log_queue: queue.Queue = queue.Queue()

        # v1075: tooltip singleton globale — evita ghost tooltips
        self._global_tip = None
        self._global_tip_after = None

        # Opzioni base
        # v1048: default "solo catalogazione" — tutto deselezionato
        self._opt_dry_run  = ctk.BooleanVar(value=False)
        self._opt_verbose  = ctk.BooleanVar(value=False)
        self._opt_no_ext   = ctk.BooleanVar(value=False)
        self._opt_cleanup  = ctk.BooleanVar(value=False)
        self._opt_correct  = ctk.BooleanVar(value=False)
        self._opt_classify = ctk.BooleanVar(value=False)
        self._opt_analyze  = ctk.BooleanVar(value=False)
        # v1053: master switch sorgenti DB (inverte logica --no-external)
        self._opt_use_ext_db = ctk.BooleanVar(value=True)

        # v1036/v1047: Sorgenti metadati selezionabili
        self._meta_sources = {
            'musicbrainz':    ctk.BooleanVar(value=True),
            'lastfm':         ctk.BooleanVar(value=True),
            'beatport':       ctk.BooleanVar(value=True),
            'getsong':        ctk.BooleanVar(value=True),
            'deezer':         ctk.BooleanVar(value=True),
            'itunes':         ctk.BooleanVar(value=True),
            'discogs_enabled': ctk.BooleanVar(value=False),
            'audd_enabled':    ctk.BooleanVar(value=False),
            'acoustid_enabled': ctk.BooleanVar(value=False),
        }

        # Opzioni duplicati
        self._dup_action = ctk.StringVar(value='keep_both')

        # Opzioni cover
        self._cover_enabled   = ctk.BooleanVar(value=True)
        self._cover_strategy  = ctk.StringVar(value='largest')
        self._cover_overwrite = ctk.BooleanVar(value=False)
        self._cover_sources   = {
            'musicbrainz': ctk.BooleanVar(value=True),
            'lastfm':      ctk.BooleanVar(value=True),
            'deezer':      ctk.BooleanVar(value=True),
            'itunes':      ctk.BooleanVar(value=True),
        }

        # v1035: DB locale — v1053: abilitato di default
        self._opt_local_db = ctk.BooleanVar(value=True)

        # Contatori progress
        self._n_proc        = 0
        self._n_total       = 0
        self._n_proc_salsa  = 0
        self._n_total_salsa = 0
        self._phase         = 'catalogazione'
        self._last_genre_stats: dict = {}   # v1048: per dialog orfani

        # v1038: storico directory recenti (max 10)
        self._recent_dirs: list = self._load_recent_dirs()

        self._log_all_lines = []
        self._log_filter = {"INFO": True, "WARNING": True, "ERROR": True}
        # v1076: niente più self._build_menu() qui — la menubar custom
        # viene creata da _build_layout() via _build_custom_menubar()
        self._build_layout()
        # Carica impostazioni caraibiche salvate
        self.root.after(200, self._load_caribbean_settings)
        # v0.0.2.4: Applica restrizioni piano (overlay lock sui tab non concessi)
        # Eseguito DOPO la costruzione completa della GUI così tutti i tab esistono
        self.root.after(300, self._apply_plan_restrictions)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(80, self._center_main_window)
        self._poll_queue()

    # ─── HELPER: safe-after per thread workers ────────────────────────
    def _safe_after(self, ms: int, callback):
        """v1085e: wrapper per `self.root.after` che protegge dal caso
        in cui la root sia già stata distrutta (es. utente chiude la
        finestra durante una richiesta HTTP in background → il thread
        worker che chiama `self.root.after()` solleva
        `RuntimeError: main thread is not in main loop` o
        `_tkinter.TclError: invalid command name`).

        Da usare al posto di `self.root.after` ovunque il chiamante sia
        un thread worker (non il main thread Tk)."""
        try:
            if not self.root or not self.root.winfo_exists():
                return None
            return self.root.after(ms, callback)
        except (RuntimeError, Exception):
            return None

    # ─── MENU ──────────────────────────────────────────────────

    def _show_profile_panel(self):
        """
        v1078: Flyout profilo con ESPANSIONE LATERALE.

        Al click su una voce con sottomenu (Lingua, Piani, Impostazioni), il
        flyout principale resta visibile a sinistra e un secondo pannello
        si apre ACCANTO a destra. Torna il senso della freccia `›`. Click su
        "← Indietro" o sulla stessa voce (toggle) chiude il pannello laterale.

        Architettura:
          self._profile_flyout       → Toplevel PRINCIPALE (300×360)
          self._profile_sub_flyout   → Toplevel LATERALE  (320×420), opzionale

        Entrambi si chiudono insieme al click esterno. Quando il principale
        si sposta non serve riposizionare il laterale (sono Toplevel
        indipendenti e il principale non viene mosso dopo l'apertura).
        """
        # v1085c: in modalità server (api_client != None), il piano e
        # username vengono da user_info (decodificato dal JWT).
        # In modalità locale fallback su config.user_plans.
        plan = None
        if self.api_client is not None and self.user_info:
            try:
                from types import SimpleNamespace
                plan_name = self.user_info.get("plan", "base")
                _disp_map = {"base": "🆓 Base", "pro": "⭐ Pro",
                             "advanced": "💎 Advanced"}
                plan = SimpleNamespace(
                    plan=plan_name,
                    display_name=_disp_map.get(plan_name, plan_name.capitalize()),
                    username=(self.user_info.get("username")
                              or self.user_info.get("email", "User").split("@")[0]
                              or "User"),
                )
            except Exception:
                plan = None

        if plan is None:
            try:
                from config.user_plans import get_plan as _gp, PLAN_FEATURES
                plan = _gp()
            except Exception:
                return

        # Toggle: se il flyout è già aperto, chiudi tutto
        if getattr(self, "_profile_flyout", None) and self._profile_flyout.winfo_exists():
            self._close_profile_flyout()
            return

        # Calcola posizione: sotto il pulsante profilo
        btn = self._profile_btn
        btn.update_idletasks()
        x = btn.winfo_rootx()
        y = btn.winfo_rooty() + btn.winfo_height() + 4
        # v1080: dimensioni flyout principale calibrate sul contenuto.
        # Header 58px + 5 voci × 40px (36 row + 2×2 pady) + margine ~12 ≈ 270.
        # Niente scrollbar perché il contenuto è fisso, sempre 5 voci.
        main_w, main_h = 260, 280

        fly = ctk.CTkToplevel(self.root)
        fly.overrideredirect(True)
        fly.attributes("-topmost", True)
        fly.geometry(f"{main_w}x{main_h}+{x}+{y}")
        # v1080: angoli rounded reali su Windows — il trucco color-key
        # richiede che il bg NATIVO TK del Toplevel sia esattamente il colore
        # dichiarato come trasparente. fly.configure(fg_color=...) di CTk
        # agisce sul frame interno, non sul bg del toplevel, motivo per cui
        # in v1079 gli angoli restavano quadrati. Usiamo configure(bg=...)
        # che scrive direttamente sul tk.Toplevel sottostante.
        _TRANSP = "#010101"
        try:
            fly.configure(bg=_TRANSP)   # bg nativo tk, NON fg_color CTk
            fly.wm_attributes("-transparentcolor", _TRANSP)
        except Exception:
            pass
        outer = ctk.CTkFrame(fly, fg_color=PALETTE["border"], corner_radius=16)
        outer.pack(fill="both", expand=True, padx=1, pady=1)
        inner = ctk.CTkFrame(outer, fg_color=PALETTE["surface2"], corner_radius=15)
        inner.pack(fill="both", expand=True)

        self._profile_flyout = fly
        self._profile_sub_flyout = None   # pannello laterale (creato on-demand)
        self._profile_main_geom = (x, y, main_w, main_h)  # per posizionare il laterale
        self._profile_active_sub = None    # quale sottomenu è attualmente aperto
        self._hover_open_after = None      # v1080: timer hover-to-open

        # Chiudi al click esterno (entrambi i Toplevel)
        def _close_if_outside(e):
            try:
                def _inside(win, e):
                    if not win or not win.winfo_exists():
                        return False
                    wx, wy = win.winfo_x(), win.winfo_y()
                    ww, wh = win.winfo_width(), win.winfo_height()
                    return wx <= e.x_root <= wx+ww and wy <= e.y_root <= wy+wh
                if not _inside(fly, e) and not _inside(self._profile_sub_flyout, e):
                    self._close_profile_flyout()
            except Exception:
                pass
        self._profile_flyout_bind = self.root.bind("<Button-1>", _close_if_outside, add="+")

        # v1085d: chiudi anche quando il mouse esce dall'area dei flyout
        # (non solo al click esterno). Implementato con polling 200ms
        # sulla posizione del puntatore.
        def _is_mouse_inside_flyouts() -> bool:
            try:
                px = self.root.winfo_pointerx()
                py = self.root.winfo_pointery()
                for w in (self._profile_flyout, self._profile_sub_flyout):
                    if w and w.winfo_exists():
                        wx, wy = w.winfo_x(), w.winfo_y()
                        ww, wh = w.winfo_width(), w.winfo_height()
                        # Tolleranza 8px per evitare flicker ai bordi
                        if (wx-8) <= px <= (wx+ww+8) and (wy-8) <= py <= (wy+wh+8):
                            return True
            except Exception:
                return True   # in caso di errore non chiudere
            return False

        def _mouse_check():
            if not getattr(self, "_profile_flyout", None) or \
               not self._profile_flyout.winfo_exists():
                return  # già chiuso
            if not _is_mouse_inside_flyouts():
                # Il mouse è fuori da entrambi i flyout: parte un timer
                # di 350ms come "grace period" (per evitare chiusure
                # se l'utente sta semplicemente passando da uno all'altro)
                if not getattr(self, "_flyout_close_timer", None):
                    self._flyout_close_timer = self.root.after(
                        250, _delayed_close)
            else:
                # Mouse rientrato: cancella eventuale timer pending
                t = getattr(self, "_flyout_close_timer", None)
                if t:
                    try: self.root.after_cancel(t)
                    except Exception: pass
                    self._flyout_close_timer = None
            # Re-schedule
            self._flyout_poll_id = self.root.after(200, _mouse_check)

        def _delayed_close():
            self._flyout_close_timer = None
            # Doppio check: il mouse è ancora fuori?
            if not _is_mouse_inside_flyouts():
                self._close_profile_flyout()

        self._flyout_close_timer = None
        self._flyout_poll_id = self.root.after(500, _mouse_check)

        # v1085d: bind <Leave> diretto su `inner` come fallback robusto.
        # Il polling con winfo_pointerx() può fallire in alcuni casi su
        # Windows con DPI scaling (pointer in coordinate scalate, widget
        # in coordinate non-scalate). Il bind <Leave> è coordinate-agnostic.
        def _on_flyout_leave(event=None):
            # Schedula chiusura ritardata; il polling la annullerà se
            # nel grace period il mouse rientra nei flyout.
            if not getattr(self, "_flyout_close_timer", None):
                self._flyout_close_timer = self.root.after(
                    250, _delayed_close)
        def _on_flyout_enter(event=None):
            t = getattr(self, "_flyout_close_timer", None)
            if t:
                try: self.root.after_cancel(t)
                except Exception: pass
                self._flyout_close_timer = None
        try:
            inner.bind("<Leave>", _on_flyout_leave, add="+")
            inner.bind("<Enter>", _on_flyout_enter, add="+")
        except Exception:
            pass

        # ── Header profilo (solo nel principale) ───────────────────────────
        hdr = ctk.CTkFrame(inner, fg_color=PALETTE.get("primary", "#3b6fd4"),
                           corner_radius=0, height=58)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        _ic_user = _get_icon("profile", 28) if _ICONS_AVAILABLE else None
        ctk.CTkLabel(hdr, image=_ic_user, text=" ").pack(side="left", padx=(14, 6), pady=8)
        vfrm = ctk.CTkFrame(hdr, fg_color="transparent")
        vfrm.pack(side="left", pady=8)
        _uname = getattr(plan, "username", None) or "User"
        ctk.CTkLabel(vfrm, text=_uname, font=(FONT_BODY[0], 13, "bold"),
                     text_color="#ffffff").pack(anchor="w")
        ctk.CTkLabel(vfrm, text=f"Piano: {plan.display_name}",
                     font=(FONT_SMALL[0], FONT_SMALL[1]-1),
                     text_color="#ccddff").pack(anchor="w")

        # ── Voci menu ──────────────────────────────────────────────────────
        # Le voci con sottomenu memorizzano un ID in self._profile_active_sub
        # per gestire il toggle (click sulla stessa voce = chiudi laterale)

        # Riferimenti alle row per poter evidenziare la voce attiva
        self._profile_rows = {}   # id → CTkFrame

        def _mk_row(parent, icon_name, label, on_click, has_submenu=False, sub_id=None):
            row = ctk.CTkFrame(parent, fg_color=PALETTE["surface2"], corner_radius=6,
                               height=36)
            row.pack(fill="x", padx=8, pady=2)
            row.pack_propagate(False)
            _ic = _get_icon(icon_name, 18) if _ICONS_AVAILABLE else None
            inner_row = ctk.CTkFrame(row, fg_color="transparent")
            inner_row.pack(fill="both", expand=True, padx=10)
            lbl = ctk.CTkLabel(inner_row, image=_ic, text=f"  {label}", compound="left",
                               font=FONT_SMALL, text_color=PALETTE["text"],
                               anchor="w")
            lbl.pack(side="left", fill="both", expand=True)   # v1079: fill="both" + expand copre la riga
            arrow = None
            if has_submenu:
                arrow = ctk.CTkLabel(inner_row, text="›", font=(FONT_BODY[0], 14, "bold"),
                                     text_color=PALETTE["text_dim"])
                arrow.pack(side="right")
            def _on_enter(e):
                try:
                    # v1080: voci senza sub_id (Aiuto/Esci) ottengono sempre
                    # l'highlight. Il check su _profile_active_sub serve solo
                    # a preservare il blu sulla voce con sub correntemente
                    # aperto (es. "Piani" resta blu mentre scorri sopra).
                    if sub_id is None or self._profile_active_sub != sub_id:
                        row.configure(fg_color=PALETTE.get("surface", "#1e2533"))
                    # v1085f: se entro in una voce SENZA sub (Esci/Aiuto)
                    # mentre un sub-flyout è aperto da un'altra voce, chiudo
                    # il sub. Senza questo, il sub di "Piani" restava aperto
                    # quando l'utente passava su "Esci".
                    if sub_id is None and self._profile_active_sub is not None:
                        # v1085g: ripristina il colore della voce sub
                        # precedente (era blu primary perché _toggle_sub
                        # l'aveva colorata). Senza questo, il blu resta
                        # anche dopo che il sub-flyout è chiuso.
                        prev_sub_id = self._profile_active_sub
                        try:
                            prev_row = self._profile_rows.get(prev_sub_id)
                            if prev_row is not None:
                                prev_row.configure(fg_color="transparent")
                        except Exception:
                            pass
                        try:
                            if (self._profile_sub_flyout
                                    and self._profile_sub_flyout.winfo_exists()):
                                self._profile_sub_flyout.destroy()
                        except Exception:
                            pass
                        self._profile_sub_flyout = None
                        self._profile_active_sub = None
                        # Cancello anche il timer di hover-to-open di altre voci
                        if getattr(self, "_hover_open_after", None) is not None:
                            try:
                                self.root.after_cancel(self._hover_open_after)
                            except Exception:
                                pass
                            self._hover_open_after = None
                    # v1080: hover-to-open sottomenu laterale con delay 250ms.
                    # Solo per voci con sub_id; se il mouse esce prima dei
                    # 250ms il timer viene cancellato in _on_leave.
                    # Se entro in un'altra voce con sub mentre un sub è già
                    # aperto, _toggle_sub gestisce la sostituzione.
                    if sub_id is not None:
                        # Cancella eventuali timer di hover pendenti
                        if getattr(self, "_hover_open_after", None) is not None:
                            try:
                                self.root.after_cancel(self._hover_open_after)
                            except Exception:
                                pass
                            self._hover_open_after = None
                        def _trigger():
                            self._hover_open_after = None
                            # Solo se non è già il sub attivo (evita re-apertura)
                            if self._profile_active_sub != sub_id:
                                _toggle_sub(sub_id)
                        self._hover_open_after = self.root.after(250, _trigger)
                except Exception:
                    pass
            def _on_leave(e):
                try:
                    if sub_id is None or self._profile_active_sub != sub_id:
                        row.configure(fg_color=PALETTE["surface2"])
                    # v1080: se esco dalla voce prima che scada il delay,
                    # cancello il timer (niente apertura "flash")
                    if sub_id is not None and getattr(self, "_hover_open_after", None) is not None:
                        try:
                            self.root.after_cancel(self._hover_open_after)
                        except Exception:
                            pass
                        self._hover_open_after = None
                except Exception:
                    pass
            # v1079: hover + click bindati su TUTTI i widget della row,
            # non solo su row/inner_row, altrimenti passando sopra il label
            # il <Leave> del frame scatta e l'highlight "lampeggia" o non
            # funziona su Aiuto/Esci dove c'è solo testo (no icona a sinistra).
            widgets_to_bind = [row, inner_row, lbl]
            if arrow is not None:
                widgets_to_bind.append(arrow)
            for w in widgets_to_bind:
                w.bind("<Enter>", _on_enter, add="+")
                w.bind("<Leave>", _on_leave, add="+")
                w.bind("<Button-1>", lambda e, cb=on_click: cb(), add="+")
            if sub_id:
                self._profile_rows[sub_id] = row
            return row

        # ── Costruzione lista voci ─────────────────────────────────────────
        list_frm = ctk.CTkFrame(inner, fg_color="transparent")
        list_frm.pack(fill="both", expand=True, pady=(8, 4))

        _mk_row(list_frm, "settings_ph", "Impostazioni",
                on_click=lambda: _toggle_sub("settings"),
                has_submenu=True, sub_id="settings")
        _mk_row(list_frm, "lang", "Lingua",
                on_click=lambda: _toggle_sub("lang"),
                has_submenu=True, sub_id="lang")
        _mk_row(list_frm, "plans", "Piani di abbonamento",
                on_click=lambda: _toggle_sub("plans"),
                has_submenu=True, sub_id="plans")
        _mk_row(list_frm, "help_ph", "Aiuto",
                on_click=lambda: (self._close_profile_flyout(), self._show_about()))
        _mk_row(list_frm, "logout", "Esci",
                on_click=lambda: _handle_exit())

        def _handle_exit():
            """
            v0.0.2.2: logout reale.
            - Cancella la sessione JWT locale (jwt_store.clear())
            - Chiude la main window
            - Mostra messaggio di conferma
            L'utente dovrà rilanciare l'app (o in futuro possiamo
            riaprire automaticamente la login window).
            """
            self._close_profile_flyout()
            if not messagebox.askyesno(
                "Conferma logout",
                "Vuoi disconnetterti?\n\n"
                "Al prossimo avvio dovrai inserire nuovamente email e password."
            ):
                return
            # Clear JWT locale — funziona anche in modalità offline
            try:
                from services.jwt_store import store as _jwt_store
                _jwt_store.clear()
            except Exception as e:
                print(f"[logout] clear jwt failed: {e}")
            # Se abbiamo un api_client attivo, logout esplicito
            try:
                if self.api_client is not None:
                    self.api_client.logout()
            except Exception:
                pass
            messagebox.showinfo(
                "Logout effettuato",
                "Sessione chiusa. Rilancia il programma per accedere\n"
                "con un altro account."
            )
            # Chiudi la main window (l'utente rilancerà manualmente)
            try:
                self.root.quit()
                self.root.destroy()
            except Exception:
                pass

        def _toggle_sub(sub_id):
            """Apre/chiude il pannello laterale. Toggle se stesso id già attivo."""
            if self._profile_active_sub == sub_id:
                # stesso click → chiudi
                _close_sub()
                return
            # Cambio voce: chiudi l'eventuale pannello precedente e apri il nuovo
            _close_sub()
            self._profile_active_sub = sub_id
            # Evidenzia la row attiva
            try:
                r = self._profile_rows.get(sub_id)
                if r:
                    r.configure(fg_color=PALETTE.get("primary", "#3b6fd4"))
            except Exception:
                pass
            _open_sub(sub_id)

        def _close_sub():
            """Chiude il Toplevel laterale (se presente) e resetta l'evidenza."""
            try:
                if self._profile_sub_flyout is not None and self._profile_sub_flyout.winfo_exists():
                    self._profile_sub_flyout.destroy()
            except Exception:
                pass
            self._profile_sub_flyout = None
            # Reset highlight della row
            try:
                if self._profile_active_sub:
                    r = self._profile_rows.get(self._profile_active_sub)
                    if r:
                        r.configure(fg_color=PALETTE["surface2"])
            except Exception:
                pass
            self._profile_active_sub = None

        # ── Sub-flyout costruzione ─────────────────────────────────────────
        def _open_sub(sub_id):
            mx, my, mw, mh = self._profile_main_geom
            # v1080: dimensioni per-id calibrate sul contenuto di ciascun sub.
            #   settings → solo 2 righe di testo placeholder → 280×160
            #   lang     → 3 lingue + placeholder → 280×220
            #   plans    → 11 feature + 3 bottoni piano → 320×440 (scrollable)
            sub_sizes = {
                "settings": (280, 160),
                "lang":     (280, 220),
                "plans":    (320, 440),
            }
            sub_w, sub_h = sub_sizes.get(sub_id, (320, 400))
            sub_x = mx + mw + 4
            sub_y = my

            sub = ctk.CTkToplevel(self.root)
            sub.overrideredirect(True)
            sub.attributes("-topmost", True)
            sub.geometry(f"{sub_w}x{sub_h}+{sub_x}+{sub_y}")
            # v1080: bg tk nativo per color-key (vedi commento flyout principale)
            try:
                sub.configure(bg="#010101")
                sub.wm_attributes("-transparentcolor", "#010101")
            except Exception:
                pass
            s_outer = ctk.CTkFrame(sub, fg_color=PALETTE["border"], corner_radius=16)
            s_outer.pack(fill="both", expand=True, padx=1, pady=1)
            s_inner = ctk.CTkFrame(s_outer, fg_color=PALETTE["surface2"], corner_radius=15)
            s_inner.pack(fill="both", expand=True)

            # Header del sub — titolo sezione (niente Back: la freccia chiude
            # cliccando di nuovo la voce nel pannello principale)
            titles = {"settings": "Impostazioni",
                      "lang":     "Lingua",
                      "plans":    "Piani di abbonamento"}
            s_hdr = ctk.CTkFrame(s_inner, fg_color=PALETTE.get("primary", "#3b6fd4"),
                                 corner_radius=0, height=46)
            s_hdr.pack(fill="x")
            s_hdr.pack_propagate(False)
            ctk.CTkLabel(s_hdr, text=titles.get(sub_id, ""),
                         font=(FONT_BODY[0], 13, "bold"),
                         text_color="#ffffff").pack(side="left", padx=16, pady=12)
            # Bottone X per chiudere solo il sub (resta aperto il principale)
            ctk.CTkButton(s_hdr, text="✕", width=28, height=28,
                          fg_color="transparent",
                          hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
                          text_color="#ffffff",
                          font=(FONT_BODY[0], 12, "bold"),
                          command=_close_sub
                          ).pack(side="right", padx=(0, 8), pady=8)

            # Contenuto per ciascun sub-id
            if sub_id == "settings":
                _fill_settings(s_inner)
            elif sub_id == "lang":
                _fill_language(s_inner)
            elif sub_id == "plans":
                _fill_plans(s_inner)

            self._profile_sub_flyout = sub

        # ── Contenuti dei sottomenu ────────────────────────────────────────
        def _fill_settings(parent):
            # v0.0.2.3: in modalità server mostra anche "Cambia password"
            if self.api_client is not None:
                ctk.CTkLabel(parent,
                             text="Account",
                             font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
                             text_color=PALETTE["text"],
                             anchor="w"
                             ).pack(fill="x", padx=10, pady=(10, 6))
                ctk.CTkButton(
                    parent,
                    text="🔒  Cambia password",
                    font=FONT_SMALL,
                    fg_color=PALETTE.get("surface2", "#2a3344"),
                    hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
                    text_color=PALETTE["text"],
                    anchor="w", height=32, corner_radius=6,
                    command=lambda: (self._close_profile_flyout(),
                                     self._show_change_password_dialog()),
                ).pack(fill="x", padx=10, pady=(0, 14))

                ctk.CTkFrame(parent, height=1,
                             fg_color=PALETTE["border"]
                             ).pack(fill="x", padx=10, pady=(0, 10))

            ctk.CTkLabel(parent,
                         text="Altre preferenze disponibili\nin una prossima versione.",
                         font=FONT_SMALL, text_color=PALETTE["text_dim"],
                         justify="center"
                         ).pack(pady=(20, 8))
            ctk.CTkLabel(parent,
                         text="Le opzioni di catalogazione sono nel\ntab Avanzate della finestra principale.",
                         font=(FONT_SMALL[0], FONT_SMALL[1]-1),
                         text_color=PALETTE["text_dim"],
                         justify="center"
                         ).pack(pady=(0, 8))

        def _fill_language(parent):
            ctk.CTkLabel(parent,
                         text="La traduzione sarà disponibile\nin una prossima versione.",
                         font=FONT_SMALL, text_color=PALETTE["text_dim"],
                         justify="center"
                         ).pack(pady=(20, 12))
            for code, name in [("it", "Italiano  (default)"),
                               ("en", "English"),
                               ("es", "Español")]:
                r = ctk.CTkFrame(parent, fg_color=PALETTE["surface2"],
                                 corner_radius=6, height=34)
                r.pack(fill="x", padx=14, pady=3)
                r.pack_propagate(False)
                ctk.CTkLabel(r, text=f"  {name}", font=FONT_SMALL,
                             text_color=PALETTE["text_dim"] if code != "it" else PALETTE["text"],
                             anchor="w"
                             ).pack(side="left", padx=10, fill="y")
                if code == "it":
                    ctk.CTkLabel(r, text="✓", font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
                                 text_color="#50aa70").pack(side="right", padx=10)

        def _fill_plans(parent):
            # v0.0.2.2: Due modalità.
            #   - Server mode (self.api_client != None): mostra piano dal
            #     JWT server, pulsante "Richiedi upgrade" che apre un dialog
            #     comparativo con tutti i piani e un bottone per ciascun
            #     upgrade disponibile. Il server decide, non il client.
            #   - Locale mode (retrocompat): mantiene i 3 bottoni di switch
            #     diretto come in v1080. Utile per sviluppo standalone.
            body = ctk.CTkScrollableFrame(parent, fg_color="transparent")
            body.pack(fill="both", expand=True, padx=10, pady=(8, 4))
            body.columnconfigure(0, weight=1)
            ctk.CTkLabel(body, text=f"Piano corrente: {plan.display_name}",
                         font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
                         text_color=PALETTE["text"]
                         ).pack(anchor="w", pady=(4, 6), padx=4)
            feature_labels = {
                "catalog_external_db": "DB online",
                "catalog_cover":        "Cover album",
                "catalog_bpm":          "Analisi BPM",
                "tab_cache":            "Tab Cache",
                "tab_quality":          "Tab Qualità",
                "tab_advanced":         "Tab Avanzate",
                "tab_caribbean":        "Tab Caraibica",
                "export_m3u":           "Playlist M3U",
                "maint_replaygain":     "ReplayGain",
                "maint_batch_rename":   "Rinomina Batch",
                "maint_integrity":      "Verifica MP3",
            }
            # Leggi features dall'utente server se disponibile, altrimenti da PLAN_FEATURES
            if self.api_client is not None and self.user_info.get("features"):
                current_features = self.user_info["features"]
            else:
                current_features = PLAN_FEATURES.get(plan.plan, {})

            for feat, label in feature_labels.items():
                has = current_features.get(feat, True)
                r = ctk.CTkFrame(body, fg_color="transparent")
                r.pack(fill="x", pady=1)
                ctk.CTkLabel(r, text="✓" if has else "✗", width=20,
                             font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
                             text_color="#50aa70" if has else "#555566"
                             ).pack(side="left", padx=(4, 2))
                ctk.CTkLabel(r, text=label, font=FONT_SMALL,
                             text_color=PALETTE["text"] if has else PALETTE["text_dim"],
                             anchor="w").pack(side="left")

            ctk.CTkFrame(body, height=1, fg_color=PALETTE["border"]).pack(fill="x", pady=8)

            # ── Sezione azione — SERVER MODE ────────────────────────
            if self.api_client is not None:
                # Pulsante "Richiedi upgrade" che apre il dialog comparativo
                current_plan = self.user_info.get("plan", plan.plan)
                # Calcola gli upgrade disponibili dal piano corrente
                plan_order = ["base", "pro", "advanced"]
                try:
                    idx = plan_order.index(current_plan)
                    upgrades = plan_order[idx + 1:]
                except ValueError:
                    upgrades = []

                if upgrades:
                    ctk.CTkButton(
                        body, text="⬆  Richiedi upgrade del piano",
                        font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
                        fg_color=PALETTE.get("primary", "#3b6fd4"),
                        hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
                        text_color="#ffffff",
                        height=34, corner_radius=8,
                        # v1085e: chiudo PRIMA il flyout, poi apro dialog.
                        # Senza questo, gli eventi <Leave> del flyout sotto
                        # interferivano col dialog rendendolo invisibile.
                        command=lambda cp=current_plan, ups=upgrades: (
                            self._close_profile_flyout(),
                            self.root.after(80,
                                lambda: self._show_upgrade_dialog(cp, ups)),
                        ),
                    ).pack(fill="x", padx=4, pady=(0, 4))
                else:
                    ctk.CTkLabel(body,
                                 text="Hai già il piano più alto disponibile ✨",
                                 font=FONT_SMALL,
                                 text_color=PALETTE["text_dim"]
                                 ).pack(pady=(4, 8))
                return

            # ── Sezione azione — LOCALE MODE (retrocompat) ──────────
            ctk.CTkLabel(body, text="Cambia piano (modalità sviluppo):",
                         font=FONT_SMALL,
                         text_color=PALETTE["text_dim"]
                         ).pack(anchor="w", padx=4, pady=(0, 4))
            plan_row = ctk.CTkFrame(body, fg_color="transparent")
            plan_row.pack(fill="x", pady=(0, 8))
            for p, badge in [("base", "🆓 Base"), ("pro", "⭐ Pro"), ("advanced", "💎 Adv")]:
                is_active = plan.plan == p
                def _switch(np=p, _plan=plan):
                    _plan.plan = np
                    _plan.save()
                    try:
                        self._plan_badge.configure(text=_plan.display_name)
                    except Exception:
                        pass
                    self._close_profile_flyout()
                    self._apply_plan_restrictions()
                    self.root.after(100, self._show_profile_panel)
                ctk.CTkButton(plan_row, text=badge, width=80, height=28,
                              font=(FONT_SMALL[0], FONT_SMALL[1]-1,
                                    "bold" if is_active else "normal"),
                              fg_color=PALETTE["primary"] if is_active else PALETTE["surface"],
                              hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
                              command=_switch
                              ).pack(side="left", padx=2)

    def _show_upgrade_dialog(self, current_plan: str, available_upgrades: list):
        """
        v1085e: Dialog modale comparativo per richiedere un upgrade.

        Se chiamato mentre un flyout profilo è aperto, lo chiude prima
        per evitare interferenze con i suoi event-bind (<Leave>, polling
        mouse) che facevano sparire o bloccare il dialog.
        """
        # v1085e: garantisce che il flyout sia chiuso prima del dialog
        try:
            if getattr(self, "_profile_flyout", None) is not None:
                self._close_profile_flyout()
        except Exception:
            pass

        import customtkinter as ctk
        from tkinter import messagebox
        import threading

        if self.api_client is None:
            messagebox.showerror("Non disponibile",
                "L'upgrade richiede la modalità connessa al server.")
            return
        try:
            from config.user_plans import PLAN_FEATURES
        except Exception:
            messagebox.showerror("Errore", "Impossibile caricare i dati dei piani")
            return

        feature_labels = [
            ("catalog_external_db",  "DB online (MusicBrainz, Deezer)"),
            ("catalog_cover",        "Cover album automatica"),
            ("catalog_bpm",          "Analisi BPM"),
            ("tab_caribbean",        "Tab Caraibica"),
            ("tab_cache",            "Tab Cache metadati"),
            ("maint_duplicates",     "Trova duplicati"),
            ("export_m3u",           "Export playlist M3U"),
            ("export_csv",           "Export CSV"),
            ("tab_advanced",         "Tab Avanzate"),
            ("maint_replaygain",     "ReplayGain"),
            ("maint_batch_rename",   "Rinomina batch"),
            ("maint_integrity",      "Verifica integrità MP3"),
        ]
        display_names = {"base": "🆓 Base", "pro": "⭐ Pro", "advanced": "💎 Advanced"}
        limits_labels = {
            "max_files_per_run": "File per run",
            "max_runs_per_day":  "Run per giorno",
        }

        plans_to_show = [current_plan] + available_upgrades
        n_plans = len(plans_to_show)

        # Dimensioni
        FEATURE_COL_W = 260
        PLAN_COL_W    = 200
        BTN_BAR_H     = 80
        TITLEBAR_H    = 44
        HEADER_H      = 90
        win_w = FEATURE_COL_W + n_plans * PLAN_COL_W + 32
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        win_w = min(win_w, sw - 80)
        win_h = min(680, sh - 80)

        win = ctk.CTkToplevel(self.root)
        win.transient(self.root)
        win.configure(fg_color=PALETTE["bg"])
        try:
            win.overrideredirect(True)
        except Exception:
            pass
        win.geometry(f"{win_w}x{win_h}+{(sw-win_w)//2}+{(sh-win_h)//2}")
        try:
            win.grab_set()
        except Exception:
            pass
        try:
            win.focus_set()
        except Exception:
            pass

        # ── 1. Titlebar (TOP) ────────────────────────────────────────
        titlebar = ctk.CTkFrame(win, fg_color=PALETTE["surface2"],
                                corner_radius=0, height=TITLEBAR_H)
        titlebar.pack(side="top", fill="x")
        titlebar.pack_propagate(False)
        ctk.CTkLabel(titlebar, text="Confronto piani disponibili",
                     font=("Segoe UI", 14, "bold"),
                     text_color=PALETTE["text"]
                     ).pack(side="left", padx=18, pady=10)
        close_btn = ctk.CTkButton(titlebar, text="✕", width=32, height=28,
                      fg_color="transparent", hover_color="#d84545",
                      text_color=PALETTE["text_dim"],
                      font=("Segoe UI", 13, "bold"),
                      command=win.destroy)
        close_btn.pack(side="right", padx=8, pady=8)

        # Drag dalla titlebar
        def _start_drag(e):
            win._drag_x = e.x_root - win.winfo_x()
            win._drag_y = e.y_root - win.winfo_y()
        def _do_drag(e):
            try:
                win.geometry(f"+{e.x_root - win._drag_x}+{e.y_root - win._drag_y}")
            except Exception:
                pass
        # Non bindare il drag sul bottone close
        for w in titlebar.winfo_children():
            if w is close_btn: continue
            try:
                w.bind("<Button-1>", _start_drag)
                w.bind("<B1-Motion>", _do_drag)
            except Exception:
                pass
        titlebar.bind("<Button-1>", _start_drag)
        titlebar.bind("<B1-Motion>", _do_drag)

        # ── 2. Btn bar (BOTTOM) — pinnato PRIMA del body ─────────────
        btn_bar = ctk.CTkFrame(win, fg_color=PALETTE["surface"],
                               height=BTN_BAR_H, corner_radius=0)
        btn_bar.pack(side="bottom", fill="x")
        btn_bar.pack_propagate(False)
        btn_bar.columnconfigure(0, weight=0, minsize=FEATURE_COL_W)
        for i in range(1, n_plans + 1):
            btn_bar.columnconfigure(i, weight=1, minsize=PLAN_COL_W,
                                     uniform="cols")
        ctk.CTkLabel(btn_bar, text="").grid(row=0, column=0)
        ctk.CTkLabel(btn_bar, text="(piano attuale)",
                     font=("Segoe UI", 10, "italic"),
                     text_color=PALETTE["text_dim"]
                     ).grid(row=0, column=1, sticky="nsew")

        def _make_upgrade_handler(target_plan: str):
            def _handler():
                msg_win = ctk.CTkToplevel(win)
                MW_W, MW_H = 460, 320
                msg_win.geometry(f"{MW_W}x{MW_H}")
                # v1085e: centra sopra il dialog upgrade `win`
                try:
                    win.update_idletasks()
                    wx = win.winfo_x(); wy = win.winfo_y()
                    ww = win.winfo_width(); wh = win.winfo_height()
                    px = wx + (ww - MW_W) // 2
                    py = wy + (wh - MW_H) // 2
                    msg_win.geometry(f"{MW_W}x{MW_H}+{px}+{py}")
                except Exception:
                    pass
                msg_win.transient(win)
                try:
                    msg_win.grab_set()
                except Exception: pass
                msg_win.configure(fg_color=PALETTE["bg"])
                try:
                    msg_win.overrideredirect(True)
                except Exception: pass

                # Mini titlebar
                mtb = ctk.CTkFrame(msg_win, fg_color=PALETTE["surface2"], height=36)
                mtb.pack(side="top", fill="x"); mtb.pack_propagate(False)
                ctk.CTkLabel(mtb, text=f"Richiedi → {display_names.get(target_plan)}",
                             font=("Segoe UI", 11, "bold"),
                             text_color=PALETTE["text"]).pack(side="left", padx=14, pady=8)
                ctk.CTkButton(mtb, text="✕", width=28, height=24,
                              fg_color="transparent", hover_color="#d84545",
                              text_color=PALETTE["text_dim"],
                              font=("Segoe UI", 11, "bold"),
                              command=msg_win.destroy
                              ).pack(side="right", padx=6, pady=6)

                # Btn row BOTTOM
                br = ctk.CTkFrame(msg_win, fg_color="transparent", height=60)
                br.pack(side="bottom", fill="x", padx=20, pady=(0, 14))
                br.pack_propagate(False)

                # Body
                body_msg = ctk.CTkFrame(msg_win, fg_color="transparent")
                body_msg.pack(side="top", fill="both", expand=True, padx=20, pady=(8, 4))
                ctk.CTkLabel(body_msg,
                             text="Messaggio per l'amministratore (opzionale)",
                             font=("Segoe UI", 10, "bold"),
                             text_color=PALETTE["text"]).pack(anchor="w", pady=(4, 4))
                msg_text = ctk.CTkTextbox(
                    body_msg, height=110, fg_color=PALETTE["surface"],
                    border_color=PALETTE["border"], border_width=1,
                    text_color=PALETTE["text"], font=("Segoe UI", 10),
                )
                msg_text.pack(fill="both", expand=True, pady=(0, 4))

                def _do_send():
                    text = msg_text.get("1.0", "end").strip()
                    btn_send.configure(state="disabled", text="Invio…")
                    btn_cancel.configure(state="disabled")
                    def _w():
                        try:
                            self.api_client.request_upgrade(target_plan, text)
                            self.root.after(0, lambda: (
                                msg_win.destroy(),
                                win.destroy(),
                                messagebox.showinfo("Richiesta inviata",
                                    f"Richiesta di upgrade a "
                                    f"{display_names.get(target_plan)} inviata.\n"
                                    f"Riceverai notifica quando l'amministratore avrà risposto."),
                            ))
                        except Exception as e:
                            err_str = str(e)
                            self.root.after(0, lambda: (
                                btn_send.configure(state="normal", text="Invia richiesta"),
                                btn_cancel.configure(state="normal"),
                                messagebox.showerror("Errore",
                                    f"Invio fallito:\n{err_str}"),
                            ))
                    threading.Thread(target=_w, daemon=True).start()

                btn_cancel = ctk.CTkButton(
                    br, text="Annulla", width=110, height=34,
                    fg_color="transparent", hover_color=PALETTE["surface"],
                    text_color=PALETTE["text_dim"],
                    font=("Segoe UI", 10), command=msg_win.destroy)
                btn_cancel.pack(side="right", padx=(4, 0))
                btn_send = ctk.CTkButton(
                    br, text="Invia richiesta", width=150, height=34,
                    fg_color=PALETTE.get("primary", "#3b6fd4"),
                    hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
                    text_color="#ffffff",
                    font=("Segoe UI", 10, "bold"), command=_do_send)
                btn_send.pack(side="right")
                msg_win.bind("<Escape>", lambda e: msg_win.destroy())
            return _handler

        for ci, p in enumerate(available_upgrades, start=2):
            cell = ctk.CTkFrame(btn_bar, fg_color="transparent")
            cell.grid(row=0, column=ci, padx=8, pady=12, sticky="nsew")
            ctk.CTkButton(
                cell, text=f"⬆  Richiedi  {display_names.get(p)}",
                font=("Segoe UI", 10, "bold"),
                fg_color=PALETTE.get("primary", "#3b6fd4"),
                hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
                text_color="#ffffff",
                height=44, corner_radius=8,
                command=_make_upgrade_handler(p),
            ).pack(fill="both", expand=True)

        # ── 3. Header piani ──────────────────────────────────────────
        header_frame = ctk.CTkFrame(win, fg_color=PALETTE["bg"], height=HEADER_H)
        header_frame.pack(side="top", fill="x", padx=16, pady=(12, 0))
        header_frame.pack_propagate(False)
        header_frame.columnconfigure(0, weight=0, minsize=FEATURE_COL_W)
        for i in range(1, n_plans + 1):
            header_frame.columnconfigure(i, weight=1, minsize=PLAN_COL_W,
                                          uniform="cols")
        ctk.CTkLabel(header_frame, text="").grid(row=0, column=0, sticky="nsew")
        for i, p in enumerate(plans_to_show, start=1):
            is_current = (p == current_plan)
            cell = ctk.CTkFrame(
                header_frame,
                fg_color=PALETTE["surface2"] if is_current else PALETTE.get("primary", "#3b6fd4"),
                corner_radius=8,
            )
            cell.grid(row=0, column=i, padx=4, pady=4, sticky="nsew")
            ctk.CTkLabel(cell, text=display_names.get(p, p),
                         font=("Segoe UI", 14, "bold"),
                         text_color=PALETTE["text"] if is_current else "#ffffff"
                         ).pack(pady=(14, 2))
            ctk.CTkLabel(cell,
                         text="(piano attuale)" if is_current else "disponibile",
                         font=("Segoe UI", 9),
                         text_color=PALETTE["text_dim"] if is_current else "#cfdaff"
                         ).pack()

        # ── 4. Body (riempie il resto) ───────────────────────────────
        body = ctk.CTkScrollableFrame(
            win, fg_color=PALETTE["bg"],
            scrollbar_button_color=PALETTE["surface2"],
            scrollbar_button_hover_color=PALETTE.get("primary", "#3b6fd4"),
        )
        body.pack(side="top", fill="both", expand=True, padx=16, pady=(8, 8))
        body.columnconfigure(0, weight=0, minsize=FEATURE_COL_W)
        for i in range(1, n_plans + 1):
            body.columnconfigure(i, weight=1, minsize=PLAN_COL_W, uniform="cols")

        # Righe feature
        for row_idx, (key, label) in enumerate(feature_labels):
            bg = PALETTE["surface"] if row_idx % 2 == 0 else PALETTE["surface2"]
            lcell = ctk.CTkFrame(body, fg_color=bg, height=32, corner_radius=0)
            lcell.grid(row=row_idx, column=0, sticky="nsew")
            lcell.pack_propagate(False)
            ctk.CTkLabel(lcell, text=label, anchor="w",
                         font=("Segoe UI", 10),
                         text_color=PALETTE["text"]
                         ).pack(side="left", fill="x", padx=14, pady=6)
            for ci, p in enumerate(plans_to_show, start=1):
                has = PLAN_FEATURES.get(p, {}).get(key, False)
                cell = ctk.CTkFrame(body, fg_color=bg, height=32, corner_radius=0)
                cell.grid(row=row_idx, column=ci, sticky="nsew")
                cell.pack_propagate(False)
                ctk.CTkLabel(cell, text="✓" if has else "—",
                             font=("Segoe UI", 13, "bold"),
                             text_color="#50aa70" if has else "#555566"
                             ).pack(pady=6)

        # Righe limiti
        for offset, (key, label) in enumerate(limits_labels.items()):
            row_idx = len(feature_labels) + offset
            bg = PALETTE["surface"] if row_idx % 2 == 0 else PALETTE["surface2"]
            lcell = ctk.CTkFrame(body, fg_color=bg, height=32, corner_radius=0)
            lcell.grid(row=row_idx, column=0, sticky="nsew")
            lcell.pack_propagate(False)
            ctk.CTkLabel(lcell, text=label, anchor="w",
                         font=("Segoe UI", 10, "bold"),
                         text_color=PALETTE["text"]
                         ).pack(side="left", padx=14, pady=6)
            for ci, p in enumerate(plans_to_show, start=1):
                val = PLAN_FEATURES.get(p, {}).get(key, 0)
                disp = "∞" if val == -1 else str(val)
                cell = ctk.CTkFrame(body, fg_color=bg, height=32, corner_radius=0)
                cell.grid(row=row_idx, column=ci, sticky="nsew")
                cell.pack_propagate(False)
                ctk.CTkLabel(cell, text=disp,
                             font=("Segoe UI", 11, "bold"),
                             text_color=PALETTE["text"]
                             ).pack(pady=6)

        win.bind("<Escape>", lambda e: win.destroy())


    def _show_change_password_dialog(self):
        """
        v0.0.2.3: Dialog modale per cambio password.

        Campi: password corrente, nuova password, conferma nuova.
        Validazione client-side minima (min 8 chars, conferma match).
        Il server rivalida e risponde 403/400 in caso di errori.
        Su successo, mostra messaggio e suggerisce di ri-loggarsi.
        """
        import customtkinter as ctk
        from tkinter import messagebox
        import threading

        if self.api_client is None:
            messagebox.showerror("Non disponibile",
                                 "Il cambio password richiede la modalità connessa al server.")
            return

        win = ctk.CTkToplevel(self.root)
        win.title("Cambia password")
        win.geometry("440x400")
        win.resizable(False, False)
        win.transient(self.root)
        try:
            win.grab_set()
        except Exception:
            pass
        win.configure(fg_color=PALETTE["bg"])
        self._set_win_icon(win)

        # Centra sulla main window
        self.root.update_idletasks()
        mx = self.root.winfo_x()
        my = self.root.winfo_y()
        mw = self.root.winfo_width()
        mh = self.root.winfo_height()
        win.geometry(f"440x400+{mx + (mw-440)//2}+{my + (mh-400)//2}")

        # Header
        ctk.CTkLabel(win, text="🔒  Cambia password",
                     font=("Segoe UI", 15, "bold"),
                     text_color=PALETTE["text"]
                     ).pack(pady=(20, 4))
        ctk.CTkLabel(win,
                     text="Inserisci la password attuale e la nuova.",
                     font=("Segoe UI", 10),
                     text_color=PALETTE["text_dim"]
                     ).pack(pady=(0, 14))

        # Form
        form = ctk.CTkFrame(win, fg_color=PALETTE.get("surface", "#1e2533"),
                            corner_radius=10)
        form.pack(fill="x", padx=24, pady=(0, 8))

        def _labeled_entry(label_text: str):
            ctk.CTkLabel(form, text=label_text, anchor="w",
                         font=("Segoe UI", 10, "bold"),
                         text_color=PALETTE["text"]
                         ).pack(fill="x", padx=14, pady=(12, 2))
            var = ctk.StringVar()
            e = ctk.CTkEntry(form, textvariable=var, show="•",
                             fg_color=PALETTE.get("surface2", "#2a3344"),
                             border_color=PALETTE["border"],
                             text_color=PALETTE["text"], height=32)
            e.pack(fill="x", padx=14, pady=(0, 6))
            return var, e

        cur_var, cur_entry = _labeled_entry("Password corrente")
        new_var, _         = _labeled_entry("Nuova password (minimo 8)")
        cnf_var, _         = _labeled_entry("Conferma nuova")
        cur_entry.focus()

        # Status label (errori inline)
        status_var = ctk.StringVar(value="")
        status_lbl = ctk.CTkLabel(win, textvariable=status_var,
                                  font=("Segoe UI", 10),
                                  text_color=PALETTE["text_dim"],
                                  wraplength=380, justify="center")
        status_lbl.pack(pady=(8, 4))

        # Buttons row
        btn_row = ctk.CTkFrame(win, fg_color="transparent")
        btn_row.pack(fill="x", padx=24, pady=(4, 16))

        def _set_status(msg: str, color_key: str = "text_dim"):
            status_var.set(msg)
            status_lbl.configure(text_color=PALETTE.get(color_key, PALETTE["text_dim"]))

        def _do_submit():
            cur = cur_var.get()
            new = new_var.get()
            cnf = cnf_var.get()
            if not cur or not new:
                _set_status("Compila tutti i campi", "error")
                return
            if len(new) < 8:
                _set_status("La nuova password deve essere di almeno 8 caratteri", "error")
                return
            if new != cnf:
                _set_status("Le due password nuove non coincidono", "error")
                return
            if new == cur:
                _set_status("La nuova password deve essere diversa dalla corrente", "error")
                return

            btn_ok.configure(text="Aggiornamento…", state="disabled")
            btn_cancel.configure(state="disabled")
            _set_status("Invio al server…", "text_dim")

            def _worker():
                try:
                    self.api_client.change_password(cur, new)
                    self.root.after(0, lambda: (
                        win.destroy(),
                        messagebox.showinfo(
                            "Password aggiornata",
                            "La tua password è stata cambiata con successo.\n\n"
                            "La sessione corrente resta attiva. I token esistenti\n"
                            "non sono invalidati automaticamente."
                        )
                    ))
                except Exception as e:
                    msg = str(e)
                    if "403" in msg:
                        msg = "Password corrente errata"
                    elif "400" in msg:
                        msg = "La nuova password non è valida"
                    self.root.after(0, lambda: (
                        _set_status(msg, "error"),
                        btn_ok.configure(text="Conferma", state="normal"),
                        btn_cancel.configure(state="normal"),
                    ))
            threading.Thread(target=_worker, daemon=True).start()

        btn_cancel = ctk.CTkButton(
            btn_row, text="Annulla", width=110, height=34,
            fg_color="transparent", hover_color=PALETTE.get("surface", "#1e2533"),
            text_color=PALETTE["text_dim"],
            font=("Segoe UI", 10),
            command=win.destroy,
        )
        btn_cancel.pack(side="right", padx=(4, 0))
        btn_ok = ctk.CTkButton(
            btn_row, text="Conferma", width=150, height=34,
            fg_color=PALETTE.get("primary", "#3b6fd4"),
            hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
            text_color="#ffffff",
            font=("Segoe UI", 10, "bold"),
            command=_do_submit,
        )
        btn_ok.pack(side="right")
        win.bind("<Return>", lambda e: _do_submit())
        win.bind("<Escape>", lambda e: win.destroy())


    def _close_profile_flyout(self):
        """v1078: chiude sia flyout principale che eventuale sub-flyout.
        v1085d: cancella anche timer di mouse-leave polling."""
        # v1080: cancella eventuale hover-to-open timer pendente
        try:
            if getattr(self, "_hover_open_after", None) is not None:
                self.root.after_cancel(self._hover_open_after)
                self._hover_open_after = None
        except Exception:
            pass
        # v1085d: cancella poll mouse e close-timer
        for attr in ("_flyout_poll_id", "_flyout_close_timer"):
            try:
                t = getattr(self, attr, None)
                if t:
                    self.root.after_cancel(t)
                    setattr(self, attr, None)
            except Exception:
                pass
        try:
            if getattr(self, "_profile_sub_flyout", None) and self._profile_sub_flyout.winfo_exists():
                self._profile_sub_flyout.destroy()
        except Exception:
            pass
        self._profile_sub_flyout = None
        try:
            if getattr(self, "_profile_flyout", None) and self._profile_flyout.winfo_exists():
                self._profile_flyout.destroy()
        except Exception:
            pass
        self._profile_flyout = None
        self._profile_active_sub = None
        try:
            self.root.unbind("<Button-1>", self._profile_flyout_bind)
        except Exception:
            pass


    def _apply_tab_icons(self):
        """v1073: applica icone ai pulsanti del CTkTabview dopo la renderizzazione."""
        tab_icon_map = {
            "  Log":        ("log",      18),
            "  DB Locale":  ("db_locale",18),
            "  Generi":     ("generi",   18),
            "  Cache":      ("cache",    18),
            "  Qualità":    ("qualita",  18),
            "  Caraibica":  ("caraibica",18),
            "  Avanzate":   ("avanzate", 18),
        }
        try:
            # CTkTabview espone i bottoni via _segmented_button._buttons_dict
            seg = self._tabview._segmented_button
            for tab_name, (icon_name, size) in tab_icon_map.items():
                if tab_name in seg._buttons_dict:
                    btn = seg._buttons_dict[tab_name]
                    _img = _get_icon(icon_name, size) if _ICONS_AVAILABLE else None
                    if _img:
                        btn.configure(image=_img, compound="left")
        except Exception:
            pass  # fallback silenzioso se la struttura interna CTk cambia

    def _apply_plan_restrictions(self):
        """
        Mostra/nasconde tab e feature in base al piano utente attivo.

        v0.0.2.4: implementazione vera. Per i tab non concessi dal piano:
          - sostituiamo il contenuto con un overlay "Feature non disponibile"
          - aggiungiamo un pulsante "Richiedi upgrade" che apre il dialog
        Per le opzioni catalog disabilitate:
          - check disabilitato + tooltip "Disponibile dal piano X"

        In modalità server (api_client != None) le feature arrivano da
        user_info.features (decodificate dal JWT). In modalità locale
        si fallback su config.user_plans.
        """
        # Determina features attive
        features = None
        plan_name = "base"
        if self.api_client is not None and self.user_info.get("features"):
            features = self.user_info["features"]
            plan_name = self.user_info.get("plan", "base")
        else:
            try:
                from config.user_plans import get_plan as _gp, PLAN_FEATURES
                plan = _gp()
                plan_name = plan.plan
                features = PLAN_FEATURES.get(plan_name, PLAN_FEATURES["advanced"])
            except Exception:
                return
        if not features:
            return

        # Mappa tab_name → feature_key
        tab_feature_map = {
            "  Cache":    "tab_cache",
            "  Qualità":  "tab_quality",
            "  Caraibica": "tab_caribbean",
            "  Avanzate":  "tab_advanced",
        }

        # Trova i nomi reali dei tab (variano con icone/spazi)
        try:
            existing_tabs = list(self._tabview._tab_dict.keys())
        except Exception:
            existing_tabs = []

        def _normalize(s: str) -> str:
            return s.strip().lower()

        plan_display = {"base": "🆓 Base", "pro": "⭐ Pro",
                        "advanced": "💎 Advanced"}.get(plan_name, plan_name)

        for tab_name, feat_key in tab_feature_map.items():
            has = features.get(feat_key, True)
            # Trova il vero tab key (matching su substring)
            real_key = None
            for k in existing_tabs:
                if _normalize(tab_name) in _normalize(k):
                    real_key = k
                    break
            if real_key is None:
                continue

            try:
                tab_widget = self._tabview.tab(real_key)
            except Exception:
                continue

            # Pulisci eventuale overlay precedente (re-apply post upgrade)
            for child in tab_widget.winfo_children():
                try:
                    if getattr(child, "_lock_overlay", False):
                        child.destroy()
                except Exception:
                    pass

            if not has:
                # Overlay "feature locked"
                self._add_lock_overlay(tab_widget, feat_key, plan_name, plan_display)

        # Disabilita check 'Abilita Sorgenti DB Online' se feature non concessa
        try:
            ext_db_widget = getattr(self, "_chk_use_ext_db", None)
            if not features.get("catalog_external_db", True):
                self._opt_use_ext_db.set(False)
                if ext_db_widget is not None:
                    ext_db_widget.configure(state="disabled")
            else:
                if ext_db_widget is not None:
                    ext_db_widget.configure(state="normal")
        except Exception:
            pass

        # Feature cover
        try:
            if not features.get("catalog_cover", True):
                if hasattr(self, "_cover_enabled"):
                    self._cover_enabled.set(False)
        except Exception:
            pass

        # v1077: aggiorna badge piano
        try:
            self._plan_badge.configure(text=plan_display)
        except Exception:
            pass


    def _add_lock_overlay(self, tab_widget, feature_key: str,
                          current_plan: str, current_plan_display: str):
        """
        Sovrappone un widget "lock" sopra al contenuto del tab.

        Mostra:
          - icona lucchetto
          - nome feature
          - "Disponibile dal piano X"
          - Pulsante "Richiedi upgrade" (solo se in modalità server)
        """
        import customtkinter as ctk

        # Determina quale piano sblocca questa feature
        feature_unlock_plan = {
            "tab_cache":      "pro",
            "tab_caribbean":  "pro",
            "tab_advanced":   "advanced",
            "tab_quality":    "base",  # fallback (dovrebbe essere già in base)
        }.get(feature_key, "pro")

        plan_disp = {"base": "🆓 Base", "pro": "⭐ Pro",
                     "advanced": "💎 Advanced"}

        feature_name = {
            "tab_cache":      "Cache metadati",
            "tab_caribbean":  "Classificazione Caraibica",
            "tab_advanced":   "Tab Avanzate",
            "tab_quality":    "Tab Qualità",
        }.get(feature_key, feature_key)

        # Nasconde i widget esistenti dietro l'overlay (NON li distrugge —
        # se l'utente fa upgrade durante la sessione, l'overlay viene
        # rimosso e i widget originali tornano visibili)
        for child in tab_widget.winfo_children():
            try:
                child.grid_forget()
                child.pack_forget()
                child.place_forget()
            except Exception:
                pass

        overlay = ctk.CTkFrame(tab_widget, fg_color=PALETTE["bg"])
        overlay._lock_overlay = True
        overlay.pack(fill="both", expand=True)

        center = ctk.CTkFrame(overlay, fg_color="transparent")
        center.place(relx=0.5, rely=0.5, anchor="center")

        ctk.CTkLabel(center, text="🔒",
                     font=("Segoe UI", 56),
                     text_color=PALETTE["text_dim"]
                     ).pack(pady=(0, 8))
        ctk.CTkLabel(center, text=feature_name,
                     font=("Segoe UI", 18, "bold"),
                     text_color=PALETTE["text"]
                     ).pack()
        ctk.CTkLabel(center,
                     text=f"Funzione disponibile dal piano "
                          f"{plan_disp.get(feature_unlock_plan, feature_unlock_plan)}",
                     font=("Segoe UI", 11),
                     text_color=PALETTE["text_dim"]
                     ).pack(pady=(4, 4))
        ctk.CTkLabel(center,
                     text=f"Il tuo piano attuale: {current_plan_display}",
                     font=("Segoe UI", 10),
                     text_color=PALETTE["text_dim"]
                     ).pack(pady=(0, 18))

        # Pulsante upgrade (solo in modalità server)
        if self.api_client is not None:
            plan_order = ["base", "pro", "advanced"]
            try:
                idx_cur = plan_order.index(current_plan)
                upgrades = plan_order[idx_cur + 1:]
            except ValueError:
                upgrades = []

            if upgrades:
                ctk.CTkButton(
                    center, text="⬆  Richiedi upgrade",
                    font=("Segoe UI", 11, "bold"),
                    fg_color=PALETTE.get("primary", "#3b6fd4"),
                    hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
                    text_color="#ffffff",
                    width=200, height=36, corner_radius=8,
                    command=lambda: self._show_upgrade_dialog(current_plan, upgrades),
                ).pack()

    def _add_tooltip(self, widget, text: str):
        """
        v1075: Tooltip SINGLETON GLOBALE.

        Architettura (fix "ghost tooltips" che si sovrappongono passando veloce
        sopra widget contigui):

        - Esiste UNA SOLA istanza `CTkToplevel` a livello di app: `self._global_tip`.
          Ogni `<Enter>` la ricrea, ogni `<Leave>` la distrugge — non possono mai
          esserci 2 tooltip vivi contemporaneamente.

        - Delay di 400ms prima del show: evita il flicker quando si scorre
          velocemente sopra una fila di widget adiacenti. Se l'utente esce
          prima dei 400ms, il tooltip non appare affatto.

        - Timeout di sicurezza a 2500ms: anche se `<Leave>` si perde per
          qualche motivo (es. focus-steal del subprocess), il tooltip viene
          distrutto comunque.

        Sostituisce i 3 helper tooltip precedenti (`_add_tooltip`, `_bind_tooltip`,
        `_tooltip_carib`) — riscritti tutti come wrapper di questo singleton.
        """
        widget._tip_pending = None  # id del after() pendente

        def _schedule_show(e):
            # cancella eventuale show pendente su questo stesso widget
            if widget._tip_pending is not None:
                try:
                    widget.after_cancel(widget._tip_pending)
                except Exception:
                    pass
                widget._tip_pending = None
            # programma lo show tra 400ms
            widget._tip_pending = widget.after(
                400, lambda ex=e.x_root, ey=e.y_root: self._show_global_tooltip(text, ex, ey)
            )

        def _cancel(e):
            # cancella show pendente
            if widget._tip_pending is not None:
                try:
                    widget.after_cancel(widget._tip_pending)
                except Exception:
                    pass
                widget._tip_pending = None
            # distruggi tooltip attivo
            self._hide_global_tooltip()

        widget.bind("<Enter>", _schedule_show, add="+")
        widget.bind("<Leave>", _cancel, add="+")
        # fallback: se il widget viene cliccato, chiudi subito il tooltip
        widget.bind("<Button-1>", _cancel, add="+")

    def _show_global_tooltip(self, text: str, x_root: int, y_root: int):
        """v1075: Mostra il tooltip globale. Distrugge l'eventuale precedente."""
        self._hide_global_tooltip()
        try:
            t = ctk.CTkToplevel(self.root)
            t.overrideredirect(True)
            t.attributes("-topmost", True)
            t.geometry(f"+{x_root + 14}+{y_root + 20}")
            ctk.CTkLabel(
                t, text=text,
                font=(FONT_SMALL[0], FONT_SMALL[1] - 1),
                fg_color=PALETTE["surface2"],
                text_color=PALETTE["text"],
                corner_radius=5, padx=8, pady=3,
            ).pack()
            self._global_tip = t
            # safety timeout
            self._global_tip_after = t.after(2500, self._hide_global_tooltip)
        except Exception:
            pass

    def _hide_global_tooltip(self):
        """v1075: Distrugge il tooltip globale attivo (se esiste)."""
        try:
            if getattr(self, "_global_tip_after", None):
                try:
                    self._global_tip.after_cancel(self._global_tip_after)
                except Exception:
                    pass
                self._global_tip_after = None
        except Exception:
            pass
        try:
            if getattr(self, "_global_tip", None) is not None:
                self._global_tip.destroy()
        except Exception:
            pass
        self._global_tip = None

    def _center_win(self, win, w: int, h: int):
        """Centra un CTkToplevel sullo schermo."""
        win.update_idletasks()
        sw = win.winfo_screenwidth()
        sh = win.winfo_screenheight()
        win.geometry(f"{w}x{h}+{(sw-w)//2}+{(sh-h)//2}")

    def _set_win_icon(self, win):
        """
        Imposta l'icona app su qualsiasi finestra figlia.

        v1076: auto-retry con `after(250)` per compensare il timing asincrono
        di `CTkToplevel` su Windows. `iconbitmap()` chiamato prima che la
        finestra sia completamente "realized" può non attaccarsi (bug storico
        tkinter). Applichiamo l'icona subito E dopo 250ms: la seconda
        chiamata è lo sforzo che fa davvero attaccare l'icona nella stragrande
        maggioranza dei casi.
        """
        def _apply():
            try:
                if not win.winfo_exists():
                    return
                from gui.app_icon import set_window_icon
                set_window_icon(win)
            except Exception:
                pass

        _apply()                    # 1° tentativo immediato (Linux/Mac)
        try:
            win.after(250, _apply)  # 2° tentativo dopo il mapping (Windows/CTk)
        except Exception:
            pass


    def _center_main_window(self):
        """v1065: centra la finestra principale dopo che il layout è costruito."""
        self.root.update_idletasks()
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        w, h = 1300, 860
        x = max(0, (sw - w) // 2)
        y = max(0, (sh - h) // 2)
        self.root.geometry(f"{w}x{h}+{x}+{y}")

    # v1076: def _build_custom_menubar — rimossa in v1077 (Opzione C).
    #   Il menu custom su row=0 è stato sostituito dal flyout profilo che
    #   ospita Impostazioni/Lingua/Piani/Aiuto/Esci. Gli strumenti File/Help
    #   sono stati spostati nel flyout o nel tab Avanzate → Manutenzione.

    def _load_recent_dirs(self) -> list:
        """v1038: carica storico directory da file JSON."""
        try:
            cfg = _get_data_dir() / "recent_dirs.json"
            if cfg.exists():
                import json as _json
                data = _json.loads(cfg.read_text(encoding="utf-8"))
                return [d for d in data if Path(d).is_dir()][:10]
        except Exception:
            pass
        return []

    def _save_recent_dirs(self) -> None:
        """v1038: salva storico directory su file JSON."""
        try:
            cfg = _get_data_dir() / "recent_dirs.json"
            import json as _json
            cfg.write_text(_json.dumps(self._recent_dirs, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _add_recent_dir(self, path: str) -> None:
        """v1038: aggiunge path in cima allo storico, rimuove duplicati."""
        if path in self._recent_dirs:
            self._recent_dirs.remove(path)
        self._recent_dirs.insert(0, path)
        self._recent_dirs = self._recent_dirs[:10]
        self._save_recent_dirs()
        self._refresh_recent_menu()

    def _refresh_recent_menu(self) -> None:
        """
        v1078: no-op dopo Opzione C (v1077).

        Prima della rimozione della menubar custom, questo metodo ripopolava
        il sottomenu "File → Directory Recenti" di `self._recent_menu` ad ogni
        modifica dello storico. Dopo la rimozione non esiste più un menu
        condiviso da tenere sincronizzato: il dropdown della sidebar
        (`_show_recent_dropdown`) crea/ripopola il suo `tk.Menu` on-demand
        leggendo direttamente `self._recent_dirs`.

        Mantengo il metodo come no-op per non rompere i due chiamanti
        esistenti (`_add_recent_dir`, `_select_path` → save e reload).
        """
        return

    # ─── LAYOUT ────────────────────────────────────────────────

    def _build_layout(self):
        # v1077: Opzione C — rimossa la toolbar custom; menu File/Help/Strumenti
        # integrati nel flyout profilo (nome utente → Impostazioni, Lingua,
        # Piani, Aiuto, Esci). Gli strumenti ex-menu (Apri Cartella Log,
        # Test Configurazione) sono spostati nel tab Avanzate → Manutenzione.
        # v1081: niente _install_resize_handler — il resize è gestito via
        # self.root.minsize/maxsize in __init__ (range 1100×720 → 1500×960).
        self.root.columnconfigure(1, weight=1)
        self.root.rowconfigure(0, weight=1)
        self._build_left_panel()
        self._build_right_panel()

    # v1080: _install_resize_handler → rimosso in v1081
    #   Lo hide-during-drag placeholder non risolveva il lag percepito
    #   dall'utente; siamo passati a min/max size stretti in __init__.

    def _build_left_panel(self):
        left = ctk.CTkFrame(
            self.root, width=400, fg_color=PALETTE["surface"], corner_radius=0
        )
        left.grid(row=0, column=0, sticky="nsew")   # v1077: torna a row=0 (toolbar rimossa)
        left.columnconfigure(0, weight=1)
        left.grid_propagate(False)  # mantiene width=400 fisso

        # v1076: struttura a 3 zone per gestire finestre basse verticalmente
        #   - header (logo + badge + version): pack top, fisso
        #   - middle (sezioni dir/options/dup/cover): CTkScrollableFrame,
        #     si espande e mostra la scrollbar verticale quando serve
        #   - footer (bottoni Avvia/Ferma/Pulisci): pack bottom, fisso
        # Questo evita che le sezioni vengano tagliate quando l'utente riduce
        # la finestra in altezza.

        # ══ Zona HEADER (fisso in alto) ═════════════════════════════════════
        # v1078: layout a 2 righe, compatto per guadagnare spazio verticale
        # e nascondere la scrollbar della sidebar in configurazione standard.
        #   Riga 1 → [icona] Music Cataloger  v1078
        #   Riga 2 → [ph-user  NomeUtente  ▼]   (full-width, stile pill blu)
        # Badge piano rimosso (refuso: il piano è nel flyout, voce "Piani").
        _hdr = ctk.CTkFrame(left, fg_color="transparent")
        _hdr.pack(fill="x", padx=20, pady=(18, 4))
        _hdr.columnconfigure(0, weight=1)

        _ic_title = _get_icon("title_icon", 44) if _ICONS_AVAILABLE else None
        # Riga 1: titolo + versione come appendice inline
        _title_row = ctk.CTkFrame(_hdr, fg_color="transparent")
        _title_row.grid(row=0, column=0, sticky="ew")
        ctk.CTkLabel(_title_row, text="  Music Cataloger",
                     font=FONT_TITLE, text_color=PALETTE["text"],
                     image=_ic_title, compound="left"
                     ).pack(side="left")
        ctk.CTkLabel(_title_row, text=f"  {APP_VERSION}",
                     font=(FONT_SMALL[0], FONT_SMALL[1]-1),
                     text_color=PALETTE["text_dim"]
                     ).pack(side="left", padx=(4, 0), pady=(8, 0), anchor="s")

        # Riga 2: pulsante profilo full-width stile "pill" blu primary
        # (stesso aspetto del vecchio badge piano v1072d)
        # v1085c: in modalità server username viene da user_info, non da
        # config.user_plans (che ha sempre "DJ" come default offline).
        try:
            if self.api_client is not None and self.user_info:
                _username = (self.user_info.get("username")
                             or self.user_info.get("email", "User").split("@")[0]
                             or "User")
                _plan_name_raw = self.user_info.get("plan", "base")
                _disp_map = {"base": "🆓 Base", "pro": "⭐ Pro",
                             "advanced": "💎 Advanced"}
                _plan_name = _disp_map.get(_plan_name_raw,
                                            _plan_name_raw.capitalize())
            else:
                from config.user_plans import get_plan as _gp
                _plan = _gp()
                _username = getattr(_plan, "username", None) or "User"
                _plan_name = _plan.display_name
        except Exception:
            _username = "User"
            _plan_name = "Advanced"
        _ic_user = _get_icon("profile", 18) if _ICONS_AVAILABLE else None
        # v1080: width=0 + sticky="w" → il bottone si adatta alla larghezza
        # del contenuto (icona + "  Nome utente" + padding interno), invece
        # di stirarsi a tutta la larghezza del titolo come in v1079.
        # Padding interno aggiuntivo via padx interno del text per un
        # aspetto "pill" più naturale (non troppo stretto attorno al testo).
        self._profile_btn = ctk.CTkButton(
            _hdr, text=f"  {_username}   ", image=_ic_user, compound="left",
            height=32, width=0,
            font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
            fg_color=PALETTE.get("primary", "#3b6fd4"),
            hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
            text_color="#ffffff",
            corner_radius=16, anchor="w",
            command=self._show_profile_panel,
        )
        self._profile_btn.grid(row=1, column=0, sticky="w", pady=(10, 0))
        # v1078: badge piano rimosso (refuso) — il piano è nel flyout "Piani"
        # Creo comunque un placeholder che i due callsite esistenti possono
        # ancora aggiornare senza errori.
        self._plan_badge = ctk.CTkLabel(_hdr, text="", height=0)  # hidden, non-packed

        ctk.CTkFrame(left, height=1, fg_color=PALETTE["border"]).pack(fill="x", padx=20, pady=(12, 6))

        # ══ Zona FOOTER (fissa in basso — pack PRIMA del middle per side=bottom) ══
        _footer = ctk.CTkFrame(left, fg_color="transparent")
        _footer.pack(side="bottom", fill="x")
        self._build_action_buttons(_footer)

        # ══ Zona MIDDLE (sezioni contenuto — NO SCROLLABLE) ═════════════════
        # v1079: rimosso CTkScrollableFrame dalla sidebar — era la causa del
        # resize lentissimo (ricalcola viewport e scroll region ad ogni
        # <Configure> del canvas interno, e con ~120 widget figli è un costo
        # proibitivo per ogni pixel di drag del bordo finestra).
        #
        # Con l'header compatto v1078 (2 righe invece di 3) e le checkbox
        # opzioni compattate (rimosso separatore interno), le 4 sezioni +
        # bottoni azione + status bar entrano in ~580px verticali. Su un
        # laptop 1366×768 (altezza utile ~700px) tutto è visibile senza
        # bisogno di scrollbar; su un desktop 1920×1080 resta oltre 400px
        # di margine.
        #
        # Trade-off accettato: se l'utente riduce la finestra sotto 600px
        # di altezza le ultime sezioni verranno tagliate — comportamento
        # tk nativo, senza lag. I pulsanti azione sono già ancorati in
        # bottom quindi restano sempre raggiungibili dall'alto scollando
        # con la rotellina (via i bind locali dei widget).
        _middle = ctk.CTkFrame(left, fg_color="transparent")
        _middle.pack(fill="both", expand=True)
        _middle.columnconfigure(0, weight=1)

        # v1053: pannello sinistro ridotto al minimo essenziale
        self._left_dir_frame     = self._build_dir_section(_middle)
        self._left_options_frame = self._build_options_section(_middle)
        self._left_dup_frame     = self._build_duplicate_section(_middle)
        self._left_cover_frame   = self._build_cover_section_slim(_middle)

        self._status_var = ctk.StringVar(value="✓  Pronto")
        ctk.CTkLabel(left, textvariable=self._status_var,
                     font=FONT_SMALL, text_color=PALETTE["text_dim"], anchor="w"
                     ).pack(padx=20, pady=(8, 20), fill="x")

    def _build_dir_section(self, parent):
        frm = ctk.CTkFrame(parent, fg_color=PALETTE["bg"], corner_radius=10)
        frm.pack(fill="x", padx=16, pady=(0, 10))
        frm.columnconfigure(0, weight=1)

        _ic_dir = _get_icon("directory", 22) if _ICONS_AVAILABLE else None
        ctk.CTkLabel(frm, text="  Directory Musicale", font=FONT_HEAD,
                     image=_ic_dir, compound="left"
                     ).grid(row=0, column=0, columnspan=3, padx=14, pady=(12, 6), sticky="w")

        # v1068: breadcrumb stile Windows Explorer (> Desktop > Pedro > Musica)
        self._breadcrumb_frame = ctk.CTkFrame(
            frm, fg_color=PALETTE["surface"], corner_radius=6, height=BTN_H
        )
        self._breadcrumb_frame.grid(row=1, column=0, padx=(14, 4), pady=(0, 12), sticky="ew")
        self._breadcrumb_frame.grid_propagate(False)
        self._breadcrumb_frame.columnconfigure(0, weight=1)
        self._breadcrumb_lbl = ctk.CTkLabel(
            self._breadcrumb_frame,
            text="  Seleziona una cartella...",
            font=FONT_SMALL, text_color=PALETTE["text_dim"], anchor="w",
        )
        self._breadcrumb_lbl.grid(row=0, column=0, padx=4, sticky="ew")
        # Entry nascosto — mantiene _selected_path per compatibilità
        self._path_entry = ctk.CTkEntry(
            frm, textvariable=self._selected_path, state="readonly",
            font=FONT_SMALL, height=BTN_H, fg_color=PALETTE["surface"],
        )
        # Non gridded — solo come holder della StringVar

        # Bottone Sfoglia principale
        _sfoglia_icon = _get_icon("folder_32", 20) if _ICONS_AVAILABLE else None
        ctk.CTkButton(frm, text="  Sfoglia", command=self._browse,
                      height=BTN_H, width=95, font=FONT_BODY,
                      image=_sfoglia_icon, compound="left",
                      fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
                      corner_radius=6,
                      ).grid(row=1, column=1, padx=(0, 2), pady=(0, 12))

        # v1049: bottone freccia ▾ che apre il menu recenti inline
        self._btn_recent = ctk.CTkButton(
            frm, text="▾", command=self._show_recent_dropdown,
            height=BTN_H, width=20, font=("Segoe UI", 11, "bold"),
            fg_color=PALETTE["primary_hover"], hover_color=PALETTE["bg"],
            corner_radius=6,
        )
        self._btn_recent.grid(row=1, column=2, padx=(0, 14), pady=(0, 12))
        return frm


    def _show_recent_dropdown(self):
        """v1069b: riusa un unico tk.Menu per non superare il limite 32 menu Windows.
        Il menu viene creato una sola volta e svuotato/ripopolato ad ogni apertura.
        """
        import tkinter as tk
        if not self._recent_dirs:
            return
        # Crea o riusa il menu persistente
        if not hasattr(self, "_recent_menu_widget") or self._recent_menu_widget is None:
            try:
                self._recent_menu_widget = tk.Menu(
                    self.root, tearoff=0,
                    bg=PALETTE["surface"], fg=PALETTE["text"],
                    activebackground=PALETTE["primary"],
                    activeforeground=PALETTE["bg"],
                    font=("Segoe UI", 10), bd=0, relief="flat"
                )
            except Exception:
                return
        # Svuota e ripopola
        menu = self._recent_menu_widget
        menu.delete(0, "end")
        for d in self._recent_dirs:
            label = d if len(d) <= 55 else "…" + d[-52:]
            menu.add_command(label=label, command=lambda p=d: self._select_path(p))
        menu.add_separator()
        menu.add_command(label="Cancella storico", command=self._clear_recent)
        try:
            x = self._btn_recent.winfo_rootx()
            y = self._btn_recent.winfo_rooty() + self._btn_recent.winfo_height()
            menu.tk_popup(x, y)
        finally:
            menu.grab_release()

    def _build_options_section(self, parent):
        frm = ctk.CTkFrame(parent, fg_color=PALETTE["bg"], corner_radius=10)
        frm.pack(fill="x", padx=16, pady=(0, 10))
        frm.columnconfigure(0, weight=1)

        _ic_opt = _get_icon("opzioni", 20) if _ICONS_AVAILABLE else None
        ctk.CTkLabel(frm, text="  Opzioni Catalogazione",
                     image=_ic_opt, compound="left", font=FONT_HEAD
                     ).grid(row=0, column=0, padx=14, pady=(12, 6), sticky="w")

        # v1053: opzioni essenziali nel pannello sinistro
        # v1075: tooltip singleton unificato — delega al _add_tooltip globale
        # che usa self._global_tip condiviso (no ghost tooltips).
        def _bind_tooltip(widget, tip_text):
            self._add_tooltip(widget, tip_text)

        checks = [
            (self._opt_analyze, "Solo Analisi",
             "Analizza la collezione senza spostare file — modalità di sola lettura"),
            (self._opt_cleanup, "Rimuovi Cartelle Vuote",
             "Elimina le cartelle vuote dopo lo spostamento dei file"),
        ]
        for i, (var, label, tooltip) in enumerate(checks):
            cb = ctk.CTkCheckBox(frm, variable=var, text=f"  {label}", font=FONT_SMALL,
                                 text_color=PALETTE["text"], fg_color=PALETTE["primary"],
                                 hover_color=PALETTE["primary_hover"],
                                 checkmark_color=PALETTE["bg"])
            cb.grid(row=i + 1, column=0, padx=20, pady=3, sticky="w")
            _bind_tooltip(cb, tooltip)

        # v1079: rimosso il separatore orizzontale interno — tutte e 3 le
        # checkbox sono "Opzioni Catalogazione", non c'era un gruppo semantico
        # diverso da dividere. Ora tutte e 3 hanno lo stesso pady=3 e l'aspetto
        # è coeso senza buco visibile tra la 2ª e la 3ª voce.

        # Checkbox sorgenti DB con tooltip
        cb_db = ctk.CTkCheckBox(
            frm, variable=self._opt_use_ext_db,
            command=self._on_ext_db_toggle,
            text="  Abilita Sorgenti DB Online",
            font=FONT_SMALL, text_color=PALETTE["text"],
            fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
            checkmark_color=PALETTE["bg"],
        )
        cb_db.grid(row=3, column=0, padx=20, pady=3, sticky="w")
        _bind_tooltip(cb_db, "Interroga MusicBrainz, Deezer, iTunes per arricchire i metadati")

        ctk.CTkLabel(frm,
                     text="  Altre opzioni (dry-run, verbose, BPM...) → tab  ⚙️  Avanzate",
                     font=(FONT_SMALL[0], FONT_SMALL[1] - 1),
                     text_color=PALETTE["text_dim"]
                     ).grid(row=4, column=0, padx=20, pady=(6, 10), sticky="w")
        return frm


    def _build_cover_section_slim(self, parent):
        """v1053: solo checkbox 'Recupera cover mancanti' — resto nel tab Avanzate."""
        frm = ctk.CTkFrame(parent, fg_color=PALETTE["bg"], corner_radius=10)
        frm.pack(fill="x", padx=16, pady=(0, 10))
        frm.columnconfigure(0, weight=1)

        _ic_cov = _get_icon("cover_album", 20) if _ICONS_AVAILABLE else None
        ctk.CTkLabel(frm, text="  Cover Album",
                     image=_ic_cov, compound="left", font=FONT_HEAD
                     ).grid(row=0, column=0, padx=14, pady=(12, 6), sticky="w")

        ctk.CTkCheckBox(frm, variable=self._cover_enabled, font=FONT_SMALL,
                        text="Recupera cover mancanti automaticamente",
                        text_color=PALETTE["text"], fg_color=PALETTE["primary"],
                        hover_color=PALETTE["primary_hover"],
                        checkmark_color=PALETTE["bg"],
                        ).grid(row=1, column=0, padx=20, pady=(0, 4), sticky="w")

        ctk.CTkLabel(frm,
                     text="  Strategia, sorgenti, sovrascrittura → tab  ⚙️  Avanzate",
                     font=(FONT_SMALL[0], FONT_SMALL[1] - 1),
                     text_color=PALETTE["text_dim"]
                     ).grid(row=2, column=0, padx=20, pady=(0, 10), sticky="w")
        return frm


    def _build_duplicate_section(self, parent):
        frm = ctk.CTkFrame(parent, fg_color=PALETTE["bg"], corner_radius=10)
        frm.pack(fill="x", padx=16, pady=(0, 10))
        frm.columnconfigure(0, weight=1)

        _ic_dup = _get_icon("gestione_dup", 20) if _ICONS_AVAILABLE else None
        ctk.CTkLabel(frm, text="  Gestione Duplicati",
                     image=_ic_dup, compound="left", font=FONT_HEAD
                     ).grid(row=0, column=0, padx=14, pady=(12, 6), sticky="w")
        ctk.CTkLabel(frm, text="Quando un file esiste già nella cartella di destinazione:",
                     font=FONT_SMALL, text_color=PALETTE["text_dim"]
                     ).grid(row=1, column=0, padx=14, pady=(0, 4), sticky="w")

        options = [
            ('keep_both',  "Mantieni entrambi (rinomina il nuovo)"),
            ('skip',       "Salta (mantieni il file esistente)"),
            ('overwrite',  "Sovrascrivi (sostituisce l'esistente)"),
        ]
        for i, (val, label) in enumerate(options):
            ctk.CTkRadioButton(
                frm, text=label, variable=self._dup_action, value=val,
                font=FONT_SMALL, text_color=PALETTE["text"],
                fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
            ).grid(row=i + 2, column=0, padx=20, pady=3, sticky="w")

        ctk.CTkFrame(frm, height=8, fg_color="transparent").grid(row=5, column=0)
        return frm


    def _build_cover_section(self, parent):
        frm = ctk.CTkFrame(parent, fg_color=PALETTE["bg"], corner_radius=10)
        frm.pack(fill="x", padx=16, pady=(0, 10))
        frm.columnconfigure(0, weight=1)

        _ic_cov = _get_icon("cover_album", 20) if _ICONS_AVAILABLE else None
        ctk.CTkLabel(frm, text="  Cover Album",
                     image=_ic_cov, compound="left", font=FONT_HEAD
                     ).grid(row=0, column=0, padx=14, pady=(12, 6), sticky="w")

        ctk.CTkCheckBox(frm, variable=self._cover_enabled, font=FONT_SMALL,
                        text="Recupera cover mancanti automaticamente",
                        text_color=PALETTE["text"], fg_color=PALETTE["primary"],
                        hover_color=PALETTE["primary_hover"],
                        checkmark_color=PALETTE["bg"],
                        ).grid(row=1, column=0, padx=20, pady=(0, 6), sticky="w")

        ctk.CTkCheckBox(frm, variable=self._cover_overwrite, font=FONT_SMALL,
                        text="Sovrascrivi cover esistente",
                        text_color=PALETTE["text"], fg_color=PALETTE["primary"],
                        hover_color=PALETTE["primary_hover"],
                        checkmark_color=PALETTE["bg"],
                        ).grid(row=2, column=0, padx=20, pady=(0, 8), sticky="w")

        ctk.CTkLabel(frm, text="Strategia di scelta:", font=FONT_SMALL,
                     text_color=PALETTE["text_dim"]
                     ).grid(row=3, column=0, padx=20, pady=(0, 2), sticky="w")

        strats = [
            ('largest',         "Usa la più grande in risoluzione"),
            ('first_available', "Usa la prima disponibile"),
        ]
        for i, (val, label) in enumerate(strats):
            ctk.CTkRadioButton(
                frm, text=label, variable=self._cover_strategy, value=val,
                font=FONT_SMALL, text_color=PALETTE["text"],
                fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
            ).grid(row=4 + i, column=0, padx=30, pady=2, sticky="w")

        ctk.CTkLabel(frm, text="Sorgenti cover (Spotify escluso — richiede licenza):", font=FONT_SMALL,
                     text_color=PALETTE["text_dim"]
                     ).grid(row=6, column=0, padx=20, pady=(8, 2), sticky="w")

        cover_labels = {
            'musicbrainz': "MusicBrainz",
            'lastfm':      "Last.fm",
            'deezer':      "Deezer",    # v1049
            'itunes':      "iTunes",    # v1049
        }
        for i, (src, label) in enumerate(cover_labels.items()):
            ctk.CTkCheckBox(
                frm, variable=self._cover_sources[src], text=label,
                font=FONT_SMALL, text_color=PALETTE["text"],
                fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
                checkmark_color=PALETTE["bg"],
            ).grid(row=7 + i, column=0, padx=30, pady=2, sticky="w")

        ctk.CTkFrame(frm, height=8, fg_color="transparent").grid(row=12, column=0)

    def _build_sources_section(self, parent):
        """v1036/v1047/v1048: sorgenti database metadati — solo pubbliche nel pannello."""
        frm = ctk.CTkFrame(parent, fg_color=PALETTE["bg"], corner_radius=10)
        frm.pack(fill="x", padx=16, pady=(0, 10))
        frm.columnconfigure(0, weight=1)

        ctk.CTkLabel(frm, text="🌐  Sorgenti Metadati", font=FONT_HEAD
                     ).grid(row=0, column=0, padx=14, pady=(12, 2), sticky="w")

        ctk.CTkLabel(
            frm,
            text="⚠  Spotify disabilitato — richiede licenza API a pagamento",
            font=FONT_SMALL, text_color=PALETTE["warning"],
        ).grid(row=1, column=0, padx=14, pady=(0, 4), sticky="w")

        public_sources = {
            'musicbrainz': "MusicBrainz",
            'lastfm':      "Last.fm",
            'beatport':    "Beatport  (BPM)",
            'getsong':     "GetSong  (BPM)",
            'deezer':      "Deezer",
            'itunes':      "iTunes Search",
        }
        for i, (key, label) in enumerate(public_sources.items()):
            ctk.CTkCheckBox(
                frm, variable=self._meta_sources.get(key, ctk.BooleanVar(value=True)),
                text=label, font=FONT_SMALL, text_color=PALETTE["text"],
                fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
                checkmark_color=PALETTE["bg"],
            ).grid(row=i + 2, column=0, padx=20, pady=2, sticky="w")

        ctk.CTkLabel(frm,
                     text="  Discogs / AudD / AcoustID → tab  ⚙️  Avanzate  →",
                     font=(FONT_SMALL[0], FONT_SMALL[1] - 1), text_color=PALETTE["text_dim"]
                     ).grid(row=len(public_sources) + 2, column=0, padx=20, pady=(4, 8), sticky="w")

        ctk.CTkFrame(frm, height=4, fg_color="transparent").grid(
            row=len(public_sources) + 3, column=0)

    def _build_local_db_section(self, parent):
        """v1035: aggiornamento DB locale mappatura generi."""
        frm = ctk.CTkFrame(parent, fg_color=PALETTE["bg"], corner_radius=10)
        frm.pack(fill="x", padx=16, pady=(0, 10))
        frm.columnconfigure(0, weight=1)

        ctk.CTkLabel(frm, text="🗄️  Libreria Locale", font=FONT_HEAD
                     ).grid(row=0, column=0, padx=14, pady=(12, 4), sticky="w")

        ctk.CTkCheckBox(
            frm, variable=self._opt_local_db,
            text="Aggiorna DB locale Generi dopo catalogazione",
            font=FONT_SMALL, text_color=PALETTE["text"],
            fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
            checkmark_color=PALETTE["bg"],
        ).grid(row=1, column=0, padx=20, pady=(0, 4), sticky="w")

        ctk.CTkLabel(
            frm,
            text="Salva la mappatura file→genere in music_library.json.\n"
                 "Permette di rilevare spostamenti manuali al prossimo avvio.",
            font=FONT_SMALL, text_color=PALETTE["text_dim"], justify="left",
        ).grid(row=2, column=0, padx=28, pady=(0, 10), sticky="w")

        ctk.CTkFrame(frm, height=4, fg_color="transparent").grid(row=3, column=0)

    def _build_action_buttons(self, parent):
        frm = ctk.CTkFrame(parent, fg_color="transparent")
        frm.pack(fill="x", padx=16, pady=(0, 4))
        frm.columnconfigure(0, weight=1)
        frm.columnconfigure(1, weight=1)

        self._btn_run = ctk.CTkButton(
            frm, text="▶  Avvia", command=self._run,
            height=BTN_H, font=FONT_BODY,
            fg_color=PALETTE["success"], hover_color="#27ae60", text_color="#ffffff",
        )
        self._btn_run.grid(row=0, column=0, padx=(0, 4), pady=4, sticky="ew")

        self._btn_stop = ctk.CTkButton(
            frm, text="■  Ferma", command=self._stop,
            height=BTN_H, font=FONT_BODY,
            fg_color=PALETTE["error"], hover_color="#c0392b", text_color="#ffffff",
            state="disabled",
        )
        self._btn_stop.grid(row=0, column=1, padx=(4, 0), pady=4, sticky="ew")

        ctk.CTkButton(frm, text="🗑  Pulisci Log", command=self._clear_log,
                      height=BTN_H, font=FONT_BODY,
                      fg_color=PALETTE["surface2"], hover_color=PALETTE["border"],
                      ).grid(row=1, column=0, columnspan=2, pady=4, sticky="ew")

    def _build_right_panel(self):
        right = ctk.CTkFrame(self.root, fg_color=PALETTE["bg"], corner_radius=0)
        right.grid(row=0, column=1, sticky="nsew", padx=(1, 0))   # v1077: torna a row=0
        right.columnconfigure(0, weight=1)
        right.rowconfigure(1, weight=1)
        self._right_panel = right   # v1080: ref per hide-during-drag

        self._build_stat_cards(right)

        # v1048/v1049: TabView — tab più leggibili
        self._tabview = ctk.CTkTabview(
            right,
            fg_color=PALETTE["surface"],
            segmented_button_fg_color=PALETTE["surface2"],
            segmented_button_selected_color=PALETTE["primary"],
            segmented_button_selected_hover_color=PALETTE["primary_hover"],
            segmented_button_unselected_color=PALETTE["surface2"],
            segmented_button_unselected_hover_color=PALETTE["bg"],
            text_color=PALETTE["text"],
            corner_radius=10,
            anchor="n",
        )
        self._tabview.grid(row=1, column=0, padx=20, pady=(0, 8), sticky="nsew")
        # Font più grande per i tab
        self._tabview._segmented_button.configure(font=("Segoe UI", 13, "bold"))

        # v1081: placeholder v1080 rimosso — non serve più, resize gestito via minsize/maxsize

        # ── Tab 1: Log ───────────────────────────────────────────────────
        tab_log = self._tabview.add("  Log")
        tab_log.columnconfigure(0, weight=1)
        tab_log.rowconfigure(1, weight=1)

        # v1068: toolbar filtri livello log
        log_toolbar = ctk.CTkFrame(tab_log, fg_color="transparent")
        log_toolbar.grid(row=0, column=0, padx=4, pady=(4, 2), sticky="ew")

        ctk.CTkLabel(log_toolbar, text="Filtra:", font=FONT_SMALL,
                     text_color=PALETTE["text_dim"]).pack(side="left", padx=(4, 6))

        self._log_filter = {"INFO": True, "WARNING": True, "ERROR": True}
        self._log_filter_btns = {}

        # Pulsanti toggle colorati (attivo = colore livello, inattivo = grigio)
        _log_level_colors = {
            "INFO":    PALETTE.get("log_info",  "#7ec8e3"),
            "WARNING": PALETTE.get("warning",   "#e0a030"),
            "ERROR":   PALETTE.get("error",     "#cc4444"),
        }

        def _make_log_toggle(level, color):
            state = {"on": True}
            def _toggle(lv=level, clr=color, s=state):
                s["on"] = not s["on"]
                self._log_filter[lv] = s["on"]
                btn.configure(
                    fg_color=clr if s["on"] else PALETTE["surface2"],
                    text_color=PALETTE["bg"] if s["on"] else PALETTE["text_dim"],
                )
                self._log_apply_filter()
            btn = ctk.CTkButton(
                log_toolbar, text=level, width=72, height=24,
                fg_color=color,
                hover_color=color,
                text_color=PALETTE["bg"],
                font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
                command=_toggle,
                corner_radius=4,
            )
            btn.pack(side="left", padx=4)
            self._log_filter_btns[level] = btn

        for _lv, _cl in _log_level_colors.items():
            _make_log_toggle(_lv, _cl)

        self._log = LogViewer(tab_log)
        self._log.grid(row=1, column=0, padx=4, pady=(0, 4), sticky="nsew")
        self._log_all_lines: list = []  # buffer di tutte le righe per il filtro

        # ── Tab 2: DB Locale ─────────────────────────────────────────────
        tab_db = self._tabview.add("  DB Locale")
        tab_db.columnconfigure(0, weight=1)
        tab_db.rowconfigure(1, weight=1)
        self._build_db_tab(tab_db)

        # ── Tab 3: Generi ────────────────────────────────────────────────
        tab_genres = self._tabview.add("  Generi")
        tab_genres.columnconfigure(0, weight=1)
        tab_genres.rowconfigure(0, weight=1)
        self._build_genres_tab(tab_genres)

        # ── Tab 4: Cache Metadati ────────────────────────────────────────────
        tab_cache = self._tabview.add("  Cache")
        tab_cache.columnconfigure(0, weight=1)
        tab_cache.rowconfigure(0, weight=0)  # toolbar compatta
        tab_cache.rowconfigure(1, weight=1)  # contenuto
        self._build_cache_tab(tab_cache)

        # ── Tab 5: Qualità Bassa ──────────────────────────────────────────
        tab_quality = self._tabview.add("  Qualità")
        tab_quality.columnconfigure(0, weight=1)
        tab_quality.rowconfigure(1, weight=1)
        self._build_quality_tab(tab_quality)

        # ── Tab 6: Classificazione Caraibica ─────────────────────────────
        tab_carib = self._tabview.add("  Caraibica")
        tab_carib.columnconfigure(0, weight=1)
        tab_carib.rowconfigure(0, weight=1)
        self._build_caribbean_tab(tab_carib)

        # ── Tab 7: Impostazioni Avanzate ─────────────────────────────────
        tab_adv = self._tabview.add("  Avanzate")
        tab_adv.columnconfigure(0, weight=1)
        tab_adv.rowconfigure(0, weight=1)
        self._build_advanced_tab(tab_adv)

        # Applica icone ai tab (dopo che il widget è stato disegnato)
        self.root.after(80, self._apply_tab_icons)

        # ── Piano utente — mostra badge nella titlebar ────────────────────
        try:
            from config.user_plans import get_plan as _get_plan
            _plan = _get_plan()
            self.root.title(
                f"Music Cataloger Advanced  {APP_VERSION}  |  {_plan.display_name}"
            )
        except Exception:
            pass

        # Barra progresso (fuori dai tab, sempre visibile)
        prog_frm = ctk.CTkFrame(right, fg_color=PALETTE["surface"], corner_radius=10)
        prog_frm.grid(row=2, column=0, padx=20, pady=(0, 20), sticky="ew")
        prog_frm.columnconfigure(0, weight=1)
        self._progress = LabeledProgressBar(prog_frm)
        self._progress.grid(row=0, column=0, padx=16, pady=14, sticky="ew")

    # ─── TAB: DB LOCALE ──────────────────────────────────────────────────────

    def _build_db_tab(self, parent):
        """v1068: toolbar compatta su singola riga, header integrato."""
        parent.rowconfigure(1, weight=1)

        # ── Riga unica: Ricarica | Cerca | Contatore ─────────────────────────
        toolbar = ctk.CTkFrame(parent, fg_color=PALETTE["surface2"], corner_radius=6)
        toolbar.grid(row=0, column=0, padx=8, pady=(6, 2), sticky="ew")
        toolbar.columnconfigure(1, weight=1)

        _ic_rel = _get_icon("reload", 20) if _ICONS_AVAILABLE else None
        _btn_db_reload = ctk.CTkButton(
            toolbar, text="", width=36, height=28,
            fg_color="transparent", hover_color=PALETTE["primary"],
            font=FONT_SMALL, image=_ic_rel, command=self._db_reload,
        )
        _btn_db_reload.grid(row=0, column=0, padx=(4, 2), pady=3)
        self._add_tooltip(_btn_db_reload, "Aggiorna")

        self._db_search_var = ctk.StringVar()
        self._db_search_after = None
        def _db_search_debounced(*_):
            if self._db_search_after:
                self.root.after_cancel(self._db_search_after)
            self._db_search_after = self.root.after(600, self._db_filter)
        self._db_search_var.trace_add("write", _db_search_debounced)
        ctk.CTkEntry(
            toolbar, textvariable=self._db_search_var, height=28,
            placeholder_text="🔍  Cerca nome file, genere, sottogenere...",
            font=FONT_SMALL, fg_color=PALETTE["bg"],
            border_width=0,
        ).grid(row=0, column=1, sticky="ew", padx=(2, 6), pady=3)

        self._db_count_var = ctk.StringVar(value="0 record")
        ctk.CTkLabel(toolbar, textvariable=self._db_count_var,
                     font=FONT_SMALL, text_color=PALETTE["text_dim"]
                     ).grid(row=0, column=2, padx=(0, 8))

        # Header fisso (non scrolla)
        db_hdr = ctk.CTkFrame(parent, fg_color=PALETTE["surface2"], corner_radius=6, height=28)
        db_hdr.grid(row=1, column=0, padx=8, pady=(0, 1), sticky="ew")
        db_hdr.grid_propagate(False)
        db_hdr.columnconfigure(0, weight=1)
        for _hc, (_hl, _hw) in enumerate([
            ("File", 0), ("Genere", 120), ("Subgenere", 110), ("Catalogato", 130)
        ]):
            ctk.CTkLabel(db_hdr, text=_hl,
                         font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
                         text_color=PALETTE["text"], width=_hw or 0, anchor="w",
                         ).grid(row=0, column=_hc,
                                padx=(10 if _hc == 0 else 4, 4), pady=3, sticky="w")

        # Lista scrollabile
        self._db_list_frame = ctk.CTkScrollableFrame(
            parent, fg_color=PALETTE["bg"], corner_radius=6
        )
        self._db_list_frame.grid(row=2, column=0, padx=8, pady=(0, 8), sticky="nsew")
        self._db_list_frame.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=0)
        parent.rowconfigure(2, weight=1)

        self._db_rows: list = []        # cache righe visibili
        self._db_data: dict = {}        # cache dati DB
        self._db_reload()

    def _db_reload(self):
        """Ricarica music_library.json e aggiorna la vista."""
        from pathlib import Path as _P
        if hasattr(sys, '_MEIPASS'):
            script_dir = _get_data_dir()
        db_path = _get_data_dir() / "music_library.json"
        self._db_data = {}
        if db_path.exists():
            try:
                import json as _json
                with open(db_path, encoding="utf-8") as f:
                    raw = _json.load(f)
                self._db_data = raw.get("files", {})
            except Exception as e:
                self._db_count_var.set(f"Errore lettura: {e}")
        self._db_filter()

    def _db_filter(self, page: int = 0):
        """v1069: paginazione 100 record — evita freeze con 500+ righe."""
        PAGE_SIZE = 100
        q = self._db_search_var.get().lower().strip()

        for w in self._db_list_frame.winfo_children():
            w.destroy()
        self._db_rows.clear()

        items = [
            (path, info) for path, info in self._db_data.items()
            if not q or q in path.lower()
               or q in (info.get("genre") or "").lower()
               or q in (info.get("subgenre") or "").lower()
        ]
        total = len(items)
        pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(0, min(page, pages - 1))
        self._db_current_page = page
        self._db_items_cache = items

        start = page * PAGE_SIZE
        end   = min(start + PAGE_SIZE, total)
        page_items = items[start:end]

        # Contatore con info pagina
        if total > PAGE_SIZE:
            self._db_count_var.set(
                f"{total} record  •  pag. {page+1}/{pages}  ({start+1}-{end})"
            )
        else:
            self._db_count_var.set(f"{total} record")

        row_offset = 0  # header ora è fisso fuori dallo scrollframe

        for idx, (path, info) in enumerate(page_items):
            bg = PALETTE["surface2"] if idx % 2 == 0 else PALETTE["surface"]
            row = ctk.CTkFrame(self._db_list_frame, fg_color=bg, corner_radius=4)
            row.grid(row=idx + row_offset, column=0, padx=0, pady=1, sticky="ew")
            row.columnconfigure(0, weight=1)
            fname = Path(path).name
            short = fname if len(fname) <= 48 else "..." + fname[-45:]
            ctk.CTkLabel(row, text=short, font=FONT_SMALL,
                         text_color=PALETTE["text"], anchor="w"
                         ).grid(row=0, column=0, padx=10, pady=3, sticky="w")
            ctk.CTkLabel(row, text=info.get("genre", "—"),
                         font=FONT_SMALL, text_color=PALETTE["primary"],
                         width=120, anchor="w"
                         ).grid(row=0, column=1, padx=4, pady=3, sticky="w")
            ctk.CTkLabel(row, text=info.get("subgenre", "—") or "—",
                         font=FONT_SMALL, text_color=PALETTE["text_dim"],
                         width=110, anchor="w"
                         ).grid(row=0, column=2, padx=4, pady=3, sticky="w")
            cat_at = (info.get("cataloged_at") or "")[:10]
            ctk.CTkLabel(row, text=cat_at or "—",
                         font=FONT_SMALL, text_color=PALETTE["text_dim"],
                         width=110, anchor="w"
                         ).grid(row=0, column=3, padx=4, pady=3, sticky="w")
            self._db_rows.append(row)

        # Navigazione pagine (solo se necessario)
        if pages > 1:
            nav = ctk.CTkFrame(self._db_list_frame, fg_color="transparent")
            nav.grid(row=len(page_items) + row_offset, column=0, pady=6)
            if page > 0:
                ctk.CTkButton(nav, text="◀ Prec", width=80, font=FONT_SMALL,
                              fg_color=PALETTE["surface2"],
                              command=lambda: self._db_filter(page - 1)
                              ).pack(side="left", padx=4)
            ctk.CTkLabel(nav, text=f"{page+1} / {pages}",
                         font=FONT_SMALL, text_color=PALETTE["text_dim"]
                         ).pack(side="left", padx=8)
            if page < pages - 1:
                ctk.CTkButton(nav, text="Succ ▶", width=80, font=FONT_SMALL,
                              fg_color=PALETTE["surface2"],
                              command=lambda: self._db_filter(page + 1)
                              ).pack(side="left", padx=4)

    # ─── TAB: QUALITÀ BASSA ──────────────────────────────────────────────────

    # Soglie qualità (kbps)
    _QUALITY_THRESHOLDS = [
        (128, "🔴  Bassa qualità",    "#C05050"),   # < 128 kbps
        (192, "🟡  Qualità media",    "#C8922A"),   # 128-191 kbps
        (320, "🟢  Buona qualità",    "#3D8A58"),   # 192-319 kbps
        (999, "💎  Alta qualità",     "#3A6EA8"),   # 320+ kbps
    ]

    def _build_quality_tab(self, parent):
        """v1065: tab Qualità — toolbar compatta, Treeview con intestazioni integrate."""
        parent.rowconfigure(0, weight=0)  # toolbar
        parent.rowconfigure(1, weight=0)  # header fisso
        parent.rowconfigure(2, weight=1)  # lista scrollabile
        parent.columnconfigure(0, weight=1)

        # ── Toolbar unica riga ────────────────────────────────────────────────
        toolbar = ctk.CTkFrame(parent, fg_color="transparent")
        toolbar.grid(row=0, column=0, padx=8, pady=(8, 4), sticky="ew")
        # col 0: Analizza | col 1: Riscansiona | col 2: spacer elastico |
        # col 3: ⚡label  | col 4: "Soglia:" | col 5: segmented
        toolbar.columnconfigure(2, weight=1)

        self._quality_scan_btn = ctk.CTkButton(
            toolbar, text="🔍  Analizza", width=110,
            fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
            font=FONT_SMALL, command=self._quality_scan,
        )
        self._quality_scan_btn.grid(row=0, column=0, padx=(0, 6))

        # Riscansiona — a sinistra, vicino ad Analizza, nascosto finché non c'è cache
        _ic_reload2 = _get_icon("reload2", 20) if _ICONS_AVAILABLE else None
        self._quality_rescan_btn = ctk.CTkButton(
            toolbar, text="  Riscansiona", width=120,
            fg_color=PALETTE["surface2"], hover_color=PALETTE["primary"],
            font=FONT_SMALL, image=_ic_reload2, compound="left",
            command=self._quality_rescan,
        )
        self._quality_rescan_btn.grid(row=0, column=1, padx=(0, 0))
        self._quality_rescan_btn.grid_remove()

        # Col 2 = spacer elastico con contatore + ⚡ in una sola StringVar
        self._quality_count_var = ctk.StringVar(
            value="Clicca Analizza — usa DB locale o legge i file MP3"
        )
        ctk.CTkLabel(toolbar, textvariable=self._quality_count_var,
                     font=FONT_SMALL, text_color=PALETTE["text_dim"]
                     ).grid(row=0, column=2, padx=(10, 0), sticky="w")
        # ⚡ label separata ma visibile solo dopo cache load
        self._quality_cache_lbl = ctk.CTkLabel(
            toolbar, text="", font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
            text_color="#f0c040"
        )
        self._quality_cache_lbl.grid(row=0, column=2, padx=(10, 0), sticky="e")

        ctk.CTkLabel(toolbar, text="Soglia:", font=FONT_SMALL,
                     text_color=PALETTE["text_dim"]).grid(row=0, column=4, padx=(0, 6))
        self._quality_threshold_var = ctk.StringVar(value="320")
        self._quality_built = False
        ctk.CTkSegmentedButton(
            toolbar,
            values=["128", "192", "256", "320"],
            variable=self._quality_threshold_var,
            font=FONT_SMALL,
            fg_color=PALETTE["surface2"],
            selected_color=PALETTE["primary"],
            selected_hover_color=PALETTE["primary_hover"],
            unselected_color=PALETTE["surface2"],
            unselected_hover_color=PALETTE["surface"],
            text_color=PALETTE["text"],
            width=240,
            command=lambda _: self._quality_filter() if self._quality_built else None,
        ).grid(row=0, column=5, padx=(0, 8))

        # ── Header fisso (non scrolla) ───────────────────────────────────────
        # v1076: padx destro di 24px compensa la scrollbar del CTkScrollableFrame
        # sottostante; altrimenti l'header risulta più largo delle righe e le
        # colonne non si allineano. Ogni label è ora cliccabile per ordinare.
        q_hdr = ctk.CTkFrame(parent, fg_color=PALETTE["surface2"], corner_radius=6, height=30)
        q_hdr.grid(row=1, column=0, padx=(8, 24), pady=(0, 1), sticky="ew")
        q_hdr.grid_propagate(False)
        q_hdr.columnconfigure(0, weight=1)

        # v1076: stato del sorting (colonna e direzione)
        #   col_idx: 0=File, 1=kbps, 2=Qualità, 3=SampleRate, 4=RG, 5=Cartella
        if not hasattr(self, "_quality_sort_col"):
            self._quality_sort_col = None    # nessun sort iniziale
            self._quality_sort_dir = "asc"   # 'asc' o 'desc'

        self._quality_hdr_labels = []   # tenuti per poter aggiornare le frecce
        _hdr_defs = [
            ("File", 0),  ("kbps", 60),        ("Qualità", 140),
            ("Sample Rate", 90), ("RG", 30),   ("Cartella", 180),
        ]
        for _c, (_l, _w) in enumerate(_hdr_defs):
            lbl = ctk.CTkLabel(
                q_hdr, text=_l,
                font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
                text_color="#8899bb", width=_w or 0, anchor="w",
                cursor="hand2",
            )
            lbl.grid(row=0, column=_c, padx=(10 if _c == 0 else 4, 4),
                     pady=4, sticky="w")
            # Click sull'header → toggle sort direction / cambio colonna
            lbl.bind("<Button-1>",
                     lambda e, idx=_c: self._quality_sort_click(idx))
            self._quality_hdr_labels.append((lbl, _l))   # (widget, testo base)

        # Applica subito le frecce (nessuna all'avvio; ma prepara il metodo)
        self._quality_refresh_header_arrows()

        # ── Lista con scroll (CTkScrollableFrame — 2 menu, totale 14/32) ──────
        self._quality_list = ctk.CTkScrollableFrame(
            parent, fg_color="transparent", corner_radius=0
        )
        self._quality_list.grid(row=2, column=0, padx=8, pady=(0, 8), sticky="nsew")
        self._quality_list.columnconfigure(0, weight=1)

        self._quality_results: list = []
        self._quality_built = True

    def _quality_rescan(self):
        """v1065: forza nuova analisi ignorando la cache salvata."""
        import os
        qa_path = _get_data_dir() / "quality_analysis.json"
        try:
            if qa_path.exists():
                os.remove(qa_path)
        except Exception:
            pass
        try:
            self._quality_cache_lbl.configure(text="")
            self._quality_rescan_btn.grid_remove()
        except Exception:
            pass
        self._quality_scan(force=True)

    def _quality_scan(self, force: bool = False):
        """v1063: avvia analisi qualità — riusa data/quality_analysis.json se disponibile."""
        import threading, json as _json
        path = self._selected_path.get().strip()
        if not path or not Path(path).is_dir():
            messagebox.showwarning("Attenzione", "Seleziona prima una directory musicale valida.")
            return

        for w in self._quality_list.winfo_children():
            w.destroy()
        self._quality_results = []

        # v1063: prova a caricare analisi già salvata per la stessa directory
        if not force:
            qa_path = _get_data_dir() / "quality_analysis.json"
            if qa_path.exists():
                try:
                    qa = _json.loads(qa_path.read_text(encoding="utf-8"))
                    if qa.get("base_path") == path and qa.get("results"):
                        self._quality_results = [tuple(r) for r in qa["results"]]
                        total = qa.get("total", len(self._quality_results))
                        # Mostra ⚡ etichetta e pulsante Riscansiona
                        self._quality_rescan_btn.grid()
                        self._quality_count_var.set(
                            f"⚡  Risultati da cache — {total} file analizzati"
                        )
                        self._quality_cache_lbl.configure(text="")
                        self._quality_filter()
                        return  # nessuna nuova analisi necessaria
                except Exception:
                    pass

        # Finestra progresso — v1061: NO grab_set (deadlock con root.after del thread)
        # Usiamo topmost + pulsante disabilitato per evitare doppi click
        self._quality_scan_btn.configure(state="disabled")
        prog_win = ctk.CTkToplevel(self.root)
        prog_win.title("Analisi qualità in corso...")
        self._set_win_icon(prog_win)          # v1074: icona uniforme su tutte le finestre
        prog_win.resizable(False, False)
        prog_win.attributes("-topmost", True)
        self._center_win(prog_win, 400, 140)
        # v1061b: consenti chiusura manuale — termina l'analisi in anticipo
        def _force_close():
            try:
                self._quality_prog_bar.stop()
                prog_win.destroy()
                self._quality_scan_btn.configure(state="normal")
                self._quality_count_var.set("Analisi interrotta")
            except Exception:
                pass
        prog_win.protocol("WM_DELETE_WINDOW", _force_close)
        prog_win.lift()

        ctk.CTkLabel(prog_win, text="🔍  Analisi bitrate in corso...",
                     font=(FONT_SMALL[0], 13, "bold")).pack(pady=(20, 8))
        self._quality_prog_label = ctk.CTkLabel(prog_win,
            text="Lettura DB locale...", font=FONT_SMALL, text_color=PALETTE["text_dim"])
        self._quality_prog_label.pack()
        prog_bar = ctk.CTkProgressBar(prog_win, width=340, mode="indeterminate")
        prog_bar.pack(pady=(8, 16))
        prog_bar.start()

        self._quality_prog_win = prog_win
        self._quality_prog_bar = prog_bar
        self._quality_count_var.set("⏳  Analisi in corso...")

        threading.Thread(target=self._quality_scan_thread, args=(path,), daemon=True).start()

    def _quality_scan_thread(self, base_path: str):
        """v1062: thread sicuro — salva risultati in data/quality_analysis.json.
        Priorità: DB locale → mutagen fallback. Salva sempre il risultato per
        riutilizzarlo senza rileggere i file al prossimo avvio.
        """
        import json as _json
        base = Path(base_path)
        results = []

        # Fase 1: leggi dal DB locale (music_library.json)
        db_kbps: dict = {}
        db_path = _get_data_dir() / "music_library.json"
        if db_path.exists():
            try:
                raw = _json.loads(db_path.read_text(encoding="utf-8"))
                for rel_path, info in raw.get("files", {}).items():
                    kbps = info.get("quality_kbps") or 0
                    if kbps:
                        fname_key = Path(rel_path).name.lower()
                        db_kbps[fname_key] = (kbps, str(Path(rel_path).parent))
            except Exception:
                pass

        self.root.after(0, lambda: self._quality_prog_label.configure(
            text=f"DB locale: {len(db_kbps)} voci. Scansione file..."))

        # Fase 2: scansiona i file fisici, mutagen come fallback
        try:
            from mutagen.mp3 import MP3 as _MP3
            mutagen_ok = True
        except ImportError:
            mutagen_ok = False

        files = list(base.rglob("*.mp3"))
        total = len(files)
        mutagen_count = 0

        for i, fp in enumerate(files):
            fname = fp.name
            if fname.lower() in db_kbps:
                kbps, folder = db_kbps[fname.lower()]
                sr_khz, rg = "—", "—"  # non disponibili dal DB
            elif mutagen_ok:
                try:
                    audio = _MP3(str(fp))
                    kbps = int(audio.info.bitrate // 1000) if audio.info else 0
                    folder = str(fp.relative_to(base).parent)
                    sample_rate = getattr(audio.info, "sample_rate", 0) if audio.info else 0
                    # Rileva sample_rate, codec, replay_gain
                    sr_khz = f"{sample_rate/1000:.1f}kHz" if sample_rate else "—"
                    rg = "✓" if (audio.tags and (
                        audio.tags.get("TXXX:replaygain_track_gain") or
                        audio.tags.getall("RVA2") or
                        audio.tags.getall("RVAD")
                    )) else "✗"
                    mutagen_count += 1
                except Exception:
                    kbps, folder = 0, str(fp.relative_to(base).parent)
                    sr_khz, rg = "—", "—"
            else:
                kbps, folder = 0, str(fp.relative_to(base).parent)
                sr_khz, rg = "—", "—"
            if kbps > 0:
                results.append((fname, kbps, folder, sr_khz, rg))

            if i % 30 == 0:
                msg = f"File {i}/{total}  (mutagen: {mutagen_count})"
                self.root.after(0, lambda m=msg: self._quality_prog_label.configure(text=m))

        results.sort(key=lambda x: x[1])
        self._quality_results = results  # (fname, kbps, folder, sr_khz, rg)

        # v1062: salva analisi in data/quality_analysis.json per riuso futuro
        try:
            qa_path = _get_data_dir() / "quality_analysis.json"
            qa_data = {
                "base_path": str(base),
                "total": total,
                "results": results,  # lista di [fname, kbps, folder]
            }
            qa_path.write_text(
                _json.dumps(qa_data, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
        except Exception:
            pass

        db_note = f" — DB: {len(db_kbps)}, mutagen: {mutagen_count}"
        self.root.after(0, lambda: self._quality_done(total, db_note))

    def _quality_done(self, total: int, note: str):
        """v1063: chiude prog_win con update_idletasks prima di popolare la lista.
        Il flush tkinter garantisce che la finestra sia completamente distrutta
        prima che i CTkScrollableFrame vengano aggiornati — fix scrollbar decontestualizzate.
        """
        try:
            self._quality_prog_bar.stop()
            self._quality_prog_win.destroy()
            self.root.update_idletasks()  # flush — rilascia la finestra completamente
        except Exception:
            pass
        try:
            self._quality_scan_btn.configure(state="normal")
            self._quality_rescan_btn.grid()
            self._quality_cache_lbl.configure(text="")
        except Exception:
            pass
        self._quality_filter()
        current = self._quality_count_var.get()
        self._quality_count_var.set(current + note)

    # ─── v1076: Sorting tab Qualità per click sull'header ─────────────────────
    def _quality_sort_click(self, col_idx: int):
        """Click su un header di colonna: toggle direction o cambia colonna."""
        if self._quality_sort_col == col_idx:
            # Stesso header cliccato → inverti direzione
            self._quality_sort_dir = "desc" if self._quality_sort_dir == "asc" else "asc"
        else:
            # Nuova colonna → parti da ascendente
            self._quality_sort_col = col_idx
            self._quality_sort_dir = "asc"

        # Ordina self._quality_results in-place
        # Indici tupla: 0=fname, 1=kbps, 2=folder, 3=sr_khz, 4=rg
        # Mappa header → indice tupla: col 0→0 (File), 1→1 (kbps), 2→1 (Qualità: derivato
        # da kbps, ordino per kbps), 3→3 (SR), 4→4 (RG), 5→2 (Cartella)
        COL2TUP = {0: 0, 1: 1, 2: 1, 3: 3, 4: 4, 5: 2}
        tup_idx = COL2TUP.get(col_idx, 0)

        def _sort_key(row):
            # row è tupla (fname, kbps, folder, sr_khz, rg) — ma rg/sr possono mancare
            try:
                val = row[tup_idx] if len(row) > tup_idx else ""
            except Exception:
                val = ""
            # numerico per kbps e sample_rate, stringa per gli altri
            if tup_idx in (1, 3):
                try:
                    # estraggo la parte numerica (es. "44.1" o "320")
                    return float(str(val).replace("—", "0").split()[0])
                except Exception:
                    return 0.0
            # stringa lowercase per ordinamento case-insensitive
            return str(val).lower()

        try:
            self._quality_results.sort(
                key=_sort_key, reverse=(self._quality_sort_dir == "desc")
            )
        except Exception:
            pass

        # Aggiorna frecce sugli header e rirenderizza la lista
        self._quality_refresh_header_arrows()
        self._quality_filter()

    def _quality_refresh_header_arrows(self):
        """Aggiorna il testo delle label header aggiungendo ▲/▼ alla colonna attiva."""
        try:
            for idx, (lbl, base_text) in enumerate(self._quality_hdr_labels):
                if idx == self._quality_sort_col:
                    arrow = " ▲" if self._quality_sort_dir == "asc" else " ▼"
                    lbl.configure(text=base_text + arrow)
                else:
                    lbl.configure(text=base_text)
        except Exception:
            pass

    def _quality_filter(self, page: int = 0):
        """v1069b: widget CTkFrame paginati (100/pag) — nessun CTkScrollableFrame."""
        PAGE_SIZE = 100

        threshold = int(self._quality_threshold_var.get())
        filtered = [r for r in self._quality_results
                    if 0 < r[1] <= threshold]

        for w in self._quality_list.winfo_children():
            w.destroy()

        if not self._quality_results:
            return

        n_tot  = len(self._quality_results)
        n_filt = len(filtered)

        total  = n_filt
        pages  = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page   = max(0, min(page, pages - 1))
        start  = page * PAGE_SIZE
        end    = min(start + PAGE_SIZE, total)
        items  = filtered[start:end]

        # Contatore
        if total > PAGE_SIZE:
            self._quality_count_var.set(
                f"{total} file ≤ {threshold} kbps  (su {n_tot} totali)"
                f"  •  pag. {page+1}/{pages}  ({start+1}-{end})"
            )
        else:
            self._quality_count_var.set(
                f"{total} file ≤ {threshold} kbps  (su {n_tot} totali)"
            )

        if not filtered:
            ctk.CTkLabel(self._quality_list,
                         text="Nessun file sotto questa soglia.",
                         font=FONT_SMALL, text_color=PALETTE["text_dim"]
                         ).pack(pady=20)
            return

        BG   = PALETTE.get("bg",       "#1a1f2e")
        BG_E = PALETTE.get("surface2", "#1e2433")
        BG_O = PALETTE.get("surface",  "#252b3a")
        FG   = "#d0d4e4"

        # Header non più qui — è fisso sopra il CTkScrollableFrame (vedi _build_quality_tab)

        # Righe come CTkFrame (stesso stile DB Locale — collaudato)
        def _sem(kbps):
            if kbps < 160: return "🔴 Scarsa", PALETTE.get("error",   "#cc4444")
            if kbps < 256: return "🟡 Media",  PALETTE.get("warning", "#e0a030")
            if kbps < 320: return "🟢 Buona",  "#50aa70"
            return             "💎 Alta",   "#4db8ff"

        for idx, item in enumerate(items):
            fname, kbps, folder = item[0], item[1], item[2]
            sem_txt, sem_color = _sem(kbps)
            bg = BG_E if idx % 2 == 0 else BG_O
            row = ctk.CTkFrame(self._quality_list, fg_color=bg,
                               corner_radius=0, height=28)
            row.pack(fill="x", pady=0)
            row.pack_propagate(False)
            row.columnconfigure(0, weight=1)

            short     = fname  if len(fname)  <= 55 else "..." + fname[-52:]
            short_dir = folder if len(folder) <= 22 else "..." + folder[-19:]

            ctk.CTkLabel(row, text=short, font=FONT_SMALL,
                         text_color=FG, anchor="w"
                         ).grid(row=0, column=0, padx=10, pady=2, sticky="w")
            ctk.CTkLabel(row, text=str(kbps), font=FONT_SMALL,
                         text_color=FG, width=60, anchor="w"
                         ).grid(row=0, column=1, padx=4, pady=2, sticky="w")
            ctk.CTkLabel(row, text=sem_txt, font=FONT_SMALL,
                         text_color=sem_color, width=140, anchor="w"
                         ).grid(row=0, column=2, padx=4, pady=2, sticky="w")
            _sr  = item[3] if len(item) > 3 else "—"
            _rg  = item[4] if len(item) > 4 else "—"
            ctk.CTkLabel(row, text=_sr, font=FONT_SMALL,
                         text_color=FG, width=90, anchor="w"
                         ).grid(row=0, column=3, padx=4, pady=2, sticky="w")
            ctk.CTkLabel(row, text=_rg, font=FONT_SMALL,
                         text_color="#50aa70" if _rg=="✓" else PALETTE.get("text_dim","#666"),
                         width=30, anchor="w"
                         ).grid(row=0, column=4, padx=4, pady=2, sticky="w")
            ctk.CTkLabel(row, text=short_dir, font=FONT_SMALL,
                         text_color=FG, width=180, anchor="w"
                         ).grid(row=0, column=5, padx=4, pady=2, sticky="w")

        # Navigazione pagine
        if pages > 1:
            nav = ctk.CTkFrame(self._quality_list, fg_color="transparent")
            nav.pack(pady=8)
            if page > 0:
                ctk.CTkButton(nav, text="◀ Prec", width=80, font=FONT_SMALL,
                              fg_color=PALETTE["surface2"],
                              command=lambda: self._quality_filter(page - 1)
                              ).pack(side="left", padx=4)
            ctk.CTkLabel(nav, text=f"{page+1} / {pages}",
                         font=FONT_SMALL, text_color=PALETTE["text_dim"]
                         ).pack(side="left", padx=8)
            if page < pages - 1:
                ctk.CTkButton(nav, text="Succ ▶", width=80, font=FONT_SMALL,
                              fg_color=PALETTE["surface2"],
                              command=lambda: self._quality_filter(page + 1)
                              ).pack(side="left", padx=4)

    # ─── TAB: CACHE METADATI ─────────────────────────────────────────────────

    def _build_cache_tab(self, parent):
        """v1069b: tab Cache — layout a 3 righe: toolbar / header+lista / dettaglio."""
        import json as _json
        import tkinter as _tk

        self._cache_data: dict = {}
        self._cache_selected_key: str = ""

        # Layout: col0=lista(3), col1=dettaglio(2), row0=toolbar, row1=contenuto
        parent.columnconfigure(0, weight=3)
        parent.columnconfigure(1, weight=2)
        parent.rowconfigure(0, weight=0)
        parent.rowconfigure(1, weight=1)

        # ── Toolbar ──────────────────────────────────────────────────────
        toolbar = ctk.CTkFrame(parent, fg_color=PALETTE["surface2"], corner_radius=6)
        toolbar.grid(row=0, column=0, columnspan=2, padx=8, pady=(6, 2), sticky="ew")
        toolbar.columnconfigure(1, weight=1)

        _ic_rel2 = _get_icon("reload", 20) if _ICONS_AVAILABLE else None
        _btn_cache_reload = ctk.CTkButton(toolbar, text="", width=36, height=28,
                      fg_color="transparent", hover_color=PALETTE["primary"],
                      font=FONT_SMALL, image=_ic_rel2, command=self._cache_reload)
        _btn_cache_reload.grid(row=0, column=0, padx=(4, 2), pady=3)
        self._add_tooltip(_btn_cache_reload, "Aggiorna")

        self._cache_search_var = ctk.StringVar()
        self._cache_search_after = None
        def _cache_search_debounced(*_):
            if not getattr(self, "_cache_built", False):
                return
            if self._cache_search_after:
                self.root.after_cancel(self._cache_search_after)
            self._cache_search_after = self.root.after(600, self._cache_filter)
        self._cache_search_var.trace_add("write", _cache_search_debounced)
        ctk.CTkEntry(toolbar, textvariable=self._cache_search_var, height=28,
                     placeholder_text="🔍  Cerca artista, titolo...",
                     font=FONT_SMALL, fg_color=PALETTE["bg"], border_width=0,
                     ).grid(row=0, column=1, sticky="ew", padx=(2, 6), pady=3)

        self._cache_count_var = ctk.StringVar(value="0 voci")
        ctk.CTkLabel(toolbar, textvariable=self._cache_count_var,
                     font=FONT_SMALL, text_color=PALETTE["text_dim"]
                     ).grid(row=0, column=2, padx=(0, 6))

        ctk.CTkButton(toolbar, text="🗑 Svuota", width=90, height=28,
                      fg_color=PALETTE["accent"], hover_color="#802020",
                      font=FONT_SMALL, command=self._clear_cache,
                      ).grid(row=0, column=3, padx=(0, 4), pady=3)

        # ── Frame sinistra: header + lista ───────────────────────────────
        left = ctk.CTkFrame(parent, fg_color="transparent")
        left.grid(row=1, column=0, padx=(8, 4), pady=(2, 8), sticky="nsew")
        left.columnconfigure(0, weight=1)
        left.rowconfigure(1, weight=1)

        # Header fisso
        hdr = ctk.CTkFrame(left, fg_color=PALETTE["surface2"], corner_radius=6)
        hdr.grid(row=0, column=0, sticky="ew", pady=(0, 1))
        hdr.columnconfigure(0, weight=1)
        for col, (lbl, w) in enumerate([
            ("Artista / Titolo", 0), ("Genere", 100), ("Sorgente", 90)
        ]):
            ctk.CTkLabel(hdr, text=lbl,
                         font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
                         text_color=PALETTE["text"], width=w or 0, anchor="w",
                         ).grid(row=0, column=col,
                                padx=(10 if col == 0 else 4, 4), pady=4, sticky="w")

        # CTkScrollableFrame — scrollbar CTk nativa (16 menu totali, sotto 32)
        self._cache_list = ctk.CTkScrollableFrame(left, fg_color=PALETTE["bg"], corner_radius=6)
        self._cache_list.grid(row=1, column=0, sticky="nsew")
        self._cache_list.columnconfigure(0, weight=1)

        # ── Pannello dettaglio / cover ────────────────────────────────────
        detail = ctk.CTkFrame(parent, fg_color=PALETTE["surface2"], corner_radius=8)
        detail.grid(row=1, column=1, padx=(4, 8), pady=(2, 8), sticky="nsew")
        detail.columnconfigure(0, weight=1)

        cover_container = ctk.CTkFrame(
            detail, width=200, height=200,
            fg_color=PALETTE["bg"], corner_radius=8
        )
        cover_container.pack(padx=16, pady=(16, 8))
        cover_container.pack_propagate(False)
        self._cache_cover_label = ctk.CTkLabel(
            cover_container, text="🎵", font=("Segoe UI", 48),
            text_color=PALETTE["text_dim"], width=200, height=200,
            fg_color="transparent", corner_radius=8,
        )
        self._cache_cover_label.place(x=0, y=0, relwidth=1, relheight=1)

        self._cache_detail_var = ctk.StringVar(value="Seleziona un brano dalla lista")
        ctk.CTkLabel(detail, textvariable=self._cache_detail_var,
                     font=FONT_SMALL, text_color=PALETTE["text"],
                     wraplength=200, justify="left",
                     ).pack(padx=16, pady=(0, 8), anchor="w")

        self._cache_built = False
        self._cache_reload()

    def _cache_reload(self):
        """v1060b: Ricarica metadata_cache.json — robusto a dati malformati o di versioni precedenti."""
        import json as _json
        self._cache_data = {}
        cache_file = _get_data_dir() / "metadata_cache.json"
        if cache_file.exists():
            try:
                raw = _json.loads(cache_file.read_text(encoding="utf-8"))
                raw_cache = raw.get("metadata_cache", {})
                # Filtra voci malformate senza cancellarle dal disco:
                # accetta solo voci dove chiave è str e valore è dict o None
                self._cache_data = {
                    k: v for k, v in raw_cache.items()
                    if isinstance(k, str) and (v is None or isinstance(v, dict))
                }
                n_skip = len(raw_cache) - len(self._cache_data)
                if n_skip:
                    self._cache_count_var.set(f"({n_skip} voci ignorate — formato non valido)")
            except Exception as e:
                self._cache_count_var.set(f"Errore lettura cache: {e}")
        self._cache_search_var.set("")
        self._cache_filter()
        if hasattr(self, '_cache_info_var'):
            self._refresh_cache_info()

    def _cache_filter(self, page: int = 0):
        """v1069c: paginazione 100 record per la lista cache."""
        PAGE_SIZE = 100
        q = self._cache_search_var.get().lower().strip()
        for w in self._cache_list.winfo_children():
            w.destroy()

        # v1071b: merge intelligente — per ogni (artist,title) mostra UNA voce
        # arricchita con i dati migliori da tutte le sorgenti disponibili
        # (cover_url da Deezer, metadati da MusicBrainz, ecc.)
        merged: dict = {}  # key: (art_lower, ttl_lower) → merged_val
        for key, val in self._cache_data.items():
            if not val:
                continue
            art = (val.get("artist") or "").lower().strip()
            ttl = (val.get("title") or "").lower().strip()
            if not art and not ttl:
                continue
            mkey = f"{art}||{ttl}"
            if mkey not in merged:
                # Prima voce: copia come base
                merged[mkey] = dict(val)
                merged[mkey]["_display_key"] = key
            else:
                # Merge: arricchisci i campi vuoti con dati da questa sorgente
                base = merged[mkey]
                for field in ["cover_url", "album", "year", "duration", "bpm", "genre"]:
                    if not base.get(field) and val.get(field):
                        base[field] = val[field]
                # Preferisci cover_url da Deezer/iTunes (migliore qualità)
                if val.get("cover_url") and val.get("source") in ("Deezer", "iTunes"):
                    base["cover_url"] = val["cover_url"]
                # Mostra le sorgenti multiple
                existing_src = base.get("source", "")
                new_src = val.get("source", "")
                if new_src and new_src not in existing_src:
                    base["source"] = f"{existing_src}+{new_src}" if existing_src else new_src

        items = [
            (v["_display_key"], v) for v in merged.values()
            if not q
               or q in (v.get("artist") or "").lower()
               or q in (v.get("title") or "").lower()
               or q in (v.get("genre") or "").lower()
               or q in (v.get("album") or "").lower()
        ]
        items.sort(key=lambda x: (
            (x[1].get("artist") or "").lower(),
            (x[1].get("title") or "").lower()
        ))
        total = len(items)
        pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page  = max(0, min(page, pages - 1))
        start = page * PAGE_SIZE
        end   = min(start + PAGE_SIZE, total)

        if total > PAGE_SIZE:
            self._cache_count_var.set(
                f"{total} voci  •  pag. {page+1}/{pages}  ({start+1}-{end})"
            )
        else:
            self._cache_count_var.set(f"{total} voci")

        for idx, (key, meta) in enumerate(items[start:end]):
            bg = PALETTE["surface2"] if idx % 2 == 0 else PALETTE["surface"]
            row = ctk.CTkFrame(self._cache_list, fg_color=bg, corner_radius=4,
                               cursor="hand2")
            row.grid(row=idx, column=0, padx=0, pady=1, sticky="ew")
            row.columnconfigure(0, weight=1)

            artist = meta.get("artist") or ""
            title  = meta.get("title") or (key.split("_", 2)[-1] if "_" in key else key)
            genre  = meta.get("genre") or "—"
            source = meta.get("source") or "—"
            display = f"{artist} — {title}" if artist else title
            short = display if len(display) <= 42 else "..." + display[-39:]

            ctk.CTkLabel(row, text=short, font=FONT_SMALL,
                         text_color=PALETTE["text"], anchor="w"
                         ).grid(row=0, column=0, padx=10, pady=4, sticky="w")
            ctk.CTkLabel(row, text=genre, font=FONT_SMALL,
                         text_color=PALETTE["primary"], width=100, anchor="w"
                         ).grid(row=0, column=1, padx=4, pady=4, sticky="w")
            ctk.CTkLabel(row, text=source, font=FONT_SMALL,
                         text_color=PALETTE["text_dim"], width=90, anchor="w"
                         ).grid(row=0, column=2, padx=4, pady=4, sticky="w")

            row.bind("<Button-1>", lambda e, k=key, m=meta: self._cache_select(k, m))
            for child in row.winfo_children():
                child.bind("<Button-1>", lambda e, k=key, m=meta: self._cache_select(k, m))

        if pages > 1:
            nav = ctk.CTkFrame(self._cache_list, fg_color="transparent")
            nav.grid(row=end - start, column=0, pady=6)
            if page > 0:
                ctk.CTkButton(nav, text="◀", width=50, font=FONT_SMALL,
                              fg_color=PALETTE["surface2"],
                              command=lambda: self._cache_filter(page - 1)
                              ).pack(side="left", padx=2)
            ctk.CTkLabel(nav, text=f"{page+1}/{pages}",
                         font=FONT_SMALL, text_color=PALETTE["text_dim"]
                         ).pack(side="left", padx=6)
            if page < pages - 1:
                ctk.CTkButton(nav, text="▶", width=50, font=FONT_SMALL,
                              fg_color=PALETTE["surface2"],
                              command=lambda: self._cache_filter(page + 1)
                              ).pack(side="left", padx=2)
        self._cache_built = True

    def _cache_select(self, key: str, meta: dict):
        """Mostra il dettaglio e la cover del record selezionato."""
        artist = meta.get("artist") or "—"
        title  = meta.get("title") or key
        album  = meta.get("album") or "—"
        genre  = meta.get("genre") or "—"
        year   = meta.get("year") or "—"
        source = meta.get("source") or "—"
        bpm    = meta.get("bpm") or "—"

        # Durata formattata
        dur = meta.get("duration")
        if dur:
            try:
                d = int(float(dur))
                dur_str = f"{d//60}:{d%60:02d}"
            except Exception:
                dur_str = str(dur)
        else:
            dur_str = "—"

        self._cache_detail_var.set(
            f"{artist}\n{title}\n\n"
            f"Album: {album}\n"
            f"Genere: {genre}\n"
            f"Anno: {year}\n"
            f"Durata: {dur_str}\n"
            f"BPM: {bpm}\n"
            f"Sorgente: {source}"
        )

        # Carica cover se disponibile
        cover_url = meta.get("cover_url")
        if cover_url:
            self._cache_load_cover(cover_url)
        else:
            # v1061: pulisce anche il widget tkinter nativo che _cache_load_cover
            # aggiorna direttamente (PhotoImage su ._label) — senza reset esplicito
            # la cover precedente rimaneva visibile in background
            self._cover_token = getattr(self, "_cover_token", 0) + 1  # invalida download attivi
            self._cover_image_ref = None
            try:
                lbl = self._cache_cover_label._label
                lbl.configure(image="", text="🎵")
                lbl.image = None
            except Exception:
                self._cache_cover_label.configure(text="🎵", image=None)

    def _cache_load_cover(self, url: str):
        """v1061c: fix definitivo TclError cover.
        Usa tkinter.PhotoImage direttamente invece di CTkImage.
        CTkImage riutilizza sempre "pyimage1" come nome interno → conflitto quando
        distrutto e ricreato. PhotoImage genera nomi univoci automaticamente.
        Il label interno del CTkLabel viene aggiornato tramite l'attributo .image
        del widget tkinter nativo, che accetta PhotoImage direttamente.
        """
        import threading as _th
        from PIL import Image, ImageTk
        import io as _io

        # Token incrementale: invalida richieste precedenti
        token = getattr(self, "_cover_token", 0) + 1
        self._cover_token = token

        # Azzera subito il label (nel main thread — siamo già nel main thread qui)
        self._cache_cover_label.configure(text="⏳", image=None)
        self._cover_image_ref = None  # rilascia vecchio PhotoImage

        def _load():
            try:
                import requests as _req
                r = _req.get(url, timeout=6)
                if r.status_code == 200 and r.content:
                    pil_img = Image.open(_io.BytesIO(r.content)).convert("RGBA")
                    pil_img = pil_img.resize((200, 200), Image.LANCZOS)

                    def _apply(img=pil_img, tok=token):
                        if self._cover_token != tok:
                            return  # richiesta superata, ignora
                        # PhotoImage crea nome univoco automaticamente (pyimage2, pyimage3...)
                        photo = ImageTk.PhotoImage(img)
                        self._cover_image_ref = photo  # DEVE rimanere in memoria
                        # Aggiorna il widget tkinter nativo interno del CTkLabel
                        lbl = self._cache_cover_label._label
                        lbl.configure(image=photo, text="")
                        lbl.image = photo  # doppio riferimento per sicurezza

                    self.root.after(0, _apply)
                    return
            except Exception:
                pass

            def _fail(tok=token):
                if self._cover_token == tok:
                    self._cache_cover_label.configure(text="🎵", image=None)
            self.root.after(0, _fail)

        _th.Thread(target=_load, daemon=True).start()

    # ─── TAB: GENERI PREFERITI ───────────────────────────────────────────────

    # Struttura generi: macrogenere → lista subgeneri con descrizione
    _GENRE_TREE = {
        "🎵  Latin": {
            "subgenres": [
                ("Salsa",             "Salsa classica, dura, romantica"),
                ("Boogaloo",          "Latin boogaloo anni '60"),
                ("Pachanga",          "Pachanga cubana"),
                ("Cha Cha Cha",       "Cha cha cha cubano"),
                ("Bachata",           "Bachata Dominicana classica"),
                ("Reggaeton",         "Reggaeton/Urbano latino"),
                ("Cumbia",            "Cumbia colombiana/argentina"),
                ("Merengue",          "Merengue dominicano"),
                ("Timba",             "Timba cubana moderna"),
                ("Latin Jazz",        "Jazz latinoamericano"),
                ("Soca",              "Soca caraibica"),
                ("Dancehall",         "Dancehall giamaicano"),
            ]
        },
        "🎬  Soundtrack": {
            "subgenres": [
                ("Soundtrack",        "Colonne sonore film"),
                ("Anime",             "Sigle e OST anime giapponesi"),
                ("TV Soundtrack",     "Colonne sonore serie TV"),
                ("Video Game",        "Musica per videogiochi"),
                ("Trailer Music",     "Musica per trailer cinematografici"),
                ("Epic Orchestral",   "Musica epica/orchestrale"),
            ]
        },
        "🎸  Rock & Alternative": {
            "subgenres": [
                ("Rock",              "Rock generico"),
                ("Alternative",       "Alternative/Indie rock"),
                ("Indie",             "Indie rock/pop"),
                ("Metal",             "Heavy/Power metal"),
                ("Death Metal",       "Death/Black metal"),
                ("Punk",              "Punk e post-punk"),
                ("Grunge",            "Grunge anni '90"),
                ("Hard Rock",         "Hard rock classico"),
                ("Progressive Rock",  "Rock progressivo"),
            ]
        },
        "🎹  Classical & Jazz": {
            "subgenres": [
                ("Classical",         "Musica classica orchestrale"),
                ("Contemporary Classical", "Classica contemporanea"),
                ("Opera",             "Opera lirica"),
                ("Piano",             "Musica per pianoforte solo"),
                ("Baroque",           "Musica barocca"),
                ("Jazz",              "Jazz swing e bebop"),
                ("Smooth Jazz",       "Jazz moderno/lounge"),
                ("Blues",             "Blues e R&B classico"),
                ("Soul",              "Soul e gospel"),
            ]
        },
        "🎧  Electronic": {
            "subgenres": [
                ("Electronic",        "Elettronica generica"),
                ("House",             "House/Deep house"),
                ("Techno",            "Techno/Industrial"),
                ("Trance",            "Trance/Progressive trance"),
                ("Ambient",           "Ambient/Chillout/New Age"),
                ("Drum and Bass",     "DnB/Jungle"),
                ("Dubstep",           "Dubstep/Bass music"),
                ("EDM",               "Electronic Dance Music"),
                ("Synthwave",         "Retrowave/Synthwave"),
                ("Tropical House",    "Tropical House"),
            ]
        },
        "🎤  Pop & R&B": {
            "subgenres": [
                ("Pop",               "Pop commerciale"),
                ("Dance Pop",         "Pop da discoteca"),
                ("R&B",               "R&B/Soul moderno"),
                ("Hip Hop",           "Hip Hop/Rap"),
                ("Trap",              "Trap/Drill"),
                ("Vocal",             "Vocal/A cappella"),
                ("K-Pop",             "Korean Pop"),
                ("J-Pop",             "Japanese Pop"),
                ("Country",           "Country americano"),
                ("Country Pop",       "Country pop moderno"),
                ("Funk",              "Funk/Soul"),
                ("Gospel",            "Gospel/R&B Gospel"),
            ]
        },
        "🌍  World & Other": {
            "subgenres": [
                ("World",             "World music generica"),
                ("Flamenco",          "Flamenco/Musica spagnola"),
                ("Reggae",            "Reggae/Dancehall"),
                ("Folk",              "Folk/Tradizionale"),
                ("African",           "Musica africana"),
                ("Brazilian",         "MPB/Samba/Bossa Nova"),
                ("Celtic",            "Musica celtica/irlandese"),
                ("Middle Eastern",    "Musica mediorientale"),
                ("Afrobeats",         "Afrobeats/Afropop"),
                ("Bossa Nova",        "Bossa Nova brasiliana"),
            ]
        },
        "🗂️  Altro": {
            "subgenres": [
                ("Instrumental",      "Strumentale senza voce"),
                ("Spoken Word",       "Spoken word/Audiolibri"),
                ("Comedy",            "Musica comica/parodia"),
                ("Children",          "Musica per bambini"),
                ("Holiday",           "Musica natalizia/festività"),
            ]
        },
    }

    def _build_genres_tab(self, parent):
        """v1049: tab Generi Preferiti — l'utente personalizza macro e subgeneri attivi."""
        import json as _json

        # Carica preferenze salvate
        prefs_file = _get_data_dir() / "genre_prefs.json"
        self._genre_prefs: dict = {}
        if prefs_file.exists():
            try:
                self._genre_prefs = _json.loads(prefs_file.read_text(encoding="utf-8"))
            except Exception:
                pass

        # Toolbar
        toolbar = ctk.CTkFrame(parent, fg_color="transparent")
        toolbar.pack(fill="x", padx=8, pady=(8, 4))
        ctk.CTkLabel(toolbar,
                     text="Seleziona i generi attivi nella tua collezione.",
                     font=FONT_SMALL, text_color=PALETTE["text_dim"]
                     ).pack(side="left", padx=(4, 12))
        ctk.CTkButton(toolbar, text="💾  Salva", width=90,
                      fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
                      font=FONT_SMALL, command=self._save_genre_prefs,
                      ).pack(side="right", padx=(4, 0))
        ctk.CTkLabel(toolbar,
                     text="💡 Deseleziona i generi da escludere dalla catalogazione, poi clicca Salva",
                     font=(FONT_SMALL[0], FONT_SMALL[1]-1), text_color=PALETTE["text_dim"]
                     ).pack(side="left", padx=(8, 0))
        ctk.CTkButton(toolbar, text="✓ Tutto", width=80,
                      fg_color=PALETTE["surface2"], hover_color=PALETTE["primary"],
                      font=FONT_SMALL, command=lambda: self._set_all_genres(True),
                      ).pack(side="right", padx=4)
        ctk.CTkButton(toolbar, text="✗ Nessuno", width=90,
                      fg_color=PALETTE["surface2"], hover_color=PALETTE["accent"],
                      font=FONT_SMALL, command=lambda: self._set_all_genres(False),
                      ).pack(side="right", padx=4)

        # Body scrollabile
        scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent")
        scroll.pack(fill="both", expand=True, padx=4, pady=(0, 4))
        scroll.columnconfigure(0, weight=1)

        self._genre_vars: dict = {}   # key: (macro, sub) → BooleanVar

        for macro, data in self._GENRE_TREE.items():
            # Header macrogenere con checkbox "abilita tutto il gruppo"
            macro_key = macro.split("  ", 1)[-1].strip()
            all_on = all(
                self._genre_prefs.get(f"{macro_key}::{sub}", True)
                for sub, _ in data["subgenres"]
            )
            macro_var = ctk.BooleanVar(value=all_on)

            hdr = ctk.CTkFrame(scroll, fg_color=PALETTE["surface2"], corner_radius=8)
            hdr.pack(fill="x", padx=4, pady=(8, 2))
            hdr.columnconfigure(1, weight=1)

            ctk.CTkCheckBox(
                hdr, variable=macro_var, text=macro,
                font=("Segoe UI", 12, "bold"), text_color=PALETTE["text"],
                fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
                checkmark_color=PALETTE["bg"],
                command=lambda mv=macro_var, mk=macro_key, d=data:
                    self._toggle_macro_genre(mv, mk, d),
            ).pack(side="left", padx=12, pady=8)

            # Subgeneri in griglia 2 colonne
            sub_frm = ctk.CTkFrame(scroll, fg_color=PALETTE["bg"], corner_radius=6)
            sub_frm.pack(fill="x", padx=4, pady=(0, 4))
            sub_frm.columnconfigure((0, 1), weight=1)

            for idx, (sub, desc) in enumerate(data["subgenres"]):
                pref_key = f"{macro_key}::{sub}"
                is_active = self._genre_prefs.get(pref_key, True)
                var = ctk.BooleanVar(value=is_active)
                self._genre_vars[(macro_key, sub)] = var

                col = idx % 2
                row = idx // 2
                cell = ctk.CTkFrame(sub_frm, fg_color="transparent")
                cell.grid(row=row, column=col, padx=8, pady=3, sticky="w")

                ctk.CTkCheckBox(
                    cell, variable=var, text=sub,
                    font=FONT_SMALL, text_color=PALETTE["text"],
                    fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
                    checkmark_color=PALETTE["bg"],
                ).pack(side="left")
                ctk.CTkLabel(
                    cell, text=f"  {desc}",
                    font=(FONT_SMALL[0], FONT_SMALL[1] - 1),
                    text_color=PALETTE["text_dim"],
                ).pack(side="left")

            ctk.CTkFrame(scroll, height=2, fg_color="transparent").pack()

        self._genre_prefs_file = prefs_file

    def _toggle_macro_genre(self, macro_var: ctk.BooleanVar, macro_key: str, data: dict):
        """Imposta tutti i subgeneri del gruppo al valore del checkbox macro."""
        val = macro_var.get()
        for sub, _ in data["subgenres"]:
            v = self._genre_vars.get((macro_key, sub))
            if v is not None:
                v.set(val)

    def _set_all_genres(self, value: bool):
        """Seleziona o deseleziona tutti i generi."""
        for v in self._genre_vars.values():
            v.set(value)

    def _save_genre_prefs(self):
        """Salva le preferenze generi in data/genre_prefs.json."""
        import json as _json
        prefs = {
            f"{mk}::{sub}": var.get()
            for (mk, sub), var in self._genre_vars.items()
        }
        try:
            self._genre_prefs_file.write_text(
                _json.dumps(prefs, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            # Feedback visivo breve
            self._status_var.set("✓  Preferenze generi salvate")
            self.root.after(2500, lambda: self._status_var.set("✓  Pronto"))
        except Exception as e:
            messagebox.showerror("Errore", f"Impossibile salvare le preferenze:\n{e}")

    # ─── TAB: IMPOSTAZIONI AVANZATE ───────────────────────────────────────────

    def _build_caribbean_tab(self, parent):
        """v1071b: Tab Classificazione Caraibica — gestione artisti noti, BPM, priorità."""
        import json as _json

        def _load_carib_settings():
            """Carica le impostazioni caraibiche dalle settings."""
            try:
                from config.settings import settings as _s
                return _s
            except Exception:
                return None

        parent.rowconfigure(0, weight=1)

        scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent")
        scroll.grid(row=0, column=0, padx=0, pady=0, sticky="nsew")
        scroll.columnconfigure(0, weight=1)

        def csection(title, desc="", icon_name=""):
            _ic = _get_icon(icon_name, 22) if (icon_name and _ICONS_AVAILABLE) else None
            ctk.CTkLabel(scroll, text=title, font=FONT_HEAD,
                         text_color=PALETTE["text"],
                         image=_ic, compound="left"
                         ).pack(anchor="w", padx=12, pady=(16, 2))
            if desc:
                ctk.CTkLabel(scroll, text=desc, font=FONT_SMALL,
                             text_color=PALETTE["text_dim"], wraplength=550, justify="left"
                             ).pack(anchor="w", padx=12, pady=(0, 4))
            frm = ctk.CTkFrame(scroll, fg_color=PALETTE["surface2"], corner_radius=8)
            frm.pack(fill="x", padx=8, pady=(0, 8))
            return frm

        # ── Priorità classificazione — DRAG & DROP con Listbox ──────────
        frm_prio = csection("  Priorità Classificazione", "Trascina le voci per riordinare la priorità. La prima ha la precedenza massima.", icon_name="classify")

        import tkinter as _tk_prio

        self._prio_items = [
            "Nome file (parola 'Salsa'/'Bachata'…)",
            "Artisti noti (lista configurata sotto)",
            "DB online (MusicBrainz, Deezer, iTunes)",
            "Detection BPM + indicatori testuali",
            "Metadati ID3 già presenti nel file",
        ]

        prio_frame = ctk.CTkFrame(frm_prio, fg_color=PALETTE["bg"], corner_radius=6)
        prio_frame.pack(fill="x", padx=12, pady=(4, 8))

        self._prio_listbox = _tk_prio.Listbox(
            prio_frame,
            bg=PALETTE["bg"], fg=PALETTE["text"],
            selectbackground=PALETTE["primary"], selectforeground=PALETTE["bg"],
            font=FONT_SMALL, relief="flat", bd=0,
            activestyle="none", height=len(self._prio_items),
        )
        self._prio_listbox.pack(fill="x", padx=4, pady=4)
        for i, item in enumerate(self._prio_items):
            self._prio_listbox.insert("end", f"  {i+1}°  {item}")

        hint_prio = ctk.CTkLabel(prio_frame,
            text="⬆ Su / ⬇ Giù  — seleziona e clicca per spostare",
            font=(FONT_SMALL[0], FONT_SMALL[1]-1), text_color=PALETTE["text_dim"])
        hint_prio.pack(pady=(0, 4))

        btn_prio_frm = ctk.CTkFrame(frm_prio, fg_color="transparent")
        btn_prio_frm.pack(pady=(0, 8))

        def _prio_move(direction):
            sel = self._prio_listbox.curselection()
            if not sel:
                return
            i = sel[0]
            j = i + direction
            if j < 0 or j >= self._prio_listbox.size():
                return
            # Scambia
            text_i = self._prio_listbox.get(i)
            text_j = self._prio_listbox.get(j)
            self._prio_listbox.delete(min(i,j), max(i,j))
            if direction < 0:
                self._prio_listbox.insert(j, text_i)
                self._prio_listbox.insert(i, text_j)
            else:
                self._prio_listbox.insert(i, text_j)
                self._prio_listbox.insert(j, text_i)
            # Rinumera
            for k in range(self._prio_listbox.size()):
                t = self._prio_listbox.get(k)
                stripped = t.strip().split("°  ", 1)[-1] if "°  " in t else t.strip()
                self._prio_listbox.delete(k)
                self._prio_listbox.insert(k, f"  {k+1}°  {stripped}")
            self._prio_listbox.selection_set(j)

        _ic_up   = _get_icon("up",   28) if _ICONS_AVAILABLE else None
        _ic_down = _get_icon("down", 28) if _ICONS_AVAILABLE else None

        # v1075: _tooltip_carib unificato nel singleton globale _add_tooltip
        def _tooltip_carib(btn, text):
            self._add_tooltip(btn, text)

        btn_su = ctk.CTkButton(btn_prio_frm,
                      text="" if _ic_up else "⬆", image=_ic_up,
                      width=44, height=36, font=FONT_SMALL,
                      fg_color=PALETTE["surface2"], hover_color=PALETTE["primary"],
                      command=lambda: _prio_move(-1))
        btn_su.pack(side="left", padx=4)
        _tooltip_carib(btn_su, "Sposta su — aumenta priorità")

        btn_giu = ctk.CTkButton(btn_prio_frm,
                       text="" if _ic_down else "⬇", image=_ic_down,
                       width=44, height=36, font=FONT_SMALL,
                       fg_color=PALETTE["surface2"], hover_color=PALETTE["primary"],
                       command=lambda: _prio_move(1))
        btn_giu.pack(side="left", padx=4)
        _tooltip_carib(btn_giu, "Sposta giù — diminuisce priorità")

        # ── Range BPM ────────────────────────────────────────────────────
        frm_bpm = csection("  Range BPM", "I range BPM usati per supportare la classificazione Salsa/Bachata.", icon_name="bpm_range")

        try:
            from config.settings import settings as _s
            b_min, b_max = _s.bpm.bachata_bpm_range
            s_min, s_max = _s.bpm.salsa_bpm_range
        except Exception:
            b_min, b_max = 90, 140
            s_min, s_max = 70, 200

        self._carib_b_min = ctk.StringVar(value=str(b_min))
        self._carib_b_max = ctk.StringVar(value=str(b_max))
        self._carib_s_min = ctk.StringVar(value=str(s_min))
        self._carib_s_max = ctk.StringVar(value=str(s_max))

        for label, vmin, vmax, hint in [
            ("Bachata BPM:", self._carib_b_min, self._carib_b_max, "tipico: 90–130"),
            ("Salsa BPM:",   self._carib_s_min, self._carib_s_max, "tipico: 70–200"),
        ]:
            row_bpm = ctk.CTkFrame(frm_bpm, fg_color="transparent")
            row_bpm.pack(fill="x", padx=12, pady=4)
            ctk.CTkLabel(row_bpm, text=label, font=FONT_SMALL,
                         text_color=PALETTE["text"], width=100, anchor="w"
                         ).pack(side="left")
            ctk.CTkEntry(row_bpm, textvariable=vmin, width=60,
                         font=FONT_SMALL, fg_color=PALETTE["bg"]
                         ).pack(side="left", padx=(0, 4))
            ctk.CTkLabel(row_bpm, text="—", font=FONT_SMALL,
                         text_color=PALETTE["text_dim"]).pack(side="left", padx=4)
            ctk.CTkEntry(row_bpm, textvariable=vmax, width=60,
                         font=FONT_SMALL, fg_color=PALETTE["bg"]
                         ).pack(side="left")
            ctk.CTkLabel(row_bpm, text=f"  ({hint})", font=(FONT_SMALL[0], FONT_SMALL[1]-1),
                         text_color=PALETTE["text_dim"]).pack(side="left", padx=8)
        ctk.CTkLabel(frm_bpm, text="", font=FONT_SMALL).pack(pady=2)

        # ── Difficoltà Salsa (range BPM per classificazione velocità) ────
        frm_sal_diff = csection("  Velocità Salsa per BPM", "Range BPM usati per classificare la difficoltà della Salsa in fase di catalogazione.", icon_name="velocita_bpm")

        # difficulty_ranges: struttura {level: {min_bpm, max_bpm, description}} o {level: (min, max)}
        try:
            from config.settings import settings as _s
            raw_ranges = getattr(_s.bpm, "difficulty_ranges", {})
            if raw_ranges and isinstance(next(iter(raw_ranges.values())), dict):
                diff_ranges = {
                    k: (v["min_bpm"], v.get("max_bpm", 999))
                    for k, v in raw_ranges.items()
                }
            elif raw_ranges:
                diff_ranges = raw_ranges  # già {level: (min, max)}
            else:
                raise ValueError("empty")
        except Exception:
            diff_ranges = {
                "1 - Romantica": (0, 79), "2 - Lenta": (80, 94),
                "3 - Media": (95, 99), "4 - Veloce": (100, 119), "5 - Crazy": (120, 999),
            }

        self._carib_diff_vars = {}
        for level, (bmin, bmax) in diff_ranges.items():
            row_d = ctk.CTkFrame(frm_sal_diff, fg_color="transparent")
            row_d.pack(fill="x", padx=12, pady=3)
            ctk.CTkLabel(row_d, text=f"{level}:", font=FONT_SMALL,
                         text_color=PALETTE["text"], width=120, anchor="w"
                         ).pack(side="left")
            vmin_d = ctk.StringVar(value=str(bmin))
            vmax_d = ctk.StringVar(value=str(bmax) if bmax < 900 else "∞")
            self._carib_diff_vars[level] = (vmin_d, vmax_d)
            ctk.CTkEntry(row_d, textvariable=vmin_d, width=55,
                         font=FONT_SMALL, fg_color=PALETTE["bg"]
                         ).pack(side="left", padx=(0, 4))
            ctk.CTkLabel(row_d, text="—", font=FONT_SMALL,
                         text_color=PALETTE["text_dim"]).pack(side="left", padx=4)
            ctk.CTkEntry(row_d, textvariable=vmax_d, width=55,
                         font=FONT_SMALL, fg_color=PALETTE["bg"]
                         ).pack(side="left")
        ctk.CTkLabel(frm_sal_diff, text="", font=FONT_SMALL).pack(pady=2)

        # ── Artisti Salsa ────────────────────────────────────────────────
        frm_salsa = csection("  Artisti Salsa Noti", "Uno per riga. Corrispondenza parziale sul nome artista (case-insensitive).", icon_name="artisti_noti")
        try:
            from config.settings import settings as _s
            salsa_artists = [x for x in _s.genre.salsa_indicators
                             if len(x) > 4 and not any(w in x for w in ['salsa','orquesta','combo','sonora'])]
        except Exception:
            salsa_artists = []

        self._carib_salsa_txt = ctk.CTkTextbox(
            frm_salsa, height=160, font=FONT_SMALL,
            fg_color=PALETTE["bg"], text_color=PALETTE["text"]
        )
        self._carib_salsa_txt.pack(fill="x", padx=12, pady=(4, 8))
        self._carib_salsa_txt.insert("end", "\n".join(salsa_artists))

        # ── Artisti Bachata ──────────────────────────────────────────────
        frm_bach = csection("  Artisti Bachata Noti", "Uno per riga. Corrispondenza parziale sul nome artista (case-insensitive).", icon_name="artisti_noti")
        try:
            from config.settings import settings as _s
            bach_artists = [x for x in _s.genre.bachata_indicators
                            if len(x) > 4 and 'bachata' not in x.lower()]
        except Exception:
            bach_artists = []

        self._carib_bach_txt = ctk.CTkTextbox(
            frm_bach, height=120, font=FONT_SMALL,
            fg_color=PALETTE["bg"], text_color=PALETTE["text"]
        )
        self._carib_bach_txt.pack(fill="x", padx=12, pady=(4, 8))
        self._carib_bach_txt.insert("end", "\n".join(bach_artists))

        # ── Indicatori testuali ──────────────────────────────────────────
        frm_ind = csection("  Indicatori Testuali (Salsa)", "Parole chiave cercate nel titolo/artista/filename per identificare la Salsa. Una per riga.", icon_name="indicatori")
        try:
            from config.settings import settings as _s
            salsa_kw = [x for x in _s.genre.salsa_indicators
                        if len(x) <= 4 or any(w in x for w in ['salsa','orquesta','combo','sonora','timba'])]
        except Exception:
            salsa_kw = ["salsa", "salsero", "orquesta", "combo", "sonora", "montuno", "guaguanco"]

        self._carib_salkw_txt = ctk.CTkTextbox(
            frm_ind, height=70, font=FONT_SMALL,
            fg_color=PALETTE["bg"], text_color=PALETTE["text"]
        )
        self._carib_salkw_txt.pack(fill="x", padx=12, pady=(4, 4))
        self._carib_salkw_txt.insert("end", "\n".join(salsa_kw))

        # ── Indicatori testuali Bachata ──────────────────────────────────
        frm_ind_bach = csection("  Indicatori Testuali (Bachata)", "Parole chiave per identificare la Bachata nel titolo/artista/filename. Una per riga.", icon_name="indicatori")
        try:
            from config.settings import settings as _s
            bach_kw = [x for x in _s.genre.bachata_indicators
                       if 'bachata' in x.lower() or len(x) <= 6]
        except Exception:
            bach_kw = ["bachata", "bachatero", "bachatera", "rey de la bachata", "principe de la bachata"]

        self._carib_bachkw_txt = ctk.CTkTextbox(
            frm_ind_bach, height=70, font=FONT_SMALL,
            fg_color=PALETTE["bg"], text_color=PALETTE["text"]
        )
        self._carib_bachkw_txt.pack(fill="x", padx=12, pady=(4, 4))
        self._carib_bachkw_txt.insert("end", "\n".join(bach_kw))

        # ── Pulsante salva ───────────────────────────────────────────────
        # v1085f: row con due bottoni in modalità admin server (salva
        # locale + pubblica come default per tutti); solo "Salva" altrove.
        btn_row_carib = ctk.CTkFrame(scroll, fg_color="transparent")
        btn_row_carib.pack(pady=(8, 16))
        ctk.CTkButton(
            btn_row_carib, text="💾  Salva impostazioni",
            fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
            font=FONT_BODY, command=self._save_caribbean_settings
        ).pack(side="left", padx=4)
        if (self.api_client is not None
                and self.user_info.get("is_admin", False)):
            ctk.CTkButton(
                btn_row_carib, text="📤  Pubblica come default per tutti",
                fg_color="#50aa70", hover_color="#3d8856",
                text_color="#ffffff",
                font=FONT_BODY,
                command=self._publish_caribbean_defaults
            ).pack(side="left", padx=4)
        # Carica impostazioni salvate nei widget
        self.root.after(100, self._populate_caribbean_widgets)

    def _publish_caribbean_defaults(self):
        """v1085f: admin pubblica le impostazioni caraibiche correnti
        come default condivisi sul server. Tutti i clienti che non hanno
        ancora un file locale `caribbean_settings.json` li riceveranno
        al prossimo boot.
        """
        from tkinter import messagebox
        import threading

        if self.api_client is None or not self.user_info.get("is_admin"):
            return

        # Costruisci payload dai widget correnti (stesso codice di _save)
        try:
            b_min = int(self._carib_b_min.get())
            b_max = int(self._carib_b_max.get())
            s_min = int(self._carib_s_min.get())
            s_max = int(self._carib_s_max.get())
            salsa_artists = [l.strip() for l in
                              self._carib_salsa_txt.get("1.0", "end").split("\n")
                              if l.strip()]
            salsa_kw = [l.strip() for l in
                         self._carib_salkw_txt.get("1.0", "end").split("\n")
                         if l.strip()]
            bach_artists = [l.strip() for l in
                             self._carib_bach_txt.get("1.0", "end").split("\n")
                             if l.strip()]
            bach_kw = [l.strip() for l in
                        self._carib_bachkw_txt.get("1.0", "end").split("\n")
                        if l.strip()] if hasattr(self, "_carib_bachkw_txt") else []
        except Exception as e:
            messagebox.showerror("Errore", f"Lettura impostazioni fallita:\n{e}")
            return

        if not messagebox.askyesno(
            "Pubblica default",
            f"Pubblicare queste impostazioni come DEFAULT per tutti gli "
            f"utenti?\n\n"
            f"• Salsa BPM: {s_min}-{s_max}\n"
            f"• Bachata BPM: {b_min}-{b_max}\n"
            f"• Artisti salsa: {len(salsa_artists)}\n"
            f"• Keyword salsa: {len(salsa_kw)}\n"
            f"• Artisti bachata: {len(bach_artists)}\n"
            f"• Keyword bachata: {len(bach_kw)}\n\n"
            f"I clienti senza impostazioni locali le scaricheranno "
            f"al prossimo avvio.\n"
            f"Chi ha già una configurazione locale NON verrà sovrascritto."):
            return

        payload = {
            "bachata_bpm_range":  [b_min, b_max],
            "salsa_bpm_range":    [s_min, s_max],
            "salsa_artists":      salsa_artists,
            "salsa_keywords":     salsa_kw,
            "bachata_artists":    bach_artists,
            "bachata_keywords":   bach_kw,
        }

        def _w():
            try:
                self.api_client.set_caribbean_defaults(payload)
                self._safe_after(0, lambda: messagebox.showinfo(
                    "Default pubblicati",
                    "Le impostazioni caraibiche sono ora i default condivisi.\n\n"
                    "I clienti senza file locale le scaricheranno al "
                    "prossimo avvio dell'applicazione."))
            except Exception as e:
                err_str = str(e)
                self._safe_after(0, lambda: messagebox.showerror(
                    "Errore", f"Pubblicazione fallita:\n{err_str}"))
        threading.Thread(target=_w, daemon=True).start()

    @staticmethod
    def _mark_caribbean_cache_dirty_static():
        """Scrive un file sentinella per segnalare al cataloger che le impostazioni caraibiche sono cambiate."""
        try:
            dirty = _get_data_dir() / "caribbean_dirty.flag"
            dirty.write_text(__import__("datetime").datetime.now().isoformat(), encoding="utf-8")
        except Exception:
            pass

    def _populate_caribbean_widgets(self):
        """Popola i widget del tab Caraibica con i dati salvati in JSON."""
        import json as _json
        carib_file = _get_data_dir() / "caribbean_settings.json"
        if not carib_file.exists():
            return
        try:
            data = _json.loads(carib_file.read_text(encoding="utf-8"))

            # BPM ranges
            if "bachata_bpm_range" in data and hasattr(self, "_carib_b_min"):
                self._carib_b_min.set(str(data["bachata_bpm_range"][0]))
                self._carib_b_max.set(str(data["bachata_bpm_range"][1]))
            if "salsa_bpm_range" in data and hasattr(self, "_carib_s_min"):
                self._carib_s_min.set(str(data["salsa_bpm_range"][0]))
                self._carib_s_max.set(str(data["salsa_bpm_range"][1]))

            # Artisti salsa
            if "salsa_artists" in data and hasattr(self, "_carib_salsa_txt"):
                self._carib_salsa_txt.delete("1.0", "end")
                self._carib_salsa_txt.insert("end", "\n".join(data["salsa_artists"]))

            # Keywords salsa
            if "salsa_keywords" in data and hasattr(self, "_carib_salkw_txt"):
                self._carib_salkw_txt.delete("1.0", "end")
                self._carib_salkw_txt.insert("end", "\n".join(data["salsa_keywords"]))

            # Artisti bachata
            if "bachata_artists" in data and hasattr(self, "_carib_bach_txt"):
                self._carib_bach_txt.delete("1.0", "end")
                self._carib_bach_txt.insert("end", "\n".join(data["bachata_artists"]))

            # Keywords bachata
            if "bachata_keywords" in data and hasattr(self, "_carib_bachkw_txt"):
                self._carib_bachkw_txt.delete("1.0", "end")
                self._carib_bachkw_txt.insert("end", "\n".join(data["bachata_keywords"]))

            # Applica anche alle settings runtime
            self._load_caribbean_settings()
        except Exception as e:
            pass  # non interrompere se il file è corrotto

    def _save_caribbean_settings(self):
        """v1072c: Salva le impostazioni caraibiche nelle settings runtime e su file JSON."""
        try:
            from config.settings import settings as _s
            import json as _json

            # BPM
            b_min = int(self._carib_b_min.get())
            b_max = int(self._carib_b_max.get())
            s_min = int(self._carib_s_min.get())
            s_max = int(self._carib_s_max.get())
            _s.bpm.bachata_bpm_range = (b_min, b_max)
            _s.bpm.salsa_bpm_range   = (s_min, s_max)

            # Artisti salsa
            salsa_lines = [l.strip() for l in
                           self._carib_salsa_txt.get("1.0", "end").split("\n")
                           if l.strip()]
            # Indicatori testuali salsa
            kw_lines = [l.strip() for l in
                        self._carib_salkw_txt.get("1.0", "end").split("\n")
                        if l.strip()]
            _s.genre.salsa_indicators = salsa_lines + kw_lines

            # Artisti bachata
            bach_lines = [l.strip() for l in
                          self._carib_bach_txt.get("1.0", "end").split("\n")
                          if l.strip()]
            # Indicatori testuali bachata
            bachkw_lines = []
            if hasattr(self, "_carib_bachkw_txt"):
                bachkw_lines = [l.strip() for l in
                                self._carib_bachkw_txt.get("1.0", "end").split("\n")
                                if l.strip()]
            core_bach = [x for x in ["bachata", "bachatero", "bachatera"]
                         if x not in bach_lines and x not in bachkw_lines]
            _s.genre.bachata_indicators = bach_lines + bachkw_lines + core_bach

            # Salva su file JSON persistente
            carib_data = {
                "bachata_bpm_range": [b_min, b_max],
                "salsa_bpm_range":   [s_min, s_max],
                "salsa_artists":     salsa_lines,
                "salsa_keywords":    kw_lines,
                "bachata_artists":   bach_lines,
                "bachata_keywords":  bachkw_lines,
            }
            carib_file = _get_data_dir() / "caribbean_settings.json"
            import datetime as _dt
            carib_data["_saved_at"] = _dt.datetime.now().isoformat()
            carib_file.write_text(
                _json.dumps(carib_data, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
            # Invalida la cache metadati per i file latini salvando il timestamp
            MusicCatalogerGUI._mark_caribbean_cache_dirty_static()
            messagebox.showinfo("Salvato",
                "Impostazioni Caraibica salvate.\nVerranno caricate ad ogni avvio.")
        except Exception as e:
            messagebox.showerror("Errore", str(e))

    def _load_caribbean_settings(self):
        """Carica le impostazioni caraibiche da file JSON se disponibili.

        v1085f: se non c'è un file locale `caribbean_settings.json`, prova a
        scaricare i default condivisi dal server (pubblicati dall'admin
        tramite `POST /admin/caribbean-settings`). Questo evita che ogni
        nuovo cliente debba ri-configurare manualmente artisti, keyword
        e BPM range. Il fetch è non-bloccante (thread separato) per non
        rallentare il boot.
        """
        import json as _json
        carib_file = _get_data_dir() / "caribbean_settings.json"

        # Caso 1: file locale presente → carico da lì
        if carib_file.exists():
            try:
                data = _json.loads(carib_file.read_text(encoding="utf-8"))
                self._apply_caribbean_data(data)
            except Exception:
                pass
            return

        # Caso 2: file locale assente + modalità server → fetch dai default
        # condivisi pubblicati dall'admin. Salvo localmente per non
        # ripetere la chiamata ad ogni boot.
        if self.api_client is None:
            return  # offline: niente da fare

        import threading
        def _fetch_worker():
            try:
                data = self.api_client.get_caribbean_defaults()
                # Verifica che ci siano dati utili (l'admin potrebbe non
                # aver ancora pubblicato nulla → struttura vuota)
                has_data = (data.get("salsa_artists") or
                            data.get("bachata_artists") or
                            data.get("salsa_keywords") or
                            data.get("bachata_keywords"))
                if not has_data:
                    return
                # Persisti il file localmente
                try:
                    import datetime as _dt
                    data["_loaded_from_server_at"] = _dt.datetime.now().isoformat()
                    carib_file.parent.mkdir(parents=True, exist_ok=True)
                    carib_file.write_text(
                        _json.dumps(data, indent=2, ensure_ascii=False),
                        encoding="utf-8")
                except Exception as e:
                    print(f"[caribbean] impossibile salvare locale: {e}")
                # Applica nelle settings runtime sul main thread
                self._safe_after(0, lambda: self._apply_caribbean_data(data))
                self._safe_after(0, lambda: self._refresh_caribbean_ui_if_open())
            except Exception as e:
                print(f"[caribbean] fetch defaults dal server fallito: {e}")
        threading.Thread(target=_fetch_worker, daemon=True).start()

    def _apply_caribbean_data(self, data: dict):
        """Applica i dati caraibici nelle settings runtime."""
        try:
            from config.settings import settings as _s
            if "bachata_bpm_range" in data and data["bachata_bpm_range"]:
                _s.bpm.bachata_bpm_range = tuple(data["bachata_bpm_range"])
            if "salsa_bpm_range" in data and data["salsa_bpm_range"]:
                _s.bpm.salsa_bpm_range = tuple(data["salsa_bpm_range"])
            sal = (data.get("salsa_artists") or []) + (data.get("salsa_keywords") or [])
            if sal:
                _s.genre.salsa_indicators = sal
            bac = ((data.get("bachata_artists") or [])
                   + (data.get("bachata_keywords") or [])
                   + ["bachata", "bachatero"])
            if bac:
                _s.genre.bachata_indicators = bac
        except Exception as e:
            print(f"[caribbean] apply failed: {e}")

    def _refresh_caribbean_ui_if_open(self):
        """Se l'utente ha già il tab Caraibica aperto, ricarica i widget
        con i nuovi valori scaricati dal server."""
        try:
            if hasattr(self, "_carib_salsa_txt"):
                from config.settings import settings as _s
                # Trigger re-popola box solo se i widget esistono
                self._carib_salsa_txt.delete("1.0", "end")
                self._carib_salsa_txt.insert("end",
                    "\n".join(_s.genre.salsa_indicators))
                if hasattr(self, "_carib_bach_txt"):
                    self._carib_bach_txt.delete("1.0", "end")
                    self._carib_bach_txt.insert("end",
                        "\n".join([x for x in _s.genre.bachata_indicators
                                    if x not in ("bachata","bachatero","bachatera")]))
        except Exception:
            pass

    def _build_admin_section(self, scroll, section_factory):
        """
        v0.0.2.3 — Sezione amministratore nel tab Avanzate.

        Mostra (in ordine dall'alto):
        - 📊 Statistiche server (utenti totali, jobs, plan distribution, ...)
        - 👤 Crea utente (admin bypass del self-service)
        - 👑 Richieste Upgrade pending (approva/rifiuta)
        - 👥 Utenti registrati (cambio piano)
        - 🔐 Stato registrazione (toggle abilita/disabilita)
        - 📋 Audit log (timeline azioni admin)

        Visibile solo se user_info.is_admin == True.
        """
        import customtkinter as ctk

        # ── v1085h: Statistiche server ─────────────────────────────
        frm_stats = section_factory(
            "  📊  Pannello Amministratore  —  Statistiche server",
            icon_name="")
        frm_stats.columnconfigure(0, weight=1)
        self._admin_stats_grid = ctk.CTkFrame(frm_stats, fg_color="transparent")
        self._admin_stats_grid.pack(fill="x", padx=12, pady=(8, 4))
        self._admin_stats_status = ctk.CTkLabel(
            frm_stats, text="Caricamento statistiche…",
            font=FONT_SMALL, text_color=PALETTE["text_dim"])
        self._admin_stats_status.pack(side="left", padx=12, pady=(0, 8))
        ctk.CTkButton(
            frm_stats, text="🔄  Aggiorna", width=100, height=28,
            font=(FONT_SMALL[0], FONT_SMALL[1]-1),
            fg_color=PALETTE.get("surface", "#1e2533"),
            hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
            text_color=PALETTE["text"],
            command=self._admin_refresh_stats,
        ).pack(side="right", padx=12, pady=(0, 8))
        self.root.after(300, self._admin_refresh_stats)

        # ── v1085h: Crea utente (admin) ────────────────────────────
        frm_create = section_factory(
            "  👤  Pannello Amministratore  —  Crea utente",
            icon_name="")
        frm_create.columnconfigure(0, weight=1)
        info_lbl = ctk.CTkLabel(
            frm_create,
            text="Crea direttamente un account per un cliente. Utile per "
                 "il pilot privato dove la registrazione self-service è "
                 "disabilitata.",
            font=(FONT_SMALL[0], FONT_SMALL[1]-1),
            text_color=PALETTE["text_dim"], anchor="w", wraplength=900,
            justify="left")
        info_lbl.pack(fill="x", padx=12, pady=(8, 4))
        ctk.CTkButton(
            frm_create, text="➕  Apri form crea utente", width=240, height=34,
            font=FONT_SMALL,
            fg_color=PALETTE.get("primary", "#3b6fd4"),
            hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
            text_color="#ffffff",
            command=self._admin_show_create_user_dialog,
        ).pack(anchor="w", padx=12, pady=(4, 12))

        frm = section_factory("  👑  Pannello Amministratore  —  Richieste Upgrade",
                              icon_name="")
        frm.columnconfigure(0, weight=1)

        # Header con Aggiorna
        hdr = ctk.CTkFrame(frm, fg_color="transparent")
        hdr.pack(fill="x", padx=12, pady=(8, 4))
        self._admin_status_lbl = ctk.CTkLabel(
            hdr, text="Caricamento richieste…",
            font=FONT_SMALL, text_color=PALETTE["text_dim"], anchor="w")
        self._admin_status_lbl.pack(side="left")
        ctk.CTkButton(
            hdr, text="🔄  Aggiorna", width=100, height=28,
            font=(FONT_SMALL[0], FONT_SMALL[1]-1),
            fg_color=PALETTE.get("surface", "#1e2533"),
            hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
            text_color=PALETTE["text"],
            command=self._admin_refresh_requests,
        ).pack(side="right")

        # Container per la lista (popolato da _admin_refresh_requests)
        self._admin_list_frame = ctk.CTkFrame(frm, fg_color="transparent")
        self._admin_list_frame.pack(fill="x", padx=12, pady=(4, 12))

        # Carica al primo build
        self.root.after(200, self._admin_refresh_requests)

        # ── v0.0.2.5: Sezione "Utenti registrati" ─────────────────
        frm_users = section_factory(
            "  👥  Pannello Amministratore  —  Utenti registrati",
            icon_name="")
        frm_users.columnconfigure(0, weight=1)

        hdr_u = ctk.CTkFrame(frm_users, fg_color="transparent")
        hdr_u.pack(fill="x", padx=12, pady=(8, 4))
        self._admin_users_status_lbl = ctk.CTkLabel(
            hdr_u, text="Caricamento utenti…",
            font=FONT_SMALL, text_color=PALETTE["text_dim"], anchor="w")
        self._admin_users_status_lbl.pack(side="left")
        ctk.CTkButton(
            hdr_u, text="🔄  Aggiorna", width=100, height=28,
            font=(FONT_SMALL[0], FONT_SMALL[1]-1),
            fg_color=PALETTE.get("surface", "#1e2533"),
            hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
            text_color=PALETTE["text"],
            command=self._admin_refresh_users,
        ).pack(side="right")

        self._admin_users_list_frame = ctk.CTkFrame(frm_users,
                                                     fg_color="transparent")
        self._admin_users_list_frame.pack(fill="x", padx=12, pady=(4, 12))

        self.root.after(400, self._admin_refresh_users)

        # ── v1085g: Sezione "Stato registrazione" ──────────────────
        frm_reg = section_factory(
            "  🔐  Pannello Amministratore  —  Registrazione self-service",
            icon_name="")
        frm_reg.columnconfigure(0, weight=1)
        reg_row = ctk.CTkFrame(frm_reg, fg_color="transparent")
        reg_row.pack(fill="x", padx=12, pady=(8, 12))
        self._admin_reg_status_lbl = ctk.CTkLabel(
            reg_row, text="Caricamento stato registrazione…",
            font=FONT_SMALL, text_color=PALETTE["text_dim"], anchor="w")
        self._admin_reg_status_lbl.pack(side="left", expand=True, fill="x")
        self._admin_reg_btn = ctk.CTkButton(
            reg_row, text="...", width=160, height=30,
            font=(FONT_SMALL[0], FONT_SMALL[1]-1, "bold"),
            command=self._admin_toggle_registration,
        )
        self._admin_reg_btn.pack(side="right")

        # Carica lo stato corrente al primo build
        self.root.after(500, self._admin_refresh_registration_status)

        # ── v1085g: Sezione "Audit log" ───────────────────────────
        frm_audit = section_factory(
            "  📋  Pannello Amministratore  —  Audit Log azioni",
            icon_name="")
        frm_audit.columnconfigure(0, weight=1)

        hdr_a = ctk.CTkFrame(frm_audit, fg_color="transparent")
        hdr_a.pack(fill="x", padx=12, pady=(8, 4))
        self._admin_audit_status_lbl = ctk.CTkLabel(
            hdr_a, text="Caricamento storico…",
            font=FONT_SMALL, text_color=PALETTE["text_dim"], anchor="w")
        self._admin_audit_status_lbl.pack(side="left")
        ctk.CTkButton(
            hdr_a, text="🔄  Aggiorna", width=100, height=28,
            font=(FONT_SMALL[0], FONT_SMALL[1]-1),
            fg_color=PALETTE.get("surface", "#1e2533"),
            hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
            text_color=PALETTE["text"],
            command=self._admin_refresh_audit,
        ).pack(side="right")

        self._admin_audit_list_frame = ctk.CTkFrame(frm_audit,
                                                     fg_color="transparent")
        self._admin_audit_list_frame.pack(fill="x", padx=12, pady=(4, 12))

        self.root.after(600, self._admin_refresh_audit)

    def _admin_refresh_registration_status(self):
        """v1085g: aggiorna label e testo bottone in base allo stato server."""
        import threading
        if self.api_client is None:
            return

        def _worker():
            try:
                resp = self.api_client.get_registration_status()
                enabled = bool(resp.get("enabled", True))

                def _ui():
                    if enabled:
                        self._admin_reg_status_lbl.configure(
                            text="✓  Registrazione self-service ATTIVA  "
                                 "—  chiunque può creare un account",
                            text_color="#50aa70")
                        self._admin_reg_btn.configure(
                            text="🔒  Disabilita",
                            fg_color="#d84545", hover_color="#a83838",
                            text_color="#ffffff")
                    else:
                        self._admin_reg_status_lbl.configure(
                            text="🔒  Registrazione DISABILITATA  "
                                 "—  solo l'admin crea account",
                            text_color="#d8a045")
                        self._admin_reg_btn.configure(
                            text="🔓  Abilita",
                            fg_color="#50aa70", hover_color="#3d8856",
                            text_color="#ffffff")
                self._safe_after(0, _ui)
            except Exception as e:
                err_str = str(e)
                self._safe_after(0, lambda:
                    self._admin_reg_status_lbl.configure(
                        text=f"Errore: {err_str}",
                        text_color=PALETTE.get("error", "#d84545")))
        threading.Thread(target=_worker, daemon=True).start()

    def _admin_toggle_registration(self):
        """Inverte lo stato registrazione (con conferma)."""
        from tkinter import messagebox
        import threading

        # Test stato corrente prima di chiedere
        try:
            current = self.api_client.get_registration_status()
            currently_enabled = bool(current.get("enabled", True))
        except Exception as e:
            messagebox.showerror("Errore", f"Stato server non disponibile:\n{e}")
            return

        if currently_enabled:
            confirm = messagebox.askyesno(
                "Disabilitare la registrazione?",
                "Vuoi davvero DISABILITARE la registrazione self-service?\n\n"
                "Dopo questa modifica:\n"
                "• Il link 'Registrati' sparirà dalla login window\n"
                "• Eventuali POST /auth/register risponderanno 403\n"
                "• Solo TU (admin) potrai creare account\n\n"
                "Puoi riabilitarla in qualunque momento da qui.")
        else:
            confirm = messagebox.askyesno(
                "Abilitare la registrazione?",
                "Vuoi RIABILITARE la registrazione self-service?\n\n"
                "Chiunque potrà creare un account dal link 'Registrati'.\n"
                "Tutti i nuovi utenti partiranno con piano Base.")
        if not confirm:
            return

        def _worker():
            try:
                if currently_enabled:
                    self.api_client.admin_disable_registration()
                else:
                    self.api_client.admin_enable_registration()
                self._safe_after(0, self._admin_refresh_registration_status)
                self._safe_after(0, lambda: messagebox.showinfo(
                    "Stato aggiornato",
                    "Registrazione " +
                    ("DISABILITATA" if currently_enabled else "ABILITATA")))
            except Exception as e:
                err_str = str(e)
                self._safe_after(0, lambda: messagebox.showerror(
                    "Errore", f"Operazione fallita:\n{err_str}"))
        threading.Thread(target=_worker, daemon=True).start()

    def _admin_refresh_stats(self):
        """Carica statistiche server e popola la griglia."""
        import threading
        if self.api_client is None:
            return

        def _worker():
            try:
                stats = self.api_client.get_admin_stats()
                self._safe_after(0, lambda: self._admin_render_stats(stats))
            except Exception as e:
                err_str = str(e)
                self._safe_after(0, lambda:
                    self._admin_stats_status.configure(
                        text=f"Errore: {err_str}",
                        text_color=PALETTE.get("error", "#d84545")))
        threading.Thread(target=_worker, daemon=True).start()

    def _admin_render_stats(self, stats: dict):
        """Render grid con KPI cards."""
        try:
            for w in self._admin_stats_grid.winfo_children():
                w.destroy()
        except Exception:
            return

        # Layout 2 righe × 4 colonne di KPI
        kpis = [
            ("👥", "Utenti totali", str(stats.get("n_users_total", 0)), "#3b6fd4"),
            ("👑", "Admin", str(stats.get("n_users_admin", 0)), "#aa70d0"),
            ("⚙️", "Job totali", str(stats.get("n_jobs_total", 0)), "#3b6fd4"),
            ("✅", "Job completati", str(stats.get("n_jobs_completed", 0)), "#50aa70"),
            ("🎵", "File processati", f"{stats.get('n_files_processed', 0):,}".replace(",", "."), "#d8a045"),
            ("⏳", "Job in corso", str(stats.get("n_jobs_running", 0)), "#d8a045"),
            ("✗", "Job falliti", str(stats.get("n_jobs_failed", 0)), "#d84545"),
            ("🔔", "Upgrade pending", str(stats.get("n_pending_upgrades", 0)), "#d8a045"),
        ]

        for i, (emoji, label, value, color) in enumerate(kpis):
            row, col = divmod(i, 4)
            cell = ctk.CTkFrame(self._admin_stats_grid,
                                fg_color=PALETTE.get("surface", "#1e2533"),
                                corner_radius=8)
            cell.grid(row=row, column=col, sticky="nsew", padx=4, pady=4)
            self._admin_stats_grid.columnconfigure(col, weight=1, uniform="kpi")
            ctk.CTkLabel(cell, text=emoji, font=("Segoe UI", 18),
                         text_color=color).pack(pady=(8, 0))
            ctk.CTkLabel(cell, text=value,
                         font=("Segoe UI", 18, "bold"),
                         text_color=PALETTE["text"]).pack()
            ctk.CTkLabel(cell, text=label,
                         font=(FONT_SMALL[0], FONT_SMALL[1]-1),
                         text_color=PALETTE["text_dim"]).pack(pady=(0, 8))

        # Status sotto la grid: plan distribution + db size + version
        plans = stats.get("n_users_by_plan", {})
        plan_str = "  ".join(
            f"{p.capitalize()}: {plans.get(p, 0)}"
            for p in ("base", "pro", "advanced"))
        db_kb = stats.get("db_size_kb", 0)
        ver  = stats.get("server_version", "?")
        self._admin_stats_status.configure(
            text=f"📦 Server v{ver}  •  💾 DB: {db_kb} KB  •  📊 Piani: {plan_str}",
            text_color=PALETTE["text_dim"])

    def _admin_show_create_user_dialog(self):
        """Dialog modale per creazione manuale utente da admin.

        Form: email, username, password, plan, is_admin checkbox.
        Sottomette via POST /auth/admin/users.
        """
        from tkinter import messagebox
        import threading

        if self.api_client is None or not self.user_info.get("is_admin"):
            return

        DLG_W, DLG_H = 460, 580
        win = ctk.CTkToplevel(self.root)
        win.title("Crea nuovo utente")
        win.geometry(f"{DLG_W}x{DLG_H}")
        win.resizable(False, False)
        # v1085m: dialog "standalone" con titlebar Windows nativa.
        # Niente overrideredirect=True, niente transient: questo lo fa
        # apparire come finestra separata in taskbar (con propria icona,
        # propria entry alt+tab) e Windows gestisce automaticamente:
        # - minimize/restore
        # - z-order
        # - clic in taskbar per portare in front
        # - non resta sopra altre app quando l'utente cambia finestra
        # È la stessa strategia della finestra "Catalogazione completata".
        win.configure(fg_color=PALETTE["bg"])
        # Imposta icona della finestra usando lo stesso meccanismo della main
        try:
            from gui.app_icon import set_window_icon
            set_window_icon(win)
        except Exception:
            pass
        try:
            win.lift()
            win.focus_force()
        except Exception:
            pass
        # Centra sopra root
        try:
            self.root.update_idletasks()
            rx = self.root.winfo_x(); ry = self.root.winfo_y()
            rw = self.root.winfo_width(); rh = self.root.winfo_height()
            win.geometry(f"{DLG_W}x{DLG_H}+{rx + (rw-DLG_W)//2}"
                         f"+{ry + (rh-DLG_H)//2}")
        except Exception:
            pass

        # Niente più titlebar custom (uso quella nativa Windows)

        # Btn row BOTTOM
        btn_row = ctk.CTkFrame(win, fg_color="transparent", height=64)
        btn_row.pack(side="bottom", fill="x", padx=20, pady=(0, 14))
        btn_row.pack_propagate(False)

        # Status
        status_var = ctk.StringVar(value="")
        status_lbl = ctk.CTkLabel(win, textvariable=status_var,
            font=("Segoe UI", 10),
            text_color=PALETTE["text_dim"],
            wraplength=400, justify="center")
        status_lbl.pack(side="bottom", fill="x", padx=20, pady=(2, 0))

        # Body
        body = ctk.CTkFrame(win, fg_color="transparent")
        body.pack(side="top", fill="both", expand=True, padx=0, pady=(8, 0))

        ctk.CTkLabel(body, text="Crea account amministrato",
                     font=("Segoe UI", 13, "bold"),
                     text_color=PALETTE["text"]
                     ).pack(pady=(8, 2))
        ctk.CTkLabel(body,
            text="L'utente potrà loggarsi subito.\n"
                 "Comunica le credenziali al cliente attraverso un canale sicuro.",
            font=("Segoe UI", 9),
            text_color=PALETTE["text_dim"], justify="center"
            ).pack(pady=(0, 10))

        form = ctk.CTkFrame(body, fg_color=PALETTE["surface"], corner_radius=10)
        form.pack(fill="x", padx=24, pady=(0, 4))

        def _field(label, show=None, default=""):
            ctk.CTkLabel(form, text=label, anchor="w",
                         font=("Segoe UI", 10, "bold"),
                         text_color=PALETTE["text"]
                         ).pack(fill="x", padx=14, pady=(8, 2))
            v = ctk.StringVar(value=default)
            kw = {"show": show} if show else {}
            ctk.CTkEntry(form, textvariable=v,
                         fg_color=PALETTE["surface2"],
                         border_color=PALETTE["border"],
                         text_color=PALETTE["text"], height=30, **kw
                         ).pack(fill="x", padx=14, pady=(0, 4))
            return v

        email_var    = _field("Email")
        username_var = _field("Nome utente")
        pwd_var      = _field("Password (min 8 caratteri)", show="•")

        # Plan + admin row
        ctk.CTkLabel(form, text="Piano iniziale", anchor="w",
                     font=("Segoe UI", 10, "bold"),
                     text_color=PALETTE["text"]
                     ).pack(fill="x", padx=14, pady=(8, 2))
        plan_var = ctk.StringVar(value="base")
        plan_dropdown = ctk.CTkOptionMenu(
            form, variable=plan_var, values=["base", "pro", "advanced"],
            fg_color=PALETTE["surface2"],
            button_color=PALETTE.get("primary", "#3b6fd4"),
            button_hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
            text_color=PALETTE["text"], width=160, height=30)
        plan_dropdown.pack(anchor="w", padx=14, pady=(0, 4))

        admin_var = ctk.BooleanVar(value=False)
        ctk.CTkCheckBox(form, text="È un amministratore (può approvare upgrade)",
                        variable=admin_var,
                        font=("Segoe UI", 10),
                        text_color=PALETTE["text"]
                        ).pack(anchor="w", padx=14, pady=(8, 12))

        def _submit():
            email = email_var.get().strip()
            uname = username_var.get().strip()
            pwd = pwd_var.get()
            if not email or "@" not in email:
                status_var.set("Email non valida")
                status_lbl.configure(text_color=PALETTE.get("error", "#d84545"))
                return
            if not uname or len(uname) < 2:
                status_var.set("Nome utente troppo corto")
                status_lbl.configure(text_color=PALETTE.get("error", "#d84545"))
                return
            if len(pwd) < 8:
                status_var.set("Password di almeno 8 caratteri")
                status_lbl.configure(text_color=PALETTE.get("error", "#d84545"))
                return
            btn_ok.configure(state="disabled", text="Creazione…")
            btn_cancel.configure(state="disabled")
            status_var.set("Invio al server…")
            status_lbl.configure(text_color=PALETTE["text_dim"])

            def _w():
                try:
                    res = self.api_client.admin_create_user(
                        email=email, username=uname, password=pwd,
                        plan=plan_var.get(), is_admin=admin_var.get())
                    self._safe_after(0, lambda: (
                        win.destroy(),
                        messagebox.showinfo("Utente creato",
                            f"Account {res.get('email')} creato con successo.\n\n"
                            f"Comunica al cliente:\n"
                            f"  Email:    {email}\n"
                            f"  Password: {pwd}\n"
                            f"  Piano:    {plan_var.get()}\n\n"
                            f"Il cliente può loggarsi subito."),
                        self._admin_refresh_users(),
                        self._admin_refresh_stats(),
                    ))
                except Exception as e:
                    err_str = str(e)
                    self._safe_after(0, lambda: (
                        status_var.set(f"Errore: {err_str}"),
                        status_lbl.configure(text_color=PALETTE.get("error", "#d84545")),
                        btn_ok.configure(state="normal", text="✓  Crea utente"),
                        btn_cancel.configure(state="normal"),
                    ))
            threading.Thread(target=_w, daemon=True).start()

        btn_cancel = ctk.CTkButton(
            btn_row, text="Annulla", width=110, height=36,
            fg_color="transparent", hover_color=PALETTE["surface"],
            text_color=PALETTE["text_dim"],
            font=("Segoe UI", 10), command=win.destroy)
        btn_cancel.pack(side="right", padx=(4, 0), pady=14)
        btn_ok = ctk.CTkButton(
            btn_row, text="✓  Crea utente", width=170, height=36,
            fg_color=PALETTE.get("primary", "#3b6fd4"),
            hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
            text_color="#ffffff",
            font=("Segoe UI", 10, "bold"), command=_submit)
        btn_ok.pack(side="right", pady=14)
        win.bind("<Escape>", lambda e: win.destroy())
        win.bind("<Return>", lambda e: _submit())

    def _admin_refresh_audit(self):
        """Ricarica audit log delle azioni admin dal server."""
        import threading
        if self.api_client is None:
            return
        try:
            for w in self._admin_audit_list_frame.winfo_children():
                w.destroy()
        except Exception:
            return
        try:
            self._admin_audit_status_lbl.configure(
                text="Caricamento storico…",
                text_color=PALETTE["text_dim"])
        except Exception:
            pass

        def _worker():
            try:
                entries = self.api_client._request("GET",
                    "/admin/audit-log?limit=50")
                self._safe_after(0, lambda: self._admin_render_audit(entries))
            except Exception as e:
                err_str = str(e)
                self._safe_after(0, lambda:
                    self._admin_audit_status_lbl.configure(
                        text=f"Errore: {err_str}",
                        text_color=PALETTE.get("error", "#d84545")))
        threading.Thread(target=_worker, daemon=True).start()

    def _admin_render_audit(self, entries: list):
        """Render della lista audit log con timeline visiva."""
        try:
            for w in self._admin_audit_list_frame.winfo_children():
                w.destroy()
        except Exception:
            return

        if not entries:
            self._admin_audit_status_lbl.configure(
                text="Nessuna azione amministrativa registrata",
                text_color=PALETTE["text_dim"])
            return

        self._admin_audit_status_lbl.configure(
            text=f"📊  {len(entries)} azioni recenti",
            text_color=PALETTE["text"])

        # Mappa action → emoji + descrizione user-friendly
        action_map = {
            "upgrade_approved":              ("✅", "Upgrade approvato"),
            "upgrade_rejected":              ("❌", "Upgrade rifiutato"),
            "plan_changed":                  ("🔄", "Piano modificato"),
            "caribbean_defaults_published":  ("📤", "Default Caraibica pubblicati"),
        }
        action_color = {
            "upgrade_approved":              "#50aa70",
            "upgrade_rejected":              "#d84545",
            "plan_changed":                  "#3b6fd4",
            "caribbean_defaults_published":  "#aa70d0",
        }

        for entry in entries[:50]:
            row = ctk.CTkFrame(self._admin_audit_list_frame,
                               fg_color=PALETTE.get("surface", "#1e2533"),
                               corner_radius=6)
            row.pack(fill="x", pady=2)
            row.columnconfigure(1, weight=1)

            action = entry.get("action", "?")
            emoji, action_label = action_map.get(action, ("•", action))
            color = action_color.get(action, PALETTE["text"])

            # Colonna sinistra: emoji
            ctk.CTkLabel(row, text=emoji, font=("Segoe UI", 16),
                         text_color=color, width=40
                         ).grid(row=0, column=0, rowspan=2, padx=(10, 6),
                                pady=8, sticky="ns")

            # Riga 1: action + target
            target_email = entry.get("target_email") or ""
            target_id    = entry.get("target_id")
            target_str = ""
            if target_email:
                target_str = f"  →  {target_email}"
            elif target_id is not None:
                target_str = f"  →  user #{target_id}"

            ctk.CTkLabel(row,
                text=f"{action_label}{target_str}",
                font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
                text_color=PALETTE["text"], anchor="w"
            ).grid(row=0, column=1, sticky="ew", padx=(0, 10), pady=(8, 0))

            # Riga 2: admin + timestamp + dettagli (parsificati da JSON)
            admin_email = entry.get("admin_email", "?")
            created    = entry.get("created_at", "") or ""
            created_disp = str(created)[:19].replace("T", " ")

            details_str = ""
            details = entry.get("details")
            if details:
                try:
                    import json as _json
                    d = _json.loads(details) if isinstance(details, str) else details
                    if action == "upgrade_approved" or action == "plan_changed":
                        old_p = d.get("old_plan", "?")
                        new_p = d.get("new_plan", "?")
                        details_str = f"  •  {old_p} → {new_p}"
                        if d.get("admin_note"):
                            note = d["admin_note"][:40]
                            details_str += f'  •  "{note}"'
                    elif action == "upgrade_rejected":
                        details_str = f"  •  {d.get('from_plan','?')} → {d.get('to_plan','?')}"
                        if d.get("admin_note"):
                            note = d["admin_note"][:40]
                            details_str += f'  •  motivo: "{note}"'
                    elif action == "caribbean_defaults_published":
                        n = d.get("n_salsa_artists", 0)
                        m = d.get("n_bachata_artists", 0)
                        details_str = f"  •  {n} artisti salsa, {m} artisti bachata"
                except Exception:
                    pass

            sub_text = f"da {admin_email}  •  {created_disp}{details_str}"
            ctk.CTkLabel(row, text=sub_text,
                font=(FONT_SMALL[0], FONT_SMALL[1]-1),
                text_color=PALETTE["text_dim"], anchor="w",
                wraplength=900, justify="left"
            ).grid(row=1, column=1, sticky="ew",
                   padx=(0, 10), pady=(0, 8))

    def _admin_refresh_users(self):
        """Ricarica la lista utenti dal server in thread separato."""
        import threading
        if self.api_client is None:
            return
        try:
            for w in self._admin_users_list_frame.winfo_children():
                w.destroy()
        except Exception:
            return
        try:
            self._admin_users_status_lbl.configure(
                text="Caricamento utenti…", text_color=PALETTE["text_dim"])
        except Exception:
            pass

        def _worker():
            try:
                users = self.api_client._request("GET", "/admin/users")
                self._safe_after(0, lambda: self._admin_render_users(users))
            except Exception as e:
                err_str = str(e)
                self._safe_after(0, lambda: self._admin_users_status_lbl.configure(
                    text=f"Errore caricamento utenti: {err_str}",
                    text_color=PALETTE.get("error", "#d84545")))
        threading.Thread(target=_worker, daemon=True).start()

    def _admin_render_users(self, users: list):
        """Disegna la lista utenti con dropdown per cambiare piano."""
        import customtkinter as ctk
        try:
            for w in self._admin_users_list_frame.winfo_children():
                w.destroy()
        except Exception:
            return

        if not users:
            self._admin_users_status_lbl.configure(
                text="Nessun utente registrato", text_color=PALETTE["text_dim"])
            return

        self._admin_users_status_lbl.configure(
            text=f"📋  {len(users)} utenti registrati",
            text_color=PALETTE.get("text", "#e8edf2"))

        plan_disp = {"base": "🆓 Base", "pro": "⭐ Pro",
                     "advanced": "💎 Advanced"}
        plan_options = ["base", "pro", "advanced"]

        for u in users:
            row = ctk.CTkFrame(self._admin_users_list_frame,
                               fg_color=PALETTE.get("surface", "#1e2533"),
                               corner_radius=6)
            row.pack(fill="x", pady=3)
            row.columnconfigure(0, weight=1)

            # Info utente (sinistra)
            info = ctk.CTkFrame(row, fg_color="transparent")
            info.grid(row=0, column=0, sticky="ew", padx=10, pady=8)
            badge = "👑 " if u.get("is_admin") else "👤 "
            uname_text = f"{badge}{u['username']}  ·  {u['email']}"
            ctk.CTkLabel(
                info, text=uname_text,
                font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
                text_color=PALETTE["text"], anchor="w"
            ).pack(fill="x")
            sub = f"id={u['id']}"
            if u.get("created_at"):
                sub += f"  ·  Creato {u['created_at'][:10]}"
            if not u.get("is_active", True):
                sub += "  ·  ⚠ Disattivato"
            ctk.CTkLabel(
                info, text=sub,
                font=(FONT_SMALL[0], FONT_SMALL[1]-2),
                text_color=PALETTE["text_dim"], anchor="w"
            ).pack(fill="x", pady=(2, 0))

            # Dropdown piano (destra)
            ctrl = ctk.CTkFrame(row, fg_color="transparent")
            ctrl.grid(row=0, column=1, sticky="e", padx=8, pady=8)
            current_plan = u.get("plan", "base")
            plan_var = ctk.StringVar(value=plan_disp.get(current_plan, current_plan))
            opt = ctk.CTkOptionMenu(
                ctrl, values=[plan_disp[p] for p in plan_options],
                variable=plan_var, width=140, height=30,
                font=(FONT_SMALL[0], FONT_SMALL[1]-1),
                fg_color=PALETTE.get("surface2", "#2a3344"),
                button_color=PALETTE.get("primary", "#3b6fd4"),
                button_hover_color=PALETTE.get("primary_hover", "#2d5ab8"),
                text_color=PALETTE["text"],
                command=lambda choice, uid=u['id'], uname=u['username'],
                              old=current_plan: self._admin_change_user_plan(
                                  uid, uname, old, choice, plan_disp,
                                  plan_options),
            )
            opt.pack(side="left")

    def _admin_change_user_plan(self, user_id: int, username: str,
                                 old_plan: str, choice_display: str,
                                 plan_disp: dict, plan_options: list):
        """Callback dropdown: chiede conferma e chiama /admin/users/{id}/set-plan."""
        from tkinter import messagebox
        import threading
        # Risolvi display → plan_name
        new_plan = None
        for p in plan_options:
            if plan_disp[p] == choice_display:
                new_plan = p
                break
        if new_plan is None or new_plan == old_plan:
            return  # nessun cambio reale

        if not messagebox.askyesno(
                "Conferma cambio piano",
                f"Cambiare il piano di '{username}' da "
                f"{plan_disp.get(old_plan, old_plan)} a "
                f"{plan_disp[new_plan]}?\n\n"
                f"L'utente dovrà rifare login per vedere il nuovo piano."):
            self._admin_refresh_users()  # ripristina dropdown alla scelta vecchia
            return

        def _worker():
            try:
                self.api_client._request(
                    "POST", f"/admin/users/{user_id}/set-plan",
                    json_body={"plan": new_plan})
                self.root.after(0, lambda: (
                    messagebox.showinfo("Aggiornato",
                        f"Piano di '{username}' aggiornato a "
                        f"{plan_disp[new_plan]}."),
                    self._admin_refresh_users(),
                ))
            except Exception as e:
                err_str = str(e)
                self.root.after(0, lambda: (
                    messagebox.showerror("Errore", f"Cambio piano fallito:\n{err_str}"),
                    self._admin_refresh_users(),
                ))
        threading.Thread(target=_worker, daemon=True).start()

    def _admin_refresh_requests(self):
        """Ricarica le richieste pending dal server in thread separato."""
        import threading

        if self.api_client is None:
            return

        # Pulisci lista corrente
        try:
            for w in self._admin_list_frame.winfo_children():
                w.destroy()
        except Exception:
            return
        try:
            self._admin_status_lbl.configure(text="Caricamento richieste…",
                                              text_color=PALETTE["text_dim"])
        except Exception:
            pass

        def _worker():
            try:
                reqs = self.api_client._request("GET", "/admin/upgrade-requests")
                self._safe_after(0, lambda: self._admin_render_list(reqs))
            except Exception as e:
                err_str = str(e)   # v1085c: cattura prima della lambda (Python 3.11+)
                self._safe_after(0, lambda: self._admin_status_lbl.configure(
                    text=f"Errore caricamento: {err_str}",
                    text_color=PALETTE.get("error", "#d84545")))
        threading.Thread(target=_worker, daemon=True).start()

    def _admin_render_list(self, reqs: list):
        """Disegna la lista delle richieste pending con bottoni azione."""
        import customtkinter as ctk

        try:
            for w in self._admin_list_frame.winfo_children():
                w.destroy()
        except Exception:
            return

        if not reqs:
            self._admin_status_lbl.configure(
                text="✓  Nessuna richiesta pending",
                text_color="#50aa70")
            ctk.CTkLabel(
                self._admin_list_frame,
                text="Le richieste di upgrade dei clienti compariranno qui.",
                font=(FONT_SMALL[0], FONT_SMALL[1]-1),
                text_color=PALETTE["text_dim"]
            ).pack(pady=(8, 4))
            return

        self._admin_status_lbl.configure(
            text=f"⚠  {len(reqs)} richiesta/e in attesa",
            text_color=PALETTE.get("primary", "#3b6fd4"))

        plan_disp = {"base": "🆓 Base", "pro": "⭐ Pro", "advanced": "💎 Adv"}
        for r in reqs:
            row = ctk.CTkFrame(self._admin_list_frame,
                               fg_color=PALETTE.get("surface", "#1e2533"),
                               corner_radius=6)
            row.pack(fill="x", pady=3)
            row.columnconfigure(0, weight=1)

            # Info utente + transizione piano
            info = ctk.CTkFrame(row, fg_color="transparent")
            info.grid(row=0, column=0, sticky="ew", padx=10, pady=8)
            # v1085e: rendering robusto a server vecchi che non includono
            # user_id/user_email nello schema. Fallback graceful.
            uname = r.get("user_name") or ""
            uemail = r.get("user_email") or ""
            uid = r.get("user_id")
            if uname and uemail:
                user_label = f"{uname} ({uemail})"
            elif uemail:
                user_label = uemail
            elif uname:
                user_label = uname
            elif uid is not None:
                user_label = f"User #{uid}"
            else:
                user_label = "Utente sconosciuto"
            ctk.CTkLabel(
                info,
                text=f"{user_label}   "
                     f"{plan_disp.get(r.get('from_plan',''), r.get('from_plan','?'))} → "
                     f"{plan_disp.get(r.get('to_plan',''), r.get('to_plan','?'))}",
                font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
                text_color=PALETTE["text"], anchor="w"
            ).pack(fill="x")
            if r.get("message"):
                msg = r["message"]
                if len(msg) > 80:
                    msg = msg[:80] + "…"
                ctk.CTkLabel(
                    info, text=f"💬  {msg}",
                    font=(FONT_SMALL[0], FONT_SMALL[1]-1),
                    text_color=PALETTE["text_dim"], anchor="w",
                    wraplength=600, justify="left"
                ).pack(fill="x", pady=(2, 0))
            created = r.get("created_at", "") or ""
            if created:
                created_disp = str(created)[:19].replace("T", " ")
            else:
                created_disp = "—"
            ctk.CTkLabel(
                info, text=f"📅  {created_disp}",
                font=(FONT_SMALL[0], FONT_SMALL[1]-2),
                text_color=PALETTE["text_dim"], anchor="w"
            ).pack(fill="x", pady=(2, 0))

            # Buttons
            btns = ctk.CTkFrame(row, fg_color="transparent")
            btns.grid(row=0, column=1, sticky="e", padx=8, pady=8)
            ctk.CTkButton(
                btns, text="✓ Approva", width=90, height=30,
                font=(FONT_SMALL[0], FONT_SMALL[1]-1, "bold"),
                fg_color="#50aa70", hover_color="#3d8856",
                text_color="#ffffff",
                command=lambda rid=r["id"]: self._admin_approve(rid),
            ).pack(side="left", padx=2)
            ctk.CTkButton(
                btns, text="✗ Rifiuta", width=90, height=30,
                font=(FONT_SMALL[0], FONT_SMALL[1]-1),
                fg_color="transparent", hover_color="#d84545",
                text_color=PALETTE.get("error", "#d84545"),
                border_width=1, border_color=PALETTE.get("error", "#d84545"),
                command=lambda rid=r["id"]: self._admin_reject(rid),
            ).pack(side="left", padx=2)

    def _admin_approve(self, req_id: int):
        """Approva richiesta + ricarica lista."""
        from tkinter import messagebox, simpledialog
        import threading

        note = simpledialog.askstring(
            "Nota approvazione (opzionale)",
            "Nota interna o messaggio all'utente:",
            parent=self.root)
        # simpledialog.askstring ritorna None se Annulla, "" se OK con campo vuoto
        if note is None:
            return  # cancelled

        def _worker():
            try:
                self.api_client._request(
                    "POST", f"/admin/upgrade-requests/{req_id}/approve",
                    json_body={"admin_note": note or None},
                )
                self.root.after(0, lambda: (
                    messagebox.showinfo(
                        "Approvata",
                        "La richiesta è stata approvata.\n"
                        "L'utente vedrà il nuovo piano al prossimo login."),
                    self._admin_refresh_requests(),
                ))
            except Exception as e:
                err_str = str(e)
                self.root.after(0, lambda: messagebox.showerror(
                    "Errore", f"Approvazione fallita:\n{err_str}"))
        threading.Thread(target=_worker, daemon=True).start()

    def _admin_reject(self, req_id: int):
        """Rifiuta richiesta — chiede motivazione obbligatoria."""
        from tkinter import messagebox, simpledialog
        import threading

        note = simpledialog.askstring(
            "Motivazione rifiuto",
            "Spiega all'utente perché la richiesta è rifiutata:",
            parent=self.root)
        if not note:
            messagebox.showwarning(
                "Motivazione obbligatoria",
                "Inserisci almeno una breve motivazione per il rifiuto.")
            return

        def _worker():
            try:
                self.api_client._request(
                    "POST", f"/admin/upgrade-requests/{req_id}/reject",
                    json_body={"admin_note": note},
                )
                self.root.after(0, lambda: (
                    messagebox.showinfo(
                        "Rifiutata", "La richiesta è stata rifiutata."),
                    self._admin_refresh_requests(),
                ))
            except Exception as e:
                err_str = str(e)
                self.root.after(0, lambda: messagebox.showerror(
                    "Errore", f"Rifiuto fallito:\n{err_str}"))
        threading.Thread(target=_worker, daemon=True).start()


    def _build_advanced_tab(self, parent):
        """v1053: tab Avanzate — tutte le impostazioni non essenziali."""
        scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent")
        scroll.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
        scroll.columnconfigure(0, weight=1)
        self._adv_controls_frame = scroll  # v1065: riferimento per disable durante run

        def section(title, icon_name=""):
            _ic = _get_icon(icon_name, 22) if (icon_name and _ICONS_AVAILABLE) else None
            ctk.CTkLabel(scroll, text=title, font=FONT_HEAD,
                         text_color=PALETTE["text"],
                         image=_ic, compound="left"
                         ).pack(anchor="w", padx=12, pady=(14, 4))
            frm = ctk.CTkFrame(scroll, fg_color=PALETTE["surface2"], corner_radius=8)
            frm.pack(fill="x", padx=8, pady=(0, 8))
            return frm

        def chk(parent, var, text, color=None):
            ctk.CTkCheckBox(
                parent, variable=var, text=text, font=FONT_SMALL,
                text_color=color or PALETTE["text"],
                fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
                checkmark_color=PALETTE["bg"],
            ).pack(anchor="w", padx=16, pady=4)

        # ── v0.0.2.3: Sezione admin (solo se utente è admin) ──────────
        if (self.api_client is not None
                and self.user_info.get("is_admin", False)):
            self._build_admin_section(scroll, section)

        # Classificazione
        frm = section("  Classificazione", icon_name="adv_classify")
        chk(frm, self._opt_dry_run,  "🧪  Modalità Simulazione (dry-run) — non sposta file")
        chk(frm, self._opt_verbose,  "📋  Output Dettagliato (DEBUG nel log)")
        chk(frm, self._opt_classify, "💃  Classifica Salsa per BPM (Easy/Medium/Hard...)")
        chk(frm, self._opt_correct,  "🔧  Correggi Metadati Cartelle Esistenti")
        ctk.CTkFrame(frm, height=4, fg_color="transparent").pack()

        # Sorgenti Metadati in ordine di priorità
        frm2 = section("  Sorgenti Metadati  (ordine = priorità)", icon_name="adv_sources")
        self._sources_adv_frame = frm2  # v1057: riferimento per enable/disable
        ctk.CTkLabel(frm2,
                     text="  L'ordine qui sotto rispecchia la priorità della cascata.\n"
                          "  La prima fonte con genere utile viene usata e le successive saltate.",
                     font=(FONT_SMALL[0], FONT_SMALL[1] - 1),
                     text_color=PALETTE["text_dim"], justify="left",
                     ).pack(anchor="w", padx=16, pady=(6, 4))
        for key, label in [
            ('musicbrainz', "1.  MusicBrainz  — generi precisi, jazz, classica, soundtrack"),
            ('deezer',      "2.  Deezer  — pop/latin, film, generi italiani (free)"),
            ('itunes',      "3.  iTunes Search  — Anime, TV, Classical (free)"),
            ('lastfm',      "4.  Last.fm  — electronic, alternative, indie"),
            ('beatport',    "5.  Beatport  — BPM, genere elettronica"),
            ('getsong',     "6.  GetSong  — BPM alternativo"),
        ]:
            chk(frm2, self._meta_sources[key], label)
        ctk.CTkFrame(frm2, height=1, fg_color=PALETTE["border"]).pack(fill="x", padx=12, pady=(4, 4))
        ctk.CTkLabel(frm2, text="  Con token API (configura in secrets.py):",
                     font=(FONT_SMALL[0], FONT_SMALL[1] - 1), text_color=PALETTE["text_dim"]
                     ).pack(anchor="w", padx=16, pady=(2, 2))
        for key, lbl in [
            ("discogs_enabled",  "7.  Discogs  — jazz/classica/vinili (token gratuito)"),
            # AudD disabilitato — API trial scaduta
            # ("audd_enabled",     "8.  AudD  — fingerprinting audio, 100 req/giorno"),
            ("acoustid_enabled", "9.  AcoustID  — fingerprinting (richiede fpcalc.exe)"),
        ]:
            var = self._meta_sources.setdefault(key, ctk.BooleanVar(value=False))
            chk(frm2, var, lbl, PALETTE["success"])
        ctk.CTkLabel(frm2,
                     text="  discogs.com/settings/developers → Generate token\n"
                          "  audd.io → Sign Up → Dashboard (API disabilitata)\n"
                          "  acoustid.org/api-key  (login MusicBrainz)",
                     font=(FONT_SMALL[0], FONT_SMALL[1] - 1),
                     text_color=PALETTE["text_dim"], justify="left",
                     ).pack(anchor="w", padx=16, pady=(0, 8))

        # Cover Album
        frm3 = section("  Cover Album — Impostazioni Avanzate", icon_name="adv_cover")
        chk(frm3, self._cover_overwrite, "Sovrascrivi cover esistente")
        ctk.CTkLabel(frm3, text="Strategia:", font=FONT_SMALL,
                     text_color=PALETTE["text_dim"]).pack(anchor="w", padx=16, pady=(4, 2))
        for val, label in [('largest', "Usa la più grande in risoluzione"),
                            ('first_available', "Usa la prima disponibile")]:
            ctk.CTkRadioButton(
                frm3, text=label, variable=self._cover_strategy, value=val,
                font=FONT_SMALL, text_color=PALETTE["text"],
                fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
            ).pack(anchor="w", padx=24, pady=2)
        ctk.CTkLabel(frm3, text="Sorgenti:", font=FONT_SMALL,
                     text_color=PALETTE["text_dim"]).pack(anchor="w", padx=16, pady=(6, 2))
        for src, label in [('musicbrainz', "MusicBrainz"), ('lastfm', "Last.fm"),
                            ('deezer', "Deezer"), ('itunes', "iTunes")]:
            chk(frm3, self._cover_sources[src], label)
        ctk.CTkFrame(frm3, height=4, fg_color="transparent").pack()

        # Libreria Locale
        frm4 = section("  Libreria Locale", icon_name="adv_library")
        chk(frm4, self._opt_local_db, "Aggiorna DB locale Generi dopo catalogazione")
        ctk.CTkLabel(frm4,
                     text="  Salva mappatura file→genere in data/music_library.json.\n"
                          "  Permette di rilevare spostamenti manuali al prossimo avvio.",
                     font=(FONT_SMALL[0], FONT_SMALL[1] - 1),
                     text_color=PALETTE["text_dim"], justify="left",
                     ).pack(anchor="w", padx=16, pady=(0, 8))

        # Rinomina File
        frm_rename = section("  Rinomina File Automatico", icon_name="rename")
        ctk.CTkLabel(frm_rename,
            text="Rinomina i file MP3 durante la catalogazione in base al pattern scelto. Default: disabilitato.",
            font=FONT_SMALL, text_color=PALETTE["text_dim"], wraplength=500, justify="left"
        ).pack(padx=12, pady=(8, 4), anchor="w")

        self._opt_rename = ctk.BooleanVar(value=False)
        chk(frm_rename, self._opt_rename, "Abilita rinomina automatica dei file durante la catalogazione")

        rename_pat_frm = ctk.CTkFrame(frm_rename, fg_color="transparent")
        rename_pat_frm.pack(fill="x", padx=12, pady=(4, 12))
        ctk.CTkLabel(rename_pat_frm, text="Pattern:", font=FONT_SMALL,
                     text_color=PALETTE["text_dim"]).pack(side="left", padx=(0, 8))
        self._rename_pattern = ctk.StringVar(value="artista - titolo")
        ctk.CTkSegmentedButton(
            rename_pat_frm,
            values=["artista - titolo", "titolo - artista"],
            variable=self._rename_pattern,
            font=FONT_SMALL,
            fg_color=PALETTE["surface"],
            selected_color=PALETTE["primary"],
            selected_hover_color=PALETTE["primary_hover"],
            unselected_color=PALETTE["surface"],
            unselected_hover_color=PALETTE["bg"],
            text_color=PALETTE["text"],
            width=280,
        ).pack(side="left")
        ctk.CTkLabel(rename_pat_frm, text=".mp3",
                     font=FONT_SMALL, text_color=PALETTE["text_dim"]
                     ).pack(side="left", padx=(4, 0))

        # Manutenzione
        frm5 = section("  Manutenzione", icon_name="advanced2")
        ctk.CTkLabel(frm5,
                     text="  Strumenti per la gestione e pulizia della collezione musicale.",
                     font=(FONT_SMALL[0], FONT_SMALL[1]-1), text_color=PALETTE["text_dim"]
                     ).pack(anchor="w", padx=16, pady=(4, 6))
        # Layout 2 colonne, 4 righe per i tool di manutenzione
        btn_cfg = dict(font=FONT_SMALL, fg_color=PALETTE["surface2"],
                       hover_color=PALETTE["primary"], text_color=PALETTE["text"],
                       anchor="w", width=240, height=32)
        _tools = [
            ("csv",          "Esporta CSV",         self._maint_export_csv),
            ("find_dups",    "Trova Duplicati",      self._maint_find_duplicates),
            ("svuota_cache", "Svuota Cache",         self._clear_cache),
            ("open_folder",       "Apri Cartella Dati",   lambda: __import__("subprocess").Popen(
                                                         ["explorer", str(_get_data_dir())])),
            ("m3u",          "Playlist M3U",         self._maint_export_m3u),
            ("rinomina_b",   "Rinomina Batch",       self._maint_batch_rename),
            ("replaygain",   "Normalizza Volume",    self._maint_replaygain),
            ("integrity",    "Verifica Integrità",   self._maint_check_integrity),
            # v1077: spostati dal menu Strumenti (Opzione C — menu bar rimossa)
            ("log",          "Apri Cartella Log",    self._open_log_folder),
            ("settings",     "Test Configurazione",  self._test_config),
        ]
        grid_maint = ctk.CTkFrame(frm5, fg_color="transparent")
        grid_maint.pack(fill="x", padx=12, pady=(4, 12))
        for i, (icon_name, label, cmd) in enumerate(_tools):
            row, col = divmod(i, 2)
            _ic = _get_icon(icon_name, 28) if _ICONS_AVAILABLE else None
            ctk.CTkButton(grid_maint, text=f"  {label}", command=cmd,
                          image=_ic, compound="left", **btn_cfg,
                          ).grid(row=row, column=col, padx=4, pady=3, sticky="ew")
        grid_maint.columnconfigure(0, weight=1)
        grid_maint.columnconfigure(1, weight=1)

    def _maint_export_m3u(self):
        """Esporta playlist M3U per ogni genere trovato nella directory musicale."""
        from tkinter import filedialog
        path = self._selected_path.get().strip()
        if not path or not Path(path).is_dir():
            messagebox.showwarning("Attenzione", "Seleziona prima una directory musicale.")
            return
        out_dir = filedialog.askdirectory(title="Cartella dove salvare le playlist M3U")
        if not out_dir:
            return
        base = Path(path)
        out = Path(out_dir)
        playlists: dict = {}
        for mp3 in base.rglob("*.mp3"):
            rel = mp3.relative_to(base)
            genre = rel.parts[0] if len(rel.parts) > 1 else "Root"
            playlists.setdefault(genre, []).append(str(mp3))
        created = 0
        for genre, files in playlists.items():
            m3u_path = out / f"{genre}.m3u"
            lines = ["#EXTM3U"] + files
            m3u_path.write_text("\n".join(lines), encoding="utf-8")
            created += 1
        messagebox.showinfo("M3U", f"Create {created} playlist in:\n{out_dir}")

    def _maint_batch_rename(self):
        """Rinomina batch con pattern personalizzato."""
        path = self._selected_path.get().strip()
        if not path or not Path(path).is_dir():
            messagebox.showwarning("Attenzione", "Seleziona prima una directory musicale.")
            return
        win = ctk.CTkToplevel(self.root)
        win.title("Rinomina Batch")
        self._set_win_icon(win)
        self._center_win(win, 500, 300)
        win.grab_set()
        ctk.CTkLabel(win, text="Pattern di rinomina:",
                     font=FONT_SMALL).pack(pady=(16,4))
        ctk.CTkLabel(win, text="Variabili: {title} {artist} {album} {year} {bpm}",
                     font=(FONT_SMALL[0], FONT_SMALL[1]-1),
                     text_color=PALETTE["text_dim"]).pack()
        pat_var = ctk.StringVar(value="{artist} - {title}")
        ctk.CTkEntry(win, textvariable=pat_var, width=380,
                     font=FONT_SMALL).pack(pady=8)
        ctk.CTkLabel(win, text="Applica a cartella (vuoto = tutte):",
                     font=FONT_SMALL).pack(pady=(8,4))
        folder_var = ctk.StringVar()
        ctk.CTkEntry(win, textvariable=folder_var, width=380,
                     font=FONT_SMALL, placeholder_text="es. Latin/Salsa").pack()
        def _do_rename():
            import re as _re
            base = Path(path)
            pattern = pat_var.get().strip()
            folder_filter = folder_var.get().strip()
            count = 0
            errors = 0
            for mp3 in base.rglob("*.mp3"):
                if folder_filter:
                    rel = str(mp3.relative_to(base).parent)
                    if folder_filter.lower() not in rel.lower():
                        continue
                try:
                    import eyed3 as _e3
                    af = _e3.load(str(mp3))
                    if not af or not af.tag:
                        continue
                    new_name = pattern
                    new_name = new_name.replace("{title}", af.tag.title or "")
                    new_name = new_name.replace("{artist}", af.tag.artist or "")
                    new_name = new_name.replace("{album}", af.tag.album or "")
                    new_name = new_name.replace("{year}", str(af.tag.recording_date or ""))
                    new_name = new_name.replace("{bpm}", str(af.tag.bpm or ""))
                    new_name = _re.sub(r'[<>:"/\\|?*]', '', new_name).strip() + ".mp3"
                    dest = mp3.parent / new_name
                    if dest != mp3 and not dest.exists():
                        mp3.rename(dest)
                        count += 1
                except Exception:
                    errors += 1
            win.destroy()
            messagebox.showinfo("Rinomina", f"Rinominati {count} file. Errori: {errors}")
        ctk.CTkButton(win, text="✓  Applica Rinomina",
                      fg_color=PALETTE["primary"],
                      hover_color=PALETTE["primary_hover"],
                      font=FONT_SMALL, command=_do_rename).pack(pady=16)

    def _maint_replaygain(self):
        """Applica ReplayGain ai file MP3 usando mutagen."""
        path = self._selected_path.get().strip()
        if not path:
            messagebox.showwarning("Attenzione", "Seleziona prima una directory musicale.")
            return
        if not messagebox.askyesno("ReplayGain",
                "Analizza e applica ReplayGain a tutti i file MP3?\n"
                "Questa operazione modifica i tag dei file — potrebbe richiedere tempo."):
            return
        import threading
        def _rg_thread():
            try:
                import subprocess as _sp
                result = _sp.run(
                    ["mp3gain", "-r", "-k", "-s", "i", "-d", "0",
                     str(Path(path))],
                    capture_output=True, text=True
                )
                if result.returncode == 0:
                    self.root.after(0, lambda: messagebox.showinfo("ReplayGain",
                        "ReplayGain applicato con successo."))
                else:
                    # Fallback: usa mutagen per scrivere solo il tag RVA2
                    self.root.after(0, lambda: messagebox.showinfo("ReplayGain",
                        "mp3gain non trovato. Installa mp3gain per la normalizzazione\n"
                        "oppure usa: pip install mutagen"))
            except FileNotFoundError:
                self.root.after(0, lambda: messagebox.showwarning("ReplayGain",
                    "mp3gain non trovato nel PATH.\n"
                    "Scarica da: https://mp3gain.sourceforge.net/"))
        threading.Thread(target=_rg_thread, daemon=True).start()

    def _maint_check_integrity(self):
        """Verifica integrità dei file MP3 cercando frame corrotti."""
        path = self._selected_path.get().strip()
        if not path or not Path(path).is_dir():
            messagebox.showwarning("Attenzione", "Seleziona prima una directory musicale.")
            return
        import threading
        def _check_thread():
            base = Path(path)
            corrupted = []
            files = list(base.rglob("*.mp3"))
            try:
                from mutagen.mp3 import MP3 as _MP3
                for i, fp in enumerate(files):
                    try:
                        audio = _MP3(str(fp))
                        if not audio.info:
                            corrupted.append(fp.name)
                    except Exception:
                        corrupted.append(fp.name)
            except ImportError:
                self.root.after(0, lambda: messagebox.showerror("Errore",
                    "mutagen non installato: pip install mutagen"))
                return
            if corrupted:
                msg = f"Trovati {len(corrupted)} file corrotti o problematici:\n"
                msg += "\n".join(f"  \u2022 {f}" for f in corrupted[:20])
                if len(corrupted) > 20:
                    msg += f"\n  ...e altri {len(corrupted)-20}"
            else:
                msg = f"✓ Tutti i {len(files)} file MP3 sono integri."
            self.root.after(0, lambda: messagebox.showinfo("Integrità MP3", msg))
        threading.Thread(target=_check_thread, daemon=True).start()
        messagebox.showinfo("Verifica", "Analisi avviata in background... attendere.")

    def _refresh_cache_info(self):
        """Aggiorna le info sulla dimensione della cache."""
        try:
            sd = _get_data_dir()
            cache_file = sd / "metadata_cache.json"
            if cache_file.exists():
                import json as _json
                data = _json.loads(cache_file.read_text(encoding="utf-8"))
                n = len(data.get("metadata_cache", {}))
                kb = cache_file.stat().st_size // 1024
                self._cache_info_var.set(f"{n} voci  ({kb} KB)")
            else:
                self._cache_info_var.set("Cache vuota")
        except Exception:
            self._cache_info_var.set("")

    def _clear_cache(self):
        """Svuota il file metadata_cache.json."""
        if not messagebox.askyesno("Conferma", "Svuotare la cache API?\nLe prossime catalogazioni saranno più lente."):
            return
        try:
            sd = _get_data_dir()
            cache_file = sd / "metadata_cache.json"
            if cache_file.exists():
                import json as _json
                cache_file.write_text(
                    _json.dumps({"metadata_cache": {}, "genre_cache": {}}, indent=2),
                    encoding="utf-8"
                )
            if hasattr(self, "_cache_info_var"):
                self._refresh_cache_info()
            self._cache_reload()
            messagebox.showinfo("Cache", "Cache svuotata con successo.")
        except Exception as e:
            messagebox.showerror("Errore", f"Impossibile svuotare la cache:\n{e}")

    def _build_stat_cards(self, parent):
        frm = ctk.CTkFrame(parent, fg_color="transparent")
        frm.grid(row=0, column=0, padx=20, pady=(20, 12), sticky="ew")
        for i in range(5):
            frm.columnconfigure(i, weight=1)

        self._card_processed = StatCard(frm, "🎵", "Processati",  icon_name="processati")
        self._card_moved      = StatCard(frm, "📂", "Spostati",    icon_name="spostati")
        self._card_updated    = StatCard(frm, "✏️", "Aggiornati",  icon_name="aggiornati")
        self._card_covers     = StatCard(frm, "🖼️", "Cover",       icon_name="cover_stat")
        self._card_uncatalog  = StatCard(frm, "⚠️", "Non Cat.",    icon_name="non_cat")

        for col, card in enumerate([
            self._card_processed, self._card_moved,
            self._card_updated, self._card_covers, self._card_uncatalog
        ]):
            card.grid(row=0, column=col, padx=3, sticky="ew")

    # ─── LOGICA ──────────────────────────────────────────────────────────

    def _browse(self):
        path = filedialog.askdirectory(title="Seleziona cartella musicale")
        if path:
            self._select_path(path)

    def _select_path(self, path: str) -> None:
        """v1068: seleziona una directory, aggiorna breadcrumb e storico."""
        self._selected_path.set(path)
        self._status_var.set(f"📁  {Path(path).name}")
        self._add_recent_dir(path)
        self._update_breadcrumb(path)

    def _update_breadcrumb(self, path_str: str):
        """v1068: aggiorna il breadcrumb stile Windows Explorer."""
        try:
            parts = Path(path_str).parts
            shown = list(parts[-4:]) if len(parts) > 4 else list(parts)
            # Rimuovi lettera drive (es. "C:\")
            if shown and len(shown[0]) <= 3:
                shown = shown[1:]
            if len(parts) > 4:
                shown = ["..."] + shown
            breadcrumb = "  ›  ".join(shown)
            if hasattr(self, "_breadcrumb_lbl"):
                self._breadcrumb_lbl.configure(
                    text=f"  {breadcrumb}",
                    text_color=PALETTE["text"]
                )
        except Exception:
            pass

    def _clear_recent(self) -> None:
        """v1038: cancella lo storico directory."""
        self._recent_dirs.clear()
        self._save_recent_dirs()
        self._refresh_recent_menu()

    def _build_command(self, path: str) -> list:
        """Costruisce il comando con tutti i parametri.

        v1085e: in modalità EXE PyInstaller, `sys.executable` è il path
        dell'EXE GUI stesso, NON il python interpreter. Lanciare
        Popen([sys.executable, "run_cataloger.py", ...]) faceva partire
        una seconda istanza della GUI che bloccava la prima.

        Soluzione: in modalità EXE usiamo il flag `--cataloger-mode` come
        primo argomento. `run_gui.py` al boot intercetta questo flag e,
        invece di costruire la GUI, esegue `run_cataloger.py:main()`
        direttamente. In modalità script (sviluppo) invochiamo
        `sys.executable run_cataloger.py` come prima.
        """
        if getattr(sys, "frozen", False):
            # EXE PyInstaller: rilancio me stesso con flag speciale
            cmd = [sys.executable, "--cataloger-mode", path]
        else:
            # Modalità script: python interpreter + run_cataloger.py
            script = Path(__file__).parent.parent / "run_cataloger.py"
            cmd = [sys.executable, str(script), path]

        if self._opt_dry_run.get():    cmd.append("--dry-run")
        if self._opt_verbose.get():    cmd.append("-v")
        if not self._opt_use_ext_db.get(): cmd.append("--no-external")
        if self._opt_cleanup.get():    cmd.append("--cleanup")
        if self._opt_correct.get():    cmd.append("--correct-folders")
        if self._opt_classify.get():   cmd.append("--classify-salsa")
        if self._opt_analyze.get():    cmd.append("--analyze-only")

        # v1068: rinomina file se abilitata
        if getattr(self, "_opt_rename", None) and self._opt_rename.get():
            pattern = getattr(self, "_rename_pattern", None)
            if pattern:
                pat = pattern.get()
                if pat == "artista - titolo":
                    cmd += ["--rename-pattern", "{artist} - {title}"]
                else:
                    cmd += ["--rename-pattern", "{title} - {artist}"]

        # v1071b: generi disabilitati nelle prefs + subgeneri storicamente rimossi dal GENRE_TREE
        # (tropical, bolero, mambo, vallenato, salsa choke, bachata sensual, bachata influence)
        # vengono sempre esclusi perché non hanno più una voce nel GENRE_TREE
        _always_excluded = [
            "Tropical", "Bolero", "Mambo", "Vallenato",
            "Salsa Choke", "Bachata Sensual", "Bachata Influence",
            "Salsa Romantica",
        ]
        excluded = list(_always_excluded)  # parti dagli always-excluded
        for macro_key_full, macro_data in self._GENRE_TREE.items():
            mk = macro_key_full.split("  ", 1)[-1].strip()
            for sub, _ in macro_data.get("subgenres", []):
                pref_key = f"{mk}::{sub}"
                if not self._genre_prefs.get(pref_key, True):
                    if sub not in excluded:
                        excluded.append(sub)
        if excluded:
            cmd += ["--excluded-genres"] + excluded

        # Duplicati
        cmd += ["--duplicate-action", self._dup_action.get()]

        # Cover
        if self._cover_enabled.get():
            cmd.append("--cover")
            cmd += ["--cover-strategy", self._cover_strategy.get()]
            if self._cover_overwrite.get():
                cmd.append("--cover-overwrite")
            sources = [s for s, v in self._cover_sources.items() if v.get()]
            if sources:
                cmd += ["--cover-sources"] + sources
        else:
            cmd.append("--no-cover")

        # DB locale
        if self._opt_local_db.get():
            cmd.append("--update-local-db")

        return cmd

    def _set_controls_state(self, state: str):
        """v1057: abilita/disabilita i controlli del pannello sinistro durante il run."""
        def _toggle(widget):
            try:
                widget.configure(state=state)
            except Exception:
                pass
            for child in widget.winfo_children():
                _toggle(child)

        # Disabilita le sezioni del pannello sinistro (salvate in _build_left_panel)
        for frame_attr in ['_left_dir_frame', '_left_options_frame',
                           '_left_dup_frame', '_left_cover_frame']:
            frm = getattr(self, frame_attr, None)
            if frm:
                _toggle(frm)
        # v1065: disabilita anche tab Avanzate durante il run
        if hasattr(self, '_adv_controls_frame'):
            _toggle(self._adv_controls_frame)

    def _run(self):
        path = self._selected_path.get().strip()
        if not path:
            messagebox.showwarning("Attenzione", "Seleziona prima una directory musicale.")
            return
        if not Path(path).is_dir():
            messagebox.showerror("Errore", f"La directory non esiste:\n{path}")
            return

        # ── v0.0.2.4: Tracking server-side della catalogazione ──
        # Se siamo in modalità connessa al server, prima di partire
        # contiamo rapidamente i file MP3 per:
        #   1. applicare le quote del piano (max_files_per_run)
        #   2. mostrare la barra progresso correttamente
        #   3. registrare il Job lato server
        self._reporter = None
        if self.api_client is not None:
            from services.catalog_reporter import CatalogReporter
            # Quick scan ricorsivo .mp3 (veloce anche per 50k file)
            try:
                files_total = sum(1 for _ in Path(path).rglob("*.mp3"))
            except Exception:
                files_total = 0

            # Costruisci dict opzioni come visto dal server (per validazione piano)
            opts = {
                "dry_run":         bool(self._opt_dry_run.get()),
                "cleanup_empty":   bool(self._opt_cleanup.get()),
                "use_external_db": bool(self._opt_use_ext_db.get()),
                "analyze_bpm":     bool(getattr(self, "_opt_analyze_bpm",
                                                 self._opt_classify).get()),
                "fetch_cover":     bool(self._cover_enabled.get()),
                "correct_folders": bool(self._opt_correct.get()),
                "classify_salsa":  bool(self._opt_classify.get()),
                "duplicate_action": str(self._dup_action.get()),
            }

            self._reporter = CatalogReporter(self.api_client)
            job_id = self._reporter.start(
                path=path, files_total=files_total, options=opts)

            if job_id is None:
                # Server ha rifiutato (quota / piano) o è offline.
                err = self._reporter.last_error or ""
                # Se è un errore di server irraggiungibile, lascio
                # andare: catalogazione locale comunque funziona.
                if err and ("non raggiungibile" not in err.lower()):
                    messagebox.showerror("Catalogazione bloccata", err)
                    return
                # Server offline → degrade graceful: cataloga senza tracking
                self._reporter = None

        self._is_running = True
        self._btn_run.configure(state="disabled")
        self._btn_stop.configure(state="normal")
        self._set_controls_state("disabled")  # v1057: greyout pannello sinistro
        self._progress.reset()
        self._n_proc        = 0
        self._n_total       = 0
        self._n_proc_salsa  = 0
        self._n_total_salsa = 0
        self._phase         = 'catalogazione'
        self._last_genre_stats = {}   # v1048: reset per dialog orfani
        self._card_processed.set(0)
        self._card_moved.set(0)
        self._card_updated.set(0)
        self._card_covers.set(0)
        self._card_uncatalog.set(0)
        self._status_var.set("⏳  Catalogazione in corso...")

        self._log.append("=" * 60, "INFO")
        self._log.append(f"▶  Avvio — {datetime.now().strftime('%H:%M:%S')}", "SUCCESS")
        self._log.append(f"   Duplicati: {self._dup_action.get()} | Cover: {'ON' if self._cover_enabled.get() else 'OFF'}", "DEBUG")
        if self._reporter is not None and self._reporter.job_id is not None:
            self._log.append(f"   Server tracking attivo (job #{self._reporter.job_id})",
                             "DEBUG")
            qr = self._reporter.quota_remaining
            if qr >= 0:
                self._log.append(f"   Quota giornaliera: {qr} catalogazioni rimaste oggi",
                                 "DEBUG")
        self._log.append("=" * 60, "INFO")

        cmd = self._build_command(path)
        self._log.append(f"Opzioni: {' '.join(cmd[3:])}", "DEBUG")

        thread = threading.Thread(target=self._run_process, args=(cmd,), daemon=True)
        thread.start()

    def _run_process(self, cmd):
        try:
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace",
                cwd=str(Path(__file__).parent.parent),
            )
            for line in self._process.stdout:
                line = line.rstrip()
                if line:
                    self._log_queue.put(("log", line))
            self._process.wait()
            self._log_queue.put(("done", self._process.returncode))
        except Exception as e:
            self._log_queue.put(("error", str(e)))

    def _stop(self):
        if self._is_running and self._process:
            if messagebox.askyesno("Conferma", "Interrompere il processo in corso?"):
                try:
                    self._process.terminate()
                except Exception:
                    pass
                self._log_queue.put(("aborted", None))

    def _poll_queue(self):
        try:
            while True:
                item = self._log_queue.get_nowait()
                kind = item[0]

                if kind == "log":
                    line = item[1]
                    # v1045/v1048: token interni processati ma NON mostrati nel log
                    _INTERNAL_TOKENS = (
                        "PROGRESS:", "TOTAL:", "ETA:", "MOVED:", "UNCATALOGED:",
                        "GENRE_STATS:",   # v1048: statistiche generi per dialog orfani
                    )
                    self._parse_stats(line)
                    if not any(line.startswith(tok) for tok in _INTERNAL_TOKENS):
                        level = self._classify_line(line)
                        self._log.append(line, level)
                        # v0.0.2.4: invio log al server per tracking
                        if self._reporter is not None:
                            srv_level = "INFO"
                            if level == "ERROR":   srv_level = "ERROR"
                            elif level == "WARNING": srv_level = "WARNING"
                            elif level == "DEBUG":   srv_level = "DEBUG"
                            pct = 0
                            if self._n_total > 0:
                                pct = min(100, int(self._n_proc * 100 / self._n_total))
                            self._reporter.progress(
                                files_done=self._n_proc,
                                progress_pct=pct,
                                files_total=self._n_total or None,
                                log_chunk=line,
                                log_level=srv_level,
                            )

                elif kind == "done":
                    rc = item[1]
                    # v0.0.2.4: notifica fine al server
                    if self._reporter is not None:
                        if rc == 0:
                            report = {
                                "processed":     self._card_processed.get(),
                                "moved":         self._card_moved.get(),
                                "updated":       self._card_updated.get(),
                                "covers_added":  self._card_covers.get(),
                                "uncategorized": self._card_uncatalog.get(),
                            }
                            self._reporter.complete(
                                files_done=self._n_proc, report=report)
                        else:
                            self._reporter.fail(f"Exit code {rc}")
                    self._finish(rc == 0)

                elif kind == "aborted":
                    self._log.append("\n⚠️  PROCESSO INTERROTTO", "WARNING")
                    if self._reporter is not None:
                        self._reporter.cancel()
                    self._finish(success=False, aborted=True)

                elif kind == "error":
                    self._log.append(f"\n✗  ERRORE: {item[1]}", "ERROR")
                    if self._reporter is not None:
                        self._reporter.fail(str(item[1]))
                    self._finish(success=False)

        except queue.Empty:
            pass
        self.root.after(80, self._poll_queue)

    def _classify_line(self, line: str) -> str:
        # v1029: ERROR prima di WARNING per evitare che errori vengano colorati giallo
        ll = line.lower()
        if " - error" in ll or "errore" in ll or "✗" in line or "error:" in ll:
            return "ERROR"
        if " - warning" in ll or "avviso" in ll or "⚠" in line or "warning:" in ll:
            return "WARNING"
        if " - debug" in ll:
            return "DEBUG"
        if "completat" in ll or "✓" in line or "successo" in ll:
            return "SUCCESS"
        return "INFO"

    def _parse_stats(self, line: str):
        """v1033-1034-1048: estrae statistiche dal log in tempo reale — token e pattern."""
        ll = line.lower()

        # ── Token speciali emessi dal cataloger (v1031-1033-1048) ────────────
        # Questi sono la fonte primaria — precisi e senza ambiguità

        if line.startswith("GENRE_STATS:"):
            # v1048: statistiche generi per dialog orfani a fine run
            try:
                import json as _json
                payload = line[len("GENRE_STATS:"):].strip()
                self._last_genre_stats = _json.loads(payload)
            except Exception:
                self._last_genre_stats = {}
            return

        if line.startswith("PROGRESS:"):
            # PROGRESS: 42/157
            try:
                parts = line.split(":")[1].strip().split("/")
                cur, tot = int(parts[0]), int(parts[1])
                self._n_proc  = cur
                self._n_total = tot
                fname = self._progress._file_var.get()
                self._progress.update(cur, tot, fname, phase=self._phase)
            except (ValueError, IndexError):
                pass
            return

        if line.startswith("TOTAL:"):
            try:
                self._n_total = int(line.split(":")[1].strip())
            except (ValueError, IndexError):
                pass
            return

        if line.startswith("ETA:"):
            # ETA: 5m30s — aggiorna label dedicata nella progress bar
            eta_str = line.split(":", 1)[1].strip()
            self._progress.set_eta(eta_str)
            return

        if line.startswith("MOVED:"):
            cur = int(self._card_moved._val_var.get())
            self._card_moved.set(cur + 1)
            return

        if line.startswith("UNCATALOGED:"):
            cur = int(self._card_uncatalog._val_var.get())
            self._card_uncatalog.set(cur + 1)
            return

        # ── FASE 1: scan_and_catalog ─────────────────────────────────────────
        m = re.search(r"trovati\s+(\d+)\s+file\s+mp3\s+da\s+elaborare", ll)
        if m:
            self._n_total = int(m.group(1))
            self._n_proc  = 0
            self._phase   = 'catalogazione'
            return

        # "*** filename.mp3 ***" → avanza progress + processati real-time
        if "***" in line and ".mp3" in ll:
            stripped = re.sub(r"^\d{4}-\d{2}-\d{2}.*?- INFO - ", "", line).strip()
            if stripped.startswith("***") and self._phase == "catalogazione":
                self._n_proc += 1
                self._card_processed.set(self._n_proc)
                fname = stripped.replace("*", "").strip()
                if self._n_total > 0:
                    self._progress.update(self._n_proc, self._n_total, fname,
                                          phase="catalogazione")
            return

        # ── FASE 2: correct_existing_folders ─────────────────────────────────
        if "correzione cartelle esistenti" in ll:
            self._phase = "correzione"
            return

        # ── FASE 3: classify_salsa_by_bpm ────────────────────────────────────
        if "classificazione salsa" in ll:
            self._phase = "salsa"
            return

        if self._phase == "salsa":
            m = re.search(r"trovati\s+(\d+)\s+file\s+nella\s+cartella", ll)
            if m:
                self._n_total_salsa = int(m.group(1))
                self._n_proc_salsa  = 0
                return
            if "[simulazione]" in ll and "-> salsa/" in ll:
                self._n_proc_salsa = getattr(self, "_n_proc_salsa", 0) + 1
                n_tot = getattr(self, "_n_total_salsa", 0)
                if n_tot > 0:
                    m2 = re.search(r"\[simulazione\]\s+(.+?\.mp3)", line, re.IGNORECASE)
                    fname = m2.group(1) if m2 else ""
                    self._progress.update(self._n_proc_salsa, n_tot, fname,
                                          phase="classifica_salsa")
                return

        # ── RIEPILOGO FINALE (sovrascrive i contatori real-time) ──────────────
        # Usa questi solo per il riepilogo definitivo, non per real-time
        m = re.search(r"file\s+processati:\s*(\d+)", ll)
        if m:
            self._card_processed.set(m.group(1))
            return

        m = re.search(r"file\s+(?:che\s+sarebbero\s+stati\s+)?spostati:\s*(\d+)", ll)
        if m:
            self._card_moved.set(m.group(1))
            return

        m = re.search(r"metadat[a-z]*\s+aggiornati:\s*(\d+)", ll)
        if m:
            self._card_updated.set(m.group(1))
            return

        m = re.search(r"cover\s+aggiunte:\s*(\d+)", ll)
        if m:
            self._card_covers.set(m.group(1))
            return

        # Non-cat: SOLO dalla riga riepilogativa — MAI dalle righe WARNING singole
        m = re.search(r"^.*non\s+catalogati:\s*(\d+)\s*$", ll)
        if m:
            self._card_uncatalog.set(m.group(1))
            return

        # ── REAL-TIME cover ────────────────────────────────────────────────────
        if ">-- cover:" in ll and "incorporata" in ll:
            cur = int(self._card_covers._val_var.get())
            self._card_covers.set(cur + 1)

    def _finish(self, success: bool, aborted: bool = False):
        self._is_running = False
        self._btn_run.configure(state="normal")
        self._btn_stop.configure(state="disabled")
        self._set_controls_state("normal")  # v1057: riabilita pannello sinistro

        if aborted:
            # v1085f: messaggio warning chiaro che l'utente ha interrotto
            # (non è un errore). In giallo/warning anziché rosso/error.
            self._status_var.set("⚠️  Processo interrotto dall'utente")
            self._progress.reset()
            self._log.append("\n" + "=" * 60, "WARNING")
            self._log.append("⚠  PROCESSO TERMINATO DALL'UTENTE", "WARNING")
            self._log.append("=" * 60, "WARNING")
            return

        if success:
            self._progress.complete()
            self._status_var.set("✓  Completato con successo")
            self._log.append("\n" + "=" * 60, "SUCCESS")
            self._log.append("✓  CATALOGAZIONE COMPLETATA", "SUCCESS")
            self._log.append("=" * 60, "SUCCESS")
            # v1048: dialog orfani prima del semplice "completato"
            self.root.after(200, self._show_orphan_dialog)
        else:
            self._progress.reset()
            self._status_var.set("✗  Completato con errori")
            self._log.append("\n" + "=" * 60, "ERROR")
            self._log.append("✗  PROCESSO TERMINATO CON ERRORI", "ERROR")
            self._log.append("=" * 60, "ERROR")
            messagebox.showerror("Errori", "Il processo è terminato con errori.\n\nControlla il log per i dettagli.")

    def _show_orphan_dialog(self):
        """v1052: dialog interattivo post-catalogazione con generi orfani (<5 file)."""
        SOGLIA = 5
        stats = self._last_genre_stats

        # Mappa: parola chiave nel nome genere → macrogenere padre
        # Solo subgeneri veri — i macrogeneri stessi (Latin, Rock, Pop…) non vengono suggeriti
        _SUB_TO_MACRO = {
            # Latin subgenres → Latin
            "salsa choke": "Latin",  "salsaton": "Latin",  "salsa romantica": "Latin",
            "bachata sensual": "Latin", "bachata fusion": "Latin",
            "bachata dominicana": "Latin",
            "reggaeton": "Latin",    "cumbia": "Latin",    "merengue": "Latin",
            "tropical": "Latin",     "cha cha cha": "Latin", "boogaloo": "Latin",
            "mambo": "Latin",
            # Classical subgenres
            "contemporary classical": "Classical", "orchestral": "Classical",
            "opera": "Classical",    "baroque": "Classical",
            # Other subgenres
            "indie": "Alternative",  "post-punk": "Alternative",
            "heavy metal": "Rock",   "death metal": "Rock", "power metal": "Rock",
            "punk": "Rock",
            "house": "Electronic",   "techno": "Electronic", "trance": "Electronic",
            "ambient": "Electronic", "drum and bass": "Electronic",
            "r&b": "R&B",           "soul": "R&B",
            "blues": "Jazz",         "swing": "Jazz",
            "anime": "Soundtrack",   "tv soundtrack": "Soundtrack",
            "film": "Soundtrack",    "video game": "Soundtrack",
            "hip hop": "Hip Hop",    "rap": "Hip Hop",
            "flamenco": "World",     "reggae": "World", "folk": "World",
            "country pop": "Country",
        }

        # Set dei macrogeneri — NON vengono segnalati come orfani
        _MACRO_GENRES = {
            "latin", "rock", "pop", "classical", "electronic", "r&b", "jazz",
            "world", "soundtrack", "alternative", "metal", "hip hop", "country",
            "vocal", "blues", "indie", "ambient",
        }

        # Filtra: orfani = pochi file E non sono già un macrogenere
        orfani = {
            g: c for g, c in stats.items()
            if c < SOGLIA and g.lower() not in _MACRO_GENRES
        }

        def _get_macro(genre: str):
            """Restituisce il macrogenere suggerito per un subgenere, o None."""
            gl = genre.lower()
            # Match esatto prima, poi parziale
            if gl in _SUB_TO_MACRO:
                return _SUB_TO_MACRO[gl]
            for key, macro in _SUB_TO_MACRO.items():
                if key in gl:
                    return macro
            return None

        def _do_move(genre_name: str, dest_folder: str, base_path: str, row_widget):
            """Sposta fisicamente i file dal genere orfano al macrogenere."""
            import shutil
            base = Path(base_path)
            src = base / genre_name
            dst = base / dest_folder
            if not src.exists():
                messagebox.showerror("Errore", f"Cartella non trovata:\n{src}")
                return
            moved = 0
            dst.mkdir(parents=True, exist_ok=True)
            for f in src.iterdir():
                if f.is_file():
                    target = dst / f.name
                    if target.exists():
                        target = dst / (f.stem + "_moved" + f.suffix)
                    shutil.move(str(f), str(target))
                    moved += 1
            try:
                src.rmdir()   # rimuove solo se vuota
            except OSError:
                pass
            row_widget.destroy()
            messagebox.showinfo("Spostati", f"{moved} file spostati in {dest_folder}/")

        # ── Dialog ───────────────────────────────────────────────────────
        win = ctk.CTkToplevel(self.root)
        win.title("✓  Catalogazione Completata")
        self._set_win_icon(win)               # v1074: icona uniforme (v1076: con retry)
        win.resizable(True, True)
        self._center_win(win, 660, 540)       # v1076: centrato sullo schermo
        win.grab_set()
        win.lift()
        win.focus_force()

        # Intestazione
        header = ctk.CTkFrame(win, fg_color=PALETTE["surface2"], corner_radius=0)
        header.pack(fill="x")
        ctk.CTkLabel(
            header, text="✓  Catalogazione Completata",
            font=("Segoe UI", 16, "bold"), text_color=PALETTE["success"]
        ).pack(pady=(16, 4))
        ctk.CTkLabel(
            header,
            text=f"Processati {self._n_total} file · {len(stats)} generi trovati",
            font=FONT_SMALL, text_color=PALETTE["text_dim"]
        ).pack(pady=(0, 14))

        # Corpo scrollabile
        body = ctk.CTkScrollableFrame(win, fg_color=PALETTE["bg"], corner_radius=0)
        body.pack(fill="both", expand=True, padx=0, pady=0)
        body.columnconfigure(0, weight=1)

        music_dir = self._selected_path.get().strip()

        if orfani:
            ctk.CTkLabel(
                body,
                text=f"⚠️  {len(orfani)} generi con meno di {SOGLIA} file",
                font=FONT_HEAD, text_color=PALETTE["warning"]
            ).pack(anchor="w", padx=16, pady=(14, 2))
            ctk.CTkLabel(
                body,
                text="Questi generi hanno pochi file. Puoi spostarli nel macrogenere\n"
                     "suggerito con il pulsante Sposta, oppure gestirli manualmente.",
                font=FONT_SMALL, text_color=PALETTE["text_dim"], justify="left"
            ).pack(anchor="w", padx=16, pady=(0, 10))

            for genre, count in sorted(orfani.items(), key=lambda x: x[1]):
                macro = _get_macro(genre)

                row = ctk.CTkFrame(body, fg_color=PALETTE["surface2"], corner_radius=6)
                row.pack(fill="x", padx=12, pady=2)
                row.columnconfigure(1, weight=1)

                ctk.CTkLabel(
                    row, text=f"  📁 {genre}",
                    font=FONT_SMALL, text_color=PALETTE["warning"], anchor="w"
                ).grid(row=0, column=0, padx=8, pady=8, sticky="w")

                ctk.CTkLabel(
                    row, text=f"{count} {'file' if count != 1 else 'file'}",
                    font=FONT_SMALL, text_color=PALETTE["text_dim"], width=55, anchor="w"
                ).grid(row=0, column=1, padx=4, pady=8, sticky="w")

                if macro:
                    # Testo suggerimento
                    ctk.CTkLabel(
                        row, text=f"→ {macro}",
                        font=(FONT_SMALL[0], FONT_SMALL[1] - 1),
                        text_color=PALETTE["primary"], anchor="e",
                    ).grid(row=0, column=2, padx=(4, 6), pady=8, sticky="e")
                    # Pulsante Sposta
                    ctk.CTkButton(
                        row, text="Sposta",
                        width=70, height=26, font=FONT_SMALL,
                        fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
                        command=lambda g=genre, m=macro, r=row:
                            _do_move(g, m, music_dir, r),
                    ).grid(row=0, column=3, padx=(2, 10), pady=8, sticky="e")
                else:
                    ctk.CTkLabel(
                        row, text="valuta manualmente",
                        font=(FONT_SMALL[0], FONT_SMALL[1] - 1),
                        text_color=PALETTE["text_dim"], anchor="e",
                    ).grid(row=0, column=2, padx=(4, 10), pady=8, columnspan=2, sticky="e")
        else:
            ctk.CTkLabel(
                body,
                text="✅  Nessun genere orfano — tutti i generi hanno almeno 5 file.",
                font=FONT_SMALL, text_color=PALETTE["success"]
            ).pack(anchor="w", padx=16, pady=20)

        # Riepilogo top generi
        if stats:
            ctk.CTkFrame(body, height=1, fg_color=PALETTE["border"]).pack(
                fill="x", padx=12, pady=(10, 6))
            ctk.CTkLabel(body, text="📊  Top Generi", font=FONT_HEAD,
                         text_color=PALETTE["text"]
                         ).pack(anchor="w", padx=16, pady=(4, 6))
            top = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:8]
            max_count = top[0][1] if top else 1
            for g, c in top:
                bar_row = ctk.CTkFrame(body, fg_color="transparent")
                bar_row.pack(fill="x", padx=16, pady=2)
                bar_row.columnconfigure(1, weight=1)
                ctk.CTkLabel(bar_row, text=g, font=FONT_SMALL,
                             text_color=PALETTE["text"], width=130, anchor="w"
                             ).grid(row=0, column=0, sticky="w")
                bar_bg = ctk.CTkFrame(bar_row, fg_color=PALETTE["surface2"],
                                      corner_radius=4, height=14)
                bar_bg.grid(row=0, column=1, sticky="ew", padx=(4, 8))
                fill_pct = c / max_count
                bar_fill = ctk.CTkFrame(bar_bg, fg_color=PALETTE["primary"],
                                        corner_radius=4, height=14)
                bar_fill.place(relwidth=fill_pct, relheight=1.0)
                ctk.CTkLabel(bar_row, text=str(c), font=FONT_SMALL,
                             text_color=PALETTE["text_dim"], width=36, anchor="e"
                             ).grid(row=0, column=2, sticky="e")

        # Footer
        footer = ctk.CTkFrame(win, fg_color=PALETTE["surface2"], corner_radius=0)
        footer.pack(fill="x", pady=0)
        ctk.CTkButton(
            footer, text="Chiudi", width=120,
            fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
            font=FONT_SMALL, command=win.destroy,
        ).pack(pady=12)


    # ─── METODI MANUTENZIONE e BINDING ───────────────────────────────────────

    def _on_ext_db_toggle(self):
        """v1057: abilita/disabilita la sezione Sorgenti Metadati in tab Avanzate."""
        enabled = self._opt_use_ext_db.get()
        state = "normal" if enabled else "disabled"
        if not hasattr(self, '_sources_adv_frame'):
            return
        def _set_state(widget):
            try:
                widget.configure(state=state)
            except Exception:
                pass
            for child in widget.winfo_children():
                _set_state(child)
        # Greyout di tutti i widget figli del frame sorgenti
        for child in self._sources_adv_frame.winfo_children():
            _set_state(child)

    def _maint_export_csv(self):
        """v1057: esporta music_library.json in CSV."""
        import json as _json, csv as _csv
        from tkinter import filedialog
        db_path = _get_data_dir() / "music_library.json"
        if not db_path.exists():
            messagebox.showwarning("Attenzione", "Il DB locale non esiste ancora.\nAvvia prima una catalogazione.")
            return
        out = filedialog.asksaveasfilename(
            title="Salva CSV", defaultextension=".csv",
            filetypes=[("CSV", "*.csv"), ("Tutti", "*.*")],
            initialfile="music_library.csv"
        )
        if not out:
            return
        try:
            raw = _json.loads(db_path.read_text(encoding="utf-8"))
            files = raw.get("files", {})
            # Carica anche metadata_cache per arricchire i dati
            meta_cache = {}
            cache_file = _get_data_dir() / "metadata_cache.json"
            if cache_file.exists():
                try:
                    craw = _json.loads(cache_file.read_text(encoding="utf-8"))
                    meta_cache = craw.get("metadata_cache", {})
                except Exception:
                    pass

            def _find_meta(fname_no_ext):
                """Cerca nei metadati cached il record più ricco per quel file."""
                fname_l = fname_no_ext.lower()
                for key, val in meta_cache.items():
                    if val and isinstance(val, dict):
                        title  = (val.get("title") or "").lower()
                        artist = (val.get("artist") or "").lower()
                        if fname_l in title or fname_l in artist or                            (artist and title and f"{artist}" in fname_l):
                            return val
                return {}

            with open(out, "w", newline="", encoding="utf-8-sig") as f:
                w = _csv.writer(f, delimiter=";")
                w.writerow(["File", "Titolo", "Artista", "Album", "Anno",
                            "Genere", "Sottogenere", "BPM", "Qualità (kbps)", "Catalogato il"])
                for rel, info in sorted(files.items()):
                    from pathlib import Path as _P
                    fname = _P(rel).name
                    fname_no_ext = _P(rel).stem
                    meta = _find_meta(fname_no_ext)
                    genre = info.get("genre", "") or ""
                    subgenre = info.get("subgenre", "") or ""
                    subgenre_out = "-" if subgenre.lower() == genre.lower() or not subgenre else subgenre
                    w.writerow([
                        fname,
                        meta.get("title", "") or "",
                        meta.get("artist", "") or "",
                        meta.get("album", "") or "",
                        meta.get("year", "") or "",
                        genre,
                        subgenre_out,
                        info.get("bpm", "") or "",
                        info.get("quality_kbps", "") or "",
                        info.get("cataloged_at", ""),
                    ])
            messagebox.showinfo("Esportazione completata", f"Esportati {len(files)} file in:\n{out}")
        except Exception as e:
            messagebox.showerror("Errore", str(e))

    def _maint_find_duplicates(self):
        """v1057: trova file con nome identico in cartelle diverse."""
        import json as _json
        db_path = _get_data_dir() / "music_library.json"
        if not db_path.exists():
            messagebox.showwarning("Attenzione", "Il DB locale non esiste ancora.")
            return
        try:
            raw = _json.loads(db_path.read_text(encoding="utf-8"))
            files = raw.get("files", {})
            # Raggruppa per nome file
            by_name: dict = {}
            for rel in files:
                fname = Path(rel).name
                by_name.setdefault(fname, []).append(rel)
            dups = {k: v for k, v in by_name.items() if len(v) > 1}
            if not dups:
                messagebox.showinfo("Nessun duplicato", "Nessun file duplicato trovato nel DB locale.")
                return
            # v1077: Radio + Conferma batch — sostituisce i bottoni per-riga.
            # Con 85+ duplicati i click singoli diventavano insostenibili.
            # Ora l'utente seleziona una radio per gruppo, poi un unico
            # pulsante "Conferma selezioni (N)" applica tutto con un solo
            # dialog di conferma.
            import tkinter as _tk

            win = ctk.CTkToplevel(self.root)
            win.title(f"Duplicati trovati — {len(dups)} nomi")
            self._set_win_icon(win)
            self._center_win(win, 720, 560)
            win.grab_set()

            # Header fisso
            hdr = ctk.CTkFrame(win, fg_color=PALETTE["surface2"], corner_radius=0)
            hdr.pack(fill="x")
            ctk.CTkLabel(hdr, text=f"⚠️  {len(dups)} file con nome duplicato",
                         font=(FONT_SMALL[0], 13, "bold"), text_color=PALETTE["text"]
                         ).pack(pady=(14, 4), padx=14, anchor="w")
            ctk.CTkLabel(hdr,
                         text="Seleziona per ciascun gruppo il file da mantenere (gli altri verranno eliminati).",
                         font=FONT_SMALL, text_color=PALETTE["text_dim"]
                         ).pack(pady=(0, 10), padx=14, anchor="w")

            # Body scrollabile
            body = ctk.CTkScrollableFrame(win, fg_color=PALETTE["bg"])
            body.pack(fill="both", expand=True, padx=8, pady=(4, 4))
            body.columnconfigure(0, weight=1)

            # State: per ogni gruppo, una StringVar con il path scelto.
            # Inizializziamo a "" (nessuna scelta) per forzare l'utente a selezionare.
            group_choices = {}   # fname → (StringVar, [paths])

            for i, (fname, paths) in enumerate(sorted(dups.items())):
                bg = PALETTE["surface2"] if i % 2 == 0 else PALETTE["surface"]
                grp = ctk.CTkFrame(body, fg_color=bg, corner_radius=4)
                grp.pack(fill="x", pady=2, padx=2)
                ctk.CTkLabel(grp, text=f"📄  {fname}",
                             font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
                             text_color=PALETTE["text"], anchor="w"
                             ).pack(padx=12, pady=(6, 2), anchor="w")
                var = _tk.StringVar(value="")   # nessuna scelta iniziale
                group_choices[fname] = (var, list(paths))
                for p in paths:
                    sub = ctk.CTkFrame(grp, fg_color="transparent")
                    sub.pack(fill="x", padx=16, pady=1)
                    ctk.CTkRadioButton(
                        sub, text=p, variable=var, value=p,
                        font=(FONT_SMALL[0], FONT_SMALL[1]-1),
                        text_color=PALETTE["text"],
                        # v1078: radio più visibile — contorno chiaro + fill saturo
                        fg_color=PALETTE["primary"],
                        hover_color=PALETTE["primary_hover"],
                        border_color="#ffffff",
                        border_width_checked=5,
                        border_width_unchecked=2,
                        radiobutton_width=18, radiobutton_height=18,
                    ).pack(side="left", padx=4, pady=2, anchor="w")

            # Footer con contatore dinamico + Conferma + Chiudi
            footer = ctk.CTkFrame(win, fg_color=PALETTE["surface2"], corner_radius=0, height=58)
            footer.pack(fill="x", side="bottom")
            footer.pack_propagate(False)

            count_var = ctk.StringVar(value=f"0 di {len(dups)} gruppi selezionati")
            ctk.CTkLabel(footer, textvariable=count_var,
                         font=FONT_SMALL, text_color=PALETTE["text_dim"]
                         ).pack(side="left", padx=16)

            # Aggiorna contatore quando cambia una radio
            # v1079: fix typo v1078 — _v è già la StringVar, non una tupla,
            # quindi .get() diretto (non _v[0].get())
            def _refresh_count(*_):
                n = sum(1 for _v, _ in group_choices.values() if _v.get())
                count_var.set(f"{n} di {len(group_choices)} gruppi selezionati")
            for _var, _paths in group_choices.values():
                _var.trace_add("write", lambda *a: _refresh_count())

            def _apply_batch():
                selections = [(fn, v.get(), pl) for fn, (v, pl) in group_choices.items() if v.get()]
                if not selections:
                    messagebox.showwarning("Nessuna selezione",
                        "Seleziona almeno un file da mantenere prima di confermare.")
                    return
                tot_del = sum(len(pl) - 1 for _, _, pl in selections)
                msg = (f"Applicare le selezioni?\n\n"
                       f"• Mantieni:  {len(selections)} file\n"
                       f"• Elimina:   {tot_del} file\n\n"
                       f"L'azione è irreversibile.")
                if not messagebox.askyesno("Conferma", msg):
                    return
                base = Path(self._selected_path.get().strip())
                errors = []
                deleted_total = 0
                for fname, keep_rel, all_rels in selections:
                    for rel in all_rels:
                        if rel == keep_rel:
                            continue
                        fp = base / rel
                        try:
                            if fp.exists():
                                fp.unlink()
                            if rel in raw["files"]:
                                del raw["files"][rel]
                            deleted_total += 1
                        except Exception as ex:
                            errors.append(f"{rel}: {ex}")
                try:
                    db_path.write_text(_json.dumps(raw, indent=2, ensure_ascii=False),
                                       encoding="utf-8")
                except Exception:
                    pass
                if errors:
                    messagebox.showwarning(
                        "Completato con errori",
                        f"Eliminati {deleted_total} file, {len(errors)} errori:\n\n"
                        + "\n".join(errors[:8]) + ("\n…" if len(errors) > 8 else ""))
                else:
                    messagebox.showinfo(
                        "Completato",
                        f"Eliminati {deleted_total} file.\nDB locale aggiornato.")
                win.destroy()

            ctk.CTkButton(footer, text="✓  Conferma selezioni",
                          fg_color=PALETTE["success"], hover_color="#27ae60",
                          text_color="#ffffff", font=FONT_BODY,
                          height=BTN_H, width=180,
                          command=_apply_batch
                          ).pack(side="right", padx=(6, 16), pady=10)
            ctk.CTkButton(footer, text="Chiudi",
                          fg_color="transparent", hover_color=PALETTE["surface"],
                          text_color=PALETTE["text_dim"], font=FONT_SMALL,
                          height=BTN_H, width=100,
                          command=win.destroy
                          ).pack(side="right", padx=0, pady=10)
        except Exception as e:
            messagebox.showerror("Errore", str(e))

    def _log_apply_filter(self):
        """v1068: riapplica il filtro livello al log completo."""
        self._log.clear()
        lines = getattr(self, "_log_all_lines", [])
        for text, level in lines:
            if self._log_filter.get(level, True):
                self._log.append(text, level)

    def _log_append(self, text: str, level: str = "INFO"):
        """v1068: aggiunge al buffer e al log (rispettando il filtro)."""
        if not hasattr(self, "_log_all_lines"):
            self._log_all_lines = []
        if not hasattr(self, "_log_filter"):
            self._log_filter = {"INFO": True, "WARNING": True, "ERROR": True}
        self._log_all_lines.append((text, level))
        if self._log_filter.get(level, True):
            self._log.append(text, level)

    def _clear_log(self):
        if messagebox.askyesno("Conferma", "Vuoi pulire il log?"):
            self._log.clear()
            self._log_all_lines = []

    # ─── STRUMENTI ───────────────────────────────────────────────────────

    def _test_config(self):
        self._log.append("\n=== TEST CONFIGURAZIONE ===", "INFO")
        try:
            project_root = Path(__file__).parent.parent
            sys.path.insert(0, str(project_root))
            from config.secrets import api_keys
            from config.settings import settings
            validation = api_keys.validate_keys()
            ok = sum(validation.values())
            total = len(validation)
            self._log.append(f"✓  API Keys: {ok}/{total} configurate", "SUCCESS")
            for svc, valid in validation.items():
                icon = "✓" if valid else "✗"
                self._log.append(f"  {icon}  {svc.capitalize()}", "SUCCESS" if valid else "WARNING")
            self._log.append(f"✓  Generi mappati: {len(settings.genre.genre_mapping)}", "SUCCESS")
            self._log.append(f"✓  Livelli Salsa: {len(settings.bpm.difficulty_ranges)}", "SUCCESS")
            self._log.append(f"✓  Artisti Bachata Dom.: {len(settings.bachata.dominicana_artists)}", "SUCCESS")
            from services.external_apis import ExternalAPIs
            from services.bpm_services import BPMServices
            from services.cover_service import CoverService
            from core.metadata_extractor import MetadataExtractor
            from core.genre_classifier import GenreClassifier
            from core.file_manager import FileManager
            from core.cataloger import MusicCataloger
            self._log.append("✓  Tutti i moduli caricati correttamente", "SUCCESS")
            self._log.append("=== TEST OK ===", "SUCCESS")
            messagebox.showinfo("Test OK", f"Configurazione valida!\n\nAPI: {ok}/{total}\nModuli: tutti OK")
        except Exception as e:
            self._log.append(f"✗  ERRORE: {e}", "ERROR")
            messagebox.showerror("Test Fallito", str(e))

    def _open_log_folder(self):
        folder = Path(__file__).parent.parent
        try:
            if os.name == 'nt':
                os.startfile(str(folder))
            else:
                subprocess.run(['xdg-open', str(folder)])
        except Exception as e:
            messagebox.showerror("Errore", str(e))

    def _show_about(self):
        """v1075: About ridisegnata — logo app reale (niente emoji) e testo
        sintetico atemporale. Il dettaglio delle versioni vive in UPGRADES.md.
        v1076: finestra centrata a schermo."""
        win = ctk.CTkToplevel(self.root)
        win.title("About")
        self._set_win_icon(win)                # v1074: icona uniforme (v1076: con retry)
        win.resizable(False, False)
        win.transient(self.root)
        self._center_win(win, 460, 440)        # v1076: centrato sullo schermo
        win.grab_set()

        # ── Logo app (PNG 256) al posto dell'emoji ──────────────────────────
        _logo_done = False
        try:
            from PIL import Image as _PilImg
            logo_path = Path(__file__).parent.parent / "icons" / "app" / "app_icon_256.png"
            if logo_path.exists():
                _img = _PilImg.open(str(logo_path)).convert("RGBA")
                _img = _img.resize((72, 72), _PilImg.LANCZOS)
                _ctk_logo = ctk.CTkImage(light_image=_img, dark_image=_img, size=(72, 72))
                _logo_lbl = ctk.CTkLabel(win, image=_ctk_logo, text="")
                _logo_lbl.pack(pady=(24, 4))
                # Mantieni reference per evitare GC dell'immagine
                win._logo_ref = _ctk_logo
                _logo_done = True
        except Exception:
            pass
        if not _logo_done:
            # Fallback: emoji se il PNG non c'è o PIL non disponibile
            ctk.CTkLabel(win, text="🎵", font=("Segoe UI", 48)).pack(pady=(30, 4))

        ctk.CTkLabel(win, text="Music Cataloger Advanced", font=FONT_TITLE).pack()
        ctk.CTkLabel(win, text=f"{APP_VERSION} — CustomTkinter",
                     font=FONT_SMALL, text_color=PALETTE["text_dim"]).pack(pady=(4, 12))
        ctk.CTkFrame(win, height=1, fg_color=PALETTE["border"]).pack(fill="x", padx=30)

        desc = (
            "Catalogazione automatica di librerie MP3 con focus\n"
            "sulla musica latina da ballo (Salsa e Bachata).\n\n"
            "Classificazione multi-sorgente: filename → ID3 → BPM\n"
            "→ DB online (MusicBrainz, Last.fm, Deezer, iTunes).\n\n"
            "Suddivisione Salsa per velocità (Romantica / Lenta /\n"
            "Media / Veloce / Crazy) e Bachata per stile\n"
            "(Dominicana / Fusion / Sensual).\n\n"
            "Per il changelog completo → UPGRADES.md"
        )
        ctk.CTkLabel(win, text=desc, font=FONT_BODY, justify="center").pack(pady=14)
        ctk.CTkLabel(win, text="© 2026 Pedro Marques — Uso personale ed educativo",
                     font=FONT_SMALL, text_color=PALETTE["text_dim"]).pack(pady=(0, 14))
        ctk.CTkButton(win, text="Chiudi", command=win.destroy, height=BTN_H, width=120).pack()

    def _on_close(self):
        if self._is_running:
            if not messagebox.askyesno("Conferma Uscita",
                                       "Un processo e' in corso.\nEscire comunque?"):
                return
            if self._process:
                try:
                    self._process.terminate()
                except Exception:
                    pass
        self.root.destroy()


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

def main():
    root = ctk.CTk()
    app = MusicCatalogerGUI(root)
    root.update_idletasks()
    sw = root.winfo_screenwidth()
    sh = root.winfo_screenheight()
    w = root.winfo_width()
    h = root.winfo_height()
    root.geometry(f"+{(sw - w) // 2}+{(sh - h) // 2}")
    root.mainloop()


if __name__ == "__main__":
    main()
