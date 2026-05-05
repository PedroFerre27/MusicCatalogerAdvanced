"""
user_plans.py — v1081: piani ripensati con logica di business
Base = tutto il core offline, Pro = metadati online + cover + BPM + caribbean,
Advanced = tool di manutenzione catalogo per power user.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import json

# ── Definizione features per piano ──────────────────────────────────────────
#
# Logica della divisione (v1081):
#   BASE       → "funziona offline, organizza la mia musica sul PC".
#                Niente chiamate API online, niente CPU-intensive (BPM),
#                niente maintenance. Zero costo variabile per il server.
#   PRO        → il cuore del valore aggiunto: DB online + BPM + cover +
#                Caribbean classification. Include qualità di vita per
#                utenti attivi (duplicati, M3U, cache).
#   ADVANCED   → tool di maintenance di una libreria già costruita:
#                ReplayGain (CPU-intensive), batch rename, verifica MP3,
#                tab Avanzate con tuning algoritmi. Target: power user.

PLAN_FEATURES = {
    "base": {
        # ── CATALOGAZIONE CORE (tutti i piani) ──────────────────────
        "catalog_local_only": True,     # classificazione da filename+tag
        "catalog_cleanup":    True,     # rimozione cartelle vuote
        "catalog_dryrun":     True,     # modalità Solo Analisi
        # ── METADATI ONLINE E BPM (pro+) ────────────────────────────
        "catalog_external_db": False,
        "catalog_cover":       False,
        "catalog_bpm":         False,
        # ── GENERI ──────────────────────────────────────────────────
        "genre_presets": True,
        "genre_custom":  False,
        # ── EXPORT ──────────────────────────────────────────────────
        "export_csv": False,
        "export_m3u": False,
        # ── TAB ─────────────────────────────────────────────────────
        "tab_log":       True,
        "tab_db":        True,
        "tab_genres":    True,
        "tab_quality":   True,   # bitrate check è utile anche in base
        "tab_cache":     False,
        "tab_advanced":  False,
        "tab_caribbean": False,  # Caribbean è una feature smart → pro
        # ── MANUTENZIONE ────────────────────────────────────────────
        "maint_duplicates":   False,
        "maint_integrity":    False,
        "maint_replaygain":   False,
        "maint_batch_rename": False,
        # ── LIMITI USO ──────────────────────────────────────────────
        "max_files_per_run":  500,
        "max_runs_per_day":   3,
    },
    "pro": {
        # Core
        "catalog_local_only": True,
        "catalog_cleanup":    True,
        "catalog_dryrun":     True,
        # Metadati online + BPM + Cover
        "catalog_external_db": True,
        "catalog_cover":       True,
        "catalog_bpm":         True,
        # Generi
        "genre_presets": True,
        "genre_custom":  True,
        # Export
        "export_csv": True,
        "export_m3u": True,
        # Tab (incluso Cache + Caribbean)
        "tab_log":       True,
        "tab_db":        True,
        "tab_genres":    True,
        "tab_quality":   True,
        "tab_cache":     True,
        "tab_advanced":  False,   # solo Advanced
        "tab_caribbean": False,   # v1085o: spostato in Advanced (era Pro)
        # Maintenance base
        "maint_duplicates":   True,
        "maint_integrity":    False,   # solo Advanced
        "maint_replaygain":   False,   # solo Advanced
        "maint_batch_rename": False,   # solo Advanced
        # Limiti
        "max_files_per_run":  5000,
        "max_runs_per_day":   20,
    },
    "advanced": {
        # Core
        "catalog_local_only": True,
        "catalog_cleanup":    True,
        "catalog_dryrun":     True,
        # Metadati + BPM + Cover
        "catalog_external_db": True,
        "catalog_cover":       True,
        "catalog_bpm":         True,
        # Generi
        "genre_presets": True,
        "genre_custom":  True,
        # Export
        "export_csv": True,
        "export_m3u": True,
        # Tab (tutto)
        "tab_log":       True,
        "tab_db":        True,
        "tab_genres":    True,
        "tab_quality":   True,
        "tab_cache":     True,
        "tab_advanced":  True,
        "tab_caribbean": True,
        # Maintenance completa
        "maint_duplicates":   True,
        "maint_integrity":    True,
        "maint_replaygain":   True,
        "maint_batch_rename": True,
        # Limiti illimitati
        "max_files_per_run":  -1,
        "max_runs_per_day":   -1,
    },
}

# ── Piano di default (sviluppo = advanced) ───────────────────────────────────
_DEFAULT_PLAN = "advanced"
_PLAN_FILE = Path(__file__).parent.parent / "data" / "user_plan.json"


@dataclass
class UserPlan:
    plan: str = _DEFAULT_PLAN
    username: str = "DJ"
    email: str = ""

    def has_feature(self, feature: str) -> bool:
        """Controlla se il piano corrente ha accesso a una feature."""
        plan_features = PLAN_FEATURES.get(self.plan, PLAN_FEATURES[_DEFAULT_PLAN])
        return plan_features.get(feature, False)

    def max_files(self) -> int:
        plan_features = PLAN_FEATURES.get(self.plan, PLAN_FEATURES[_DEFAULT_PLAN])
        return plan_features.get("max_files_per_run", 100)

    @property
    def display_name(self) -> str:
        return {
            "base":     "🆓 Base",
            "pro":      "⭐ Pro",
            "advanced": "💎 Advanced",
        }.get(self.plan, self.plan)

    def save(self):
        """Salva il piano corrente."""
        try:
            _PLAN_FILE.parent.mkdir(parents=True, exist_ok=True)
            _PLAN_FILE.write_text(
                json.dumps({"plan": self.plan, "username": self.username,
                            "email": self.email}, indent=2),
                encoding="utf-8"
            )
        except Exception:
            pass

    @classmethod
    def load(cls) -> "UserPlan":
        """Carica il piano dal file, default advanced se non trovato."""
        try:
            if _PLAN_FILE.exists():
                data = json.loads(_PLAN_FILE.read_text(encoding="utf-8"))
                return cls(**data)
        except Exception:
            pass
        return cls()


# ── Istanza globale ───────────────────────────────────────────────────────────
_current_plan: Optional[UserPlan] = None


def get_plan() -> UserPlan:
    """Restituisce il piano utente corrente (singleton)."""
    global _current_plan
    if _current_plan is None:
        _current_plan = UserPlan.load()
    return _current_plan


def has_feature(feature: str) -> bool:
    """Shortcut: controlla se il piano corrente ha accesso a feature."""
    return get_plan().has_feature(feature)


def require_feature(feature: str, parent=None) -> bool:
    """Mostra un dialog di upgrade se la feature non è disponibile.
    Returns True se la feature è disponibile, False altrimenti.
    """
    if has_feature(feature):
        return True
    plan = get_plan()
    try:
        import customtkinter as ctk
        from tkinter import messagebox
        plan_name = plan.display_name
        messagebox.showinfo(
            "Funzionalità Pro/Advanced",
            f"Questa funzionalità non è disponibile nel piano {plan_name}.\n\n"
            f"Passa a Pro o Advanced per accedervi."
        )
    except Exception:
        pass
    return False
