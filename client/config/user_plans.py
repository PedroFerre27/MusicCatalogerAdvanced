"""
config/user_plans.py — Music Cataloger Advanced (v1086.7 dev/security-audit)

Definizione dei piani utente con relative feature.

⚠️ CAMBIO IMPORTANTE v1086.7:
- Il default di `_DEFAULT_PLAN` e' ora "base" (era "advanced") — niente
  piu' "dev mode" che dava advanced a chi non aveva fatto login.
- `has_feature()` ora restituisce False di default per feature sconosciute
  (era True). I default permissivi sono stati eliminati ovunque.

⚠️ SICUREZZA:
- Questo dict PLAN_FEATURES e' una "vista" del client per la UI. Non e'
  autoritativo. Il vero gating delle feature avviene server-side: ogni
  endpoint API verifica il plan dal JWT prima di eseguire l'operazione.
- Un attaccante che modifica questo file ottiene SOLO di vedere bottoni
  in piu' nella UI. Quando li clicca, le chiamate al server vengono
  rifiutate con 403 Forbidden.
- I check `has_feature()` lato client servono per "non mostrare quello
  che non e' usabile" — UX, non security. La sicurezza vera e' server.

⚠️ COMPATIBILITA' con server:
- Il dict PLAN_FEATURES qui DEVE essere sincronizzato con quello del
  server (server e' la fonte di verita'). Se il server cambia i piani,
  questo file va aggiornato di conseguenza.
- In futuro: il server potrebbe esporre `/api/v1/plans` per scaricare
  dinamicamente la struttura → niente piu' duplicazione.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import json

# ── Definizione features per piano ──────────────────────────────────────────
#
# NOTA: questa e' una COPIA del dict autoritativo lato server. Se il
# server cambia, qui va sincronizzato. Lo schema dei nomi feature DEVE
# essere identico a quello dei `@require_feature("...")` decorator
# nel server FastAPI.

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
        "tab_quality":   True,
        "tab_cache":     False,
        "tab_advanced":  False,
        "tab_caribbean": False,
        # ── MANUTENZIONE ────────────────────────────────────────────
        "maint_duplicates":   False,
        "maint_integrity":    False,
        "maint_replaygain":   False,
        "maint_batch_rename": False,
        # ── LIMITI USO (enforced server-side via job queue) ─────────
        "max_files_per_run":  500,
        "max_runs_per_day":   3,
    },
    "pro": {
        "catalog_local_only": True,
        "catalog_cleanup":    True,
        "catalog_dryrun":     True,
        "catalog_external_db": True,
        "catalog_cover":       True,
        "catalog_bpm":         True,
        "genre_presets": True,
        "genre_custom":  True,
        "export_csv": True,
        "export_m3u": True,
        "tab_log":       True,
        "tab_db":        True,
        "tab_genres":    True,
        "tab_quality":   True,
        "tab_cache":     True,
        "tab_advanced":  False,
        "tab_caribbean": False,
        "maint_duplicates":   True,
        "maint_integrity":    False,
        "maint_replaygain":   False,
        "maint_batch_rename": False,
        "max_files_per_run":  5000,
        "max_runs_per_day":   20,
    },
    "advanced": {
        "catalog_local_only": True,
        "catalog_cleanup":    True,
        "catalog_dryrun":     True,
        "catalog_external_db": True,
        "catalog_cover":       True,
        "catalog_bpm":         True,
        "genre_presets": True,
        "genre_custom":  True,
        "export_csv": True,
        "export_m3u": True,
        "tab_log":       True,
        "tab_db":        True,
        "tab_genres":    True,
        "tab_quality":   True,
        "tab_cache":     True,
        "tab_advanced":  True,
        "tab_caribbean": True,
        "maint_duplicates":   True,
        "maint_integrity":    True,
        "maint_replaygain":   True,
        "maint_batch_rename": True,
        "max_files_per_run":  -1,
        "max_runs_per_day":   -1,
    },
}

# v1086.7: default = "base" (NON piu' "advanced"). Il default e' usato
# SOLO quando nessuna sessione utente e' attiva (non loggato). Prima
# essendo "advanced", un attaccante poteva cancellare session.json e
# avere accesso a tutte le feature. Ora deve almeno autenticarsi come
# base.
_DEFAULT_PLAN = "base"
_PLAN_FILE = Path(__file__).parent.parent / "data" / "user_plan.json"


@dataclass
class UserPlan:
    """Rappresentazione del piano utente lato client.

    v1086.7 sicurezza: i campi qui sono "view" — la verita' viene
    dal server. Modifiche locali non danno accesso a feature non pagate
    (il server le rifiuta), servono solo a influenzare l'UI.
    """
    plan: str = _DEFAULT_PLAN
    username: str = ""
    email: str = ""

    def has_feature(self, feature: str) -> bool:
        """Controlla se il piano corrente ha accesso a una feature.

        v1086.7: default = False per feature sconosciute. Prima era
        True (permissivo), il che significava che bastava passare un
        feature key invalida per by-passare il check.
        """
        plan_features = PLAN_FEATURES.get(self.plan, PLAN_FEATURES["base"])
        return plan_features.get(feature, False)

    def max_files(self) -> int:
        plan_features = PLAN_FEATURES.get(self.plan, PLAN_FEATURES["base"])
        return plan_features.get("max_files_per_run", 100)

    @property
    def display_name(self) -> str:
        return {
            "base":     "🆓 Base",
            "pro":      "⭐ Pro",
            "advanced": "💎 Advanced",
        }.get(self.plan, self.plan)

    def save(self):
        """Salva il piano corrente nel file locale.

        v1086.7 nota: questo file e' SOLO un cache UX. Il vero plan
        viene letto dal JWT a ogni avvio. Manipolare user_plan.json non
        da' accesso a feature non pagate.
        """
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
        """Carica il piano dal file, default 'base' se non trovato."""
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


def set_plan_from_server(plan: str, username: str = "", email: str = "") -> None:
    """v1086.7: imposta il piano corrente con dati ricevuti dal server
    (post-login). Sostituisce qualsiasi piano caricato dal file locale,
    che e' solo un cache. Il server e' la fonte di verita'.
    """
    global _current_plan
    _current_plan = UserPlan(plan=plan, username=username, email=email)
    _current_plan.save()


def has_feature(feature: str) -> bool:
    """Shortcut: controlla se il piano corrente ha accesso a feature.

    v1086.7: default False (era True). Le chiamate che usavano questo
    metodo con default implicito True ora devono essere esplicite.
    """
    return get_plan().has_feature(feature)


def require_feature(feature: str, parent=None) -> bool:
    """Mostra un dialog di upgrade se la feature non e' disponibile.
    Returns True se la feature e' disponibile, False altrimenti.

    NOTA UX: questo e' un hint visivo. La vera protezione e' che la
    chiamata API verra' comunque rifiutata dal server se l'utente non
    ha il piano richiesto.
    """
    if has_feature(feature):
        return True
    plan = get_plan()
    try:
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
