"""
services/plans.py — Definizione feature per piano

COPIA sincronizzata di client/config/user_plans.py::PLAN_FEATURES.
Il server usa questa tabella per:
1. Inserirla dentro il JWT (il client legge direttamente dal token)
2. Validare le richieste lato server (un client manomesso non può
   abilitare feature pro se ha JWT di piano base)

IMPORTANTE: quando cambi le feature sul client, aggiornare anche qui.
"""

PLAN_FEATURES = {
    "base": {
        "catalog_local_only":  True,
        "catalog_cleanup":     True,
        "catalog_dryrun":      True,
        "catalog_external_db": False,
        "catalog_cover":       False,
        "catalog_bpm":         False,
        "genre_presets":       True,
        "genre_custom":        False,
        "export_csv":          False,
        "export_m3u":          False,
        "tab_log":             True,
        "tab_db":              True,
        "tab_genres":          True,
        "tab_quality":         True,
        "tab_cache":           False,
        "tab_advanced":        False,
        "tab_caribbean":       False,
        "maint_duplicates":    False,
        "maint_integrity":     False,
        "maint_replaygain":    False,
        "maint_batch_rename":  False,
        "max_files_per_run":   500,
        "max_runs_per_day":    3,
    },
    "pro": {
        "catalog_local_only":  True,
        "catalog_cleanup":     True,
        "catalog_dryrun":      True,
        "catalog_external_db": True,
        "catalog_cover":       True,
        "catalog_bpm":         True,
        "genre_presets":       True,
        "genre_custom":        True,
        "export_csv":          True,
        "export_m3u":          True,
        "tab_log":             True,
        "tab_db":              True,
        "tab_genres":          True,
        "tab_quality":         True,
        "tab_cache":           True,
        "tab_advanced":        False,
        "tab_caribbean":       True,
        "maint_duplicates":    True,
        "maint_integrity":     False,
        "maint_replaygain":    False,
        "maint_batch_rename":  False,
        "max_files_per_run":   5000,
        "max_runs_per_day":    20,
    },
    "advanced": {
        "catalog_local_only":  True,
        "catalog_cleanup":     True,
        "catalog_dryrun":      True,
        "catalog_external_db": True,
        "catalog_cover":       True,
        "catalog_bpm":         True,
        "genre_presets":       True,
        "genre_custom":        True,
        "export_csv":          True,
        "export_m3u":          True,
        "tab_log":             True,
        "tab_db":              True,
        "tab_genres":          True,
        "tab_quality":         True,
        "tab_cache":           True,
        "tab_advanced":        True,
        "tab_caribbean":       True,
        "maint_duplicates":    True,
        "maint_integrity":     True,
        "maint_replaygain":    True,
        "maint_batch_rename":  True,
        "max_files_per_run":   -1,
        "max_runs_per_day":    -1,
    },
}

PLAN_DISPLAY_NAMES = {
    "base":     "🆓 Base",
    "pro":      "⭐ Pro",
    "advanced": "💎 Advanced",
}

PLAN_ORDER = ["base", "pro", "advanced"]


def get_features(plan: str) -> dict:
    return PLAN_FEATURES.get(plan, PLAN_FEATURES["base"])


def can_upgrade_to(current: str, target: str) -> bool:
    """True se target è un piano superiore a current."""
    try:
        return PLAN_ORDER.index(target) > PLAN_ORDER.index(current)
    except ValueError:
        return False


def upgrades_available(current: str) -> list[str]:
    """Lista dei piani a cui l'utente può fare upgrade."""
    try:
        i = PLAN_ORDER.index(current)
        return PLAN_ORDER[i + 1:]
    except ValueError:
        return []
