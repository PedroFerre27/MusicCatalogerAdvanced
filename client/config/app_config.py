"""
config/app_config.py — Configurazione runtime del client (non-codice).

L'URL del server è configurabile via file `data/client_config.json`, creato
al primo avvio con valori di default. L'utente può editarlo a mano o da
una futura "Finestra Impostazioni" nella GUI.

In sviluppo: http://localhost:8000
In produzione: https://api.choros27.synology.me

v1085m: il path della cartella `data/` viene risolto in modo diverso a
seconda della modalità di esecuzione:
  - script normale (.py): accanto al file source (parent del package config)
  - PyInstaller onefile/onedir: accanto all'EXE (`sys.executable`)

Questo è critico in modalità onefile, dove `__file__` punta dentro la
temp dir `_MEI<random>` che viene CANCELLATA ad ogni avvio. Salvare la
config lì significa perderla immediatamente. Per i bundle PyInstaller
saviamo nella stessa cartella dell'EXE (persistente).
"""
from __future__ import annotations
import json
import os
import sys
from dataclasses import dataclass, asdict
from pathlib import Path


def _resolve_data_dir() -> Path:
    """v1085m: ritorna il path della cartella `data/` persistente.

    - PyInstaller bundle (onefile/onedir): accanto all'EXE
    - Script Python: accanto al package config (parent del file source)
    """
    if getattr(sys, "frozen", False):
        # Siamo in EXE PyInstaller — usa la dir dell'EXE
        return Path(sys.executable).parent / "data"
    # Modalità script
    return Path(__file__).parent.parent / "data"


_DATA_DIR = _resolve_data_dir()
CONFIG_FILE = _DATA_DIR / "client_config.json"

# v0.0.2.4: Default produzione (NAS Synology). Per sviluppo locale,
# il dev imposta MCS_SERVER_URL="http://localhost:8020" come env var
# oppure modifica server_url in data/client_config.json al primo avvio.
DEFAULT_SERVER_PROD = "https://api.choros27.synology.me"
DEFAULT_SERVER_DEV  = "http://localhost:8020"


@dataclass
class ClientConfig:
    server_url:       str  = DEFAULT_SERVER_PROD
    offline_ok:       bool = True    # consenti uso offline con last-known plan
    remember_email:   bool = True
    last_email:       str  = ""


def _load_or_create() -> ClientConfig:
    if CONFIG_FILE.exists():
        try:
            data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            cfg = ClientConfig(**{k: v for k, v in data.items()
                                    if k in ClientConfig.__dataclass_fields__})
            # v0.0.2.4: Auto-migrazione config legacy.
            # Se l'utente ha un client_config.json salvato dalle versioni
            # iniziali (server=localhost:8000), lo aggiorniamo al default
            # produzione. Questo evita che utenti su versioni vecchie
            # restino bloccati su localhost dopo l'aggiornamento.
            legacy_urls = (
                "http://localhost:8000",
                "http://localhost",
                "https://localhost:8000",
            )
            if cfg.server_url in legacy_urls:
                print(f"[app_config] Migrazione server_url legacy "
                      f"'{cfg.server_url}' → '{DEFAULT_SERVER_PROD}'")
                cfg.server_url = DEFAULT_SERVER_PROD
                save(cfg)
            return cfg
        except Exception:
            pass
    # default con override da env var (utile per packaging multi-target)
    env_url = os.environ.get("MCS_SERVER_URL")
    cfg = ClientConfig()
    if env_url:
        cfg.server_url = env_url
    save(cfg)
    return cfg


def save(cfg: ClientConfig) -> None:
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(
        json.dumps(asdict(cfg), indent=2, ensure_ascii=False),
        encoding="utf-8"
    )


# Singleton caricato al primo import
config: ClientConfig = _load_or_create()


def reload() -> ClientConfig:
    """Ricarica dal disco — utile dopo che l'utente ha modificato il file."""
    global config
    config = _load_or_create()
    return config
