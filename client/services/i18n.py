"""
services/i18n.py — Internazionalizzazione UI client (R6.0).

v1091.0: implementazione JSON-based con API gettext-friendly per
una migrazione futura senza dover toccare i call site.

Uso (call site):
    from services.i18n import t
    label = ctk.CTkLabel(parent, text=t("login.btn_login"))
    msg = t("linked_accounts.spotify.detail_connected", user=email)

Init al boot (run_gui.py):
    from services.i18n import init_i18n
    from config.app_config import config as client_config
    init_i18n(client_config.lang)    # None -> autodetect

Convenzioni chiavi:
- Gerarchiche dot-separated: "section.subsection.key"
- camelCase / snake_case da evitare → snake_case con underscore
- placeholders Python style: "{user}", "{count}", interpolati via .format()

Migrazione futura a gettext:
- L'API esterna `t(key, **kwargs)` resta identica.
- Cambia solo l'implementazione interna di `_translate()` per usare
  `gettext.gettext` con domain "tracklab" + locale dir.
- Le chiavi attuali (es. "login.btn_login") diventano msgid in .po
  oppure si mantiene un wrapper key→msgid.
"""
from __future__ import annotations

import json
import locale as _locale_mod
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# Lingue supportate da R6.0. ES e altre verranno aggiunte aggiungendo
# il file translations/<code>.json senza tocchi al codice.
SUPPORTED_LANGS = ("it", "en")
DEFAULT_LANG = "it"      # fallback se autodetect non identifica nulla


def _resolve_translations_dir() -> Path:
    """
    Path della cartella `translations/`, bundle-safe per PyInstaller.

    - PyInstaller onefile/onedir: dentro `sys._MEIPASS/translations`
      (i file vengono bundlati dagli spec via `datas`)
    - Script normale: `client/translations/`
    """
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return Path(meipass) / "translations"
    return Path(__file__).parent.parent / "translations"


def _detect_locale() -> str:
    """
    Autodetect dalla locale OS.

    locale.getlocale() ritorna p.es. ('Italian_Italy', '1252') su
    Windows IT, ('it_IT', 'UTF-8') su Linux. Bastano i primi 2 char.
    """
    try:
        loc = _locale_mod.getlocale()[0] or ""
        loc_low = loc.lower()
        if loc_low.startswith("it") or "italian" in loc_low:
            return "it"
        if loc_low.startswith("en") or "english" in loc_low:
            return "en"
    except Exception as e:
        logger.debug(f"[i18n] _detect_locale fallita: {e}")
    # Default conservativo: italiano (utenti pilot)
    return DEFAULT_LANG


class _I18n:
    """Stato singleton dell'i18n. Caricato una volta al boot."""

    def __init__(self):
        self._lang: str = DEFAULT_LANG
        self._translations: Dict[str, Any] = {}
        # Fallback IT per chiavi mancanti in altre lingue
        self._fallback: Dict[str, Any] = {}

    def init(self, lang: Optional[str] = None) -> str:
        """
        Inizializza l'i18n. Ritorna la lingua effettivamente attiva.

        - lang=None  → autodetect da locale OS, fallback "it"
        - lang valido → usa quella
        - lang non supportato → log warning + fallback "it"
        """
        if lang is None:
            lang = _detect_locale()
            logger.info(f"[i18n] auto-detect locale: {lang}")
        if lang not in SUPPORTED_LANGS:
            logger.warning(
                f"[i18n] lingua {lang!r} non supportata, fallback "
                f"a {DEFAULT_LANG!r}")
            lang = DEFAULT_LANG
        self._lang = lang
        self._translations = self._load_file(lang)
        # Carica IT come fallback se l'utente e' su altra lingua,
        # cosi' chiavi non ancora tradotte non rompono la UI.
        if lang != DEFAULT_LANG:
            self._fallback = self._load_file(DEFAULT_LANG)
        else:
            self._fallback = {}
        logger.info(
            f"[i18n] init OK lang={lang!r} "
            f"keys={self._count_keys(self._translations)}")
        return lang

    @staticmethod
    def _load_file(lang: str) -> Dict[str, Any]:
        path = _resolve_translations_dir() / f"{lang}.json"
        if not path.exists():
            logger.warning(f"[i18n] file traduzioni mancante: {path}")
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            logger.error(f"[i18n] JSON mal formato in {path}: {e}")
            return {}
        except Exception as e:
            logger.error(f"[i18n] impossibile leggere {path}: {e}")
            return {}

    def t(self, key: str, **kwargs) -> str:
        """
        Traduce una chiave. Risolve in ordine:
          1. Tabella della lingua attiva
          2. Tabella fallback (IT) se chiave mancante
          3. La chiave stessa (signal visibile per dev)

        kwargs vengono interpolati via str.format(). Se mancano
        placeholders, ritorna la stringa non interpolata + warning.
        """
        val = self._lookup(self._translations, key)
        if val is None:
            val = self._lookup(self._fallback, key)
        if val is None:
            logger.debug(f"[i18n] chiave mancante: {key!r}")
            return key   # visibile in UI: aiuta a stanare le mancanti
        if kwargs:
            try:
                return val.format(**kwargs)
            except (KeyError, IndexError) as e:
                logger.warning(
                    f"[i18n] placeholder mancante in {key!r}: {e}")
                return val
        return val

    def current_lang(self) -> str:
        return self._lang

    @staticmethod
    def _lookup(d: Dict[str, Any], key: str) -> Optional[str]:
        """Lookup gerarchico: 'login.btn_ok' → d['login']['btn_ok']."""
        node: Any = d
        for part in key.split("."):
            if isinstance(node, dict) and part in node:
                node = node[part]
            else:
                return None
        return node if isinstance(node, str) else None

    @classmethod
    def _count_keys(cls, d: Dict[str, Any], depth: int = 0) -> int:
        """Conta foglie stringa (per logging diagnostico)."""
        n = 0
        for v in d.values():
            if isinstance(v, str):
                n += 1
            elif isinstance(v, dict):
                n += cls._count_keys(v, depth + 1)
        return n


# Singleton globale
_inst = _I18n()


# ── API pubblica ────────────────────────────────────────────────────

def init_i18n(lang: Optional[str] = None) -> str:
    """Init al boot. Vedi _I18n.init()."""
    return _inst.init(lang)


def t(key: str, **kwargs) -> str:
    """Traduzione. Vedi _I18n.t()."""
    return _inst.t(key, **kwargs)


def current_lang() -> str:
    """Lingua attualmente attiva (codice ISO 639-1 lowercase)."""
    return _inst.current_lang()


def supported_langs() -> tuple[str, ...]:
    """Lingue supportate per la UI selettore."""
    return SUPPORTED_LANGS
