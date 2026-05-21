"""
services/local_db.py — Music Cataloger Advanced v1086.3

DB locale UNIFICATO (v2): un solo file `local_db.json` contiene sia la
mappatura permanente file→metadati (ex music_library.json) sia la cache
delle query API esterne (ex metadata_cache.json).

Schema v2:
    {
      "version": 2,
      "last_updated": "...",
      "files": {
        "<relative_path>": {
          "artist": "...",
          "title": "...",
          "album": "...",
          "genre": "...",       # genere finale assegnato dal cataloger
          "subgenre": "...",
          "bpm": float | null,
          "quality_kbps": int | null,
          "cover_present": bool | null,
          "external_lookup": {  # dati grezzi dalla cache API (era metadata_cache)
            "source": "MusicBrainz" | "iTunes" | ...,
            "raw_genre": "...",
            "raw_bpm": float | null,
            "cached_at": "..."
          },
          "cataloged_at": "..."  # quando il cataloger ha processato il file
        }
      },
      "lookup_by_query": {
        "<artist>|||<title>": "<relative_path>"   # indice secondario
      }
    }

Migration v1 → v2: gestita da `migrate_legacy_to_v2()`. I file legacy
(metadata_cache.json + music_library.json) vengono uniti e poi
rinominati `.migrated_v2`. Nessun dato perso.

Design rationale (v1086.3):
- Un record per brano = single source of truth (no piu' duplicazione)
- `lookup_by_query` e' un indice INVERSO: artist|title → path. Permette
  alla cascata API esterna di chiedere "ho gia' visto questo (artist,
  title)?" senza scansionare tutti i files.
- Strutture compatibili con futuro community DB (path = pivot,
  metadati raggruppabili lato server).
"""
from __future__ import annotations
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)
DB_VERSION = 2  # v1086.3 — schema unificato


def _normalize_query_key(artist: str, title: str) -> str:
    """Normalizza la chiave di lookup `(artist, title)` → str.
    Lower-case, strip, separatore distintivo."""
    a = (artist or "").strip().lower()
    t = (title or "").strip().lower()
    return f"{a}|||{t}"


class LocalDB:
    """
    DB locale unificato v2. Sostituisce LocalMusicDB (v1) e CacheManager.

    Esempio uso:
        db = LocalDB(Path("data/local_db.json"))
        db.load()  # carica da disco se esiste
        db.upsert_file("Beatles - Yesterday.mp3",
                        artist="Beatles", title="Yesterday",
                        genre="Pop", bpm=76, quality_kbps=320)
        cached = db.get_cached_metadata("Beatles", "Yesterday")
        db.cache_external_lookup("Beatles", "Yesterday",
                                  source="MusicBrainz", raw_genre="rock", raw_bpm=76)
        db.save()
    """

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self._data: Dict[str, Any] = {
            "version": DB_VERSION,
            "last_updated": "",
            "files": {},
            "lookup_by_query": {},
        }

    # ─────────────────────────────────────────────────────────────────
    # Persistenza
    # ─────────────────────────────────────────────────────────────────
    def load(self) -> bool:
        """Carica il DB. Ritorna True se caricato, False se file non esiste."""
        if not self.db_path.exists():
            return False
        try:
            with open(self.db_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                logger.warning(f"DB malformato (non e' dict): {self.db_path.name}")
                return False
            self._data = {
                "version": data.get("version", 1),
                "last_updated": data.get("last_updated", ""),
                "files": data.get("files", {}) or {},
                "lookup_by_query": data.get("lookup_by_query", {}) or {},
            }
            logger.info(
                f"DB locale caricato: {len(self._data['files'])} file, "
                f"{len(self._data['lookup_by_query'])} cache entries "
                f"({self.db_path.name})")
            return True
        except Exception as e:
            logger.warning(f"DB locale non leggibile: {e}")
            return False

    def save(self) -> bool:
        """Salva il DB con scrittura atomica (tmp + rename)."""
        try:
            self._data["last_updated"] = datetime.now().isoformat(timespec="seconds")
            self._data["version"] = DB_VERSION
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.db_path.with_suffix(self.db_path.suffix + ".tmp")
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=2)
            tmp_path.replace(self.db_path)
            logger.info(
                f"DB locale salvato: {len(self._data['files'])} file, "
                f"{len(self._data['lookup_by_query'])} cache entries "
                f"→ {self.db_path.name}")
            return True
        except Exception as e:
            logger.error(f"Errore salvataggio DB locale: {e}")
            return False

    # ─────────────────────────────────────────────────────────────────
    # API: gestione record file (ex LocalMusicDB)
    # ─────────────────────────────────────────────────────────────────
    def upsert_file(
        self,
        relative_path: str,
        *,
        artist: Optional[str] = None,
        title: Optional[str] = None,
        album: Optional[str] = None,
        genre: Optional[str] = None,
        subgenre: Optional[str] = None,
        bpm: Optional[float] = None,
        quality_kbps: Optional[int] = None,
        cover_present: Optional[bool] = None,
    ) -> None:
        """
        Inserisce o aggiorna il record per il file `relative_path`.

        Tutti i campi sono opzionali (kwargs). Solo i campi passati
        esplicitamente vengono aggiornati — il resto del record rimane
        invariato. Coercion difensiva su bpm/quality_kbps perche' possono
        arrivare come str dal tag ID3.
        """
        rec = self._data.setdefault("files", {}).setdefault(relative_path, {})

        if artist is not None: rec["artist"] = artist
        if title is not None: rec["title"] = title
        if album is not None: rec["album"] = album
        if genre is not None: rec["genre"] = genre
        if subgenre is not None: rec["subgenre"] = subgenre
        if cover_present is not None: rec["cover_present"] = bool(cover_present)

        if bpm is not None:
            rec["bpm"] = self._coerce_bpm(bpm)

        if quality_kbps is not None:
            rec["quality_kbps"] = self._coerce_kbps(quality_kbps)

        rec["cataloged_at"] = datetime.now().isoformat(timespec="seconds")

        # Aggiorna l'indice secondario lookup_by_query se artist+title sono noti
        if rec.get("artist") and rec.get("title"):
            qk = _normalize_query_key(rec["artist"], rec["title"])
            # Se esisteva un record orfano per questo (artist, title),
            # promuoviamolo: copia external_lookup del record orfano nel
            # record nuovo, poi rimuovi l'orfano.
            old_target = self._data.setdefault("lookup_by_query", {}).get(qk)
            if old_target and old_target != relative_path \
                    and old_target.startswith("__orphan__:"):
                orphan = self._data["files"].pop(old_target, None)
                if orphan and orphan.get("external_lookup"):
                    rec.setdefault("external_lookup", orphan["external_lookup"])
            self._data["lookup_by_query"][qk] = relative_path

    def get_file(self, relative_path: str) -> Optional[Dict]:
        """Ritorna il record per `relative_path`, o None se non trovato."""
        return self._data.get("files", {}).get(relative_path)

    def remove_file(self, relative_path: str) -> bool:
        """Rimuove il record per `relative_path`. Pulisce anche l'indice
        inverso. Ritorna True se qualcosa e' stato rimosso."""
        rec = self._data.get("files", {}).pop(relative_path, None)
        if rec is None:
            return False
        if rec.get("artist") and rec.get("title"):
            qk = _normalize_query_key(rec["artist"], rec["title"])
            existing = self._data.get("lookup_by_query", {}).get(qk)
            if existing == relative_path:
                self._data["lookup_by_query"].pop(qk, None)
        return True

    def all_files(self) -> Dict[str, Dict]:
        """Ritorna il dict completo files. Esclude i record orfani
        (interni alla cache, non visibili nella library)."""
        return {
            p: r for p, r in self._data.get("files", {}).items()
            if not p.startswith("__orphan__:")
        }

    def count_files(self) -> int:
        """Numero di file reali (esclude orfani)."""
        return sum(1 for p in self._data.get("files", {})
                   if not p.startswith("__orphan__:"))

    # ─────────────────────────────────────────────────────────────────
    # API: cache lookup esterno (ex CacheManager)
    # ─────────────────────────────────────────────────────────────────
    def get_cached_metadata(self, artist: str, title: str) -> Optional[Dict]:
        """
        Cerca la cache: hai gia' visto questo (artist, title)?
        Ritorna il dict `external_lookup` del record corrispondente, o None.
        """
        qk = _normalize_query_key(artist, title)
        path = self._data.get("lookup_by_query", {}).get(qk)
        if not path:
            return None
        rec = self._data.get("files", {}).get(path)
        if not rec:
            self._data.get("lookup_by_query", {}).pop(qk, None)
            return None
        return rec.get("external_lookup")

    def cache_external_lookup(
        self,
        artist: str,
        title: str,
        *,
        source: str,
        raw_genre: Optional[str] = None,
        raw_bpm: Optional[float] = None,
        relative_path: Optional[str] = None,
    ) -> None:
        """
        Memorizza in cache l'esito di una query API esterna.

        Se `relative_path` e' fornito, la cache viene attaccata al record
        di quel file. Altrimenti, se esiste gia' un record per (artist,
        title) nell'indice, la cache va li'. Altrimenti, crea un record
        "orfano" indicizzato solo per (artist, title) — sara' linkato a
        un path quando il cataloger processera' il file.
        """
        qk = _normalize_query_key(artist, title)

        target_path = relative_path
        if target_path is None:
            target_path = self._data.get("lookup_by_query", {}).get(qk)

        if target_path is None:
            target_path = f"__orphan__:{qk}"

        rec = self._data.setdefault("files", {}).setdefault(target_path, {})
        rec["artist"] = rec.get("artist") or artist
        rec["title"] = rec.get("title") or title

        bpm_num = self._coerce_bpm(raw_bpm) if raw_bpm is not None else None

        rec["external_lookup"] = {
            "source": source,
            "raw_genre": raw_genre,
            "raw_bpm": bpm_num,
            "cached_at": datetime.now().isoformat(timespec="seconds"),
        }

        self._data.setdefault("lookup_by_query", {})[qk] = target_path

    def cache_count(self) -> int:
        """Numero di entry in cache (= entry in lookup_by_query)."""
        return len(self._data.get("lookup_by_query", {}))

    # ─────────────────────────────────────────────────────────────────
    # Coercion helpers
    # ─────────────────────────────────────────────────────────────────
    @staticmethod
    def _coerce_bpm(bpm: Any) -> Optional[float]:
        try:
            v = float(bpm)
            return round(v, 1) if v > 0 else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _coerce_kbps(kbps: Any) -> Optional[int]:
        try:
            return int(kbps)
        except (ValueError, TypeError):
            return None


# ═════════════════════════════════════════════════════════════════════
# BACKWARD COMPATIBILITY: alias LocalMusicDB
# ═════════════════════════════════════════════════════════════════════
class LocalMusicDB(LocalDB):
    """
    DEPRECATED v1086.3: usa LocalDB direttamente. Mantenuto per backcompat
    con codice che importa LocalMusicDB e chiama il vecchio upsert().
    """

    def upsert(self, relative_path: str, genre: str,
               subgenre: Optional[str] = None,
               bpm: Optional[float] = None,
               quality_kbps: Optional[int] = None) -> None:
        """Shim per la vecchia signature LocalMusicDB.upsert()."""
        self.upsert_file(
            relative_path,
            genre=genre,
            subgenre=subgenre,
            bpm=bpm,
            quality_kbps=quality_kbps,
        )


# ═════════════════════════════════════════════════════════════════════
# Migration helpers
# ═════════════════════════════════════════════════════════════════════
def migrate_legacy_to_v2(data_dir: Path, *, dry_run: bool = False) -> Dict[str, Any]:
    """
    Migra i file legacy (metadata_cache.json + music_library.json) in un
    unico `local_db.json` v2.

    Args:
        data_dir: directory che contiene i file legacy e dove salvare il nuovo
        dry_run: se True, non scrive niente ma ritorna le statistiche

    Returns:
        Dict con statistiche:
          - did_migration: bool
          - files_migrated: int
          - cache_migrated: int
          - cache_orphans: int (entries cache senza file path corrispondente)
          - errors: list[str]
          - new_db_path: str

    Comportamento:
    - Se `local_db.json` esiste gia' → no-op
    - Se nessun file legacy esiste → no-op (installazione fresh)
    - Altrimenti unisce e rinomina i legacy a `.migrated_v2`
    """
    data_dir = Path(data_dir)
    new_db_path = data_dir / "local_db.json"
    legacy_cache = data_dir / "metadata_cache.json"
    legacy_lib = data_dir / "music_library.json"

    stats = {
        "did_migration": False,
        "files_migrated": 0,
        "cache_migrated": 0,
        "cache_orphans": 0,
        "errors": [],
        "new_db_path": str(new_db_path),
    }

    if new_db_path.exists():
        logger.debug("Migration skip: local_db.json gia' esiste")
        return stats

    has_cache = legacy_cache.exists()
    has_lib = legacy_lib.exists()
    if not has_cache and not has_lib:
        logger.debug("Migration skip: nessun file legacy (fresh install)")
        return stats

    logger.info(
        f"Migration v1→v2 avviata "
        f"(cache={has_cache}, library={has_lib}, dry_run={dry_run})")

    db = LocalDB(new_db_path)

    # 1) Importa music_library.json (records canonici file→genere)
    library_data = {}
    if has_lib:
        try:
            with open(legacy_lib, "r", encoding="utf-8") as f:
                library_data = json.load(f) or {}
        except Exception as e:
            stats["errors"].append(f"Read music_library.json: {e}")
            library_data = {}

    files_v1 = library_data.get("files", {}) or {}
    for rel_path, rec in files_v1.items():
        try:
            # v1086.3: durante la migration, library v1 NON aveva
            # artist/title nel record (solo path → genre/bpm). Per
            # collegare la cache (indicizzata per artist|title) al
            # record file, inferiamo artist e title dal filename
            # con pattern "Artist - Title.mp3". Funziona per la maggior
            # parte dei file post-rename del cataloger; quelli con
            # nome non standard restano scollegati dalla cache (la
            # cache diventera' orfana → non un disastro, solo data
            # ridondante).
            inferred_artist, inferred_title = _infer_artist_title_from_path(rel_path)
            db.upsert_file(
                rel_path,
                artist=inferred_artist,
                title=inferred_title,
                genre=rec.get("genre"),
                subgenre=rec.get("subgenre"),
                bpm=rec.get("bpm"),
                quality_kbps=rec.get("quality_kbps"),
            )
            if rec.get("cataloged_at"):
                db._data["files"][rel_path]["cataloged_at"] = rec["cataloged_at"]
            stats["files_migrated"] += 1
        except Exception as e:
            stats["errors"].append(f"upsert_file({rel_path}): {e}")

    # 2) Importa metadata_cache.json (cache API per query)
    cache_data = {}
    if has_cache:
        try:
            with open(legacy_cache, "r", encoding="utf-8") as f:
                cache_data = json.load(f) or {}
        except Exception as e:
            stats["errors"].append(f"Read metadata_cache.json: {e}")
            cache_data = {}

    metadata_cache = cache_data.get("metadata_cache", {}) or {}

    # v1086.4: le chiavi della cache legacy hanno formato
    # "<provider>_<artist>_<title>[_<album>]" (es. "itunes_Akon_Lonely",
    # "mb_Beatles_Yesterday_Help!"). NON "artist|||title".
    # Aggreghiamo per (artist, title) i payload dei vari provider.
    import re
    prefix_map = {
        "mb": "musicbrainz", "lfm": "lastfm", "sp": "spotify",
        "deezer": "deezer", "itunes": "itunes", "discogs": "discogs",
    }
    aggregated: Dict[str, Dict] = {}  # qk → {provider: data, ...}
    for query_key, cache_rec in metadata_cache.items():
        if not isinstance(cache_rec, dict):
            continue
        # v1087.1: la cache legacy aveva chiave "<prefix>_<query_artist>_<query_title>[_<album>]".
        # I parametri DI QUERY (con cui il cataloger ha cercato) corrispondono
        # al filename/tag, quindi al record file via lookup_by_query.
        # Usiamo questi invece dei campi canonici dal provider, che possono
        # essere diversi (es. iTunes ritorna "Big Rob Savage" per "Audiomachine -
        # Kill 'Em All" → creerebbe un orfano).
        artist = None
        title = None
        m = re.match(r"^([a-z]+)_(.+)$", query_key)
        if m:
            rest = m.group(2)
            # Split su "_" — l'ultima parte e' l'album opzionale (solo MB).
            # Senza modo deterministico di separare artist da title, prendiamo
            # il primo "_" come separator. Funziona per la maggior parte;
            # per query con underscore nel nome (raro) cade nel fallback.
            if "_" in rest:
                a_q, t_q = rest.split("_", 1)
                # Se contiene un altro "_" e il prefisso e' mb, probabilmente
                # e' "<artist>_<title>_<album>" → t_q = "title_album" → tagliamo l'album
                if m.group(1) == "mb" and "_" in t_q:
                    t_q = t_q.rsplit("_", 1)[0]
                artist = a_q.strip()
                title = t_q.strip()

        # Fallback al payload canonico se il parsing non funziona
        if not artist or not title:
            artist = (cache_rec.get("artist") or "").strip()
            title = (cache_rec.get("title") or "").strip()
        # Ultimo fallback: parser legacy "artist|||title"
        if not artist or not title:
            artist, title = _parse_legacy_query_key(query_key)
        if not artist or not title:
            stats["errors"].append(f"Cache key non parsabile: {query_key!r}")
            continue
        provider = prefix_map.get(m.group(1), m.group(1)) if m else "unknown"
        qk = _normalize_query_key(artist, title)
        aggregated.setdefault(qk, {})[provider] = cache_rec

    for qk, providers in aggregated.items():
        try:
            first = next(iter(providers.values()))
            artist = first.get("artist") or ""
            title = first.get("title") or ""
            target_path = db._data.get("lookup_by_query", {}).get(qk)
            if target_path is None:
                target_path = f"__orphan__:{qk}"
            rec = db._data.setdefault("files", {}).setdefault(target_path, {})
            rec["artist"] = rec.get("artist") or artist
            rec["title"] = rec.get("title") or title
            if not rec.get("album"):
                for p_data in providers.values():
                    if p_data.get("album"):
                        rec["album"] = p_data["album"]
                        break
            ext = {
                "primary": next(iter(providers.keys())),
                "providers": providers,
                "cached_at": datetime.now().isoformat(timespec="seconds"),
                "source": next(iter(providers.keys())),
                "raw_genre": first.get("genre"),
                "raw_bpm": first.get("bpm"),
            }
            rec["external_lookup"] = ext
            db._data.setdefault("lookup_by_query", {})[qk] = target_path
            stats["cache_migrated"] += 1
            if target_path.startswith("__orphan__:"):
                stats["cache_orphans"] += 1
        except Exception as e:
            stats["errors"].append(f"cache aggregation({qk}): {e}")

    if dry_run:
        logger.info(f"Migration dry-run: {stats}")
        return stats

    # 3) Salva e rinomina i legacy
    if db.save():
        stats["did_migration"] = True
        if has_cache:
            try:
                shutil.move(str(legacy_cache),
                            str(legacy_cache.with_suffix(".json.migrated_v2")))
            except Exception as e:
                stats["errors"].append(f"Rename legacy cache: {e}")
        if has_lib:
            try:
                shutil.move(str(legacy_lib),
                            str(legacy_lib.with_suffix(".json.migrated_v2")))
            except Exception as e:
                stats["errors"].append(f"Rename legacy lib: {e}")
        logger.info(
            f"Migration completata: {stats['files_migrated']} file, "
            f"{stats['cache_migrated']} cache entries "
            f"({stats['cache_orphans']} orfani)")
    else:
        stats["errors"].append("Save local_db.json fallito")

    return stats


def _parse_legacy_query_key(key: str):
    """Parsa una chiave cache legacy in (artist, title).
    Supporta formati: 'artist|||title', 'artist|title', 'artist - title'."""
    for sep in ("|||", "|", " - "):
        if sep in key:
            parts = key.split(sep, 1)
            if len(parts) == 2:
                return parts[0].strip(), parts[1].strip()
    return None, None


def _infer_artist_title_from_path(rel_path: str):
    """v1086.3: inferenza euristica di (artist, title) dal filename.

    Pattern atteso: `<Genere>/<Artist> - <Title>.mp3` o
    `<Genere>/<Subgenere>/<Artist> - <Title>.mp3` (formato post-cataloger
    standard). Ritorna (None, None) se il filename non rispetta il
    pattern (es. file legacy non ancora processati dal cataloger).

    Usato esclusivamente durante la migration v1→v2 per collegare i
    record library (indicizzati per path) alle entry cache (indicizzate
    per artist|title).
    """
    from pathlib import PurePosixPath
    # Normalizza separatori (la migration legge sia "/" che "\")
    name = rel_path.replace("\\", "/")
    fname = PurePosixPath(name).stem  # senza estensione
    # Cerco " - " come separatore (dopo trim)
    if " - " in fname:
        parts = fname.split(" - ", 1)
        if len(parts) == 2:
            artist = parts[0].strip()
            title = parts[1].strip()
            if artist and title:
                return artist, title
    return None, None
