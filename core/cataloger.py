"""
Music Cataloger Advanced — Core Cataloger v0.0.2.2
Logica principale con:
- Fix unicode cp1252
- Metadati sempre aggiornati
- Cover album automatica
- Gestione duplicati configurabile
- Bachata Dominicana detection
- Progress callback preciso (include classify_salsa)
"""

import json
import logging
import shutil
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

try:
    import eyed3
    eyed3.log.setLevel("ERROR")
except ImportError:
    eyed3 = None

try:
    import mutagen
    from mutagen.mp3 import MP3
    from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC, TCON, TBPM
except ImportError:
    mutagen = None

try:
    import musicbrainzngs
except ImportError:
    musicbrainzngs = None

try:
    import requests
except ImportError:
    requests = None

try:
    import librosa
    LIBROSA_AVAILABLE = True
except ImportError:
    LIBROSA_AVAILABLE = False

try:
    from config.secrets import api_keys
    from config.settings import settings
    CONFIG_AVAILABLE = True
except ImportError:
    CONFIG_AVAILABLE = False
    api_keys = None
    settings = None

try:
    from services.external_apis import ExternalAPIs
    from services.bpm_services import BPMServices
    from services.cover_service import CoverService
    SERVICES_AVAILABLE = True
except ImportError:
    SERVICES_AVAILABLE = False
    ExternalAPIs = None
    BPMServices = None
    CoverService = None

try:
    from core.metadata_extractor import MetadataExtractor
    from core.genre_classifier import GenreClassifier
    from core.file_manager import FileManager
    CORE_AVAILABLE = True
except ImportError:
    CORE_AVAILABLE = False
    MetadataExtractor = None
    GenreClassifier = None
    FileManager = None


class CatalogProgress:
    """Struttura dati per comunicare il progresso alla GUI"""
    def __init__(self):
        self.total_files: int = 0
        self.processed_files: int = 0
        self.current_file: str = ""
        self.phase: str = "catalogazione"   # 'catalogazione' | 'classifica_salsa'
        self.moved_files: int = 0
        self.updated_files: int = 0
        self.uncatalogued: int = 0
        self.is_complete: bool = False
        self.error: Optional[str] = None


class MusicCataloger:
    """
    Classe principale per la catalogazione musicale.
    Versione v0.0.2.2 — tutti i fix dal log inclusi.
    """

    def __init__(
        self,
        base_path: str,
        dry_run: bool = True,
        use_external_db: bool = True,
        verbose: bool = False,
        # Opzioni duplicati
        duplicate_action: str = 'keep_both',   # 'skip', 'overwrite', 'keep_both'
        # Opzioni cover
        cover_enabled: bool = True,
        cover_strategy: str = 'largest',       # 'first_available', 'largest'
        cover_source_priority: Optional[List[str]] = None,
        cover_overwrite: bool = False,
        # v1046: DB locale
        update_local_db: bool = False,
        # v1057: generi esclusi dalle preferenze GUI
        excluded_genres: Optional[List[str]] = None,
        rename_pattern: Optional[str] = None,
        # v1086.1: priorita' sorgenti esterne (selezione UI)
        metadata_sources: Optional[List[str]] = None,
        bpm_sources: Optional[List[str]] = None,
        # Callback
        progress_callback: Optional[Callable[[CatalogProgress], None]] = None,
        log_callback: Optional[Callable[[str, str], None]] = None,
    ):
        self.base_path = Path(base_path)
        self.dry_run = dry_run
        self.use_external_db = use_external_db
        self.verbose = verbose
        self.cover_enabled = cover_enabled
        self.update_local_db = update_local_db
        self.excluded_genres: set = set(excluded_genres or [])  # v1057
        self.rename_pattern: Optional[str] = rename_pattern  # v1069
        # v1086.1: tengo refs come attributi cosi' init di ExternalAPIs/BPMServices
        # piu' avanti nello stesso __init__ puo' leggerle.
        self.metadata_sources = metadata_sources
        self.bpm_sources = bpm_sources
        self.progress_callback = progress_callback
        self.log_callback = log_callback

        # Statistiche
        self.processed_files = 0
        self.moved_files = 0
        self.updated_files = 0
        self.cover_added = 0
        self.uncatalogued_files: List[Dict] = []
        self.api_calls = 0
        self.start_time = time.time()

        # Directory script e dati
        if hasattr(sys, '_MEIPASS'):
            self.script_dir = Path(sys.executable).parent
        else:
            self.script_dir = Path(__file__).parent.parent.absolute()

        # v1049: tutti i file dati vanno in data/
        self.data_dir = self.script_dir / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Cache
        self.metadata_cache: Dict = {}
        self.genre_cache: Dict = {}

        # v1046: DB locale — inizializzato dopo _setup_logging() (vedi sotto)
        self._local_db = None

        # Rate limiting
        self.last_musicbrainz_call = 0
        self.last_lastfm_call = 0

        # Setup logging (prima di tutto)
        self._setup_logging()

        # Aggiorna settings con opzioni runtime
        if CONFIG_AVAILABLE and settings:
            settings.duplicate.action = duplicate_action
            settings.cover.strategy = cover_strategy
            settings.cover.overwrite_existing = cover_overwrite
            if cover_source_priority:
                settings.cover.source_priority = cover_source_priority

        # API Keys
        if CONFIG_AVAILABLE and api_keys:
            self.getsongbpm_api_key = api_keys.GETSONG_API_KEY
            self.lastfm_api_key = api_keys.LASTFM_API_KEY
            self.spotify_client_id = api_keys.SPOTIFY_CLIENT_ID
            self.spotify_client_secret = api_keys.SPOTIFY_CLIENT_SECRET
            self.musicbrainz_contact = api_keys.MUSICBRAINZ_CONTACT
        else:
            self.getsongbpm_api_key = ""
            self.lastfm_api_key = ""
            self.spotify_client_id = ""
            self.spotify_client_secret = ""
            self.musicbrainz_contact = "music@cataloger.local"

        # Inizializza servizi
        self._init_services()

        # Settings genere/BPM
        if CONFIG_AVAILABLE and settings:
            self.genre_mapping = settings.genre.genre_mapping
            self.latin_subgenres = settings.genre.latin_subgenres
            self.bachata_indicators = settings.genre.bachata_indicators
            self.salsa_indicators = settings.genre.salsa_indicators
            self.latin_indicators_generic = settings.genre.latin_indicators_generic
            self.exclude_genre_tags = settings.genre.exclude_genre_tags
            self.bpm_valid_range = (settings.bpm.valid_range_min, settings.bpm.valid_range_max)
            self.difficulty_mapping = settings.bpm.difficulty_ranges
            self.duplicate_action = settings.duplicate.action
        else:
            self._set_fallback_settings()
            self.duplicate_action = duplicate_action

        # MusicBrainz
        if musicbrainzngs and self.use_external_db:
            musicbrainzngs.set_useragent("MusicCatalogerAdvanced", "v0022", self.musicbrainz_contact)
            musicbrainzngs.set_rate_limit(limit_or_interval=1.2, new_requests=1)
            self._suppress_musicbrainz_warnings()

        self.logger.info(f"Directory progetto: {self.script_dir}")
        self.logger.info(f"Directory musica: {self.base_path}")
        self.logger.info(f"Duplicati: {self.duplicate_action} | Cover: {'ON' if cover_enabled else 'OFF'}")

        # v1086.3: DB locale UNIFICATO (local_db.json) — gestisce sia la
        # mappatura file→genere (era music_library.json) sia la cache delle
        # query API esterne (era metadata_cache.json). Migration automatica
        # al primo boot dai file legacy.
        self._local_db = None
        if self.update_local_db:
            try:
                from services.local_db import LocalDB, migrate_legacy_to_v2
                # Tenta migration legacy → v2 (no-op se già migrato o fresh)
                stats = migrate_legacy_to_v2(self.data_dir)
                if stats.get("did_migration"):
                    self.logger.info(
                        f"DB legacy migrato → local_db.json: "
                        f"{stats['files_migrated']} file, "
                        f"{stats['cache_migrated']} cache entries "
                        f"({stats['cache_orphans']} orfani)")
                    if stats.get("errors"):
                        self.logger.warning(
                            f"Migration: {len(stats['errors'])} errori non fatali")
                # Carica il DB unificato
                db_path = self.data_dir / "local_db.json"
                self._local_db = LocalDB(db_path)
                self._local_db.load()
                self.logger.info(
                    f"DB locale attivo: {db_path.name} "
                    f"({self._local_db.count_files()} file, "
                    f"{self._local_db.cache_count()} cache entries)")
            except Exception as e:
                self.logger.warning(f"DB locale non disponibile: {e}")

    # ─── SETUP ──────────────────────────────────────────────────────────

    def _setup_logging(self):
        # import io  # rimosso v1050

        # ── Log in output/ ────────────────────────────────────────────────
        output_dir = self.script_dir / "output"
        output_dir.mkdir(exist_ok=True)
        log_filename = f"MusicCatalogerAdvanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_path = output_dir / log_filename

        # v1050: non pulire il root logger
        fmt_str = '%(asctime)s - %(levelname)s - %(message)s'
        level = logging.DEBUG if self.verbose else logging.INFO

        # ── FILE handler: UTF-8 completo ─────────────────────────────────
        try:
            file_handler = logging.FileHandler(log_path, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(logging.Formatter(fmt_str))
        except Exception:
            file_handler = None

        # v1050: sys.stdout diretto — TextIOWrapper causava doppio flush su Windows
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(logging.Formatter(fmt_str))

        self.logger = logging.getLogger('MusicCataloger')
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        self.logger.handlers.clear()
        self.logger.addHandler(console_handler)
        if file_handler:
            self.logger.addHandler(file_handler)

        if self.log_callback:
            class GUIHandler(logging.Handler):
                def __init__(self, cb):
                    super().__init__()
                    self.cb = cb
                def emit(self, record):
                    try:
                        self.cb(self.format(record), record.levelname)
                    except Exception:
                        pass
            gui_h = GUIHandler(self.log_callback)
            gui_h.setFormatter(logging.Formatter(fmt_str))
            gui_h.setLevel(level)
            self.logger.addHandler(gui_h)

        if self.dry_run:
            self.logger.info("=== MODALITA SIMULAZIONE ATTIVA ===")
        self.logger.info(f"Catalogazione avviata | DB esterni: {'ON' if self.use_external_db else 'OFF'}")

    def _init_services(self):
        if SERVICES_AVAILABLE and CONFIG_AVAILABLE:
            # v1086.1: passo metadata_sources e bpm_sources cosi' la
            # cascata rispetta la priorita' UI.
            # v1087.3 (security Fase 2): costruisci un ApiClient per il
            # proxy lookup server-side. run_cataloger.py gira come
            # subprocess separato (non ha l'ApiClient della GUI in RAM),
            # ma puo' ricostruirlo da app_config.server_url + jwt_store
            # (token gia' salvati su disco dal login GUI). Se non c'e'
            # sessione valida o il modulo non e' disponibile, api_client
            # resta None → ExternalAPIs fa fallback ai provider pubblici.
            _api_client = None
            try:
                from config.app_config import config as _client_config
                from services.api_client import ApiClient
                _api_client = ApiClient(_client_config.server_url)
                self.logger.debug(
                    f"Proxy lookup attivo → {_client_config.server_url}")
            except Exception as _e:
                self.logger.debug(
                    f"Proxy lookup non disponibile ({_e}) — "
                    f"uso solo provider pubblici")
                _api_client = None

            self.external_apis = ExternalAPIs(
                api_keys, settings, self.logger,
                enabled_sources=self.metadata_sources,
                api_client=_api_client,
            )
            # v1056: propaga cache già caricata (se load_cache è stata chiamata prima)
            if self.metadata_cache:
                self.external_apis.metadata_cache = self.metadata_cache
            self.bpm_services = BPMServices(
                api_keys, settings, self.logger,
                enabled_sources=self.bpm_sources,
            )
            self.cover_service = CoverService(api_keys, settings, self.logger) if self.cover_enabled else None
        else:
            self.external_apis = None
            self.bpm_services = None
            self.cover_service = None

        if CORE_AVAILABLE and CONFIG_AVAILABLE:
            self.metadata_extractor = MetadataExtractor(settings, self.logger)
            self.genre_classifier = GenreClassifier(settings, self.logger)
            self.file_manager = FileManager(self.base_path, settings, self.dry_run, self.logger)
        else:
            self.metadata_extractor = None
            self.genre_classifier = None
            self.file_manager = None

    def _set_fallback_settings(self):
        self.genre_mapping = {
            'rock': 'Rock', 'pop': 'Pop', 'jazz': 'Jazz',
            'salsa': 'Salsa', 'bachata': 'Bachata', 'latin': 'Latin',
            'merengue': 'Latin', 'reggaeton': 'Latin',
            'soundtrack': 'Soundtrack', 'films/games': 'Soundtrack',
        }
        self.latin_subgenres = ['salsa', 'bachata', 'merengue', 'cumbia', 'reggaeton']
        self.bachata_indicators = ['bachata', 'aventura', 'romeo santos']
        self.salsa_indicators = ['salsa', 'orquesta', 'gran combo']
        self.latin_indicators_generic = ['salsa', 'bachata', 'merengue']
        self.exclude_genre_tags = ['seen live', 'favorite']
        self.bpm_valid_range = (60, 200)
        self.difficulty_mapping = {
            '1 - Romantica': {'min_bpm': 0,   'max_bpm': 79},
            '2 - Lenta':     {'min_bpm': 80,  'max_bpm': 94},
            '3 - Media':     {'min_bpm': 95,  'max_bpm': 99},
            '4 - Veloce':    {'min_bpm': 100, 'max_bpm': 119},
            '5 - Crazy':     {'min_bpm': 120, 'max_bpm': 999},
        }
        self.duplicate_action = 'keep_both'

    def _suppress_musicbrainz_warnings(self):
        mb_logger = logging.getLogger('musicbrainzngs')
        mb_logger.setLevel(logging.ERROR)
        class MBFilter(logging.Filter):
            def filter(self, record):
                unwanted = ['uncaught attribute', 'uncaught <first-release-date>', 'in <ws2:']
                return not any(m in record.getMessage() for m in unwanted)
        for name in ['musicbrainzngs', 'xml']:
            logging.getLogger(name).addFilter(MBFilter())

    # ─── METADATA ────────────────────────────────────────────────────────

    def _extract_all_metadata(self, file_path: Path) -> Dict:
        if self.metadata_extractor:
            return self.metadata_extractor.extract_all_metadata(file_path)
        # Fallback: eyed3 poi mutagen
        import re as re_mod, warnings as w_mod
        if eyed3:
            try:
                with w_mod.catch_warnings():
                    w_mod.simplefilter("ignore")
                    af = eyed3.load(str(file_path))
                if af and af.tag:
                    tag = af.tag
                    meta = {
                        'title': str(tag.title) if tag.title else None,
                        'artist': str(tag.artist) if tag.artist else None,
                        'album': str(tag.album) if tag.album else None,
                        'genre': str(tag.genre.name) if tag.genre else None,
                        'bpm': str(tag.bpm) if tag.bpm else None,
                        'duration': float(af.info.time_secs) if af.info else None,
                    }
                    return {k: v for k, v in meta.items() if v is not None and str(v).strip()}
            except Exception:
                pass
        if mutagen:
            try:
                audio = MP3(str(file_path))
                meta = {
                    'title': str(audio.get('TIT2', [''])[0]) if audio.get('TIT2') else None,
                    'artist': str(audio.get('TPE1', [''])[0]) if audio.get('TPE1') else None,
                    'album': str(audio.get('TALB', [''])[0]) if audio.get('TALB') else None,
                    'genre': str(audio.get('TCON', [''])[0]) if audio.get('TCON') else None,
                    'bpm': str(audio.get('TBPM', [''])[0]) if audio.get('TBPM') else None,
                    'duration': float(audio.info.length) if hasattr(audio, 'info') and audio.info else None,
                }
                return {k: v for k, v in meta.items() if v is not None and (k == 'duration' or str(v).strip())}
            except Exception:
                pass
        return {}

    def _guess_from_filename(self, file_path: Path) -> Dict:
        import re
        filename = file_path.stem
        patterns = [
            r'^(.+?)\s*-\s*(.+)$',
            r'^(.+?)\s*\u2013\s*(.+)$',
            r'^(\d+)\.\s*(.+?)\s*-\s*(.+)$',
        ]
        for pattern in patterns:
            match = re.match(pattern, filename, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 2:
                    return {'artist': groups[0].strip(), 'title': groups[1].strip()}
                elif len(groups) == 3:
                    return {'track_num': groups[0], 'artist': groups[1].strip(), 'title': groups[2].strip()}
        return {'title': filename}

    def _update_metadata(self, file_path: Path, metadata: Dict) -> bool:
        """Aggiorna i metadati ID3 del file. Sempre eseguito (non solo in dry_run)."""
        if not mutagen:
            return False
        try:
            try:
                tags = ID3(str(file_path))
            except Exception:
                tags = ID3()

            changed = False
            field_map = {
                'title': (TIT2, lambda v: TIT2(encoding=3, text=v)),
                'artist': (TPE1, lambda v: TPE1(encoding=3, text=v)),
                'album': (TALB, lambda v: TALB(encoding=3, text=v)),
                'genre': (TCON, lambda v: TCON(encoding=3, text=v)),
                'bpm': (TBPM, lambda v: TBPM(encoding=3, text=str(v))),
            }

            for field, (frame_cls, frame_factory) in field_map.items():
                val = metadata.get(field)
                if val:
                    frame_id = frame_cls.__name__
                    existing = tags.get(frame_id)
                    existing_val = str(existing) if existing else ''
                    if not existing_val or str(val) != existing_val:
                        tags[frame_id] = frame_factory(val)
                        changed = True

            if changed:
                tags.save(str(file_path))
                return True
            return False
        except Exception as e:
            self.logger.warning(f"Errore aggiornamento metadati {file_path.name}: {e}")
            return False

    def _determine_bpm(self, file_path: Path, metadata: Dict) -> Optional[str]:
        bpm_str = metadata.get('bpm')
        if bpm_str:
            try:
                bpm = int(float(bpm_str))
                mn, mx = self.bpm_valid_range
                if mn <= bpm <= mx:
                    return str(bpm)
            except (ValueError, TypeError):
                pass
        if self.bpm_services:
            bpm = self.bpm_services.estimate_bpm(file_path, metadata)
        elif LIBROSA_AVAILABLE:
            bpm = self._estimate_bpm_librosa(file_path)
        else:
            bpm = None
        return str(bpm) if bpm else None

    def _estimate_bpm_librosa(self, file_path: Path) -> Optional[int]:
        if not LIBROSA_AVAILABLE:
            return None
        try:
            import librosa as lr
            y, sr = lr.load(str(file_path), duration=60, mono=True)
            tempo, _ = lr.beat.beat_track(y=y, sr=sr)
            bpm = int(round(float(tempo)))
            mn, mx = self.bpm_valid_range
            return bpm if mn <= bpm <= mx else None
        except Exception:
            return None

    # ─── DUPLICATE HANDLING ──────────────────────────────────────────────

    def _resolve_destination(self, file_path: Path, dest_folder: Path) -> Optional[Path]:
        """
        Determina il percorso finale tenendo conto della policy duplicati.
        Returns:
          - Path valido da usare
          - None se l'azione e' 'skip'
        """
        destination = dest_folder / file_path.name

        if not destination.exists():
            return destination

        # Il file esiste gia'
        action = self.duplicate_action

        if action == 'skip':
            self.logger.info(f"[SKIP DUPLICATO] {file_path.name} gia' presente in {dest_folder.name}/")
            return None

        elif action == 'overwrite':
            self.logger.info(f"[SOVRASCRITTURA] {file_path.name}")
            return destination

        else:  # 'keep_both' — default
            counter = 1
            stem = file_path.stem
            suffix = file_path.suffix
            while destination.exists() and counter < 1000:
                destination = dest_folder / f"{stem}_{counter}{suffix}"
                counter += 1
            self.logger.info(f"[DUPLICATO] {file_path.name} -> rinominato in {destination.name}")
            return destination

    # ─── GENRE / MOVE ────────────────────────────────────────────────────

    def _determine_genre(self, file_path: Path, final_metadata: Dict,
                          external_metadata: Optional[Dict]) -> Tuple[str, str]:
        if self.genre_classifier:
            return self.genre_classifier.determine_genre(file_path, final_metadata, external_metadata)

        # Fallback manuale
        genre = None
        raw_genre = None
        if external_metadata and external_metadata.get('genre'):
            raw_genre = external_metadata['genre']
            gl = raw_genre.lower()
            if gl in ['salsa', 'bachata', 'merengue', 'cumbia', 'reggaeton']:
                genre = raw_genre.capitalize()
            else:
                genre = self.genre_mapping.get(gl, 'Other')
                if not genre or genre == 'Other':
                    for k, v in self.genre_mapping.items():
                        if k in gl or gl in k:
                            genre = v
                            break

        if not genre or genre == 'Other':
            if final_metadata.get('genre'):
                raw_genre = final_metadata['genre']
                gl = raw_genre.lower()
                genre = self.genre_mapping.get(gl, 'Other')

        if not genre or genre in ('Other', 'Unknown'):
            genre = 'Unknown'
            raw_genre = 'unknown'

        return genre, raw_genre

    def _is_latin_file(self, file_path: Path,
                        metadata: Optional[Dict] = None) -> bool:
        """v1085m: ritorna True se il file è "probabilmente latino"
        (salsa/bachata/merengue/...).

        Euristica conservativa basata su filename + metadata
        artist/album/title. Usata da `process_mp3_file` per decidere
        se forzare la riclassificazione filename-first quando
        `_caribbean_dirty=True` (l'admin ha aggiornato gli indicatori
        salsa/bachata e cache esistente potrebbe essere obsoleta).

        NB: NON è un classificatore. È un test "vale la pena
        riclassificare questo file?" — falsi positivi sono OK
        (riclassificheremo un file pop tre volte invece di una),
        falsi negativi sono il vero problema (mancato refresh).

        Match case-insensitive su:
        - filename (basename senza ext)
        - metadata: artist, album, title, genre (se presenti)
        contro la lista combinata `salsa_indicators + bachata_indicators`
        + qualche keyword fissa di riferimento per latini macro.
        """
        try:
            indicators = []
            indicators.extend(self.salsa_indicators or [])
            indicators.extend(self.bachata_indicators or [])
            # Keyword di riferimento per latini macro — sempre incluse
            # anche se gli indicators custom non le contengono.
            indicators.extend([
                'salsa', 'bachata', 'merengue', 'reggaeton', 'cumbia',
                'mambo', 'bolero', 'cha cha', 'chachacha', 'timba',
                'son cubano', 'guaracha',
            ])
            haystack_parts = [file_path.stem]
            if metadata:
                for k in ('artist', 'album', 'title', 'genre'):
                    v = metadata.get(k)
                    if isinstance(v, str) and v:
                        haystack_parts.append(v)
            haystack = " | ".join(haystack_parts).lower()
            for kw in indicators:
                if kw and kw.lower() in haystack:
                    return True
        except Exception:
            # Se qualcosa va storto qui, non vogliamo bloccare la
            # catalogazione: torniamo False (= no skip cache, comportamento
            # standard).
            pass
        return False

    def _move_to_genre_folder(self, file_path: Path, genre: str, raw_genre: str,
                               bachata_subtype: Optional[str] = None,
                               final_metadata: Optional[Dict] = None) -> bool:
        try:
            # v1057/v1059: applica filtro generi esclusi — case-insensitive
            if self.excluded_genres:
                # Normalizza per confronto case-insensitive
                excluded_lower = {g.lower() for g in self.excluded_genres}

                # Controlla il genere principale
                if genre.strip().lower() in excluded_lower:
                    parent = self._get_parent_genre(genre.strip())
                    # v1067: se il macrogenere padre = il genere stesso (es. World→World),
                    # il genere non ha un contenitore valido → sposta in Uncategorized
                    if parent.lower() == genre.strip().lower():
                        # v1068: genere escluso senza macrogenere alternativo
                        # → lascia nella directory radice come non catalogato
                        self.logger.info(f"   >-- Genere {genre} Escluso -> non catalogato (lasciato in root)")
                        return False   # non spostare il file
                    else:
                        self.logger.info(f"   >-- Genere {genre} Escluso -> Macrogenere: {parent}")
                        genre = parent
                        raw_genre = parent.lower()

                # v1059: controlla anche il raw_genre usato come subfolder latino
                # (es. raw_genre="tropical" → creerebbe Latin/Tropical/ anche se genre="Latin")
                elif raw_genre and raw_genre.strip().lower() in excluded_lower:
                    # v1071b: subgenere escluso
                    # Regola: se macrogenere ATTIVO → file va nel macrogenere
                    #         se macrogenere ESCLUSO → file rimane in root
                    # v1085p: passo raw_genre.strip() invece di .capitalize() —
                    # _get_parent_genre ora fa lookup case-insensitive quindi
                    # "hard rock" / "HARD ROCK" / "Hard Rock" matchano tutti.
                    sub_name = raw_genre.strip()
                    parent = self._get_parent_genre(sub_name)
                    parent_lower = parent.lower()
                    if parent_lower in excluded_lower or parent_lower == sub_name.lower():
                        # Macrogenere escluso o subgenere = macrogenere → resta in root
                        self.logger.info(
                            f"   >-- Subfolder Escluso: {sub_name} -> macrogenere {parent} anche escluso -> root"
                        )
                        return False
                    else:
                        # Macrogenere attivo → sposta nel macrogenere senza subfolder
                        self.logger.info(
                            f"   >-- Subfolder Escluso: {sub_name} -> usa solo {parent} (no subfolder)"
                        )
                        genre = parent
                        raw_genre = ""  # vuoto = nessuna subfolder

            if self.genre_classifier:
                dest_folder_rel = self.genre_classifier.get_genre_folder_path(
                    genre, raw_genre, bachata_subtype
                )
            else:
                dest_folder_rel = Path(genre)
                if self.genre_classifier is None:
                    # Fallback manuale: controlla se raw_genre contiene un subgenere latino
                    raw_lower = raw_genre.lower() if raw_genre else ''
                    for sub in self.latin_subgenres:
                        if sub == raw_lower or sub in raw_lower:
                            dest_folder_rel = Path('Latin') / sub.capitalize()
                            break

            full_dest = self.base_path / dest_folder_rel

            if self.dry_run:
                self.logger.info(f"\\-- [SIMULAZIONE] -> {dest_folder_rel}/")
                # v1031: token anche in dry-run per GUI
                print(f"MOVED: 1", flush=True)
                return True

            full_dest.mkdir(parents=True, exist_ok=True)
            destination = self._resolve_destination(file_path, full_dest)

            if destination is None:
                return False  # skip

            shutil.move(str(file_path), str(destination))

            # v1085g: rinomina file secondo il pattern se configurato.
            # FIX: prima la lookup avveniva su _metadata_cache con un
            # match testuale fragilissimo che falliva quasi sempre,
            # lasciando il file con il nome originale. Adesso usiamo
            # `final_metadata` (artist+title già normalizzati a riga ~691)
            # passato come argomento dal caller `process_file`.
            # v1085h: guard `final_metadata is not None` per sicurezza
            # (il caller potrebbe non passarlo in un futuro refactor).
            if (self.rename_pattern and not self.dry_run
                    and final_metadata
                    and final_metadata.get('artist')
                    and final_metadata.get('title')):
                try:
                    artist = str(final_metadata['artist']).strip()
                    title  = str(final_metadata['title']).strip()
                    if self.rename_pattern == "{artist} - {title}":
                        new_name = f"{artist} - {title}.mp3"
                    elif self.rename_pattern == "{title} - {artist}":
                        new_name = f"{title} - {artist}.mp3"
                    else:
                        # Pattern custom con sostituzione {artist}/{title}
                        new_name = (self.rename_pattern
                                    .replace("{artist}", artist)
                                    .replace("{title}",  title)
                                    + ".mp3")
                    # Rimuovi caratteri non validi per filesystem Windows
                    import re as _re
                    new_name = _re.sub(r'[<>:"/\\|?*]', '', new_name)
                    # Squash spazi multipli
                    new_name = _re.sub(r'\s+', ' ', new_name).strip()
                    new_path = destination.parent / new_name
                    if str(new_path) != str(destination):
                        if not new_path.exists():
                            destination.rename(new_path)
                            self.logger.info(f"   >-- Rinomina: {new_name}")
                            destination = new_path
                        else:
                            # File con il nome target esiste già: log e non sovrascrivere
                            self.logger.debug(
                                f"   [RINOMINA SKIP] target esiste già: {new_name}")
                except Exception as rex:
                    self.logger.warning(f"   [RINOMINA FALLITA] {rex}")

            self.logger.info(f"\\-- Spostata in {dest_folder_rel}/")

            # v1046/v1056: salva nel DB locale con bitrate.
            # v1086.3: passa anche artist/title (se disponibili) cosi' il
            # nuovo LocalDB v2 popola correttamente l'indice
            # lookup_by_query, permettendo alla cache esterna di trovare
            # questo file alla prossima query.
            if self._local_db is not None:
                try:
                    rel = (dest_folder_rel / file_path.name).as_posix()
                    kbps = None
                    try:
                        from mutagen.mp3 import MP3 as _MP3
                        _audio = _MP3(str(destination))
                        if _audio.info and hasattr(_audio.info, 'bitrate'):
                            kbps = int(_audio.info.bitrate // 1000)
                    except Exception:
                        pass
                    # Estraggo artist/title dal final_metadata se presente
                    fm_artist = (final_metadata or {}).get("artist")
                    fm_title = (final_metadata or {}).get("title")
                    fm_album = (final_metadata or {}).get("album")
                    fm_bpm = (final_metadata or {}).get("bpm")
                    self._local_db.upsert_file(
                        relative_path=rel,
                        artist=fm_artist,
                        title=fm_title,
                        album=fm_album,
                        genre=genre,
                        subgenre=str(raw_genre or "") or None,
                        bpm=fm_bpm,
                        quality_kbps=kbps,
                    )
                except Exception:
                    pass

            return True
        except Exception as e:
            self.logger.error(f"Errore spostamento {file_path.name}: {e}")
            return False

    # ─── PROCESS SINGLE FILE ─────────────────────────────────────────────

    def process_mp3_file(self, file_path: Path) -> bool:
        self.logger.info(f"*** {file_path.name} ***")
        try:
            # 1. Estrai metadati locali
            local_metadata = self._extract_all_metadata(file_path)
            if not local_metadata:
                local_metadata = self._guess_from_filename(file_path)

            final_metadata = local_metadata.copy()

            # 2. Completa con filename
            if not final_metadata.get('artist') or not final_metadata.get('title'):
                guessed = self._guess_from_filename(file_path)
                for k, v in guessed.items():
                    if k not in final_metadata or not final_metadata[k]:
                        final_metadata[k] = v

            artist = final_metadata.get('artist', 'Unknown')
            title = final_metadata.get('title', file_path.stem)
            self.logger.info(f">-- {artist} - {title}")

            # 3. DB esterni
            # v1073: se i parametri caraibici sono stati modificati,
            # forza riclassificazione filename-first per i file latini
            _skip_cache_for_genre = (
                getattr(self, "_caribbean_dirty", False) and
                self._is_latin_file(file_path, final_metadata)
            )
            if _skip_cache_for_genre:
                self.logger.debug(
                    f"Caribbean dirty — riclassificazione filename: {file_path.name}"
                )
                # Prova classificazione da filename prima della cache
                _fn_genre = None
                fn_lower = file_path.stem.lower()
                for _kw, _gn in [
                    ('salsa','Salsa'),('bachata','Bachata'),
                    ('merengue','Merengue'),('cumbia','Cumbia'),('timba','Timba'),
                ]:
                    if _kw in fn_lower:
                        _fn_genre = _gn
                        break
                if _fn_genre:
                    final_metadata['_genre_from_filename'] = _fn_genre

            external_metadata = None
            if self.use_external_db and artist != 'Unknown' and title:
                if self.external_apis:
                    external_metadata = self.external_apis.search_all(
                        artist, title, final_metadata.get('album')
                    )
                    if external_metadata:
                        self.api_calls += 1
                if external_metadata:
                    for k, v in external_metadata.items():
                        if k not in ('all_genres',) and v and (k not in final_metadata or not final_metadata[k]):
                            final_metadata[k] = v

            # 4. BPM
            bpm = self._determine_bpm(file_path, final_metadata)
            if bpm:
                final_metadata['bpm'] = bpm

            # 5. Genere
            genre, raw_genre = self._determine_genre(file_path, final_metadata, external_metadata)
            source = external_metadata.get('source', '') if external_metadata else ''
            bpm_str = f" | BPM: {bpm}" if bpm else ""
            src_str = f"{source}: " if source else ""
            self.logger.info(f">-- {src_str}Genere: {genre}{bpm_str}")

            # 6. Bachata subtype detection
            bachata_subtype = None
            if raw_genre.lower() == 'bachata' and self.genre_classifier:
                bachata_subtype = self.genre_classifier.detect_bachata_subtype(
                    final_metadata.get('artist', ''),
                    final_metadata.get('title', ''),
                    final_metadata.get('album', ''),
                    final_metadata
                )
                if bachata_subtype and bachata_subtype != 'Bachata':
                    self.logger.info(f">-- Bachata: {bachata_subtype}")

            # 7. Aggiorna metadati sul file (tag ID3 genre) — anche prima dello spostamento
            # così il genre nel file è sempre coerente con la destinazione
            if genre not in ('Unknown', 'Other'):
                final_metadata['genre'] = genre
                if not self.dry_run:
                    updated = self._update_metadata(file_path, final_metadata)
                    if updated:
                        self.updated_files += 1
                        self.logger.debug(f"Metadati aggiornati: {file_path.name}")
                    else:
                        # Se _update_metadata non ha aggiornato, prova a forzare solo il genre
                        try:
                            import eyed3 as _e3
                            _af = _e3.load(str(file_path))
                            if _af and _af.tag:
                                _af.tag.genre = genre
                                _af.tag.save()
                                self.updated_files += 1
                        except Exception:
                            pass
                else:
                    self.logger.debug(f"[SIMULAZIONE] Metadati da aggiornare per: {file_path.name}")

            # 8. Cover album
            if self.cover_service:
                cover_result = self.cover_service.process_file(file_path, final_metadata, dry_run=self.dry_run)
                # v1042: distingue tra cover già presente (nessuna azione) e scaricata
                if cover_result == 'existing':
                    self.logger.debug(f"|-- Cover: già presente, nessuna azione")
                elif cover_result == 'downloaded':
                    self.cover_added += 1
                    cover_source = getattr(self.cover_service, '_last_source', '')
                    src_label = f" ({cover_source})" if cover_source else ""
                    if self.dry_run:
                        self.logger.info(f">-- Cover: trovata online{src_label}")
                    else:
                        self.logger.info(f">-- Cover: scaricata e incorporata{src_label}")
                elif cover_result == 'not_found':
                    self.logger.debug(f"|-- Cover: non trovata online")
                # 'error' → nessun log aggiuntivo (già loggato dentro cover_service)

            # 9. Sposta o skip per Unknown
            if genre in ('Unknown', 'Other'):
                # v1028/v1032: uncatalogued incrementato SOLO qui.
                # v1032: token UNCATALOGED: per aggiornamento real-time GUI
                self.uncatalogued_files.append({
                    'file': file_path.name, 'reason': 'Genere sconosciuto',
                    'metadata': final_metadata, 'external_found': bool(external_metadata),
                    'genre_attempted': genre,
                })
                print(f"UNCATALOGED: 1", flush=True)
                return False

            success = self._move_to_genre_folder(
                file_path, genre, raw_genre, bachata_subtype,
                final_metadata=final_metadata)
            if success:
                self.moved_files += 1
                # v1031: token MOVED: intercettato dalla GUI per aggiornamento real-time
                print(f"MOVED: 1", flush=True)
                self.logger.info("")
                return True
            else:
                self.uncatalogued_files.append({
                    'file': file_path.name, 'reason': 'Errore spostamento o SKIP duplicato',
                    'metadata': final_metadata, 'external_found': bool(external_metadata),
                    'genre_attempted': genre,
                })
                self.logger.info("")
                return False

        except Exception as e:
            # v1028 BUG-03 FIX: gli errori/warning del parser audio (es. Illegal
            # Audio-MPEG-Header, Trying to resync...) generano eccezioni che NON
            # devono incrementare uncatalogued_files — quello è riservato ai file
            # che non ricevono un genere valido (vedi blocco "Unknown" sopra).
            # Logghiamo l'errore ma non contiamo questo come "non catalogato".
            self.logger.error(f"Errore inaspettato per {file_path.name}: {e}")
            self.logger.info("")
            if self.verbose:
                import traceback
                self.logger.error(traceback.format_exc())
            return False

    # ─── SCAN AND CATALOG ────────────────────────────────────────────────

    def scan_and_catalog(self):
        """Scansiona e cataloga tutti i file MP3"""
        self.logger.info("Inizio scansione file MP3...")
        if self.file_manager:
            mp3_files = self.file_manager.scan_mp3_files(recursive=False)
        else:
            # v1026 BUG-01 FIX: glob root-only, NON rglob.
            # rglob includeva anche i file già classificati nelle sottocartelle
            # → il totale veniva raddoppiato → progress bar al 50% invece del 100%.
            mp3_files = [
                f for f in self.base_path.glob("*.[mM][pP]3")
                if f.is_file() and f.parent == self.base_path
            ]

        if not mp3_files:
            self.logger.warning("Nessun file MP3 trovato nella directory principale")
            return

        total = len(mp3_files)
        self.logger.info(f"Trovati {total} file MP3 da elaborare")
        print(f"TOTAL: {total}", flush=True)

        progress = CatalogProgress()
        progress.total_files = total
        progress.phase = 'catalogazione'

        # v1033: finestra mobile per ETA
        import collections as _col
        _file_times = _col.deque(maxlen=20)
        _eta_counter = 0

        for i, mp3_file in enumerate(list(mp3_files)):
            if not mp3_file.exists():
                continue
            _t0 = time.time()
            self.processed_files += 1
            progress.processed_files = i + 1
            progress.current_file = mp3_file.name
            progress.moved_files = self.moved_files
            progress.updated_files = self.updated_files
            progress.uncatalogued = len(self.uncatalogued_files)
            if self.progress_callback:
                self.progress_callback(progress)
            # Token progress per la GUI
            print(f"PROGRESS: {i + 1}/{total}", flush=True)
            try:
                self.process_mp3_file(mp3_file)
            except Exception as e:
                self.logger.error(f"Errore per {mp3_file.name}: {e}")
                self.uncatalogued_files.append({
                    'file': mp3_file.name, 'reason': f'Errore: {e}',
                    'metadata': {}, 'external_found': False,
                })
            _elapsed = time.time() - _t0
            _file_times.append(_elapsed)
            _eta_counter += 1
            if _eta_counter >= 5 and _file_times:
                _avg = sum(_file_times) / len(_file_times)
                _remaining = total - (i + 1)
                _eta_sec = int(_avg * _remaining)
                # v1048: solo minuti/ore — niente secondi per evitare apparenza di blocco
                if _eta_sec >= 3600:
                    _eta_str = f"{_eta_sec // 3600}h{(_eta_sec % 3600) // 60:02d}m"
                elif _eta_sec >= 60:
                    _eta_str = f"{_eta_sec // 60}m"
                else:
                    _eta_str = "<1m"
                print(f"ETA: {_eta_str}", flush=True)
                _eta_counter = 0

        progress.is_complete = True
        if self.progress_callback:
            self.progress_callback(progress)
        self.logger.info(f"Elaborazione completata. Processati {total} file")

    # ─── CLASSIFY SALSA ─────────────────────────────────────────────────

    def classify_salsa_by_bpm(self):
        """Classifica la salsa in sottocartelle per velocita' BPM"""
        self.logger.info("=== CLASSIFICAZIONE SALSA PER VELOCITA' ===")
        salsa_folder = self.base_path / 'Latin' / 'Salsa'
        if not salsa_folder.exists():
            self.logger.info("Cartella Salsa non trovata")
            return

        mp3_files = [f for f in salsa_folder.glob("*.[mM][pP]3") if f.is_file()]
        total = len(mp3_files)
        self.logger.info(f"Trovati {total} file nella cartella Salsa")
        if not mp3_files:
            return

        progress = CatalogProgress()
        progress.total_files = total
        progress.phase = 'classifica_salsa'

        moved_count = 0
        no_bpm_count = 0

        for i, mp3_file in enumerate(mp3_files):
            progress.processed_files = i + 1
            progress.current_file = mp3_file.name
            if self.progress_callback:
                self.progress_callback(progress)

            try:
                metadata = self._extract_all_metadata(mp3_file)
                bpm_str = metadata.get('bpm')
                if not bpm_str or bpm_str == 'None':
                    bpm_val = self._determine_bpm(mp3_file, metadata)
                    if bpm_val:
                        bpm_str = bpm_val
                        metadata['bpm'] = bpm_str
                        if not self.dry_run:
                            self._update_metadata(mp3_file, metadata)

                if not bpm_str or bpm_str == 'None':
                    no_bpm_count += 1
                    continue

                try:
                    bpm = int(float(bpm_str))
                except (ValueError, TypeError):
                    no_bpm_count += 1
                    continue

                difficulty = None
                for diff_name, diff_info in self.difficulty_mapping.items():
                    if diff_info['min_bpm'] <= bpm <= diff_info['max_bpm']:
                        difficulty = diff_name
                        break

                if not difficulty:
                    continue

                diff_folder = salsa_folder / difficulty

                if self.dry_run:
                    self.logger.info(f"[SIMULAZIONE] {mp3_file.name} -> Salsa/{difficulty}/ (BPM: {bpm})")
                    moved_count += 1
                else:
                    diff_folder.mkdir(exist_ok=True)
                    destination = self._resolve_destination(mp3_file, diff_folder)
                    if destination is not None:
                        shutil.move(str(mp3_file), str(destination))
                        self.logger.info(f"[+] {mp3_file.name} -> Salsa/{difficulty}/ (BPM: {bpm})")
                        moved_count += 1

            except Exception as e:
                self.logger.warning(f"Errore per {mp3_file.name}: {e}")

        progress.is_complete = True
        if self.progress_callback:
            self.progress_callback(progress)

        action = "Da spostare" if self.dry_run else "Spostati"
        self.logger.info(f"{action}: {moved_count} file")
        if no_bpm_count > 0:
            self.logger.info(f"File senza BPM: {no_bpm_count}")

    # ─── UTILITY OPERATIONS ─────────────────────────────────────────────

    def correct_existing_folders(self):
        """v1071: scansiona TUTTE le cartelle genere, corregge metadati e aggiorna il DB locale.
        Sposta anche i file da cartelle di subgeneri esclusi al loro macrogenere.
        Gestisce i file spostati manualmente dall'utente tra una catalogazione e l'altra.
        """
        self.logger.info("=== CORREZIONE CARTELLE ESISTENTI ===")
        corrected = 0
        db_updates = 0
        moved_excluded = 0

        # ── Fase 0: sposta file da subgeneri esclusi al macrogenere ──────
        if self.excluded_genres:
            excluded_lower = {g.lower() for g in self.excluded_genres}
            for genre_dir in sorted(self.base_path.iterdir()):
                if not genre_dir.is_dir():
                    continue
                for sub_dir in sorted(genre_dir.iterdir()):
                    if not sub_dir.is_dir():
                        continue
                    sub_name = sub_dir.name.lower()
                    if sub_name not in excluded_lower:
                        continue
                    # Questo subgenere è escluso — sposta i file nel macrogenere
                    parent_dir = genre_dir
                    self.logger.info(
                        f"[SUBGENERE ESCLUSO] {genre_dir.name}/{sub_dir.name} "
                        f"→ sposto file in {genre_dir.name}/"
                    )
                    for mp3 in list(sub_dir.glob("*.[mM][pP]3")):
                        dest = parent_dir / mp3.name
                        if dest.exists():
                            # Mantieni entrambi con suffisso
                            import re as _re
                            stem = mp3.stem
                            suffix = mp3.suffix
                            counter = 1
                            while dest.exists():
                                dest = parent_dir / f"{stem} ({counter}){suffix}"
                                counter += 1
                        if not self.dry_run:
                            try:
                                import shutil as _sh
                                _sh.move(str(mp3), str(dest))
                                moved_excluded += 1
                                self.logger.info(f"  [SPOSTATO] {mp3.name} → {genre_dir.name}/")
                            except Exception as me:
                                self.logger.warning(f"  [ERR SPOSTA] {mp3.name}: {me}")
                        else:
                            self.logger.info(f"  [SIM] {mp3.name} → {genre_dir.name}/")
                    # Rimuovi cartella vuota
                    if not self.dry_run:
                        try:
                            if not any(sub_dir.iterdir()):
                                sub_dir.rmdir()
                                self.logger.info(f"  [RIMOSSA] Cartella {sub_dir.name}/")
                        except Exception:
                            pass

        # Carica DB locale per aggiornamento
        local_db = {}
        if self.update_local_db and hasattr(self, 'local_db') and self.local_db:
            try:
                local_db = self.local_db.load() or {}
            except Exception:
                pass

        # Scansiona tutte le sottocartelle della directory musicale
        for genre_dir in sorted(self.base_path.iterdir()):
            if not genre_dir.is_dir():
                continue
            genre_name = genre_dir.name
            if genre_name.startswith('.') or genre_name in ('output', 'data'):
                continue

            # Cerca mp3 nella cartella e nelle sue sottocartelle (es. Latin/Salsa)
            for mp3_file in genre_dir.rglob("*.[mM][pP]3"):
                try:
                    # Determina il genere atteso dalla posizione nella cartella
                    rel = mp3_file.relative_to(self.base_path)
                    parts = rel.parts
                    expected_genre = parts[0]  # es. "Latin", "Rock", "Pop"
                    expected_subgenre = parts[1] if len(parts) > 2 else ""  # es. "Salsa", "Bachata"

                    metadata = self._extract_all_metadata(mp3_file) or {}
                    current_genre = metadata.get('genre', '').strip()

                    # Aggiorna il tag ID3 se diverso dalla cartella
                    tag_genre = expected_subgenre if expected_subgenre else expected_genre
                    if current_genre.lower() != tag_genre.lower():
                        metadata['genre'] = tag_genre
                        if not self.dry_run:
                            try:
                                import eyed3 as _e3
                                af = _e3.load(str(mp3_file))
                                if af and af.tag:
                                    af.tag.genre = tag_genre
                                    af.tag.save()
                                    corrected += 1
                                    self.logger.info(
                                        f"[CORRETTO] {mp3_file.name}: "
                                        f"'{current_genre}' → '{tag_genre}'"
                                    )
                            except Exception as ex:
                                self.logger.warning(f"Errore tag {mp3_file.name}: {ex}")
                        else:
                            self.logger.info(
                                f"[SIMULAZIONE] {mp3_file.name}: "
                                f"'{current_genre}' → '{tag_genre}'"
                            )

                    # Aggiorna DB locale con la posizione attuale
                    # v1086.3: usa upsert_file con artist/title cosi' la
                    # cache esterna trova il file via lookup_by_query.
                    if self.update_local_db and self._local_db:
                        rel_str = str(rel).replace("\\", "/")
                        try:
                            quality_kbps = metadata.get('quality_kbps') or metadata.get('bitrate')
                            self._local_db.upsert_file(
                                relative_path=rel_str,
                                artist=metadata.get('artist'),
                                title=metadata.get('title'),
                                album=metadata.get('album'),
                                genre=expected_genre,
                                subgenre=expected_subgenre or tag_genre,
                                quality_kbps=int(quality_kbps) if quality_kbps else None,
                                bpm=metadata.get('bpm'),
                            )
                            db_updates += 1
                        except Exception as dex:
                            self.logger.debug(f"DB update err: {dex}")

                except Exception as e:
                    self.logger.warning(f"Errore analisi {mp3_file.name}: {e}")

        # Salva il DB locale aggiornato
        if self.update_local_db and self._local_db and db_updates > 0:
            try:
                self._local_db.save()
                self.logger.info(f"DB locale salvato: {db_updates} voci aggiornate")
            except Exception as se:
                self.logger.warning(f"Errore salvataggio DB: {se}")

        self.logger.info(
            f"=== CORREZIONE COMPLETATA: {corrected} metadati aggiornati, "
            f"{db_updates} voci DB aggiornate ==="
        )

    def analyze_collection(self) -> Dict:
        if self.file_manager:
            return self.file_manager.analyze_collection_structure()
        stats = {}
        for folder in self.base_path.iterdir():
            if not folder.is_dir() or folder.name.startswith('.'):
                continue
            count = sum(1 for f in folder.glob("*.[mM][pP]3") if f.is_file())
            if count > 0:
                stats[folder.name] = count
            if folder.name == 'Latin':
                for sub in folder.iterdir():
                    if sub.is_dir():
                        sc = sum(1 for f in sub.glob("*.[mM][pP]3") if f.is_file())
                        if sc > 0:
                            # Include sottocartelle bachata
                            for subsub in sub.iterdir():
                                if subsub.is_dir():
                                    ssc = sum(1 for f in subsub.glob("*.[mM][pP]3") if f.is_file())
                                    if ssc > 0:
                                        stats[f"Latin/{sub.name}/{subsub.name}"] = ssc
                                    sc -= ssc
                            if sc > 0:
                                stats[f"Latin/{sub.name}"] = sc
        return stats

    def cleanup_empty_folders(self):
        if self.file_manager:
            removed = self.file_manager.cleanup_empty_folders(root_only=False)
            if removed > 0:
                self.logger.info(f"Rimosse {removed} cartelle vuote")
        elif not self.dry_run:
            # Ricorsivo: rimuovi dalle foglie
            for folder in sorted(self.base_path.rglob("*"), key=lambda x: len(x.parts), reverse=True):
                if folder.is_dir() and not folder.name.startswith('.'):
                    try:
                        if not any(folder.iterdir()):
                            folder.rmdir()
                            self.logger.info(f"Rimossa cartella vuota: {folder.relative_to(self.base_path)}")
                    except Exception:
                        pass

    # ─── CACHE & REPORT ─────────────────────────────────────────────────

    def _get_parent_genre(self, genre: str) -> str:
        """v1059: dato un genere/subgenere, restituisce il macrogenere padre.
        v1085p: lookup case-insensitive — fix per "Hard Rock" / "Country Pop"
        / "Death Metal" che con .capitalize() diventavano "Hard rock" e
        non venivano matchati nel dizionario."""
        _PARENT_MAP = {
            # Latin subgeneri → Latin
            "Salsa": "Latin", "Salsa Romantica": "Latin", "Salsa Choke": "Latin",
            "Bachata": "Latin", "Bachata Sensual": "Latin", "Bachata Influence": "Latin",
            "Merengue": "Latin", "Reggaeton": "Latin", "Cumbia": "Latin",
            "Tropical": "Latin", "Mambo": "Latin", "Timba": "Latin",
            "Vallenato": "Latin", "Bolero": "Latin", "Boogaloo": "Latin",
            "Cha Cha Cha": "Latin", "Pachanga": "Latin", "Latin Jazz": "Latin",
            "Soca": "Latin", "Dancehall": "Latin",
            # Rock
            "Alternative": "Rock", "Indie": "Rock", "Metal": "Rock",
            "Death Metal": "Rock", "Punk": "Rock", "Grunge": "Rock",
            "Hard Rock": "Rock", "Progressive Rock": "Rock",
            # Classical & Jazz
            "Jazz": "Classical", "Blues": "Classical", "Soul": "Classical",
            "Smooth Jazz": "Classical", "Opera": "Classical", "Piano": "Classical",
            "Baroque": "Classical", "Contemporary Classical": "Classical",
            # Electronic
            "House": "Electronic", "Techno": "Electronic", "Trance": "Electronic",
            "Ambient": "Electronic", "Drum and Bass": "Electronic",
            "Dubstep": "Electronic", "EDM": "Electronic", "Synthwave": "Electronic",
            "Tropical House": "Electronic",
            # Pop & R&B
            "Dance Pop": "Pop", "R&B": "Pop", "Hip Hop": "Pop",
            "Trap": "Pop", "Vocal": "Pop", "K-Pop": "Pop", "J-Pop": "Pop",
            "Country": "Pop", "Country Pop": "Pop", "Funk": "Pop", "Gospel": "Pop",
            # Soundtrack
            "Anime": "Soundtrack", "TV Soundtrack": "Soundtrack",
            "Video Game": "Soundtrack", "Trailer Music": "Soundtrack",
            "Epic Orchestral": "Soundtrack",
            # World — i subgeneri rimangono in World ma World stesso non ha padre
            "Flamenco": "World", "Reggae": "World", "Folk": "World",
            "African": "World", "Brazilian": "World", "Celtic": "World",
            "Middle Eastern": "World", "Afrobeats": "World", "Bossa Nova": "World",
            # World come macrogenere → nessun padre, resta World
            "World": "World",
        }
        # v1085p: lookup case-insensitive. Costruisco mappa lowercase
        # ma mantengo i valori originali (case-corretto) per output pulito.
        # Cache su classe per evitare di ricostruire ad ogni call.
        if not hasattr(self, "_parent_map_lower"):
            self._parent_map_lower = {k.lower(): v for k, v in _PARENT_MAP.items()}
        return self._parent_map_lower.get(genre.strip().lower(), genre)

    def load_cache(self):
        """v1086.3: la cache delle query API esterne ora vive nel
        local_db.json unificato. Questo metodo trasferisce la cache
        del LocalDB → memoria di ExternalAPIs (dict in-RAM)
        per compatibilita' con il codice esistente di external_apis.py
        che lavora con `self.metadata_cache[cache_key]`."""
        # Caricamento dei flag caraibici (immutati dalla v1)
        cache_file = self.data_dir / "local_db.json"
        dirty_file = self.data_dir / "caribbean_dirty.flag"
        self._caribbean_dirty = False
        if dirty_file.exists() and cache_file.exists():
            try:
                dirty_ts = dirty_file.stat().st_mtime
                cache_ts = cache_file.stat().st_mtime
                if dirty_ts > cache_ts:
                    self._caribbean_dirty = True
                    self.logger.info(
                        "⚠ Parametri caraibici modificati dopo l'ultima cache — "
                        "i file latini verranno riclassificati dal filename."
                    )
            except Exception:
                pass

        # Caricamento cache da LocalDB unificato.
        # v1086.4: la cache in-RAM di external_apis.py usa chiavi
        # "<provider>_<artist>_<title>" (es. "itunes_Akon_Lonely"). Il nuovo
        # external_lookup ha la struttura aggregata { providers: {...} }.
        # Ricostruiamo le entries per-provider in RAM cosi' external_apis.py
        # puo' fare cache hit con le sue chiavi originali.
        if self._local_db is None:
            return
        # Prefissi cache_key usati da external_apis.py
        provider_prefixes = {
            "musicbrainz": "mb",
            "lastfm": "lfm",
            "spotify": "sp",
            "deezer": "deezer",
            "itunes": "itunes",
            "discogs": "discogs",
        }
        in_ram_cache = {}
        for path, rec in self._local_db._data.get("files", {}).items():
            ext = rec.get("external_lookup")
            if not ext:
                continue
            providers = ext.get("providers") or {}
            if not providers:
                # Schema legacy round 3: flat payload, un solo provider
                # implicito. Lo ricostruisco come single-provider.
                src = ext.get("source") or ext.get("primary") or "unknown"
                providers = {src: ext}
            artist = (rec.get("artist") or "").strip()
            title = (rec.get("title") or "").strip()
            if not artist or not title:
                continue
            for provider, payload in providers.items():
                if not isinstance(payload, dict):
                    continue
                prefix = provider_prefixes.get(provider)
                if not prefix:
                    continue
                # Costruisco la chiave originale di external_apis.py
                # NOTA: MusicBrainz usa "mb_<artist>_<title>_<album>";
                # gli altri "<prefix>_<artist>_<title>". Per restare
                # compatibile con entrambi, salvo la chiave senza
                # album (matchera' la maggior parte delle query MB
                # che non specificano album).
                cache_key = f"{prefix}_{artist}_{title}"
                in_ram_cache[cache_key] = payload
                # Per MusicBrainz, salvo anche la variante con album
                if provider == "musicbrainz" and payload.get("album"):
                    in_ram_cache[f"{prefix}_{artist}_{title}_{payload['album']}"] = payload
        self.metadata_cache = in_ram_cache
        self.genre_cache = {}
        if self.external_apis is not None:
            self.external_apis.metadata_cache = self.metadata_cache
        self.logger.info(f"Cache caricata: {len(self.metadata_cache)} metadati")

    def save_cache(self):
        """v1086.4: salva la cache API → local_db.json. 

        Background del bug v1086.3: `external_apis.metadata_cache` usa
        chiavi nel formato "<provider>_<artist>_<title>" (es. "itunes_Akon_Lonely"),
        NON "artist|||title". Il vecchio save_cache splittava sulla
        stringa "|||" che non e' mai presente → 0 entries salvate, mai.

        Strategia v1086.4: estraiamo da ogni chiave il provider e l'asse
        (artist, title), poi AGGREGHIAMO le risposte di provider diversi
        per lo stesso (artist, title) in un unico blob
        `external_lookup` con sotto-sezioni per provider.

        Schema risultante:
            external_lookup: {
                "primary": "itunes",
                "providers": {
                    "itunes": { artist, title, genre, bpm, cover_url, ... },
                    "musicbrainz": { artist, title, genre, bpm, ... },
                },
                "cached_at": "..."
            }
        """
        if self._local_db is None or self.external_apis is None:
            return
        import re
        from datetime import datetime as _dt

        in_ram = self.external_apis.metadata_cache or {}
        # Mappatura prefisso → nome provider canonico
        prefix_map = {
            "mb": "musicbrainz", "lfm": "lastfm", "sp": "spotify",
            "deezer": "deezer", "itunes": "itunes", "discogs": "discogs",
        }

        # Aggrega per (artist, title): provider → payload
        aggregated: dict = {}  # qk_canonical → {provider: data, ...}
        for cache_key, data in in_ram.items():
            if not data or not isinstance(data, dict):
                continue
            # v1087.1: preferisci _query_artist/_query_title (i parametri
            # originali con cui il cataloger ha cercato — corrispondono ai
            # campi del filename/tag, e quindi al record file via
            # lookup_by_query). Fallback ai canonici se la query non c'e'
            # (es. record migrati dalla cache legacy senza _query_*).
            q_artist = (data.get("_query_artist") or "").strip()
            q_title  = (data.get("_query_title")  or "").strip()
            artist = q_artist or (data.get("artist") or "").strip()
            title  = q_title  or (data.get("title")  or "").strip()
            if not artist or not title:
                continue
            # Prefisso provider
            provider = "unknown"
            m = re.match(r"^([a-z]+)_", cache_key)
            if m:
                provider = prefix_map.get(m.group(1), m.group(1))
            qk = f"{artist.lower()}|||{title.lower()}"
            aggregated.setdefault(qk, {})[provider] = data

        promoted = 0
        now = _dt.now().isoformat(timespec="seconds")
        for qk, providers in aggregated.items():
            try:
                # Recupera artist/title originali (case preservato) dal
                # primo provider disponibile
                first = next(iter(providers.values()))
                artist = first.get("artist", "")
                title = first.get("title", "")
                if not artist or not title:
                    continue

                target_path = self._local_db._data.get(
                    "lookup_by_query", {}).get(qk)
                if target_path is None:
                    target_path = f"__orphan__:{qk}"
                rec = self._local_db._data.setdefault(
                    "files", {}).setdefault(target_path, {})
                rec["artist"] = rec.get("artist") or artist
                rec["title"] = rec.get("title") or title
                # Album: preferisci dal record library (assegnato dal
                # cataloger), fallback al primo provider che ce l'ha
                if not rec.get("album"):
                    for p_data in providers.values():
                        if p_data.get("album"):
                            rec["album"] = p_data["album"]
                            break

                # Costruisci external_lookup con tutti i provider
                ext = {
                    "primary": next(iter(providers.keys())),
                    "providers": providers,
                    "cached_at": now,
                    # Campi top-level per backcompat (lettori vecchi)
                    "source": next(iter(providers.keys())),
                    "raw_genre": first.get("genre"),
                    "raw_bpm": first.get("bpm"),
                }
                rec["external_lookup"] = ext
                self._local_db._data.setdefault(
                    "lookup_by_query", {})[qk] = target_path
                promoted += 1
            except Exception as e:
                self.logger.debug(f"save_cache aggregation err: {e}")

        if self._local_db.save():
            self.logger.info(
                f"Cache salvata: {promoted} voci aggregate → local_db.json "
                f"(da {len(in_ram)} entries per-provider)")
        else:
            self.logger.warning("Errore salvataggio cache nel local_db.json")

    def backup_cache(self):
        """v1086.3: backup di local_db.json (era metadata_cache.json)."""
        cache_file = self.data_dir / "local_db.json"
        if not cache_file.exists():
            return
        try:
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            shutil.copy2(str(cache_file), str(self.data_dir / f"local_db_backup_{ts}.json"))
            backups = sorted(self.data_dir.glob("local_db_backup_*.json"),
                             key=lambda x: x.stat().st_mtime, reverse=True)
            for old in backups[5:]:
                old.unlink()
        except Exception as e:
            self.logger.warning(f"Errore backup cache: {e}")

    def generate_report(self) -> Dict:
        genre_stats = self.analyze_collection()
        elapsed = time.time() - self.start_time

        # v1043: deduplica uncatalogued_files per filename (può accadere se
        # scan_and_catalog e altri step processano lo stesso file)
        seen_files = set()
        unique_uncatalogued = []
        for fi in self.uncatalogued_files:
            fname = fi.get('file', '')
            if fname not in seen_files:
                seen_files.add(fname)
                unique_uncatalogued.append(fi)

        report = {
            'timestamp': datetime.now().isoformat(),
            'base_path': str(self.base_path),
            'version': 'v1043',
            'configuration': {
                'dry_run': self.dry_run,
                'external_db': self.use_external_db,
                'api_calls': self.api_calls,
                'duplicate_action': self.duplicate_action,
                'cover_enabled': self.cover_enabled,
            },
            'statistics': {
                'total_processed': self.processed_files,
                'successfully_moved': self.moved_files,
                'metadata_updated': self.updated_files,
                'covers_added': self.cover_added,
                'uncatalogued': len(unique_uncatalogued),
                'genres_found': len(genre_stats),
                'elapsed_seconds': round(elapsed, 1),
            },
            'genre_distribution': genre_stats,
            'uncatalogued_files': unique_uncatalogued,
        }
        report_file = self.script_dir / "output" / f"cataloging_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Report salvato: {report_file.name}")
        except Exception as e:
            self.logger.error(f"Errore salvataggio report: {e}")

        # v1046: salva DB locale
        if self._local_db is not None and not self.dry_run:
            self._local_db.save()

        mode = "SIMULAZIONE" if self.dry_run else "CATALOGAZIONE"
        self.logger.info(f"=== RIEPILOGO {mode} ===")
        self.logger.info(f"File processati: {self.processed_files}")
        self.logger.info(f"File {'che sarebbero stati ' if self.dry_run else ''}spostati: {self.moved_files}")
        self.logger.info(f"Metadati aggiornati: {self.updated_files}")
        if self.cover_enabled:
            self.logger.info(f"Cover aggiunte: {self.cover_added}")
        self.logger.info(f"Non catalogati: {len(self.uncatalogued_files)}")
        if genre_stats:
            # v1086.2: rimosso il limite [:10]. Pedro: "perche' se ci sono
            # generi con meno di 5 canzoni non lo segnala? Deve essere una
            # funzionalita' attiva per tutta la catalogazione". Mostro tutti
            # i generi rilevati ordinati per count desc. Cambio anche il
            # titolo da TOP GENERI a DISTRIBUZIONE GENERI per chiarezza.
            self.logger.info(f"=== DISTRIBUZIONE GENERI ({len(genre_stats)}) ===")
            for g, c in sorted(genre_stats.items(), key=lambda x: x[1], reverse=True):
                self.logger.info(f"  {g}: {c} file")
            # v1048: token per il dialog generi orfani nella GUI
            import json as _json
            print(f"GENRE_STATS: {_json.dumps(genre_stats)}", flush=True)
        if self.uncatalogued_files:
            self.logger.warning(f"\n=== FILE NON CATALOGATI ({len(self.uncatalogued_files)}) ===")
            for fi in self.uncatalogued_files:
                ext = " [DB consultato]" if fi.get('external_found') else ""
                self.logger.warning(f"  - {fi['file']}: {fi['reason']}{ext}")
        return report
