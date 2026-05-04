"""
Configurazioni generali per Music Cataloger Advanced
Tutte le impostazioni non sensibili (non API keys)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class APISettings:
    """Configurazioni per le API esterne"""
    timeout: int = 10
    max_retries: int = 3
    retry_delay: float = 1.0
    musicbrainz_rate_limit: float = 1.2
    lastfm_rate_limit: float = 0.5
    spotify_rate_limit: float = 0.1
    bpm_services_rate_limit: float = 1.0


@dataclass
class BPMSettings:
    """Configurazioni per la ricerca BPM"""
    valid_range_min: int = 60
    valid_range_max: int = 200

    difficulty_ranges: Dict[str, Dict] = field(default_factory=lambda: {
        '1 - Romantica': {'min_bpm': 0,   'max_bpm': 79,  'description': 'sotto 80 BPM'},
        '2 - Lenta':     {'min_bpm': 80,  'max_bpm': 94,  'description': '80-94 BPM'},
        '3 - Media':     {'min_bpm': 95,  'max_bpm': 99,  'description': '95-99 BPM'},
        '4 - Veloce':    {'min_bpm': 100, 'max_bpm': 119, 'description': '100-119 BPM'},
        '5 - Crazy':     {'min_bpm': 120, 'max_bpm': 999, 'description': '120+ BPM'},
    })

    bachata_bpm_range: tuple = (110, 135)
    salsa_bpm_range: tuple = (150, 220)


@dataclass
class BachataSettings:
    """Configurazioni per la classificazione Bachata"""

    # Artisti Bachata Dominicana tradizionale (puro)
    dominicana_artists: List[str] = field(default_factory=lambda: [
        'luis vargas', 'antony santos', 'anthony santos', 'raulin rodriguez',
        'joe veras', 'frank reyes', 'zacarias ferreira', 'elvis martinez',
        'alex bueno', 'hector acosta', 'el torito', 'monchy alexandra',
        'toño rosario', 'tono rosario', 'blas duran', 'juan luis guerra',
        'sergio vargas', 'felix manuel', 'rony santos', 'teodoro reyes',
        'luis segura', 'winston vargas', 'melvin vargas', 'eladio romero santos',
        'jose manuel calderon', 'rafael mejia', 'mario de jesus',
        'la india canela', 'yoskar sarante', 'yiyo sarante',
        # Artisti storici fondatori
        'jose manuel', 'jose manuel calderon',
    ])

    # Artisti Bachata Fusion/Moderna
    fusion_artists: List[str] = field(default_factory=lambda: [
        'romeo santos', 'aventura', 'prince royce', 'romeo',
        'bachata heightz', 'groupo extra', 'grupo extra',
        'daniel santacruz', 'nelson kanzela',
    ])

    # Artisti Bachata Sensual
    sensual_artists: List[str] = field(default_factory=lambda: [
        'korke y judith', 'bachata sensual', 'sensual brothers',
    ])

    # Parole chiave titolo/album per Dominicana
    dominicana_keywords: List[str] = field(default_factory=lambda: [
        'perico ripiao', 'tipico', 'tipica', 'guitarra',
        'romantica', 'romantico', 'despecho', 'amargue', 'amargado',
        'bachata rosa', 'campo', 'campesino', 'tierra',
        'dominicana', 'dominicano', 'republica',
        'bolero', 'son', 'guaracha',  # influenze originali
    ])

    # Parole chiave per Fusion
    fusion_keywords: List[str] = field(default_factory=lambda: [
        'urban', 'urbano', 'remix', 'electronic', 'electronica',
        'hip hop', 'rap', 'trap', 'r&b', 'pop fusion',
        'modern', 'moderno',
    ])

    # Range BPM per Dominicana (tende ad essere piu' lenta e cadenzata)
    dominicana_bpm_max: int = 130  # sopra questo e' piu' probabile fusion


@dataclass
class CoverSettings:
    """Configurazioni per il recupero cover album"""
    # Strategia: 'first_available', 'largest', 'manual'
    strategy: str = 'largest'

    # Priorita' sorgenti (ordine di preferimento)
    source_priority: List[str] = field(default_factory=lambda: [
        'spotify', 'musicbrainz', 'lastfm'
    ])

    # Dimensione minima accettabile (pixel)
    min_size_px: int = 300

    # Qualita' JPEG per salvataggio
    jpeg_quality: int = 90

    # Sovrascrivi cover esistente
    overwrite_existing: bool = False


@dataclass
class DuplicateSettings:
    """Configurazioni per la gestione dei file duplicati"""
    # Azioni disponibili: 'skip', 'overwrite', 'keep_both'
    action: str = 'keep_both'

    # Se 'keep_both': come rinominare
    # 'counter': filename_1.mp3, filename_2.mp3
    # 'timestamp': filename_20240101.mp3
    rename_strategy: str = 'counter'


@dataclass
class GenreSettings:
    """Configurazioni per la classificazione generi"""

    genre_mapping: Dict[str, str] = field(default_factory=lambda: {
        # Rock
        'rock': 'Rock', 'alternative rock': 'Rock', 'indie rock': 'Rock',
        'classic rock': 'Rock', 'hard rock': 'Rock', 'soft rock': 'Rock',
        'folk rock': 'Rock', 'punk rock': 'Rock', 'progressive rock': 'Rock',
        'nu metal': 'Rock', 'grunge': 'Rock', 'post-rock': 'Rock',

        # Pop
        'pop': 'Pop', 'pop rock': 'Pop', 'indie pop': 'Pop',
        'electropop': 'Pop', 'synthpop': 'Pop', 'dance pop': 'Pop',
        'j-pop': 'Pop', 'jpop': 'Pop', 'k-pop': 'Pop', 'kpop': 'Pop',
        'asian pop': 'Pop', 'c-pop': 'Pop',

        # Electronic
        'electronic': 'Electronic', 'electro': 'Electronic', 'techno': 'Electronic',
        'house': 'Electronic', 'trance': 'Electronic', 'ambient': 'Electronic',
        'edm': 'Electronic', 'dubstep': 'Electronic', 'drum and bass': 'Electronic',
        'dnb': 'Electronic', 'tech house': 'Electronic', 'deep house': 'Electronic',

        # Hip Hop
        'hip hop': 'Hip Hop', 'hip-hop': 'Hip Hop', 'rap': 'Hip Hop', 'trap': 'Hip Hop',

        # R&B / Soul
        'r&b': 'R&B', 'rnb': 'R&B', 'soul': 'R&B', 'neo soul': 'R&B',
        'neo-soul': 'R&B', 'funk': 'R&B',

        # Jazz
        'jazz': 'Jazz', 'smooth jazz': 'Jazz', 'fusion': 'Jazz', 'bebop': 'Jazz',
        'latin jazz': 'Jazz',

        # Classical
        'classical': 'Classical', 'classic': 'Classical',
        'orchestra': 'Classical', 'symphony': 'Classical', 'opera': 'Classical',

        # Reggae
        'reggae': 'Reggae', 'dancehall': 'Reggae', 'dub': 'Reggae',

        # Country / Folk
        'country': 'Country', 'folk': 'Folk', 'acoustic': 'Folk',

        # Metal
        'metal': 'Metal', 'heavy metal': 'Metal',
        'death metal': 'Metal', 'black metal': 'Metal',

        # Blues
        'blues': 'Blues',

        # Soundtrack / Score — FIX per Zac Efron "Films/Games/Film Scores"
        'soundtrack': 'Soundtrack', 'film score': 'Soundtrack', 'score': 'Soundtrack',
        'films/games': 'Soundtrack', 'films/games/film scores': 'Soundtrack',
        'game soundtrack': 'Soundtrack', 'video game': 'Soundtrack',
        'anime': 'Soundtrack', 'musical': 'Soundtrack', 'theatrical': 'Soundtrack',

        # Latin
        'salsa': 'Salsa', 'mambo': 'Salsa',
        'bachata': 'Bachata',
        'merengue': 'Latin', 'reggaeton': 'Latin', 'cumbia': 'Latin',
        'latin': 'Latin', 'latino': 'Latin', 'tropical': 'Latin',
        'vallenato': 'Latin', 'cha cha': 'Latin', 'tango': 'Latin',
        'bossa nova': 'Latin', 'samba': 'Latin',
        'latin pop': 'Latin', 'latin rock': 'Latin',

        # World
        'world': 'World', 'world music': 'World',
        'ethnic': 'World', 'traditional': 'World',

        # Alternative
        'alternative': 'Alternative', 'indie': 'Indie',
        'experimental': 'Experimental', 'vocal': 'Vocal',
    })

    latin_subgenres: List[str] = field(default_factory=lambda:
        ['salsa', 'bachata', 'merengue', 'cumbia', 'reggaeton',
         'boogaloo', 'cha cha cha', 'pachanga']
    )

    bachata_indicators: List[str] = field(default_factory=lambda: [
        'bachata', 'bachatero', 'bachatera', 'principe de la bachata',
        'rey de la bachata', 'aventura', 'romeo santos', 'frank reyes',
        'raulin rodriguez', 'anthony santos', 'antony santos', 'luis vargas',
        'joe veras', 'hector acosta', 'elvis martinez',
        'zacarias ferreira', 'monchy alexandra',
    ])

    salsa_indicators: List[str] = field(default_factory=lambda: [
        'salsa', 'salsero', 'salsera', 'son', 'mambo', 'guaguanco',
        'montuno', 'timba', 'orquesta', 'combo', 'sonora', 'gran combo',
        'willie colon', 'hector lavoe', 'ruben blades', 'cheo feliciano',
        'ismael rivera', 'andy montanez', 'gilberto santa rosa', 'oscar dleon',
        'oscar d\'leon', 'tito nieves', 'jerry rivera', 'tony vega',
        'frankie ruiz', 'eddie santiago', 'luis enrique', 'lalo rodriguez',
        'tommy olivencia', 'el gran combo', 'sonora poncena',
        'dimension latina', 'grupo niche',
    ])

    latin_indicators_generic: List[str] = field(default_factory=lambda:
        ['chiquito', 'salsa', 'bachata', 'merengue', 'reggaeton',
         'boogaloo', 'pachanga', 'titanes', 'latinos']
    )

    exclude_genre_tags: List[str] = field(default_factory=lambda: [
        'male vocalists', 'female vocalists', 'seen live', 'favorite',
        'love', 'beautiful', 'relaxing', 'energetic', 'happy', 'sad',
        'classic', 'old', 'new', '80s', '90s', '2000s', 'decade',
        'album', 'single', 'ep', 'live', 'remix', 'cover', 'instrumental',
    ])


@dataclass
class CacheSettings:
    max_age_days: int = 30
    max_backup_files: int = 5
    cache_filename: str = "metadata_cache.json"
    backup_prefix: str = "metadata_cache_backup"
    enable_metadata_cache: bool = True
    enable_genre_cache: bool = True
    enable_bpm_cache: bool = True


@dataclass
class LoggingSettings:
    log_filename_pattern: str = "MusicCatalogerAdvanced_{timestamp}.log"
    console_level: str = "INFO"
    file_level: str = "DEBUG"
    max_log_files: int = 10
    log_format: str = '%(asctime)s - %(levelname)s - %(message)s'


@dataclass
class FileSettings:
    # v1041 FIX: solo '.mp3' minuscolo — su Windows il filesystem è case-insensitive,
    # avere anche '.MP3' fa restituire ogni file due volte dal glob → conteggio doppio.
    supported_extensions: List[str] = field(default_factory=lambda: ['.mp3'])
    invalid_filename_chars: str = r'[<>:"/\\|?*]'
    ignore_folders: List[str] = field(default_factory=lambda: [
        '.git', '__pycache__', 'node_modules', '.vscode'
    ])


class Settings:
    def __init__(self):
        self.api = APISettings()
        self.bpm = BPMSettings()
        self.bachata = BachataSettings()
        self.cover = CoverSettings()
        self.duplicate = DuplicateSettings()
        self.genre = GenreSettings()
        self.cache = CacheSettings()
        self.logging = LoggingSettings()
        self.files = FileSettings()

    def print_summary(self):
        print("=== Configurazioni Music Cataloger ===")
        print(f"API Timeout: {self.api.timeout}s")
        print(f"BPM Range: {self.bpm.valid_range_min}-{self.bpm.valid_range_max}")
        print(f"Generi mappati: {len(self.genre.genre_mapping)}")
        print(f"Cache max age: {self.cache.max_age_days} giorni")
        print(f"Duplicati: {self.duplicate.action}")
        print(f"Cover strategy: {self.cover.strategy}")
        print("=" * 40)


settings = Settings()

if __name__ == "__main__":
    settings.print_summary()
