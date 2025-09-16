# Note V0017:
#Funzione di sotto-genere aggiunta:
#Gestito il sottogenere e quindi la crezione di sotto cartelle per Salsa e Bachata

#Gestione Log migliorata
#Il log INFO dà informazioni più puntuali
#Log tecnici spostati sul DEBUG

import argparse
import json
import logging
import os
import re
import shutil
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import eyed3
    eyed3.log.setLevel("ERROR")  # Riduci verbosità eyed3
except ImportError:
    eyed3 = None

try:
    import mutagen
    from mutagen.mp3 import MP3
    from mutagen.id3 import ID3, APIC, TIT2, TPE1, TALB, TDRC, TCON, TBPM
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


class MusicCatalogerAdvanced:
    def __init__(self, base_path: str, log_level=logging.INFO, dry_run=False, use_external_db=True):
        self.base_path = Path(base_path)
        self.dry_run = dry_run
        self.use_external_db = use_external_db
        self.verbose = log_level == logging.INFO
        self.getsongbpm_api_key = "YOUR_API_KEY_HERE"
        
        # Statistiche
        self.processed_files = 0
        self.moved_files = 0
        self.updated_files = 0
        self.uncatalogued_files = []
        self.api_calls = 0
        
        # Cache
        self.metadata_cache = {}
        self.genre_cache = {}
        
        # Rate limiting
        self.last_musicbrainz_call = 0
        self.last_lastfm_call = 0
        
        # CORRETTO: Determina la directory dello script PRIMA del setup logging
        if hasattr(sys, '_MEIPASS'):
            # Se è un eseguibile PyInstaller
            self.script_dir = Path(sys.executable).parent
        else:
            # Directory dove si trova lo script Python
            self.script_dir = Path(__file__).parent.absolute()

        # Setup logging DOPO aver definito script_dir
        self.setup_logging(log_level)
        
        # Ora possiamo usare il logger
        self.logger.info(f"Directory cache: {self.script_dir}")
        self.logger.info(f"Directory musica: {self.base_path}")
        
        # NUOVO: Lista per tracciare i file processati ed evitare double processing
        self.processed_file_paths = set()
        
        # Setup MusicBrainz con soppressione warning migliorata
        if musicbrainzngs and self.use_external_db:
            musicbrainzngs.set_useragent("MusicCatalogerAdvanced", "v0017", "captainjoker27@gmail.com")
            musicbrainzngs.set_rate_limit(limit_or_interval=1.2, new_requests=1)
            
            # NUOVO: Soppressione globale warning MusicBrainz
            self._suppress_musicbrainz_warnings()
        
        # Genre mapping (resto del codice rimane uguale)
        self.genre_mapping = {
            # Rock e derivati
            'rock': 'Rock',
            'alternative rock': 'Rock',
            'indie rock': 'Rock',
            'classic rock': 'Rock',
            'hard rock': 'Rock',
            'soft rock': 'Rock',
            'folk rock': 'Rock',
            'punk rock': 'Rock',
            'progressive rock': 'Rock',
            
            # Pop e derivati
            'pop': 'Pop',
            'pop rock': 'Pop',
            'indie pop': 'Pop',
            'electropop': 'Pop',
            'synthpop': 'Pop',
            'dance pop': 'Pop',
            
            # Electronic e derivati
            'electronic': 'Electronic',
            'electro': 'Electronic',
            'techno': 'Electronic',
            'house': 'Electronic',
            'trance': 'Electronic',
            'ambient': 'Electronic',
            'edm': 'Electronic',
            'dubstep': 'Electronic',
            'drum and bass': 'Electronic',
            'dnb': 'Electronic',
            
            # Hip Hop
            'hip hop': 'Hip Hop',
            'hip-hop': 'Hip Hop',
            'rap': 'Hip Hop',
            'trap': 'Hip Hop',
            
            # R&B e Soul
            'r&b': 'R&B',
            'rnb': 'R&B',
            'soul': 'R&B',
            'neo soul': 'R&B',
            'neo-soul': 'R&B',
            
            # Jazz e derivati
            'jazz': 'Jazz',
            'smooth jazz': 'Jazz',
            'fusion': 'Jazz',
            'bebop': 'Jazz',
            
            # Classica
            'classical': 'Classical',
            'classic': 'Classical',
            'orchestra': 'Classical',
            'symphony': 'Classical',
            
            # Reggae
            'reggae': 'Reggae',
            'dancehall': 'Reggae',
            'dub': 'Reggae',
            
            # Country
            'country': 'Country',
            'folk': 'Folk',
            'acoustic': 'Folk',
            
            # Metal
            'metal': 'Metal',
            'heavy metal': 'Metal',
            'death metal': 'Metal',
            'black metal': 'Metal',
            
            # Blues
            'blues': 'Blues',
            
            # Latin genres - AGGIUNTO
            'salsa': 'Latin',
            'bachata': 'Latin', 
            'merengue': 'Latin',
            'reggaeton': 'Latin',
            'latin': 'Latin',
            'latino': 'Latin',
            'tropical': 'Latin',
            'cumbia': 'Latin',
            'vallenato': 'Latin',
            'mambo': 'Latin',
            'cha cha': 'Latin',
            'tango': 'Latin',
            'bossa nova': 'Latin',
            'samba': 'Latin',
            'latin pop': 'Latin',
            'latin rock': 'Latin',
            
            # World music
            'world': 'World',
            'world music': 'World',
            'ethnic': 'World',
            'traditional': 'World',
            
            # Altro
            'alternative': 'Alternative',
            'indie': 'Indie',
            'experimental': 'Experimental',
            'soundtrack': 'Soundtrack',
            'vocal': 'Vocal'
        }
        
        # Sotto-Genre mapping per musiche latine
        self.subgenre_mapping = {
            'Latin': ['salsa', 'bachata', 'merengue', 'cumbia', 'reggaeton', 'tropical']
        }

    def _suppress_musicbrainz_warnings(self):
        """NUOVO: Soppressione globale dei warning MusicBrainz"""
        import logging
        
        # Soppressione warning musicbrainz specifici
        musicbrainz_logger = logging.getLogger('musicbrainzngs')
        musicbrainz_logger.setLevel(logging.ERROR)
        
        # Soppressione warning XML parsing
        xml_logger = logging.getLogger('xml')
        xml_logger.setLevel(logging.ERROR)
        
        # Filtro personalizzato per warning specifici
        class MusicBrainzWarningFilter(logging.Filter):
            def filter(self, record):
                unwanted_messages = [
                    'uncaught attribute',
                    'uncaught <first-release-date>',
                    'in <ws2:',
                ]
                return not any(msg in record.getMessage() for msg in unwanted_messages)
        
        # Applica il filtro a tutti i logger
        for logger_name in ['musicbrainzngs', 'xml', 'root']:
            logger = logging.getLogger(logger_name)
            logger.addFilter(MusicBrainzWarningFilter())

    def setup_logging(self, level):
        """CORRETTO: Configura il sistema di logging"""
        log_filename = f"mp3_cataloger_advanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_path = self.script_dir / log_filename

        # CORRETTO: Configurazione logging più pulita
        # Rimuovi tutti i handler esistenti
        logging.getLogger().handlers.clear()
        
        # Formato del log
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Handler per console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        
        # Handler per file
        try:
            file_handler = logging.FileHandler(log_path, encoding='utf-8')
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
        except Exception as e:
            file_handler = None
            print(f"Avviso: Impossibile creare file di log: {e}")
        
        # Configura logger principale
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level)
        self.logger.addHandler(console_handler)
        if file_handler:
            self.logger.addHandler(file_handler)
        
        # CORRETTO: Configura anche il logger root per catturare tutti i messaggi
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        
        # Messaggio iniziale
        if self.dry_run:
            self.logger.info("=== MODALITÀ SIMULAZIONE ATTIVA ===")
            self.logger.info("Nessun file sarà spostato o modificato")
        self.logger.info(f"Avvio catalogazione MP3 avanzata in: {self.base_path}")
        self.logger.info(f"Database esterni: {'ABILITATI' if self.use_external_db else 'DISABILITATI'}")
        self.logger.info(f"Livello logging: {logging.getLevelName(level)}")
        if file_handler:
            self.logger.info(f"Log salvato in: {log_path}")
    
    def extract_metadata_eyed3(self, file_path: Path) -> Optional[Dict]:
        """Estrae metadati usando eyed3 con fix per date parsing"""
        if not eyed3:
            return None
        
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                audiofile = eyed3.load(str(file_path))
            
            if not audiofile or not audiofile.tag:
                return None
            
            tag = audiofile.tag
            metadata = {
                'title': str(tag.title) if tag.title else None,
                'artist': str(tag.artist) if tag.artist else None,
                'album': str(tag.album) if tag.album else None,
                'genre': str(tag.genre.name) if tag.genre else None,
                'track_num': str(tag.track_num[0]) if tag.track_num and tag.track_num[0] else None,
                'bpm': str(tag.bpm) if tag.bpm else None,
                'duration': float(audiofile.info.time_secs) if audiofile.info else None
            }
            
            # CORRETTO: Fix per date parsing deprecation warning
            if tag.getBestDate():
                try:
                    best_date = tag.getBestDate()
                    if hasattr(best_date, 'year') and best_date.year:
                        metadata['year'] = str(best_date.year)
                    else:
                        # Fallback per date senza anno
                        date_str = str(best_date)
                        year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
                        if year_match:
                            metadata['year'] = year_match.group(0)
                except Exception as e:
                    self.logger.debug(f"Errore parsing data eyed3 per {file_path.name}: {e}")
            
            # Clean values
            cleaned = {}
            for k, v in metadata.items():
                if v is not None:
                    if k == 'duration':
                        cleaned[k] = v
                    else:
                        str_val = str(v).strip()
                        if str_val:
                            cleaned[k] = str_val
            
            return cleaned
            
        except Exception as e:
            self.logger.warning(f"Errore eyed3 per {file_path.name}: {e}")
            return None

    def extract_metadata_mutagen(self, file_path: Path) -> Optional[Dict]:
        """Estrae metadati usando Mutagen"""
        if not mutagen:
            return None
        
        try:
            audio = MP3(str(file_path))
            
            metadata = {
                'title': str(audio.get('TIT2', [''])[0]) if audio.get('TIT2') else None,
                'artist': str(audio.get('TPE1', [''])[0]) if audio.get('TPE1') else None,
                'album': str(audio.get('TALB', [''])[0]) if audio.get('TALB') else None,
                'year': str(audio.get('TDRC', [''])[0]) if audio.get('TDRC') else None,
                'genre': str(audio.get('TCON', [''])[0]) if audio.get('TCON') else None,
                'track_num': str(audio.get('TRCK', [''])[0]).split('/')[0] if audio.get('TRCK') else None,
                'bpm': str(audio.get('TBPM', [''])[0]) if audio.get('TBPM') else None,
                'duration': float(audio.info.length) if hasattr(audio, 'info') and audio.info else None
            }
            
            # Clean values properly
            cleaned = {}
            for k, v in metadata.items():
                if v is not None:
                    if k == 'duration':
                        # Keep duration as float
                        cleaned[k] = v
                    else:
                        # Convert to string and strip
                        str_val = str(v).strip()
                        if str_val:
                            cleaned[k] = str_val
            
            return cleaned
            
        except Exception as e:
            self.logger.warning(f"Errore Mutagen per {file_path.name}: {e}")
            return None
        
    def guess_metadata_from_filename(self, file_path: Path) -> Dict:
        """Indovina metadati dal nome del file"""
        filename = file_path.stem
        
        # Pattern comuni
        patterns = [
            r'^(.+?)\s*-\s*(.+)$',  # "Artist - Title"
            r'^(.+?)\s*–\s*(.+)$',  # "Artist – Title" (em dash)
            r'^(\d+)\.\s*(.+?)\s*-\s*(.+)$',  # "01. Artist - Title"
            r'^(.+?)\s*_\s*(.+)$',  # "Artist _ Title"
        ]
        
        metadata = {}
        
        for pattern in patterns:
            match = re.match(pattern, filename, re.IGNORECASE)
            if match:
                if len(match.groups()) == 2:
                    metadata['artist'] = match.group(1).strip()
                    metadata['title'] = match.group(2).strip()
                elif len(match.groups()) == 3:
                    metadata['track_num'] = match.group(1).strip()
                    metadata['artist'] = match.group(2).strip()
                    metadata['title'] = match.group(3).strip()
                break
        
        if not metadata:
            # Fallback: usa tutto il filename come titolo
            metadata['title'] = filename
        
        return metadata
    
    def search_musicbrainz(self, artist: str, title: str, album: str = None) -> Optional[Dict]:
        """CORRETTO: Cerca metadati su MusicBrainz con soppressione warning migliorata"""
        if not musicbrainzngs or not self.use_external_db:
            self.logger.debug("MusicBrainz non disponibile o DB esterni disabilitati")
            return None
        
        cache_key = f"mb_{artist}_{title}_{album or ''}"
        if cache_key in self.metadata_cache:
            self.logger.debug(f"MusicBrainz cache hit per: {artist} - {title}")
            return self.metadata_cache[cache_key]
        
        try:
            # Rate limiting
            elapsed = time.time() - self.last_musicbrainz_call
            if elapsed < 1.2:
                wait_time = 1.2 - elapsed
                self.logger.debug(f"MusicBrainz rate limiting: aspetto {wait_time:.2f}s")
                time.sleep(wait_time)
            
            self.last_musicbrainz_call = time.time()
            self.api_calls += 1
            
            # Build query
            query_parts = [f'artist:"{artist}"', f'recording:"{title}"']
            if album:
                query_parts.append(f'release:"{album}"')
            
            query = ' AND '.join(query_parts)
            self.logger.debug(f"MusicBrainz query: {query}")
            
            # Execute search with SSL context fix and warning suppression
            import ssl
            import urllib.request
            
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            original_opener = urllib.request.build_opener()
            https_handler = urllib.request.HTTPSHandler(context=ssl_context)
            opener = urllib.request.build_opener(https_handler)
            urllib.request.install_opener(opener)
            
            # Suppress MusicBrainz XML warnings
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore")
                
                # Cattura e sopprime anche i log warning
                mb_logger = logging.getLogger('musicbrainzngs')
                original_level = mb_logger.level
                mb_logger.setLevel(logging.ERROR)
                
                try:
                    result = musicbrainzngs.search_recordings(query=query, limit=3)
                finally:
                    mb_logger.setLevel(original_level)
            
            if not result.get('recording-list'):
                self.logger.debug(f"MusicBrainz: nessun risultato per {artist} - {title}")
                self.metadata_cache[cache_key] = None
                return None
            
            self.logger.debug(f"MusicBrainz: trovati {len(result['recording-list'])} risultati")
            recording = result['recording-list'][0]
            
            try:
                recording_id = recording['id']
                self.logger.debug(f"MusicBrainz: recupero dettagli per recording {recording_id}")
                
                # CORRETTO: Chiamata con soppressione warning completa
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore")
                    mb_logger = logging.getLogger('musicbrainzngs')
                    original_level = mb_logger.level
                    mb_logger.setLevel(logging.ERROR)
                    
                    try:
                        detailed = musicbrainzngs.get_recording_by_id(
                            recording_id, 
                            includes=['releases', 'artist-credits', 'tags']
                        )
                    finally:
                        mb_logger.setLevel(original_level)
                
                rec_data = detailed['recording']
                metadata = {
                    'title': rec_data.get('title'),
                    'artist': artist,
                    'duration': int(rec_data.get('length', 0)) / 1000 if rec_data.get('length') else None
                }
                
                self.logger.debug(f"MusicBrainz: metadati base estratti: {list(metadata.keys())}")
                
                # Estrai generi dai tag del recording
                genres = []
                if rec_data.get('tag-list'):
                    self.logger.debug(f"MusicBrainz: trovati {len(rec_data['tag-list'])} tag nel recording")
                    for tag in rec_data['tag-list']:
                        tag_name = tag.get('name', '').lower()
                        if self._is_music_genre_tag(tag_name):
                            genres.append(tag_name)
                            self.logger.debug(f"MusicBrainz: genere trovato nei tag recording: {tag_name}")
                
                # Info dal release
                if rec_data.get('release-list'):
                    release = rec_data['release-list'][0]
                    metadata['album'] = release.get('title')
                    
                    if release.get('date'):
                        metadata['year'] = release['date'][:4]
                        self.logger.debug(f"MusicBrainz: anno estratto: {metadata['year']}")
                    
                    # Generi dal release se disponibili
                    if release.get('tag-list'):
                        self.logger.debug(f"MusicBrainz: trovati tag nel release")
                        for tag in release['tag-list']:
                            tag_name = tag.get('name', '').lower()
                            if self._is_music_genre_tag(tag_name):
                                genres.append(tag_name)
                                self.logger.debug(f"MusicBrainz: genere trovato nei tag release: {tag_name}")
                
                # Cerca generi dall'artista (con rate limiting extra)
                if not genres and rec_data.get('artist-credit'):
                    try:
                        artist_mbid = rec_data['artist-credit'][0]['artist']['id']
                        self.logger.debug(f"MusicBrainz: cerco generi dall'artista {artist_mbid}")
                        
                        time.sleep(0.8)  # Rate limiting più aggressivo
                        
                        with warnings.catch_warnings():
                            warnings.filterwarnings("ignore")
                            mb_logger = logging.getLogger('musicbrainzngs')
                            original_level = mb_logger.level
                            mb_logger.setLevel(logging.ERROR)
                            
                            try:
                                artist_detailed = musicbrainzngs.get_artist_by_id(
                                    artist_mbid,
                                    includes=['tags']
                                )
                            finally:
                                mb_logger.setLevel(original_level)
                        
                        if artist_detailed.get('artist', {}).get('tag-list'):
                            self.logger.debug(f"MusicBrainz: trovati {len(artist_detailed['artist']['tag-list'])} tag nell'artista")
                            for tag in artist_detailed['artist']['tag-list']:
                                tag_name = tag.get('name', '').lower()
                                if self._is_music_genre_tag(tag_name):
                                    genres.append(tag_name)
                                    self.logger.debug(f"MusicBrainz: genere trovato nei tag artista: {tag_name}")
                    
                    except Exception as e:
                        self.logger.debug(f"MusicBrainz: errore recupero tag artista: {e}")
                
                # Processa i generi trovati
                if genres:
                    unique_genres = list(dict.fromkeys(genres))
                    self.logger.debug(f"MusicBrainz: generi trovati per {artist} - {title}: {unique_genres}")
                    
                    primary_genre = self._select_primary_genre(unique_genres)
                    if primary_genre:
                        metadata['genre'] = primary_genre
                        self.logger.debug(f"MusicBrainz: genere primario selezionato: {primary_genre}")
                    
                    metadata['all_genres'] = unique_genres[:5]
                else:
                    self.logger.debug(f"MusicBrainz: nessun genere trovato per {artist} - {title}")
                
                self.logger.info(f"├── Metadati completi {list(metadata.keys())}")
                self.metadata_cache[cache_key] = metadata
                return metadata
                
            except Exception as e:
                self.logger.warning(f"MusicBrainz: errore dettagli per {artist} - {title}: {e}")
                self.metadata_cache[cache_key] = None
                return None
                
        except Exception as e:
            self.logger.warning(f"MusicBrainz: errore connessione per {artist} - {title}: {e}")
            self.metadata_cache[cache_key] = None
            return None

    def _is_music_genre_tag(self, tag_name: str) -> bool:
        """Determina se un tag è un genere musicale"""
        # Lista di generi musicali noti
        known_genres = {
            'rock', 'pop', 'jazz', 'blues', 'classical', 'electronic', 'hip hop', 
            'country', 'folk', 'reggae', 'metal', 'punk', 'alternative', 'indie',
            'soul', 'funk', 'disco', 'house', 'techno', 'trance', 'ambient',
            'salsa', 'bachata', 'merengue', 'reggaeton', 'latin', 'tropical',
            'cumbia', 'tango', 'bossa nova', 'samba', 'mambo', 'cha cha',
            'world', 'experimental', 'soundtrack', 'vocal', 'instrumental'
        }
        
        # Tag da escludere (non sono generi)
        exclude_tags = {
            'male vocalists', 'female vocalists', 'seen live', 'favorite',
            'love', 'beautiful', 'relaxing', 'energetic', 'happy', 'sad',
            'classic', 'old', 'new', '80s', '90s', '2000s', 'decade',
            'album', 'single', 'ep', 'live', 'remix', 'cover', 'instrumental'
        }
        
        if tag_name in exclude_tags:
            return False
        
        # Controllo diretto
        if tag_name in known_genres:
            return True
        
        # Controllo parziale per generi composti
        for genre in known_genres:
            if genre in tag_name or tag_name in genre:
                return True
        
        # Controllo pattern comuni dei generi
        genre_patterns = [
            r'\w+ rock$', r'\w+ pop$', r'\w+ jazz$', r'\w+ metal$',
            r'neo \w+', r'post \w+', r'alt \w+', r'indie \w+',
            r'\w+ house$', r'\w+ techno$', r'\w+ trance$'
        ]
        
        for pattern in genre_patterns:
            if re.match(pattern, tag_name):
                return True
        
        return False
    
    def _select_primary_genre(self, genres: List[str]) -> Optional[str]:
        """Seleziona il genere primario da una lista di generi"""
        if not genres:
            return None
        
        # Priorità per generi specifici vs generici
        priority_order = [
            # Generi latini (alta priorità per il tuo caso)
            'salsa', 'bachata', 'merengue', 'reggaeton', 'cumbia', 'mambo',
            'tango', 'bossa nova', 'samba', 'tropical', 'vallenato',
            
            # Generi specifici
            'jazz fusion', 'progressive rock', 'death metal', 'drum and bass',
            'deep house', 'tech house', 'minimal techno',
            
            # Generi comuni ma specifici
            'rock', 'pop', 'jazz', 'blues', 'electronic', 'hip hop',
            'metal', 'reggae', 'folk', 'country', 'classical',
            
            # Generi generici (bassa priorità)
            'alternative', 'indie', 'experimental', 'world'
        ]
        
        # Cerca generi nell'ordine di priorità
        for priority_genre in priority_order:
            for genre in genres:
                if priority_genre == genre or priority_genre in genre:
                    return genre
        
        # Se nessuna priorità trovata, restituisci il primo
        return genres[0]
    
    # 3. Fix for Last.fm API key issue         api_key = "8b79bf6197a85dc2ff9e076da46792c5"  # Replace with actual API key
    def search_lastfm(self, artist: str, title: str) -> Optional[Dict]:
        """Cerca metadati su Last.fm con estrazione genere potenziata"""
        if not requests or not self.use_external_db:
            return None
        
        # Check if we have a valid API key
        api_key = "8b79bf6197a85dc2ff9e076da46792c5"  # Replace with actual API key
        if api_key == "YOUR_LASTFM_API_KEY":
            self.logger.debug("Last.fm API key non configurata, skip")
            return None
        
        cache_key = f"lfm_{artist}_{title}"
        if cache_key in self.metadata_cache:
            return self.metadata_cache[cache_key]
        
        try:
            # Rate limiting
            elapsed = time.time() - self.last_lastfm_call
            if elapsed < 0.5:
                time.sleep(0.5 - elapsed)
            
            self.last_lastfm_call = time.time()
            self.api_calls += 1
            
            # Chiamata principale per track info
            track_params = {
                'method': 'track.getInfo',
                'artist': artist,
                'track': title,
                'api_key': api_key,
                'format': 'json'
            }
            
            response = requests.get('https://ws.audioscrobbler.com/2.0/', params=track_params, timeout=10)
            
            if response.status_code != 200:
                self.logger.debug(f"Last.fm HTTP error: {response.status_code}")
                self.metadata_cache[cache_key] = None
                return None
            
            data = response.json()
            
            if 'track' not in data or 'error' in data:
                error_msg = data.get('message', 'Unknown error') if 'error' in data else 'No track found'
                self.logger.debug(f"Last.fm error: {error_msg}")
                self.metadata_cache[cache_key] = None
                return None
            
            track = data['track']
            
            # Estrai metadati base
            metadata = {
                'title': track.get('name'),
                'artist': track.get('artist', {}).get('name') if isinstance(track.get('artist'), dict) else artist
            }
            
            # Album info esistente
            if track.get('album'):
                album_info = track['album']
                metadata['album'] = album_info.get('title')
            
            # Duration esistente
            if track.get('duration'):
                try:
                    duration_ms = int(track['duration'])
                    if duration_ms > 0:
                        metadata['duration'] = duration_ms / 1000.0
                except (ValueError, TypeError):
                    pass
            
            # Estrazione genere più completa
            genres = []
            
            # 1. Generi dai tag del track
            if track.get('toptags', {}).get('tag'):
                tags = track['toptags']['tag']
                if isinstance(tags, list):
                    for tag in tags:
                        if isinstance(tag, dict):
                            tag_name = tag.get('name', '').lower().strip()
                            if tag_name and self._is_music_genre_tag(tag_name):
                                genres.append(tag_name)
                elif isinstance(tags, dict):
                    tag_name = tags.get('name', '').lower().strip()
                    if tag_name and self._is_music_genre_tag(tag_name):
                        genres.append(tag_name)
            
            # 2. Cerca generi dall'artista
            if not genres:  # Solo se non abbiamo generi dal track
                try:
                    artist_params = {
                        'method': 'artist.getInfo',
                        'artist': artist,
                        'api_key': api_key,
                        'format': 'json'
                    }
                    
                    time.sleep(0.3)
                    artist_response = requests.get('https://ws.audioscrobbler.com/2.0/', params=artist_params, timeout=10)
                    
                    if artist_response.status_code == 200:
                        artist_data = artist_response.json()
                        if 'artist' in artist_data:
                            artist_info = artist_data['artist']
                            
                            # Generi dai tag dell'artista
                            if artist_info.get('tags', {}).get('tag'):
                                artist_tags = artist_info['tags']['tag']
                                if isinstance(artist_tags, list):
                                    for tag in artist_tags:
                                        if isinstance(tag, dict):
                                            tag_name = tag.get('name', '').lower().strip()
                                            if tag_name and self._is_music_genre_tag(tag_name):
                                                genres.append(tag_name)
                
                except Exception as e:
                    self.logger.debug(f"Errore recupero generi artista Last.fm: {e}")
            
            # 3. Cerca generi dall'album se disponibile
            if track.get('album', {}).get('artist') and track.get('album', {}).get('title'):
                try:
                    album_artist = track['album']['artist']
                    album_title = track['album']['title']
                    
                    album_params = {
                        'method': 'album.getInfo',
                        'artist': album_artist,
                        'album': album_title,
                        'api_key': api_key,
                        'format': 'json'
                    }
                    
                    time.sleep(0.3)
                    album_response = requests.get('https://ws.audioscrobbler.com/2.0/', params=album_params, timeout=10)
                    
                    if album_response.status_code == 200:
                        album_data = album_response.json()
                        if 'album' in album_data:
                            album_info = album_data['album']
                            
                            # Anno dal rilascio album
                            if not metadata.get('year') and album_info.get('wiki', {}).get('published'):
                                published = album_info['wiki']['published']
                                year_match = re.search(r'\b(19|20)\d{2}\b', published)
                                if year_match:
                                    metadata['year'] = year_match.group(0)
                            
                            # Generi dai tag dell'album
                            if album_info.get('tags', {}).get('tag'):
                                album_tags = album_info['tags']['tag']
                                if isinstance(album_tags, list):
                                    for tag in album_tags:
                                        if isinstance(tag, dict):
                                            tag_name = tag.get('name', '').lower().strip()
                                            if tag_name and self._is_music_genre_tag(tag_name):
                                                genres.append(tag_name)
                
                except Exception as e:
                    self.logger.debug(f"Errore recupero generi album Last.fm: {e}")
            
            # Processa generi trovati
            if genres:
                unique_genres = list(dict.fromkeys(genres))  # Rimuovi duplicati
                primary_genre = self._select_primary_genre(unique_genres)
                if primary_genre:
                    metadata['genre'] = primary_genre
                
                # Salva tutti i generi per debug
                metadata['all_genres'] = unique_genres[:5]
            
            # Playcount e popolarità esistenti
            if track.get('playcount'):
                try:
                    playcount = int(track['playcount'])
                    metadata['playcount'] = playcount
                    
                    if playcount > 1000000:
                        metadata['popularity'] = 'high'
                    elif playcount > 100000:
                        metadata['popularity'] = 'medium'
                    else:
                        metadata['popularity'] = 'low'
                except (ValueError, TypeError):
                    pass
            
            if track.get('url'):
                metadata['lastfm_url'] = track['url']
            
            # Pulisci metadati vuoti
            cleaned_metadata = {}
            for k, v in metadata.items():
                if v is not None:
                    if isinstance(v, str):
                        v = v.strip()
                        if v:
                            cleaned_metadata[k] = v
                    else:
                        cleaned_metadata[k] = v
            
            self.logger.debug(f"Last.fm metadata per {artist} - {title}: {list(cleaned_metadata.keys())}")
            
            self.metadata_cache[cache_key] = cleaned_metadata
            return cleaned_metadata
            
        except Exception as e:
            self.logger.debug(f"Errore Last.fm per {artist} - {title}: {e}")
            self.metadata_cache[cache_key] = None
            return None

    def merge_metadata(self, existing: Dict, external: Optional[Dict], filename: Dict) -> Dict:
        """Unisce metadati con priorità intelligente"""
        final = {}
        
        # Campi base con priorità: esistenti > esterni > filename
        base_fields = ['title', 'artist', 'album', 'genre', 'track_num']
        
        for field in base_fields:
            value = (existing.get(field) or 
                    (external.get(field) if external else None) or 
                    filename.get(field))
            
            if value:
                final[field] = str(value).strip()
        
        # Anno: priorità intelligente
        year_sources = [
            existing.get('year'),
            external.get('year') if external else None,
            external.get('year_estimated') if external else None,
            filename.get('year')
        ]
        
        for year in year_sources:
            if year:
                final['year'] = str(year).strip()
                break
        
        # Durata: preferisci valori precisi
        duration_sources = [
            existing.get('duration'),
            external.get('duration') if external else None
        ]
        
        for duration in duration_sources:
            if duration and isinstance(duration, (int, float)) and duration > 0:
                final['duration'] = duration
                break
        
        # BPM: preferisci valori misurati rispetto a stimati
        bpm_sources = [
            existing.get('bpm'),
            external.get('bpm') if external else None,
            external.get('bpm_estimated') if external else None
        ]
        
        for bpm in bpm_sources:
            if bpm:
                final['bpm'] = str(bpm).strip()
                break
        
        # Campi aggiuntivi da fonti esterne
        if external:
            additional_fields = ['playcount', 'popularity', 'lastfm_url']
            for field in additional_fields:
                if external.get(field):
                    final[field] = external[field]
        
        return final
    
    def validate_metadata(self, metadata: Dict, file_path: Path) -> Dict:
        """Valida e pulisce i metadati"""
        validated = {}
        
        for key, value in metadata.items():
            if not value:
                continue
            
            value = str(value).strip()
            
            if key == 'year':
                # Estrai solo l'anno
                year_match = re.search(r'\b(19|20)\d{2}\b', value)
                if year_match:
                    validated[key] = year_match.group(0)
            elif key == 'bpm':
                # Valida BPM (deve essere numerico)
                try:
                    bpm_val = float(value)
                    if 60 <= bpm_val <= 200:  # Range ragionevole
                        validated[key] = str(int(bpm_val))
                except ValueError:
                    pass
            elif key == 'track_num':
                # Pulisci numero traccia
                track_match = re.search(r'\d+', value)
                if track_match:
                    validated[key] = track_match.group(0)
            else:
                validated[key] = value
        
        return validated
    
    def normalize_genre(self, genre: str) -> Optional[str]:
        """Normalizza il genere musicale"""
        if not genre:
            return None
        
        genre_lower = genre.lower().strip()
        
        # Cache check
        if genre_lower in self.genre_cache:
            return self.genre_cache[genre_lower]
        
        # Cerca corrispondenza esatta
        if genre_lower in self.genre_mapping:
            normalized = self.genre_mapping[genre_lower]
            self.genre_cache[genre_lower] = normalized
            return normalized
        
        # Cerca corrispondenza parziale
        for key, value in self.genre_mapping.items():
            if key in genre_lower or genre_lower in key:
                self.genre_cache[genre_lower] = value
                return value
        
        # Prova a indovinare da parole chiave
        genre_words = genre_lower.split()
        for word in genre_words:
            if word in self.genre_mapping:
                normalized = self.genre_mapping[word]
                self.genre_cache[genre_lower] = normalized
                return normalized
        
        # Default a "Other" se non trovato
        self.genre_cache[genre_lower] = "Other"
        return "Other"
    
    def estimate_bpm_from_audio(self, file_path: Path, metadata: Dict) -> int | None:
        """
        Recupera il BPM da getsongbpm.com o lo stima da audio tramite librosa come fallback.
        """
        # 1. Tentativo da API getsongbpm.com
        if self.getsongbpm_api_key:
            artist = metadata.get('artist')
            title = metadata.get('title')
            if artist and title:
                try:
                    self.logger.debug(f"Ricerca BPM via API per '{artist}' - '{title}'...")
                    base_url = "https://api.getsongbpm.com/search/"
                    params = {
                        "api_key": self.getsongbpm_api_key,
                        "artist": artist,
                        "song_title": title
                    }
                    response = requests.get(base_url, params=params)
                    response.raise_for_status() # Lancia un errore per risposte non 2xx
    
                    data = response.json()
                    results = data.get("search", [])
    
                    if results and results[0].get("tempo"):
                        bpm = int(float(results[0]["tempo"]))
                        self.logger.debug(f"BPM trovato via API: {bpm}")
                        return bpm
                    else:
                        self.logger.debug("Nessun risultato BPM trovato dall'API.")
                except requests.exceptions.RequestException as e:
                    self.logger.warning(f"Errore nella richiesta API per BPM: {e}")
                except (ValueError, KeyError, IndexError) as e:
                    self.logger.warning(f"Errore nella gestione della risposta API per BPM: {e}")
    
        # 2. Fallback con analisi audio locale (librosa)
        self.logger.debug("Fallback: stima BPM da analisi audio locale...")
        try:
            y, sr = librosa.load(file_path, sr=None)
            tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
            bpm = int(round(tempo))
            self.logger.debug(f"BPM stimato localmente: {bpm}")
            return bpm
        except Exception as e:
            self.logger.warning(f"Impossibile stimare il BPM da file audio: {e}")
            return None
        
    def clean_filename(self, name: str) -> str:
        """Pulisce il nome per uso come cartella"""
        # Rimuovi caratteri non validi
        cleaned = re.sub(r'[<>:"/\\|?*]', '', name)
        # Rimuovi spazi multipli
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned
    
    def update_metadata_mutagen(self, file_path: Path, metadata: Dict, cover_data: bytes = None):
        """Aggiorna metadati del file usando Mutagen"""
        if not mutagen:
            self.logger.warning("Mutagen non disponibile per aggiornare metadati")
            return False
        
        try:
            audio = MP3(str(file_path))
            
            # Assicurati che esistano i tag ID3
            if audio.tags is None:
                audio.add_tags()
            
            # Aggiorna metadati
            if metadata.get('title'):
                audio.tags['TIT2'] = TIT2(encoding=3, text=metadata['title'])
            if metadata.get('artist'):
                audio.tags['TPE1'] = TPE1(encoding=3, text=metadata['artist'])
            if metadata.get('album'):
                audio.tags['TALB'] = TALB(encoding=3, text=metadata['album'])
            if metadata.get('year'):
                audio.tags['TDRC'] = TDRC(encoding=3, text=metadata['year'])
            if metadata.get('genre'):
                audio.tags['TCON'] = TCON(encoding=3, text=metadata['genre'])
            if metadata.get('bpm'):
                audio.tags['TBPM'] = TBPM(encoding=3, text=metadata['bpm'])
            
            # Aggiungi cover se disponibile
            if cover_data:
                audio.tags['APIC'] = APIC(
                    encoding=3,
                    mime='image/jpeg',
                    type=3,
                    desc='Cover',
                    data=cover_data
                )
            
            audio.save()
            self.logger.debug(f"Metadati aggiornati per: {file_path.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Errore aggiornamento metadati per {file_path.name}: {e}")
            return False
    
    def download_album_art(self, artist: str, album: str, file_path: Path) -> Optional[bytes]:
        """Scarica copertina album (implementazione base)"""
        # Implementazione semplificata - si può estendere con API specifiche
        return None

    def process_mp3_file(self, file_path: Path):
        """CORRETTO: Processa un singolo file MP3 con controllo duplicati e BPM"""
        # NUOVO: Controlla se il file è già stato processato
        if str(file_path) in self.processed_file_paths:
            self.logger.debug(f"File già processato, skip: {file_path.name}")
            return False
        
        # NUOVO: Controlla se il file esiste ancora (potrebbe essere stato spostato)
        if not file_path.exists():
            self.logger.debug(f"File non più esistente, skip: {file_path.name}")
            return False
        
        # Aggiungi il file alla lista dei processati
        self.processed_file_paths.add(str(file_path))
        
        self.logger.info(f"*** {file_path.name} ***")
        
        try:
            # 1. Estrai metadati esistenti
            self.logger.debug("Fase 1: Estrazione metadati esistenti")
            existing_metadata = {}
            
            try:
                existing_metadata = self.extract_metadata_eyed3(file_path) or {}
                if existing_metadata:
                    self.logger.debug(f"eyed3: trovati campi {list(existing_metadata.keys())}")
            except Exception as e:
                self.logger.debug(f"eyed3 fallito: {e}")
            
            if not existing_metadata:
                try:
                    existing_metadata = self.extract_metadata_mutagen(file_path) or {}
                    if existing_metadata:
                        self.logger.debug(f"Mutagen: trovati campi {list(existing_metadata.keys())}")
                except Exception as e:
                    self.logger.debug(f"Mutagen fallito: {e}")
            
            # 2. Deduci da nome file
            self.logger.debug("Fase 2: Analisi nome file")
            filename_metadata = self.guess_metadata_from_filename(file_path)
            self.logger.debug(f"Filename: estratti campi {list(filename_metadata.keys())}")
            
            # 3. Cerca database esterni
            external_metadata = None
            search_artist = existing_metadata.get('artist') or filename_metadata.get('artist')
            search_title = existing_metadata.get('title') or filename_metadata.get('title')
            search_album = existing_metadata.get('album') or filename_metadata.get('album')
    
            self.logger.debug(f"Fase 3: Ricerca DB esterni - artist='{search_artist}', title='{search_title}'")
            
            if self.use_external_db and search_artist and search_title:
                # Prova MusicBrainz per primo
                self.logger.debug("Tentativo MusicBrainz...")
                try:
                    external_metadata = self.search_musicbrainz(search_artist, search_title, search_album)
                    if external_metadata:
                        self.logger.debug(f"MusicBrainz: successo per {file_path.name}")
                    else:
                        self.logger.debug("MusicBrainz: nessun risultato")
                except Exception as e:
                    self.logger.debug(f"MusicBrainz: errore {e}")
                
                # Prova Last.fm come fallback
                if not external_metadata:
                    self.logger.debug("Tentativo Last.fm...")
                    try:
                        external_metadata = self.search_lastfm(search_artist, search_title)
                        if external_metadata:
                            self.logger.debug(f"Last.fm: successo per {file_path.name}")
                        else:
                            self.logger.debug("Last.fm: nessun risultato")
                    except Exception as e:
                        self.logger.debug(f"Last.fm: errore {e}")
            else:
                self.logger.debug("Ricerca DB esterni saltata (dati insufficienti o disabilitata)")
            
            # 4. Unisci metadati
            self.logger.debug("Fase 4: Unione metadati")
            final_metadata = self.merge_metadata(existing_metadata, external_metadata, filename_metadata)
            final_metadata = self.validate_metadata(final_metadata, file_path)
            
            self.logger.debug(f"Metadati finali: {list(final_metadata.keys())}")
            
            # NUOVO: Recupero e aggiungi il BPM
            bpm = self.estimate_bpm_from_audio(file_path, final_metadata)
            if bpm:
                final_metadata['bpm'] = bpm
                self.logger.debug(f"BPM recuperato e aggiunto: {bpm}")

            # 5. Aggiorna metadati del file
            if final_metadata and not self.dry_run:
                try:
                    self.update_metadata_mutagen(file_path, final_metadata)
                    self.updated_files += 1
                    self.logger.debug(f"Metadati aggiornati per: {file_path.name}")
                except Exception as e:
                    self.logger.warning(f"Errore aggiornamento metadati per {file_path.name}: {e}")
            
            # 6. Normalizza genere con logging migliorato
            self.logger.debug("Fase 5: Normalizzazione genere")
            raw_genre = final_metadata.get('genre') or 'Unknown' # Correzione errore: gestisce il caso di genere mancante
            self.logger.debug(f"Genere grezzo: '{raw_genre}'")
            
            genre = self.normalize_genre(raw_genre)
            self.logger.debug(f"Genere normalizzato: '{genre}'")
            
            # Logica di fallback potenziata con logging
            if not genre or genre == "Other":
                self.logger.debug("Tentativo inferenza genere da artista/filename...")
                artist_name = (final_metadata.get('artist', '') + ' ' + file_path.stem).lower()
                self.logger.debug(f"Testo per analisi: '{artist_name[:50]}...'")
                
                latin_indicators = ['chiquito', 'salsa', 'bachata', 'merengue', 'reggaeton', 'tropical', 'titanes', 'latinos']
                
                found_indicator = None
                for indicator in latin_indicators:
                    if indicator in artist_name:
                        found_indicator = indicator
                        break
                
                if found_indicator:
                    genre = 'Latin'
                    self.logger.debug(f"Genere Latino inferito da indicatore: '{found_indicator}' in {file_path.name}")
                else:
                    genre = 'Unknown'
                    self.logger.debug(f"Genere sconosciuto per {file_path.name} - assegnato 'Unknown'")
            
            # 7. Sposta in cartella del genere
            primary_genre = self.genre_mapping.get(raw_genre.lower(), raw_genre) if raw_genre != 'Unknown' else 'Unknown'
            success = self.move_to_genre_folder(file_path, primary_genre, final_metadata, original_raw_genre=raw_genre)
            if success:
                self.moved_files += 1
                return True
            else:
                self.uncatalogued_files.append({
                    'file': file_path.name,
                    'reason': 'Failed to move file',
                    'metadata': final_metadata,
                    'external_found': bool(external_metadata),
                    'genre_attempted': genre
                })
                return False
                
        except Exception as e:
            self.logger.error(f"Errore inaspettato processando {file_path.name}: {e}")
            if self.verbose:
                import traceback
                self.logger.error(traceback.format_exc())
            
            self.uncatalogued_files.append({
                'file': file_path.name,
                'reason': f'Unexpected error: {str(e)}',
                'metadata': {},
                'external_found': False,
                'genre_attempted': None
            })
            return False
            
    def move_to_genre_folder(self, file_path: Path, genre: str, metadata: Dict, original_raw_genre: str = None) -> bool:
        """Sposta il file in una cartella per genere con gestione potenziata degli errori"""
        if not genre or genre == "None":
            self.logger.error(f"Genere non valido per {file_path.name}: '{genre}'")
            return False
        
        try:
            # Verifica se il genere originale è un sottogenere Latino
            final_genre_folder = Path(self.clean_filename(genre))
            
            # Controlla se il genere originale è nella lista dei sottogeneri di 'Latin'
            if original_raw_genre and original_raw_genre.lower() in [s.lower() for s in self.subgenre_mapping.get('Latin', [])]:
                # Crea la cartella del sottogenere sotto 'Latin'
                final_genre_folder = Path('Latin') / self.clean_filename(original_raw_genre.capitalize())
            else:
                # Altrimenti, usa il genere principale come nome della cartella
                final_genre_folder = Path(self.clean_filename(genre))
            
            destination_folder = self.base_path / final_genre_folder
            self.logger.debug(f"Cartella destinazione: {destination_folder}")
            
            if self.dry_run:
                self.logger.info(f"[SIMULAZIONE] Sposterei {file_path.name} -> {final_genre_folder}/")
                return True
            
            if not file_path.exists():
                self.logger.warning(f"File non più esistente al momento dello spostamento: {file_path.name}")
                return False
            
            destination_folder.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Cartella creata/verificata: {destination_folder}")
            
            # Gestione dei conflitti di nome file
            destination = destination_folder / file_path.name
            original_name = file_path.name
            
            if destination.exists():
                counter = 1
                stem = file_path.stem
                suffix = file_path.suffix
                while destination.exists():
                    new_name = f"{stem}_{counter}{suffix}"
                    destination = destination_folder / new_name
                    counter += 1
                self.logger.debug(f"Nome file modificato per evitare conflitti: {destination.name}")
            
            shutil.move(str(file_path), str(destination))
            moved_name = destination.name
            
            if moved_name != original_name:
                self.logger.info(f"├── Spostamento completato {original_name} -> {final_genre_folder}/{moved_name} (rinominato)")
            else:
                self.logger.info(f"├── Spostamento completato {original_name} -> {final_genre_folder}/")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Errore spostando {file_path.name} in '{genre}': {e}")
            return False
        
    def estimate_bpm_from_audio(self, file_path):
        """Stima il BPM dall'analisi audio (implementazione base)"""
        try:
            # Implementazione semplificata - richiede librerie come librosa
            # Per ora restituisce None, ma si può estendere
            return None
        except Exception as e:
            self.logger.debug(f"Errore stima BPM per {file_path}: {e}")
            return None
    
    def scan_and_catalog(self):
        """Scansiona e cataloga tutti i file MP3"""
        self.logger.info("Inizio scansione file MP3...")
        
        # Trova tutti i file MP3 nella directory principale (non nelle sottocartelle)
        mp3_files = []
        for pattern in ["*.mp3", "*.MP3"]:
            mp3_files.extend([f for f in self.base_path.glob(pattern) if f.is_file()])
        
        if not mp3_files:
            self.logger.warning("Nessun file MP3 trovato nella directory principale")
            return
        
        self.logger.info(f"Trovati {len(mp3_files)} file MP3 da elaborare")
        
        # NUOVO: Crea lista statica dei file da processare per evitare re-scansioni
        files_to_process = list(mp3_files)  # Copia statica
        
        # Processa ogni file
        for mp3_file in files_to_process:
            self.processed_files += 1
            
            # NUOVO: Controlla se il file esiste ancora prima di processarlo
            if not mp3_file.exists():
                self.logger.debug(f"File non più esistente: {mp3_file.name}")
                continue
                
            try:
                self.process_mp3_file(mp3_file)
            except Exception as e:
                self.logger.error(f"Errore processando {mp3_file.name}: {e}")
                self.uncatalogued_files.append({
                    'file': mp3_file.name,
                    'reason': f'Errore processamento: {e}',
                    'metadata': {},
                    'external_found': False
                })
        
        self.logger.info(f"Elaborazione completata. Processati {len(files_to_process)} file")
    
    def analyze_collection(self):
        """Analizza la collezione esistente per statistiche"""
        self.logger.info("Analisi collezione in corso...")
        
        genre_stats = {}
        year_stats = {}
        
        # Scansiona le cartelle genere esistenti
        for genre_folder in self.base_path.iterdir():
            if genre_folder.is_dir() and not genre_folder.name.startswith('.'):
                mp3_count = len(list(genre_folder.glob("*.mp3")) + list(genre_folder.glob("*.MP3")))
                if mp3_count > 0:
                    genre_stats[genre_folder.name] = mp3_count
        
        # Log statistiche
        if genre_stats:
            self.logger.info("=== STATISTICHE COLLEZIONE ===")
            for genre, count in sorted(genre_stats.items(), key=lambda x: x[1], reverse=True):
                self.logger.info(f"{genre}: {count} file")
        
        return genre_stats
    
    def cleanup_empty_folders(self):
        """Rimuove cartelle vuote"""
        if self.dry_run:
            self.logger.info("[SIMULAZIONE] Controllerei cartelle vuote da rimuovere")
            return
        
        removed_count = 0
        for folder in self.base_path.iterdir():
            if folder.is_dir() and not folder.name.startswith('.'):
                try:
                    # Controlla se la cartella è vuota
                    if not any(folder.iterdir()):
                        folder.rmdir()
                        self.logger.info(f"Rimossa cartella vuota: {folder.name}")
                        removed_count += 1
                except Exception as e:
                    self.logger.debug(f"Errore rimozione cartella {folder.name}: {e}")
        
        if removed_count > 0:
            self.logger.info(f"Rimosse {removed_count} cartelle vuote")
    
    def generate_report(self):
        """Genera un report finale nella directory dello script"""
        # CAMBIATO: report nella directory dello script
        report_file = self.script_dir / f"cataloging_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Analizza collezione finale
        genre_stats = self.analyze_collection()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'base_path': str(self.base_path),
            'script_directory': str(self.script_dir),  # NUOVO: includi directory script nel report
            'configuration': {
                'dry_run': self.dry_run,
                'external_db_enabled': self.use_external_db,
                'api_calls_made': self.api_calls
            },
            'statistics': {
                'total_processed': self.processed_files,
                'successfully_moved': self.moved_files,
                'metadata_updated': self.updated_files,
                'uncatalogued': len(self.uncatalogued_files),
                'genres_found': len(genre_stats),
                'cache_hits': len(self.metadata_cache),
                'genre_normalizations': len(self.genre_cache)
            },
            'genre_distribution': genre_stats,
            'uncatalogued_files': self.uncatalogued_files,
            'performance_metrics': {
                'files_per_minute': round(self.processed_files / max(1, (time.time() - getattr(self, 'start_time', time.time())) / 60), 2),
                'api_calls_per_file': round(self.api_calls / max(1, self.processed_files), 2) if self.use_external_db else 0
            }
        }
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Report dettagliato salvato in: {report_file}")
        except Exception as e:
            self.logger.error(f"Errore salvataggio report: {e}")
        
        # Stampa summary migliorato
        mode_text = "SIMULAZIONE" if self.dry_run else "CATALOGAZIONE"
        self.logger.info(f"=== RIEPILOGO {mode_text} ===")
        self.logger.info(f"File processati: {self.processed_files}")
        
        if self.dry_run:
            self.logger.info(f"File che sarebbero stati spostati: {self.moved_files}")
            self.logger.info(f"File con metadati che sarebbero stati aggiornati: {self.updated_files}")
        else:
            self.logger.info(f"File spostati: {self.moved_files}")
            self.logger.info(f"Metadati aggiornati: {self.updated_files}")
        
        self.logger.info(f"File non catalogati: {len(self.uncatalogued_files)}")
        self.logger.info(f"Generi diversi trovati: {len(genre_stats)}")
        
        if self.use_external_db:
            self.logger.debug(f"Chiamate API database esterni: {self.api_calls}")
            cache_hit_rate = (len(self.metadata_cache) / max(1, self.api_calls)) * 100
            self.logger.debug(f"Cache hit rate: {cache_hit_rate:.1f}%")
        
        # Mostra top generi
        if genre_stats:
            self.logger.info("\n=== TOP 10 GENERI ===")
            top_genres = sorted(genre_stats.items(), key=lambda x: x[1], reverse=True)[:10]
            for genre, count in top_genres:
                self.logger.info(f"{genre}: {count} file")
        
        # Mostra file non catalogati con dettagli
        if self.uncatalogued_files:
            self.logger.warning(f"\n=== FILE NON CATALOGATI ({len(self.uncatalogued_files)}) ===")
            for file_info in self.uncatalogued_files:
                external_info = " [DB esterno consultato]" if file_info.get('external_found') else ""
                self.logger.warning(f"  - {file_info['file']}: {file_info['reason']}{external_info}")
    
    def save_cache(self):
        """Salva la cache nella directory dello script"""
        # CAMBIATO: cache nella directory dello script invece che nella directory delle musiche
        cache_file = self.script_dir / "metadata_cache.json"
        
        cache_data = {
            'metadata_cache': self.metadata_cache,
            'genre_cache': self.genre_cache,
            'last_updated': datetime.now().isoformat(),
            'base_path': str(self.base_path)  # Salva anche il percorso usato
        }
        
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Cache salvata in: {cache_file}")
        except Exception as e:
            self.logger.error(f"Errore salvataggio cache: {e}")
    
    def load_cache(self):
        """Carica la cache dalla directory dello script"""
        # CAMBIATO: cerca la cache nella directory dello script
        cache_file = self.script_dir / "metadata_cache.json"
        
        if not cache_file.exists():
            self.logger.info("Nessuna cache esistente trovata")
            return
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            self.metadata_cache = cache_data.get('metadata_cache', {})
            self.genre_cache = cache_data.get('genre_cache', {})
            
            last_updated = cache_data.get('last_updated', '')
            cached_base_path = cache_data.get('base_path', '')
            
            self.logger.info(f"Cache caricata: {len(self.metadata_cache)} metadati, {len(self.genre_cache)} generi")
            self.logger.info(f"Ultimo aggiornamento cache: {last_updated}")
            
            # NUOVO: Avvisa se la cache è per una directory diversa
            if cached_base_path and cached_base_path != str(self.base_path):
                self.logger.warning(f"Cache era per directory diversa: {cached_base_path}")
                self.logger.warning("I risultati potrebbero non essere ottimali")
            
        except Exception as e:
            self.logger.warning(f"Errore caricamento cache: {e}")
            # Reset cache in caso di errore
            self.metadata_cache = {}
            self.genre_cache = {}

    def cleanup_old_cache(self, days_old=30):
        """Rimuove cache più vecchie di X giorni"""
        cache_file = self.script_dir / "metadata_cache.json"
        
        if not cache_file.exists():
            return
        
        try:
            # Controlla l'età del file cache
            cache_age = time.time() - cache_file.stat().st_mtime
            cache_age_days = cache_age / (24 * 3600)
            
            if cache_age_days > days_old:
                cache_file.unlink()
                self.logger.info(f"Cache rimossa (vecchia di {cache_age_days:.1f} giorni)")
            else:
                self.logger.debug(f"Cache mantenuta (età: {cache_age_days:.1f} giorni)")
                
        except Exception as e:
            self.logger.debug(f"Errore controllo età cache: {e}")
    
    def backup_cache(self):
        """Crea backup della cache esistente"""
        cache_file = self.script_dir / "metadata_cache.json"
        
        if not cache_file.exists():
            return
        
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = self.script_dir / f"metadata_cache_backup_{timestamp}.json"
            
            shutil.copy2(str(cache_file), str(backup_file))
            self.logger.info(f"Backup cache creato: {backup_file}")
            
            # Mantieni solo gli ultimi 5 backup
            backup_pattern = self.script_dir.glob("metadata_cache_backup_*.json")
            backups = sorted(backup_pattern, key=lambda x: x.stat().st_mtime, reverse=True)
            
            for old_backup in backups[5:]:  # Rimuovi backup oltre i primi 5
                old_backup.unlink()
                self.logger.debug(f"Rimosso vecchio backup: {old_backup.name}")
                
        except Exception as e:
            self.logger.warning(f"Errore creazione backup cache: {e}")

    def get_script_directory():
        """Restituisce la directory dove si trova lo script"""
        if hasattr(sys, '_MEIPASS'):
            # PyInstaller executable
            return Path(sys.executable).parent
        else:
            # Python script
            return Path(__file__).parent.absolute()
    
# AGGIORNAMENTO al main() per gestire meglio cache e backup
def main():
    parser = argparse.ArgumentParser(
        description='MP3 Cataloger Avanzato - Cataloga file MP3 per genere con database esterni',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Esempi:
  %(prog)s /path/to/music                    # Catalogazione normale
  %(prog)s /path/to/music --dry-run          # Modalità simulazione
  %(prog)s /path/to/music -v --no-external   # Verbose senza DB esterni
  %(prog)s /path/to/music --analyze-only     # Solo analisi collezione esistente

Note:
  - Il programma usa MusicBrainz per migliorare i metadati
  - La cache velocizza le operazioni successive
  - I file vengono spostati dalla directory principale alle sottocartelle per genere
        '''
    )
    
    parser.add_argument('path', help='Percorso della directory contenente i file MP3')
    parser.add_argument('-v', '--verbose', action='store_true', help='Output dettagliato (debug)')
    parser.add_argument('--dry-run', action='store_true', help='Modalità simulazione (non sposta file)')
    parser.add_argument('--no-external', action='store_true', help='Disabilita ricerca database esterni')
    parser.add_argument('--analyze-only', action='store_true', help='Solo analisi collezione esistente')
    parser.add_argument('--cleanup', action='store_true', help='Rimuovi cartelle vuote alla fine')
    
    args = parser.parse_args()
    
    # Verifica che il percorso esista
    if not os.path.exists(args.path):
        print(f"ERRORE: Il percorso {args.path} non esiste")
        sys.exit(1)
    
    # Configura livello di log
    log_level = logging.DEBUG if args.verbose else logging.INFO
    
    # Inizializza catalogatore
    cataloger = MusicCatalogerAdvanced(
        args.path, 
        log_level, 
        args.dry_run,
        use_external_db=not args.no_external
    )
    
    # Salva tempo di inizio per metriche
    cataloger.start_time = time.time()
    
    if args.dry_run:
        cataloger.logger.info("NOTA: Nessuna modifica sarà effettuata ai file")
    
    if args.no_external:
        cataloger.logger.info("NOTA: Database esterni disabilitati")
    
    try:
        # NUOVO: Opzione per pulire cache vecchie
        if hasattr(args, 'clean_cache') and args.clean_cache:
            cataloger.cleanup_old_cache()
        
        # NUOVO: Backup cache esistente prima di iniziare
        if not args.dry_run:
            cataloger.backup_cache()
        
        # Carica cache
        cataloger.load_cache()
        
        if args.analyze_only:
            # Solo analisi
            cataloger.analyze_collection()
        else:
            # Esegui catalogazione completa
            cataloger.scan_and_catalog()
            
            # Cleanup se richiesto
            if args.cleanup:
                cataloger.cleanup_empty_folders()
        
        # Genera report
        cataloger.generate_report()
        
        # Salva cache se sono stati fatti aggiornamenti
        if cataloger.api_calls > 0 or cataloger.updated_files > 0:
            cataloger.save_cache()
        
    except KeyboardInterrupt:
        cataloger.logger.info("Catalogazione interrotta dall'utente")
        if cataloger.api_calls > 0:
            cataloger.save_cache()
    except Exception as e:
        cataloger.logger.error(f"Errore durante la catalogazione: {e}")
        if cataloger.verbose:
            import traceback
            cataloger.logger.error(traceback.format_exc())
        sys.exit(1)
        
if __name__ == "__main__":
    main()