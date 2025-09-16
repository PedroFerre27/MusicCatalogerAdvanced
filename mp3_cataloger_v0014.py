import argparse
import json
import logging
import os
import re
import shutil
import sys
import time
import warnings  # AGGIUNTO: Import mancante
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


class MP3CatalogerAdvanced:
    def __init__(self, base_path: str, log_level=logging.INFO, dry_run=False, use_external_db=True):
        self.base_path = Path(base_path)
        self.dry_run = dry_run
        self.use_external_db = use_external_db
        self.verbose = log_level == logging.DEBUG
        
        # CORRETTO: Setup logging PRIMA di tutto il resto
        self.setup_logging(log_level)
        
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
        
        # SPOSTATO: Determina la directory dello script DOPO aver configurato il logger
        if hasattr(sys, '_MEIPASS'):
            # Se è un eseguibile PyInstaller
            self.script_dir = Path(sys.executable).parent
        else:
            # Directory dove si trova lo script Python
            self.script_dir = Path(__file__).parent.absolute()
        
        # Ora possiamo usare il logger
        self.logger.info(f"Directory cache: {self.script_dir}")
        self.logger.info(f"Directory musica: {self.base_path}")
        
        # Setup MusicBrainz
        if musicbrainzngs and self.use_external_db:
            musicbrainzngs.set_useragent("MP3CatalogerAdvanced", "1.4", "captainjoker27@gmail.com")
            musicbrainzngs.set_rate_limit(limit_or_interval=1.2, new_requests=1)
        
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

    def setup_logging(self, level):
        """Configura il sistema di logging"""
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    # 1. Fix for the 'float' object has no attribute 'strip' error
    def extract_metadata_eyed3(self, file_path: Path) -> Optional[Dict]:
        """Estrae metadati usando eyed3"""
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
                'year': str(tag.getBestDate()) if tag.getBestDate() else None,
                'genre': str(tag.genre.name) if tag.genre else None,
                'track_num': str(tag.track_num[0]) if tag.track_num and tag.track_num[0] else None,
                'bpm': str(tag.bpm) if tag.bpm else None,
                'duration': float(audiofile.info.time_secs) if audiofile.info else None
            }
            
            # Clean values - handle both string and numeric types
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
    
    # 2. Fix for SSL certificate error with MusicBrainz
    def search_musicbrainz(self, artist: str, title: str, album: str = None) -> Optional[Dict]:
        """Cerca metadati su MusicBrainz"""
        if not musicbrainzngs or not self.use_external_db:
            return None
        
        cache_key = f"mb_{artist}_{title}_{album or ''}"
        if cache_key in self.metadata_cache:
            return self.metadata_cache[cache_key]
        
        try:
            # Rate limiting
            elapsed = time.time() - self.last_musicbrainz_call
            if elapsed < 1.2:
                wait_time = 1.2 - elapsed
                time.sleep(wait_time)
            
            self.last_musicbrainz_call = time.time()
            self.api_calls += 1
            
            # Build query
            query_parts = [f'artist:"{artist}"', f'recording:"{title}"']
            if album:
                query_parts.append(f'release:"{album}"')
            
            query = ' AND '.join(query_parts)
            
            # Execute search with SSL context fix
            import ssl
            import urllib.request
            
            # Create SSL context that allows self-signed certificates
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            # Temporarily patch the urllib opener
            original_opener = urllib.request.build_opener()
            https_handler = urllib.request.HTTPSHandler(context=ssl_context)
            opener = urllib.request.build_opener(https_handler)
            urllib.request.install_opener(opener)
            
            try:
                result = musicbrainzngs.search_recordings(query=query, limit=3)
            finally:
                # Restore original opener
                urllib.request.install_opener(original_opener)
            
            if not result.get('recording-list'):
                self.metadata_cache[cache_key] = None
                return None
            
            # Process first result
            recording = result['recording-list'][0]
            
            try:
                recording_id = recording['id']
                detailed = musicbrainzngs.get_recording_by_id(
                    recording_id, 
                    includes=['releases', 'artist-credits']
                )
                
                rec_data = detailed['recording']
                metadata = {
                    'title': rec_data.get('title'),
                    'artist': artist,
                    'duration': int(rec_data.get('length', 0)) / 1000 if rec_data.get('length') else None
                }
                
                # Get info from first release
                if rec_data.get('release-list'):
                    release = rec_data['release-list'][0]
                    metadata['album'] = release.get('title')
                    if release.get('date'):
                        metadata['year'] = release['date'][:4]
                
                self.metadata_cache[cache_key] = metadata
                return metadata
                
            except Exception as e:
                self.logger.warning(f"Errore dettagli MusicBrainz: {e}")
                self.metadata_cache[cache_key] = None
                return None
                
        except Exception as e:
            self.logger.warning(f"Errore connessione MusicBrainz: {e}")
            self.metadata_cache[cache_key] = None
            return None
    
    # 3. Fix for Last.fm API key issue
    def search_lastfm(self, artist: str, title: str) -> Optional[Dict]:
        """Cerca metadati su Last.fm"""
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
            
            params = {
                'method': 'track.getInfo',
                'artist': artist,
                'track': title,
                'api_key': api_key,
                'format': 'json'
            }
            
            response = requests.get('https://ws.audioscrobbler.com/2.0/', params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'track' in data and 'error' not in data:
                    track = data['track']
                    
                    metadata = {
                        'title': track.get('name'),
                        'artist': track.get('artist', {}).get('name') if isinstance(track.get('artist'), dict) else artist,
                        'album': track.get('album', {}).get('title') if track.get('album') else None
                    }
                    
                    # Extract genres from tags
                    if track.get('toptags', {}).get('tag'):
                        tags = track['toptags']['tag']
                        if isinstance(tags, list) and tags:
                            metadata['genre'] = tags[0]['name']
                        elif isinstance(tags, dict):
                            metadata['genre'] = tags['name']
                    
                    self.metadata_cache[cache_key] = metadata
                    return metadata
                else:
                    self.logger.debug(f"Last.fm error response: {data.get('message', 'Unknown error')}")
            else:
                self.logger.debug(f"Last.fm HTTP error: {response.status_code}")
            
            self.metadata_cache[cache_key] = None
            return None
            
        except Exception as e:
            self.logger.debug(f"Errore Last.fm: {e}")
            self.metadata_cache[cache_key] = None
            return None

    def merge_metadata(self, existing: Dict, external: Optional[Dict], filename: Dict) -> Dict:
        """Unisce metadati da diverse fonti (priorità: esistenti > esterni > filename)"""
        final = {}
        
        # Lista dei campi da processare
        fields = ['title', 'artist', 'album', 'year', 'genre', 'track_num', 'bpm', 'duration']
        
        for field in fields:
            # Priorità: esistenti > esterni > filename
            value = (existing.get(field) or 
                    (external.get(field) if external else None) or 
                    filename.get(field))
            
            if value:
                final[field] = str(value).strip()
        
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
        
        # MIGLIORATO: Prova a indovinare da parole chiave
        genre_words = genre_lower.split()
        for word in genre_words:
            if word in self.genre_mapping:
                normalized = self.genre_mapping[word]
                self.genre_cache[genre_lower] = normalized
                return normalized
        
        # Default a "Other" se non trovato
        self.genre_cache[genre_lower] = "Other"
        return "Other"
    
    def estimate_bpm_from_audio(self, file_path: Path) -> Optional[int]:
        """MIGLIORATO: Stima il BPM dall'analisi audio usando librosa"""
        if not LIBROSA_AVAILABLE or not file_path.exists():
            return None
        
        try:
            # Carica audio
            y, sr = librosa.load(str(file_path), sr=22050, duration=30)  # Prime 30 secondi
            
            # Estrai tempo
            tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
            
            # Arrotonda a intero
            bpm = int(round(tempo))
            
            # Valida range ragionevole
            if 60 <= bpm <= 200:
                self.logger.debug(f"BPM stimato per {file_path.name}: {bpm}")
                return bpm
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Errore stima BPM per {file_path}: {e}")
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
    
    # 5. Better error handling and fallback logic
    def process_mp3_file(self, file_path):
        """Processa un singolo file MP3 con gestione errori migliorata"""
        self.logger.info(f"Processando: {file_path.name}")
        
        try:
            # 1. Extract existing metadata with better error handling
            existing_metadata = {}
            
            # Try eyed3 first
            try:
                existing_metadata = self.extract_metadata_eyed3(file_path) or {}
            except Exception as e:
                self.logger.debug(f"eyed3 failed for {file_path.name}: {e}")
            
            # Try mutagen as fallback
            if not existing_metadata:
                try:
                    existing_metadata = self.extract_metadata_mutagen(file_path) or {}
                except Exception as e:
                    self.logger.debug(f"Mutagen failed for {file_path.name}: {e}")
            
            # 2. Guess from filename
            filename_metadata = self.guess_metadata_from_filename(file_path)
            
            # 3. Search external databases with better error handling
            external_metadata = None
            search_artist = existing_metadata.get('artist') or filename_metadata.get('artist')
            search_title = existing_metadata.get('title') or filename_metadata.get('title')
            search_album = existing_metadata.get('album') or filename_metadata.get('album')
    
            if self.use_external_db and search_artist and search_title:
                # Try MusicBrainz first
                try:
                    external_metadata = self.search_musicbrainz(search_artist, search_title, search_album)
                except Exception as e:
                    self.logger.debug(f"MusicBrainz search failed: {e}")
                
                # Try Last.fm as fallback
                if not external_metadata:
                    try:
                        external_metadata = self.search_lastfm(search_artist, search_title)
                    except Exception as e:
                        self.logger.debug(f"Last.fm search failed: {e}")
            
            # 4. Merge and validate metadata
            final_metadata = self.merge_metadata(existing_metadata, external_metadata, filename_metadata)
            final_metadata = self.validate_metadata(final_metadata, file_path)
            
            # 5. Update file metadata
            if final_metadata and not self.dry_run:
                try:
                    self.update_metadata_mutagen(file_path, final_metadata)
                    self.updated_files += 1
                except Exception as e:
                    self.logger.warning(f"Failed to update metadata for {file_path.name}: {e}")
            
            # 6. Normalize genre with better fallback
            genre = self.normalize_genre(final_metadata.get('genre'))
            
            # If no genre found, try to guess from artist or filename
            if not genre or genre == "Other":
                # Check if it's Latin music based on artist name patterns
                artist_name = (final_metadata.get('artist', '') + ' ' + file_path.stem).lower()
                latin_indicators = ['chiquito', 'salsa', 'bachata', 'merengue', 'reggaeton', 'tropical']
                
                if any(indicator in artist_name for indicator in latin_indicators):
                    genre = 'Latin'
                    self.logger.info(f"Inferred Latin genre for {file_path.name}")
                else:
                    # Default to "Unknown" instead of failing
                    genre = 'Unknown'
                    self.logger.info(f"Using 'Unknown' genre for {file_path.name}")
            
            # 7. Create genre folder and move file
            success = self.move_to_genre_folder(file_path, genre, final_metadata)
            
            if success:
                self.moved_files += 1
                return True
            else:
                self.uncatalogued_files.append({
                    'file': file_path.name,
                    'reason': 'Failed to move file',
                    'metadata': final_metadata,
                    'external_found': bool(external_metadata)
                })
                return False
                
        except Exception as e:
            self.logger.error(f"Unexpected error processing {file_path.name}: {e}")
            self.uncatalogued_files.append({
                'file': file_path.name,
                'reason': f'Unexpected error: {str(e)}',
                'metadata': {},
                'external_found': False
            })
            return False

    def move_to_genre_folder(self, file_path: Path, genre: str, metadata: Dict) -> bool:
        """Move file to genre folder with better error handling"""
        try:
            genre_folder = self.base_path / self.clean_filename(genre)
            
            if self.dry_run:
                self.logger.info(f"[SIMULATION] Would move {file_path.name} -> {genre}/")
                return True
            
            # Create genre folder
            genre_folder.mkdir(exist_ok=True)
            
            # Handle filename conflicts
            destination = genre_folder / file_path.name
            if destination.exists():
                counter = 1
                stem = file_path.stem
                suffix = file_path.suffix
                while destination.exists():
                    new_name = f"{stem}_{counter}{suffix}"
                    destination = genre_folder / new_name
                    counter += 1
            
            # Move file
            shutil.move(str(file_path), str(destination))
            self.logger.info(f"Moved {file_path.name} -> {genre}/{destination.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error moving {file_path.name} to {genre}: {e}")
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
        
        self.logger.info(f"Trovati {len(mp3_files)} file MP3")
        
        # Processa ogni file
        for mp3_file in mp3_files:
            self.processed_files += 1
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
            self.logger.info(f"Chiamate API database esterni: {self.api_calls}")
            cache_hit_rate = (len(self.metadata_cache) / max(1, self.api_calls)) * 100
            self.logger.info(f"Cache hit rate: {cache_hit_rate:.1f}%")
        
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

    # NUOVO: Metodo per pulire cache vecchie
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
    
    # NUOVO: Metodo per backup cache
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

    # NUOVO: Funzione di utilità per ottenere la directory dello script
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
    cataloger = MP3CatalogerAdvanced(
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