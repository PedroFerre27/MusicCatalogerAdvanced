#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MP3 Cataloger Avanzato - Catalogatore MP3 per Genere con DB Esterni
Cataloga i file MP3 per genere, li smista in cartelle e aggiorna i metadati usando database musicali esterni.
"""

import os
import sys
import shutil
import logging
from pathlib import Path
from datetime import datetime
import argparse
import json
import re
import requests
from urllib.parse import quote
import time
from io import BytesIO
from PIL import Image
import hashlib

try:
    import eyed3
    eyed3.log.setLevel("ERROR")  # Riduce i log di eyed3
except ImportError:
    print("ERRORE: eyed3 non installato. Installa con: pip install eyed3")
    sys.exit(1)

try:
    from mutagen.mp3 import MP3
    from mutagen.id3 import ID3, TIT2, TPE1, TCON, TDRC, TBPM, TALB, APIC
    from mutagen._file import FileType
except ImportError:
    print("ERRORE: mutagen non installato. Installa con: pip install mutagen")
    sys.exit(1)

try:
    import musicbrainzngs
except ImportError:
    print("ERRORE: musicbrainzngs non installato. Installa con: pip install musicbrainzngs")
    sys.exit(1)

class MP3CatalogerAdvanced:
    def __init__(self, base_path, log_level=logging.INFO, dry_run=False, use_external_db=True):
        self.base_path = Path(base_path)
        self.dry_run = dry_run
        self.use_external_db = use_external_db
        self.uncatalogued_files = []
        self.processed_files = 0
        self.moved_files = 0
        self.updated_files = 0
        self.api_calls = 0
        self.api_limit_per_second = 1  # Rate limiting per API
        self.last_api_call = 0
        
        # Configurazione logging
        self.setup_logging(log_level)
        
        # Configurazione MusicBrainz
        if self.use_external_db:
            musicbrainzngs.set_useragent("MP3Cataloger", "0.2", "captainjoker27@gmail.com")
            
        # Cache per evitare chiamate API duplicate
        self.metadata_cache = {}
        self.genre_cache = {}
        
        # Mapping generi esteso e multilingua
        self.genre_mapping = {
            # Inglese
            'rock': 'Rock', 'pop': 'Pop', 'jazz': 'Jazz', 'classical': 'Classical',
            'hip hop': 'Hip Hop', 'hip-hop': 'Hip Hop', 'electronic': 'Electronic',
            'dance': 'Dance', 'country': 'Country', 'blues': 'Blues', 'r&b': 'R&B',
            'reggae': 'Reggae', 'folk': 'Folk', 'metal': 'Metal', 'punk': 'Punk',
            'alternative': 'Alternative', 'indie': 'Indie', 'soundtrack': 'Soundtrack',
            'world': 'World Music', 'ambient': 'Ambient', 'experimental': 'Experimental',
            'house': 'House', 'techno': 'Techno', 'trance': 'Trance', 'dubstep': 'Dubstep',
            'drum and bass': 'Drum & Bass', 'dnb': 'Drum & Bass', 'garage': 'Garage',
            
            # Italiano
            'rock italiano': 'Rock', 'pop italiano': 'Pop', 'musica italiana': 'Italian Music',
            'cantautori': 'Italian Singer-Songwriter', 'napoletana': 'Neapolitan',
            'classica': 'Classical', 'opera': 'Opera', 'folk italiano': 'Italian Folk',
            
            # Spagnolo
            'rock en español': 'Latin Rock', 'pop latino': 'Latin Pop', 'salsa': 'Salsa',
            'merengue': 'Merengue', 'bachata': 'Bachata', 'reggaeton': 'Reggaeton',
            'cumbia': 'Cumbia', 'tango': 'Tango', 'flamenco': 'Flamenco',
            'música tropical': 'Tropical', 'bolero': 'Bolero', 'ranchera': 'Ranchera',
            
            # Francese
            'chanson française': 'French Chanson', 'pop française': 'French Pop',
            'rock français': 'French Rock', 'variété française': 'French Variety',
            
            # Altri generi comuni
            'soul': 'Soul', 'funk': 'Funk', 'disco': 'Disco', 'new wave': 'New Wave',
            'grunge': 'Grunge', 'progressive': 'Progressive', 'symphonic': 'Symphonic',
            'acoustic': 'Acoustic', 'instrumental': 'Instrumental', 'vocal': 'Vocal',
            'christmas': 'Christmas', 'holiday': 'Holiday', 'children': 'Children',
            'meditation': 'Meditation', 'new age': 'New Age', 'nature': 'Nature Sounds',
            
            # Generi elettronici specifici
            'breakbeat': 'Breakbeat', 'jungle': 'Jungle', 'hardcore': 'Hardcore',
            'minimal': 'Minimal', 'deep house': 'Deep House', 'tech house': 'Tech House',
            
            # Generi metal specifici
            'death metal': 'Death Metal', 'black metal': 'Black Metal', 'power metal': 'Power Metal',
            'heavy metal': 'Heavy Metal', 'thrash metal': 'Thrash Metal', 'doom metal': 'Doom Metal',
            
            # Generi jazz specifici
            'smooth jazz': 'Smooth Jazz', 'bebop': 'Bebop', 'fusion': 'Jazz Fusion',
            'swing': 'Swing', 'big band': 'Big Band', 'latin jazz': 'Latin Jazz'
        }
        
        # Mapping BPM per genere (valori approssimativi)
        self.genre_bpm_ranges = {
            'House': (120, 130), 'Techno': (120, 140), 'Trance': (128, 140),
            'Dubstep': (70, 75), 'Drum & Bass': (160, 180), 'Hip Hop': (70, 140),
            'Pop': (100, 130), 'Rock': (100, 140), 'Metal': (100, 180),
            'Classical': (60, 120), 'Jazz': (60, 200), 'Reggae': (60, 90),
            'Salsa': (150, 250), 'Merengue': (120, 160), 'Bachata': (120, 130)
        }
    
    def setup_logging(self, log_level):
        """Configura il sistema di logging"""
        log_filename = f"mp3_cataloger_advanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_path = self.base_path / log_filename
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        if self.dry_run:
            self.logger.info("=== MODALITÀ SIMULAZIONE ATTIVA ===")
            self.logger.info("Nessun file sarà spostato o modificato")
        self.logger.info(f"Avvio catalogazione MP3 avanzata in: {self.base_path}")
        self.logger.info(f"Database esterni: {'ABILITATI' if self.use_external_db else 'DISABILITATI'}")
        self.logger.info(f"Log salvato in: {log_path}")
    
    def rate_limit_api_call(self):
        """Implementa rate limiting per le chiamate API"""
        current_time = time.time()
        time_since_last_call = current_time - self.last_api_call
        
        if time_since_last_call < self.api_limit_per_second:
            sleep_time = self.api_limit_per_second - time_since_last_call
            time.sleep(sleep_time)
        
        self.last_api_call = time.time()
        self.api_calls += 1
    
    def normalize_genre(self, genre):
        """Normalizza il nome del genere usando il mapping esteso"""
        if not genre:
            return None
        
        # Cache check
        genre_lower = genre.lower().strip()
        if genre_lower in self.genre_cache:
            return self.genre_cache[genre_lower]
        
        # Rimuovi caratteri speciali e numeri
        genre_clean = re.sub(r'[^\w\s-àáèéìíòóùúñç]', '', genre_lower)
        genre_clean = re.sub(r'\d+', '', genre_clean).strip()
        
        # Cerca corrispondenza esatta
        if genre_clean in self.genre_mapping:
            normalized = self.genre_mapping[genre_clean]
            self.genre_cache[genre_lower] = normalized
            return normalized
        
        # Cerca corrispondenze parziali
        for key, value in self.genre_mapping.items():
            if key in genre_clean or genre_clean in key:
                normalized = value
                self.genre_cache[genre_lower] = normalized
                return normalized
        
        # Se non trova corrispondenze, usa il genere originale pulito
        normalized = ' '.join(word.capitalize() for word in genre_clean.split())
        self.genre_cache[genre_lower] = normalized
        return normalized
    
    def clean_filename(self, filename):
        """Pulisce il nome del file da caratteri non validi per le cartelle"""
        return re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    def extract_metadata_eyed3(self, file_path):
        """Estrae metadati usando eyed3 con gestione migliorata degli errori"""
        try:
            audiofile = eyed3.load(file_path)
            if audiofile.tag is None:
                return None
            
            # Gestione sicura del BPM
            bpm = None
            if hasattr(audiofile.tag, 'bpm') and audiofile.tag.bpm:
                try:
                    # Gestisce sia liste che valori singoli
                    if isinstance(audiofile.tag.bpm, (list, tuple)):
                        bpm = audiofile.tag.bpm[0] if audiofile.tag.bpm else None
                    else:
                        bpm = audiofile.tag.bpm
                except (IndexError, TypeError, AttributeError):
                    bpm = None
            
            # Gestione sicura delle date
            year = None
            if hasattr(audiofile.tag, 'recording_date') and audiofile.tag.recording_date:
                try:
                    year = audiofile.tag.recording_date.year
                except (AttributeError, ValueError):
                    # Prova ad estrarre l'anno come stringa
                    date_str = str(audiofile.tag.recording_date)
                    year_match = re.search(r'(\d{4})', date_str)
                    if year_match:
                        year = int(year_match.group(1))
            
            # Gestione album
            album = audiofile.tag.album if hasattr(audiofile.tag, 'album') else None
            
            # Gestione genere
            genre = None
            if hasattr(audiofile.tag, 'genre') and audiofile.tag.genre:
                genre = audiofile.tag.genre.name if hasattr(audiofile.tag.genre, 'name') else str(audiofile.tag.genre)
            
            return {
                'title': audiofile.tag.title,
                'artist': audiofile.tag.artist,
                'album': album,
                'genre': genre,
                'year': year,
                'bpm': bpm
            }
        except Exception as e:
            self.logger.warning(f"Errore eyed3 per {file_path.name}: {e}")
            return None
    
    def extract_metadata_mutagen(self, file_path):
        """Estrae metadati usando mutagen (fallback) con gestione migliorata"""
        try:
            audio = MP3(file_path)
            
            # Funzione helper per estrarre valori sicuri
            def safe_get(tag_key, convert_func=str):
                try:
                    value = audio.get(tag_key)
                    if value and len(value) > 0:
                        return convert_func(value[0])
                except (IndexError, ValueError, TypeError):
                    pass
                return None
            
            # Estrazione anno sicura
            year = None
            year_value = safe_get('TDRC')
            if year_value:
                year_match = re.search(r'(\d{4})', str(year_value))
                if year_match:
                    year = int(year_match.group(1))
            
            # Estrazione BPM sicura
            bpm = None
            bpm_value = safe_get('TBPM')
            if bpm_value:
                try:
                    bpm = int(float(str(bpm_value)))
                except (ValueError, TypeError):
                    pass
            
            return {
                'title': safe_get('TIT2'),
                'artist': safe_get('TPE1'),
                'album': safe_get('TALB'),
                'genre': safe_get('TCON'),
                'year': year,
                'bpm': bpm
            }
        except Exception as e:
            self.logger.warning(f"Errore mutagen per {file_path.name}: {e}")
            return None
    
    def search_musicbrainz(self, artist, title, album=None):
        """Cerca metadati su MusicBrainz"""
        if not self.use_external_db:
            return None
        
        # Cache key
        cache_key = f"{artist}|{title}|{album or ''}"
        if cache_key in self.metadata_cache:
            return self.metadata_cache[cache_key]
        
        try:
            self.rate_limit_api_call()
            
            # Costruisci query di ricerca
            query_parts = []
            if artist:
                query_parts.append(f'artist:"{artist}"')
            if title:
                query_parts.append(f'recording:"{title}"')
            if album:
                query_parts.append(f'release:"{album}"')
            
            query = ' AND '.join(query_parts)
            
            # Cerca recording
            result = musicbrainzngs.search_recordings(
                query=query,
                limit=3,
                includes=['artist-credits', 'releases', 'tags', 'genres']
            )
            
            if result['recording-list']:
                recording = result['recording-list'][0]
                
                # Estrai metadati
                metadata = {
                    'title': recording.get('title'),
                    'artist': None,
                    'album': None,
                    'genre': None,
                    'year': None,
                    'bpm': None
                }
                
                # Artista
                if 'artist-credit' in recording:
                    artists = [ac['artist']['name'] for ac in recording['artist-credit'] if 'artist' in ac]
                    if artists:
                        metadata['artist'] = artists[0]
                
                # Album e anno
                if 'release-list' in recording:
                    release = recording['release-list'][0]
                    metadata['album'] = release.get('title')
                    if 'date' in release:
                        year_match = re.search(r'(\d{4})', release['date'])
                        if year_match:
                            metadata['year'] = int(year_match.group(1))
                
                # Generi/tag
                genres = []
                if 'tag-list' in recording:
                    genres.extend([tag['name'] for tag in recording['tag-list']])
                if 'genre-list' in recording:
                    genres.extend([genre['name'] for genre in recording['genre-list']])
                
                if genres:
                    # Usa il primo genere riconosciuto
                    for genre in genres:
                        normalized = self.normalize_genre(genre)
                        if normalized and normalized != genre.title():
                            metadata['genre'] = normalized
                            break
                    if not metadata['genre']:
                        metadata['genre'] = self.normalize_genre(genres[0])
                
                # Stima BPM basata sul genere
                if metadata['genre'] and metadata['genre'] in self.genre_bpm_ranges:
                    bpm_range = self.genre_bpm_ranges[metadata['genre']]
                    metadata['bpm'] = (bpm_range[0] + bpm_range[1]) // 2
                
                # Cache result
                self.metadata_cache[cache_key] = metadata
                return metadata
                
        except Exception as e:
            self.logger.debug(f"Errore ricerca MusicBrainz per {artist} - {title}: {e}")
        
        self.metadata_cache[cache_key] = None
        return None
    
    def search_lastfm(self, artist, title):
        """Cerca metadati su Last.fm (implementazione base)"""
        if not self.use_external_db:
            return None
        
        # Implementazione semplificata - richiede API key
        # Per ora restituisce None, ma si può estendere
        return None
    
    def download_album_art(self, artist, album, file_path):
        """Scarica la cover dell'album (implementazione base)"""
        if not self.use_external_db or self.dry_run:
            return None
        
        try:
            # Implementazione semplificata - si può estendere con API come Last.fm o Spotify
            # Per ora restituisce None
            return None
        except Exception as e:
            self.logger.debug(f"Errore download cover per {artist} - {album}: {e}")
            return None
    
    def update_metadata_mutagen(self, file_path, metadata, cover_data=None):
        """Aggiorna i metadati usando mutagen con supporto per cover"""
        if self.dry_run:
            self.logger.info(f"[SIMULAZIONE] Aggiornerei metadati per: {file_path.name}")
            return True
            
        try:
            audio = MP3(file_path)
            if audio.tags is None:
                audio.add_tags()
            
            updated = False
            
            # Titolo
            if metadata.get('title') and not audio.get('TIT2'):
                audio.tags['TIT2'] = TIT2(encoding=3, text=metadata['title'])
                updated = True
            
            # Artista
            if metadata.get('artist') and not audio.get('TPE1'):
                audio.tags['TPE1'] = TPE1(encoding=3, text=metadata['artist'])
                updated = True
            
            # Album
            if metadata.get('album') and not audio.get('TALB'):
                audio.tags['TALB'] = TALB(encoding=3, text=metadata['album'])
                updated = True
            
            # Genere
            if metadata.get('genre') and not audio.get('TCON'):
                audio.tags['TCON'] = TCON(encoding=3, text=metadata['genre'])
                updated = True
            
            # Anno
            if metadata.get('year') and not audio.get('TDRC'):
                audio.tags['TDRC'] = TDRC(encoding=3, text=str(metadata['year']))
                updated = True
            
            # BPM
            if metadata.get('bpm') and not audio.get('TBPM'):
                audio.tags['TBPM'] = TBPM(encoding=3, text=str(metadata['bpm']))
                updated = True
            
            # Cover art
            if cover_data and not [tag for tag in audio.tags.values() if isinstance(tag, APIC)]:
                audio.tags['APIC'] = APIC(
                    encoding=3,
                    mime='image/jpeg',
                    type=3,  # Cover (front)
                    desc='Cover',
                    data=cover_data
                )
                updated = True
            
            if updated:
                audio.save()
                self.logger.info(f"Metadati aggiornati per: {file_path.name}")
                self.updated_files += 1
            
            return updated
        except Exception as e:
            self.logger.error(f"Errore aggiornamento metadati per {file_path}: {e}")
            return False
    
    def guess_metadata_from_filename(self, file_path):
        """Cerca di indovinare i metadati dal nome del file con pattern estesi"""
        filename = file_path.stem
        
        # Pattern comuni migliorati
        patterns = [
            # Con anno all'inizio
            r'^(\d{4})\s*[-_]\s*(.+?)\s*[-_]\s*(.+?)(?:\s*\[.*\])?$',  # Anno - Artista - Titolo [extra]
            r'^(\d{4})\s*[-_]\s*(.+?)(?:\s*\[.*\])?$',                 # Anno - Artista/Titolo [extra]
            
            # Senza anno
            r'^(.+?)\s*[-_]\s*(.+?)\s*[-_]\s*(.+?)(?:\s*\[.*\])?$',   # Artista - Album - Titolo [extra]
            r'^(.+?)\s*[-_]\s*(.+?)(?:\s*\[.*\])?$',                  # Artista - Titolo [extra]
            
            # Pattern con parentesi per album
            r'^(.+?)\s*\((.+?)\)\s*[-_]\s*(.+?)(?:\s*\[.*\])?$',     # Artista (Album) - Titolo [extra]
            
            # Pattern con numero traccia
            r'^\d+\.?\s*[-_]?\s*(.+?)\s*[-_]\s*(.+?)(?:\s*\[.*\])?$', # 01 - Artista - Titolo [extra]
            r'^\d+\.?\s*[-_]?\s*(.+?)(?:\s*\[.*\])?$',                # 01 - Titolo [extra]
        ]
        
        for pattern in patterns:
            match = re.match(pattern, filename)
            if match:
                groups = match.groups()
                
                if len(groups) >= 3 and groups[0].isdigit():  # Anno - Artista - Titolo
                    return {
                        'year': int(groups[0]),
                        'artist': groups[1].strip(),
                        'title': groups[2].strip()
                    }
                elif len(groups) == 3:  # Artista - Album - Titolo o Artista (Album) - Titolo
                    return {
                        'artist': groups[0].strip(),
                        'album': groups[1].strip(),
                        'title': groups[2].strip()
                    }
                elif len(groups) == 2 and groups[0].isdigit():  # Anno - Artista/Titolo
                    return {
                        'year': int(groups[0]),
                        'title': groups[1].strip()
                    }
                elif len(groups) == 2:  # Artista - Titolo
                    return {
                        'artist': groups[0].strip(),
                        'title': groups[1].strip()
                    }
                elif len(groups) == 1:  # Solo titolo
                    return {
                        'title': groups[0].strip()
                    }
        
        # Fallback: usa il nome del file come titolo
        return {'title': filename}
    
    def merge_metadata(self, existing, external, filename_guess):
        """Unisce i metadati da diverse fonti con priorità"""
        merged = {}
        
        # Priorità: esterno > esistente > filename
        for key in ['title', 'artist', 'album', 'genre', 'year', 'bpm']:
            if external and external.get(key):
                merged[key] = external[key]
            elif existing and existing.get(key):
                merged[key] = existing[key]
            elif filename_guess and filename_guess.get(key):
                merged[key] = filename_guess[key]
        
        return merged
    
    def validate_metadata(self, metadata, file_path):
        """Valida e corregge i metadati"""
        if not metadata:
            return metadata
        
        # Validazione anno
        if metadata.get('year'):
            try:
                year = int(metadata['year'])
                current_year = datetime.now().year
                if year < 1900 or year > current_year + 1:
                    self.logger.warning(f"Anno non valido {year} per {file_path.name}, rimosso")
                    metadata['year'] = None
                else:
                    metadata['year'] = year
            except (ValueError, TypeError):
                metadata['year'] = None
        
        # Validazione BPM
        if metadata.get('bpm'):
            try:
                bpm = int(metadata['bpm'])
                if bpm < 20 or bpm > 300:
                    self.logger.warning(f"BPM non valido {bpm} per {file_path.name}, rimosso")
                    metadata['bpm'] = None
                else:
                    metadata['bpm'] = bpm
            except (ValueError, TypeError):
                metadata['bpm'] = None
        
        # Pulizia stringhe
        for key in ['title', 'artist', 'album', 'genre']:
            if metadata.get(key):
                # Rimuovi caratteri di controllo e spazi extra
                cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', str(metadata[key]))
                cleaned = re.sub(r'\s+', ' ', cleaned).strip()
                metadata[key] = cleaned if cleaned else None
        
        return metadata
    
    def process_mp3_file(self, file_path):
        """Processa un singolo file MP3 con ricerca database esterno"""
        self.logger.info(f"Processando: {file_path.name}")
        
        # 1. Estrai metadati esistenti
        existing_metadata = self.extract_metadata_eyed3(file_path)
        if not existing_metadata:
            existing_metadata = self.extract_metadata_mutagen(file_path)
        
        if not existing_metadata:
            self.logger.warning(f"Impossibile leggere metadati per: {file_path.name}")
            existing_metadata = {}
        
        # 2. Indovina metadati dal filename
        filename_metadata = self.guess_metadata_from_filename(file_path)
        self.logger.debug(f"Metadati da filename per {file_path.name}: {filename_metadata}")
        
        # 3. Cerca su database esterni se abbiamo artista e titolo
        external_metadata = None
        search_artist = existing_metadata.get('artist') or filename_metadata.get('artist')
        search_title = existing_metadata.get('title') or filename_metadata.get('title')
        search_album = existing_metadata.get('album') or filename_metadata.get('album')
        
        if self.use_external_db and search_artist and search_title:
            self.logger.info(f"Ricerca database esterni per: {search_artist} - {search_title}")
            external_metadata = self.search_musicbrainz(search_artist, search_title, search_album)
            if external_metadata:
                self.logger.info(f"Metadati trovati su MusicBrainz per {file_path.name}")
            else:
                external_metadata = self.search_lastfm(search_artist, search_title)
        
        # 4. Unisci i metadati
        final_metadata = self.merge_metadata(existing_metadata, external_metadata, filename_metadata)
        
        # 5. Valida i metadati
        final_metadata = self.validate_metadata(final_metadata, file_path)
        
        # 6. Scarica cover se possibile
        cover_data = None
        if (final_metadata.get('artist') and final_metadata.get('album') and 
            not self.dry_run and self.use_external_db):
            cover_data = self.download_album_art(
                final_metadata['artist'], 
                final_metadata['album'], 
                file_path
            )
        
        # 7. Aggiorna metadati del file
        if final_metadata:
            self.update_metadata_mutagen(file_path, final_metadata, cover_data)
        
        # 8. Normalizza il genere per la catalogazione
        genre = self.normalize_genre(final_metadata.get('genre'))
        
        if not genre:
            self.logger.warning(f"Genere non trovato per: {file_path.name}")
            self.uncatalogued_files.append({
                'file': file_path.name,
                'reason': 'Genere non trovato',
                'metadata': final_metadata,
                'external_found': bool(external_metadata)
            })
            return False
        
        # 9. Crea cartella genere se non esiste
        genre_folder = self.base_path / self.clean_filename(genre)
        
        if self.dry_run:
            self.logger.info(f"[SIMULAZIONE] Creerei cartella: {genre}")
            self.logger.info(f"[SIMULAZIONE] Sposterei {file_path.name} -> {genre}/")
            self.moved_files += 1
            return True
        
        try:
            genre_folder.mkdir(exist_ok=True)
        except Exception as e:
            self.logger.error(f"Errore creazione cartella {genre}: {e}")
            return False
        
        # 10. Sposta il file
        destination = genre_folder / file_path.name
        try:
            # Evita sovrascritture
            if destination.exists():
                counter = 1
                name_parts = file_path.stem, counter, file_path.suffix
                while destination.exists():
                    destination = genre_folder / f"{name_parts[0]}_{name_parts[1]}{name_parts[2]}"
                    counter += 1
            
            shutil.move(str(file_path), str(destination))
            self.logger.info(f"Spostato {file_path.name} -> {genre}/{destination.name}")
            self.moved_files += 1
            return True
            
        except Exception as e:
            self.logger.error(f"Errore spostamento {file_path.name}: {e}")
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
        """Genera un report finale dettagliato"""
        report_file = self.base_path / f"cataloging_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Analizza collezione finale
        genre_stats = self.analyze_collection()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'base_path': str(self.base_path),
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
        """Salva la cache su disco per riutilizzo futuro"""
        cache_file = self.base_path / "metadata_cache.json"
        
        cache_data = {
            'metadata_cache': self.metadata_cache,
            'genre_cache': self.genre_cache,
            'last_updated': datetime.now().isoformat()
        }
        
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Cache salvata in: {cache_file}")
        except Exception as e:
            self.logger.error(f"Errore salvataggio cache: {e}")
    
    def load_cache(self):
        """Carica la cache da disco"""
        cache_file = self.base_path / "metadata_cache.json"
        
        if not cache_file.exists():
            return
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            self.metadata_cache = cache_data.get('metadata_cache', {})
            self.genre_cache = cache_data.get('genre_cache', {})
            
            last_updated = cache_data.get('last_updated', '')
            self.logger.info(f"Cache caricata: {len(self.metadata_cache)} metadati, {len(self.genre_cache)} generi")
            self.logger.info(f"Ultimo aggiornamento cache: {last_updated}")
            
        except Exception as e:
            self.logger.warning(f"Errore caricamento cache: {e}")

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
        if cataloger.api_calls > 0:
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