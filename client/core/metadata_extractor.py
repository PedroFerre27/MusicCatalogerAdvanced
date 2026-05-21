"""
Estrazione e gestione metadati MP3
Estratto da MusicCatalogerAdvanced_v0020.py
"""

import logging
import re
import warnings
from pathlib import Path
from typing import Dict, Optional

try:
    import eyed3
    eyed3.log.setLevel("ERROR")
except ImportError:
    eyed3 = None

try:
    import mutagen
    from mutagen.mp3 import MP3
    from mutagen.id3 import ID3, APIC, TIT2, TPE1, TALB, TDRC, TCON, TBPM
except ImportError:
    mutagen = None


class MetadataExtractor:
    """
    Classe per estrarre, unire e validare metadati MP3
    """
    
    def __init__(self, settings, logger=None):
        """
        Inizializza l'estrattore metadati
        
        Args:
            settings: Oggetto con le configurazioni
            logger: Logger per output (opzionale)
        """
        self.settings = settings
        self.logger = logger or logging.getLogger(__name__)
        
        # Verifica disponibilità librerie
        self.eyed3_available = eyed3 is not None
        self.mutagen_available = mutagen is not None
        
        if not self.eyed3_available:
            self.logger.warning("eyed3 non disponibile")
        if not self.mutagen_available:
            self.logger.warning("mutagen non disponibile")
    
    def extract_metadata_eyed3(self, file_path: Path) -> Optional[Dict]:
        """
        Estrae metadati usando eyed3
        
        Args:
            file_path: Path del file MP3
            
        Returns:
            Dict con metadati o None
        """
        if not self.eyed3_available:
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
            
            # Estrai anno con gestione deprecation warning
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", DeprecationWarning)
                    best_date = tag.getBestDate()
                
                if best_date:
                    if hasattr(best_date, 'year') and best_date.year:
                        metadata['year'] = str(best_date.year)
                    else:
                        # Fallback regex
                        date_str = str(best_date)
                        year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
                        if year_match:
                            metadata['year'] = year_match.group(0)
            except Exception as e:
                self.logger.debug(f"Errore parsing data eyed3: {e}")
            
            # Pulisci valori
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
        """
        Estrae metadati usando Mutagen
        
        Args:
            file_path: Path del file MP3
            
        Returns:
            Dict con metadati o None
        """
        if not self.mutagen_available:
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
                'duration': float(audio.info.length) if hasattr(audio, 'info') and audio.info else None,
                'bitrate': int(audio.info.bitrate // 1000) if hasattr(audio, 'info') and audio.info and hasattr(audio.info, 'bitrate') else None,
            }
            
            # Pulisci valori
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
            self.logger.warning(f"Errore Mutagen per {file_path.name}: {e}")
            return None
    
    def guess_metadata_from_filename(self, file_path: Path) -> Dict:
        """
        Indovina metadati dal nome del file
        
        Args:
            file_path: Path del file MP3
            
        Returns:
            Dict con metadati dedotti
        """
        filename = file_path.stem
        
        # Pattern comuni
        patterns = [
            r'^(.+?)\s*-\s*(.+)$',              # "Artist - Title"
            r'^(.+?)\s*—\s*(.+)$',              # "Artist — Title" (em dash)
            r'^(\d+)\.\s*(.+?)\s*-\s*(.+)$',   # "01. Artist - Title"
            r'^(.+?)\s*_\s*(.+)$',              # "Artist _ Title"
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
            # Fallback: usa tutto come titolo
            metadata['title'] = filename
        
        return metadata
    
    def merge_metadata(self, existing: Dict, external: Optional[Dict], filename: Dict) -> Dict:
        """
        Unisce metadati con priorità intelligente
        Priorità: esistenti > esterni > filename
        
        Args:
            existing: Metadati dal file
            external: Metadati da DB esterni (opzionale)
            filename: Metadati dedotti dal nome file
            
        Returns:
            Dict con metadati uniti
        """
        final = {}
        
        # Campi base
        base_fields = ['title', 'artist', 'album', 'genre', 'track_num']
        
        for field in base_fields:
            value = (existing.get(field) or 
                    (external.get(field) if external else None) or 
                    filename.get(field))
            
            if value:
                final[field] = str(value).strip()
        
        # Anno con priorità
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
        
        # BPM: preferisci valori misurati
        bpm_sources = [
            existing.get('bpm'),
            external.get('bpm') if external else None,
            external.get('bpm_estimated') if external else None
        ]
        
        for bpm in bpm_sources:
            if bpm:
                final['bpm'] = str(bpm).strip()
                break
        
        # Campi aggiuntivi da esterni
        if external:
            additional_fields = ['playcount', 'popularity', 'lastfm_url', 
                               'spotify_url', 'cover_url', 'all_genres']
            for field in additional_fields:
                if external.get(field):
                    final[field] = external[field]
        
        return final
    
    def validate_metadata(self, metadata: Dict, file_path: Path) -> Dict:
        """
        Valida e pulisce i metadati
        
        Args:
            metadata: Dict con metadati da validare
            file_path: Path del file (per log)
            
        Returns:
            Dict con metadati validati
        """
        validated = {}
        
        for key, value in metadata.items():
            if not value:
                continue
            
            value = str(value).strip()
            
            if key == 'year':
                # Estrai solo l'anno (4 cifre)
                year_match = re.search(r'\b(19|20)\d{2}\b', value)
                if year_match:
                    validated[key] = year_match.group(0)
            
            elif key == 'bpm':
                # Valida BPM numerico
                try:
                    bpm_val = float(value)
                    min_bpm = self.settings.bpm.valid_range_min
                    max_bpm = self.settings.bpm.valid_range_max
                    
                    if min_bpm <= bpm_val <= max_bpm:
                        validated[key] = str(int(bpm_val))
                    else:
                        self.logger.debug(f"BPM fuori range ({bpm_val}): {file_path.name}")
                except ValueError:
                    self.logger.debug(f"BPM non numerico: {value}")
            
            elif key == 'track_num':
                # Pulisci numero traccia (es: "3/12" -> "3")
                track_match = re.search(r'\d+', value)
                if track_match:
                    validated[key] = track_match.group(0)
            
            elif key == 'duration':
                # Mantieni duration come numero
                try:
                    validated[key] = float(value)
                except (ValueError, TypeError):
                    pass
            
            else:
                # Altri campi: semplice pulizia
                validated[key] = value
        
        return validated
    
    def update_metadata_mutagen(self, file_path: Path, metadata: Dict, 
                                cover_data: bytes = None) -> bool:
        """
        Aggiorna metadati del file usando Mutagen
        
        Args:
            file_path: Path del file MP3
            metadata: Dict con metadati da scrivere
            cover_data: Dati immagine cover (opzionale)
            
        Returns:
            True se successo, False altrimenti
        """
        if not self.mutagen_available:
            self.logger.warning("Mutagen non disponibile per aggiornamento")
            return False
        
        try:
            audio = MP3(str(file_path))
            
            # Assicura tag ID3
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
            self.logger.debug(f"Metadati aggiornati: {file_path.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Errore aggiornamento metadati {file_path.name}: {e}")
            return False
    
    def extract_all_metadata(self, file_path: Path) -> Dict:
        """
        Estrae metadati usando tutti i metodi disponibili
        Priorità: eyed3 > mutagen > filename
        
        Args:
            file_path: Path del file MP3
            
        Returns:
            Dict con metadati completi
        """
        metadata = {}
        
        # 1. Prova eyed3
        if self.eyed3_available:
            metadata = self.extract_metadata_eyed3(file_path) or {}
            if metadata:
                self.logger.debug(f"eyed3: {list(metadata.keys())}")
        
        # 2. Fallback mutagen se eyed3 fallisce
        if not metadata and self.mutagen_available:
            metadata = self.extract_metadata_mutagen(file_path) or {}
            if metadata:
                self.logger.debug(f"mutagen: {list(metadata.keys())}")
        
        # 3. Arricchisci con filename
        filename_metadata = self.guess_metadata_from_filename(file_path)
        
        # Unisci dando priorità ai metadati estratti
        for key, value in filename_metadata.items():
            if key not in metadata or not metadata[key]:
                metadata[key] = value
        
        return metadata
    
    def get_stats(self) -> dict:
        """Restituisce statistiche sull'estrattore"""
        return {
            'eyed3_available': self.eyed3_available,
            'mutagen_available': self.mutagen_available,
            'can_extract': self.eyed3_available or self.mutagen_available,
            'can_update': self.mutagen_available
        }