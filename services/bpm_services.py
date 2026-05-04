"""
Gestione servizi BPM: GetSong, TuneBat, SongBPM, Beatport, Librosa
Estratto da MusicCatalogerAdvanced_v0020.py
"""

import logging
import re
import time
import urllib.parse
from pathlib import Path
from typing import Optional

try:
    import requests
except ImportError:
    requests = None

try:
    import librosa
    LIBROSA_AVAILABLE = True
except ImportError:
    LIBROSA_AVAILABLE = False


class BPMServices:
    """
    Classe per gestire il recupero BPM da varie fonti
    Priorità: metadati esistenti > GetSong > TuneBat > SongBPM > Beatport > librosa
    """
    
    def __init__(self, api_keys, settings, logger=None):
        """
        Inizializza il gestore BPM
        
        Args:
            api_keys: Oggetto con le API keys (da config.secrets)
            settings: Oggetto con le configurazioni (da config.settings)
            logger: Logger per output (opzionale)
        """
        self.api_keys = api_keys
        self.settings = settings
        self.logger = logger or logging.getLogger(__name__)
        
        # Cache per BPM
        self.bpm_cache = {}
        
        # Contatori
        self.api_calls = 0
        
        # Validazione librosa
        if LIBROSA_AVAILABLE:
            self.logger.debug("Librosa disponibile per calcolo BPM")
        else:
            self.logger.debug("Librosa NON disponibile")
    
    def estimate_bpm(self, file_path: Path, metadata: dict) -> Optional[int]:
        """
        Recupera BPM con cascata di fallback
        
        Args:
            file_path: Path del file MP3
            metadata: Dict con metadati esistenti
            
        Returns:
            BPM come intero o None
        """
        artist = metadata.get('artist')
        title = metadata.get('title')
        
        # 1. Check metadati esistenti
        existing_bpm = metadata.get('bpm')
        if existing_bpm:
            try:
                bpm_val = int(float(existing_bpm))
                if self._validate_bpm(bpm_val):
                    self.logger.debug(f"BPM già presente nei metadati: {bpm_val}")
                    # Salva in cache
                    self._cache_bpm(artist, title, file_path, bpm_val)
                    return bpm_val
            except (ValueError, TypeError):
                self.logger.debug("BPM esistente non valido")
        
        # 2. Se mancano artist/title, usa solo librosa
        if not artist or not title:
            self.logger.debug("Artista o titolo mancanti, uso solo librosa")
            return self._estimate_bpm_librosa(file_path)
        
        # 3. Check cache
        cache_key = self._get_cache_key(artist, title, file_path)
        if cache_key in self.bpm_cache:
            cached_bpm = self.bpm_cache[cache_key]
            if cached_bpm:
                self.logger.debug(f"BPM trovato in cache: {cached_bpm}")
                return cached_bpm
        
        # 4. Prova servizi esterni in ordine
        bpm = None
        
        # GetSong
        if self.api_keys.GETSONG_API_KEY:
            bpm = self._get_bpm_from_getsongbpm(artist, title)
            if bpm:
                self.logger.info(f">-- BPM: {bpm} (GetSong)")
                self._cache_bpm(artist, title, file_path, bpm)
                return bpm
        
        # TuneBat
        bpm = self._get_bpm_from_tunebat(artist, title)
        if bpm:
            self.logger.info(f">-- BPM: {bpm} (TuneBat)")
            self._cache_bpm(artist, title, file_path, bpm)
            return bpm
        
        # SongBPM.com
        bpm = self._get_bpm_from_songbpm_com(artist, title)
        if bpm:
            self.logger.info(f">-- BPM: {bpm} (SongBPM)")
            self._cache_bpm(artist, title, file_path, bpm)
            return bpm
        
        # Beatport
        bpm = self._get_bpm_from_beatport(artist, title)
        if bpm:
            self.logger.info(f">-- BPM: {bpm} (Beatport)")
            self._cache_bpm(artist, title, file_path, bpm)
            return bpm
        
        # 5. Fallback librosa
        bpm = self._estimate_bpm_librosa(file_path)
        if bpm:
            self.logger.info(f">-- BPM: {bpm} (analisi audio)")
            self._cache_bpm(artist, title, file_path, bpm)
            return bpm
        
        # Nessun BPM trovato
        self.logger.debug("Nessun BPM trovato con tutti i metodi")
        self.bpm_cache[cache_key] = None
        return None
    
    def _get_cache_key(self, artist: str, title: str, file_path: Path) -> str:
        """Genera chiave cache per BPM"""
        if artist and title:
            return f"bpm_{artist}_{title}".lower().replace(' ', '_')
        return f"bpm_{file_path.stem}".lower().replace(' ', '_')
    
    def _cache_bpm(self, artist: str, title: str, file_path: Path, bpm: int):
        """Salva BPM in cache"""
        cache_key = self._get_cache_key(artist, title, file_path)
        self.bpm_cache[cache_key] = bpm
    
    def _validate_bpm(self, bpm: int) -> bool:
        """Valida che il BPM sia in un range ragionevole"""
        min_bpm = self.settings.bpm.valid_range_min
        max_bpm = self.settings.bpm.valid_range_max
        return min_bpm <= bpm <= max_bpm
    
    def _get_bpm_from_getsongbpm(self, artist: str, title: str) -> Optional[int]:
        """Recupera BPM da getsong.co API"""
        if not requests:
            return None
        
        try:
            # Rate limiting
            time.sleep(self.settings.api.bpm_services_rate_limit)
            self.api_calls += 1
            
            base_url = "https://api.getsong.co/search/"
            params = {
                "api_key": self.api_keys.GETSONG_API_KEY,
                "type": "both",
                "lookup": f"artist:{artist} song:{title}"
            }
            
            headers = {
                'User-Agent': 'MusicCatalogerAdvanced/0.0.1.9'
            }
            
            self.logger.debug(f"GetSong query: {params['lookup']}")
            
            response = requests.get(
                base_url,
                params=params,
                headers=headers,
                timeout=self.settings.api.timeout
            )
            
            if response.status_code == 403:
                self.logger.warning("GetSong API: Accesso negato (403)")
                return None
            
            if response.status_code != 200:
                self.logger.debug(f"GetSong HTTP error: {response.status_code}")
                return None
            
            data = response.json()
            
            if isinstance(data, dict):
                if data.get("search") and len(data["search"]) > 0:
                    result = data["search"][0]
                    if result.get("tempo"):
                        bpm = int(float(result["tempo"]))
                        if self._validate_bpm(bpm):
                            return bpm
                elif data.get("tempo"):
                    bpm = int(float(data["tempo"]))
                    if self._validate_bpm(bpm):
                        return bpm
            
            self.logger.debug("GetSong: Nessun risultato BPM")
            return None
            
        except Exception as e:
            self.logger.debug(f"GetSong error: {e}")
            return None
    
    def _get_bpm_from_tunebat(self, artist: str, title: str) -> Optional[int]:
        """Scraping BPM da TuneBat.com"""
        if not requests:
            return None
        
        try:
            time.sleep(self.settings.api.bpm_services_rate_limit)
            
            query = f"{artist} {title}".strip()
            encoded_query = urllib.parse.quote(query)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            search_url = f"https://tunebat.com/Search?q={encoded_query}"
            
            response = requests.get(
                search_url,
                headers=headers,
                timeout=self.settings.api.timeout
            )
            
            if response.status_code != 200:
                return None
            
            html = response.text
            
            # Pattern BPM
            bpm_patterns = [
                r'<span[^>]*class="[^"]*bpm[^"]*"[^>]*>(\d+)</span>',
                r'"bpm":\s*(\d+)',
                r'BPM:\s*(\d+)',
                r'(\d+)\s*BPM'
            ]
            
            for pattern in bpm_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    bpm = int(match.group(1))
                    if self._validate_bpm(bpm):
                        self.logger.debug(f"TuneBat BPM trovato: {bpm}")
                        return bpm
            
            return None
            
        except Exception as e:
            self.logger.debug(f"TuneBat error: {e}")
            return None
    
    def _get_bpm_from_songbpm_com(self, artist: str, title: str) -> Optional[int]:
        """Scraping BPM da SongBPM.com"""
        if not requests:
            return None
        
        try:
            time.sleep(self.settings.api.bpm_services_rate_limit)
            
            query = f"{artist} {title}".strip()
            encoded_query = urllib.parse.quote(query)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            search_url = f"https://songbpm.com/search?q={encoded_query}"
            
            response = requests.get(
                search_url,
                headers=headers,
                timeout=self.settings.api.timeout
            )
            
            if response.status_code != 200:
                return None
            
            html = response.text
            
            bpm_match = re.search(r'(\d+)\s*BPM', html, re.IGNORECASE)
            if bpm_match:
                bpm = int(bpm_match.group(1))
                if self._validate_bpm(bpm):
                    self.logger.debug(f"SongBPM.com BPM trovato: {bpm}")
                    return bpm
            
            return None
            
        except Exception as e:
            self.logger.debug(f"SongBPM.com error: {e}")
            return None
    
    def _get_bpm_from_beatport(self, artist: str, title: str) -> Optional[int]:
        """Scraping BPM da Beatport (per musica elettronica/dance)"""
        if not requests:
            return None
        
        try:
            time.sleep(self.settings.api.bpm_services_rate_limit)
            
            query = f"{artist} {title}".strip()
            encoded_query = urllib.parse.quote(query)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            search_url = f"https://www.beatport.com/search?q={encoded_query}"
            
            response = requests.get(
                search_url,
                headers=headers,
                timeout=self.settings.api.timeout
            )
            
            if response.status_code != 200:
                return None
            
            html = response.text
            
            bpm_match = re.search(r'"bpm":\s*(\d+)', html)
            if bpm_match:
                bpm = int(bpm_match.group(1))
                if self._validate_bpm(bpm):
                    self.logger.debug(f"Beatport BPM trovato: {bpm}")
                    return bpm
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Beatport error: {e}")
            return None
    
    def _estimate_bpm_librosa(self, file_path: Path) -> Optional[int]:
        """Stima BPM usando librosa"""
        if not LIBROSA_AVAILABLE:
            self.logger.debug("Librosa non disponibile")
            return None
        
        try:
            self.logger.debug("Calcolo BPM con librosa...")
            y, sr = librosa.load(str(file_path), sr=None)
            tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
            
            # Fix numpy deprecation
            if hasattr(tempo, 'item'):
                bpm = int(round(tempo.item()))
            else:
                bpm = int(round(float(tempo)))
            
            if self._validate_bpm(bpm):
                return bpm
            else:
                self.logger.debug(f"BPM librosa fuori range: {bpm}")
                return None
                
        except Exception as e:
            self.logger.debug(f"Errore calcolo BPM librosa: {e}")
            return None
    
    def get_cache_stats(self) -> dict:
        """Restituisce statistiche sulla cache BPM"""
        return {
            'cache_size': len(self.bpm_cache),
            'api_calls': self.api_calls,
            'librosa_available': LIBROSA_AVAILABLE
        }