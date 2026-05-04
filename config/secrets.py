"""
Gestione centralizzata delle API Keys e credenziali
IMPORTANTE: Non committare questo file in repository pubblici!
"""

import os
from typing import Optional

class APIKeys:
    """Gestione centralizzata delle API keys con fallback da variabili d'ambiente"""
    
    def __init__(self):
        # Spotify API - Presi dal tuo file esistente linea ~710
        self.SPOTIFY_CLIENT_ID = self._get_key(
            'SPOTIFY_CLIENT_ID', 
            '682cb59a3f5743cbad34c1ac22b4229d'
        )
        self.SPOTIFY_CLIENT_SECRET = self._get_key(
            'SPOTIFY_CLIENT_SECRET', 
            'b9f26e830df94d138ad0f382158a6c91'
        )
        
        # GetSong/BPM API - Presa dal tuo file linea ~70
        self.GETSONG_API_KEY = self._get_key(
            'GETSONG_API_KEY', 
            'c1d3052529a15b51c20932a4283db3f1'
        )
        
        # Last.fm API - Presa dal tuo file linea ~420
        self.LASTFM_API_KEY = self._get_key(
            'LASTFM_API_KEY', 
            '8b79bf6197a85dc2ff9e076da46792c5'
        )
        
        # AcoustID (fingerprinting audio — opzionale, richiede fpcalc.exe)
        # Ottieni su: https://acoustid.org/api-key  (login con account MusicBrainz)
        self.ACOUSTID_API_KEY = self._get_key('4c23noZ1UA', None)

        # Discogs (gratuito con account — ottimo per jazz/classica/vinili)
        # Ottieni su: https://www.discogs.com/settings/developers → "Generate new token"
        self.DISCOGS_TOKEN = self._get_key('uDnXzYJqaiNqwclprniLgPlCsEqfoEzBaTyDPAiF', None)

        # AudD (fingerprinting Shazam-like — 100 req/giorno gratis)
        # Ottieni su: https://audd.io → Sign Up → Dashboard
        self.AUDD_API_KEY = self._get_key('ebfab499d0b0fd6a7add88d50352f0d9', None)

        # Deezer e iTunes non richiedono chiave — API pubbliche gratuite
        
        # MusicBrainz (configurazione) - Presa dal tuo file linea ~95
        self.MUSICBRAINZ_USER_AGENT = "MusicCatalogerAdvanced"
        self.MUSICBRAINZ_VERSION = "v0019"
        self.MUSICBRAINZ_CONTACT = "captainjoker27@gmail.com"
    
    def _get_key(self, env_var: str, default: Optional[str] = None) -> Optional[str]:
        """
        Ottieni API key da variabile d'ambiente o usa default
        
        Per usare variabili d'ambiente in Windows:
        set SPOTIFY_CLIENT_ID=your_key_here
        
        In Linux/Mac:
        export SPOTIFY_CLIENT_ID=your_key_here
        """
        return os.getenv(env_var, default)
    
    def validate_keys(self) -> dict:
        """Valida la disponibilità delle API keys"""
        return {
            'lastfm':    bool(self.LASTFM_API_KEY),
            'acoustid':  bool(self.ACOUSTID_API_KEY),
            'discogs':   bool(self.DISCOGS_TOKEN),
            'audd':      bool(self.AUDD_API_KEY),
            'deezer':    True,   # API pubblica, nessuna chiave
            'itunes':    True,   # API pubblica, nessuna chiave
            'musicbrainz': True, # API pubblica, nessuna chiave
        }
    
    def get_missing_keys(self) -> list:
        """Restituisce lista delle API keys mancanti o non valide"""
        validation = self.validate_keys()
        return [service for service, valid in validation.items() if not valid]
    
    def print_status(self):
        """Stampa lo stato delle API keys (per debug)"""
        validation = self.validate_keys()
        print("=== Stato API Keys ===")
        for service, valid in validation.items():
            status = "✓ OK" if valid else "✗ Mancante"
            print(f"{service.capitalize()}: {status}")
        print("=" * 30)

# Istanza globale - importabile con: from config.secrets import api_keys
api_keys = APIKeys()

# Per debugging - decommentare per testare
# if __name__ == "__main__":
#     api_keys.print_status()