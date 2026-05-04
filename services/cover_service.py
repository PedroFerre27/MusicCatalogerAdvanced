"""
Servizio per il recupero e l'incorporazione delle cover album nei tag ID3.
Supporta: Spotify, MusicBrainz, Last.fm
"""

import io
import logging
import time
from pathlib import Path
from typing import Optional, Tuple

try:
    import requests
except ImportError:
    requests = None

try:
    from mutagen.mp3 import MP3
    from mutagen.id3 import ID3, APIC
    MUTAGEN_AVAILABLE = True
except ImportError:
    MUTAGEN_AVAILABLE = False

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False


class CoverService:
    """
    Recupera e incorpora cover album.
    Strategia configurabile: 'first_available', 'largest', 'manual'
    """

    def __init__(self, api_keys, settings, logger=None):
        self.api_keys = api_keys
        self.settings = settings
        self.cfg = settings.cover
        self.logger = logger or logging.getLogger(__name__)

        # Cache: (artist, album) -> url
        self.cover_cache: dict = {}

    def process_file(self, file_path: Path, metadata: dict, dry_run: bool = False) -> str:
        """
        Verifica se la cover e' presente; se manca, la recupera e la incorpora.
        Returns:
            'existing'    — cover gia' presente, nessuna azione
            'downloaded'  — cover scaricata/incorporata (o simulata in dry_run)
            'not_found'   — nessuna cover trovata online
            'error'       — errore durante l'incorporazione
        """
        self._last_source = ''
        if not MUTAGEN_AVAILABLE:
            self.logger.debug("mutagen non disponibile, skip cover")
            return 'error'

        # Controlla se la cover esiste gia'
        if not self.cfg.overwrite_existing and self._has_cover(file_path):
            self.logger.debug(f"Cover gia' presente: {file_path.name}")
            return 'existing'

        artist    = metadata.get('artist', '')
        album     = metadata.get('album', '')
        title     = metadata.get('title', '')
        cover_url = metadata.get('cover_url', '')

        # v1069c: priorità 0 — usa cover_url già presente nei metadati
        # (da Deezer/iTunes nella cache) — evita chiamata API aggiuntiva
        if cover_url:
            img_data = self._download_image(cover_url)
            if img_data:
                if dry_run:
                    return 'downloaded'
                success = self._embed_cover(file_path, img_data, 'image/jpeg')
                if success:
                    self._last_source = metadata.get('source', 'cache')
                    return 'downloaded'

        if not artist:
            self.logger.debug(f"Artista mancante per cover: {file_path.name}")
            return 'not_found'

        # Cerca la cover tramite API
        cover_data, mime_type, source = self._fetch_cover(artist, album, title)
        self._last_source = source

        if not cover_data:
            self.logger.debug(f"Nessuna cover trovata per: {artist} - {album or title}")
            return 'not_found'

        if dry_run:
            return 'downloaded'

        # Incorpora nel tag ID3
        success = self._embed_cover(file_path, cover_data, mime_type)
        return 'downloaded' if success else 'error'

    def _has_cover(self, file_path: Path) -> bool:
        """Controlla se il file ha gia' una cover APIC"""
        try:
            tags = ID3(str(file_path))
            return bool(tags.getall('APIC'))
        except Exception:
            return False

    def _fetch_cover(self, artist: str, album: str, title: str) -> Tuple[Optional[bytes], str, str]:
        """
        Cerca la cover nelle sorgenti configurate.
        Returns: (image_bytes, mime_type, source_name)
        """
        if self.cfg.strategy == 'largest':
            return self._fetch_largest(artist, album, title)
        else:  # 'first_available'
            return self._fetch_first(artist, album, title)

    def _fetch_first(self, artist: str, album: str, title: str) -> Tuple[Optional[bytes], str, str]:
        """Prende la prima cover disponibile nell'ordine di priorita'"""
        for source in self.cfg.source_priority:
            result = self._fetch_from_source(source, artist, album, title)
            if result[0]:
                return result
        return None, '', ''

    def _fetch_largest(self, artist: str, album: str, title: str) -> Tuple[Optional[bytes], str, str]:
        """Prende la cover piu' grande tra tutte le sorgenti"""
        if not PIL_AVAILABLE:
            # Senza PIL non possiamo confrontare dimensioni, usa first_available
            return self._fetch_first(artist, album, title)

        best_data = None
        best_size = 0
        best_mime = 'image/jpeg'
        best_source = ''

        for source in self.cfg.source_priority:
            data, mime, src = self._fetch_from_source(source, artist, album, title)
            if data:
                try:
                    img = Image.open(io.BytesIO(data))
                    size = img.width * img.height
                    if size > best_size and min(img.width, img.height) >= self.cfg.min_size_px:
                        best_data = data
                        best_size = size
                        best_mime = mime
                        best_source = src
                except Exception:
                    pass

        return best_data, best_mime, best_source

    def _fetch_from_source(self, source: str, artist: str, album: str, title: str) -> Tuple[Optional[bytes], str, str]:
        """Delega al metodo specifico per sorgente"""
        try:
            if source == 'spotify':
                return self._from_spotify(artist, album or title)
            elif source == 'musicbrainz':
                return self._from_musicbrainz(artist, album or title)
            elif source == 'lastfm':
                return self._from_lastfm(artist, album, title)
            elif source == 'deezer':          # v1049
                return self._from_deezer(artist, album or title)
            elif source == 'itunes':           # v1049
                return self._from_itunes(artist, album or title)
        except Exception as e:
            self.logger.debug(f"Cover {source} error per '{artist}': {e}")
        return None, '', ''

    def _download_image(self, url: str) -> Optional[bytes]:
        """Scarica l'immagine dall'URL"""
        if not requests or not url:
            return None
        try:
            r = requests.get(url, timeout=self.settings.api.timeout)
            if r.status_code == 200 and r.content:
                return r.content
        except Exception as e:
            self.logger.debug(f"Download image error: {e}")
        return None

    def _from_spotify(self, artist: str, album: str) -> Tuple[Optional[bytes], str, str]:
        if not requests:
            return None, '', ''
        api_key = getattr(self.api_keys, 'SPOTIFY_CLIENT_ID', '')
        secret = getattr(self.api_keys, 'SPOTIFY_CLIENT_SECRET', '')
        if not api_key or not secret:
            return None, '', ''
        try:
            # Token
            token_r = requests.post(
                'https://accounts.spotify.com/api/token',
                data={'grant_type': 'client_credentials'},
                auth=(api_key, secret),
                timeout=10
            )
            if token_r.status_code != 200:
                return None, '', ''
            token = token_r.json().get('access_token', '')
            if not token:
                return None, '', ''

            # Search
            q = f"album:{album} artist:{artist}" if album else f"artist:{artist}"
            search_r = requests.get(
                'https://api.spotify.com/v1/search',
                params={'q': q, 'type': 'album', 'limit': 1},
                headers={'Authorization': f'Bearer {token}'},
                timeout=10
            )
            if search_r.status_code != 200:
                return None, '', ''

            items = search_r.json().get('albums', {}).get('items', [])
            if not items:
                return None, '', ''

            images = items[0].get('images', [])
            if not images:
                return None, '', ''

            # Prendi immagine piu' grande
            best = max(images, key=lambda x: x.get('width', 0))
            url = best.get('url', '')
            data = self._download_image(url)
            return (data, 'image/jpeg', 'Spotify') if data else (None, '', '')
        except Exception as e:
            self.logger.debug(f"Spotify cover error: {e}")
            return None, '', ''

    def _from_musicbrainz(self, artist: str, album: str) -> Tuple[Optional[bytes], str, str]:
        if not requests:
            return None, '', ''
        try:
            time.sleep(1.2)
            # Search release
            q = f'artist:"{artist}" AND release:"{album}"' if album else f'artist:"{artist}"'
            r = requests.get(
                'https://musicbrainz.org/ws/2/release/',
                params={'query': q, 'limit': 1, 'fmt': 'json'},
                headers={'User-Agent': 'MusicCatalogerAdvanced/0.20 (music@cataloger.local)'},
                timeout=10
            )
            if r.status_code != 200:
                return None, '', ''
            releases = r.json().get('releases', [])
            if not releases:
                return None, '', ''

            mbid = releases[0].get('id', '')
            if not mbid:
                return None, '', ''

            # Cover Art Archive
            time.sleep(0.5)
            caa_r = requests.get(
                f'https://coverartarchive.org/release/{mbid}',
                timeout=10,
                allow_redirects=True
            )
            if caa_r.status_code != 200:
                return None, '', ''

            images = caa_r.json().get('images', [])
            front = next((i for i in images if i.get('front')), None)
            if not front:
                front = images[0] if images else None
            if not front:
                return None, '', ''

            url = front.get('image', '')
            data = self._download_image(url)
            return (data, 'image/jpeg', 'MusicBrainz') if data else (None, '', '')
        except Exception as e:
            self.logger.debug(f"MusicBrainz cover error: {e}")
            return None, '', ''

    def _from_lastfm(self, artist: str, album: str, title: str) -> Tuple[Optional[bytes], str, str]:
        if not requests:
            return None, '', ''
        api_key = getattr(self.api_keys, 'LASTFM_API_KEY', '')
        if not api_key:
            return None, '', ''
        try:
            if album:
                params = {
                    'method': 'album.getInfo',
                    'artist': artist, 'album': album,
                    'api_key': api_key, 'format': 'json'
                }
            else:
                params = {
                    'method': 'track.getInfo',
                    'artist': artist, 'track': title,
                    'api_key': api_key, 'format': 'json'
                }
            r = requests.get('https://ws.audioscrobbler.com/2.0/', params=params, timeout=10)
            if r.status_code != 200:
                return None, '', ''
            data = r.json()

            images = (
                data.get('album', {}).get('image', []) or
                data.get('track', {}).get('album', {}).get('image', [])
            )
            if not images:
                return None, '', ''

            # Prendi extralarge o mega
            size_order = ['mega', 'extralarge', 'large', 'medium', 'small']
            url = ''
            for size in size_order:
                for img in images:
                    if img.get('size') == size and img.get('#text'):
                        url = img['#text']
                        break
                if url:
                    break

            if not url:
                return None, '', ''

            img_data = self._download_image(url)
            return (img_data, 'image/jpeg', 'Last.fm') if img_data else (None, '', '')
        except Exception as e:
            self.logger.debug(f"Last.fm cover error: {e}")
            return None, '', ''

    def _embed_cover(self, file_path: Path, image_data: bytes, mime_type: str) -> bool:
        """Incorpora la cover nel tag ID3 APIC"""
        try:
            try:
                tags = ID3(str(file_path))
            except Exception:
                tags = ID3()

            # Rimuovi cover esistenti
            tags.delall('APIC')

            tags.add(APIC(
                encoding=3,
                mime=mime_type,
                type=3,  # Cover (front)
                desc='Cover',
                data=image_data
            ))
            tags.save(str(file_path))
            return True
        except Exception as e:
            self.logger.warning(f"Errore embed cover {file_path.name}: {e}")
            return False

    def _from_deezer(self, artist: str, album: str) -> Tuple[Optional[bytes], str, str]:
        """v1049: cover da Deezer (API pubblica, nessuna chiave)."""
        if not requests:
            return None, '', ''
        try:
            r = requests.get(
                "https://api.deezer.com/search",
                params={"q": f'artist:"{artist}" album:"{album}"', "limit": 1},
                timeout=self.settings.api.timeout,
            )
            if r.status_code != 200:
                return None, '', ''
            data = r.json().get("data", [])
            if not data:
                return None, '', ''
            cover_url = (data[0].get("album") or {}).get("cover_xl") \
                     or (data[0].get("album") or {}).get("cover_big")
            if not cover_url:
                return None, '', ''
            img = self._download_image(cover_url)
            return (img, 'image/jpeg', 'Deezer') if img else (None, '', '')
        except Exception as e:
            self.logger.debug(f"Deezer cover error: {e}")
            return None, '', ''

    def _from_itunes(self, artist: str, album: str) -> Tuple[Optional[bytes], str, str]:
        """v1049: cover dall'iTunes Search API (pubblica, nessuna chiave)."""
        if not requests:
            return None, '', ''
        try:
            r = requests.get(
                "https://itunes.apple.com/search",
                params={
                    "term": f"{artist} {album}",
                    "media": "music",
                    "entity": "album",
                    "limit": 3,
                    "country": "US",
                },
                timeout=self.settings.api.timeout,
            )
            if r.status_code != 200:
                return None, '', ''
            results = r.json().get("results", [])
            if not results:
                return None, '', ''
            # Prendi il risultato con artista più simile
            al = artist.lower()
            best = next((x for x in results if al in (x.get("artistName") or "").lower()), results[0])
            url = (best.get("artworkUrl100") or "").replace("100x100", "600x600")
            if not url:
                return None, '', ''
            img = self._download_image(url)
            return (img, 'image/jpeg', 'iTunes') if img else (None, '', '')
        except Exception as e:
            self.logger.debug(f"iTunes cover error: {e}")
            return None, '', ''
