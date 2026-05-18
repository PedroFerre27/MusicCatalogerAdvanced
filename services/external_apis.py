"""
Gestione API esterne: MusicBrainz, Last.fm, Spotify
Estratto da MusicCatalogerAdvanced_v0020.py
"""

import logging
import re
import time
import warnings
from typing import Dict, List, Optional

try:
    import musicbrainzngs
except ImportError:
    musicbrainzngs = None

try:
    import requests
except ImportError:
    requests = None


class ExternalAPIs:
    """
    Classe per gestire tutte le chiamate alle API esterne
    """
    
    # v1086.1: lista canonical delle sorgenti METADATA (no BPM-only).
    # Ordine = priorita' di default cascata. La GUI puo' override
    # passando una lista propria.
    DEFAULT_METADATA_SOURCES = ['musicbrainz', 'deezer', 'itunes', 'lastfm']
    # Sorgenti che richiedono token (devono essere esplicitamente abilitate)
    TOKEN_METADATA_SOURCES = ['discogs', 'acoustid']

    def __init__(self, api_keys, settings, logger=None,
                  enabled_sources=None, api_client=None):
        """
        Inizializza il gestore API

        Args:
            api_keys: Oggetto con le API keys (da config.secrets)
            settings: Oggetto con le configurazioni (da config.settings)
            logger: Logger per output (opzionale)
            enabled_sources: Lista ordinata delle sorgenti metadata abilitate
                (es. ['musicbrainz', 'deezer', 'discogs']). La cascata
                rispetta questo ordine. Se None → DEFAULT_METADATA_SOURCES.
                Sorgenti non in questa lista sono SKIPPATE.
                v1086.1: aggiunto per fix priorita' sorgenti UI.
            api_client: v1087.3 (security Fase 2) — istanza ApiClient per
                il proxy lookup server-side di Discogs/Last.fm/Spotify.
                Se None, i provider che richiedono token (non piu'
                disponibili client-side) vengono skippati gracefully.
        """
        self.api_keys = api_keys
        self.settings = settings
        self.logger = logger or logging.getLogger(__name__)
        # v1087.3: client per proxy lookup (token server-side)
        self.api_client = api_client

        # v1086.1 (revisione 3): distinguere None (non passato → default)
        # da [] (esplicitamente vuoto → cascata disattivata).
        if enabled_sources is None:
            self.enabled_sources = list(self.DEFAULT_METADATA_SOURCES)
        else:
            # Filtra solo sorgenti note per evitare typo/casing issues
            valid = set(self.DEFAULT_METADATA_SOURCES) | set(self.TOKEN_METADATA_SOURCES)
            self.enabled_sources = [s.lower() for s in enabled_sources
                                     if s.lower() in valid]
            # NON fare fallback a default se la lista era esplicitamente
            # vuota o conteneva solo typo: l'utente ha disabilitato tutto.
        self.logger.debug(f"ExternalAPIs: cascata abilitata = {self.enabled_sources}")

        # v1086.1 (revisione 3): warning se sorgenti token-based sono
        # abilitate ma il token manca in secrets.py. Senza questo l'utente
        # vede la sorgente "abilitata" nei log ma in realta' search_*
        # ritorna sempre None silenziosamente.
        if 'discogs' in self.enabled_sources:
            if not getattr(self.api_keys, 'DISCOGS_TOKEN', None):
                self.logger.warning(
                    "Discogs abilitato in UI ma DISCOGS_TOKEN mancante in "
                    "secrets.py — la sorgente verra' saltata. Genera token su "
                    "https://www.discogs.com/settings/developers")
        if 'acoustid' in self.enabled_sources:
            if not getattr(self.api_keys, 'ACOUSTID_API_KEY', None):
                self.logger.warning(
                    "AcoustID abilitato in UI ma ACOUSTID_API_KEY mancante "
                    "in secrets.py. Inoltre AcoustID e' fingerprint-only "
                    "(richiede fpcalc.exe) e attualmente NON e' integrato "
                    "nella cascata search_all — sara' aggiunto in pilot 2.")
        
        # Cache per ridurre chiamate API
        self.metadata_cache = {}
        
        # Rate limiting
        self.last_musicbrainz_call = 0
        self.last_lastfm_call = 0
        self.last_spotify_call = 0
        
        # Contatori
        self.api_calls = 0
        
        # Setup MusicBrainz
        if musicbrainzngs:
            musicbrainzngs.set_useragent(
                api_keys.MUSICBRAINZ_USER_AGENT,
                api_keys.MUSICBRAINZ_VERSION,
                api_keys.MUSICBRAINZ_CONTACT
            )
            musicbrainzngs.set_rate_limit(
                limit_or_interval=settings.api.musicbrainz_rate_limit,
                new_requests=1
            )
            self._suppress_musicbrainz_warnings()
    
    def _suppress_musicbrainz_warnings(self):
        """Sopprime warning di MusicBrainz"""
        musicbrainz_logger = logging.getLogger('musicbrainzngs')
        musicbrainz_logger.setLevel(logging.ERROR)
        
        xml_logger = logging.getLogger('xml')
        xml_logger.setLevel(logging.ERROR)
        
        class MusicBrainzWarningFilter(logging.Filter):
            def filter(self, record):
                unwanted_messages = [
                    'uncaught attribute',
                    'uncaught <first-release-date>',
                    'in <ws2:',
                ]
                return not any(msg in record.getMessage() for msg in unwanted_messages)
        
        for logger_name in ['musicbrainzngs', 'xml']:
            logger = logging.getLogger(logger_name)
            logger.addFilter(MusicBrainzWarningFilter())

    def _proxy_lookup(self, provider: str, artist: str, title: str):
        """v1087.3 (security Fase 2): inoltra la ricerca al server proxy
        (/api/v1/lookup) che possiede i token. Ritorna il dict metadati
        normalizzato o None.

        None significa "proxy non disponibile / nessun risultato" → il
        chiamante fa fallback alla logica diretta (che con token client
        rimossi ritornera' comunque None, quindi si passa al provider
        pubblico successivo nella cascata). Nessuna eccezione propagata:
        la catalogazione non deve mai fermarsi per il proxy.
        """
        if self.api_client is None:
            return None
        try:
            result = self.api_client.lookup(provider, artist, title)
            if result:
                self.logger.debug(
                    f">-- {provider} (proxy server): metadati trovati")
            return result
        except Exception as e:
            self.logger.debug(f"_proxy_lookup {provider} exc: {e}")
            return None

    def search_all(self, artist: str, title: str, album: str = None) -> Optional[Dict]:
        """
        Cascata metadati — v1086.1 (rispetta self.enabled_sources):

        L'ordine della cascata e' determinato da self.enabled_sources, che
        viene popolato dall'argomento `--metadata-sources` della CLI o dal
        default del costruttore. La cascata SKIPPA le sorgenti non
        presenti in enabled_sources.

        Sorgenti supportate (riconosciute):
          - musicbrainz   - massima precisione, jazz, classica, soundtrack
          - deezer        - pop/latin, film, generi italiani (free)
          - itunes        - Anime, TV, Classical (free)
          - lastfm        - electronic, alternative, indie (free)
          - discogs       - jazz, vinili, world (richiede token)
          - acoustid      - fingerprinting (richiede fpcalc + token)

        La cascata si ferma alla prima sorgente che restituisce un genere
        utile (non vuoto / non in {'other','unknown','musique du monde'}).
        Se nessuna sorgente trova un genere valido, ritorna il primo
        candidato non-vuoto (che almeno ha BPM/album).
        """
        if not artist or not title:
            return None

        # Mappa sorgente → metodo (chiamato lazy)
        source_methods = {
            'musicbrainz': lambda: self.search_musicbrainz(artist, title, album),
            'deezer':      lambda: self.search_deezer(artist, title),
            'itunes':      lambda: self.search_itunes(artist, title),
            'lastfm':      lambda: self.search_lastfm(artist, title),
            'discogs':     lambda: self.search_discogs(artist, title),
            # acoustid e' file-based, non query-based — gestito separatamente
        }
        source_display_names = {
            'musicbrainz': 'MusicBrainz', 'deezer': 'Deezer',
            'itunes': 'iTunes', 'lastfm': 'Last.fm', 'discogs': 'Discogs',
        }

        candidates = []
        for source_id in self.enabled_sources:
            method = source_methods.get(source_id)
            if method is None:
                # AcoustID e altre sorgenti non query-based — skippa qui,
                # vengono provate altrove (search_acoustid via file)
                continue
            display_name = source_display_names.get(source_id, source_id)
            try:
                result = method()
                if result:
                    result['source'] = display_name
                    # v1087.1: salva i parametri ORIGINALI della query come
                    # hint per save_cache, cosi' puo' collegare l'esito al
                    # record file giusto. Senza questi, save_cache usa
                    # `result['artist']` (versione canonica dal provider,
                    # es. "Big Rob Savage" invece del "Audiomachine" del
                    # filename) e crea orfani.
                    result.setdefault('_query_artist', artist)
                    result.setdefault('_query_title', title)
                    genre = (result.get('genre') or '').lower()
                    if genre and genre not in ('', 'other', 'unknown',
                                                'musique du monde'):
                        self.logger.debug(
                            f"search_all: genere trovato da {display_name} → '{genre}'")
                        return result
                    candidates.append(result)
            except Exception as e:
                self.logger.debug(f"search_all {display_name} exc: {e}")

        return candidates[0] if candidates else None

    def search_musicbrainz(self, artist: str, title: str, album: str = None) -> Optional[Dict]:
        """
        Cerca metadati su MusicBrainz
        
        Args:
            artist: Nome artista
            title: Titolo traccia
            album: Nome album (opzionale)
            
        Returns:
            Dict con metadati o None se non trovato
        """
        if not musicbrainzngs:
            self.logger.debug("MusicBrainz non disponibile")
            return None
        
        # Check cache
        cache_key = f"mb_{artist}_{title}_{album or ''}"
        if cache_key in self.metadata_cache:
            self.logger.debug("MusicBrainz: Cache trovate")
            return self.metadata_cache[cache_key]
        
        try:
            # Rate limiting
            elapsed = time.time() - self.last_musicbrainz_call
            if elapsed < self.settings.api.musicbrainz_rate_limit:
                wait_time = self.settings.api.musicbrainz_rate_limit - elapsed
                self.logger.debug(f"MusicBrainz: rate limiting: aspetto {wait_time:.2f}s")
                time.sleep(wait_time)
            
            self.last_musicbrainz_call = time.time()
            self.api_calls += 1
            
            # Build query
            query_parts = [f'artist:"{artist}"', f'recording:"{title}"']
            if album:
                query_parts.append(f'release:"{album}"')
            
            query = ' AND '.join(query_parts)
            self.logger.debug(f"MusicBrainz: query: {query}")
            
            # Execute search with SSL fix
            self._setup_ssl_context()
            
            # Suppress warnings
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore")
                
                mb_logger = logging.getLogger('musicbrainzngs')
                original_level = mb_logger.level
                mb_logger.setLevel(logging.ERROR)
                
                try:
                    result = musicbrainzngs.search_recordings(query=query, limit=3)
                finally:
                    mb_logger.setLevel(original_level)
            
            if not result.get('recording-list'):
                self.logger.debug(f"MusicBrainz: nessun risultato")
                self.metadata_cache[cache_key] = None
                return None
            
            self.logger.debug(f"MusicBrainz: trovati {len(result['recording-list'])} risultati")
            recording = result['recording-list'][0]
            
            # Get detailed info
            metadata = self._get_musicbrainz_details(recording['id'], artist, title)
            
            self.metadata_cache[cache_key] = metadata
            return metadata
            
        except Exception as e:
            self.logger.warning(f"MusicBrainz: errore per {artist} - {title}: {e}")
            self.metadata_cache[cache_key] = None
            return None
    
    def _setup_ssl_context(self):
        """Setup SSL context per MusicBrainz"""
        import ssl
        import urllib.request
        
        try:
            ssl_context = ssl.create_default_context()
            https_handler = urllib.request.HTTPSHandler(context=ssl_context)
            opener = urllib.request.build_opener(https_handler)
            urllib.request.install_opener(opener)
        except Exception:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            https_handler = urllib.request.HTTPSHandler(context=ssl_context)
            opener = urllib.request.build_opener(https_handler)
            urllib.request.install_opener(opener)
    
    def _get_musicbrainz_details(self, recording_id: str, artist: str, title: str) -> Dict:
        """Ottiene dettagli completi da MusicBrainz"""
        try:
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
            
            # Extract genres
            genres = []
            if rec_data.get('tag-list'):
                for tag in rec_data['tag-list']:
                    tag_name = tag.get('name', '').lower()
                    if self._is_music_genre_tag(tag_name):
                        genres.append(tag_name)
            
            # Release info
            if rec_data.get('release-list'):
                release = rec_data['release-list'][0]
                metadata['album'] = release.get('title')
                
                if release.get('date'):
                    metadata['year'] = release['date'][:4]
                
                if release.get('tag-list'):
                    for tag in release['tag-list']:
                        tag_name = tag.get('name', '').lower()
                        if self._is_music_genre_tag(tag_name):
                            genres.append(tag_name)
            
            # Artist genres (with extra rate limiting)
            if not genres and rec_data.get('artist-credit'):
                try:
                    artist_mbid = rec_data['artist-credit'][0]['artist']['id']
                    time.sleep(0.8)
                    
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
                        for tag in artist_detailed['artist']['tag-list']:
                            tag_name = tag.get('name', '').lower()
                            if self._is_music_genre_tag(tag_name):
                                genres.append(tag_name)
                
                except Exception as e:
                    self.logger.debug(f"MusicBrainz: errore tag artista: {e}")
            
            # Process genres
            if genres:
                unique_genres = list(dict.fromkeys(genres))
                primary_genre = self._select_primary_genre(unique_genres)
                if primary_genre:
                    metadata['genre'] = primary_genre
                metadata['all_genres'] = unique_genres[:5]
            
            self.logger.debug(f"MusicBrainz: metadati completi: {list(metadata.values())}")
            self.logger.info(f">-- MusicBrainz: trovati metadati ({metadata.get('genre', 'genere sconosciuto')})")
            return metadata
            
        except Exception as e:
            self.logger.warning(f"MusicBrainz: errore dettagli: {e}")
            return None
    
    def _is_music_genre_tag(self, tag_name: str) -> bool:
        """Determina se un tag è un genere musicale"""
        known_genres = {
            'rock', 'pop', 'jazz', 'blues', 'classical', 'electronic', 'hip hop',
            'country', 'folk', 'reggae', 'metal', 'punk', 'alternative', 'indie',
            'soul', 'funk', 'disco', 'house', 'techno', 'trance', 'ambient',
            'salsa', 'bachata', 'merengue', 'reggaeton', 'latin', 'tropical',
            'cumbia', 'tango', 'bossa nova', 'samba', 'mambo', 'cha cha',
            'world', 'experimental', 'soundtrack', 'vocal', 'instrumental'
        }
        
        # Usa exclude_tags da settings
        exclude_tags = set(self.settings.genre.exclude_genre_tags)
        
        if tag_name in exclude_tags:
            return False
        
        if tag_name in known_genres:
            return True
        
        for genre in known_genres:
            if genre in tag_name or tag_name in genre:
                return True
        
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
        """Seleziona il genere primario da una lista"""
        if not genres:
            return None
        
        priority_order = [
            # Latin genres (alta priorità)
            'salsa', 'bachata', 'merengue', 'reggaeton', 'cumbia', 'mambo',
            'tango', 'bossa nova', 'samba', 'tropical', 'vallenato',
            
            # Specific genres
            'jazz fusion', 'progressive rock', 'death metal', 'drum and bass',
            'deep house', 'tech house', 'minimal techno',
            
            # Common genres
            'rock', 'pop', 'jazz', 'blues', 'electronic', 'hip hop',
            'metal', 'reggae', 'folk', 'country', 'classical',
            
            # Generic (bassa priorità)
            'alternative', 'indie', 'experimental', 'world'
        ]
        
        for priority_genre in priority_order:
            for genre in genres:
                if priority_genre == genre or priority_genre in genre:
                    return genre
        
        return genres[0]
    
    def search_lastfm(self, artist: str, title: str) -> Optional[Dict]:
        """
        Cerca metadati su Last.fm
        
        Args:
            artist: Nome artista
            title: Titolo traccia
            
        Returns:
            Dict con metadati o None
        """
        # v1087.3 (security Fase 2): prova prima il proxy server-side.
        # I token Last.fm/Discogs/Spotify NON sono piu' nel client →
        # il server li tiene e fa la chiamata. Se il proxy risponde,
        # usiamo quello. Altrimenti fallback al codice diretto sotto
        # (che con api_key None ritornera' None graceful).
        proxied = self._proxy_lookup("lastfm", artist, title)
        if proxied is not None:
            cache_key = f"lfm_{artist}_{title}"
            self.metadata_cache[cache_key] = proxied
            return proxied

        if not requests:
            return None
        
        api_key = self.api_keys.LASTFM_API_KEY
        if not api_key or api_key == "YOUR_LASTFM_API_KEY":
            self.logger.debug("Last.fm API key non configurata")
            return None
        
        cache_key = f"lfm_{artist}_{title}"
        if cache_key in self.metadata_cache:
            return self.metadata_cache[cache_key]
        
        try:
            # Rate limiting
            elapsed = time.time() - self.last_lastfm_call
            if elapsed < self.settings.api.lastfm_rate_limit:
                time.sleep(self.settings.api.lastfm_rate_limit - elapsed)
            
            self.last_lastfm_call = time.time()
            self.api_calls += 1
            
            # Track info
            track_params = {
                'method': 'track.getInfo',
                'artist': artist,
                'track': title,
                'api_key': api_key,
                'format': 'json'
            }
            
            response = requests.get(
                'https://ws.audioscrobbler.com/2.0/',
                params=track_params,
                timeout=self.settings.api.timeout
            )
            
            if response.status_code != 200:
                self.logger.debug(f"Last.fm HTTP error: {response.status_code}")
                self.metadata_cache[cache_key] = None
                return None
            
            data = response.json()
            
            if 'track' not in data or 'error' in data:
                self.metadata_cache[cache_key] = None
                return None
            
            track = data['track']
            metadata = {
                'title': track.get('name'),
                'artist': track.get('artist', {}).get('name') if isinstance(track.get('artist'), dict) else artist
            }
            
            # Album info
            if track.get('album'):
                metadata['album'] = track['album'].get('title')
            
            # Duration
            if track.get('duration'):
                try:
                    duration_ms = int(track['duration'])
                    if duration_ms > 0:
                        metadata['duration'] = duration_ms / 1000.0
                except (ValueError, TypeError):
                    pass
            
            # Extract genres (implementazione completa dal file originale)
            genres = self._extract_lastfm_genres(track, artist, api_key)
            
            if genres:
                unique_genres = list(dict.fromkeys(genres))
                primary_genre = self._select_primary_genre(unique_genres)
                if primary_genre:
                    metadata['genre'] = primary_genre
                metadata['all_genres'] = unique_genres[:5]
            
            # Playcount
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
            
            # Clean metadata
            cleaned_metadata = {k: v for k, v in metadata.items() 
                              if v is not None and str(v).strip()}
            
            self.logger.debug(f"Last.fm: metadati completi: {list(cleaned_metadata.values())}")
            self.logger.info(f">-- Last.fm: trovati metadati ({cleaned_metadata.get('genre', 'genere sconosciuto')})")
            self.metadata_cache[cache_key] = cleaned_metadata
            return cleaned_metadata
            
        except Exception as e:
            self.logger.debug(f"Last.fm error: {e}")
            self.metadata_cache[cache_key] = None
            return None
    
    def _extract_lastfm_genres(self, track: Dict, artist: str, api_key: str) -> List[str]:
        """Estrae generi da Last.fm (track, artist, album)"""
        genres = []
        
        # Track tags
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
        
        # Artist tags (se non abbiamo generi)
        if not genres:
            try:
                artist_params = {
                    'method': 'artist.getInfo',
                    'artist': artist,
                    'api_key': api_key,
                    'format': 'json'
                }
                
                time.sleep(0.3)
                artist_response = requests.get(
                    'https://ws.audioscrobbler.com/2.0/',
                    params=artist_params,
                    timeout=self.settings.api.timeout
                )
                
                if artist_response.status_code == 200:
                    artist_data = artist_response.json()
                    if 'artist' in artist_data:
                        artist_tags = artist_data['artist'].get('tags', {}).get('tag', [])
                        if isinstance(artist_tags, list):
                            for tag in artist_tags:
                                if isinstance(tag, dict):
                                    tag_name = tag.get('name', '').lower().strip()
                                    if tag_name and self._is_music_genre_tag(tag_name):
                                        genres.append(tag_name)
            except Exception:
                pass
        
        return genres
    
    def get_spotify_metadata(self, artist: str, title: str) -> Optional[Dict]:
        """
        Cerca metadati su Spotify
        
        Args:
            artist: Nome artista
            title: Titolo traccia
            
        Returns:
            Dict con metadati o None
        """
        # v1087.3 (security Fase 2): proxy-first. SPOTIFY_CLIENT_SECRET
        # non e' piu' nel client → senza proxy questo metodo ritornava
        # sempre None. Col proxy il server fa l'OAuth e la search.
        proxied = self._proxy_lookup("spotify", artist, title)
        if proxied is not None:
            cache_key = f"sp_{artist}_{title}"
            self.metadata_cache[cache_key] = proxied
            return proxied

        if not requests:
            return None
        
        cache_key = f"sp_{artist}_{title}"
        if cache_key in self.metadata_cache:
            self.logger.debug("Spotify: Cache trovate")
            return self.metadata_cache[cache_key]
        
        try:
            import base64
            
            client_id = self.api_keys.SPOTIFY_CLIENT_ID
            client_secret = self.api_keys.SPOTIFY_CLIENT_SECRET

            # v1086.7 security-audit: client_secret puo' essere None
            # (sposto server-side). Skip graceful con log.
            if not client_id or not client_secret or                client_id == "YOUR_SPOTIFY_CLIENT_ID":
                self.logger.debug(
                    "Credenziali Spotify non configurate lato client "
                    "(server proxy non ancora implementato)")
                return None
            
            # Rate limiting
            elapsed = time.time() - self.last_spotify_call
            if elapsed < self.settings.api.spotify_rate_limit:
                time.sleep(self.settings.api.spotify_rate_limit - elapsed)
            
            self.last_spotify_call = time.time()
            self.api_calls += 1
            
            # Get access token
            auth_string = f"{client_id}:{client_secret}"
            auth_bytes = auth_string.encode("utf-8")
            auth_b64 = base64.b64encode(auth_bytes).decode("utf-8")
            
            token_headers = {
                "Authorization": f"Basic {auth_b64}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            token_data = {"grant_type": "client_credentials"}
            
            token_response = requests.post(
                "https://accounts.spotify.com/api/token",
                headers=token_headers,
                data=token_data,
                timeout=self.settings.api.timeout
            )
            
            if token_response.status_code != 200:
                self.logger.debug(f"Spotify token error: {token_response.status_code}")
                self.metadata_cache[cache_key] = None
                return None
            
            access_token = token_response.json().get("access_token")
            if not access_token:
                return None
            
            # Search track
            search_headers = {"Authorization": f"Bearer {access_token}"}
            search_params = {
                "q": f'artist:"{artist}" track:"{title}"',
                "type": "track",
                "limit": 1
            }
            
            search_response = requests.get(
                "https://api.spotify.com/v1/search",
                headers=search_headers,
                params=search_params,
                timeout=self.settings.api.timeout
            )
            
            if search_response.status_code != 200:
                self.logger.debug(f"Spotify search error: {search_response.status_code}")
                self.metadata_cache[cache_key] = None
                return None
            
            search_data = search_response.json()
            tracks = search_data.get("tracks", {}).get("items", [])
            
            if not tracks:
                return None
            
            track = tracks[0]
            
            # Extract metadata
            metadata = {
                'title': track.get('name'),
                'artist': track['artists'][0]['name'] if track.get('artists') else None,
                'album': track['album']['name'] if track.get('album') else None,
                'year': track['album']['release_date'][:4] if track.get('album', {}).get('release_date') else None,
                'track_num': str(track.get('track_number')) if track.get('track_number') else None,
                'duration': track.get('duration_ms', 0) / 1000.0 if track.get('duration_ms') else None,
                'popularity': track.get('popularity'),
                'spotify_url': track.get('external_urls', {}).get('spotify')
            }
            
            # Cover art
            if track.get('album', {}).get('images'):
                images = track['album']['images']
                cover_url = images[0]['url'] if images else None
                if cover_url:
                    metadata['cover_url'] = cover_url
            
            self.logger.debug(f"Spotify: metadati completi: {list(metadata.values())}")
            self.logger.info(f">-- Spotify: trovati metadati ({metadata.get('genre', 'genere sconosciuto')})")
            self.metadata_cache[cache_key] = metadata
            return metadata
            
        except Exception as e:
            self.logger.debug(f"Spotify error: {e}")
            self.metadata_cache[cache_key] = None
            return None

    # ─── DEEZER ──────────────────────────────────────────────────────────────
    # API gratuita, nessuna autenticazione richiesta. Ottima copertura pop/latin.

    def search_deezer(self, artist: str, title: str) -> Optional[Dict]:
        """Cerca metadati su Deezer (API pubblica, nessuna chiave richiesta)."""
        if not requests:
            return None
        cache_key = f"deezer_{artist}_{title}"
        if cache_key in self.metadata_cache:
            return self.metadata_cache[cache_key]
        try:
            import time as _time
            elapsed = _time.time() - getattr(self, '_last_deezer_call', 0)
            if elapsed < 0.5:
                _time.sleep(0.5 - elapsed)
            self._last_deezer_call = _time.time()

            resp = requests.get(
                "https://api.deezer.com/search",
                params={"q": f'artist:"{artist}" track:"{title}"', "limit": 1},
                timeout=self.settings.api.timeout,
            )
            if resp.status_code != 200:
                self.logger.debug(f"Deezer HTTP {resp.status_code}")
                return None

            data = resp.json()
            tracks = data.get("data", [])
            if not tracks:
                self.logger.debug("Deezer: nessun risultato")
                return None

            t = tracks[0]
            genre = None
            # Deezer restituisce genre nel dettaglio album — fetch separato se disponibile
            album_id = t.get("album", {}).get("id")
            if album_id:
                try:
                    alb = requests.get(
                        f"https://api.deezer.com/album/{album_id}",
                        timeout=self.settings.api.timeout,
                    ).json()
                    genres = alb.get("genres", {}).get("data", [])
                    if genres:
                        genre = genres[0].get("name")
                except Exception:
                    pass

            metadata = {
                "title":    t.get("title"),
                "artist":   t.get("artist", {}).get("name"),
                "album":    t.get("album", {}).get("title"),
                "duration": t.get("duration"),
                "genre":    genre,
                "cover_url": t.get("album", {}).get("cover_xl") or t.get("album", {}).get("cover_big"),
            }
            metadata = {k: v for k, v in metadata.items() if v}
            if metadata:
                genre_label = metadata.get("genre", "genere sconosciuto")
                self.logger.debug(f"Deezer: trovati metadati ({genre_label})")
                self.metadata_cache[cache_key] = metadata
                return metadata
        except Exception as e:
            self.logger.debug(f"Deezer error: {e}")
        self.metadata_cache[cache_key] = None
        return None

    # ─── ITUNES SEARCH API ───────────────────────────────────────────────────
    # API pubblica Apple, gratuita, nessuna autenticazione. Ottima per pop/latin.

    def search_itunes(self, artist: str, title: str) -> Optional[Dict]:
        """Cerca metadati sull'iTunes Search API (pubblica, nessuna chiave)."""
        if not requests:
            return None
        cache_key = f"itunes_{artist}_{title}"
        if cache_key in self.metadata_cache:
            return self.metadata_cache[cache_key]
        try:
            import time as _time
            elapsed = _time.time() - getattr(self, '_last_itunes_call', 0)
            if elapsed < 0.2:
                _time.sleep(0.2 - elapsed)
            self._last_itunes_call = _time.time()

            resp = requests.get(
                "https://itunes.apple.com/search",
                params={
                    "term":        f"{artist} {title}",
                    "media":       "music",
                    "entity":      "song",
                    "limit":       3,
                    "country":     "US",
                },
                timeout=self.settings.api.timeout,
            )
            if resp.status_code != 200:
                self.logger.debug(f"iTunes HTTP {resp.status_code}")
                return None

            results = resp.json().get("results", [])
            if not results:
                self.logger.debug("iTunes: nessun risultato")
                return None

            # Scegli il risultato con artista più simile
            artist_lower = artist.lower()
            best = None
            for r in results:
                if artist_lower in (r.get("artistName") or "").lower():
                    best = r
                    break
            if not best:
                best = results[0]

            metadata = {
                "title":    best.get("trackName"),
                "artist":   best.get("artistName"),
                "album":    best.get("collectionName"),
                "year":     (best.get("releaseDate") or "")[:4] or None,
                "genre":    best.get("primaryGenreName"),
                "duration": (best.get("trackTimeMillis") or 0) / 1000.0 or None,
                "cover_url": (best.get("artworkUrl100") or "").replace("100x100", "600x600") or None,
            }
            metadata = {k: v for k, v in metadata.items() if v}
            if metadata:
                genre_label = metadata.get("genre", "genere sconosciuto")
                self.logger.debug(f"iTunes: trovati metadati ({genre_label})")
                self.metadata_cache[cache_key] = metadata
                return metadata
        except Exception as e:
            self.logger.debug(f"iTunes error: {e}")
        self.metadata_cache[cache_key] = None
        return None

    # ─── DISCOGS ─────────────────────────────────────────────────────────────
    # REST API gratuita. Richiede token personale (ottieni su discogs.com/settings/developers).

    def search_discogs(self, artist: str, title: str) -> Optional[Dict]:
        """Cerca metadati su Discogs. v1087.3: il token e' ora SOLO
        sul server → prova prima il proxy. Fallback diretto solo se
        un DISCOGS_TOKEN fosse settato via env (dev mode)."""
        proxied = self._proxy_lookup("discogs", artist, title)
        if proxied is not None:
            cache_key = f"discogs_{artist}_{title}"
            self.metadata_cache[cache_key] = proxied
            return proxied

        if not requests:
            return None
        token = getattr(self.api_keys, 'DISCOGS_TOKEN', None)
        if not token:
            return None
        cache_key = f"discogs_{artist}_{title}"
        if cache_key in self.metadata_cache:
            return self.metadata_cache[cache_key]
        try:
            import time as _time
            elapsed = _time.time() - getattr(self, '_last_discogs_call', 0)
            if elapsed < 1.0:   # Discogs: max 60 req/min
                _time.sleep(1.0 - elapsed)
            self._last_discogs_call = _time.time()

            resp = requests.get(
                "https://api.discogs.com/database/search",
                params={
                    "artist": artist,
                    "track":  title,
                    "type":   "release",
                    "per_page": 3,
                },
                headers={
                    "Authorization": f"Discogs token={token}",
                    "User-Agent":    "MusicCatalogerAdvanced/v1046",
                },
                timeout=self.settings.api.timeout,
            )
            if resp.status_code == 401:
                self.logger.debug("Discogs: token non valido")
                return None
            if resp.status_code != 200:
                self.logger.debug(f"Discogs HTTP {resp.status_code}")
                return None

            results = resp.json().get("results", [])
            if not results:
                self.logger.debug("Discogs: nessun risultato")
                return None

            r = results[0]
            genres  = r.get("genre", [])
            styles  = r.get("style", [])
            # Preferisci style (più specifico) se disponibile
            genre   = styles[0] if styles else (genres[0] if genres else None)
            year    = str(r.get("year")) if r.get("year") else None

            metadata = {
                "title":    title,
                "artist":   artist,
                "album":    r.get("title"),
                "year":     year,
                "genre":    genre,
                "all_genres": genres + styles,
                "cover_url":  r.get("cover_image"),
            }
            metadata = {k: v for k, v in metadata.items() if v}
            if metadata:
                genre_label = metadata.get("genre", "genere sconosciuto")
                self.logger.debug(f"Discogs: trovati metadati ({genre_label})")
                self.metadata_cache[cache_key] = metadata
                return metadata
        except Exception as e:
            self.logger.debug(f"Discogs error: {e}")
        self.metadata_cache[cache_key] = None
        return None

    # ─── AUDD ────────────────────────────────────────────────────────────────
    # Fingerprinting audio (Shazam-like). 100 req/giorno gratis. Richiede AuddAPI token.
    # Utile come ultimo resort per file senza metadati riconoscibili.

    def search_audd(self, file_path) -> Optional[Dict]:
        """Identifica un brano tramite fingerprinting audio con AudD (richiede AUDD_API_KEY)."""
        if not requests:
            return None
        token = getattr(self.api_keys, 'AUDD_API_KEY', None)
        if not token:
            return None
        try:
            import time as _time
            elapsed = _time.time() - getattr(self, '_last_audd_call', 0)
            if elapsed < 2.0:
                _time.sleep(2.0 - elapsed)
            self._last_audd_call = _time.time()

            from pathlib import Path as _Path
            fp = _Path(file_path)
            if not fp.exists():
                return None

            with open(fp, 'rb') as f:
                resp = requests.post(
                    "https://api.audd.io/",
                    data={"api_token": token, "return": "apple_music,deezer"},
                    files={"file": (fp.name, f, "audio/mpeg")},
                    timeout=30,
                )
            if resp.status_code != 200:
                self.logger.debug(f"AudD HTTP {resp.status_code}")
                return None

            data = resp.json()
            if data.get("status") != "success" or not data.get("result"):
                self.logger.debug("AudD: nessun risultato")
                return None

            r = data["result"]
            # Prova a prendere il genere da Apple Music se disponibile
            genre = None
            apple = r.get("apple_music", {})
            if apple and apple.get("genreNames"):
                genre = apple["genreNames"][0]

            metadata = {
                "title":    r.get("title"),
                "artist":   r.get("artist"),
                "album":    r.get("album"),
                "year":     r.get("release_date", "")[:4] or None,
                "genre":    genre,
            }
            metadata = {k: v for k, v in metadata.items() if v}
            if metadata:
                self.logger.debug(f"AudD: identificato '{r.get('title')}' da {r.get('artist')}")
                return metadata
        except Exception as e:
            self.logger.debug(f"AudD error: {e}")
        return None

    # ─── ACOUSTID ────────────────────────────────────────────────────────────
    # Fingerprinting tramite fpcalc (Chromaprint). Preciso, open source.
    # Richiede: pip install pyacoustid + fpcalc.exe nella cartella progetto/PATH.

    def search_acoustid(self, file_path) -> Optional[Dict]:
        """Identifica un brano tramite fingerprinting AcoustID (richiede ACOUSTID_API_KEY + fpcalc)."""
        token = getattr(self.api_keys, 'ACOUSTID_API_KEY', None)
        if not token or not requests:
            return None
        try:
            import acoustid
            import time as _time
            elapsed = _time.time() - getattr(self, '_last_acoustid_call', 0)
            if elapsed < 0.5:
                _time.sleep(0.5 - elapsed)
            self._last_acoustid_call = _time.time()

            results = acoustid.match(token, str(file_path), meta="recordings releases")
            for score, recording_id, title, artist in results:
                if score < 0.5:
                    continue
                self.logger.debug(f"AcoustID: match {score:.0%} → '{title}' da '{artist}'")
                # Recupera genere da MusicBrainz con il recording_id
                genre = None
                if musicbrainzngs and recording_id:
                    try:
                        rec = musicbrainzngs.get_recording_by_id(
                            recording_id, includes=["tags", "releases"]
                        )
                        tags = rec.get("recording", {}).get("tag-list", [])
                        if tags:
                            genre = tags[0].get("name")
                    except Exception:
                        pass
                return {
                    "title":  title,
                    "artist": artist,
                    "genre":  genre,
                    "acoustid_score": round(score, 2),
                }
        except ImportError:
            self.logger.debug("AcoustID: libreria pyacoustid non installata")
        except Exception as e:
            self.logger.debug(f"AcoustID error: {e}")
        return None
