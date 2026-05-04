"""
Classificazione e normalizzazione generi musicali
Include: Bachata Dominicana detection
"""

import logging
import re
from pathlib import Path
from typing import Dict, Optional, Tuple


class GenreClassifier:
    """Classifica, normalizza e riconosce generi musicali"""

    def __init__(self, settings, logger=None):
        self.settings = settings
        self.logger = logger or logging.getLogger(__name__)
        self.genre_cache = {}

        self.genre_mapping = settings.genre.genre_mapping
        self.latin_subgenres = settings.genre.latin_subgenres
        self.bachata_indicators = settings.genre.bachata_indicators
        self.salsa_indicators = settings.genre.salsa_indicators
        self.latin_indicators_generic = settings.genre.latin_indicators_generic

        # Bachata subtypes settings
        self.bachata_cfg = settings.bachata

    # ─── GENRE NORMALIZATION ────────────────────────────────────────────

    def normalize_genre(self, genre: str) -> Optional[str]:
        if not genre:
            return None
        # v1027 BUG-02 FIX: .lower().strip() garantito prima di qualsiasi lookup
        gl = genre.lower().strip()
        if gl in self.genre_cache:
            return self.genre_cache[gl]

        # Exact match (le chiavi in genre_mapping sono già tutte lowercase)
        if gl in self.genre_mapping:
            result = self.genre_mapping[gl]
            self.genre_cache[gl] = result
            return result

        # Partial match
        for key, val in self.genre_mapping.items():
            if key in gl or gl in key:
                self.genre_cache[gl] = val
                return val

        # Word match
        for word in gl.split():
            if word in self.genre_mapping:
                result = self.genre_mapping[word]
                self.genre_cache[gl] = result
                return result

        self.genre_cache[gl] = 'Other'
        return 'Other'

    def is_latin_subgenre(self, genre: str) -> bool:
        if not genre:
            return False
        gl = genre.lower()
        # Match esatto
        if gl in self.latin_subgenres:
            return True
        # v1044: match parziale — gestisce varianti come 'salsa choke', 'salsa peruana',
        # 'salsaton', 'bachata sensual', ecc. che non sono nella lista esatta
        for sub in self.latin_subgenres:
            if sub in gl:   # es. 'salsa' in 'salsaton' → True
                return True
        return False

    # ─── BACHATA SUBTYPE DETECTION ───────────────────────────────────────

    def detect_bachata_subtype(
        self,
        artist: str,
        title: str,
        album: str,
        metadata: Dict
    ) -> str:
        """
        Determina il sottotipo di Bachata:
        - 'Dominicana': stile tradizionale dominicano
        - 'Fusion': stile moderno/urban
        - 'Sensual': stile sensual
        - 'Bachata': generico se non determinabile

        Logica (punteggi):
        1. Tag espliciti nei metadati (massima priorita')
        2. Lista artisti noti
        3. Parole chiave nel titolo/album
        4. BPM (dominicana tende ad essere piu' lenta)
        """
        combined = f"{artist} {title} {album}".lower()

        # 1. Tag espliciti nei metadati
        existing_genre = metadata.get('genre', '').lower()
        if 'dominican' in existing_genre or 'tipic' in existing_genre or 'tradicional' in existing_genre:
            self.logger.debug(f"Bachata Dominicana da tag esplicito: '{existing_genre}'")
            return 'Dominicana'
        if 'fusion' in existing_genre:
            return 'Fusion'
        if 'sensual' in existing_genre:
            return 'Sensual'

        # 2. Lista artisti
        artist_lower = artist.lower()
        for dom_artist in self.bachata_cfg.dominicana_artists:
            if dom_artist in artist_lower or artist_lower in dom_artist:
                self.logger.debug(f"Bachata Dominicana da artista: '{artist}'")
                return 'Dominicana'

        for fus_artist in self.bachata_cfg.fusion_artists:
            if fus_artist in artist_lower:
                self.logger.debug(f"Bachata Fusion da artista: '{artist}'")
                return 'Fusion'

        for sen_artist in self.bachata_cfg.sensual_artists:
            if sen_artist in artist_lower:
                return 'Sensual'

        # 3. Parole chiave nel titolo/album
        dom_score = sum(1 for kw in self.bachata_cfg.dominicana_keywords if kw in combined)
        fus_score = sum(1 for kw in self.bachata_cfg.fusion_keywords if kw in combined)

        if dom_score > fus_score and dom_score >= 1:
            self.logger.debug(f"Bachata Dominicana da keywords (score={dom_score})")
            return 'Dominicana'
        if fus_score > dom_score and fus_score >= 1:
            return 'Fusion'

        # 4. BPM hint (dominicana < soglia configurata)
        bpm_str = metadata.get('bpm')
        if bpm_str:
            try:
                bpm = int(float(bpm_str))
                if bpm <= self.bachata_cfg.dominicana_bpm_max:
                    self.logger.debug(f"Bachata Dominicana da BPM ({bpm} <= {self.bachata_cfg.dominicana_bpm_max})")
                    return 'Dominicana'
                else:
                    return 'Fusion'
            except (ValueError, TypeError):
                pass

        return 'Bachata'  # generico

    # ─── LATIN SUBGENRE DETECTION ────────────────────────────────────────

    def detect_latin_subgenre(self, artist: str, title: str, filename: str, metadata: Dict) -> Optional[str]:
        combined = f"{artist} {title} {filename}".lower()
        bachata_score = 0
        salsa_score = 0

        # v1085d: matching con word boundary per evitare falsi positivi
        # come "timba" che matcha "Timbaland" o "salsa" che matcha
        # "salsacake". Compiliamo i pattern una sola volta per efficienza
        # (chiamato per ogni file della libreria).
        import re
        if not hasattr(self, '_compiled_indicators'):
            def _compile(indicators):
                # \b matcha confine parola: lettere/numeri vs altro.
                # Per indicatori con spazi (es. "El Gran Combo"), \b funziona
                # comunque sui bordi esterni.
                return [re.compile(r'\b' + re.escape(ind.lower()) + r'\b')
                        for ind in indicators]
            self._compiled_indicators = {
                'bachata': _compile(self.bachata_indicators),
                'salsa':   _compile(self.salsa_indicators),
            }

        for pat, raw in zip(self._compiled_indicators['bachata'],
                             self.bachata_indicators):
            if pat.search(combined):
                bachata_score += 2 if len(raw) > 6 else 1

        for pat, raw in zip(self._compiled_indicators['salsa'],
                             self.salsa_indicators):
            if pat.search(combined):
                salsa_score += 2 if len(raw) > 6 else 1

        bpm_str = metadata.get('bpm')
        if bpm_str:
            try:
                bpm = int(float(bpm_str))
                bmin, bmax = self.settings.bpm.bachata_bpm_range
                smin, smax = self.settings.bpm.salsa_bpm_range
                if bmin <= bpm <= bmax:
                    bachata_score += 1
                elif smin <= bpm <= smax:
                    salsa_score += 1
            except (ValueError, TypeError):
                pass

        # v1085d: word boundary anche sulle keyword letterali del title.
        # Senza \b, "salsa" matchava "salsacake", ecc.
        if re.search(r'\bbachata\b', title.lower()):
            bachata_score += 3
        if re.search(r'\bsalsa\b', title.lower()):
            salsa_score += 3

        if bachata_score > salsa_score and bachata_score >= 2:
            return 'Bachata'
        elif salsa_score > bachata_score and salsa_score >= 2:
            return 'Salsa'
        return None

    def infer_genre_from_filename(self, artist: str, filename_stem: str) -> Optional[str]:
        text = f"{artist} {filename_stem}".lower()
        # v1085d: word boundary per evitare matching parziali (es. "salsa"
        # in "salsacake", "merengue" in "merenguestyle"). Compiliamo i
        # pattern al primo accesso e li riutilizziamo.
        import re
        if not hasattr(self, '_compiled_generic'):
            self._compiled_generic = [
                (re.compile(r'\b' + re.escape(ind.lower()) + r'\b'), ind)
                for ind in self.latin_indicators_generic
            ]
        for pat, indicator in self._compiled_generic:
            if pat.search(text):
                if indicator in ['salsa', 'bachata', 'merengue', 'reggaeton']:
                    return indicator.capitalize()
                return 'Latin'
        return None

    # ─── MAIN GENRE DETERMINATION ────────────────────────────────────────

    def determine_genre(self, file_path: Path, final_metadata: Dict,
                        external_metadata: Optional[Dict]) -> Tuple[str, str]:
        """
        v1069b: Priorità di classificazione per Salsa/Bachata:
        1. FILENAME — se contiene "bachata" o "salsa" → vince sempre
        2. ARTISTI NOTI — se artista è in salsa_indicators/bachata_indicators
        3. DB ESTERNI — genere restituito dall'API (track)
        4. LATIN SUBGENRE DETECTION — score da indicatori + BPM
        5. ALL_GENRES — lista generi aggiuntivi da DB
        6. METADATI LOCALI — tag ID3 già presenti nel file
        7. UNKNOWN
        """
        genre = None
        raw_genre = None
        filename_lc = file_path.stem.lower()
        artist_lc   = (final_metadata.get('artist') or '').lower()
        title_lc    = (final_metadata.get('title')  or '').lower()

        # ── Priorità 1: keyword nel filename — SEMPRE vince ──────────────
        for kw in ['bachata', 'salsa', 'merengue', 'cumbia', 'reggaeton']:
            if kw in filename_lc or kw in title_lc:
                genre    = kw.capitalize()
                raw_genre = kw
                self.logger.debug(f"[P1-filename] Genere da filename: {genre}")
                break

        # ── Priorità 2: artisti noti salsa/bachata ───────────────────────
        # v1085g: word boundary regex per evitare falsi positivi tipo
        # "timba" che matcha "Timbaland" (era il bug Timbaland → Salsa
        # rimasto anche dopo v1085d/f, perché quei fix coprivano solo
        # detect_latin_subgenre e priorità 3, non questo blocco).
        import re as _re
        if not genre:
            for indicator in self.salsa_indicators:
                if len(indicator) > 4 and _re.search(
                        r'\b' + _re.escape(indicator.lower()) + r'\b', artist_lc):
                    genre     = 'Salsa'
                    raw_genre = 'salsa'
                    self.logger.debug(f"[P2-artist] Artista salsa noto: {indicator}")
                    break
            if not genre:
                for indicator in self.bachata_indicators:
                    if len(indicator) > 4 and _re.search(
                            r'\b' + _re.escape(indicator.lower()) + r'\b', artist_lc):
                        genre     = 'Bachata'
                        raw_genre = 'bachata'
                        self.logger.debug(f"[P2-artist] Artista bachata noto: {indicator}")
                        break

        # ── Priorità 3: DB esterni ───────────────────────────────────────
        if not genre and external_metadata and external_metadata.get('genre'):
            raw_genre = external_metadata['genre']
            raw_lower = raw_genre.lower().strip()
            if raw_lower in ['salsa', 'bachata', 'merengue', 'cumbia', 'reggaeton']:
                # v1085f: anche quando il DB dice direttamente "salsa" o
                # "bachata", verifichiamo con detect_latin_subgenre.
                # iTunes/Deezer a volte sbagliano (es. "Apologize" di
                # Timbaland & OneRepublic etichettato Salsa). Se NESSUN
                # indicatore latino matcha (artista/titolo/filename) e
                # il BPM non è coerente col genere, ignoriamo il DB.
                detected = self.detect_latin_subgenre(
                    final_metadata.get('artist', ''),
                    final_metadata.get('title', ''),
                    file_path.stem,
                    final_metadata
                )
                if detected and detected.lower() == raw_lower:
                    # DB e detect concordano: accetto
                    genre    = raw_lower.capitalize()
                    raw_genre = raw_lower
                elif detected:
                    # DB dice X, detect dice Y → fido del detect
                    self.logger.debug(
                        f"[P3-mismatch] DB dice '{raw_lower}' ma detect → {detected}")
                    genre    = detected
                    raw_genre = detected.lower()
                else:
                    # detect non trova nulla → DB probabilmente sbaglia.
                    # Caso tipico: iTunes etichetta come Salsa una traccia
                    # di Timbaland senza che artista/titolo abbiano alcun
                    # indicatore latino. Rifiutiamo il DB e proseguiamo
                    # con altre priorità (Latin generic, all_genres, etc.)
                    self.logger.debug(
                        f"[P3-reject] DB dice '{raw_lower}' ma nessun "
                        f"indicatore latino su {final_metadata.get('artist','')} "
                        f"- {final_metadata.get('title','')} → DB ignorato")
                    raw_genre = None
            else:
                genre = self.normalize_genre(raw_genre)
                # Se il DB dice "pop/rock/hip hop" ma l'artista suona solitamente
                # latin → non fidarci ciecamente (genere album ≠ genere traccia)
                # Verifichiamo con detect_latin_subgenre prima di accettare
                if genre and genre not in ('Other', 'Unknown', 'Latin'):
                    detected = self.detect_latin_subgenre(
                        final_metadata.get('artist', ''),
                        final_metadata.get('title', ''),
                        file_path.stem,
                        final_metadata
                    )
                    if detected:
                        self.logger.debug(
                            f"[P3-override] DB dice '{genre}' ma artista/filename → {detected}"
                        )
                        genre    = detected
                        raw_genre = detected.lower()

        # ── Priorità 4: latin subgenre detection ─────────────────────────
        if not genre or genre in ('Other', 'Latin'):
            detected = self.detect_latin_subgenre(
                final_metadata.get('artist', ''),
                final_metadata.get('title', ''),
                file_path.stem,
                final_metadata
            )
            if detected:
                genre    = detected
                raw_genre = detected.lower()

        # ── Priorità 5: all_genres da DB ─────────────────────────────────
        if (not genre or genre == 'Other') and external_metadata and external_metadata.get('all_genres'):
            for g in external_metadata['all_genres']:
                if g.lower() in ['salsa', 'bachata', 'merengue', 'cumbia', 'reggaeton']:
                    genre    = g.capitalize()
                    raw_genre = g.lower()
                    break
            if not genre or genre == 'Other':
                if external_metadata['all_genres']:
                    raw_genre = external_metadata['all_genres'][0]
                    genre = self.normalize_genre(raw_genre)

        # ── Priorità 6: metadati locali ID3 ──────────────────────────────
        if (not genre or genre == 'Other') and final_metadata.get('genre'):
            raw_genre = final_metadata['genre']
            genre = self.normalize_genre(raw_genre)

        # ── Fallback ──────────────────────────────────────────────────────
        if not genre or genre == 'Other':
            genre    = 'Unknown'
            raw_genre = 'unknown'

        self.logger.debug(f"Genere finale: '{genre}' (raw: '{raw_genre}')")
        return genre, raw_genre

    # ─── FOLDER PATH ────────────────────────────────────────────────────

    def get_genre_folder_path(self, genre: str, raw_genre: Optional[str] = None,
                               bachata_subtype: Optional[str] = None) -> Path:
        """
        Restituisce il path cartella per il genere.
        v1044: per varianti come 'salsa choke', 'salsaton', 'bachata sensual'
        estrae il termine base per costruire il path corretto.
        """
        if raw_genre and self.is_latin_subgenre(raw_genre):
            # Trova quale subgenere latino è contenuto nel raw_genre
            gl = raw_genre.lower()
            matched_sub = raw_genre  # default
            for sub in self.latin_subgenres:
                if sub == gl or sub in gl:
                    matched_sub = sub  # usa il termine canonico (es. 'salsa', 'bachata')
                    break
            base = Path('Latin') / matched_sub.capitalize()
            # Aggiungi sottocartella per bachata
            if matched_sub.lower() == 'bachata' and bachata_subtype and bachata_subtype != 'Bachata':
                return base / bachata_subtype
            return base
        return Path(genre)

    def get_cache_stats(self) -> dict:
        return {
            'cache_size': len(self.genre_cache),
            'genres_mapped': len(self.genre_mapping),
            'latin_subgenres': len(self.latin_subgenres),
            'bachata_indicators': len(self.bachata_indicators),
            'salsa_indicators': len(self.salsa_indicators),
        }
