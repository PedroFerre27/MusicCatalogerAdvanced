#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MP3 Cataloger - Catalogatore MP3 per Genere
Cataloga i file MP3 per genere, li smista in cartelle e aggiorna i metadati mancanti.
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

try:
    import eyed3
    eyed3.log.setLevel("ERROR")  # Riduce i log di eyed3
except ImportError:
    print("ERRORE: eyed3 non installato. Installa con: pip install eyed3")
    sys.exit(1)

try:
    from mutagen.mp3 import MP3
    from mutagen.id3 import ID3, TIT2, TPE1, TCON, TDRC, TBPM
except ImportError:
    print("ERRORE: mutagen non installato. Installa con: pip install mutagen")
    sys.exit(1)

class MP3Cataloger:
    def __init__(self, base_path, log_level=logging.INFO, dry_run=False):
        self.base_path = Path(base_path)
        self.dry_run = dry_run
        self.uncatalogued_files = []
        self.processed_files = 0
        self.moved_files = 0
        self.updated_files = 0
        
        # Configurazione logging
        self.setup_logging(log_level)
        
        # Mapping generi comuni per normalizzazione
        self.genre_mapping = {
            'rock': 'Rock',
            'pop': 'Pop',
            'jazz': 'Jazz',
            'classical': 'Classical',
            'hip hop': 'Hip Hop',
            'hip-hop': 'Hip Hop',
            'electronic': 'Electronic',
            'dance': 'Dance',
            'country': 'Country',
            'blues': 'Blues',
            'r&b': 'R&B',
            'reggae': 'Reggae',
            'folk': 'Folk',
            'metal': 'Metal',
            'punk': 'Punk',
            'alternative': 'Alternative',
            'indie': 'Indie',
            'soundtrack': 'Soundtrack',
            'world': 'World Music',
            'ambient': 'Ambient',
            'experimental': 'Experimental'
        }
    
    def setup_logging(self, log_level):
        """Configura il sistema di logging"""
        log_filename = f"mp3_cataloger_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
        self.logger.info(f"Avvio catalogazione MP3 in: {self.base_path}")
        self.logger.info(f"Log salvato in: {log_path}")
    
    def normalize_genre(self, genre):
        """Normalizza il nome del genere"""
        if not genre:
            return None
        
        genre_clean = re.sub(r'[^\w\s-]', '', genre.lower().strip())
        return self.genre_mapping.get(genre_clean, genre.title())
    
    def clean_filename(self, filename):
        """Pulisce il nome del file da caratteri non validi per le cartelle"""
        return re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    def extract_metadata_eyed3(self, file_path):
        """Estrae metadati usando eyed3"""
        try:
            audiofile = eyed3.load(file_path)
            if audiofile.tag is None:
                return None
            
            return {
                'title': audiofile.tag.title,
                'artist': audiofile.tag.artist,
                'genre': audiofile.tag.genre.name if audiofile.tag.genre else None,
                'year': audiofile.tag.recording_date.year if audiofile.tag.recording_date else None,
                'bpm': audiofile.tag.bpm[0] if audiofile.tag.bpm else None
            }
        except Exception as e:
            self.logger.warning(f"Errore eyed3 per {file_path}: {e}")
            return None
    
    def extract_metadata_mutagen(self, file_path):
        """Estrae metadati usando mutagen (fallback)"""
        try:
            audio = MP3(file_path)
            return {
                'title': audio.get('TIT2', [None])[0],
                'artist': audio.get('TPE1', [None])[0],
                'genre': audio.get('TCON', [None])[0],
                'year': int(str(audio.get('TDRC', [None])[0])[:4]) if audio.get('TDRC') else None,
                'bpm': int(str(audio.get('TBPM', [None])[0])) if audio.get('TBPM') else None
            }
        except Exception as e:
            self.logger.warning(f"Errore mutagen per {file_path}: {e}")
            return None
    
    def update_metadata_mutagen(self, file_path, metadata):
        """Aggiorna i metadati usando mutagen"""
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
            
            if updated:
                audio.save()
                self.logger.info(f"Metadati aggiornati per: {file_path.name}")
                self.updated_files += 1
            
            return updated
        except Exception as e:
            self.logger.error(f"Errore aggiornamento metadati per {file_path}: {e}")
            return False
    
    def guess_metadata_from_filename(self, file_path):
        """Cerca di indovinare i metadati dal nome del file"""
        filename = file_path.stem
        
        # Pattern comuni: "Artista - Titolo", "Titolo - Artista", "Anno - Artista - Titolo"
        patterns = [
            r'^(\d{4})\s*-\s*(.+?)\s*-\s*(.+)$',  # Anno - Artista - Titolo
            r'^(.+?)\s*-\s*(.+)$',                  # Artista - Titolo
        ]
        
        for pattern in patterns:
            match = re.match(pattern, filename)
            if match:
                groups = match.groups()
                if len(groups) == 3:  # Anno - Artista - Titolo
                    return {
                        'year': int(groups[0]),
                        'artist': groups[1].strip(),
                        'title': groups[2].strip()
                    }
                elif len(groups) == 2:  # Artista - Titolo
                    return {
                        'artist': groups[0].strip(),
                        'title': groups[1].strip()
                    }
        
        # Se nessun pattern funziona, usa il nome del file come titolo
        return {'title': filename}
    
    def process_mp3_file(self, file_path):
        """Processa un singolo file MP3"""
        self.logger.info(f"Processando: {file_path.name}")
        
        # Estrai metadati esistenti
        metadata = self.extract_metadata_eyed3(file_path)
        if not metadata:
            metadata = self.extract_metadata_mutagen(file_path)
        
        if not metadata:
            self.logger.warning(f"Impossibile leggere metadati per: {file_path.name}")
            metadata = {}
        
        # Cerca di indovinare metadati mancanti dal nome del file
        if not any(metadata.values()):
            guessed = self.guess_metadata_from_filename(file_path)
            metadata.update(guessed)
            self.logger.info(f"Metadati indovinati per {file_path.name}: {guessed}")
        
        # Aggiorna metadati mancanti
        if metadata and not self.dry_run:
            self.update_metadata_mutagen(file_path, metadata)
        elif metadata and self.dry_run:
            # In modalità simulazione, simula l'aggiornamento
            missing_metadata = []
            if not metadata.get('title'):
                missing_metadata.append('titolo')
            if not metadata.get('artist'): 
                missing_metadata.append('artista')
            if not metadata.get('year'):
                missing_metadata.append('anno')
            if not metadata.get('bpm'):
                missing_metadata.append('BPM')
                
            if missing_metadata:
                self.logger.info(f"[SIMULAZIONE] Aggiornerei metadati mancanti per {file_path.name}: {', '.join(missing_metadata)}")
                self.updated_files += 1
        
        # Normalizza il genere
        genre = self.normalize_genre(metadata.get('genre'))
        
        if not genre:
            self.logger.warning(f"Genere non trovato per: {file_path.name}")
            self.uncatalogued_files.append({
                'file': file_path.name,
                'reason': 'Genere non trovato',
                'metadata': metadata
            })
            return False
        
        # Crea cartella genere se non exists
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
        
        # Sposta il file
        destination = genre_folder / file_path.name
        try:
            # Evita sovrascritture
            if destination.exists():
                counter = 1
                while destination.exists():
                    name_parts = file_path.stem, counter, file_path.suffix
                    destination = genre_folder / f"{name_parts[0]}_{name_parts[1]}{name_parts[2]}"
                    counter += 1
            
            shutil.move(str(file_path), str(destination))
            self.logger.info(f"Spostato {file_path.name} -> {genre}/{destination.name}")
            self.moved_files += 1
            return True
            
        except Exception as e:
            self.logger.error(f"Errore spostamento {file_path.name}: {e}")
            return False
    
    def scan_and_catalog(self):
        """Scansiona e cataloga tutti i file MP3"""
        self.logger.info("Inizio scansione file MP3...")
        
        # Trova tutti i file MP3 nella directory principale
        mp3_files = list(self.base_path.glob("*.mp3"))
        mp3_files.extend(list(self.base_path.glob("*.MP3")))
        
        if not mp3_files:
            self.logger.warning("Nessun file MP3 trovato nella directory")
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
                    'metadata': {}
                })
    
    def generate_report(self):
        """Genera un report finale"""
        report_file = self.base_path / f"cataloging_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'base_path': str(self.base_path),
            'statistics': {
                'total_processed': self.processed_files,
                'successfully_moved': self.moved_files,
                'metadata_updated': self.updated_files,
                'uncatalogued': len(self.uncatalogued_files)
            },
            'uncatalogued_files': self.uncatalogued_files
        }
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Report salvato in: {report_file}")
        except Exception as e:
            self.logger.error(f"Errore salvataggio report: {e}")
        
        # Stampa summary
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
        
        if self.uncatalogued_files:
            self.logger.warning("File non catalogati:")
            for file_info in self.uncatalogued_files:
                self.logger.warning(f"  - {file_info['file']}: {file_info['reason']}")

def main():
    parser = argparse.ArgumentParser(description='MP3 Cataloger - Cataloga file MP3 per genere')
    parser.add_argument('path', help='Percorso della directory contenente i file MP3')
    parser.add_argument('-v', '--verbose', action='store_true', help='Output dettagliato')
    parser.add_argument('--dry-run', action='store_true', help='Modalità simulazione (non sposta file)')
    
    args = parser.parse_args()
    
    # Verifica che il percorso esista
    if not os.path.exists(args.path):
        print(f"ERRORE: Il percorso {args.path} non esiste")
        sys.exit(1)
    
    # Configura livello di log
    log_level = logging.DEBUG if args.verbose else logging.INFO
    
    # Inizializza catalogatore
    cataloger = MP3Cataloger(args.path, log_level, args.dry_run)
    
    if args.dry_run:
        cataloger.logger.info("NOTA: Nessuna modifica sarà effettuata ai file")
    
    try:
        # Esegui catalogazione
        cataloger.scan_and_catalog()
        
        # Genera report
        cataloger.generate_report()
        
    except KeyboardInterrupt:
        cataloger.logger.info("Catalogazione interrotta dall'utente")
    except Exception as e:
        cataloger.logger.error(f"Errore durante la catalogazione: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
