"""
Gestione file system: spostamento, pulizia nomi, organizzazione
Estratto da MusicCatalogerAdvanced_v0020.py
"""

import logging
import re
import shutil
from pathlib import Path
from typing import Dict, Optional

class FileManager:
    """
    Classe per gestire operazioni sui file MP3
    """
    
    def __init__(self, base_path: Path, settings, dry_run=False, logger=None):
        """
        Inizializza il file manager
        
        Args:
            base_path: Directory base della collezione musicale
            settings: Oggetto con le configurazioni
            dry_run: Se True, simula operazioni senza modifiche
            logger: Logger per output (opzionale)
        """
        self.base_path = Path(base_path)
        self.settings = settings
        self.dry_run = dry_run
        self.logger = logger or logging.getLogger(__name__)
        
        # Statistiche
        self.moved_files = 0
        self.renamed_files = 0
        self.folders_created = 0
    
    def clean_filename(self, name: str) -> str:
        """
        Pulisce un nome per uso come file/cartella
        Rimuove caratteri non validi del filesystem
        
        Args:
            name: Nome da pulire
            
        Returns:
            Nome pulito
        """
        # Rimuovi caratteri non validi
        cleaned = re.sub(self.settings.files.invalid_filename_chars, '', name)
        
        # Rimuovi spazi multipli
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Limita lunghezza (opzionale, alcuni FS hanno limiti)
        max_length = 200
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length].strip()
        
        return cleaned
    
    def move_to_folder(self, file_path: Path, destination_folder: Path, 
                       handle_duplicates: bool = True) -> Optional[Path]:
        """
        Sposta un file in una cartella di destinazione
        
        Args:
            file_path: Path del file sorgente
            destination_folder: Path della cartella destinazione (relativa a base_path)
            handle_duplicates: Se True, rinomina automaticamente i duplicati
            
        Returns:
            Path finale del file spostato o None se fallito
        """
        if not file_path.exists():
            self.logger.warning(f"File non esistente: {file_path.name}")
            return None
        
        # Crea path destinazione completo
        full_destination_folder = self.base_path / destination_folder
        
        if self.dry_run:
            self.logger.info(f"[SIMULAZIONE] {file_path.name} -> {destination_folder}/")
            return full_destination_folder / file_path.name
        
        try:
            # Crea cartella se non esiste
            if not full_destination_folder.exists():
                full_destination_folder.mkdir(parents=True, exist_ok=True)
                self.folders_created += 1
                self.logger.debug(f"Cartella creata: {destination_folder}")
            
            # Determina nome file destinazione
            destination = full_destination_folder / file_path.name
            original_name = file_path.name
            
            # Gestione duplicati
            if destination.exists() and handle_duplicates:
                destination = self._handle_duplicate_filename(
                    full_destination_folder, 
                    file_path
                )
                self.renamed_files += 1
            
            # Sposta file
            shutil.move(str(file_path), str(destination))
            self.moved_files += 1
            
            # Log risultato
            if destination.name != original_name:
                self.logger.info(
                    f"└── Spostamento completato -> {destination_folder}\\{destination.name} (rinominato)"
                )
            else:
                self.logger.info(f"└── Spostamento completato -> {destination_folder}")
            
            return destination
            
        except Exception as e:
            self.logger.error(f"Errore spostamento {file_path.name}: {e}")
            return None
    
    def _handle_duplicate_filename(self, folder: Path, file_path: Path) -> Path:
        """
        Gestisce conflitti di nome file aggiungendo contatore
        
        Args:
            folder: Cartella destinazione
            file_path: File da rinominare
            
        Returns:
            Path con nome univoco
        """
        stem = file_path.stem
        suffix = file_path.suffix
        counter = 1
        
        destination = folder / file_path.name
        
        while destination.exists():
            new_name = f"{stem}_{counter}{suffix}"
            destination = folder / new_name
            counter += 1
            
            # Sicurezza: evita loop infiniti
            if counter > 1000:
                self.logger.error(f"Troppi duplicati per {file_path.name}")
                break
        
        self.logger.debug(f"Nome modificato per conflitto: {destination.name}")
        return destination
    
    def scan_mp3_files(self, recursive: bool = False) -> list:
        """
        Scansiona directory per file MP3
        
        Args:
            recursive: Se True, scansiona anche sottocartelle
            
        Returns:
            Lista di Path ai file MP3 trovati
        """
        mp3_files = []
        
        if recursive:
            # Scansione ricorsiva
            for ext in self.settings.files.supported_extensions:
                mp3_files.extend(self.base_path.rglob(f"*{ext}"))
        else:
            # Solo directory principale — v1030 BUG-01 FIX:
            # glob("*.mp3") è già non-ricorsivo, ma su Windows con certi
            # filesystem può restituire file nelle sottocartelle se la
            # cartella base contiene symlink o junction points.
            # Il filtro esplicito f.parent == base_path garantisce che
            # si prendano SOLO i file nella root, mai quelli già classificati
            # nelle sottocartelle genere (Latin/Salsa/, Rock/, ecc.)
            for ext in self.settings.files.supported_extensions:
                pattern = f"*{ext}"
                mp3_files.extend(
                    f for f in self.base_path.glob(pattern)
                    if f.parent == self.base_path   # ← ROOT ONLY
                )
        
        # Filtra solo file (non directory) ed escludi cartelle da ignorare
        filtered_files = []
        for f in mp3_files:
            if not f.is_file():
                continue
            
            # Check se il file è in cartelle da ignorare
            should_ignore = False
            for ignore_folder in self.settings.files.ignore_folders:
                if ignore_folder in f.parts:
                    should_ignore = True
                    break
            
            if not should_ignore:
                filtered_files.append(f)
        
        return filtered_files
    
    def cleanup_empty_folders(self, root_only: bool = False) -> int:
        """
        Rimuove cartelle vuote
        
        Args:
            root_only: Se True, rimuove solo cartelle nella root
            
        Returns:
            Numero di cartelle rimosse
        """
        if self.dry_run:
            self.logger.info("[SIMULAZIONE] Controllerei cartelle vuote")
            return 0
        
        removed_count = 0
        
        if root_only:
            # Solo cartelle dirette nella base_path
            folders = [f for f in self.base_path.iterdir() if f.is_dir()]
        else:
            # Tutte le cartelle ricorsivamente (dal basso verso l'alto)
            folders = sorted(self.base_path.rglob("*"), key=lambda x: len(x.parts), reverse=True)
            folders = [f for f in folders if f.is_dir()]
        
        for folder in folders:
            # Skip cartelle speciali
            if folder.name.startswith('.'):
                continue
            
            # Skip cartelle da ignorare
            if folder.name in self.settings.files.ignore_folders:
                continue
            
            try:
                # Controlla se vuota
                if not any(folder.iterdir()):
                    folder.rmdir()
                    self.logger.info(f"Rimossa cartella vuota: {folder.relative_to(self.base_path)}")
                    removed_count += 1
            except Exception as e:
                self.logger.debug(f"Errore rimozione cartella {folder.name}: {e}")
        
        return removed_count
    
    def analyze_collection_structure(self) -> Dict:
        """
        Analizza la struttura della collezione
        
        Returns:
            Dict con statistiche per genere/cartella
        """
        genre_stats = {}
        
        # Scansiona cartelle genere
        for genre_folder in self.base_path.iterdir():
            if not genre_folder.is_dir() or genre_folder.name.startswith('.'):
                continue
            
            # Conta MP3 (non ricorsivo per evitare sottocartelle)
            mp3_count = sum(
                1 for f in genre_folder.glob("*.[mM][pP]3") 
                if f.is_file()
            )
            
            if mp3_count > 0:
                genre_stats[genre_folder.name] = mp3_count
            
            # Se è Latin, controlla anche sottocartelle
            if genre_folder.name == 'Latin':
                for subfolder in genre_folder.iterdir():
                    if subfolder.is_dir():
                        sub_count = sum(
                            1 for f in subfolder.glob("*.[mM][pP]3")
                            if f.is_file()
                        )
                        if sub_count > 0:
                            genre_stats[f"Latin/{subfolder.name}"] = sub_count
        
        return genre_stats
    
    def get_stats(self) -> dict:
        """Restituisce statistiche operazioni"""
        return {
            'moved_files': self.moved_files,
            'renamed_files': self.renamed_files,
            'folders_created': self.folders_created,
            'dry_run': self.dry_run
        }
    
    def reset_stats(self):
        """Resetta contatori statistiche"""
        self.moved_files = 0
        self.renamed_files = 0
        self.folders_created = 0