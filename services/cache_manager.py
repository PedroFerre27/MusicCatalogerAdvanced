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
