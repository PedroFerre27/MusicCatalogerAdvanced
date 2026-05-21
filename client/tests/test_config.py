"""
Test per verificare che le configurazioni funzionino
"""

import sys
from pathlib import Path

# Aggiungi la directory corrente al path
sys.path.insert(0, str(Path(__file__).parent))

def test_imports():
    """Test import moduli"""
    print("Test 1: Import moduli config...")
    try:
        from config.secrets import api_keys
        from config.settings import settings
        print("✓ Import riuscito")
        return True
    except ImportError as e:
        print(f"✗ Errore import: {e}")
        return False

def test_api_keys():
    """Test API keys"""
    print("\nTest 2: Verifica API keys...")
    try:
        from config.secrets import api_keys
        
        api_keys.print_status()
        
        missing = api_keys.get_missing_keys()
        if missing:
            print(f"\nATTENZIONE: Alcune API keys mancanti: {missing}")
        else:
            print("\n✓ Tutte le API keys configurate")
        
        return True
    except Exception as e:
        print(f"✗ Errore: {e}")
        return False

def test_main_class():
    """Test che la classe principale funzioni con config"""
    print("\nTest 3: Inizializzazione classe principale...")
    try:
        from MusicCatalogerAdvanced_v0020 import MusicCatalogerAdvanced
        
        # Prova a creare istanza in dry-run mode
        cataloger = MusicCatalogerAdvanced(
            base_path=".",  # Directory corrente
            dry_run=True,
            use_external_db=False  # Disabilita DB per test
        )
        
        # Verifica che le API keys siano state caricate
        assert cataloger.getsongbpm_api_key is not None
        assert cataloger.lastfm_api_key is not None
        
        print("✓ Classe inizializzata correttamente")
        print(f"  - GetSong API Key: {cataloger.getsongbpm_api_key[:20]}...")
        print(f"  - LastFM API Key: {cataloger.lastfm_api_key[:20]}...")
        
        return True
    except Exception as e:
        print(f"✗ Errore: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_settings():
    """Test configurazioni"""
    print("\nTest 4: Verifica settings...")
    try:
        from config.settings import settings
        
        # Test accesso settings
        assert settings.api.timeout > 0
        assert len(settings.genre.genre_mapping) > 0
        assert len(settings.bpm.difficulty_ranges) == 5
        
        print("✓ Settings caricate correttamente")
        print(f"  - Generi mappati: {len(settings.genre.genre_mapping)}")
        print(f"  - Livelli difficoltà Salsa: {len(settings.bpm.difficulty_ranges)}")
        print(f"  - BPM range valido: {settings.bpm.valid_range_min}-{settings.bpm.valid_range_max}")
        
        # Stampa riepilogo
        settings.print_summary()
        
        return True
    except Exception as e:
        print(f"✗ Errore: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_external_apis_module():
    """Test modulo External APIs"""
    print("\nTest 5: Modulo External APIs...")
    try:
        from services.external_apis import ExternalAPIs
        from config.secrets import api_keys
        from config.settings import settings
        import logging
        
        # Crea logger di test
        logger = logging.getLogger('test')
        
        # Inizializza External APIs
        ext_apis = ExternalAPIs(api_keys, settings, logger)
        
        assert ext_apis is not None
        assert ext_apis.api_keys == api_keys
        assert ext_apis.settings == settings
        
        print("✓ Modulo External APIs caricato")
        print(f"  - API calls counter: {ext_apis.api_calls}")
        print(f"  - Cache size: {len(ext_apis.metadata_cache)}")
        
        return True
    except Exception as e:
        print(f"✗ Errore: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_bpm_services_module():
    """Test modulo BPM Services"""
    print("\nTest 6: Modulo BPM Services...")
    try:
        from services.bpm_services import BPMServices, LIBROSA_AVAILABLE
        from config.secrets import api_keys
        from config.settings import settings
        import logging
        
        logger = logging.getLogger('test')
        
        # Inizializza BPM Services
        bpm_services = BPMServices(api_keys, settings, logger)
        
        assert bpm_services is not None
        assert bpm_services.api_keys == api_keys
        assert bpm_services.settings == settings
        
        # Test validazione BPM
        assert bpm_services._validate_bpm(120) == True
        assert bpm_services._validate_bpm(30) == False
        assert bpm_services._validate_bpm(250) == False
        
        # Stats
        stats = bpm_services.get_cache_stats()
        
        print("✓ Modulo BPM Services caricato")
        print(f"  - Cache size: {stats['cache_size']}")
        print(f"  - API calls: {stats['api_calls']}")
        print(f"  - Librosa disponibile: {stats['librosa_available']}")
        print(f"  - BPM range valido: {settings.bpm.valid_range_min}-{settings.bpm.valid_range_max}")
        
        return True
    except Exception as e:
        print(f"✗ Errore: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_metadata_extractor_module():
    """Test modulo Metadata Extractor"""
    print("\nTest 7: Modulo Metadata Extractor...")
    try:
        from core.metadata_extractor import MetadataExtractor
        from config.settings import settings
        import logging
        
        logger = logging.getLogger('test')
        
        # Inizializza extractor
        extractor = MetadataExtractor(settings, logger)
        
        assert extractor is not None
        assert extractor.settings == settings
        
        # Test stats
        stats = extractor.get_stats()
        
        print("✓ Modulo Metadata Extractor caricato")
        print(f"  - eyed3 disponibile: {stats['eyed3_available']}")
        print(f"  - mutagen disponibile: {stats['mutagen_available']}")
        print(f"  - Può estrarre: {stats['can_extract']}")
        print(f"  - Può aggiornare: {stats['can_update']}")
        
        # Test validazione BPM
        test_metadata = {'bpm': '125', 'year': '2020', 'track_num': '3/12'}
        from pathlib import Path
        validated = extractor.validate_metadata(test_metadata, Path('test.mp3'))
        
        assert validated['bpm'] == '125'
        assert validated['year'] == '2020'
        assert validated['track_num'] == '3'
        
        print("  - Validazione metadati: OK")
        
        return True
    except Exception as e:
        print(f"✗ Errore: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_genre_classifier_module():
    """Test modulo Genre Classifier"""
    print("\nTest 8: Modulo Genre Classifier...")
    try:
        from core.genre_classifier import GenreClassifier
        from config.settings import settings
        import logging
        from pathlib import Path
        
        logger = logging.getLogger('test')
        
        # Inizializza classifier
        classifier = GenreClassifier(settings, logger)
        
        assert classifier is not None
        assert classifier.settings == settings
        
        # Test normalizzazione
        assert classifier.normalize_genre('rock') == 'Rock'
        assert classifier.normalize_genre('salsa') == 'Salsa'
        assert classifier.normalize_genre('bachata') == 'Bachata'
        assert classifier.normalize_genre('progressive rock') == 'Rock'
        assert classifier.normalize_genre('unknown_genre') == 'Other'
        
        print("✓ Normalizzazione generi: OK")
        
        # Test riconoscimento latin subgenre
        metadata_bachata = {'bpm': '120'}
        result = classifier.detect_latin_subgenre(
            'Romeo Santos', 
            'Propuesta Indecente',
            'romeo_santos_propuesta',
            metadata_bachata
        )
        print(f"  - Riconoscimento Bachata: {result}")
        
        # Test stats
        stats = classifier.get_cache_stats()
        print(f"  - Generi mappati: {stats['genres_mapped']}")
        print(f"  - Sottogeneri latini: {stats['latin_subgenres']}")
        print(f"  - Cache size: {stats['cache_size']}")
        
        # Test path cartelle
        path_salsa = classifier.get_genre_folder_path('Salsa', 'salsa')
        assert str(path_salsa) == 'Latin\\Salsa' or str(path_salsa) == 'Latin/Salsa'
        print("  - Path cartelle Latin: OK")
        
        return True
    except Exception as e:
        print(f"✗ Errore: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_file_manager_module():
    """Test modulo File Manager"""
    print("\nTest 9: Modulo File Manager...")
    try:
        from core.file_manager import FileManager
        from config.settings import settings
        import logging
        from pathlib import Path
        
        logger = logging.getLogger('test')
        
        # Inizializza file manager in dry-run mode
        file_mgr = FileManager(Path('.'), settings, dry_run=True, logger=logger)
        
        assert file_mgr is not None
        assert file_mgr.dry_run == True
        
        # Test pulizia nomi
        assert file_mgr.clean_filename('Artist: "Test"') == 'Artist Test'
        assert file_mgr.clean_filename('Song/Title\\Name') == 'SongTitleName'
        assert file_mgr.clean_filename('  Multiple   Spaces  ') == 'Multiple Spaces'
        
        print("✓ Pulizia nomi file: OK")
        
        # Test scansione (nella directory test)
        mp3_files = file_mgr.scan_mp3_files(recursive=False)
        print(f"  - File MP3 trovati: {len(mp3_files)}")
        
        # Test stats
        stats = file_mgr.get_stats()
        print(f"  - Moved files: {stats['moved_files']}")
        print(f"  - Folders created: {stats['folders_created']}")
        print(f"  - Dry run: {stats['dry_run']}")
        
        return True
    except Exception as e:
        print(f"✗ Errore: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 50)
    print("TEST CONFIGURAZIONI MUSIC CATALOGER")
    print("=" * 50)
    
    results = []
    results.append(test_imports())
    results.append(test_api_keys())
    results.append(test_main_class())
    results.append(test_settings())
    results.append(test_external_apis_module())
    results.append(test_bpm_services_module())
    results.append(test_metadata_extractor_module())
    results.append(test_genre_classifier_module())
    results.append(test_file_manager_module())
    
    print("\n" + "=" * 50)
    if all(results):
        print("✓ TUTTI I TEST SUPERATI")
    else:
        print("✗ ALCUNI TEST FALLITI")
    print("=" * 50)
