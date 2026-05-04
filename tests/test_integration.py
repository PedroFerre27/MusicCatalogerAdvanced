"""
Test di integrazione completo per Music Cataloger Advanced
Verifica che tutti i moduli funzionino insieme
"""

import sys
import logging
from pathlib import Path
import tempfile
import shutil

# Setup path
sys.path.insert(0, str(Path(__file__).parent))

def setup_test_environment():
    """Crea ambiente di test temporaneo"""
    temp_dir = Path(tempfile.mkdtemp(prefix='music_cataloger_test_'))
    
    # Crea file MP3 di test fittizi (vuoti)
    test_files = [
        'Romeo Santos - Propuesta Indecente.mp3',
        'Hector Lavoe - El Cantante.mp3',
        'The Beatles - Hey Jude.mp3',
        'Unknown Artist - Unknown Song.mp3'
    ]
    
    for filename in test_files:
        (temp_dir / filename).touch()
    
    return temp_dir

def test_full_workflow():
    """Test workflow completo"""
    print("=" * 60)
    print("TEST DI INTEGRAZIONE COMPLETO")
    print("=" * 60)
    
    # Crea ambiente temporaneo
    print("\n1. Setup ambiente di test...")
    temp_dir = setup_test_environment()
    print(f"   Creato directory temporanea: {temp_dir}")
    
    try:
        # Import tutti i moduli
        print("\n2. Import moduli...")
        from config.secrets import api_keys
        from config.settings import settings
        from services.external_apis import ExternalAPIs
        from services.bpm_services import BPMServices
        from core.metadata_extractor import MetadataExtractor
        from core.genre_classifier import GenreClassifier
        from core.file_manager import FileManager
        from MusicCatalogerAdvanced_v0020 import MusicCatalogerAdvanced
        
        print("   ✓ Tutti i moduli importati correttamente")
        
        # Verifica configurazioni
        print("\n3. Verifica configurazioni...")
        api_validation = api_keys.validate_keys()
        print(f"   API Keys disponibili: {sum(api_validation.values())}/{len(api_validation)}")
        print(f"   Generi mappati: {len(settings.genre.genre_mapping)}")
        
        # Inizializza cataloger in dry-run mode
        print("\n4. Inizializzazione cataloger (dry-run)...")
        cataloger = MusicCatalogerAdvanced(
            base_path=str(temp_dir),
            log_level=logging.WARNING,  # Riduce output
            dry_run=True,
            use_external_db=False  # Disabilita per test veloce
        )
        
        print("   ✓ Cataloger inizializzato")
        
        # Verifica che tutti i moduli siano caricati
        print("\n5. Verifica moduli caricati...")
        assert cataloger.external_apis is not None, "ExternalAPIs non caricato"
        assert cataloger.bpm_services is not None, "BPMServices non caricato"
        assert cataloger.metadata_extractor is not None, "MetadataExtractor non caricato"
        assert cataloger.genre_classifier is not None, "GenreClassifier non caricato"
        assert cataloger.file_manager is not None, "FileManager non caricato"
        print("   ✓ Tutti i moduli core caricati")
        
        # Test scansione file
        print("\n6. Test scansione file...")
        cataloger.scan_and_catalog()
        print(f"   ✓ Processati {cataloger.processed_files} file")
        
        # Test analisi collezione
        print("\n7. Test analisi collezione...")
        stats = cataloger.analyze_collection()
        print(f"   ✓ Trovati {len(stats)} generi/cartelle")
        
        # Test statistiche moduli
        print("\n8. Statistiche moduli:")
        
        if cataloger.external_apis:
            print(f"   - API calls: {cataloger.external_apis.api_calls}")
        
        if cataloger.bpm_services:
            bpm_stats = cataloger.bpm_services.get_cache_stats()
            print(f"   - BPM cache: {bpm_stats['cache_size']}")
            print(f"   - Librosa: {bpm_stats['librosa_available']}")
        
        if cataloger.metadata_extractor:
            meta_stats = cataloger.metadata_extractor.get_stats()
            print(f"   - eyed3: {meta_stats['eyed3_available']}")
            print(f"   - mutagen: {meta_stats['mutagen_available']}")
        
        if cataloger.genre_classifier:
            genre_stats = cataloger.genre_classifier.get_cache_stats()
            print(f"   - Generi cache: {genre_stats['cache_size']}")
        
        if cataloger.file_manager:
            file_stats = cataloger.file_manager.get_stats()
            print(f"   - File spostati: {file_stats['moved_files']}")
        
        print("\n" + "=" * 60)
        print("✓ TEST DI INTEGRAZIONE COMPLETATO CON SUCCESSO")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n✗ ERRORE NEL TEST: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        print(f"\n9. Cleanup ambiente di test...")
        try:
            shutil.rmtree(temp_dir)
            print("   ✓ Directory temporanea rimossa")
        except Exception as e:
            print(f"   Avviso: Impossibile rimuovere {temp_dir}: {e}")

def test_individual_modules():
    """Test rapidi moduli individuali"""
    print("\n" + "=" * 60)
    print("TEST MODULI INDIVIDUALI")
    print("=" * 60)
    
    results = []
    
    # Test 1: Secrets
    print("\nTest 1: Config Secrets...")
    try:
        from config.secrets import api_keys
        validation = api_keys.validate_keys()
        working = sum(validation.values())
        print(f"   ✓ {working}/{len(validation)} API keys configurate")
        results.append(True)
    except Exception as e:
        print(f"   ✗ Errore: {e}")
        results.append(False)
    
    # Test 2: Settings
    print("\nTest 2: Config Settings...")
    try:
        from config.settings import settings
        assert len(settings.genre.genre_mapping) > 0
        assert len(settings.bpm.difficulty_ranges) == 5
        print(f"   ✓ Settings caricati correttamente")
        results.append(True)
    except Exception as e:
        print(f"   ✗ Errore: {e}")
        results.append(False)
    
    # Test 3: External APIs
    print("\nTest 3: External APIs...")
    try:
        from services.external_apis import ExternalAPIs
        from config.secrets import api_keys
        from config.settings import settings
        import logging
        
        api = ExternalAPIs(api_keys, settings, logging.getLogger('test'))
        assert api.api_calls == 0
        print(f"   ✓ External APIs inizializzato")
        results.append(True)
    except Exception as e:
        print(f"   ✗ Errore: {e}")
        results.append(False)
    
    # Test 4: BPM Services
    print("\nTest 4: BPM Services...")
    try:
        from services.bpm_services import BPMServices
        from config.secrets import api_keys
        from config.settings import settings
        import logging
        
        bpm = BPMServices(api_keys, settings, logging.getLogger('test'))
        assert bpm._validate_bpm(120) == True
        assert bpm._validate_bpm(30) == False
        print(f"   ✓ BPM Services inizializzato")
        results.append(True)
    except Exception as e:
        print(f"   ✗ Errore: {e}")
        results.append(False)
    
    # Test 5: Metadata Extractor
    print("\nTest 5: Metadata Extractor...")
    try:
        from core.metadata_extractor import MetadataExtractor
        from config.settings import settings
        import logging
        
        extractor = MetadataExtractor(settings, logging.getLogger('test'))
        stats = extractor.get_stats()
        assert stats['can_extract'] or stats['can_update']
        print(f"   ✓ Metadata Extractor inizializzato")
        results.append(True)
    except Exception as e:
        print(f"   ✗ Errore: {e}")
        results.append(False)
    
    # Test 6: Genre Classifier
    print("\nTest 6: Genre Classifier...")
    try:
        from core.genre_classifier import GenreClassifier
        from config.settings import settings
        import logging
        
        classifier = GenreClassifier(settings, logging.getLogger('test'))
        assert classifier.normalize_genre('rock') == 'Rock'
        assert classifier.normalize_genre('salsa') == 'Salsa'
        print(f"   ✓ Genre Classifier inizializzato")
        results.append(True)
    except Exception as e:
        print(f"   ✗ Errore: {e}")
        results.append(False)
    
    # Test 7: File Manager
    print("\nTest 7: File Manager...")
    try:
        from core.file_manager import FileManager
        from config.settings import settings
        import logging
        from pathlib import Path
        
        fm = FileManager(Path('.'), settings, True, logging.getLogger('test'))
        assert fm.clean_filename('Test: "File"') == 'Test File'
        print(f"   ✓ File Manager inizializzato")
        results.append(True)
    except Exception as e:
        print(f"   ✗ Errore: {e}")
        results.append(False)
    
    # Riepilogo
    print("\n" + "=" * 60)
    passed = sum(results)
    total = len(results)
    if passed == total:
        print(f"✓ TUTTI I {total} TEST MODULI SUPERATI")
    else:
        print(f"✗ {passed}/{total} test superati, {total-passed} falliti")
    print("=" * 60)
    
    return passed == total

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("MUSIC CATALOGER ADVANCED - TEST SUITE COMPLETA")
    print("=" * 60)
    
    # Test moduli individuali
    modules_ok = test_individual_modules()
    
    # Test integrazione
    if modules_ok:
        integration_ok = test_full_workflow()
    else:
        print("\n⚠ Salto test integrazione a causa di errori nei moduli")
        integration_ok = False
    
    # Risultato finale
    print("\n" + "=" * 60)
    if modules_ok and integration_ok:
        print("✓✓✓ TUTTI I TEST SUPERATI ✓✓✓")
        print("Il sistema è pronto per l'uso!")
    else:
        print("✗✗✗ ALCUNI TEST FALLITI ✗✗✗")
        print("Controlla gli errori sopra")
    print("=" * 60)
    
    sys.exit(0 if (modules_ok and integration_ok) else 1)