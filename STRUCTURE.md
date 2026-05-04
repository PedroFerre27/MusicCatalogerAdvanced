# Music Cataloger Advanced - Struttura Moduli

## Architettura del Progetto
```
MusicCatalogerAdvanced/
├── config/                          # Configurazioni
│   ├── __init__.py
│   ├── secrets.py                   # API keys e credenziali
│   └── settings.py                  # Impostazioni generali
│
├── services/                        # Servizi esterni
│   ├── __init__.py
│   ├── external_apis.py            # MusicBrainz, Last.fm, Spotify
│   └── bpm_services.py             # Recupero BPM da varie fonti
│
├── core/                           # Logica core
│   ├── __init__.py
│   ├── metadata_extractor.py      # Estrazione metadati MP3
│   ├── genre_classifier.py        # Classificazione generi
│   └── file_manager.py            # Gestione file system
│
├── MusicCatalogerAdvanced_v0020.py # File principale (monolitico legacy)
├── test_config.py                  # Test configurazioni
├── test_integration.py             # Test integrazione completa
└── README.md                       # Documentazione utente
```

## Moduli

### Config (config/)

#### secrets.py
- **Classe**: `APIKeys`
- **Responsabilità**: Gestione centralizzata API keys
- **Features**:
  - Caricamento da variabili d'ambiente
  - Validazione disponibilità keys
  - Fallback a valori default
- **API Keys gestite**:
  - Spotify (Client ID/Secret)
  - GetSong BPM
  - Last.fm
  - AcoustID (opzionale)

#### settings.py
- **Classi**: `APISettings`, `BPMSettings`, `GenreSettings`, `CacheSettings`, etc.
- **Responsabilità**: Tutte le configurazioni non sensibili
- **Features**:
  - Mapping generi musicali (190+ mappature)
  - Range BPM validazione (60-200)
  - Classificazione difficoltà Salsa (5 livelli)
  - Indicatori riconoscimento Bachata/Salsa
  - Timeout e rate limiting API

### Services (services/)

#### external_apis.py
- **Classe**: `ExternalAPIs`
- **Responsabilità**: Interfaccia con database musicali esterni
- **API Supportate**:
  - **MusicBrainz**: Metadati completi, generi da tag
  - **Last.fm**: Metadati, generi, playcount
  - **Spotify**: Metadati, album art, popolarità
- **Features**:
  - Rate limiting automatico
  - Cache integrata
  - Selezione genere primario intelligente
  - Gestione SSL context

#### bpm_services.py
- **Classe**: `BPMServices`
- **Responsabilità**: Recupero BPM da multiple fonti
- **Fonti** (in ordine di priorità):
  1. Metadati esistenti
  2. GetSong API
  3. TuneBat (scraping)
  4. SongBPM.com (scraping)
  5. Beatport (scraping)
  6. Librosa (calcolo audio)
- **Features**:
  - Validazione range BPM
  - Cache risultati
  - Fallback automatico

### Core (core/)

#### metadata_extractor.py
- **Classe**: `MetadataExtractor`
- **Responsabilità**: Estrazione e gestione metadati MP3
- **Librerie**:
  - eyed3 (primario)
  - mutagen (fallback)
- **Funzioni**:
  - Estrazione metadati (titolo, artista, album, anno, BPM, genere)
  - Deduzione da nome file
  - Merge metadati con priorità
  - Validazione e pulizia dati
  - Aggiornamento file ID3

#### genre_classifier.py
- **Classe**: `GenreClassifier`
- **Responsabilità**: Classificazione e normalizzazione generi
- **Features**:
  - Normalizzazione 190+ generi
  - Riconoscimento automatico Salsa/Bachata
  - Analisi multi-fonte (artist, title, filename, BPM)
  - Logica priorità per determinazione genere
  - Gestione sottogeneri latini
  - Cache normalizzazioni

#### file_manager.py
- **Classe**: `FileManager`
- **Responsabilità**: Operazioni file system
- **Funzioni**:
  - Scansione directory MP3
  - Pulizia nomi file/cartelle
  - Spostamento file con gestione duplicati
  - Creazione struttura cartelle
  - Cleanup cartelle vuote
  - Analisi struttura collezione

## Flusso di Esecuzione

### 1. Inizializzazione
```
Main → Config (secrets + settings) → Services + Core modules
```

### 2. Processo Catalogazione
```
1. Scan MP3 files (FileManager)
2. Extract metadata (MetadataExtractor)
3. Get external metadata (ExternalAPIs)
4. Get BPM (BPMServices)
5. Determine genre (GenreClassifier)
6. Update file metadata (MetadataExtractor)
7. Move to genre folder (FileManager)
```

### 3. Determinazione Genere (Priorità)
```
1. Genere dal track (MusicBrainz/Last.fm)
2. Deduzione da filename
3. Riconoscimento Latin subgenre
4. Genere dall'artista (all_genres)
5. Metadati esistenti
6. "Unknown"
```

## Dipendenze tra Moduli
```
MusicCatalogerAdvanced (main)
    ↓
    ├─→ config.secrets (api_keys)
    ├─→ config.settings (settings)
    ↓
    ├─→ services.external_apis (api_keys, settings)
    ├─→ services.bpm_services (api_keys, settings)
    ├─→ core.metadata_extractor (settings)
    ├─→ core.genre_classifier (settings)
    └─→ core.file_manager (settings)
```

## Configurazione API Keys

### Opzione 1: Variabili d'Ambiente
```bash
# Windows
set SPOTIFY_CLIENT_ID=your_id
set SPOTIFY_CLIENT_SECRET=your_secret

# Linux/Mac
export SPOTIFY_CLIENT_ID=your_id
export SPOTIFY_CLIENT_SECRET=your_secret
```

### Opzione 2: Modifica Diretta
Modifica `config/secrets.py` con le tue credenziali.

## Estensibilità

### Aggiungere Nuovo Servizio BPM
1. Aggiungi metodo in `services/bpm_services.py`
2. Inserisci nella cascata `estimate_bpm()`

### Aggiungere Nuovo Genere
1. Modifica `config/settings.py` → `GenreSettings.genre_mapping`
2. Il sistema lo riconoscerà automaticamente

### Aggiungere Nuova API Metadata
1. Aggiungi metodo in `services/external_apis.py`
2. Chiama nel workflow di catalogazione

## Testing
```bash
# Test configurazioni
python test_config.py

# Test integrazione completa
python test_integration.py
```

## Note di Compatibilità

Il file `MusicCatalogerAdvanced_v0020.py` mantiene i metodi legacy come fallback se i moduli non sono disponibili. Questo garantisce:
- Compatibilità con vecchie installazioni
- Funzionamento anche con import parziali
- Possibilità di disabilitare moduli specifici