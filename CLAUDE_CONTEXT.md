# Music Cataloger Advanced — Context per Claude

## Struttura progetto
```
Music Cataloger/                          ← root progetto
├── run_cataloger.py                      ← ENTRY POINT CLI (usa core/)
├── run_gui.py                            ← launcher GUI
├── run_gui.bat                           ← launcher Windows
├── MusicCatalogerAdvanced_v0020.py       ← monolite legacy (NON usato dalla GUI)
├── config/
│   ├── secrets.py                        ← API keys (LASTFM, SPOTIFY, MUSICBRAINZ, GETSONG)
│   └── settings.py                       ← configurazione generi, BPM, bachata
├── core/
│   ├── cataloger.py                      ← logica principale ✅ MODIFICATO
│   ├── file_manager.py
│   ├── genre_classifier.py
│   └── metadata_extractor.py
├── gui/
│   ├── main_window.py                    ← GUI CustomTkinter ✅ MODIFICATO
│   ├── styles.py                         ← stili (non in uso attivo, PALETTE è in main_window.py)
│   ├── widgets.py
│   └── dialogs.py
├── services/
│   ├── cover_service.py                  ✅ MODIFICATO
│   ├── bpm_services.py
│   ├── external_apis.py                  ✅ MODIFICATO
│   └── cache_manager.py
├── utils/
│   ├── logging_config.py                 ✅ MODIFICATO (frammento: solo metodi da incollare)
│   └── helpers.py
├── output/                               ← cartella creata automaticamente
│   ├── *.log                             ← file di log
│   └── cataloging_report_*.json          ← report JSON
└── metadata_cache.json                   ← cache metadati
```

## Percorsi su Windows
- Progetto: `C:\Users\pmarquesf\OneDrive - Indra\Desktop\Pedro\Music Cataloger\`
- Musica: `C:\Users\pmarquesf\OneDrive - Indra\Desktop\Pedro\Musica\`
- Python: 3.13 (path standard)

## Come funziona il flusso
1. Utente apre la GUI via `run_gui.py` o `run_gui.bat`
2. GUI (`gui/main_window.py`) costruisce il comando con `_build_command()`
3. Il comando lancia `run_cataloger.py` via `subprocess.Popen`
4. `run_cataloger.py` istanzia `core/cataloger.py` con i parametri CLI
5. `core/cataloger.py` usa `services/external_apis.py`, `services/bpm_services.py`, `services/cover_service.py`
6. L'output stdout viene letto dalla GUI in tempo reale e parsato da `_parse_stats()`

## Classificazione musica
### Salsa — 5 livelli BPM + speciali
- **1 - Romantica**: ≤80 BPM
- **2 - Lenta**: 81-95 BPM
- **3 - Media**: 96-100 BPM
- **4 - Veloce**: 101-119 BPM
- **5 - Crazy**: ≥120 BPM
- Boogaloo, Cha-cha-cha (separati)

### Bachata — 4 sottotipi
- Dominicana, Fusion, Influence, Sensual

## Struttura cartelle musica
```
Musica/                    ← base_path (dir principale)
├── *.mp3                  ← file da catalogare
├── Latin/
│   ├── Salsa/
│   │   ├── 1 - Romantica/
│   │   ├── 2 - Lenta/
│   │   ├── 3 - Media/
│   │   ├── 4 - Veloce/
│   │   └── 5 - Crazy/
│   ├── Bachata/
│   ├── Reggaeton/
│   └── ...
├── Pop/
├── Rock/
├── Electronic/
├── Soundtrack/
└── ...
```

## Problema noto: musica latina e DB esterni
La maggior parte della musica salsa/bachata NON è taggata come "Salsa"/"Bachata" nei DB online
(Last.fm, MusicBrainz, Spotify) ma come "Latin Music" o "Latina".
**Il sistema prioritizza il parsing del filename** rispetto ai DB esterni per la classificazione latina.

## Protocollo PROGRESS per la GUI
Il cataloger emette righe speciali che la GUI intercetta:
```
PROGRESS: X/Y    ← aggiorna progress bar
```
Le 3 fasi:
1. `scan_and_catalog()` → "Trovati X file MP3 da elaborare" → PROGRESS 1..X/X
2. `correct_existing_folders()` → "Trovati X file MP3 da verificare" → PROGRESS 1..X/X  
3. `classify_salsa_by_bpm()` → "CLASSIFICAZIONE SALSA" → PROGRESS 1..X/X

## Log strutturato (alberatura)
```
*** filename.mp3 ***          ← inizio file (blu in GUI)
├── Last.fm: Salsa | BPM: 95  ← step intermedi (azzurro grigio)
├── Cover: scaricata da Spotify
└── filename.mp3 -> Latin/Salsa/  ← risultato (verde)
```
- File .log: UTF-8, caratteri ├── └── originali
- Console/pipe: ├── → |--, └── → \-- (SafeFormatter)

## Palette GUI (PALETTE dict in main_window.py)
Tema "steel blue" desaturato, bassa luminosità:
```python
"bg":        "#0A1520"   # quasi nero-blu
"surface":   "#0F1E2E"   # pannelli
"primary":   "#3A6EA8"   # blu acciaio (NON elettrico)
"text":      "#B8CCDF"   # bianco-blu soft
"text_dim":  "#5A7A95"   # label secondarie
```

## Metodi ExternalAPIs (services/external_apis.py)
- `search_all(artist, title, album)` → wrapper cascata Spotify→MusicBrainz→Last.fm
- `get_spotify_metadata(artist, title)`
- `search_musicbrainz(artist, title, album)`
- `search_lastfm(artist, title)`

## Output
Tutto va in `output/` (creata automaticamente):
- Log: `output/MusicCatalogerAdvanced_YYYYMMDD_HHMMSS.log`
- Report: `output/cataloging_report_YYYYMMDD_HHMMSS.json`

## Versione attuale: v0.0.2.2
## Stack: Python 3.13, CustomTkinter, eyed3, mutagen, librosa, musicbrainzngs, requests
