# Music Cataloger Advanced v0.0.2.0

Sistema avanzato e modulare per catalogare, organizzare e arricchire collezioni musicali MP3.

## 🎵 Caratteristiche Principali

- **Catalogazione Automatica**: Organizza MP3 per genere in sottocartelle
- **Metadati Intelligenti**: Recupera informazioni da MusicBrainz, Last.fm, Spotify
- **BPM Automatico**: Recupero da 5+ servizi o calcolo con analisi audio
- **Gestione Latina**: Classificazione speciale Salsa/Bachata con sottogeneri
- **Classificazione Difficoltà**: Organizza Salsa per livello BPM (5 livelli)
- **Architettura Modulare**: Design pulito, estensibile, manutenibile
- **Cache Intelligente**: Velocizza ricerche successive

## 🏗️ Architettura
```
MusicCatalogerAdvanced/
├── config/              # Configurazioni (API keys, settings)
├── services/            # Servizi esterni (APIs, BPM)
├── core/                # Logica core (metadata, generi, file)
├── gui/                 # Interfaccia grafica (opzionale)
└── tests/               # Test suite completa
```

Vedi [STRUCTURE.md](STRUCTURE.md) per dettagli architettura.

## 📦 Installazione

### Requisiti
- Python 3.7+
- pip

### Dipendenze Base
```bash
pip install eyed3 mutagen musicbrainzngs requests
```

### Dipendenze Opzionali
```bash
# Per calcolo BPM audio
pip install librosa

# Per scraping web avanzato
pip install beautifulsoup4

# Per fingerprinting audio
pip install pyacoustid
```

### Setup Rapido
```bash
# 1. Clona/scarica il progetto
cd MusicCatalogerAdvanced

# 2. Installa dipendenze
pip install -r requirements.txt

# 3. Configura API keys (opzionale)
# Modifica config/secrets.py con le tue chiavi

# 4. Test installazione
python test_integration.py
```

## 🚀 Utilizzo

### Modalità CLI

#### Catalogazione Base
```bash
# Simulazione (consigliato per prima volta)
python MusicCatalogerAdvanced_v0020.py /path/to/music --dry-run -v

# Catalogazione reale
python MusicCatalogerAdvanced_v0020.py /path/to/music

# Senza database esterni (più veloce)
python MusicCatalogerAdvanced_v0020.py /path/to/music --no-external
```

#### Funzioni Avanzate
```bash
# Correzione cartelle Salsa/Bachata esistenti
python MusicCatalogerAdvanced_v0020.py /path/to/music --correct-folders

# Classificazione Salsa per difficoltà
python MusicCatalogerAdvanced_v0020.py /path/to/music --classify-salsa

# Combinazione funzioni
python MusicCatalogerAdvanced_v0020.py /path/to/music --correct-folders --classify-salsa --cleanup
```

#### Opzioni Disponibili
```
Argomenti posizionali:
  path                  Directory con file MP3

Opzioni:
  -h, --help           Mostra questo aiuto
  -v, --verbose        Output debug dettagliato
  --dry-run            Simulazione senza modifiche
  --no-external        Disabilita database esterni
  --analyze-only       Solo analisi collezione
  --correct-folders    Correggi metadati cartelle esistenti
  --classify-salsa     Classifica Salsa per difficoltà
  --cleanup            Rimuovi cartelle vuote
```

### Modalità GUI (Coming Soon)
```bash
python run_gui.py
```

## 📁 Struttura Output
```
Directory_Musica/
├── Rock/
│   ├── song1.mp3
│   └── song2.mp3
├── Pop/
│   └── hit.mp3
├── Latin/
│   ├── Salsa/
│   │   ├── 1 - Romantica/  # <80 BPM
│   │   ├── 2 - Lenta/      # 80-94 BPM
│   │   ├── 3 - Media/      # 95-99 BPM
│   │   ├── 4 - Veloce/     # 100-119 BPM
│   │   └── 5 - Crazy/      # 120+ BPM
│   ├── Bachata/
│   │   └── song.mp3
│   └── Merengue/
│       └── song.mp3
└── Unknown/                     # Genere non identificato
```

## 🔑 Configurazione API Keys

### Spotify (Consigliato)
1. Vai su [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
2. Crea nuova applicazione
3. Copia Client ID e Client Secret
4. Modifica `config/secrets.py`:
```python
self.SPOTIFY_CLIENT_ID = "tuo_client_id"
self.SPOTIFY_CLIENT_SECRET = "tuo_client_secret"
```

### Variabili d'Ambiente (Alternativa)
```bash
# Windows
set SPOTIFY_CLIENT_ID=tuo_id
set SPOTIFY_CLIENT_SECRET=tuo_secret

# Linux/Mac
export SPOTIFY_CLIENT_ID=tuo_id
export SPOTIFY_CLIENT_SECRET=tuo_secret
```

## 🎯 Workflow Tipico

### Nuova Collezione
```bash
# 1. Test iniziale
python MusicCatalogerAdvanced_v0020.py /path/music --dry-run -v

# 2. Catalogazione
python MusicCatalogerAdvanced_v0020.py /path/music

# 3. Classificazione Salsa
python MusicCatalogerAdvanced_v0020.py /path/music --classify-salsa
```

### Collezione Esistente
```bash
# Correzione e manutenzione
python MusicCatalogerAdvanced_v0020.py /path/music --correct-folders --cleanup
```

### Solo Analisi
```bash
# Verifica senza modifiche
python MusicCatalogerAdvanced_v0020.py /path/music --analyze-only
```

## 🧪 Testing
```bash
# Test configurazioni
python test_config.py

# Test integrazione completa
python test_integration.py
```

## 📊 Output e Report

### File Generati
- `MusicCatalogerAdvanced_YYYYMMDD_HHMMSS.log` - Log dettagliato
- `cataloging_report_YYYYMMDD_HHMMSS.json` - Report statistiche
- `metadata_cache.json` - Cache metadati per velocizzare ricerche

### Esempio Report JSON
```json
{
  "timestamp": "2025-11-18T16:00:00",
  "statistics": {
    "total_processed": 150,
    "successfully_moved": 142,
    "metadata_updated": 138,
    "api_calls_made": 89
  },
  "genre_distribution": {
    "Latin/Salsa": 45,
    "Latin/Bachata": 23,
    "Rock": 18
  }
}
```

## 🎼 Generi Supportati

### Mappatura Automatica (78 generi)
- **Rock** e derivati (alternative, indie, classic, hard, punk, progressive)
- **Pop** e derivati (pop rock, indie pop, electropop, synthpop)
- **Electronic** (techno, house, trance, EDM, dubstep, drum & bass)
- **Hip Hop** (rap, trap)
- **R&B & Soul** (neo soul)
- **Jazz** (smooth, fusion, bebop)
- **Latin** con sottogeneri:
  - Salsa (con classificazione difficoltà)
  - Bachata
  - Merengue, Cumbia, Reggaeton, Tropical
- **Classical**, **Reggae**, **Country**, **Folk**, **Metal**, **Blues**
- E molti altri...

### Classificazione Salsa

| Livello | Range BPM | Descrizione |
|---------|-----------|-------------|
| 🌹 Romantica | <80 | Ballate lente |
| 👶 Lenta | 80-94 | Ideale per imparare |
| 🎯 Media | 95-99 | Consolidamento tecnica |
| ⚡ Veloce | 100-119 | Richiede esperienza |
| 🔥 Crazy | 120+ | Solo esperti |

## 🔧 Configurazioni Avanzate

### Modifica Generi
Modifica `config/settings.py` → `GenreSettings.genre_mapping`

### Modifica Range BPM
Modifica `config/settings.py` → `BPMSettings`

### Timeout API
Modifica `config/settings.py` → `APISettings.timeout`

## 🐛 Troubleshooting

### File Non Catalogati
I file rimangono nella directory principale se:
- Genere "Unknown" (non identificabile)
- Errori lettura metadati
- Problemi permessi file

### Errori Comuni

**"Module not found"**
```bash
pip install <modulo_mancante>
```

**"API 403 Forbidden"**
- Verifica API keys in `config/secrets.py`
- Controlla limiti quota API

**"SSL Certificate Error"**
- Il programma gestisce automaticamente con fallback

### Debug
```bash
# Output dettagliato
python MusicCatalogerAdvanced_v0020.py /path/music -v

# Solo simulazione per vedere cosa farebbe
python MusicCatalogerAdvanced_v0020.py /path/music --dry-run -v
```

## 📝 Changelog

### v0.0.1.9 (Novembre 2025)
- ✨ Architettura completamente modularizzata
- ✨ Separazione config/services/core
- ✨ Sistema cache migliorato
- ✨ Test suite completa
- ✨ Documentazione estesa
- 🐛 Fix deprecation warnings
- 🐛 Fix gestione SSL MusicBrainz
- 🐛 Fix duplicazione file processati

### v0.0.1.7-0.0.1.8
- ✨ BPM da multiple fonti (GetSong, TuneBat, etc.)
- ✨ Integrazione Spotify
- ✨ Miglioramento riconoscimento Latin
- ✨ Classificazione Salsa per difficoltà

## 🤝 Contributi

Questo è un progetto personale, ma suggerimenti e bug report sono benvenuti!

## ⚠️ Disclaimer

- **Backup**: Fai SEMPRE backup della collezione prima del primo utilizzo
- **Licenze**: Rispetta copyright e licenze musicali
- **API**: Rispetta termini di servizio delle API utilizzate
- **Garanzie**: Fornito "as-is" senza garanzie

## 📄 Licenza

Uso personale ed educativo.

## 🙏 Crediti

### API & Database
- [MusicBrainz](https://musicbrainz.org/) - Database musicale open
- [Last.fm](https://www.last.fm/api) - Metadati e scrobbling
- [Spotify](https://developer.spotify.com/) - Metadati e artwork
- [GetSong BPM](https://getsongbpm.com/) - Database BPM

### Librerie
- eyed3, mutagen - Metadati ID3
- librosa - Analisi audio
- musicbrainzngs - MusicBrainz API
- requests - HTTP client

---

**Versione**: v0.0.1.9  
**Data**: Novembre 2025  
**Python**: 3.7+  
**Compatibilità**: Windows, macOS, Linux