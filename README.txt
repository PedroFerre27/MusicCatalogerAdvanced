# MP3 Cataloger - Catalogatore MP3 per Genere

Un programma Python per catalogare automaticamente i file MP3 per genere, spostandoli in cartelle dedicate e aggiornando i metadati mancanti.

## Funzionalità

- **Catalogazione automatica**: Organizza i file MP3 in cartelle per genere
- **Aggiornamento metadati**: Completa automaticamente titolo, artista, genere, anno e BPM
- **Normalizzazione generi**: Standardizza i nomi dei generi musicali
- **Gestione duplicati**: Evita sovrascritture rinominando i file
- **Logging dettagliato**: Registra tutte le operazioni con livelli INFO, WARNING ed ERROR
- **Report finale**: Genera un report JSON con statistiche e file non catalogati

## Installazione

### Prerequisiti
- Python 3.6 o superiore (spuntare l’opzione:✔️ "Add Python to PATH")
- pip (gestore pacchetti Python)

### Installazione delle dipendenze
pip install eyed3 mutagen musicbrainzngs requests Pillow
##opzionali
pip install librosa spotipy


## Utilizzo

### Metodo 1: Script batch (Windows)
1. Posiziona `mp3_cataloger.py` e `run_cataloger.bat` nella stessa cartella
2. Esegui `run_cataloger.bat`
3. Inserisci il percorso della cartella con i file MP3

### Metodo 2: Linea di comando
# Installa dipendenze
pip install -r requirements.txt

# Modalità simulazione (consigliata prima volta)
python mp3_cataloger.py /path/to/music --dry-run -v
es:python mp3_cataloger_v0014.py /Users/pmarquesf/Desktop/Pedro/Musica --dry-run -v

# Catalogazione normale con DB esterni
python mp3_cataloger.py /path/to/music

# Solo analisi collezione esistente
python mp3_cataloger.py /path/to/music --analyze-only

# Senza database esterni (più veloce)
python mp3_cataloger.py /path/to/music --no-external

# Con cleanup cartelle vuote
python mp3_cataloger.py /path/to/music --cleanup

## Come funziona

1. **Scansione**: Il programma cerca tutti i file .mp3 nella directory specificata
2. **Lettura metadati**: Estrae i metadati usando eyed3 (con mutagen come fallback)
3. **Deduzione intelligente**: Se i metadati mancano, cerca di dedurli dal nome del file
4. **Aggiornamento**: Completa i metadati mancanti nel file
5. **Normalizzazione genere**: Standardizza il nome del genere
6. **Creazione cartelle**: Crea la cartella del genere se non esiste
7. **Spostamento**: Sposta il file nella cartella appropriata

## Struttura delle cartelle risultante

```
Directory_Musica/
├── Rock/
│   ├── canzone1.mp3
│   └── canzone2.mp3
├── Pop/
│   ├── hit1.mp3
│   └── hit2.mp3
├── Jazz/
│   └── standard.mp3
├── file_senza_genere.mp3  (rimane nella directory principale)
├── mp3_cataloger_YYYYMMDD_HHMMSS.log
└── cataloging_report_YYYYMMDD_HHMMSS.json
```

## Gestione dei metadati

Il programma cerca di completare questi metadati se mancanti:

- **Titolo**: Dal nome del file o dai metadati esistenti
- **Artista**: Dal nome del file (formato "Artista - Titolo") o dai metadati
- **Genere**: Dai metadati esistenti (normalizzato)
- **Anno**: Dal nome del file (formato "Anno - Artista - Titolo") o dai metadati
- **BPM**: Solo dai metadati esistenti

### Pattern riconosciuti nei nomi dei file
- `Anno - Artista - Titolo.mp3`
- `Artista - Titolo.mp3`
- `Titolo.mp3`

## Generi supportati

Il programma normalizza automaticamente questi generi:
- Rock, Pop, Jazz, Classical
- Hip Hop, Electronic, Dance
- Country, Blues, R&B, Reggae
- Folk, Metal, Punk, Alternative
- Indie, Soundtrack, World Music
- Ambient, Experimental

## File di output

### Log file
- Nome: `mp3_cataloger_YYYYMMDD_HHMMSS.log`
- Contiene: Tutte le operazioni con timestamp e livelli di log

### Report JSON
- Nome: `cataloging_report_YYYYMMDD_HHMMSS.json`
- Contiene: Statistiche complete e lista dei file non catalogati

## Risoluzione problemi

### File non catalogati
I file rimangono nella directory principale se:
- Non hanno metadati di genere
- Il genere non può essere normalizzato
- Si verificano errori durante la lettura

### Errori comuni
- **"eyed3 non installato"**: Installa con `pip install eyed3`
- **"mutagen non installato"**: Installa con `pip install mutagen`
- **"Percorso non esiste"**: Verifica il percorso della directory

### Caratteri speciali
I nomi delle cartelle vengono automaticamente puliti da caratteri non validi (`<>:"/\\|?*`)

## Sicurezza

- Il programma non sovrascrive mai i file esistenti
- Crea backup automatici rinominando i duplicati
- La modalità `--dry-run` permette di testare senza modifiche

## Licenza

Questo programma è fornito "così com'è" senza garanzie. Usa a tuo rischio e pericolo.
Consigliato fare backup dei file prima dell'uso.