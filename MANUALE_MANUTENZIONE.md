# 🛠️ Manuale Strumenti di Manutenzione — Music Cataloger Advanced

---

## 📋 Esporta CSV

**Cosa fa:** Esporta l'intero database locale (`music_library.json`) in un file CSV separato da `;`, apribile con Excel o LibreOffice Calc.

**Colonne esportate:**
| Colonna | Descrizione |
|---------|-------------|
| File | Nome file MP3 |
| Titolo | Titolo del brano (dalla cache metadati) |
| Artista | Nome artista |
| Album | Album di appartenenza |
| Anno | Anno di pubblicazione |
| Genere | Genere assegnato (es. Salsa, Rock) |
| Sottogenere | Sottocategoria (`-` se uguale al genere) |
| BPM | Battiti per minuto |
| Qualità (kbps) | Bitrate del file MP3 |
| Catalogato il | Data di ultima catalogazione |

**Quando usarlo:** Per analisi esterne, backup dati, o condivisione della libreria con altri sistemi.

---

## 🔍 Trova Duplicati

**Cosa fa:** Scansiona il database locale e raggruppa i file con lo **stesso nome** presenti in cartelle diverse — tipico risultato di catalogazioni multiple o spostamenti manuali.

**Come funziona:**
1. Apre una finestra con tutti i gruppi di duplicati trovati
2. Per ogni gruppo mostra i percorsi completi di ogni copia
3. Ogni riga ha il pulsante **✓ Mantieni questo** — elimina fisicamente le altre copie e aggiorna il DB

⚠️ **Attenzione:** l'eliminazione è **permanente**. Verificare sempre quale copia mantenere prima di procedere.

---

## 🗑️ Svuota Cache

**Cosa fa:** Cancella il contenuto di `metadata_cache.json`, che contiene i metadati recuperati da MusicBrainz, Deezer, iTunes e altre fonti online.

**Effetti:**
- La prossima catalogazione interrogherà nuovamente tutte le API (più lenta)
- Le cover già incorporate nei file MP3 **non vengono** rimosse
- Le informazioni nel DB locale (`music_library.json`) **non vengono** cancellate

**Quando usarlo:** Se la cache contiene dati errati o datati, o per liberare spazio su disco.

---

## 📂 Apri Cartella Dati

**Cosa fa:** Apre in Esplora File la cartella `data/` del programma, che contiene:
- `music_library.json` — database locale con tutti i file catalogati
- `metadata_cache.json` — cache dei metadati online
- `genre_prefs.json` — preferenze generi attivi/disattivati
- `caribbean_settings.json` — impostazioni classificazione caraibica
- `quality_analysis.json` — ultima analisi qualità eseguita
- `user_plan.json` — piano utente attivo

**Quando usarlo:** Per backup manuale dei dati, ispezione diretta dei file JSON, o ripristino da backup.

---

## 🎵 Playlist M3U per Genere

**Cosa fa:** Scansiona la directory musicale selezionata e crea automaticamente **un file `.m3u` per ogni cartella genere** trovata (Rock, Latin, Salsa, ecc.).

**Output:** Le playlist vengono salvate nella cartella scelta dall'utente, con nome `NomeGenere.m3u`.

**Compatibilità:** I file M3U sono aperti da VLC, foobar2000, Winamp, e qualsiasi lettore multimediale moderno.

**Quando usarlo:** Per creare playlist da importare in lettori musicali o DJ software.

---

## ✂️ Rinomina Batch con Pattern

**Cosa fa:** Rinomina tutti i file MP3 in base a un pattern personalizzabile usando i metadati ID3.

**Variabili disponibili:**
| Variabile | Sostituita con |
|-----------|---------------|
| `{title}` | Titolo del brano |
| `{artist}` | Artista principale |
| `{album}` | Nome album |
| `{year}` | Anno di pubblicazione |
| `{bpm}` | BPM (se presente nel tag) |

**Esempi di pattern:**
- `{artist} - {title}` → `Hector Lavoe - El Cantante.mp3`
- `{year} - {artist} - {title}` → `1975 - Hector Lavoe - El Cantante.mp3`
- `{title}` → `El Cantante.mp3`

**Filtro cartella:** Per applicare solo a una sottocartella specifica (es. `Latin/Salsa`).

⚠️ **Attenzione:** Il programma non rinomina se il file di destinazione esiste già. Operazione irreversibile — fare backup prima di usare.

---

## 🔊 Normalizza Volume (ReplayGain)

**Cosa fa:** Analizza i file MP3 e applica il **tag ReplayGain** per normalizzare il volume percepito a -89 dBFS (standard ReplayGain 2.0), senza modificare l'audio digitale.

**Requisiti:** Richiede `mp3gain` installato nel sistema.
- Windows: https://mp3gain.sourceforge.net/
- Oppure: `winget install mp3gain`

**Come funziona:** Scrive il valore di guadagno nei tag ID3 del file — i lettori compatibili (VLC, foobar2000) usano questo valore per equalizzare il volume durante la riproduzione. **Non modifica i dati audio**, è completamente reversibile.

**Quando usarlo:** Per equalizzare brani con volumi molto diversi nella stessa sessione DJ o playlist.

---

## 🛡️ Verifica Integrità File MP3

**Cosa fa:** Legge l'header audio di ogni file MP3 con `mutagen` e segnala i file che:
- Non possono essere aperti
- Hanno un header audio corrotto o mancante
- Non sono file MP3 validi nonostante l'estensione

**Output:** Una finestra con la lista dei file problematici trovati (max 20 mostrati).

**Quando usarlo:** Dopo operazioni di copia/spostamento di massa, o per verificare integrità di file scaricati da fonti non affidabili.

---

*Versione documento: v1072d — Music Cataloger Advanced*
