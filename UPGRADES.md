# 📋 UPGRADES — Music Cataloger Advanced
**Changelog completo dalla v1025 alla v1072d**

> Questo documento raccoglie tutti i note di aggiornamento del progetto.
> Ogni sezione corrisponde a una versione rilasciata, in ordine cronologico.

---

## Music Cataloger Advanced — Upgrade Notes v1025
**Data:** 2026-03-23  
**Versione precedente:** v0.0.2.2  
**Versione corrente:** v1025  

---

## 🐛 BUG FIX

### BUG-01 · Conteggio doppio nella progress bar (Fase 1)
**File:** `core/cataloger.py`  
**Problema:** `scan_and_catalog()` contava sia i file MP3 nella root **che** quelli già nelle sottocartelle, raddoppiando il totale. La progress bar arrivava al 50% anziché al 100%.  
**Fix:** Il conteggio iniziale deve includere **solo** i file MP3 direttamente nella `base_path` (non ricorsivi). I file già classificati nelle sottocartelle vengono gestiti separatamente da `correct_existing_folders()`.

```python
# PRIMA (errato):
all_mp3 = list(base_path.rglob("*.mp3"))   # ricorsivo → doppio conteggio

# DOPO (corretto):
root_mp3 = [f for f in base_path.glob("*.mp3") if f.parent == base_path]
```

---

### BUG-02 · Genere "Salsa" non mappato a `Latin\Salsa`
**File:** `core/genre_classifier.py` e/o `core/cataloger.py`  
**Problema:** Quando MusicBrainz/Last.fm restituisce il tag `"salsa"` (lowercase o maiuscolo), il file veniva spostato in una cartella `Salsa/` nella root invece di `Latin/Salsa/`.  
**Fix:** Aggiungere/verificare nel mapper dei generi:

```python
GENRE_MAP = {
    ...
    "salsa":      ("Latin", "Salsa"),
    "salsa":      ("Latin", "Salsa"),   # case-insensitive
    ...
}
```
Controllare che la normalizzazione `.lower()` sia applicata **prima** del lookup nella mappa.

---

### BUG-03 · Conteggio "Non categorizzati" include i WARNING del log
**File:** `core/cataloger.py` (sezione statistiche finali)  
**Problema:** Il contatore `uncategorized` veniva incrementato anche quando il logger emetteva un WARNING (es. `Illegal Audio-MPEG-Header`, `Trying to resync...`), portando a un conteggio falso (8 nel report vs 5 file reali non spostati).  
**Fix:** Incrementare `uncategorized` **solo** quando un file non riceve alcuna destinazione valida, non su eccezioni/warning del parser audio.

```python
# PRIMA: any exception → uncategorized++
# DOPO: solo se dest_folder is None dopo tutti i tentativi
if dest_folder is None:
    stats["uncategorized"] += 1
    logger.warning(f"Non categorizzato: {filename}")
```

---

## ✨ NUOVE FEATURE

### FEAT-01 · Stima tempo di completamento
**File:** `core/cataloger.py`, `gui/main_window.py`  
**Descrizione:** Al termine della scansione iniziale (sappiamo quanti file ci sono), il sistema calcola una stima ETA basata su:
1. **Tempo medio per file** misurato sui primi N file elaborati (finestra mobile di 20)
2. **File rimanenti × tempo medio** = ETA in minuti/ore

Il cataloger emette una riga speciale intercettabile dalla GUI:
```
ETA: 42m30s   (aggiornata ogni 30 file)
```
La GUI mostra l'ETA nella status bar accanto alla progress bar.  

**Formula:**
```python
avg_time = sum(recent_times[-20:]) / len(recent_times[-20:])
eta_seconds = avg_time * files_remaining
```

---

### FEAT-02 · Generi orfani — avviso e suggerimento genere padre
**File:** `core/cataloger.py` (sezione report finale), `gui/main_window.py`  
**Descrizione:** Al termine della catalogazione, il sistema analizza le statistiche per genere. Se un genere ha **meno di 5 file** (soglia configurabile in `settings.py`), viene emesso un avviso interattivo.

Il sistema consulta la gerarchia MusicBrainz (da mapping locale embedded) per suggerire il genere padre:

```python
GENRE_HIERARCHY = {
    "world":       "Latin",
    "vocal":       "Pop",
    "alternative": "Rock",
    "indie":       "Rock",
    "blues rock":  "Rock",
    "folk rock":   "Rock",
    "j-rock":      "Rock",
    "j-pop":       "Pop",
    "k-pop":       "Pop",
    "cumbia":      "Latin",
    "merengue":    "Latin",
    # ... ecc.
}
```

Il cataloger emette:
```
ORPHAN_GENRE: World|1|Latin
ORPHAN_GENRE: Vocal|1|Pop
```
La GUI mostra un dialog al termine:
> *"Il genere **World** ha solo 1 file. Vuoi spostarlo in **Latin**?"*  
> [Sì] [No] [Sì a tutti]

**Soglia configurabile** in `config/settings.py`:
```python
ORPHAN_GENRE_THRESHOLD = 5  # file minimi per genere
```

---

### FEAT-03 · Soundtrack → riclassificazione via MusicBrainz
**File:** `core/genre_classifier.py`  
**Descrizione:** "Soundtrack" non è un genere riconosciuto in MusicBrainz (è un tag/uso commerciale). Quando le API restituiscono `"soundtrack"`, il sistema applica una logica di fallback:

1. Cerca il genere **secondario** restituito dall'API (es. `"classical"`, `"electronic"`)
2. Se trovato, usa quello
3. Se non trovato, controlla il **nome artista** contro una lista di compositori noti (Hans Zimmer, John Williams, Howard Shore, ecc.) → `Classical`
4. Solo come ultimo resort, usa una cartella `Soundtrack/` (mantenuta per retrocompatibilità)

```python
SOUNDTRACK_ARTIST_FALLBACK = {
    "hans zimmer": "Classical",
    "john williams": "Classical",
    "howard shore": "Classical",
    "james horner": "Classical",
    "two steps from hell": "Classical",
    "audiomachine": "Classical",
    # ...
}
```

---

### FEAT-04 · DB locale con sync manuale post-catalogazione
**File:** `services/local_db.py` *(nuovo file)*  
**Descrizione:** Dopo ogni catalogazione completa, il sistema salva un DB locale (`music_library.json`) con la mappatura `filepath → genere/cartella`.

**Struttura DB:**
```json
{
  "version": 1,
  "last_updated": "2026-03-23T10:00:00",
  "files": {
    "Latin/Salsa/Marc Anthony - Vivir Mi Vida.mp3": {
      "genre": "Salsa",
      "subgenre": null,
      "bpm": 103,
      "quality_kbps": 320,
      "cataloged_at": "2026-03-18T16:38:14"
    }
  }
}
```

**Opzione in GUI:** checkbox "Aggiorna DB locale" (default ON).

**Sync post-manuale:** Quando l'utente avvia una nuova catalogazione con l'opzione "Verifica modifiche manuali" attiva, il sistema:
1. Scansiona tutte le sottocartelle
2. Confronta posizione attuale vs DB locale
3. Per ogni file spostato manualmente → aggiorna il DB (la posizione manuale dell'utente ha priorità)
4. Emette un report delle differenze trovate

```
MANUAL_MOVE_DETECTED: "Salsa/brano.mp3" → "Latin/Salsa/3 - Media/brano.mp3"
DB aggiornato: 3 modifiche manuali rilevate
```

---

### FEAT-05 · Log separato per file a bassa qualità
**File:** `core/cataloger.py`, `utils/logging_config.py`  
**Descrizione:** Durante la catalogazione, per ogni file viene letta la bitrate con `mutagen`. Se `bitrate < 320 kbps`, il file viene registrato in un log dedicato.

**Output:** `output/low_quality_YYYYMMDD_HHMMSS.log`

**Formato:**
```
[LOW QUALITY] 128 kbps | Latin/Salsa/Marc Anthony - Aguanile.mp3
[LOW QUALITY] 192 kbps | Rock/Coldplay - Clocks.mp3
[LOW QUALITY] 256 kbps | Pop/Adele - Rolling In the Deep.mp3
```

**Soglia configurabile** in `config/settings.py`:
```python
LOW_QUALITY_THRESHOLD_KBPS = 320
```

**In GUI:** dopo la catalogazione, se il log esiste e ha contenuto, mostra un banner:
> *"⚠ 47 file a bassa qualità rilevati — Apri report"*

---

## 📁 FILE MODIFICATI / CREATI

| File | Tipo | Modifica |
|------|------|----------|
| `core/cataloger.py` | Modifica | BUG-01, BUG-03, FEAT-01, FEAT-04, FEAT-05 |
| `core/genre_classifier.py` | Modifica | BUG-02, FEAT-03 |
| `gui/main_window.py` | Modifica | FEAT-01 (ETA display), FEAT-02 (dialog orfani), FEAT-05 (banner qualità) |
| `services/local_db.py` | **Nuovo** | FEAT-04 (DB locale + sync) |
| `config/settings.py` | Modifica | FEAT-02 (soglia orfani), FEAT-05 (soglia qualità) |
| `utils/logging_config.py` | Modifica | FEAT-05 (nuovo handler low_quality) |

---

## ⚙️ CONFIGURAZIONE settings.py — nuovi parametri

```python
# --- Generi orfani ---
ORPHAN_GENRE_THRESHOLD = 5          # sotto questa soglia → avviso GUI

# --- Qualità audio ---
LOW_QUALITY_THRESHOLD_KBPS = 320    # sotto → log separato

# --- DB locale ---
LOCAL_DB_ENABLED = True             # abilita salvataggio DB
LOCAL_DB_SYNC_ON_START = False      # controlla modifiche manuali all'avvio
```

---

## 🔄 PROTOCOLLO PROGRESS — aggiornamenti

Nuovi token emessi su stdout (intercettati dalla GUI):

| Token | Formato | Descrizione |
|-------|---------|-------------|
| `ETA:` | `ETA: 42m30s` | Stima tempo rimanente |
| `ORPHAN_GENRE:` | `ORPHAN_GENRE: World\|1\|Latin` | Genere con pochi file + padre suggerito |
| `MANUAL_MOVE_DETECTED:` | `MANUAL_MOVE_DETECTED: src\|dst` | Differenza vs DB locale |
| `LOW_QUALITY_COUNT:` | `LOW_QUALITY_COUNT: 47` | Totale file bassa qualità |

---

## 📊 GERARCHIA GENERI (MusicBrainz-based)

Usata da FEAT-02 e FEAT-03:

```
Rock
  ├── Alternative Rock
  ├── Hard Rock / Heavy Metal
  ├── Folk Rock / Indie Rock
  ├── J-Rock / K-Rock
  └── ...

Pop
  ├── Dance-Pop / Electropop
  ├── J-Pop / K-Pop
  ├── Vocal (→ Pop)
  └── ...

Latin
  ├── Salsa (con sottocartelle BPM)
  ├── Bachata (Dominicana / Fusion / Sensual / Influence)
  ├── Reggaeton
  ├── Cumbia
  ├── Merengue
  └── World (→ Latin per musica folk/world latinoamericana)

Classical
  ├── Soundtrack (compositori noti → Classical)
  └── ...
```

---

## ⚠️ NOTE DI MIGRAZIONE

- Il file `music_library.json` viene creato automaticamente nella directory del progetto alla prima esecuzione con FEAT-04 attivo.
- I file già catalogati nelle sessioni precedenti **non** vengono retro-inseriti nel DB automaticamente; verrà popolato progressivamente.
- La logica "Soundtrack → Classical" è **opt-in** per default: il parametro `SOUNDTRACK_RECLASSIFY = True` in `settings.py` può essere disabilitato per mantenere il comportamento precedente.

---

## Music Cataloger Advanced — Upgrade Notes
**Data:** 2026-03-23
**Versione di partenza:** v0.0.2.2
**Versione finale:** v1029

---

## v1026 — BUG FIX: Progress bar ferma al 50%

**File modificato:** `core/cataloger.py` → metodo `scan_and_catalog()`

**Problema:**
`scan_and_catalog()` usava `self.base_path.glob("*.[mM][pP]3")` senza filtrare
per cartella padre. In Python il `glob` con wildcard senza `**` è già non-ricorsivo,
ma in alcune versioni del `FileManager` il metodo delegato usava `rglob` che includeva
anche i file già classificati nelle sottocartelle.
Risultato: il totale veniva raddoppiato → la progress bar arrivava al 50% e si bloccava.

**Fix:**
```python
# PRIMA:
mp3_files = [f for f in self.base_path.glob("*.[mM][pP]3") if f.is_file()]

# DOPO:
mp3_files = [
    f for f in self.base_path.glob("*.[mM][pP]3")
    if f.is_file() and f.parent == self.base_path   # ← filtro esplicito root-only
]
```

---

## v1027 — BUG FIX: Genere "Salsa" non mappato a Latin/Salsa

**File modificato:** `core/genre_classifier.py` → metodi `determine_genre()` e `normalize_genre()`

**Problema:**
Quando MusicBrainz/Last.fm restituiva il tag `"Salsa"` (con maiuscola iniziale o
in qualsiasi variante di capitalizzazione), il confronto con la lista
`['salsa', 'bachata', 'merengue', 'cumbia', 'reggaeton']` falliva perché il confronto
non era case-insensitive a monte.
Risultato: il file veniva classificato come `Salsa` (cartella root) invece di `Latin/Salsa`.

**Fix in `determine_genre()`:**
```python
# PRIMA:
if raw_genre.lower() in ['salsa', 'bachata', ...]:
    genre = raw_genre.capitalize()

# DOPO:
raw_lower = raw_genre.lower().strip()          # normalizza prima
if raw_lower in ['salsa', 'bachata', ...]:
    genre = raw_lower.capitalize()
    raw_genre = raw_lower                      # aggiorna anche raw_genre per get_genre_folder_path
```

**Fix in `normalize_genre()`:**
Aggiunto commento esplicativo e verifica che `.lower().strip()` venga applicato
prima di qualsiasi lookup nella `genre_mapping`. Le chiavi del dict sono già tutte
lowercase — ora è documentato e garantito.

---

## v1028 — BUG FIX: Contatore "Non catalogati" inflazionato dai WARNING audio

**File modificato:** `core/cataloger.py` → metodo `process_mp3_file()`

**Problema:**
Il blocco `except Exception` in `process_mp3_file()` aggiungeva il file a
`uncatalogued_files` per qualsiasi eccezione, incluse quelle generate dai warning
del parser audio (`Illegal Audio-MPEG-Header`, `Trying to resync...`).
Poiché alcuni file generano più warning in sequenza, lo stesso file poteva essere
contato più volte.
Risultato: il report mostrava 8 non-catalogati mentre i file effettivamente
non spostati erano 5.

**Fix:**
```python
# PRIMA (nel blocco except):
self.uncatalogued_files.append({'file': ..., 'reason': f'Errore: {e}', ...})

# DOPO (nel blocco except): solo log, nessun append
self.logger.error(f"Errore inaspettato per {file_path.name}: {e}")
# uncatalogued_files viene aggiornato SOLO nel blocco "if genre in ('Unknown', 'Other')"
```

L'unico posto dove `uncatalogued_files` viene incrementato è ora il blocco esplicito
`if genre in ('Unknown', 'Other')` dopo che tutti i tentativi di classificazione
sono stati esauriti.

---

## v1029 — FEAT: Colori WARNING e ERROR differenziati su console e GUI

**File modificati:**
- `utils/logging_config.py` → classe `SafeFormatter`
- `gui/main_window.py` → `PALETTE` + `_classify_line()`

**Problema:**
I WARNING venivano visualizzati in rosso (come gli ERROR) sia nella console
che nel log viewer della GUI, rendendo impossibile distinguerli visivamente.

### Console (terminale reale)

Aggiunta rilevazione `isatty()` per applicare colori ANSI solo su terminale
(non su pipe verso la GUI, evitando escape codes nel log viewer):

```python
_IS_TTY = hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()

# WARNING → giallo ANSI  \033[33m
# ERROR   → rosso brillante ANSI  \033[91m
```

Se `colorama` è installato viene inizializzato automaticamente per il supporto
colori su terminale Windows.

### GUI LogViewer

```python
# PRIMA:
"log_warning": "#B08040",   # ambra quasi invisibile sul fondo scuro
"log_error":   "#9A5050",   # rosso muted

# DOPO:
"log_warning": "#C8922A",   # ambra-arancio visibile (giallo-warm)
"log_error":   "#C05050",   # rosso muted, alzato per contrasto
```

Il metodo `_classify_line()` ora controlla anche i pattern `warning:` e `error:`
(oltre a `- WARNING -` e `- ERROR -`) per gestire messaggi di eccezione
che non passano dal formatter standard.

---

## Riepilogo file modificati

| File | Versione | Tipo |
|------|---------|------|
| `core/cataloger.py` | v1026, v1028 | Bug fix |
| `core/genre_classifier.py` | v1027 | Bug fix |
| `utils/logging_config.py` | v1029 | Feature |
| `gui/main_window.py` | v1029 | Feature + versione |

---

## Note tecniche aggiuntive

### Colori console — dipendenza colorama (opzionale)
`colorama` non è obbligatorio. Se non installato, i colori ANSI funzionano
nativamente su Windows 10+ (Console Host e Windows Terminal supportano ANSI
dal 2019). Su versioni precedenti i caratteri `ESC[33m` appaiono come testo
ma non causano crash. Per installarlo: `pip install colorama`.

### Compatibilità pipe → GUI
Il `SafeFormatter` verifica `sys.stdout.isatty()` prima di aggiungere escape codes.
Quando la GUI lancia il cataloger via `subprocess.Popen` con `stdout=PIPE`,
`isatty()` restituisce `False` → nessun escape code nel testo catturato dalla GUI.
I colori nel log viewer della GUI sono gestiti da `LogViewer._detect_tag()` in modo
indipendente, basandosi sul contenuto della riga.

---

## Music Cataloger Advanced — Upgrade Notes
**Data:** 2026-03-23  
**Versione di partenza:** v0.0.2.2  
**Versione finale:** v1039

---

## v1026 — BUG FIX: Progress bar ferma al 50%
**File:** `core/cataloger.py`  
Aggiunto filtro `f.parent == self.base_path` nel fallback glob, oltre al fix già presente nel metodo principale. La progress bar si bloccava al 50% perché il conteggio includeva file già classificati nelle sottocartelle.

---

## v1027 — BUG FIX: Genere "Salsa" non mappato a Latin/Salsa
**File:** `core/genre_classifier.py`  
`raw_lower = raw_genre.lower().strip()` applicato prima del confronto. Aggiornato anche `raw_genre` per `get_genre_folder_path()`. I file con tag API "Salsa" (maiuscolo) finivano in `Salsa/` root invece di `Latin/Salsa/`.

---

## v1028 — BUG FIX: Contatore "Non catalogati" inflazionato dai WARNING audio
**File:** `core/cataloger.py`  
Rimosso `uncatalogued_files.append()` dal blocco `except Exception`. Ora il contatore si incrementa SOLO nel ramo `if genre in ('Unknown', 'Other')` dopo che tutti i tentativi di classificazione sono esauriti.

---

## v1029 — FEAT: Colori WARNING giallo / ERROR rosso
**File:** `utils/logging_config.py`, `gui/main_window.py`  
Console: colori ANSI solo se `isatty()` (no escape codes nella pipe verso GUI). GUI: `#C8922A` ambra per WARNING, `#C05050` rosso per ERROR. Prima entrambi erano rossi.

---

## v1030 — BUG FIX: Conteggio doppio file (FileManager root-only)
**File:** `core/file_manager.py`  
Il metodo `scan_mp3_files(recursive=False)` ora filtra esplicitamente `f.parent == self.base_path` anche nel ramo non-ricorsivo. Su certi filesystem Windows il `glob("*.mp3")` poteva restituire file nelle sottocartelle già classificate, raddoppiando il totale (es. 314 invece di 157).

---

## v1031 — FEAT: Spostati aggiornati in tempo reale
**File:** `core/cataloger.py`, `gui/main_window.py`  
Aggiunto token `MOVED: 1` su stdout dopo ogni file spostato (sia in dry-run che in run reale). La GUI intercetta questo token e incrementa il contatore "Spostati" immediatamente, senza aspettare il riepilogo finale. Prima funzionava solo in dry-run (cercava la stringa `\-- [SIMULAZIONE]`).

---

## v1032 — FEAT: Non catalogati aggiornati in tempo reale
**File:** `core/cataloger.py`, `gui/main_window.py`  
Aggiunto token `UNCATALOGED: 1` su stdout quando un file viene classificato come Unknown. La GUI intercetta questo token per aggiornamento real-time. Il parser della riga riepilogativa finale `non catalogati: N` ora usa un pattern anchored (`^.*non\s+catalogati:\s*(\d+)\s*$`) per evitare false-positive sulle righe WARNING dei file singoli.

---

## v1033 — FEAT: ETA in minuti/ore nella progress bar
**File:** `core/cataloger.py`  
Finestra mobile degli ultimi 20 file (deque) per calcolare il tempo medio per file. L'ETA viene aggiornata ogni 5 file ed emessa come token `ETA: Xm XXs` su stdout. Viene mostrata nella progress bar (`Fase 1/2 — Catalogazione  ⏱ 5m30s`). Aggiunto anche token `TOTAL: N` e `PROGRESS: X/Y` per aggiornamento barra preciso.

---

## v1034 — FEAT: Parsing token aggiornato nella GUI
**File:** `gui/main_window.py`  
`_parse_stats()` riscritto per gestire i nuovi token (`PROGRESS:`, `TOTAL:`, `ETA:`, `MOVED:`, `UNCATALOGED:`) come fonte primaria di aggiornamento. I pattern regex sul testo del log restano come fallback per compatibilità.

---

## v1035 — FEAT: Opzione "Aggiorna DB Locale Generi" nella GUI
**File:** `gui/main_window.py`, `run_cataloger.py`  
Nuova sezione "🗄️ Libreria Locale" nel pannello sinistro con checkbox "Aggiorna DB locale Generi dopo catalogazione". Salva la mappatura file→genere in `music_library.json`. Nuovo argomento CLI `--update-local-db`.

---

## v1036 — FEAT: Sorgenti metadati selezionabili
**File:** `gui/main_window.py`  
Nuova sezione "🌐 Sorgenti Metadati" con checkbox per MusicBrainz, Last.fm, Beatport, GetSong. **Spotify rimosso dalla lista** con avviso visibile ("richiede licenza API a pagamento"). Questo corregge anche il fatto che Spotify veniva interrogato inutilmente generando errori 403 nel log.

---

## v1037 — FIX: Spotify rimosso dalle sorgenti cover
**File:** `gui/main_window.py`  
La sezione "Cover Album" ora mostra solo MusicBrainz e Last.fm come sorgenti. Spotify era incluso per default ma genera errori 403 senza licenza. Il default di `--cover-sources` in `run_cataloger.py` è ora `musicbrainz lastfm` (rimosso `spotify`).

---

## v1038 — FEAT: Storico directory recenti
**File:** `gui/main_window.py`  
Menu `File → Directory Recenti` con le ultime 10 directory utilizzate. Lo storico viene salvato in `recent_dirs.json` nella cartella del progetto. Include opzione "Cancella storico". Quando si seleziona una directory (via Sfoglia o dal menu recenti) viene automaticamente aggiunta in cima alla lista.

---

## v1039 — Aggiornamento versione e About
**File:** `gui/main_window.py`  
Titolo applicazione aggiornato a v1039. Dialog About aggiornato con riepilogo di tutte le versioni dalla v1026 alla v1039.

---

## Riepilogo file modificati

| File | Versioni | Tipo |
|------|---------|------|
| `core/file_manager.py` | v1030 | Bug fix |
| `core/cataloger.py` | v1026, v1028, v1031, v1032, v1033 | Bug fix + Feature |
| `core/genre_classifier.py` | v1027 | Bug fix |
| `utils/logging_config.py` | v1029 | Feature |
| `gui/main_window.py` | v1029, v1034–v1039 | Feature + Bug fix |
| `run_cataloger.py` | v1035, v1037 | Feature |

---

## Note su Spotify

Spotify Web API richiede approvazione del piano **Extended Access** (a pagamento) per accedere a metadati in produzione. Le chiamate con credenziali standard restituiscono errore 403. È stato rimosso da:
- Sorgenti metadati (sezione nuova)
- Sorgenti cover (default cambiato)

MusicBrainz è completamente gratuito e open. Last.fm ha API gratuite con rate limit ragionevole. Beatport e GetSong sono gratuiti con limitazioni.

## Altri DB online potenzialmente utilizzabili (valutazione futura)

| DB | Tipo | Note |
|----|------|------|
| **Discogs** | REST API gratuita | Ottima per vinili, classica, jazz. Richiede account. |
| **AcoustID** | Fingerprinting audio | Identifica brani da audio, molto preciso. Libreria `chromaprint` richiesta. |
| **AudD** | Fingerprinting audio | Simile a Shazam. Piano gratuito limitato (100 req/giorno). |
| **Deezer** | REST API | Gratuita, ottima copertura pop/latin. Nessuna autenticazione per ricerca base. |
| **iTunes/Apple Music** | REST API | Gratuita, ottima per pop. Endpoint: `itunes.apple.com/search`. |

---

## Music Cataloger Advanced — Upgrade Notes v1040–v1046
**Data:** 2026-03-23 · **Versione precedente:** v1039 · **Versione finale:** v1046

---

## v1040 — BUG FIX: Spotify ancora chiamato nei metadati
**File:** `services/external_apis.py`

**Problema:** `search_all()` aveva ancora Spotify come primo step della cascata nonostante fosse stato rimosso dalla GUI. Ogni file generava un errore 403 nel log a livello DEBUG.

**Fix:**
```python
# PRIMA: Spotify → MusicBrainz → Last.fm
# DOPO:  MusicBrainz → Last.fm
```
Spotify completamente rimosso dalla cascata. Le credenziali restano in `secrets.py` per eventuale integrazione futura con licenza.

---

## v1041 — BUG FIX: Conteggio doppio file (root cause definitiva)
**File:** `config/settings.py`

**Problema:** `supported_extensions = ['.mp3', '.MP3']` — su Windows il filesystem è case-insensitive, quindi `glob('*.mp3')` e `glob('*.MP3')` restituiscono **gli stessi file** entrambi. Risultato: 155 file contati come 310, progress bar al 50%.

**Fix:**
```python
# PRIMA:
supported_extensions: List[str] = field(default_factory=lambda: ['.mp3', '.MP3'])

# DOPO:
supported_extensions: List[str] = field(default_factory=lambda: ['.mp3'])
# Un solo pattern. Su Windows trova anche i .MP3 automaticamente (case-insensitive).
# Su Linux/macOS il glob è case-sensitive ma i file .MP3 sono rari.
```

---

## v1042 — BUG FIX / CLARITY: "Cover: incorporata" mostrata anche se la cover era già presente
**File:** `core/cataloger.py`

**Problema:** Il codice controllava `if cover_result:` che era True per qualsiasi stringa non vuota, incluso `'existing'` (cover già presente). Risultato: il log mostrava "Cover: incorporata" anche per file che avevano già la cover, e il contatore Cover si incrementava erroneamente.

**Fix:** Distinzione esplicita tra i 4 stati restituiti da `cover_service.process_file()`:
- `'existing'` → log DEBUG silenzioso, nessun incremento contatore
- `'downloaded'` → log INFO "Cover: scaricata e incorporata", incremento contatore  
- `'not_found'` → log DEBUG silenzioso
- `'error'` → nessun log aggiuntivo

---

## v1043 — BUG FIX: Non-catalogati duplicati nel report JSON
**File:** `core/cataloger.py` → `generate_report()`

**Problema:** Il report JSON mostrava gli stessi file duplicati in `uncatalogued_files` (es. DJ Husky, Jensen, Marc Anthony comparivano 2 volte ciascuno). Causato da scenari dove lo stesso file può essere processato da più step.

**Fix:** Deduplicazione per filename prima di scrivere il report:
```python
seen = set()
unique_uncatalogued = []
for fi in self.uncatalogued_files:
    if fi['file'] not in seen:
        seen.add(fi['file'])
        unique_uncatalogued.append(fi)
```
La statistica `uncatalogued` nel report riflette ora il numero corretto di file unici.

---

## v1044 — BUG FIX: "Salsa choke", "salsaton", "salsa peruana" → `Salsa/` invece di `Latin/Salsa/`
**File:** `core/genre_classifier.py`

**Problema:** `is_latin_subgenre()` faceva solo match esatto contro la lista `['salsa', 'bachata', 'merengue', ...]`. Tag come `'salsa choke'`, `'salsaton'`, `'salsa peruana'` non matchavano → il file finiva in `Salsa/` root invece di `Latin/Salsa/`.

**Fix in `is_latin_subgenre()`:**
```python
# PRIMA: return genre.lower() in self.latin_subgenres
# DOPO:  match esatto O parziale (sub in genre_lower)
for sub in self.latin_subgenres:
    if sub == gl or sub in gl:   # 'salsa' in 'salsaton' → True
        return True
```

**Fix in `get_genre_folder_path()`:** estrae il termine canonico dalla variante:
```python
for sub in self.latin_subgenres:
    if sub == gl or sub in gl:
        matched_sub = sub   # usa 'salsa', non 'salsaton'
        break
base = Path('Latin') / matched_sub.capitalize()
# → Latin/Salsa  (non Latin/Salsaton)
```

---

## v1045 — UX: Token interni nascosti dal log + ETA visibile nella barra
**File:** `gui/main_window.py`

**Problema 1 — Token nel log:** I token `PROGRESS:`, `MOVED:`, `ETA:`, ecc. apparivano nel log viewer rendendo il log caotico e difficile da leggere.

**Fix:** In `_poll_queue()`, i token vengono processati da `_parse_stats()` ma **non passati al log viewer**:
```python
_INTERNAL_TOKENS = ("PROGRESS:", "TOTAL:", "ETA:", "MOVED:", "UNCATALOGED:")
self._parse_stats(line)
if not any(line.startswith(tok) for tok in _INTERNAL_TOKENS):
    self._log.append(line, level)   # solo le righe log reali
```

**Problema 2 — ETA non visibile:** L'ETA veniva aggiornata nel `_phase_var` della progress bar (usato per la fase) sovrascrivendone il testo, o non era visibile affatto.

**Fix:** Aggiunta label dedicata `_eta_var` nella `LabeledProgressBar`, posizionata a destra del nome file e a sinistra della percentuale:
```
Fase 1/2 — Catalogazione    Heroes Never Die.mp3    ⏱ 5m30s    42%
[██████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░]
```
Metodo pubblico `set_eta(eta_str)` chiamato dal parser quando riceve token `ETA:`.

---

## v1046 — FEAT: DB locale music_library.json realmente integrato
**File:** `core/cataloger.py`, `services/local_db.py` (nuovo), `run_cataloger.py`

**Problema:** Il flag `--update-local-db` era accettato dalla CLI ma non utilizzato dal cataloger. Il file `music_library.json` non veniva mai creato.

**Soluzione:**
1. **`services/local_db.py`** — classe `LocalMusicDB` con `load()`, `save()`, `upsert()`, `count()`
2. **`MusicCataloger.__init__()`** — nuovo parametro `update_local_db=False`. Se True, istanzia `LocalMusicDB` e carica il DB esistente
3. **`_move_to_genre_folder()`** — dopo ogni spostamento riuscito, chiama `_local_db.upsert(relative_path, genre, subgenre)`
4. **`generate_report()`** — al termine, chiama `_local_db.save()` (solo in modalità reale, non dry-run)
5. **`run_cataloger.py`** — passa `update_local_db=args.update_local_db` al cataloger

**Percorso file:** `{cartella_progetto}/music_library.json`

**Struttura:**
```json
{
  "version": 1,
  "last_updated": "2026-03-23T12:41:37",
  "files": {
    "Soundtrack/47 Ronin - 47 Ronin Soundtrack.mp3": {
      "genre": "Soundtrack",
      "subgenre": "",
      "bpm": null,
      "quality_kbps": null,
      "cataloged_at": "2026-03-23T12:34:15"
    }
  }
}
```

---

## Riepilogo file modificati

| File | Versioni | Tipo |
|------|---------|------|
| `services/external_apis.py` | v1040 | Bug fix |
| `config/settings.py` | v1041 | Bug fix (root cause conteggio doppio) |
| `core/cataloger.py` | v1042, v1043, v1046 | Bug fix + Feature |
| `core/genre_classifier.py` | v1044 | Bug fix |
| `gui/main_window.py` | v1045, v1046 | UX fix + Feature |
| `run_cataloger.py` | v1046 | Feature |
| `services/local_db.py` | v1046 | **Nuovo file** |

---

## Pulizia file progetto — cosa eliminare

**Sicuro da eliminare (Docker/Build non usati):**
```
Dockerfile
docker-compose.yml
docker-entrypoint.sh
docker-helper.bat
test-docker-setup.bat
test-docker-setup.sh
DOCKER_ARCHITECTURE.md
CHECKLIST_DOCKER.md
QUICKSTART_DOCKER.md
README_DOCKER.md
build_config.spec
build_config_safe.spec
build_exe.py
build_portable.py
build_safe.py
build_simple.bat
README_BUILD_SAFE.md
README_PACKAGE.md
apply_gui_fix.bat
apply_gui_fix.py
FIX_GUI_INSTRUCTIONS.md
run_process_FIXED.py
run_gui_improved.bat
generate_icons.py        (se non usi icone custom)
cataloging_report_20260318_130818.json   (report nella root, va in output/)
```

**Da tenere:**
```
run_gui.py, run_gui.bat, run_cataloger.py   ← entry point
requirements.txt                            ← dipendenze pip
CLAUDE_CONTEXT.md                          ← context per Claude
README.md, INDEX.md                        ← documentazione
version_info.txt                           ← usato dal build exe
icons/                                     ← icone GUI
config/, core/, gui/, services/, utils/, tests/   ← codice
output/                                    ← log e report (generata automaticamente)
metadata_cache.json                        ← cache API (generata automaticamente)
```

---

## Music Cataloger Advanced — Upgrade Notes v1040–v1047
**Data:** 2026-03-23 · **Versione precedente:** v1039 · **Versione finale:** v1047

---

## v1040 — BUG FIX: Spotify ancora chiamato nei metadati
**File:** `services/external_apis.py`

**Problema:** `search_all()` aveva ancora Spotify come primo step della cascata nonostante fosse stato rimosso dalla GUI. Ogni file generava un errore 403 nel log a livello DEBUG, rallentando la catalogazione.

**Fix:**
```python
# PRIMA: Spotify → MusicBrainz → Last.fm
# DOPO:  MusicBrainz → Last.fm
```
Spotify completamente rimosso dalla cascata `search_all()`. Le credenziali restano in `secrets.py` per eventuale integrazione futura con licenza.

---

## v1041 — BUG FIX: Conteggio doppio file (root cause definitiva)
**File:** `config/settings.py`

**Problema:** `supported_extensions = ['.mp3', '.MP3']` — su Windows il filesystem è case-insensitive, quindi `glob('*.mp3')` e `glob('*.MP3')` restituiscono **gli stessi file** entrambi. Risultato: 155 file contati come 310, progress bar bloccata al 50%.

**Fix:**
```python
# PRIMA:
supported_extensions: List[str] = field(default_factory=lambda: ['.mp3', '.MP3'])

# DOPO:
supported_extensions: List[str] = field(default_factory=lambda: ['.mp3'])
# Un solo pattern. Su Windows trova anche i .MP3 automaticamente (case-insensitive).
```

---

## v1042 — BUG FIX / CLARITY: "Cover: incorporata" mostrata anche se già presente
**File:** `core/cataloger.py`

**Problema:** Il codice controllava `if cover_result:` che era True per qualsiasi stringa non vuota, incluso `'existing'` (cover già presente). Risultato: il log mostrava "Cover: incorporata" anche per file con cover preesistente, e il contatore Cover si incrementava erroneamente.

**Fix:** Distinzione esplicita tra i 4 stati restituiti da `cover_service.process_file()`:

| Stato | Log | Contatore |
|-------|-----|-----------|
| `'existing'` | DEBUG silenzioso | non incrementato |
| `'downloaded'` | INFO "Cover: scaricata e incorporata" | incrementato |
| `'not_found'` | DEBUG silenzioso | non incrementato |
| `'error'` | nessun log aggiuntivo | non incrementato |

---

## v1043 — BUG FIX: Non-catalogati duplicati nel report JSON
**File:** `core/cataloger.py` → `generate_report()`

**Problema:** Il report JSON mostrava gli stessi file duplicati in `uncatalogued_files` (es. DJ Husky, Jensen, Marc Anthony comparivano 2 volte ciascuno).

**Fix:** Deduplicazione per filename prima di scrivere il report:
```python
seen = set()
unique_uncatalogued = []
for fi in self.uncatalogued_files:
    if fi['file'] not in seen:
        seen.add(fi['file'])
        unique_uncatalogued.append(fi)
```
La statistica `uncatalogued` nel report riflette ora il numero corretto di file unici.

---

## v1044 — BUG FIX: "Salsa choke", "salsaton", "salsa peruana" → `Salsa/` invece di `Latin/Salsa/`
**File:** `core/genre_classifier.py`

**Problema:** `is_latin_subgenre()` faceva solo match esatto contro la lista `['salsa', 'bachata', 'merengue', ...]`. Tag come `'salsa choke'`, `'salsaton'`, `'salsa peruana'` non matchavano → il file finiva in `Salsa/` root invece di `Latin/Salsa/`.

**Fix in `is_latin_subgenre()`:** match parziale aggiunto:
```python
# PRIMA: return genre.lower() in self.latin_subgenres
# DOPO:
for sub in self.latin_subgenres:
    if sub == gl or sub in gl:   # 'salsa' in 'salsaton' → True
        return True
```

**Fix in `get_genre_folder_path()`:** estrae il termine canonico dalla variante:
```python
for sub in self.latin_subgenres:
    if sub == gl or sub in gl:
        matched_sub = sub   # usa 'salsa', non 'salsaton'
        break
base = Path('Latin') / matched_sub.capitalize()
# → Latin/Salsa  (non Latin/Salsaton)
```

---

## v1045 — UX: Token interni nascosti dal log + ETA visibile nella barra
**File:** `gui/main_window.py`

**Problema 1 — Token nel log:** I token `PROGRESS:`, `MOVED:`, `ETA:`, ecc. apparivano nel log viewer rendendo il log caotico e difficile da leggere per l'utente.

**Fix in `_poll_queue()`:**
```python
_INTERNAL_TOKENS = ("PROGRESS:", "TOTAL:", "ETA:", "MOVED:", "UNCATALOGED:")
self._parse_stats(line)   # sempre processato
if not any(line.startswith(tok) for tok in _INTERNAL_TOKENS):
    self._log.append(line, level)   # solo righe visibili all'utente
```

**Problema 2 — ETA non visibile nella barra:** L'ETA non era visibile nella barra di avanzamento.

**Fix:** Aggiunta label dedicata `_eta_var` nella `LabeledProgressBar`, posizionata tra il nome file e la percentuale:
```
Fase 1/2 — Catalogazione    Heroes Never Die.mp3    ⏱ 5m30s    42%
[██████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░]
```
Nuovo metodo pubblico `set_eta(eta_str)` chiamato dal parser quando riceve token `ETA:`.

---

## v1046 — FEAT: DB locale music_library.json realmente integrato
**File:** `core/cataloger.py`, `services/local_db.py` (nuovo), `run_cataloger.py`

**Problema:** Il flag `--update-local-db` era accettato dalla CLI ma non utilizzato dal cataloger. Il file `music_library.json` non veniva mai creato.

**Soluzione:**
1. **`services/local_db.py`** — classe `LocalMusicDB` con `load()`, `save()`, `upsert()`, `count()`
2. **`MusicCataloger.__init__()`** — nuovo parametro `update_local_db=False`; se True, istanzia `LocalMusicDB` e carica il DB esistente dopo `_setup_logging()`
3. **`_move_to_genre_folder()`** — dopo ogni spostamento riuscito, chiama `_local_db.upsert(relative_path, genre, subgenre)`
4. **`generate_report()`** — al termine, chiama `_local_db.save()` (solo in modalità reale, non dry-run)
5. **`run_cataloger.py`** — passa `update_local_db=args.update_local_db` al cataloger

**Percorso file:** `{cartella_progetto}/music_library.json`

**Struttura:**
```json
{
  "version": 1,
  "last_updated": "2026-03-23T12:41:37",
  "files": {
    "Soundtrack/47 Ronin - 47 Ronin Soundtrack.mp3": {
      "genre": "Soundtrack",
      "subgenre": "",
      "bpm": null,
      "quality_kbps": null,
      "cataloged_at": "2026-03-23T12:34:15"
    }
  }
}
```

---

## v1047 — FEAT: 5 nuovi database metadati + fix IndentationError al boot
**File:** `services/external_apis.py`, `config/secrets.py`, `gui/main_window.py`, `core/cataloger.py`

### Fix crash IndentationError
`if CONFIG_AVAILABLE and api_keys:` aveva il primo statement sulla stessa riga — errore introdotto durante il refactoring v1046. Corretto separando su righe distinte.

### Nuovi database integrati

| DB | Metodo | Autenticazione | Note |
|----|--------|---------------|------|
| **Deezer** | `search_deezer()` | Nessuna — API pubblica | Ottima copertura pop/latin, include copertine |
| **iTunes Search** | `search_itunes()` | Nessuna — API pubblica | Generi molto precisi, ottima per pop |
| **Discogs** | `search_discogs()` | Token gratuito | Migliore per jazz, classica, vinili |
| **AudD** | `search_audd(file)` | API key (100 req/giorno free) | Fingerprinting audio, identifica senza metadati |
| **AcoustID** | `search_acoustid(file)` | API key + `fpcalc.exe` | Fingerprinting open source, molto preciso |

### Cascata search_all aggiornata e intelligente

```
MusicBrainz → Last.fm → Deezer → iTunes → Discogs
```

La cascata non si ferma al primo risultato ma **continua finché trova un genere utile** (non generico come `'other'` o `'unknown'`). Se nessuna fonte ha un genere, ritorna il primo candidato con metadati base.

AudD e AcoustID vengono chiamati solo dai BPM services (fingerprinting su file audio), non dalla cascata testuale, per evitare di inviare file audio a ogni ricerca.

### Registrazione account per i DB con chiave

**Discogs** (raccomandato — gratuito):
1. Crea account su https://www.discogs.com/register
2. Vai su https://www.discogs.com/settings/developers → "Generate new token"
3. Incolla il token in `config/secrets.py`:
   ```python
   self.DISCOGS_TOKEN = self._get_key('DISCOGS_TOKEN', 'IL_TUO_TOKEN_QUI')
   ```

**AcoustID** (se hai file senza metadati):
1. Accedi con account MusicBrainz su https://acoustid.org/login
2. Vai su https://acoustid.org/api-key → copia la chiave
3. Scarica `fpcalc.exe` da https://acoustid.org/chromaprint → metti nella cartella `Music Cataloger/`
4. Installa: `pip install pyacoustid`
5. In `secrets.py`: `self.ACOUSTID_API_KEY = self._get_key('ACOUSTID_API_KEY', 'LA_TUA_CHIAVE')`

**AudD** (opzionale):
1. Crea account su https://audd.io
2. Dashboard → copia API Token
3. In `secrets.py`: `self.AUDD_API_KEY = self._get_key('AUDD_API_KEY', 'IL_TUO_TOKEN')`

**Deezer e iTunes** — nessuna registrazione necessaria, funzionano subito.

### Aggiornamenti GUI

La sezione "🌐 Sorgenti Metadati" ora mostra tutti i DB suddivisi in due gruppi:
- **Gratuite senza registrazione:** MusicBrainz, Last.fm, Beatport, GetSong, Deezer ✓ NUOVO, iTunes ✓ NUOVO
- **Con token API (opzionali):** Discogs ✓ NUOVO, AudD ✓ NUOVO, AcoustID ✓ NUOVO

Le istruzioni di registrazione sono mostrate direttamente nella sezione con i link diretti.

---

## Riepilogo file modificati v1040–v1047

| File | Versioni | Tipo |
|------|---------|------|
| `services/external_apis.py` | v1040, v1047 | Bug fix + Feature |
| `config/settings.py` | v1041 | Bug fix |
| `core/cataloger.py` | v1042, v1043, v1046, v1047 | Bug fix + Feature |
| `core/genre_classifier.py` | v1044 | Bug fix |
| `gui/main_window.py` | v1045, v1047 | UX fix + Feature |
| `run_cataloger.py` | v1046 | Feature |
| `config/secrets.py` | v1047 | Feature |
| `services/local_db.py` | v1046 | **Nuovo file** |

---

## Pulizia file progetto — cosa eliminare

**Sicuro da eliminare (Docker/Build non in uso):**
```
Dockerfile                   docker-compose.yml
docker-entrypoint.sh         docker-helper.bat
test-docker-setup.bat        test-docker-setup.sh
DOCKER_ARCHITECTURE.md       CHECKLIST_DOCKER.md
QUICKSTART_DOCKER.md         README_DOCKER.md
build_config.spec            build_config_safe.spec
build_exe.py                 build_portable.py
build_safe.py                build_simple.bat
README_BUILD_SAFE.md         README_PACKAGE.md
apply_gui_fix.bat            apply_gui_fix.py
FIX_GUI_INSTRUCTIONS.md      run_process_FIXED.py
run_gui_improved.bat
generate_icons.py            (se non usi icone custom)
cataloging_report_20260318_130818.json   (nella root, va in output/)
```

**Da tenere:**
```
run_gui.py                   run_gui.bat
run_cataloger.py             requirements.txt
CLAUDE_CONTEXT.md            README.md
INDEX.md                     version_info.txt
icons/                       config/
core/                        gui/
services/                    utils/
tests/                       output/
metadata_cache.json
```

---

## Music Cataloger Advanced — Upgrade Notes v1048
**Data:** 2026-03-23 · **Versione precedente:** v1047 · **Versione finale:** v1048

---

## File modificati

| File | Tipo |
|------|------|
| `gui/main_window.py` | Feature + UX fix |
| `core/cataloger.py` | Feature + UX fix |

---

## 1 — FIX: ETA — solo minuti/ore, niente secondi, niente spazio fantasma

**File:** `core/cataloger.py`, `gui/main_window.py`

**Problema 1 — Formato:** L'ETA mostrava secondi (es. `5m30s`) che rimanevano fermi per 5 file creando l'impressione che il programma si fosse bloccato.

**Nuovo formato:**
| Tempo rimanente | Visualizzato |
|----------------|-------------|
| ≥ 1 ora | `1h05m` |
| ≥ 1 minuto | `5m` |
| < 1 minuto | `<1m` |

**Problema 2 — Spazio fantasma:** La label ETA aveva `width=80` fisso. Quando l'ETA spariva (inizio/fine run), lo spazio vuoto rimaneva e la percentuale appariva spostata a sinistra.

**Fix:** Rimosso `width=80` dall'`_eta_label`. La label ora si adatta al contenuto e non occupa spazio quando vuota.

---

## 2 — FEAT: `APP_VERSION` — costante unica per aggiornare la versione

**File:** `gui/main_window.py`

In cima al file, prima di qualsiasi import, è ora presente:

```python
APP_VERSION = "v1048"
```

Per aggiornare la versione in tutta la GUI (titolo finestra, label pannello sinistro, dialog About) basta cambiare **solo questa riga**. Non serve più cercare le occorrenze nel codice.

---

## 3 — FEAT: Default "solo catalogazione" — tutto deselezionato

**File:** `gui/main_window.py`

Tutti i checkbox di opzione ora partono deselezionati all'avvio:

```python
self._opt_dry_run  = ctk.BooleanVar(value=False)   # catalogazione reale
self._opt_verbose  = ctk.BooleanVar(value=False)
self._opt_cleanup  = ctk.BooleanVar(value=False)
self._opt_classify = ctk.BooleanVar(value=False)
# ... tutti gli altri False
```

Questo rispecchia il workflow primario: l'utente apre l'app, seleziona la cartella, preme Avvia — nessuna opzione extra selezionata per default.

---

## 4 — FEAT: Ristrutturazione pannello destro con 3 Tab

**File:** `gui/main_window.py`

Il pannello destro è stato ristrutturato con un `CTkTabview` a 3 tab. La barra di progresso rimane sempre visibile sotto i tab.

### Tab 1: `📜  Log`
Il log real-time come prima, ora dentro un tab dedicato.

### Tab 2: `📚  DB Locale`
Visualizzazione user-friendly di `music_library.json`:
- **Toolbar** con bottone Ricarica e campo ricerca live (filtra per file, genere, subgenere)
- **Intestazione** fissa: File / Genere / Subgenere / Catalogato
- **Righe a zebra** (alternanza colore per leggibilità)
- **Contatore** record aggiornato in tempo reale durante la ricerca
- Si ricarica automaticamente con il bottone Ricarica; mostra "0 record" se il DB non esiste ancora

### Tab 3: `⚙️  Avanzate`
Impostazioni spostate dal pannello sinistro (che si è alleggerito):

**Sezione Classificazione:**
- Modalità Simulazione (dry-run)
- Output Dettagliato (DEBUG)
- Correggi Metadati Cartelle Esistenti
- Classifica Salsa per BPM
- Solo Analisi Collezione

**Sezione Database Metadati Avanzati:**
- Discogs, AudD, AcoustID (con istruzioni di registrazione inline)

**Sezione Manutenzione:**
- Rimuovi Cartelle Vuote
- Disabilita Database Esterni

**Sezione Cache API:**
- Info dimensione cache (numero voci + KB)
- Bottone "Svuota Cache" con conferma

---

## 5 — FEAT: Pannello sinistro semplificato

**File:** `gui/main_window.py`

Le sezioni nel pannello sinistro ora mostrano solo le opzioni più usate:

**Opzioni Catalogazione** (rimaste):
- Simulazione dry-run
- Output Dettagliato
- Rimuovi Cartelle Vuote
- Classifica Salsa per BPM

Con un link testuale `→ tab ⚙️ Avanzate →` per le opzioni avanzate.

**Sorgenti Metadati** (semplificate): solo i nomi brevi delle 6 sorgenti pubbliche. Discogs/AudD/AcoustID rimossi dalla lista (ora nel tab Avanzate) con un link `→ tab ⚙️ Avanzate →`.

---

## 6 — FEAT: Dialog interattivo generi orfani a fine catalogazione

**File:** `core/cataloger.py`, `gui/main_window.py`

Alla fine di ogni catalogazione completata con successo, appare una finestra popup `CTkToplevel` con:

### Intestazione
- Titolo "✓ Catalogazione Completata"
- Contatore file processati e generi trovati

### Sezione generi orfani
Un genere è **orfano** se contiene meno di 5 file. Per ogni genere orfano viene mostrato:
- Nome cartella
- Numero file
- Suggerimento macrogenere (es. `Salsa Choke` → `→ sposta in Latin`)

La mappatura macrogenere copre: tutti i subgeneri latin, Classical, Alternative, Rock, Pop, Electronic, R&B, Jazz, World, Soundtrack, Vocal, Hip Hop.

Se non ci sono generi orfani viene mostrato un messaggio verde di conferma.

### Sezione top generi
Barre orizzontali proporzionali per i top 8 generi della collezione, con conteggio file.

### Implementazione tecnica
Il cataloger emette un nuovo token su stdout:
```
GENRE_STATS: {"Soundtrack": 95, "Alternative": 23, ...}
```
La GUI lo intercetta in `_parse_stats()`, lo salva in `_last_genre_stats` e lo filtra dal log viewer. A fine run, `_finish()` chiama `_show_orphan_dialog()` con un delay di 200ms (per evitare conflitti con il rendering finale del log).

---

## Music Cataloger Advanced — Upgrade Notes v1049
**Data:** 2026-03-24 · **Versione precedente:** v1048 · **Versione finale:** v1049

---

## File modificati

| File | Tipo |
|------|------|
| `gui/main_window.py` | 5 feature |
| `core/cataloger.py` | Architettura dati |
| `services/external_apis.py` | Ottimizzazione priorità |
| `services/cover_service.py` | 2 nuove sorgenti cover |

---

## 1 — ARCH: Cartella `data/` per tutti i file dati

**File:** `core/cataloger.py`, `gui/main_window.py`

Tutti i file di dati sono stati spostati dalla directory principale del progetto in una sottocartella dedicata `data/`, creata automaticamente al primo avvio.

| File | Prima | Dopo |
|------|-------|------|
| Cache metadati API | `metadata_cache.json` (root) | `data/metadata_cache.json` |
| DB locale collezione | `music_library.json` (root) | `data/music_library.json` |
| Directory recenti GUI | `recent_dirs.json` (root) | `data/recent_dirs.json` |
| Preferenze generi | *(non esisteva)* | `data/genre_prefs.json` |

La cartella `output/` (log e report JSON) rimane invariata.

**Implementazione:** funzione helper `_get_data_dir()` in `main_window.py` e attributo `self.data_dir` nel cataloger, entrambi creano la cartella se non esiste.

> **Nota migrazione:** Se hai già un `metadata_cache.json` o `music_library.json` nella root del progetto, spostali manualmente in `data/` per non perdere la cache esistente.

---

## 2 — FIX: Pannello sinistro — nessuna duplicazione con tab Avanzate

**File:** `gui/main_window.py`

Le opzioni già presenti nel tab **⚙️ Avanzate** (Correggi Metadati, Solo Analisi, Disabilita DB Esterni, e i DB con token Discogs/AudD/AcoustID) non compaiono più nel pannello sinistro scorrevole. Il pannello sinistro ora mostra solo le 4 opzioni di uso quotidiano:
- Simulazione dry-run
- Output Dettagliato
- Rimuovi Cartelle Vuote
- Classifica Salsa per BPM

---

## 3 — UX: Bottone ▾ per directory recenti accanto a Sfoglia

**File:** `gui/main_window.py`

Accanto al bottone "Sfoglia" è stato aggiunto un bottone compatto **▾** che apre direttamente il menu delle directory recenti come popup contestuale, posizionato esattamente sotto il bottone stesso. Questo elimina il percorso `File → Directory Recenti → seleziona` — ora bastano 2 click.

Il menu del menubar `File → Directory Recenti` rimane per retrocompatibilità.

---

## 4 — UX: Tab più grandi e leggibili

**File:** `gui/main_window.py`

I tab del pannello destro usano ora font `("Segoe UI", 12, "bold")` tramite:
```python
self._tabview._segmented_button.configure(font=("Segoe UI", 12, "bold"))
```
Il parametro `anchor="nw"` posiziona i tab in alto a sinistra.

---

## 5 — FEAT: Cover da Deezer e iTunes

**File:** `services/cover_service.py`, `gui/main_window.py`

Aggiunti due nuovi metodi nella `CoverService`:
- `_from_deezer()` — usa l'endpoint pubblico `/search` di Deezer, recupera `cover_xl` (la risoluzione più alta disponibile)
- `_from_itunes()` — usa iTunes Search API, recupera `artworkUrl100` e la ridimensiona a `600x600`

Entrambi sono selezionabili nella sezione **Cover Album** del pannello sinistro, attivi per default. Il codice di selezione della cover "più grande" (`_fetch_largest`) funziona automaticamente anche con queste sorgenti.

---

## 6 — OTTIMIZZAZIONE: Nuovo ordine priorità cascata metadati

**File:** `services/external_apis.py`

Ordine precedente: MusicBrainz → Last.fm → Deezer → iTunes → Discogs

**Nuovo ordine v1049:** MusicBrainz → **Deezer** → **iTunes** → Last.fm → Discogs

| Posizione | Sorgente | Motivazione |
|-----------|---------|-------------|
| 1ª | **MusicBrainz** | Massima precisione — generi esatti per jazz, classica, soundtrack, artisti noti |
| 2ª | **Deezer** | Ottimo per pop/latin (Salsa, Reggaeton), film, generi italiani; risponde velocemente |
| 3ª | **iTunes** | Generi Apple Music molto precisi (Anime, TV Soundtrack, Classical); buono per pop |
| 4ª | **Last.fm** | Tag community utili per electronic, alternative, indie; debole su latin (restituisce "latina") |
| 5ª | **Discogs** | Specializzato jazz/vinili/world — richiede token, usato come ultimo resort |

Il cambio più significativo: **Deezer sale al 2° posto** perché per la collezione (molto soundtrack + latin) trova il genere corretto molto più spesso e velocemente di Last.fm, che spesso restituisce "genere sconosciuto" o il generico "latina".

---

## 7 — FEAT: Tab `🎵 Generi` — Gestione generi preferiti

**File:** `gui/main_window.py`

Nuovo tab nel pannello destro che permette all'utente di definire il proprio standard personale di generi. Le preferenze vengono salvate in `data/genre_prefs.json`.

### Struttura

7 macrogeneri con i loro subgeneri:

| Macrogenere | Subgeneri inclusi |
|-------------|------------------|
| 🎵 Latin | Salsa, Salsa Romantica, Bachata, Bachata Sensual, Reggaeton, Cumbia, Merengue, Tropical, Cha Cha Cha, Boogaloo, Mambo, Salsa Choke |
| 🎬 Soundtrack | Soundtrack, Anime, TV Soundtrack |
| 🎸 Rock & Alternative | Rock, Alternative, Metal, Punk |
| 🎹 Classical & Jazz | Classical, Contemporary Classical, Jazz, Blues |
| 🎧 Electronic | Electronic, House, Techno, Ambient |
| 🎤 Pop & R&B | Pop, R&B, Hip Hop, Vocal |
| 🌍 World & Other | World, Flamenco, Reggae, Folk |

### Funzionalità
- **Checkbox per macrogenere** — seleziona/deseleziona tutto il gruppo con un click
- **Griglia 2 colonne** per i subgeneri con descrizione inline
- **Bottoni toolbar:** Salva / ✓ Tutto / ✗ Nessuno
- **Persistenza:** le preferenze vengono caricate all'avvio e salvate in `data/genre_prefs.json`
- **Feedback:** la status bar mostra "✓ Preferenze generi salvate" per 2.5 secondi dopo il salvataggio

### Prossimo passo (da implementare)
Le preferenze salvate potranno essere usate dal classificatore per filtrare i generi suggeriti e personalizzare il dialog generi orfani in base ai generi realmente usati dall'utente.

---

## 8 — CHIARIMENTO: "Cache API" → "Cache Metadati"

La sezione nel tab Avanzate è stata rinominata da "Cache API" a "Cache Metadati" per chiarezza. Il file in questione è `data/metadata_cache.json` (spostato da `metadata_cache.json` nella root). Contiene le risposte dei database online (MusicBrainz, Deezer, Last.fm, ecc.) per evitare di ripetere le stesse chiamate API su file già processati.

---

## Music Cataloger Advanced — Upgrade Notes v1050
**Data:** 2026-03-24 · **Versione precedente:** v1049 · **File modificato:** `core/cataloger.py`

---

## BUG FIX: Log duplicato nel viewer della GUI

### Problema
Ogni riga del log appariva due volte nel log viewer della GUI (es. `INFO - Directory progetto: ...` → stampata due volte di fila).

### Causa
In `_setup_logging()`, il console handler veniva creato così:

```python
safe_stream = io.TextIOWrapper(
    sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True
)
console_handler = logging.StreamHandler(safe_stream)
```

Su Windows, `io.TextIOWrapper(sys.stdout.buffer, ...)` crea un **nuovo wrapper** sullo stesso buffer sottostante di `sys.stdout`. Quando Python flushava sia il wrapper originale che quello nuovo, ogni riga veniva scritta due volte nella PIPE. La GUI legge la PIPE riga per riga → ogni riga veniva accodata due volte nel log viewer.

Secondariamente, `logging.getLogger().handlers.clear()` puliva il root logger globale, che poteva interferire con altri moduli che usano `logging`.

### Fix

```python
# PRIMA (causa doppio flush su Windows):
safe_stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', ...)
console_handler = logging.StreamHandler(safe_stream)

# DOPO (v1050 — sys.stdout diretto, nessun wrapper aggiuntivo):
console_handler = logging.StreamHandler(sys.stdout)
```

Rimosso anche `logging.getLogger().handlers.clear()` (non necessario — il logger nominato `MusicCataloger` usa già `self.logger.handlers.clear()` prima di aggiungere i nuovi handler).

---

## Music Cataloger Advanced — Upgrade Notes v1050–v1053
**Data:** 2026-03-24 · **Versione precedente:** v1049 · **Versione finale:** v1053

---

## v1050 — BUG FIX: Log duplicato nel viewer GUI
**File:** `core/cataloger.py`

`io.TextIOWrapper(sys.stdout.buffer, ...)` creava su Windows un secondo buffer sovrapposto a `sys.stdout`. Ogni riga flushava due volte nella PIPE, la GUI la leggeva due volte. Fix: `logging.StreamHandler(sys.stdout)` diretto, senza wrapper. Rimosso anche `logging.getLogger().handlers.clear()` che interferiva con il root logger globale.

---

## v1051 — BUG FIX: ETA a destra invece che a sinistra della percentuale
**File:** `gui/main_window.py`

Con `pack(side="right")` Tkinter impacca da destra verso sinistra: l'ultimo widget packato finisce più a sinistra. Invertito l'ordine: prima `%` (estrema destra), poi `⏱ ETA` (subito alla sua sinistra). Risultato: `⏱ 5m  42%`.

---

## v1052 — FEAT/FIX: Dialog generi orfani migliorato
**File:** `gui/main_window.py`

**Pulsante Sposta:** accanto al suggerimento `→ Latin` c'è ora il pulsante **Sposta** che sposta fisicamente i file, rimuove la cartella sorgente se vuota, e fa sparire la riga dal dialog.

**Macrogeneri esclusi:** `Latin`, `Rock`, `Pop`, `Classical` ecc. non vengono più segnalati come orfani anche se hanno pochi file — non ha senso suggerire di spostare `Latin` in `Latin`.

**Mapping corretto:** tabella esplicita subgenere→macrogenere (es. `Blues→Jazz`, `Indie→Alternative`, `Anime→Soundtrack`, `Reggaeton→Latin`).

---

## v1053 — Riorganizzazione completa GUI
**File:** `gui/main_window.py`, `core/cataloger.py`

### Pannello sinistro — ridotto al minimo

Il pannello sinistro ora mostra solo l'essenziale:

**Directory Musicale**
- Entry path + pulsante Sfoglia + pulsante ▾ (dropdown recenti, ridotto a 20px)

**Opzioni Catalogazione**
- Solo Analisi Collezione
- Rimuovi Cartelle Vuote
- Correggi Metadati Cartelle Esistenti
- ✅ Abilita Sorgenti DB Online (master switch, collegato a `--no-external`)

**Gestione Duplicati** (invariata)

**Cover Album** (solo checkbox principale)
- Recupera cover mancanti automaticamente
- Link → tab Avanzate per il resto

**Bottoni Avvia / Ferma / Pulisci Log**

Tutto il resto (dry-run, verbose, BPM, sorgenti metadati, cover avanzate, libreria locale, manutenzione) è nel tab ⚙️ Avanzate.

### Nuova variabile `_opt_use_ext_db`

Sostituisce la logica invertita di `_opt_no_ext` nel pannello sinistro. Se deselezionata → passa `--no-external` al processo. `_opt_no_ext` rimane nel tab Avanzate come opzione aggiuntiva.

### Libreria locale attiva di default

`self._opt_local_db = ctk.BooleanVar(value=True)` — il DB locale viene aggiornato automaticamente a ogni catalogazione senza che l'utente debba ricordarsi di selezionarlo.

### Tab ⚙️ Avanzate — contenuto completo

5 sezioni:
1. **Classificazione** — dry-run, verbose, classifica Salsa per BPM
2. **Sorgenti Metadati (ordine = priorità)** — le 6 sorgenti pubbliche numerate 1-6 nell'ordine esatto della cascata, poi 3 sorgenti con token numerate 7-9. Il numero indica la priorità: la prima fonte che restituisce un genere utile blocca la cascata.
3. **Cover Album** — sovrascrittura, strategia, sorgenti (MusicBrainz/Last.fm/Deezer/iTunes)
4. **Libreria Locale** — checkbox aggiorna DB locale
5. **Manutenzione** — disabilita DB esterni

### Tab 💾 Cache (nuovo, 5° tab)

Visualizzazione tabellare di `data/metadata_cache.json`:
- **Toolbar** con Ricarica, ricerca live, Svuota Cache
- **Lista** con alternanza zebra: Artista/Titolo — Genere — Sorgente
- **Pannello dettaglio** a destra: al click su una riga mostra Artist, Title, Album, Genre, Year, BPM, Source
- **Anteprima cover** in background: se il record ha `cover_url`, scarica e mostra l'immagine 200×200px. Usa `CTkImage` per HiDPI. Fallback emoji 🎵 se nessuna cover disponibile o download fallito.

### Tab ⚙️ Avanzate — rimozione sezione Cache

La sezione "Cache Metadati" è stata spostata nel tab dedicato 💾 Cache. Il bottone Svuota Cache è ora nel toolbar del tab Cache.

### Tab 🎵 Generi — espanso a 8 macrogeneri e 60+ subgeneri

Aggiunti:
- **Latin:** Salsa Easy/Medium/Hard/Master, Timba, Vallenato, Bolero, Bachata Influence
- **Soundtrack:** Video Game, Trailer Music, Epic Orchestral
- **Rock:** Indie, Grunge, Hard Rock, Progressive Rock, Death Metal
- **Classical & Jazz:** Opera, Piano, Baroque, Smooth Jazz, Soul
- **Electronic:** Trance, Drum and Bass, Dubstep, EDM, Synthwave
- **Pop & R&B:** Dance Pop, Trap, K-Pop, J-Pop, Country, Country Pop
- **World:** African, Brazilian (MPB/Samba), Celtic, Middle Eastern
- **🗂️ Altro** (nuovo macrogenere): Instrumental, Spoken Word, Comedy, Children, Holiday

### Tab centrati e font più grandi

`anchor="n"` (centrati) invece di `"nw"` (a sinistra). Font tab: `Segoe UI 13 bold`.

---

## Note migrazione

**Cache:** se hai `metadata_cache.json` nella root del progetto, spostalo in `data/metadata_cache.json` per non perdere la cache esistente. Il cataloger scrive già in `data/` dal v1049.

**File di versione:** d'ora in poi gli UPGRADE saranno cumulativi (un file per sessione, non uno per versione).

---

## Music Cataloger Advanced — Upgrade Notes v1051
**Data:** 2026-03-24 · **Versione precedente:** v1050 · **File modificato:** `gui/main_window.py`

---

## BUG FIX: ETA visualizzata a destra della percentuale invece che a sinistra

### Problema
L'ETA (`⏱ 5m`) appariva alla destra della percentuale (`42%`) invece che alla sua sinistra. Risultato: `42%  ⏱ 5m` invece del desiderato `⏱ 5m  42%`.

### Causa
Nel widget `LabeledProgressBar`, entrambe le label usano `pack(side="right")`. Con questo layout manager Tkinter, **l'ordine di pack determina la posizione**: il primo widget packato con `side="right"` occupa l'estrema destra, il successivo si posiziona immediatamente alla sua sinistra.

Il codice precedente packava prima `_eta_label` poi `_pct_label`:
```
pack → _eta_label (estrema destra)
pack → _pct_label (alla sua sinistra)
```
Risultato visivo: `[fase] [file...]  [42%]  [⏱ 5m]`

### Fix
Invertito l'ordine di pack:
```python
# v1051: prima % (va all'estrema destra), poi ETA (va alla sua sinistra)
self._pct_label.pack(side="right")          # → estrema destra
self._eta_label.pack(side="right", ...)     # → subito a sinistra di %
```
Risultato visivo corretto: `[fase] [file...]  [⏱ 5m]  [42%]`

---

## Music Cataloger Advanced — Upgrade Notes v1052
**Data:** 2026-03-24 · **Versione precedente:** v1051 · **File modificato:** `gui/main_window.py`

---

## FEAT/FIX: Dialog generi orfani — 3 miglioramenti

### 1 — Pulsante "Sposta" accanto al suggerimento

**Prima:** il dialog mostrava solo il testo `→ sposta in Latin` come suggerimento passivo.

**Dopo (v1052):** accanto al suggerimento c'è un pulsante **Sposta** che esegue lo spostamento fisico dei file:

```
📁 Indie  1 file    → Alternative    [Sposta]
📁 Blues  2 file    → Jazz           [Sposta]
📁 Anime  3 file    → Soundtrack     [Sposta]
📁 Indie Pop 1 file    valuta manualmente
```

Il metodo `_do_move(genre, dest_folder, base_path, row_widget)`:
1. Cerca la cartella `base_path/genre`
2. Sposta tutti i file in `base_path/dest_folder/`
3. Gestisce i conflitti di nome (aggiunge `_moved` al nome)
4. Rimuove la cartella sorgente se vuota
5. Distrugge la riga dal dialog (feedback visivo immediato)
6. Mostra un `showinfo` con il conteggio dei file spostati

Se la cartella non esiste (già spostata manualmente) mostra un errore descrittivo.

---

### 2 — Macrogeneri non segnalati come orfani

**Prima:** generi come `Latin`, `Rock`, `Pop`, `Classical` con pochi file venivano segnalati come orfani con il suggerimento `→ sposta in Latin` (suggeriva di spostare Latin in Latin — non ha senso).

**Dopo (v1052):** i macrogeneri sono esclusi dalla lista orfani, indipendentemente dal numero di file:

```python
_MACRO_GENRES = {
    "latin", "rock", "pop", "classical", "electronic", "r&b", "jazz",
    "world", "soundtrack", "alternative", "metal", "hip hop", "country",
    "vocal", "blues", "indie", "ambient",
}
orfani = {
    g: c for g, c in stats.items()
    if c < SOGLIA and g.lower() not in _MACRO_GENRES
}
```

---

### 3 — Suggerimento macrogenere corretto (non ripetitivo)

**Prima:** la mappatura usava match parziale generico che poteva suggerire lo stesso nome (es. `Latin/Merengue` → `→ sposta in Latin`). Questo era corretto ma il pulsante Sposta era assente.

**Dopo (v1052):** mapping esplicito subgenere→macrogenere con priorità al match esatto:

| Subgenere | Macrogenere suggerito |
|-----------|----------------------|
| Salsa Choke, Salsaton, Salsa Romantica | Latin |
| Bachata Sensual, Bachata Fusion | Latin |
| Reggaeton, Cumbia, Merengue, Tropical | Latin |
| Cha Cha Cha, Boogaloo, Mambo | Latin |
| Contemporary Classical, Orchestral, Opera | Classical |
| Indie, Post-Punk | Alternative |
| Heavy Metal, Death Metal, Power Metal, Punk | Rock |
| House, Techno, Trance, Drum and Bass | Electronic |
| Soul | R&B |
| Blues, Swing | Jazz |
| Anime, TV Soundtrack, Film | Soundtrack |
| Hip Hop, Rap | Hip Hop |
| Flamenco, Reggae, Folk | World |

Generi senza corrispondenza mostrano `valuta manualmente` (senza pulsante Sposta).

---

## Music Cataloger Advanced — Upgrade Notes v1054–v1055
**Data:** 2026-03-24 · **Versione precedente:** v1053 · **Versione finale:** v1055

---

## v1054 — Fix + Riorganizzazione

### BUG FIX: `--cover-sources` rifiutava `deezer` e `itunes`
**File:** `run_cataloger.py`

Il parser argparse aveva ancora le scelte hardcoded a `["spotify", "musicbrainz", "lastfm"]`. La GUI passava `deezer` e `itunes` → crash `invalid choice`.

```python
# PRIMA:
choices=["spotify", "musicbrainz", "lastfm"]
default=["musicbrainz", "lastfm"]

# DOPO:
choices=["spotify", "musicbrainz", "lastfm", "deezer", "itunes"]
default=["musicbrainz", "lastfm", "deezer", "itunes"]
```

### Aggiornamento APP_VERSION
**File:** `gui/main_window.py`

`APP_VERSION = "v1054"`. La costante aggiorna automaticamente: titolo finestra, label pannello sinistro, dialog About. **Non** propaga ai file README, `run_gui.bat`, `version_info.txt` — questi vanno aggiornati manualmente se necessario.

### Spostamento "Correggi Metadati Cartelle Esistenti" in tab Avanzate
**File:** `gui/main_window.py`

Rimosso dal pannello sinistro (sezione Opzioni Catalogazione) e aggiunto nella sezione **Classificazione** del tab ⚙️ Avanzate, sotto "Classifica Salsa per BPM".

### Rimozione livelli BPM Salsa dal tab Generi
**File:** `gui/main_window.py`

I livelli `Salsa Easy`, `Salsa Medium`, `Salsa Hard`, `Salsa Master` sono stati rimossi dal `_GENRE_TREE`. Come correttamente osservato, non sono sottogeneri editoriali ma il risultato di una feature di classificazione automatica (il flag `--classify-salsa`). La feature rimane nel tab Avanzate.

---

## v1055 — Tab ⚠️ Qualità: file MP3 a bassa qualità

### Obiettivo
Permettere all'utente di identificare rapidamente i file MP3 con bitrate basso, così da poterli riscaricare in qualità superiore.

### Nuovo tab `⚠️  Qualità`
**File:** `gui/main_window.py`, `core/metadata_extractor.py`

Il tab è il 5° nel pannello destro (tra Cache e Avanzate).

**Toolbar:**
- Pulsante **Scansiona** — avvia la lettura dei bitrate in background (non blocca la GUI)
- Menu **Mostra ≤ [128 / 192 / 256 / 320] kbps** — filtra i risultati in tempo reale
- Contatore: `X file ≤ Y kbps (su Z totali)`

**Lista risultati:**
Ogni riga mostra:
- Nome file (troncato se lungo)
- Bitrate in kbps (colorato)
- Etichetta qualità (colorata)
- Cartella relativa

**Codice colori:**

| Bitrate | Etichetta | Colore |
|---------|-----------|--------|
| < 128 kbps | 🔴 Bassa qualità | Rosso |
| 128–191 kbps | 🟡 Qualità media | Ambra |
| 192–319 kbps | 🟢 Buona qualità | Verde |
| 320+ kbps | 💎 Alta qualità | Blu |

**Default filtro:** ≤ 192 kbps — mostra i file che vale la pena riscaricare.

**Implementazione:**
- La scansione usa `mutagen.mp3.MP3` (già dipendenza del progetto) per leggere `audio.info.bitrate`
- Gira in `threading.Thread` separato — la GUI rimane responsive
- I risultati sono ordinati per bitrate crescente (i peggiori in cima)
- Il filtro si ricalcola istantaneamente al cambio della soglia, senza ri-scansionare

**Aggiornamento `metadata_extractor.py`:**
Aggiunto il campo `bitrate` all'estrazione `extract_metadata_mutagen`:
```python
'bitrate': int(audio.info.bitrate // 1000) if audio.info and hasattr(audio.info, 'bitrate') else None
```

### Note d'uso
Seleziona la directory musicale nel pannello sinistro, vai nel tab Qualità, clicca Scansiona. La scansione è separata dalla catalogazione — puoi usarla in qualsiasi momento senza avviare il processo completo.

---

## Music Cataloger Advanced — Upgrade Notes v1056
**Data:** 2026-03-24 · **Versione precedente:** v1055 · **File modificati:** `gui/main_window.py`, `core/cataloger.py`, `version.py` (nuovo)

---

## 1 — BUG FIX CRITICO: Log doppio e doppia finestra (stessa causa)

**File:** `gui/main_window.py`

**Root cause:** Nel `_poll_queue`, il blocco `if not any(line.startswith(tok)...)` conteneva **due** chiamate identiche a `self._log.append(line, level)` — un residuo di un vecchio find-replace che aveva duplicato le righe. Questo causava:
- Ogni riga del log stampata due volte
- Il token `"done"` processato due volte → `_finish()` chiamato due volte → **due finestre "Catalogazione Completata"**

**Fix:** rimossa la riga duplicata. Una sola chiamata `_log.append` per ogni riga ricevuta.

---

## 2 — BUG FIX CRITICO: Cache metadati sempre vuota

**File:** `core/cataloger.py`

**Root cause:** `ExternalAPIs` ha la propria `self.metadata_cache = {}` separata da quella del cataloger (`self.metadata_cache`). Il flusso era rotto:
- `load_cache()` caricava la cache in `cataloger.metadata_cache` ma non la propagava a `ExternalAPIs`
- Durante la catalogazione, `ExternalAPIs` scriveva i dati nella **propria** dict separata
- `save_cache()` salvava `cataloger.metadata_cache` (rimasta vuota) invece di `external_apis.metadata_cache`

**Fix:**
- `load_cache()` propaga la cache caricata a `external_apis.metadata_cache`
- `save_cache()` legge da `external_apis.metadata_cache` prima di scrivere
- `_init_services()` propaga la cache esistente ai servizi appena creati

Da questa versione la cache cresce correttamente ad ogni catalogazione e viene riutilizzata nelle esecuzioni successive, evitando chiamate API ripetute per brani già elaborati.

---

## 3 — FIX: Tab Qualità non bloccante + usa DB locale

**File:** `gui/main_window.py`

**Problemi v1055:**
- Il thread chiamava `winfo_children()` direttamente — non thread-safe in tkinter → freeze della GUI
- Rescansionava tutti i file con mutagen anche se i dati erano già nel DB locale

**Fix v1056:**
- **Fase 1:** legge i bitrate dal `music_library.json` (DB locale) — istantaneo, nessuna I/O pesante
- **Fase 2:** usa mutagen solo per i file non presenti nel DB locale (fallback)
- Tutti gli aggiornamenti alla GUI avvengono via `root.after(0, ...)` — thread-safe
- Il pulsante si chiama ora **Analizza** (più descrittivo di "Scansiona")

**Conseguenza:** dopo una catalogazione con `--update-local-db`, il tab Qualità mostra i risultati in pochi secondi invece di bloccarsi.

---

## 4 — FIX: Bitrate salvato nel DB locale durante catalogazione

**File:** `core/cataloger.py`

In `_move_to_genre_folder()`, dopo aver spostato il file, ora viene letto il bitrate con mutagen e salvato nel DB locale tramite `upsert(quality_kbps=kbps)`. Questo popola il campo `quality_kbps` nel `music_library.json`, rendendo il tab Qualità operativo immediatamente dopo la prima catalogazione.

---

## 5 — FIX: Rimossa ridondanza "Disabilita Database Esterni"

**File:** `gui/main_window.py`

La checkbox "🚫 Disabilita Database Esterni" nel tab Avanzate era ridondante con "🌐 Abilita Sorgenti DB Online" nel pannello sinistro — stessa funzione con polarità invertita. Rimossa dal tab Avanzate. Rimane solo il master switch nel pannello sinistro.

---

## 6 — NUOVO: `version.py` — versione centralizzata

**File:** `version.py` (nuovo, nella root del progetto)

```python
APP_NAME    = "Music Cataloger Advanced"
APP_VERSION = "v1056"
VERSION_STRING = f"{APP_NAME}  {APP_VERSION}"
```

`main_window.py` importa `APP_VERSION` da questo file. Per aggiornare la versione in tutta l'applicazione basta modificare `version.py`. Se l'import fallisce (pacchetto standalone), usa `"v1056"` come fallback.

**Da aggiornare manualmente** (non ancora automatizzato): `run_gui.bat`, `README.md`, `version_info.txt`.

---

## Music Cataloger Advanced — Upgrade Notes v1057
**Data:** 2026-03-25 · **Versione precedente:** v1056 · **File modificati:** `gui/main_window.py`, `core/cataloger.py`, `run_cataloger.py`, `utils/logging_config.py`, `version.py`

---

## 1 — BUG FIX CRITICO: Doppia finestra "Catalogazione Completata"

**File:** `gui/main_window.py`

**Root cause:** In `_show_orphan_dialog` erano presenti **due** blocchi `win = ctk.CTkToplevel(self.root)` — il secondo era un residuo della vecchia versione del dialog rimasto dopo una sostituzione parziale (circa 5900 caratteri di codice duplicato alle righe 2167–2290). Entrambi i dialog venivano aperti ad ogni fine catalogazione.

**Fix:** rimosso il secondo blocco completo. Il dialog corretto (con i pulsanti **Sposta**) era il primo (riga 2045). Il residuo è stato eliminato.

---

## 2 — BUG FIX: Cache tab — `AttributeError: 'NoneType' object has no attribute 'get'`

**File:** `gui/main_window.py`

**Root cause:** `metadata_cache.json` contiene 144 voci con valore `None` (ricerche API fallite salvate come `None` per evitare ri-ricerche). Il metodo `_cache_filter` iterava su tutte le voci senza controllare se il valore fosse `None`, e chiamava `val.get("artist")` su `None`.

**Fix:** aggiunto `if val is not None` nel list comprehension di `_cache_filter`. Le voci `None` vengono saltate sia nel rendering che nella ricerca.

---

## 3 — BUG FIX: Tab Qualità — lista vuota dopo analisi

**File:** `gui/main_window.py`

**Root cause:** Le chiavi nel `music_library.json` sono nel formato `"Genere/filename.mp3"` (es. `"Soundtrack/About Time - Nick Laird Clowes.mp3"`), ma il thread del tab Qualità cercava solo per `fname` (nome file senza cartella). Il confronto `if fname in db_kbps` falliva sempre perché `db_kbps` era indicizzato per path intera.

**Fix:** il dizionario `db_kbps` ora usa `Path(rel_path).name.lower()` come chiave (solo nome file, case-insensitive). Il confronto usa `fname.lower()` per essere case-insensitive. Risultato: 180/229 file nel DB locale vengono trovati immediatamente senza usare mutagen.

---

## 4 — FEAT: Generi preferiti collegati alla classificazione

**File:** `gui/main_window.py`, `core/cataloger.py`, `run_cataloger.py`

I generi deselezionati nel tab **🎵 Generi** ora influenzano realmente la catalogazione.

**Flusso:**
1. `_build_command()` legge `self._genre_prefs` e raccoglie tutti i subgeneri con valore `False`
2. Li passa come `--excluded-genres Vocal Indie ...` al processo
3. `run_cataloger.py` accetta il nuovo argomento e lo passa a `MusicCataloger`
4. In `_move_to_genre_folder()`, se il genere assegnato è in `excluded_genres`, viene sostituito con il macrogenere padre (es. `Vocal → Pop`, `Indie → Rock`, `Anime → Soundtrack`)
5. Il log mostra: `[GENERE ESCLUSO] Vocal → uso macrogenere: Pop`

**Nota:** `_get_parent_genre()` contiene la mappa completa subgenere→macrogenere per tutti i 60+ subgeneri.

---

## 5 — FEAT: "Abilita Sorgenti DB Online" disabilita la sezione in Avanzate

**File:** `gui/main_window.py`

Aggiunto `command=self._on_ext_db_toggle` alla checkbox "Abilita Sorgenti DB Online" nel pannello sinistro.

Il metodo `_on_ext_db_toggle()` itera ricorsivamente su tutti i widget figli del frame "Sorgenti Metadati" nel tab Avanzate e imposta `state="disabled"` o `state="normal"` in base allo stato della checkbox.

---

## 6 — FEAT: Sezione Manutenzione con 4 azioni

**File:** `gui/main_window.py`

La sezione **🧹 Manutenzione** nel tab Avanzate ora contiene 4 pulsanti:

| Pulsante | Descrizione |
|----------|-------------|
| 📋 Esporta lista file in CSV | Esporta `music_library.json` in CSV con colonne: File, Genere, Sottogenere, Qualità (kbps), BPM, Catalogato il |
| 🔍 Trova duplicati per nome file | Mostra un dialog con tutti i file che hanno lo stesso nome in cartelle diverse |
| 🗑️ Svuota Cache Metadati | Svuota `metadata_cache.json` (stesso pulsante del tab Cache) |
| 📂 Apri Cartella Dati | Apre `data/` in Esplora File |

---

## 7 — FIX: Unicode cp1252 — nomi file con caratteri speciali

**File:** `utils/logging_config.py`, `run_cataloger.py`

**Problema:** su Windows con console cp1252, i nomi file contenenti caratteri come `ć` (`\u0107`), BOM (`\ufeff`), o NUL (`\x00`) causavano `UnicodeEncodeError: 'charmap' codec can't encode character`.

**Fix in due parti:**

1. **`SafeFormatter.format()`** — prima di formattare:
   - Rimuove BOM (`\ufeff`) e NUL (`\x00`) dal messaggio
   - Encode/decode con `errors='replace'` per cp1252: i caratteri non encodabili vengono sostituiti con `?` invece di crashare

2. **`run_cataloger.py`** — `sys.stdout.reconfigure(encoding='utf-8', errors='replace')` all'avvio (Python 3.7+): forza stdout in UTF-8, eliminando il problema alla radice quando il processo viene avviato dalla GUI.

---

## 8 — FIX: `_build_command` — rimossa opzione `--no-external` duplicata

**File:** `gui/main_window.py`

`_build_command` conteneva due righe che aggiungevano `--no-external`:
- una per `_opt_use_ext_db` (master switch)
- una per `_opt_no_ext` (checkbox rimossa dal tab Avanzate nel v1054 ma rimasta nel codice)

Rimossa la seconda riga duplicata.

---

## Music Cataloger Advanced — Upgrade Notes v1058
**Data:** 2026-03-25 · **Versione precedente:** v1057 · **File modificati:** `gui/main_window.py`, `run_cataloger.py`, `version.py`

---

## 1 — BUG FIX CRITICO: `NameError: name 'music_path' is not defined`

**File:** `run_cataloger.py`

Il tentativo di aggiungere `sys.stdout.reconfigure()` nella v1057 aveva spezzato l'indentazione del file: il blocco `import sys as _sys` e l'intera istanza `MusicCataloger(...)` erano finiti **fuori** dalla funzione `main()`, rendendo `music_path` e `args` inaccessibili.

**Fix:** `run_cataloger.py` riscritto da zero in modo pulito. Il `sys.stdout.reconfigure(encoding='utf-8', errors='replace')` è ora al livello del modulo (prima di `main()`), come deve essere.

---

## 2 — FIX: Tab Qualità — lista sempre vuota + freeze senza feedback

**File:** `gui/main_window.py`

**Problema 1 — lista vuota:** i figli di `CTkScrollableFrame` devono usare `pack()`, non `grid()`. `_quality_filter` usava `row.grid(row=idx, column=0, ...)` → i widget venivano creati ma non visualizzati.

**Fix:** sostituiti tutti i `grid()` con `pack(fill="x", ...)` per i figli del `_quality_list`.

**Problema 2 — freeze senza feedback:** quando mutagen doveva leggere file non presenti nel DB locale, il thread girava in background ma la finestra principale diventava grigia senza spiegazione, lasciando l'utente nel dubbio che tutto si fosse bloccato.

**Fix:** aggiunta una **finestra modale di progresso** (`CTkToplevel` non chiudibile) che appare durante tutta la scansione:
- Barra di progresso indeterminata animata
- Label aggiornata ogni 30 file: `File 120/1839 (mutagen: 45)`
- La finestra si chiude automaticamente al termine
- Il contatore finale include la fonte: `180 file ≤ 192 kbps — DB locale: 180, mutagen: 12`

---

## 3 — FIX: Scrollbar superflua nel pannello sinistro

**File:** `gui/main_window.py`

Il pannello sinistro usava `CTkScrollableFrame` che aggiunge sempre una scrollbar verticale, anche quando il contenuto ci sta. Sostituito con `CTkFrame` normale + `grid_propagate(False)` per mantenere la larghezza fissa (400px). La scrollbar è sparita.

---

## 4 — FEAT: Controlli disabilitati durante la catalogazione

**File:** `gui/main_window.py`

Durante il run, tutti i controlli interattivi del pannello sinistro vengono disabilitati (greyout): Directory, Opzioni, Gestione Duplicati, Cover Album. Al termine (`_finish()`), vengono riabilitati automaticamente.

Implementato tramite `_set_controls_state(state)` che itera ricorsivamente sui 4 frame del pannello sinistro (salvati come `_left_dir_frame`, `_left_options_frame`, `_left_dup_frame`, `_left_cover_frame`).

---

## 5 — FIX: Cover cache — garbage collection

**File:** `gui/main_window.py`

`CTkImage` veniva creato nel thread, passato a una lambda di `root.after`, poi garbage-collected prima che la lambda venisse eseguita. Il secondo click su un record non mostrava più la cover.

**Fix:** `self._cover_image_ref = ctk_img` viene assegnato **prima** di `root.after`, e la lambda cattura l'oggetto direttamente via parametro `lambda i=ctk_img: ...` invece di catturare la variabile locale (che potrebbe essere GC'd).

---

## 6 — FIX: Spazio doppio "Svuota  Cache Metadati"

**File:** `gui/main_window.py`

L'emoji `🗑️` seguita da due spazi `  ` nel pulsante Manutenzione creava uno spazio visivamente strano. Ridotto a un solo spazio.

---

## Music Cataloger Advanced — Upgrade Notes v1059
**Data:** 2026-03-25 · **Versione precedente:** v1058 · **File modificati:** `gui/main_window.py`, `core/cataloger.py`, `version.py`

---

## 1 — BUG FIX: Cover cache — `TclError: image "pyimage1" doesn't exist`

**File:** `gui/main_window.py`

**Root cause:** `CTkImage` è un oggetto tkinter e **deve essere creato nel thread principale**. Il vecchio codice creava `CTkImage` dentro il thread di download (secondario), poi lo passava a `root.after`. Tkinter non consente la creazione di risorse grafiche fuori dal main thread.

**Fix:** Separazione in due fasi:
1. **Thread secondario:** scarica l'immagine HTTP e la converte in `PIL.Image` (operazione I/O, può girare in qualsiasi thread)
2. **Main thread (via `root.after`):** la closure `_apply_cover(img=pil_img)` crea `CTkImage` e aggiorna il label — tutto nel thread principale

Ora la cover si carica correttamente ad ogni click senza eccezioni.

---

## 2 — BUG FIX: Tab Qualità — finestra progresso bloccata / non si chiude

**File:** `gui/main_window.py`

**Root cause:** `prog_win.grab_set()` cattura tutti gli eventi tkinter verso quella finestra — compresi i `root.after()` usati dal thread per aggiornare la UI e per chiamare `_quality_done`. Il risultato era un deadlock: la finestra aspettava il thread, il thread aspettava gli eventi → freeze permanente.

**Fix:**
- Rimosso `grab_set()` dalla finestra progresso
- Aggiunto `attributes("-topmost", True)` per tenerla visibile senza bloccare il loop eventi
- Il pulsante **Analizza** viene disabilitato all'avvio e riabilitato in `_quality_done` — impedisce doppi click senza bloccare la GUI
- `_quality_done` chiama `prog_win.destroy()` senza `grab_release()` (non necessario)

---

## 3 — BUG FIX: Generi esclusi non rispettati (Vocal, World, Tropical)

**File:** `core/cataloger.py`

**Root causes multipli:**

**Vocal/World — confronto case-sensitive:** il confronto `genre_check in self.excluded_genres` era case-sensitive. MusicBrainz restituisce `"Vocal"` ma il confronto falliva se il genere arrivava come `"vocal"`. Fix: `excluded_lower = {g.lower() for g in self.excluded_genres}`.

**Tropical — subfolder bypassa il filtro:** `Tropical` viene assegnato come `raw_genre="tropical"` con `genre="Latin"`. Il filtro controllava solo il campo `genre` ("Latin" non è escluso), ma poi `get_genre_folder_path` usava `raw_genre` per creare la sottocartella `Latin/Tropical/`. Fix: aggiunto secondo controllo su `raw_genre` — se il raw_genre è nella lista degli esclusi, viene azzerato al macrogenere padre.

**World come macrogenere:** `World` era nel `_PARENT_MAP` solo come destinazione per i suoi subgeneri, non come chiave. Aggiunto `"World": "World"` per gestire il caso in cui `World` stesso venga escluso.

**Flusso aggiornato in `_move_to_genre_folder`:**
```python
excluded_lower = {g.lower() for g in self.excluded_genres}
if genre.lower() in excluded_lower:
    genre = _get_parent_genre(genre)  # es. Vocal → Pop
elif raw_genre and raw_genre.lower() in excluded_lower:
    raw_genre = _get_parent_genre(raw_genre).lower()  # es. tropical → latin (no subfolder)
```

---

## 4 — FEAT: Tutte le finestre centrate a schermo

**File:** `gui/main_window.py`

Aggiunta funzione `_center_window(win, w, h)` globale e metodo `_center_win(win, w, h)` d'istanza. Applicati a:
- **Finestra principale** (1300×860, centrata al lancio)
- **Dialog "Catalogazione Completata"** (640×540)
- **Dialog "Analisi qualità"** (380×130)
- **Dialog "Duplicati trovati"** (620×440)
- **Dialog "About"** (440×320)

---

## 5 — FIX: Pannello sinistro — scrollbar dinamica al resize

**File:** `gui/main_window.py`

Ripristinato `CTkScrollableFrame` per il pannello sinistro con colori personalizzati per la scrollbar. Comportamento: la scrollbar è quasi invisibile quando il contenuto ci sta (finestra normale), appare automaticamente quando la finestra viene ridimensionata in altezza.

---

## 6 — FEAT: Tab Avanzate disabilitato durante la catalogazione

**File:** `gui/main_window.py`

Salvato il riferimento `self._adv_controls_frame = scroll` nel `_build_advanced_tab`. Il metodo `_set_controls_state()` ora greyout anche l'intero tab Avanzate durante il run (insieme al pannello sinistro), impedendo modifiche ai parametri mentre la catalogazione è in esecuzione.

---

## 7 — FEAT: "Mantieni questo" nel dialog duplicati

**File:** `gui/main_window.py`

Ogni percorso nella lista duplicati ha ora il pulsante **✓ Mantieni questo**. Al click:
1. Chiede conferma con askyesno
2. Elimina fisicamente gli altri file (`Path.unlink()`)
3. Rimuove le voci eliminate dal `music_library.json`
4. Salva il DB locale aggiornato
5. Sostituisce la riga con `✅ Mantenuto: percorso (N eliminati)`

---

## 8 — FEAT: Svuota Cache — conferma già presente (v1057)

La conferma `messagebox.askyesno("Conferma", "Svuotare la cache API?...")` era già implementata nel v1057. Verificato e confermato operativo.

---

## Note tecniche

**Aggiornamento DB locale per spostamenti manuali:** non implementato in questa versione. Per ora: se sposti un file manualmente, il DB locale conserva la posizione precedente. Al prossimo avvio di catalogazione, il file viene re-elaborato dalla posizione originale. Un futuro "Scansione collezione" leggerà le posizioni reali senza spostare file.

**Cache API:** la v1059 usa correttamente la cache — la seconda catalogazione nel log mostra `Cache caricata: 1335 metadati` e `Trovati 1 file MP3 da elaborare` (solo il file non catalogato). Questo è il comportamento atteso: i brani già elaborati vengono saltati grazie al DB locale.

---

## Music Cataloger Advanced — Upgrade Notes v1059
**Data:** 2026-03-25 · **Base:** v1058 stabile · **File modificati:** `gui/main_window.py`, `version.py`

---

## FIX CRITICO: `TclError: No more menus can be allocated`

**Unica modifica in questa versione.**

### Problema
L'app non si avviava con errore `No more menus can be allocated` — Windows limita i menu tkinter a 32 per processo.

Conteggio menu prima della fix:
- 5 menu dalla barra di sistema (File, Recenti, Strumenti, Help + root menu)
- 1 dropdown button recenti
- 7 × `CTkScrollableFrame` × 2 menu interni = **14 menu**
- 1 × `CTkOptionMenu` (tab Qualità) = **1 menu** ← il menu che fa superare il limite

**Totale: ~21 → in condizioni particolari raggiungeva 32 e crashava.**

### Fix — `CTkOptionMenu` → `CTkSegmentedButton`
Il selettore soglia kbps `128 / 192 / 256 / 320` è stato sostituito con `CTkSegmentedButton`:

- `CTkOptionMenu` crea internamente un `DropdownMenu` (= 1 menu tkinter)
- `CTkSegmentedButton` usa `CTkButton` normali — **zero menu tkinter interni**

Aggiunto anche flag `_quality_built` per evitare che `CTkSegmentedButton` chiami `_quality_filter()` durante la costruzione del tab (prima che i widget esistano).

### Cosa testare
1. L'app si avvia senza errori
2. Il tab **Qualità** mostra 4 pulsanti affiancati `128 | 192 | 256 | 320`
3. Cambiare la soglia filtra correttamente la lista
4. La catalogazione funziona normalmente

### Prossimo step (v1060)
Se v1059 è stabile → applicare il fix successivo dalla lista pending.

---

## Music Cataloger Advanced — Upgrade Notes v1059b (Hotfix)
**Data:** 2026-03-25 · **Versione precedente:** v1059 · **File modificati:** `gui/main_window.py`, `version.py`

---

## BUG CRITICO: `TclError: No more menus can be allocated`

**Errore:** L'applicazione non si avviava con il messaggio `No more menus can be allocated`.

**Root cause:** Windows ha un limite di **32 menu tkinter** per processo. Ogni `CTkScrollableFrame` crea internamente 2 menu (scrollbar orizzontale + verticale). Con 8 `CTkScrollableFrame` + 5 menu dalla menubar + 1 dal dropdown button = circa 21 menu. L'aggiunta del `CTkOptionMenu` nel tab Qualità (che crea anch'esso un `DropdownMenu` tkinter) ha portato il conteggio al limite, causando il crash durante la costruzione dell'interfaccia.

**Fix — due modifiche:**

### 1. `CTkOptionMenu` → `CTkSegmentedButton` (tab Qualità)
Il selettore soglia kbps è stato sostituito con `CTkSegmentedButton` che mostra le opzioni `128 | 192 | 256 | 320` come pulsanti affiancati. `CTkSegmentedButton` **non crea menu tkinter interni** (usa `CTkButton` normale), eliminando il problema.

### 2. Pannello sinistro → `CTkFrame` normale
Il pannello sinistro è stato riportato a `CTkFrame` con `grid_propagate(False)` per mantenere la larghezza fissa. `CTkScrollableFrame` creava 2 menu aggiuntivi non necessari — il pannello sinistro non ha contenuto sufficiente da richiedere scroll a dimensione normale.

**Conteggio menu dopo la fix:**
- 7 `CTkScrollableFrame` rimasti (necessari) × 2 = 14 menu
- 5 menu dalla menubar di sistema
- 1 dal dropdown button recenti
- **Totale: ~20 menu — ben entro il limite di 32**

---

## Music Cataloger Advanced — Upgrade Notes v1059c (Hotfix)
**Data:** 2026-03-25 · **Versione precedente:** v1059b · **File modificati:** `gui/main_window.py`, `version.py`

---

## Bug risolti

### 1 — Layout nero / finestre spot durante l'avvio

**Causa 1 — Doppio `geometry()`:**
`__init__` impostava `root.geometry("1300x860")` poi `main()` chiamava *di nuovo* `root.geometry("+X+Y")` con `update_idletasks()` nel mezzo. La seconda chiamata processava gli eventi in sospeso (trace, binding) e sovrascriveva il layout già costruito, rendendo il pannello sinistro nero.

**Fix:** rimosso il secondo centramento da `main()`. Il centramento ora avviene con `root.after(50, _center_main_window)` — **50ms dopo** che il layout è completamente costruito e reso. `main()` diventa semplicissimo:
```python
def main():
    root = ctk.CTk()
    app = MusicCatalogerGUI(root)
    root.mainloop()
```

**Causa 2 — `CTkSegmentedButton` chiama `command()` durante l'init:**
All'inizializzazione, `CTkSegmentedButton` chiama il suo `command` quando imposta il valore di default — questo triggherava `_quality_filter()` prima che `_quality_list` esistesse → frame fantasma.

**Causa 3 — `trace_add` su `_cache_search_var` chiama `_cache_filter()` durante init:**
Il trace veniva registrato prima che la lista cache fosse costruita → tentativo di accedere a widget non ancora esistenti.

**Fix per entrambe:** aggiunto flag di guard `_quality_built` e `_cache_built` — le callback ignorano le chiamate fino a quando i widget non sono completamente costruiti.

---

## Music Cataloger Advanced — Upgrade Notes v1060
**Data:** 2026-03-25 · **Base:** v1059 · **File modificati:** `gui/main_window.py`, `version.py`

---

## FIX: Cover cache — `TclError: image "pyimage1" doesn't exist`

**Unica modifica in questa versione.**

### Problema
Cliccando su una voce nel tab **Cache** per visualizzare la cover, in alcuni casi compariva l'errore:
```
TclError: image "pyimage1" doesn't exist
```
La cover non veniva visualizzata.

### Root cause
`CTkImage` internamente crea un `PhotoImage` tkinter. Su Windows, le risorse tkinter **devono essere create nel main thread**. Il vecchio codice creava `CTkImage` direttamente nel thread secondario di download:
```python
# SBAGLIATO — gira nel thread secondario
ctk_img = ctk.CTkImage(...)  # crea PhotoImage → crash su Windows
self.root.after(0, lambda i=ctk_img: label.configure(image=i))
```

### Fix — separazione in due fasi
```
Thread secondario:  HTTP download + PIL.Image.open() + resize()  ← solo I/O, sicuro
Main thread:        CTkImage(pil_img) + label.configure()        ← risorse tkinter
```
La closure `_apply()` viene schedulata via `root.after(0, _apply)` e gira nel main thread, dove la creazione di `CTkImage` è sicura.

### Cosa testare
1. Tab **Cache** → clicca su una voce con URL cover
2. La cover deve comparire senza errori
3. Cambiando selezione la cover si aggiorna correttamente

### Prossimo step (v1061)
Fix tab Qualità — finestra progresso bloccata (`grab_set` deadlock).

---

## Music Cataloger Advanced — Upgrade Notes v1060b
**Data:** 2026-03-25 · **Base:** v1060 · **File modificati:** `gui/main_window.py`, `version.py`

---

## FIX: Cover cache — `TclError: image "pyimage1" doesn't exist` dalla seconda cover in poi

**Unica modifica in questa versione.**

### Problema
La prima cover si caricava correttamente. Dalla seconda in poi, cliccando su un'altra voce nel tab Cache, compariva `TclError: image "pyimage1" doesn't exist`.

### Root cause
Quando `self._cover_image_ref` veniva sovrascritto con il nuovo `CTkImage`, Python distruggeva il vecchio oggetto tramite garbage collection — che a sua volta distruggeva il `PhotoImage` tkinter interno (`pyimage1`). Ma il `CTkLabel` tkinter referenziava ancora `pyimage1` internamente, e la successiva chiamata a `configure()` tentava di usare un'immagine già distrutta → TclError.

In più, il chiamante creava un thread esterno e `_cache_load_cover` ne creava un secondo interno — doppio thread inutile.

### Fix — tre interventi

**1. Token di versione:** ogni nuova richiesta cover incrementa `self._cover_token`. La closure `_apply` controlla che il token non sia cambiato prima di aggiornare il label — scarta silenziosamente le risposte di richieste superate (click veloci).

**2. Reset esplicito prima della nuova CTkImage:** il label viene azzerato con `image=None` e `self._cover_image_ref = None` **prima** di avviare il download. Questo rilascia il vecchio `PhotoImage` in modo controllato, con il label già scollegato da esso.

**3. Thread gestito internamente:** rimosso il thread esterno nel chiamante. `_cache_load_cover` ora crea e gestisce il proprio thread daemon direttamente.

### Cosa testare
1. Tab Cache → clicca su più voci in sequenza
2. Ogni cover deve aggiornarsi senza errori
3. Click rapidi su più voci non devono causare errori (il token scarta le risposte superate)

### Prossimo step (v1061)
Fix tab Qualità — finestra progresso bloccata (`grab_set` deadlock).

---

## Music Cataloger Advanced — Upgrade Notes v1060c
**Data:** 2026-03-25 · **Base:** v1060b · **File modificati:** `gui/main_window.py`, `version.py`

---

## FIX 1 — Crash all'avvio con dati cache di versioni precedenti

### Problema
L'app crashava all'avvio se `metadata_cache.json` conteneva voci in formato diverso
da quello atteso dalla versione corrente (dati prodotti da versioni precedenti).

### Principio: i dati NON vengono mai cancellati
Il fix non tocca i file su disco. `_cache_reload` ora filtra in memoria le voci
malformate (chiave non-stringa o valore non-dict/None) e le ignora silenziosamente,
mostrando un contatore: *"(N voci ignorate — formato non valido)"*.
I dati originali su `metadata_cache.json` rimangono intatti.

---

## FIX 2 — Cover cache: `TclError: image "pyimage1" doesn't exist`

### Root cause definitiva
`CTkImage` internamente assegna sempre il nome fisso `"pyimage1"` al suo
`PhotoImage` tkinter. Quando un vecchio `CTkImage` viene distrutto e ne viene
creato uno nuovo, tkinter tenta di registrare un nuovo widget con lo stesso nome
mentre quello vecchio non è ancora completamente deallocato → `TclError`.

### Fix definitivo — `ImageTk.PhotoImage` diretto
Sostituito `CTkImage` con `ImageTk.PhotoImage` (da `PIL`), che assegna nomi
**univoci e progressivi** automaticamente (`pyimage2`, `pyimage3`, ...) — nessun
conflitto di nomi.

Il `CTkLabel` interno viene aggiornato accedendo al widget tkinter nativo
`._label` e impostando direttamente `.image` e `.configure(image=...)` — questo
è il modo corretto per usare `PhotoImage` con un widget tkinter:

```python
photo = ImageTk.PhotoImage(pil_img)   # nome univoco automatico
self._cover_image_ref = photo          # mantiene in vita il PhotoImage (no GC)
lbl = self._cache_cover_label._label   # widget tkinter nativo interno
lbl.configure(image=photo, text="")
lbl.image = photo                      # doppio riferimento di sicurezza
```

Aggiunto anche **token di versione**: click rapidi su più voci non si accumulano,
le risposte di richieste superate vengono scartate silenziosamente.

### Cosa testare
1. Avvio con dati cache esistenti → nessun crash
2. Tab Cache → clicca più voci in sequenza → cover si aggiorna senza errori CMD
3. Click rapidi su voci diverse → nessun errore, mostra l'ultima selezionata

---

## Music Cataloger Advanced — Upgrade Notes v1061
**Data:** 2026-03-25 · **Base:** v1060c · **File modificati:** `gui/main_window.py`, `version.py`

---

## FIX 1 — Tab Qualità: finestra progresso bloccata / non si chiude

### Problema
Cliccando **Analizza** nel tab Qualità, la finestra di progresso si apriva ma si bloccava
e non si chiudeva mai al termine della scansione.

### Root cause
`prog_win.grab_set()` cattura **tutti** gli eventi tkinter verso quella finestra —
compresi i `root.after()` inviati dal thread di scansione per aggiornare la label
e chiamare `_quality_done`. Il risultato era un deadlock: la finestra aspettava
il thread, il thread aspettava gli eventi → freeze permanente.

### Fix
- Rimosso `grab_set()` e `grab_release()`
- Aggiunto `attributes("-topmost", True)` — la finestra resta in primo piano senza bloccare il loop eventi
- Il pulsante **Analizza** viene disabilitato all'avvio e riabilitato in `_quality_done` — impedisce doppi click senza bloccare la GUI

### Cosa testare
1. Tab Qualità → click **Analizza**
2. La finestra di progresso appare e si aggiorna
3. Al termine si chiude automaticamente e mostra i risultati
4. Il pulsante Analizza si riabilita

---

## FIX 2 — Tab Cache: cover precedente visibile in background su voci senza immagine

### Problema
Selezionando una voce senza cover dopo una con cover, l'immagine precedente
rimaneva visibile in background con l'emoji 🎵 sovrapposta.

### Root cause
`_cache_load_cover` aggiorna il widget tkinter nativo `._label` direttamente
con `PhotoImage`. Il ramo `else` (nessuna cover) usava solo `CTkLabel.configure()`
che non resetta `._label.image` — il `PhotoImage` precedente rimaneva agganciato
al widget tkinter interno.

### Fix
Il ramo `else` ora resetta esplicitamente `._label` con `image=""` e `lbl.image = None`,
cancellando completamente la cover precedente prima di mostrare l'emoji.

---

## Music Cataloger Advanced — Upgrade Notes v1061b
**Data:** 2026-03-25 · **Base:** v1061 · **File modificati:** `gui/main_window.py`, `version.py`

---

## BUG CRITICO: Analisi qualità non terminava mai + scrollbar fuori posto

### Causa root
Nel file erano presenti **tre metodi duplicati** lasciati da un merge parziale di versioni precedenti:
- `_quality_scan_thread` (due versioni: v1057 e v1056)
- `_quality_done` (due versioni)
- `_quality_filter` (due versioni)

Python esegue solo l'**ultima** definizione di un metodo nella classe. La seconda versione di `_quality_filter` conteneva:
1. Un `w.destroy()` errato dentro il loop dei risultati
2. Un `threading.Thread(...).start()` alla fine — che **rilanciava il thread di scansione all'infinito**
3. `_quality_done` senza `prog_win.destroy()` — la finestra progresso non veniva mai chiusa

La `prog_win` (CTkToplevel) non distrutta causava anche il problema delle **scrollbar fuori posto**: tkinter associava le scrollbar dei CTkScrollableFrame alla finestra orfana invece che alla finestra principale, facendole apparire in una finestra separata sul bordo dello schermo.

### Fix
- Rimosso il blocco duplicato (~4700 caratteri) — rimane solo la versione corretta v1057 con progresso e `prog_win.destroy()`
- Abilitata la **chiusura manuale** della finestra progresso tramite la X: interrompe l'analisi in corso, riabilita il pulsante Analizza e mostra "Analisi interrotta"

### Cosa testare
1. Tab Qualità → click **Analizza**
2. La finestra di progresso mostra l'avanzamento e si chiude automaticamente al termine
3. I risultati compaiono nella lista
4. Cliccando la X della finestra progresso l'analisi si interrompe correttamente
5. Le scrollbar dei tab rimangono nelle posizioni corrette dopo l'analisi

---

## Music Cataloger Advanced — Upgrade Notes v1062
**Data:** 2026-03-25 · **Base:** v1061b · **File modificati:** `gui/main_window.py`, `version.py`

---

## FIX 1 — Tab Qualità: voci vuote/troncate nella lista risultati

### Causa root
`_quality_filter` usava `pack()` per popolare il `CTkScrollableFrame`. Con molte
righe (100+), `pack()` non aggiorna correttamente il canvas interno del frame
scrollabile — le ultime righe risultavano troncate o completamente vuote.

### Fix
Sostituito `pack()` con `grid(row=idx, column=0, sticky="ew")` — il layout manager
corretto per `CTkScrollableFrame`. Aggiunto `columnconfigure(0, weight=1)` per
garantire che le righe si espandano orizzontalmente.

Aggiunto anche **rendering a batch**: le righe vengono create 50 alla volta tramite
`root.after(10, ...)` — la GUI rimane reattiva durante il popolamento di liste
lunghe (es. 275+ voci al filtro 256kbps) senza freeze visibili.

---

## FEAT — Salvataggio analisi qualità in `data/quality_analysis.json`

### Comportamento
Dopo ogni analisi, i risultati vengono salvati in `data/quality_analysis.json`
con struttura:
```json
{
  "base_path": "C:/Users/.../Musica",
  "total": 1241,
  "results": [["nomefile.mp3", 128, "Latin/Salsa"], ...]
}
```

Al click successivo di **Analizza**:
- Se esiste un'analisi salvata **per la stessa directory**, viene caricata
  istantaneamente senza rileggere i file (indicato da `⚡ Risultati da cache`)
- Se la directory è diversa, oppure il file non esiste, riparte l'analisi completa
  e sovrascrive il file

### Cambiare soglia (128 / 192 / 256 / 320)
Il cambio soglia ora è **sempre istantaneo** — filtra i risultati già in memoria
senza nessuna nuova analisi in background.

### Invalidare la cache manualmente
Per forzare una nuova analisi (es. dopo aver aggiunto file), basta cliccare
**Analizza** di nuovo — se i risultati vengono dalla cache l'app lo indica
con `⚡`. Per forzare la riscansione, si può eliminare `data/quality_analysis.json`.
In futuro si potrà aggiungere un pulsante "🔄 Riscansiona".

---

## Music Cataloger Advanced — Upgrade Notes v1063
**Data:** 2026-03-25 · **Base:** v1062 · **File modificati:** `gui/main_window.py`, `version.py`

---

## FIX 1 — Filtro soglia 192/256/320 mostra lista vuota o parziale

### Causa root
`_quality_filter` avviava batch con `root.after(10)` ma quando l'utente cambiava soglia rapidamente, il vecchio batch continuava a girare mentre quello nuovo aveva già svuotato il `CTkScrollableFrame`. Le righe del vecchio batch venivano aggiunte su widget già distrutti → righe fantasma o lista vuota.

### Fix — Token di cancellazione
Ogni chiamata a `_quality_filter` incrementa `_filter_token`. Ogni batch controlla il token prima di aggiungere righe — se il token è cambiato (soglia cambiata) il batch si ferma immediatamente. Controllo anche a metà batch per interrompere loop lunghi.

---

## FIX 2 — Scrollbar decontestualizzate dopo analisi qualità

### Causa root
`prog_win.destroy()` in `_quality_done` veniva chiamato ma tkinter non completava il flush della finestra prima di aggiornare i `CTkScrollableFrame`. Le scrollbar venivano temporaneamente associate alla finestra in fase di chiusura.

### Fix
Aggiunto `self.root.update_idletasks()` subito dopo `prog_win.destroy()` — forza tkinter a completare la distruzione della finestra prima di procedere con il rendering della lista.

---

## FIX 3 — Spinner durante il rendering

Mentre la lista viene popolata (può richiedere qualche secondo per 1000+ righe), viene mostrato **⏳ Caricamento lista...** al posto della lista vuota, così l'utente sa che il programma sta lavorando.

---

## FEAT — Pulsante 🔄 Riscansiona

Appare nella toolbar del tab Qualità solo quando i dati vengono caricati dalla cache (dopo la prima analisi). Permette di forzare una nuova scansione dei file ignorando la cache salvata — utile quando si aggiungono nuovi file alla collezione.

Workflow:
- Prima apertura → click **Analizza** → analisi completa → risultati salvati in `data/quality_analysis.json`
- Riapertura → click **Analizza** → carica dalla cache istantaneamente → appare **⚡ Cache — N file totali** + pulsante **🔄 Riscansiona**
- Click **🔄 Riscansiona** → cancella la cache → riparte l'analisi completa

---

## Note sul comportamento atteso

**Il cambio soglia non rianalizza nulla** — filtra i dati già in memoria. La lentezza osservata è solo il tempo di rendering dei widget tkinter (100-200ms per 1000+ righe). Con il token di cancellazione, cambi rapidi di soglia non causano più sovrapposizioni.

**Finestre spot al riavvio** — se si verificano ancora finestre spot al primo avvio dopo il riavvio del programma, probabilmente derivano da `CTkToplevel` legato alla `prog_win` di una sessione precedente non completamente distrutta. Il `update_idletasks()` in `_quality_done` dovrebbe risolvere anche questo.

---

## Music Cataloger Advanced — Upgrade Notes v1064
**Data:** 2026-03-26 · **Base:** v1063 · **File modificati:** `gui/main_window.py`, `version.py`

---

## FIX CRITICO — Tab Qualità: freeze e righe vuote per 192/256/320 kbps

### Causa root
Creare N×5 widget tkinter (CTkFrame + 4×CTkLabel) per ogni riga blocca il main thread. Con 170 righe (soglia 192) = 850 widget, con 1241 righe (soglia 320) = 6205 widget. Tkinter non può fare altro mentre li crea tutti → freeze, "Non risponde", righe vuote.

### Fix — Canvas virtuale istantaneo
Sostituito completamente il rendering a widget con un **Canvas tkinter** che disegna il testo direttamente tramite `create_text()` e `create_rectangle()`. Il Canvas è un'operazione nativa e **disegna 1241 righe in meno di 50ms** senza bloccare il main thread.

- Nessun widget creato per ogni riga — solo primitive Canvas
- Scroll nativo del Canvas integrato nel CTkScrollableFrame
- Colori differenziati per qualità: 🔴 <160kbps / 🟡 160-255kbps / 🟢 256+kbps
- Cambio soglia istantaneo anche per 1241 righe

---

## FIX — Ricerca DB Locale e Cache: debounce 600ms

La ricerca in tempo reale ad ogni tasto causava freeze perché rieseguiva il filtro su migliaia di voci ad ogni carattere digitato.

**Fix:** aggiunto debounce di **600ms** — la ricerca parte solo 600ms dopo l'ultima lettera digitata. Se l'utente continua a digitare, il timer si resetta. L'icona 🔍 nel placeholder indica visivamente che si tratta di una barra di ricerca.

---

## FIX — Toolbar Qualità: layout e pulsante Riscansiona

- Il pulsante **Riscansiona** ora mostra solo il testo "Riscansiona" senza icona ambigua
- L'etichetta "Soglia:" è allineata direttamente accanto ai pulsanti di soglia
- Il pulsante Riscansiona rimane nascosto finché non esiste una cache — appare solo dopo la prima analisi

---

## Note sul Canvas virtuale

Il Canvas disegna testo con il font di sistema, non con il rendering CustomTkinter. L'aspetto è leggermente diverso dal resto dell'UI (nessun bordo arrotondato per riga) ma la performance è incomparabile — istantanea per qualsiasi numero di righe. Se in futuro si vuole tornare ai widget CTkFrame per le righe, occorrerà limitare il numero massimo di file mostrati (es. max 200).

---

## Music Cataloger Advanced — Upgrade Notes v1064b (Hotfix)
**Data:** 2026-03-26 · **Base:** v1064 · **File modificati:** `gui/main_window.py`, `version.py`

---

## FIX 1 — `AttributeError: '_cache_built'`

Il flag `_cache_built` era referenziato nel debounce della ricerca Cache ma non inizializzato. Fix: sostituito con `getattr(self, "_cache_built", False)` — sicuro anche se l'attributo non esiste.

---

## FIX 2 — Tab Qualità: `AttributeError: '_scrollable_frame'`

Il Canvas virtuale di v1064 dipendeva dall'attributo privato `._scrollable_frame` di `CTkScrollableFrame`, che non esiste in tutte le versioni di CustomTkinter.

**Soluzione definitiva: `ttk.Treeview` nativo**

Sostituito Canvas con `ttk.Treeview` (widget C nativo di tkinter):
- Inserisce 1241 righe in modo istantaneo — è implementato in C, non in Python
- Non dipende da nessun attributo privato di CustomTkinter
- Scroll integrato con scrollbar verticale stilizzata
- Colori per qualità: 🔴 <160 kbps / 🟡 160–255 kbps / 🟢 256+ kbps
- Sfondo alternato riga pari/dispari

---

## FEAT — Spinner ⏳ durante caricamento DB Locale e Cache

Mentre i dati vengono filtrati e i widget creati, appare brevemente `⏳ Caricamento...` al posto della lista vuota, così l'utente sa che il programma sta lavorando.

---

## Music Cataloger Advanced — Upgrade Notes v1064d
**Data:** 2026-03-26 · **Base:** v1064c · **File modificati:** `gui/main_window.py`, `version.py`

---

## FIX 1 — Soglia default 320 kbps
Il selettore soglia ora parte da **320** invece di 192 — mostra tutta la collezione di default.

---

## FIX 2 — Soglia e pulsanti spostati a destra
La label "Soglia:" e i pulsanti `128 | 192 | 256 | 320` sono ora allineati a destra della toolbar tramite `columnconfigure(1, weight=1)` che funge da spacer elastico.

---

## FIX 3 — Colorazione semaforo (solo ● e voce qualità, non tutta la riga)
La riga ora ha sfondo neutro alternato. Solo la colonna **Qualità** è colorata con il simbolo ●:

| Colore | Simbolo | Soglia |
|--------|---------|--------|
| 🔴 Rosso | ● Scarsa | < 160 kbps |
| 🟡 Arancio | ● Media | 160–255 kbps |
| 🟢 Verde | ● Buona | 256–319 kbps |
| 💎 Blu | ● Alta | 320 kbps |

---

## FIX 4 — Intestazione statica rimossa + spaziatura aumentata
Rimossa la vecchia intestazione `CTkFrame` statica che si sovrapponeva alla Treeview. Le intestazioni sono ora integrate nella Treeview (`File / kbps / Qualità / Cartella`). Altezza riga aumentata da 26 a **30px** per migliore leggibilità.

---

## Music Cataloger Advanced — Upgrade Notes v1065
**Data:** 2026-03-26 · **Base:** v1064d · **File modificati:** `gui/main_window.py`, `core/cataloger.py`, `version.py`

---

## FIX 1 — Tab Qualità: intestazione doppia rimossa

Il vecchio `CTkFrame` statico con "File / kbps / Qualità / Cartella" è stato rimosso.
Le intestazioni sono ora integrate nella Treeview (`show="headings"`) con spaziatura interna aumentata (`padding=(8, 7)`).

---

## FIX 2 — Tab Qualità: colori semaforo SOLO nella colonna Qualità

La riga ha sfondo neutro alternato. Solo la colonna **Qualità** mostra il ● colorato:
- 🔴 `● Scarsa` — rosso `#cc4444` — < 160 kbps
- 🟡 `● Media` — arancio `#e0a030` — 160–255 kbps
- 🟢 `● Buona` — verde `#50aa70` — 256–319 kbps
- 💎 `● Alta` — blu `#4db8ff` — 320 kbps

Implementato con tag per-`iid` post-insert che sovrascrive il foreground solo per quella voce.

---

## FIX 3 — Tab Qualità: layout toolbar

- Pulsante **↺ Riscansiona** rimane a sinistra vicino ad Analizza
- Etichetta `⚡ Risultati da cache — N file analizzati` appare nella parte centrale della toolbar quando i dati vengono caricati dalla cache
- "Soglia:" e `128|192|256|320` sono a destra
- Nessuna doppia riga — tutto in una singola riga orizzontale

---

## FIX 4 — Tab Avanzate disabilitato durante run

Il tab Avanzate (CTkScrollableFrame) viene disabilitato insieme al pannello sinistro durante la catalogazione, e riabilitato al termine. Riferimento salvato in `self._adv_controls_frame`.

---

## FIX 5 — Centramento finestra principale

La finestra principale viene centrata a schermo tramite `root.after(80, _center_main_window)` — 80ms dopo che il layout è completamente costruito e renderizzato, evitando il problema del layout nero.

---

## FIX 6 — Generi esclusi: Vocal, World, Tropical (`cataloger.py`)

Tre root cause risolte:
- **Case-sensitive**: confronto ora case-insensitive (`excluded_lower = {g.lower() for g in ...}`)
- **Tropical bypass**: `raw_genre="tropical"` creava `Latin/Tropical/` anche se escluso — ora viene controllato anche `raw_genre`
- **World mancante**: aggiunto `"World": "World"` in `_get_parent_genre`

---

## Music Cataloger Advanced — Upgrade Notes v1066
**Data:** 2026-03-31 · **Base:** v1065b · **File modificati:** `gui/main_window.py`, `version.py`

---

## FIX 1 — Colonna Qualità: emoji semaforo corrette, colore solo nel testo

Il tag per-`iid` della versione precedente colorava **tutta la riga** (comportamento ttk) invece di solo la cella.

**Soluzione definitiva:** tag di riga solo per sfondo alternato (neutro), il colore compare esclusivamente come emoji Unicode nel **testo** della colonna Qualità:
- 🔴 `Scarsa` — < 160 kbps
- 🟡 `Media` — 160–255 kbps
- 🟢 `Buona` — 256–319 kbps
- 💎 `Alta` — 320 kbps

Ripristinate le emoji originali richieste (erano state sostituite con ● in v1064d).

---

## FIX 2 — Header Treeview: nessuna sottolineatura al hover

`style.map("Q.Treeview.Heading", ...)` ora fissa `background`, `foreground` e `relief` anche nello stato `"active"` — il passaggio del cursore sull'intestazione non produce più effetti visivi.

---

## FIX 3 — Scrollbar Treeview moderna

Scrollbar verticale stilizzata con `Q.Vertical.TScrollbar`: sottile (8px), colore tema dark, senza bordi, con hover leggermente più chiaro.

---

## FIX 4 — Etichetta `⚡ Risultati da cache`

La label appare nella parte sinistra della toolbar (dopo il contatore) quando i dati vengono caricati dalla cache. Il colore è ora giallo-oro `#f0c040` per maggiore visibilità.

---

## INFO — Generi esclusi (Vocal, World, Tropical)

**Il fix nel cataloger funziona correttamente** — il parametro `--excluded-genres` viene passato solo se i generi sono stati **deselezionati** nel tab Generi e **salvati** con il pulsante 💾 Salva.

Nel log riportato, il comando era `--cleanup --duplicate-action keep_both --cover ...` **senza** `--excluded-genres` — questo significa che nel tab Generi, Vocal, World e Tropical erano ancora selezionati (stato default = attivo).

**Come escludere un genere:**
1. Vai al tab **🎵 Generi**
2. Deseleziona il genere (es. Vocal, World, Tropical)
3. Clicca **💾 Salva**
4. Alla prossima catalogazione i file di quel genere verranno spostati nel macrogenere padre (es. Vocal → Pop, World → World/Unknown, Tropical → Latin)

Aggiunta nota esplicativa `💡 Deseleziona i generi da escludere dalla catalogazione, poi clicca Salva` nella toolbar del tab Generi.

---

## Music Cataloger Advanced — Upgrade Notes v1067
**Data:** 2026-03-31 · **Base:** v1066 · **File modificati:** `gui/main_window.py`, `version.py`

---

## BUG CRITICO FIX — Generi esclusi non venivano letti correttamente

### Root cause
In `_build_cmd`, la chiave per cercare le preferenze veniva costruita **con l'emoji**:
```
"🎤  Pop & R&B::Vocal"   ← sbagliato (include emoji)
```
Ma le preferenze vengono **salvate senza emoji**:
```
"Pop & R&B::Vocal"        ← corretto (salvato da _save_genre_prefs)
```
Il risultato: `self._genre_prefs.get("🎤  Pop & R&B::Vocal", True)` restituiva sempre `True` (default) — tutti i generi sembravano attivi, nessun genere veniva mai escluso.

### Fix
```python
mk = macro_key_full.split("  ", 1)[-1].strip()  # strip emoji
pref_key = f"{mk}::{sub}"                        # "Pop & R&B::Vocal" ✓
```

---

## FIX — Colonna Qualità: colore visibile

I tag `q_scarsa/q_media/q_buona/q_alta` vengono applicati **dopo** il tag di sfondo riga tramite `tree.item(iid, tags=(bg_tag, f"q_{qlevel}"))` — il secondo tag sovrascrive il foreground del primo. Il testo della colonna mostra l'emoji e il colore qualità: 🔴🟡🟢💎

---

## FIX — Etichetta ⚡ `Risultati da cache`

Contatore e label ⚡ erano entrambi su `column=2` con sticky diversi — si sovrascrivevano. Ora sono in un `CTkFrame` intermedio con layout interno, visibili contemporaneamente.

---

## FIX — Scrollbar Treeview più coerente con il tema

Colori aggiornati per essere più vicini allo stile dark dell'app. Larghezza 10px.

---

## FIX — Header Treeview: spaziatura interna ripristinata

`padding=(10, 8)` ripristinato nelle intestazioni della Treeview.

---

## Music Cataloger Advanced — Upgrade Notes v1067b
**Data:** 2026-03-31 · **Base:** v1067 · **File modificati:** `gui/main_window.py`, `core/cataloger.py`, `version.py`

---

## FIX 1 — World escluso va in `Uncategorized/` invece che in `World/`

**File:** `core/cataloger.py`

Se un genere escluso è anche il suo stesso macrogenere (es. `World → World`, `Folk → World`), il cataloger non aveva dove spostare il file e lo rimetteva nella stessa cartella. Ora:
- Genere escluso con macrogenere diverso → spostato nel macrogenere (es. `Vocal → Pop` ✓)
- Genere escluso che è il proprio macrogenere → spostato in `Uncategorized/` ✓

---

## FIX 2 — Colonna Qualità: colorazione reale per-cella

**File:** `gui/main_window.py`

`ttk.Treeview` su Windows non supporta `foreground` per singola cella — i tag colorano sempre tutta la riga, indipendentemente da come vengono configurati.

**Soluzione definitiva:** sostituito `Treeview` con `tk.Text` in modalità read-only. `tk.Text` supporta tag per-carattere nativamente → il colore 🔴🟡🟢💎 appare **solo** nella colonna Qualità, il resto della riga rimane nel colore neutro.

Vantaggi:
- Colorazione reale per-cella su tutti i sistemi operativi
- Scroll integrato con scrollbar tkinter nativa coerente con il tema
- Velocità identica (nessun widget per riga)

---

## FIX 3 — ⚡ `Risultati da cache` visibile

La label ⚡ era nascosta perché il layout la posizionava fuori dalla zona visibile. Ora il messaggio `⚡ Risultati da cache — N file analizzati` appare direttamente nella variabile del contatore risultati, garantendo visibilità totale.

---

## FIX 4 — Scrollbar coerente con il tema

La scrollbar del pannello Qualità usa ora la stessa scrollbar tkinter nativa dei `CTkScrollableFrame` degli altri tab — stesso colore dark, stessa larghezza 10px.

---

## Music Cataloger Advanced — Upgrade Notes v1068
**Data:** 2026-03-31 · **Base:** v1067b · **File modificati:** `gui/main_window.py`, `core/cataloger.py`, `version.py`

---

## FIX 1 — Tab Qualità: colonne allineate con tabstop

Il `tk.Text` ora usa `tabs=("590p", "660p", "810p")` per allineare le colonne a posizioni fisse in pixel. Ogni riga ha struttura: `File \t kbps \t Qualità \t Cartella`. L'header usa coordinate `place()` per allinearsi agli stessi pixel. La colonna Qualità mostra 🔴🟡🟢💎 colorati, le altre colonne restano in colore neutro.

## FIX 2 — Tab Qualità: scrollbar identica agli altri tab

Sostituita la scrollbar tkinter custom con `CTkScrollableFrame` — la stessa usata nel tab DB Locale e Cache. La scrollbar è ora visivamente identica.

## FIX 3 — Generi esclusi senza macrogenere alternativo (`cataloger.py`)

Se un genere escluso è il proprio macrogenere (es. `World → World`), il file non viene più spostato in `Uncategorized/` ma viene **lasciato nella directory radice** come file non catalogato, che l'utente gestirà manualmente.

## FIX 4 — Toolbar DB Locale e Cache: compatta su singola riga

Il pulsante Ricarica diventa un'icona `🔄` compatta (36px), la barra di ricerca si espande sull'intera larghezza disponibile, contatore e Svuota (solo Cache) a destra. Tutta la toolbar occupa una sola riga di ~34px invece di due righe separate, liberando spazio per la tabella.

## FIX 5 — Esportazione CSV: separatore `;`

Il `csv.writer` usa ora `delimiter=";"` — i dati vengono divisi correttamente in colonne quando il CSV viene aperto in Excel o LibreOffice Calc.

## FIX 6 — Duplicati: pulsante "✓ Mantieni questo"

Il dialog duplicati ora mostra per ogni percorso il pulsante **✓ Mantieni questo** che:
1. Chiede conferma
2. Elimina fisicamente gli altri file
3. Rimuove le voci eliminate da `music_library.json`
4. Salva il DB aggiornato
5. Sostituisce il gruppo con `✅ Mantenuto: percorso (N eliminati)`

## FIX 7 — Cover Cache: no resize layout al cambio voce

La cover è ora in un `CTkFrame` con `pack_propagate(False)` e dimensione fissa 200×200. Cambiando da una voce con cover a una senza cover (e viceversa), il layout del pannello destro non si ridimensiona più.

## FIX 8 — Dialog centrati a schermo

- Dialog analisi qualità centrato con `_center_win(prog_win, 400, 140)`
- Dialog duplicati centrato con `_center_win(win, 660, 500)`

---

## Music Cataloger Advanced — Upgrade Notes v1068b
**Data:** 2026-03-31 · **Base:** v1068 · **File modificati:** `gui/main_window.py`, `version.py`

---

## BUG CRITICO FIX — `AttributeError: 'MusicCatalogerGUI' object has no attribute '_center_win'`

Il metodo `_center_win(win, w, h)` era andato perso in un merge. Aggiunto prima di `_center_main_window`. Risolve:
- Crash all'apertura del dialog **Duplicati trovati**
- Crash all'apertura della finestra progresso **Analisi qualità**

---

## FIX — CSV: colonne ricche con dati da metadata_cache

Il CSV esportato ora include queste colonne separate da `;`:

| File | Titolo | Artista | Album | Anno | Genere | Sottogenere | BPM | Qualità (kbps) | Catalogato il |
|------|--------|---------|-------|------|--------|-------------|-----|----------------|---------------|

- **File**: solo nome file con estensione (non il percorso completo)
- **Sottogenere**: se uguale al genere → `-`
- I dati Titolo/Artista/Album/Anno vengono arricchiti dalla `metadata_cache.json` se disponibili

---

## FIX — Tab Cache: toolbar compatta (rowconfigure corretto)

Il tab Cache ora ha `rowconfigure(0, weight=0)` per la toolbar e `rowconfigure(1, weight=1)` per il contenuto — la toolbar occupa solo lo spazio necessario e tutta l'altezza rimanente va alla lista.

---

## FEAT — Breadcrumb percorso stile Windows Explorer

Il campo percorso nel pannello sinistro mostra il percorso in stile `  Desktop  ›  Pedro  ›  Musica` invece del percorso completo. Si aggiorna automaticamente ogni volta che si seleziona una directory (da Sfoglia o dalle directory recenti).

---

## FEAT — Filtri livello log nel tab Log

Sopra il log appaiono tre checkbox attivabili/disattivabili:
- **INFO** (azzurro) — messaggi informativi standard
- **WARNING** (arancio) — avvertimenti
- **ERROR** (rosso) — errori

Il filtro è **additivo**: attivando più livelli si vedono tutti insieme. Disattivando un livello, le righe di quel tipo spariscono. Il log viene ricostruito in tempo reale al cambio.

---

## FEAT — Rinomina File Automatico (tab Avanzate)

Nuova sezione **✏️ Rinomina File Automatico** nel tab Avanzate (default: disabilitato). Quando abilitato, i file MP3 vengono rinominati durante la catalogazione secondo il pattern scelto:
- `artista - titolo.mp3` (default)
- `titolo - artista.mp3`

Il parametro `--rename-pattern` viene passato al cataloger. **Nota:** il cataloger deve supportare `--rename-pattern` per applicarlo — questa versione aggiunge solo l'interfaccia grafica.

---

## Music Cataloger Advanced — Upgrade Notes v1069
**Data:** 2026-04-01 · **Base:** v1068b · **File modificati:** `gui/main_window.py`, `core/cataloger.py`, `run_cataloger.py`, `version.py`

---

## BUG FIX 1 — `TclError: unknown option "0.0": must be moveto or scroll`

**File:** `gui/main_window.py`

Il `tk.Text` nel tab Qualità usava `yscrollcommand=sf._parent_canvas.yview` — questo passava direttamente i valori float della scrollbar al metodo `.yview()` del canvas, che invece si aspetta i comandi `moveto` o `scroll`. Rimosso il collegamento errato — il `CTkScrollableFrame` gestisce il proprio scroll internamente.

---

## BUG FIX 2 — `AttributeError: '_cache_info_var'` allo svuotamento cache

**File:** `gui/main_window.py`

`_clear_cache` chiamava `_refresh_cache_info()` che usa `_cache_info_var`, ma questo attributo non viene sempre inizializzato (dipende dal tab Avanzate). Aggiunto `hasattr` guard + chiamata a `_cache_reload()` per aggiornare la lista dopo lo svuotamento.

---

## BUG FIX 3 — Tropical persiste nonostante deselezionato

**File:** `core/cataloger.py`

Il fix precedente impostava `raw_genre = parent.lower()` (es. `"latin"`) dopo aver escluso Tropical. Ma `"latin"` è ancora un subgenere latino riconosciuto da `get_genre_folder_path`, che creava comunque `Latin/Latin/`. Fix: quando un subfolder è escluso, imposta `genre = parent` e `raw_genre = ""` — stringa vuota = nessuna subfolder.

---

## BUG FIX 4 — Filtri log: deselezionando e riselezionando si perde il log

**File:** `gui/main_window.py`

`_log_all_lines` e `_log_filter` non erano inizializzati in `__init__` ma solo in `_build_layout`. Se si deselezionava un filtro prima che fossero pronti, il buffer non veniva mai popolato. Inizializzazione spostata in `__init__` e aggiunto `getattr` sicuro ovunque.

---

## EVOLUTIVA — Paginazione DB Locale (100 record per pagina)

**File:** `gui/main_window.py`

Il DB Locale ora mostra 100 record per volta con navigazione **◀ Prec** / **Succ ▶**. Il contatore mostra `N record • pag. X/Y (start-end)`. La paginazione evita il freeze con 500+ righe. La ricerca filtra tutti i record prima di paginare.

---

## EVOLUTIVA — Rinomina file automatico (completa implementazione)

**File:** `core/cataloger.py`, `run_cataloger.py`

Il parametro `--rename-pattern` è ora implementato end-to-end:
- `run_cataloger.py`: accetta `--rename-pattern "{artist} - {title}"` o `"{title} - {artist}"`
- `cataloger.py`: dopo lo spostamento del file, recupera i metadati dalla cache e rinomina il file usando il pattern scelto
- Caratteri non validi per il filesystem vengono rimossi automaticamente
- Se la rinomina fallisce (metadati mancanti o errore), il file rimane con il nome originale e viene loggato un warning

Attivazione: tab **Avanzate → ✏️ Rinomina File Automatico** → abilita + scegli pattern.

---

## FIX — Dimensioni tab Qualità proporzionali alla finestra

L'header usa ora `grid()` con `columnconfigure(weight=...)` invece di `place()` con coordinate fisse — le colonne si adattano alla larghezza della finestra.

---

## Music Cataloger Advanced — Upgrade Notes v1069b
**Data:** 2026-04-01 · **Base:** v1069 · **File modificati:** `gui/main_window.py`, `version.py`

---

## BUG CRITICO — `No more menus can be allocated` (limite 32 menu Windows)

### Root cause
Ogni `CTkScrollableFrame` alloca **2 menu tkinter interni** su Windows. Con 7 scrollframe nell'app:
- 7 × 2 = 14 menu da CTkScrollableFrame
- 5 menu dalla menubar
- Il dropdown "Recenti" **creava un nuovo `tk.Menu` ad ogni click** → dopo ~13 click si raggiungeva il limite

### Fix
1. **Menu recenti**: sostituito con un **menu persistente** creato una volta sola e svuotato/ripopolato ad ogni apertura — non accumula più menu
2. **Tab Cache**: rimosso `CTkScrollableFrame`, sostituito con `tk.Canvas + Scrollbar` (0 menu extra)
3. **Dialog Duplicati**: rimosso `CTkScrollableFrame`, sostituito con `tk.Canvas + Scrollbar`
4. **Dialog Orfani**: rimosso `CTkScrollableFrame`, sostituito con `tk.Canvas + Scrollbar`

**Conteggio finale**: 3 × 2 = 6 + 5 menubar + 1 recenti = **12 totali** — ben sotto il limite 32.

---

## BUG — Tab Cache: header mancante e layout rotto

**Root cause**: il cambio `rowconfigure` aveva spostato header e lista sullo stesso `row=1`, con `pady=(30,8)` sulla lista che la abbassava di 30px sovrascrivendo l'header.

**Fix**: riscritto completamente `_build_cache_tab` con layout a 3 livelli:
- `row=0` → toolbar compatta
- `row=1, col=0` → frame sinistra che contiene: header fisso (row=0) + lista scrollabile (row=1)
- `row=1, col=1` → pannello dettaglio/cover

---

## BUG — Tab Qualità: colonne non allineate

**Fix definitivo**: abbandonato `tk.Text` con tabstop (non funziona su Windows con font non monospace). Sostituito con lo stesso approccio del tab DB Locale: **widget `CTkFrame` per riga, paginazione 100 record con ◀/▶**. Il colore semaforo 🔴🟡🟢💎 viene applicato come `text_color` sul singolo `CTkLabel` della colonna Qualità — funziona su tutti i sistemi.

---

## BUG — DB Locale: header tabella mancante

Aggiunto header fisso con colonne File / Genere / Subgenere / Catalogato visibile sulla prima pagina.

---

## FIX — Salsa Romantica rimossa dal GENRE_TREE

`"Salsa Romantica"` rimossa dall'elenco dei subgeneri latini — era un genere di classificazione interno, non un sottogenere musicale reale da usare come cartella di destinazione.

---

## Music Cataloger Advanced — Upgrade Notes v1069c
**Data:** 2026-04-01 · **Base:** v1069b · **File modificati:** `gui/main_window.py`, `core/genre_classifier.py`, `core/cataloger.py`, `version.py`

---

## BUG CRITICO FIX — `NameError: name 'row_offset' is not defined` (crash avvio)

**File:** `gui/main_window.py`

`row_offset` era definito solo in `_db_filter` (paginazione DB Locale) ma era stato inserito erroneamente anche in `_cache_filter` che non ha paginazione. Rimosso — le righe Cache usano `row=idx` diretto.

---

## FIX CRITICO — Priorità di classificazione genere ribaltata

**File:** `core/genre_classifier.py`

Il codice precedente aveva il DB esterno come **priorità 1**, causando:
- Brani latini classificati come Hip Hop / Pop / Rock (il DB restituisce il genere dell'album, non della traccia singola)
- Artisti metal che in un brano cantano pop → catalogati come Pop (corretto per quel brano, ma non per artisti latin noti)

### Nuova priorità corretta (definita insieme):

| # | Sorgente | Logica |
|---|----------|--------|
| 1 | **Filename** | Se il nome file contiene "salsa", "bachata", "merengue", "cumbia", "reggaeton" → classifica subito, nessuna API necessaria |
| 2 | **Artisti noti** | Se l'artista è nelle liste `salsa_indicators` o `bachata_indicators` (Hector Lavoe, Willie Colón, Romeo Santos, Gilberto Santa Rosa, ecc.) → classifica senza API |
| 3 | **DB esterno** | Ma solo se i passi 1 e 2 non hanno dato risultato. Se il DB dice "pop/rock/hip hop" ma `detect_latin_subgenre` dà un risultato positivo → la detection sovrascrive il DB |
| 4 | **Latin subgenre detection** | Score basato su indicatori + BPM |
| 5 | **all_genres** | Lista generi aggiuntivi da MusicBrainz/Deezer |
| 6 | **Metadati locali ID3** | Tag già presenti nel file |
| 7 | **Unknown** | Nessuna fonte ha dato un risultato |

Questo risolve il problema tipico: iTunes classifica "El Gran Combo de Puerto Rico" come "Worldwide" (genere album) — il nuovo codice lo riconosce dall'artista noto e lo cataloga correttamente come Salsa.

---

## FIX — Tag ID3 genre aggiornato anche dopo lo spostamento

**File:** `core/cataloger.py`

Il tag ID3 `genre` veniva aggiornato prima dello spostamento ma solo se `_update_metadata` restituiva `True`. Per file già parzialmente taggati o con metadati minimi, l'aggiornamento poteva fallire silenziosamente. Aggiunto fallback con `eyed3` che scrive direttamente solo il campo `genre` se `_update_metadata` non ha aggiornato nulla.

---

## Conteggio menu Windows post v1069b

Con tutte le modifiche accumulate:
- 3 `CTkScrollableFrame` rimasti (DB Locale + 2 tab Avanzate) × 2 = **6**
- Menubar (mb + file_m + recent_m + tools_m + help_m) = **5**
- Menu recenti `_recent_menu_widget` (creato lazy al primo click) = **1**
- **Totale: 12 / 32** — ampiamente sotto il limite

**Nota:** se il crash `No more menus can be allocated` persiste ancora, significa che il PC sta usando una versione precedente di `main_window.py`. Verificare che la riga ~490 del file contenga `"v1069b: riusa un unico tk.Menu"` nel docstring di `_show_recent_dropdown`.

---

## Music Cataloger Advanced — Upgrade Notes v1070
**Data:** 2026-04-01 · **Base:** v1069c · **File modificati:** `gui/main_window.py`, `services/cover_service.py`, `version.py`

---

## BUG FIX — `AttributeError: '_cache_meta_var'` al click su un record Cache

`_cache_select` usava `self._cache_meta_var` che non era mai stato inizializzato nel nuovo layout. Il testo di dettaglio (Album, Genere, Anno, BPM, Sorgente) è ora consolidato direttamente in `_cache_detail_var` che esiste ed è correttamente collegato al label del pannello destro.

---

## BUG FIX — Tab Qualità vuoto dopo "Analizza"

`_quality_list` era un `CTkFrame` senza scrollbar — i widget venivano creati correttamente ma uscivano fuori dalla zona visibile senza possibilità di scroll. Sostituito con `CTkScrollableFrame` (il conteggio totale di menu rimane 14/32, ampiamente sotto il limite).

---

## BUG FIX — Scrollbar decontestualizzate dopo analisi

Il canvas della Cache usava `bind_all("<MouseWheel>", ...)` che si applicava **globalmente a tutti i widget** dell'applicazione, inclusa la Quality tab e le altre scrollbar. Sostituito con `bind("<MouseWheel>", ...)` locale al canvas e al frame interno — non contamina più altri tab.

---

## BUG FIX — Header DB Locale non appariva

`row_offset` era definito ma non veniva usato nel `row.grid(row=idx, ...)` — le righe partivano da 0 sovrascrivendo l'header. Corretto in `row.grid(row=idx + row_offset, ...)`.

---

## FEAT — Paginazione Cache (100 record per pagina)

`_cache_filter` ora supporta la paginazione con navigazione ◀/▶, uguale al DB Locale e al tab Qualità. Il contatore mostra `N voci • pag. X/Y (start-end)`.

---

## FIX — Cover album: priorità alla `cover_url` già in cache

**File:** `services/cover_service.py`

Il metodo `process_file` cercava la cover sempre tramite API anche quando i metadati già contenevano una `cover_url` (URL diretto a Deezer/iTunes 1000×1000). Aggiunta **priorità 0**: se `metadata['cover_url']` è presente, scarica e incorpora direttamente — nessuna chiamata API aggiuntiva. Se il download fallisce, prosegue normalmente con le API.

Questo risolve i casi di cover assente per file che avevano già trovato i metadati nei run precedenti ma la cover non era stata incorporata.

---

## Music Cataloger Advanced — Upgrade Notes v1071
**Data:** 2026-04-01 · **Base:** v1070 · **File modificati:** `gui/main_window.py`, `core/cataloger.py`, `version.py`

---

## FIX — Header fisso fuori dallo scroll (DB Locale + Qualità)

L'header (intestazione colonne) è ora un `CTkFrame` separato posizionato **sopra** il `CTkScrollableFrame` usando il layout `grid`. Quando si scorre la tabella o si cambia pagina, l'header rimane fisso. Il tab DB usa `row=1` per l'header e `row=2` per la lista; il tab Qualità usa la stessa struttura.

---

## FIX — Scrollbar Cache e dialog duplicati/orfani ripristinata

Sostituita la scrollbar tkinter custom (canvas+scrollbar) con `CTkScrollableFrame` standard — stessa scrollbar degli altri tab. Il conteggio menu aggiornato: **20/32** (7 CTkScrollableFrame × 2 = 14, + 5 menubar + 1 recenti).

---

## FIX — Generi rimossi dal GENRE_TREE

Rimossi perché generi di classificazione per il ballo o sotto-sotto-generi non realistici come cartella di destinazione:
- **Bachata Sensual** ✗
- **Bachata Influence** ✗  
- **Tropical** ✗
- **Bolero** ✗
- **Mambo** ✗
- **Vallenato** ✗
- **Salsa Choke** ✗

**Aggiunto:**
- **Pachanga** ✓ (Pachanga cubana)

---

## FIX — `correct_existing_folders` ora aggiorna TUTTE le cartelle

**File:** `core/cataloger.py`

Il metodo precedente correggeva solo `Latin/Salsa` e `Latin/Bachata`. Ora:
1. **Scansiona tutte le cartelle** della directory musicale (incluse quelle create manualmente)
2. **Aggiorna il tag ID3 `genre`** di ogni file con il genere corrispondente alla posizione nella cartella (es. file in `Anime/` → tag `genre = "Anime"`)
3. **Aggiorna il DB locale** (`music_library.json`) con `upsert` per ogni file trovato
4. **Salva il DB** al termine

Questo gestisce i file spostati manualmente dall'utente tra una catalogazione e l'altra: quando si avvia con "Correggi metadati cartelle esistenti" + "Aggiorna DB Locale", tutti i file nelle sottocartelle vengono allineati.

---

## Nota sul funzionamento "Trova duplicati"

Il tool **Trova duplicati** opera sul `music_library.json` (DB locale). Quando si clicca **✓ Mantieni questo**:
1. Elimina fisicamente i file MP3 degli altri percorsi
2. Rimuove le voci corrispondenti dal DB locale
3. Salva il DB aggiornato

Per sincronizzare il DB locale con i file fisici effettivi (compresi spostamenti manuali), usa **"Correggi metadati cartelle esistenti"** con **"Aggiorna DB Locale"** abilitato — questa operazione fa un `upsert` di tutti i file trovati nelle cartelle.

---

## Music Cataloger Advanced — Upgrade Notes v1071b
**Data:** 2026-04-01 · **Base:** v1071 · **File:** `gui/main_window.py`, `core/cataloger.py`, `version.py`

---

## FIX — Cache: deduplicazione e metadati completi

La cache conteneva più chiavi per la stessa canzone (`mb_`, `deezer_`, `itunes_`, `lfm_`). Il tab mostrava ogni chiave come riga separata — es. "Dani J - Entre Tú y Mil Mares" appariva con dati ma senza cover perché la chiave `mb_` non ha `cover_url`.

**Fix:** `_cache_filter` ora deduplicata per `(artist, title)`, mantenendo il record più ricco (quello con cover_url, album, year). Le 1059 chiavi diventano ~616 voci uniche con cover dove disponibile.

**Metadati nel pannello dettaglio:** Artista, Titolo, Album, Genere, Anno, Durata (mm:ss), BPM, Sorgente.

---

## FIX — Generi esclusi: file nelle sottocartelle escluse vengono spostati

**File:** `core/cataloger.py`

La catalogazione normale processa solo i file nella directory radice. I file già in `Latin/Salsa Choke/` rimanevano lì anche dopo aver escluso il subgenere.

**Fase 0 aggiunta a `correct_existing_folders`:** prima di correggere i metadati, scansiona tutte le sottocartelle. Se trova una cartella il cui nome corrisponde a un subgenere escluso nelle prefs, sposta tutti i file nel macrogenere padre e rimuove la cartella vuota.

**Esempio:** `Latin/Salsa Choke/` (Salsa Choke escluso) → tutti i file vengono spostati in `Latin/` → la cartella viene rimossa.

Attivare con: **Correggi metadati cartelle esistenti** (opzione in tab Opzioni Catalogazione).

---

## FEAT — Log: pulsanti toggle colorati

I filtri INFO/WARNING/ERROR sono ora **pulsanti** con colore pieno quando attivi e grigio quando disattivi — visivamente più chiari delle checkbox. Colori:
- **INFO** — azzurro `#7ec8e3`
- **WARNING** — arancio `#e0a030`  
- **ERROR** — rosso `#cc4444`

---

## Music Cataloger Advanced — Upgrade Notes v1072
**Data:** 2026-04-03 · **Base:** v1071b · **File:** `main_window.py`, `cataloger.py`, `settings.py`, `version.py`

---

## FIX CRITICO — Tropical persiste nonostante rimosso dal GENRE_TREE

**Root cause:** `settings.py` conteneva ancora `'tropical'` in `latin_subgenres`. Quindi il genre_classifier, quando riceveva "Música tropical" da iTunes, lo mappava a `Latin/Tropical/` indipendentemente dalle prefs.

**Fix:** rimosso `tropical` (e `bolero`, `mambo`, `vallenato`) da `latin_subgenres` e `latin_indicators_generic`. Aggiunti `boogaloo`, `cha cha cha`, `pachanga` — coerente con il GENRE_TREE attuale.

---

## FIX CRITICO — Logica generi esclusi: subgenere + macrogenere

**Regola corretta implementata:**
- Subgenere escluso, macrogenere **attivo** → file va nel macrogenere (es. Cumbia → `Latin/`)
- Subgenere escluso, macrogenere **anche escluso** → file **resta in root** (non spostato)
- Macrogenere escluso = proprio macrogenere (es. World → World) → root

**File:** `cataloger.py` — blocco `SUBFOLDER ESCLUSO` in `_move_to_genre_folder`.

---

## FIX CRITICO — `_build_cmd`: aggiunge always_excluded automaticamente

Subgeneri rimossi dal GENRE_TREE (Tropical, Bolero, Mambo, Vallenato, Salsa Choke, Bachata Sensual, Bachata Influence, Salsa Romantica) vengono ora **sempre aggiunti** alla lista `--excluded-genres` anche senza voce nelle prefs — così non compaiono mai come cartelle di destinazione.

---

## FIX — Cache: merge intelligente per record (artist, title)

Ripristinata la visualizzazione di **tutte le canzoni** (non deduplicata per chiave) ma con **merging intelligente**: per ogni coppia (artista, titolo) viene mostrata una sola riga che combina i dati da tutte le sorgenti disponibili — cover_url preferita da Deezer/iTunes, metadati da MusicBrainz. La colonna Sorgente mostra `MusicBrainz+Deezer` quando i dati provengono da più fonti.

---

## FEAT — Tab 🌴 Classificazione Caraibica

Nuovo tab tra Qualità e Avanzate con:
- **Priorità classificazione** — schema visivo dei 5 livelli di priorità
- **Range BPM** — campi editabili per Bachata (min-max) e Salsa (min-max)
- **Artisti Salsa noti** — lista editabile (uno per riga)
- **Artisti Bachata noti** — lista editabile (uno per riga)  
- **Indicatori testuali Salsa** — keyword editabili
- **💾 Salva impostazioni** — aggiorna le settings runtime per la sessione corrente

---

## FEAT — Tab Qualità: nuove colonne

Aggiunte colonne **Sample Rate** (es. `44.1kHz`) e **RG** (ReplayGain: ✓/✗) lette da mutagen. Le colonne del DB locale mostrano `—` per questi campi (non disponibili senza rileggere il file).

---

## FEAT — Nuovi generi nel GENRE_TREE

| Macrogenere | Nuovi subgeneri |
|-------------|-----------------|
| Latin | Latin Jazz, Soca, Dancehall |
| World & Other | Afrobeats, Brazilian (MPB/Bossa Nova) |
| Pop & R&B | Funk, Gospel |
| Electronic | Tropical House |

---

## FEAT — Nuovi tool Manutenzione (tab Avanzate)

- **🎵 Esporta Playlist M3U per Genere** — crea un file `.m3u` per ogni cartella genere
- **✂️ Rinomina Batch con Pattern** — rinomina con variabili `{title}`, `{artist}`, `{album}`, `{year}`, `{bpm}` con filtro per cartella
- **🔊 Normalizza Volume (ReplayGain)** — chiama `mp3gain` se installato, altrimenti mostra istruzioni
- **🛠️ Verifica Integrità File MP3** — usa mutagen per rilevare frame corrotti

---

## Roadmap feature grandi (da pianificare)

### EXE/Distribuzione
- **Windows portable**: `pyinstaller --onefile --windowed run_gui.py` → `.exe` standalone
- **Windows installer**: usa **Inno Setup** o **NSIS** con l'exe generato da PyInstaller
- **macOS**: `py2app` → `.app` bundle, poi `hdiutil` per `.dmg`
- **Linux**: `AppImageTool` o pacchetto `deb`/`rpm`
- **Mobile**: Kivy o BeeWare per Android/iOS — significativo refactoring richiesto

### Profilazione con piani
Layer di permessi sopra la GUI: `PLAN_FEATURES = {"base": [...], "pro": [...], "advanced": [...]}`. Ogni sezione della GUI controlla se la feature è nel piano attivo prima di renderla. Posso iniziare questo nella prossima iterazione.

### Icone Phosphor
Download PNG da https://phosphoricons.com/ → dizionario `ICONS: dict[str, CTkImage]` → sostituzione delle emoji nei pulsanti. Posso implementarlo quando mi fornisci le PNG preferite (o scelgo io quelle standard).

---

## Music Cataloger Advanced — Upgrade Notes v1072b
**Data:** 2026-04-03 · **Base:** v1072 · **Nuovi file:** `gui/icons.py`, `config/user_plans.py`, `music_cataloger.spec`, `BUILD_INSTRUCTIONS.md`, `icons/phosphor/*.png`

---

## FEAT — Icone Phosphor Icons (16 icone, 3 dimensioni)

**File:** `gui/icons.py` + `icons/phosphor/` (48 PNG generate)

Icone open source MIT — https://phosphoricons.com/

**Icone incluse:** `music-note`, `folder`, `gear`, `database`, `magnifying-glass`, `warning`, `check-circle`, `playlist`, `upload`, `download`, `star`, `x`, `arrows-clockwise`, `faders`, `chart-bar`, `palm-tree`

**Utilizzo nel codice:**
```python
from gui.icons import get_icon, icon_button

# CTkImage per qualsiasi widget
img = get_icon("music", size=24)

# CTkButton con icona
btn = icon_button(parent, "folder", text="Sfoglia", ...)

# CTkLabel con icona
lbl = icon_label(parent, "database", text="DB Locale", ...)
```

Le icone vengono caricate con `@lru_cache` — una sola lettura da disco, poi in memoria.

---

## FEAT — Sistema Piani Utente (`config/user_plans.py`)

Layer di permessi sopra la GUI. Tre piani:

| Feature | Base | Pro | Advanced |
|---------|------|-----|---------|
| Catalogazione locale | ✓ | ✓ | ✓ |
| DB online (MusicBrainz, Deezer) | ✗ | ✓ | ✓ |
| Recupero cover | ✗ | ✓ | ✓ |
| Tab Cache | ✗ | ✓ | ✓ |
| Tab Qualità | ✗ | ✓ | ✓ |
| Tab Avanzate | ✗ | ✗ | ✓ |
| Tab Caraibica | ✗ | ✗ | ✓ |
| ReplayGain | ✗ | ✗ | ✓ |
| Max file per run | 100 | 1000 | ∞ |

**Utilizzo:**
```python
from config.user_plans import has_feature, require_feature, get_plan

# Controllo semplice
if has_feature("catalog_external_db"):
    # abilita DB online

# Con dialog upgrade automatico
if require_feature("tab_caribbean"):
    self._build_caribbean_tab(tab)

# Info piano corrente
plan = get_plan()
print(plan.display_name)  # "💎 Advanced"
```

Il piano viene salvato in `data/user_plan.json`. Default: `advanced` (per sviluppo/test).

---

## FEAT — Build EXE Windows (`music_cataloger.spec`)

Spec PyInstaller pronto per la build. Per generare l'EXE:

```bat
cd "C:\...\Music Cataloger"
pip install pyinstaller
pyinstaller music_cataloger.spec --clean
```

Output: `dist\Music Cataloger Advanced\Music Cataloger Advanced.exe`

Vedere `BUILD_INSTRUCTIONS.md` per build su macOS (py2app) e Linux (AppImage).

---

## Fix inclusi da v1072

Tutti i fix di v1072 sono inclusi: Tropical rimosso, logica esclusi corretta, cache merge intelligente, tab Caraibica, Sample Rate + ReplayGain in Qualità, nuovi tool manutenzione.

---

## Music Cataloger Advanced — Upgrade Notes v1072c
**Data:** 2026-04-03 · **Base:** v1072b · **File:** `main_window.py`, `user_plans.py`, `version_info.txt`, `music_cataloger.spec`

---

## FIX — PyInstaller: `FileNotFoundError: version_info.txt`

Aggiunto `version_info.txt` con i metadati Windows corretti (FileVersion, ProductName, Copyright). Ora `pyinstaller music_cataloger.spec --clean` funziona senza errori.

---

## FIX — Nuovi generi nel GENRE_TREE (erano mancanti)

I generi aggiunti in v1072 non erano stati salvati nel file corretto. Ora presenti:

| Macrogenere | Nuovi subgeneri |
|-------------|-----------------|
| 🎵 Latin | Latin Jazz, Soca, Dancehall |
| 🎧 Electronic | Tropical House |
| 🎤 Pop & R&B | Funk, Gospel |
| 🌍 World & Other | Afrobeats, Bossa Nova |

---

## FIX — Icone Phosphor integrate nella GUI

Le icone ora appaiono nei pulsanti principali:
- **Sfoglia** → icona `folder`
- **▶ Avvia** → icona `star`
- **■ Ferma** → icona `x`

Le icone sono opzionali — se `gui/icons.py` non è disponibile, i pulsanti usano solo il testo (nessun crash).

---

## FEAT — Tab Caraibica: miglioramenti

### Difficoltà Salsa per BPM
Nuova sezione **⚡ Difficoltà Salsa per BPM** con i range editabili:
- Romantica: 0–79 BPM
- Lenta: 80–94 BPM  
- Media: 95–99 BPM
- Veloce: 100–119 BPM
- Crazy: 120+ BPM

### Hint BPM
Range Bachata e Salsa mostrano il valore tipico accanto ai campi.

### Indicatori testuali Bachata
Nuova sezione **🔍 Indicatori Testuali (Bachata)** con le keyword `bachata`, `bachatero`, `rey de la bachata`, ecc. — modificabile.

### Priorità classificazione DINAMICA
La sezione priorità usa ora una **Listbox con pulsanti ⬆ Su / ⬇ Giù** per riordinare le voci. La numerazione si aggiorna automaticamente. L'ordine configurato viene poi usato nella prossima catalogazione (quando il salvataggio sarà collegato al genre_classifier).

---

## FEAT — Piano utente nella titlebar

La barra del titolo ora mostra il piano attivo:
```
Music Cataloger Advanced  v1072c  |  💎 Advanced
```

---

## FEAT — Manutenzione: layout 2 colonne

I tool di manutenzione sono ora disposti in **4 righe × 2 colonne** per usare meglio lo spazio:
```
📋 Esporta CSV          🔍 Trova Duplicati
🗑️ Svuota Cache         📂 Apri Cartella Dati
🎵 Playlist M3U         ✂️ Rinomina Batch
🔊 Normalizza Volume    🛠️ Verifica Integrità
```

---

## Build EXE — Procedura corretta

```bat
cd "C:\...\Music Cataloger"
pip install pyinstaller
pyinstaller music_cataloger.spec --clean
```

**Nota:** eseguire da terminale NON-amministratore (PyInstaller 7.0 bloccherà la build da admin).

Output: `dist\Music Cataloger Advanced\Music Cataloger Advanced.exe`

---

## v1072d — Fix & Icone Custom (2026-04-14)
**File modificati:** `gui/main_window.py`, `gui/icons.py`, `icons/app/*.png`, `icons/music_cataloger.ico`

### FIX CRITICO — `NameError: name '_ic_dir' is not defined`
La variabile `_ic_dir` veniva referenziata in `_build_dir_section()` senza essere definita nello scope locale. Fix: definita inline prima dell'uso con `_get_icon("folder", 20)`.

### FIX — Impostazioni Caraibiche non caricate all'avvio
Il metodo `_load_caribbean_settings()` veniva chiamato prima che i widget del tab esistessero. Fix: aggiunto `_populate_caribbean_widgets()` chiamato con 100ms delay alla fine di `_build_caribbean_tab()`, che popola direttamente i textbox dal JSON salvato.

### FIX — AudD rimosso dalla lista sorgenti
API trial scaduta. La voce AudD è commentata nella lista checkbox del tab Avanzate → Sorgenti Metadati.

### FIX — Stella rimossa dal pulsante Avvia
L'icona Phosphor "star" è stata sostituita con l'icona custom `analyze2` del set utente.

### FEAT — Icone custom integrate (53 PNG dal design utente)
Tutte le icone fornite dall'utente (sfondo trasparente) sono caricate tramite `gui/icons.py`:
- Pulsanti manutenzione: CSV, Duplicati, Svuota Cache, M3U, ReplayGain, Rinomina Batch, Integrità, Apri Cartella
- Pulsanti Avvia / Ferma
- Header "Directory Musicale" (icona cartella)

### FEAT — Icona applicazione (EXE + finestra + taskbar)
`icons/music_cataloger.ico` multi-risoluzione (16/32/48/64/128/256px) generato dall'icona app del design. Il programma lo imposta con `root.iconbitmap()` all'avvio → appare nella barra titolo, taskbar Windows e sull'EXE con PyInstaller.

### FEAT — Menu profilo VOLANTE (stile Claude Desktop)
`_show_profile_panel()` sostituito con un flyout `overrideredirect` posizionato sotto il badge piano. Si chiude cliccando fuori. Switching piano → riapertura immediata con feature aggiornate.


---

## v1073 — Icone custom, Tooltip, Cache Caraibica, Piano Utente (2026-04-15)
**File:** `gui/main_window.py`, `gui/icons.py`, `core/cataloger.py`, `version.py`, `version_info.txt`, `icons/app/*.png`

### FEAT — Icone custom in tutta la GUI

**Stat bar** — ogni card usa l'icona dedicata:
`processati.png` · `moved.png` · `lapis.png` · `cover.png` · `warning.png`

**Pannello sinistro:**
- Titolo "Music Cataloger" → `taskbar_active.png`
- Directory Musicale → `library.png`
- Sfoglia → `library.png` (icona folder)
- Opzioni Catalogazione → `settings2.png`
- Gestione Duplicati → `duplicates.png`
- Cover Album → `cover.png`
- Avvia → `analyze2.png`, Ferma → `warning.png`, Pulisci Log → `clear_cache.png`
- Tutti i pulsanti Ricarica → `reload.png`

**Tab Caraibica** (via `csection(..., icon_name=)`):
- Priorità Classificazione → `classify_priority.png`
- Range BPM → `analyze.png`
- Velocità Salsa per BPM (rinominato da "Difficoltà") → `velocita_bpm.png`
- Artisti Salsa/Bachata Noti → `artisti_noti.png`
- Indicatori Testuali Salsa/Bachata → `indicatori_testuali.png`

**Tab Avanzate** (via `section(..., icon_name=)`):
tutte le sezioni con icone dedicate

**Tab Qualità:**
- Riscansiona → `reload2.png`

### FEAT — Tooltip hover sulle checkbox Opzioni

Le voci "Solo Analisi" e "Rimuovi Cartelle Vuote" mostrano un tooltip al passaggio del mouse. Testo dell'opzione accorciato (senza emoji).

### FEAT — Cache invalidation per parametri caraibici

Quando si salvano le impostazioni caraibiche, viene scritto `data/caribbean_dirty.flag`. All'avvio della catalogazione successiva, `load_cache()` confronta il timestamp del flag con quello della cache: se il flag è più recente, i file con indicatori latini nel nome/cartella vengono **riclassificati dal filename** prima di usare la cache per il resto dei metadati.

### FEAT — Piano utente: apply_plan_restrictions

Aggiunto `_apply_plan_restrictions()` chiamato automaticamente quando si cambia piano dal flyout. Aggiorna il badge profilo e prepara l'infrastruttura per la visibilità condizionale dei tab (espandibile per nascondere/mostrare tab in base al piano).

### FIX — SyntaxError csection (positional argument follows keyword argument)

Le 7 chiamate `csection("titolo", icon_name="x", "desc")` corrette in `csection("titolo", "desc", icon_name="x")`.

### FIX — AudD rimosso dalla lista sorgenti

Commentato nella lista checkbox del tab Avanzate → Sorgenti Metadati. L'API trial era scaduta.

### Versione: v1073 · version_info.txt aggiornato (FileVersion 1.0.73)


---

## v1073b — Fix icone tab, tooltip, ZIP struttura (2026-04-15)
**File:** `gui/main_window.py`, `gui/icons.py`, `core/cataloger.py`, `icons/app/*.png` (62 icone)

### FIX — ZIP con struttura cartelle corretta
Da questa versione lo ZIP ha la struttura `Music Cataloger/gui/`, `Music Cataloger/core/`, ecc. — basta estrarre e sovrascrivere senza navigare dentro cartelle.

### FIX — Tooltip: un solo attivo alla volta, auto-destroy dopo 2.5s
Il sistema precedente lasciava tooltip multipli aperti. Nuovo sistema: ogni widget gestisce un solo `_tip`, con chiusura automatica garantita su `<Leave>` e dopo 2500ms.

### FIX — Tooltip "Aggiorna" sui pulsanti reload (DB Locale e Cache)
Aggiunto via helper `_add_tooltip()` globale.

### FIX — Avvia/Ferma/Pulisci Log: icone minimali
- **▶ Avvia** — testo Unicode puro (no icona custom)
- **■ Ferma** — testo Unicode puro
- **🗑 Pulisci Log** — emoji trash

### FEAT — Icone nei tab CTkTabview (via `_apply_tab_icons()`)
Poiché CTkTabview non espone `image=` nel metodo `.add()`, i bottoni interni vengono aggiornati dopo 80ms tramite accesso a `_segmented_button._buttons_dict`. Icone: `log.png`, `localdb.png`, `genres.png`, `cache.png`, `quality_icon.png`, `caribbean_top.png`, `advanced.png`.

### FEAT — Su/Giù Caraibica: solo icone con tooltip
I pulsanti di riordino priorità usano `up.png`/`down.png` (44×36px) senza testo, con tooltip "Sposta su" / "Sposta giù".

### FEAT — Icone complete in Tab Avanzate
Tutte le sezioni hanno icona dedicata: `classify.png`, `online_db.png`, `albums.png`, `library2.png`, `rename.png`, `advanced2.png`.

### FEAT — `_set_win_icon()` su tutte le finestre figlie
Helper centralizzato che imposta `music_cataloger.ico` su ogni `CTkToplevel` aperta dal programma.

### FEAT — Menu bar: colori app applicati
`tk.Menu` configurato con `bg=PALETTE["bg"]`, `fg=PALETTE["text"]`, `activebackground=PALETTE["primary"]`. Su Windows la barra bianca dipende dal sistema operativo e non è modificabile via tkinter; i colori del dropdown e dei sottomenu sono comunque aggiornati.

### FIX — `_is_latin_file` nel cataloger
Il metodo era stato aggiunto in una sessione precedente ma non salvato correttamente. Ripristinato.

---

## v1074 — Fix `_is_latin_file` + uniformazione icone finestre (2026-04-20)
**File:** `core/cataloger.py`, `gui/main_window.py`, `version.py`, `version_info.txt`

### Contesto
La v1073 era stata marcata stabile, ma la sezione **v1073b → "FIX `_is_latin_file` nel cataloger"** dichiarava il metodo "ripristinato" senza che lo fosse davvero. In un test reale con flag `caribbean_dirty` attivo, tutti i 71 file elaborati finivano in errore ripetuto:

```
ERROR - Errore inaspettato per <file>.mp3:
        'MusicCataloger' object has no attribute '_is_latin_file'
```

Conseguenza: **0 file spostati su 71**, pur avendo il `GenreClassifier` la priorità 1 filename-first già operativa. Il `try/except` esterno intercettava l'`AttributeError`, loggava e saltava ogni file.

### FIX-01 · Metodo `_is_latin_file` definitivo in `MusicCataloger`
**File:** `core/cataloger.py` — inserito subito dopo `_guess_from_filename`.

Il metodo valuta tre segnali in OR:
1. **Path** — file già dentro `Latin/<sub>/` o una cartella subgenere (salsa, bachata, merengue, cumbia, reggaeton).
2. **Testo** — filename, artist, title, album contengono una keyword da `latin_indicators_generic` (fallback hardcoded se settings non disponibili).
3. **Tag locale** — ID3 genre contiene "latin", "tropical" o un subgenere latino.

È difensivo: `getattr(self, ...)` con fallback se i settings non sono ancora caricati, `try/except` sul path parsing. Non tocca la logica di classificazione esistente — si limita a abilitare il branch `_skip_cache_for_genre` quando il flag `caribbean_dirty` è attivo.

### FIX-02 · Icona custom assente su 4 CTkToplevel
**File:** `gui/main_window.py`

Il helper `_set_win_icon(win)` (introdotto in v1073b per applicare `music_cataloger.ico` con fallback PNG 256) era stato omesso su 4 finestre "titolate". Inventario completo delle 9 `CTkToplevel`:

| Riga | Uso | Stato pre-v1074 |
|------|---------------------------|----|
| Flyout profilo             | ✓ già OK |
| Tooltip `_add_tooltip`     | `overrideredirect(True)` — senza titolo, N/A |
| Tooltip `_bind_tooltip`    | `overrideredirect(True)` — N/A |
| **Analisi qualità**        | ✗ **fix applicata** |
| Tooltip Caraibica          | `overrideredirect(True)` — N/A |
| Rinomina Batch             | ✓ già OK |
| **Catalogazione Completata** | ✗ **fix applicata** (quella dello screenshot) |
| **Duplicati DB**           | ✗ **fix applicata** |
| **About**                  | ✗ **fix applicata** |

I 3 tooltip restano volutamente esclusi: senza barra del titolo, l'icona non avrebbe dove comparire.

### Bump versione
`v1073` → `v1074` · `filevers=(1,0,7,4)` · `FileVersion 1.0.74` · `ProductVersion 1.0.74`.

Scelta del bump minore invece di suffisso lettera (`v1073c`): la FIX-01 correggeva un bug che bloccava la catalogazione in un ramo molto usato (parametri caraibici modificati) — merita una versione separata e identificabile.

---

## v1075 — Caribbean subprocess, tooltip singleton, About refresh (2026-04-20)
**File:** `run_cataloger.py`, `gui/main_window.py`, `version.py`, `version_info.txt`

### Contesto
Dopo la v1074 la catalogazione non crashava più, ma nei test su una libreria con parametri caraibici modificati risultava che **artisti noti dichiarati dall'utente nel tab Caraibica** (Chanel, Dani J, Johnny Sky, Prince Royce, SP Polanco, Mr. Don, Cal Tjader, Marc Anthony, Ray Barretto, ecc.) **venivano classificati in Pop, Hip Hop o Jazz** invece di Latin/Bachata o Latin/Salsa. Il log mostrava:

```
>-- Dani J - Favorito
>-- Deezer: Genere: Pop | BPM: 135     ← dovrebbe vincere P2 artisti noti
\-- Spostata in Pop/
```

In parallelo: tooltip "ghost" che restavano aperti passando velocemente il mouse sopra widget adiacenti (già tentato fix in v1073b con timer di auto-destroy, ma insufficiente sul pattern di scroll veloce). E l'About mostrava ancora emoji 🎵 al centro e testo fermo a v1047.

### 🐛 FIX-01 · Caribbean settings non arrivavano al cataloger
**File:** `run_cataloger.py`  
**Severity:** Critica (la lista artisti noti del tab Caraibica veniva ignorata dal motore)

#### Root cause
Il flusso era disallineato tra processi:

- `gui/main_window.py::_load_caribbean_settings()` legge `data/caribbean_settings.json` al boot della GUI e aggiorna `settings.genre.salsa_indicators` / `bachata_indicators` **in-memory nella GUI**.
- Ma la catalogazione è lanciata tramite `subprocess.Popen([sys.executable, run_cataloger.py, …])` — **un nuovo processo Python separato** che importa `config/settings.py` da zero e vede solo i default hardcoded.

Quindi il `GenreClassifier` istanziato nel subprocess non conosceva "Dani J", "Chanel", "Prince Royce", ecc. — la Priorità 2 (artisti noti salsa/bachata) non scattava mai, il controllo passava a Priorità 3 (DB esterni), e Deezer/iTunes rispondevano "Pop"/"Hip Hop" (genre dell'album ≠ genre del brano per la musica latina).

#### Soluzione
Aggiunto `_load_caribbean_settings_from_json()` in `run_cataloger.py`, chiamato **prima** di istanziare `MusicCataloger`. Replica la logica della GUI:

```python
data = json.loads((project_root / "data" / "caribbean_settings.json").read_text("utf-8"))

# BPM range
if "bachata_bpm_range" in data:
    _s.bpm.bachata_bpm_range = tuple(data["bachata_bpm_range"])
if "salsa_bpm_range" in data:
    _s.bpm.salsa_bpm_range = tuple(data["salsa_bpm_range"])

# Salsa indicators = artisti noti + keyword testuali
sal = (data.get("salsa_artists", []) or []) + (data.get("salsa_keywords", []) or [])
if sal:
    _s.genre.salsa_indicators = [x.strip().lower() for x in sal if x.strip()]

# Bachata indicators = artisti + keyword + core obbligatori
bac = (data.get("bachata_artists", []) or []) + (data.get("bachata_keywords", []) or [])
merged = [x.strip().lower() for x in bac if x.strip()]
for c in ("bachata", "bachatero", "bachatera"):
    if c not in merged:
        merged.append(c)
if merged:
    _s.genre.bachata_indicators = merged
```

Difensivo su ogni passo: file mancante → silent skip; JSON corrotto → warning a stdout e default; eccezione sull'applicazione → warning e default. In caso di successo, log di conferma:

```
✔ Caribbean settings caricate: 32 indicatori salsa, 23 indicatori bachata
```

#### Effetto atteso
Rifacendo la catalogazione dell'ultimo log (file di Dani J, Chanel, Prince Royce, Cal Tjader, Marc Anthony, ecc.) — dopo la patch v1075 — la Priorità 2 del classifier scatta sull'artista dichiarato, il genere diventa Salsa o Bachata e il file finisce in `Latin/Salsa/` o `Latin/Bachata/<sottotipo>/` già prima delle chiamate alle API online.

### 🐛 FIX-02 · Tooltip ghost — singleton globale
**File:** `gui/main_window.py`  
**Severity:** UX (non bloccante ma visivamente rotto)

#### Root cause
L'architettura pre-v1075 creava per **ogni widget** un binding `<Enter>`/`<Leave>` con la sua `_tip` personale. Passando velocemente il mouse tra widget contigui:

- Widget A: `<Enter>` → crea tooltip A.
- Prima che il mouse rilasci A, il mouse è già sopra B.
- Widget B: `<Enter>` → crea tooltip B. Ma `<Leave>` di A non viene sempre emesso per tempo (o si perde nel buffering degli eventi), quindi tooltip A resta vivo.
- Il timer di safety (2500ms) è l'unico che li ripulisce — nel frattempo si vedono 2+ tooltip sovrapposti.

La v1073b aveva aggiunto il timer proprio per mitigare il problema, ma il difetto è architetturale: **finché ogni widget gestisce il suo tooltip, la sincronizzazione è garantita solo dagli eventi del SO**.

#### Soluzione — Singleton globale
Introdotte due variabili di istanza nel `__init__`:

```python
self._global_tip = None         # UNA sola CTkToplevel a livello di app
self._global_tip_after = None   # id del safety timer
```

`_add_tooltip` riscritto completamente:

```python
def _add_tooltip(self, widget, text):
    widget._tip_pending = None
    def _schedule_show(e):
        # cancella show pendente, programma nuovo tra 400ms
        if widget._tip_pending is not None:
            widget.after_cancel(widget._tip_pending)
        widget._tip_pending = widget.after(
            400, lambda ex=e.x_root, ey=e.y_root:
                self._show_global_tooltip(text, ex, ey)
        )
    def _cancel(e):
        if widget._tip_pending is not None:
            widget.after_cancel(widget._tip_pending)
            widget._tip_pending = None
        self._hide_global_tooltip()
    widget.bind("<Enter>", _schedule_show, add="+")
    widget.bind("<Leave>",  _cancel,        add="+")
    widget.bind("<Button-1>", _cancel,      add="+")  # click → chiudi subito

def _show_global_tooltip(self, text, x, y):
    self._hide_global_tooltip()    # distruggi il precedente, sempre
    t = ctk.CTkToplevel(self.root)
    t.overrideredirect(True)
    t.attributes("-topmost", True)
    t.geometry(f"+{x+14}+{y+20}")
    ctk.CTkLabel(t, text=text, ...).pack()
    self._global_tip = t
    self._global_tip_after = t.after(2500, self._hide_global_tooltip)

def _hide_global_tooltip(self):
    # cancella safety timer e distrugge l'istanza
    if self._global_tip_after:
        self._global_tip.after_cancel(self._global_tip_after)
    if self._global_tip is not None:
        self._global_tip.destroy()
    self._global_tip = None
    self._global_tip_after = None
```

Tre garanzie strutturali:
1. **Unicità** — esiste al massimo UNA `CTkToplevel` tooltip a livello di app. Ogni show distrugge il precedente.
2. **Delay 400ms** — l'hover deve essere "intenzionale": sotto i 400ms il tooltip non appare, eliminando il flicker su scroll veloce.
3. **Safety 2500ms** — anche se `<Leave>` si perde (focus-steal, popup, altro), il tooltip muore comunque dopo 2.5s.

I due tooltip-locali nested pre-v1075 (`_bind_tooltip` nel pannello Opzioni, `_tooltip_carib` nel tab Caraibica) ora sono semplici wrapper:

```python
def _bind_tooltip(widget, tip_text):
    self._add_tooltip(widget, tip_text)

def _tooltip_carib(btn, text):
    self._add_tooltip(btn, text)
```

Così non restano code path indipendenti che possano creare ghost tooltips.

### 🎨 FEAT · Finestra About ridisegnata
**File:** `gui/main_window.py::_show_about`

Pre-v1075: emoji 🎵 grande al centro, elenco testuale delle versioni v1040–v1047, copyright "© 2025". Fuori data e fuori stile rispetto al resto dell'interfaccia.

Post-v1075:
- **Logo app reale** (72×72) caricato da `icons/app/app_icon_256.png` tramite `CTkImage`, ridimensionato con `PIL.Image.LANCZOS`. Fallback automatico all'emoji se il PNG manca o PIL non è disponibile.
- **Descrizione atemporale** — cosa fa il programma, non la cronologia. Rimanda a `UPGRADES.md` per il changelog completo:

  > Catalogazione automatica di librerie MP3 con focus sulla musica latina da ballo (Salsa e Bachata).
  > Classificazione multi-sorgente: filename → ID3 → BPM → DB online (MusicBrainz, Last.fm, Deezer, iTunes).
  > Suddivisione Salsa per velocità (Romantica / Lenta / Media / Veloce / Crazy) e Bachata per stile (Dominicana / Fusion / Sensual).
  > Per il changelog completo → UPGRADES.md

- **Copyright aggiornato** a "© 2026 Pedro Marques — Uso personale ed educativo".
- **Icona custom sulla barra del titolo** garantita da `self._set_win_icon(win)` (già aggiunta in v1074, confermata).
- Geometria aumentata a 460×440 per accogliere il nuovo contenuto.

### Note operative — rimandi
- **Menu bar Windows bianca**: il punto è stato discusso in sessione. La v1073b aveva già documentato che `tk.Menu` su Windows ignora i colori impostati (limite di sistema operativo). La soluzione pulita — rimuovere il menu e creare una titlebar/menubar custom con `overrideredirect(True)` + re-implementazione di drag, resize, minimize/maximize/close — è un refactor medio-grande, non adatto a una release di stabilizzazione pre-test. **Rimandato a una versione futura** (v1080 o successive). Per ora il menu resta così com'è.
- **Fix `_is_latin_file`**: già in v1074, non toccata in v1075.

### Bump versione
`v1074` → `v1075` · `filevers=(1,0,7,5)` · `FileVersion 1.0.75` · `ProductVersion 1.0.75`.

### File modificati in v1075
```
run_cataloger.py          +80 righe    (loader caribbean settings + chiamata pre-cataloger)
gui/main_window.py        +90/-45      (tooltip singleton + About refresh + 4 _set_win_icon)
version.py                 1 riga
version_info.txt           4 campi
UPGRADES.md               +270 righe   (questa sezione + sezione v1074)
```

---

## v1076 — Menubar custom, sidebar scrollabile, sort Qualità, fix icone dialog (2026-04-20)
**File:** `gui/main_window.py`, `version.py`, `version_info.txt`

### Contesto
Dopo la v1075 la catalogazione funziona correttamente (Latin artists riconosciuti, tooltip puliti, About aggiornata). Restano in sospeso 5 punti emersi dai test pilota:

1. **Icona mancante** sulla barra del titolo di About, Duplicati e Rinomina Batch (nonostante `_set_win_icon` sia stato aggiunto in v1074/v1075 — problema di timing Windows).
2. **Finestre About e Catalogazione Completata non centrate** a schermo.
3. **Tab Qualità**: header disallineato rispetto alle colonne; serve ordinamento per click su header.
4. **Sidebar sinistra non mostra tutti i contenuti** quando la finestra principale è ridotta verticalmente.
5. **Menu bar Windows bianca** — da decidere se/come rimuovere.

### 🐛 FIX-01 · Icona dialog non visibile — auto-retry timing
**File:** `gui/main_window.py::_set_win_icon`
**Severity:** Bassa (cosmetica ricorrente)

#### Root cause
`iconbitmap()` su Windows con `CTkToplevel` ha un timing problematico: se invocato prima che la finestra sia completamente "realized" (mapped sullo screen), l'icona non si attacca alla barra del titolo. Questo spiega perché su About l'icona mancava anche se il codice la chiamava — la `CTkToplevel` non era ancora pronta nel momento esatto della chiamata.

#### Soluzione
`_set_win_icon` riscritta con **auto-retry doppio**:

```python
def _set_win_icon(self, win):
    def _apply():
        if not win.winfo_exists(): return
        _ico = Path(__file__).parent.parent / "icons" / "music_cataloger.ico"
        if _ico.exists():
            win.iconbitmap(str(_ico))
        else:
            # fallback PNG → iconphoto
            ...
    _apply()                    # 1° tentativo immediato (Linux/Mac)
    win.after(250, _apply)      # 2° tentativo dopo il mapping (Windows/CTk)
```

Il primo tentativo soddisfa Linux/Mac dove il timing non è un problema. Il secondo (250ms dopo) è il tentativo che fa davvero attaccare l'icona su Windows: a quel punto la finestra è mapped, iconbitmap prende la richiesta e Windows la onora.

La modifica è retroattiva — non serve toccare le chiamate esistenti di `_set_win_icon(win)`, tutte le 4+2 finestre esistenti beneficiano automaticamente del retry.

### 🐛 FIX-02 · Finestre non centrate a schermo
**File:** `gui/main_window.py::_show_about`, `gui/main_window.py` (dialog Catalogazione Completata)

Le due finestre usavano `win.geometry("WxH")` senza coordinate — tkinter le posiziona nell'angolo in alto a sinistra o vicino al cursore, non centrate.

Sostituito con `self._center_win(win, W, H)` (helper già esistente, usato correttamente in Rinomina Batch e Analisi qualità). Geometria About portata a 460×440 per accogliere il logo 72×72 introdotto in v1075.

### 🎨 FIX-03 · Tab Qualità — header allineato con le righe
**File:** `gui/main_window.py::_build_quality_tab`
**Severity:** Cosmetica

#### Root cause
L'header era `padx=8` simmetrico, mentre le righe sottostanti sono dentro un `CTkScrollableFrame` che riserva ~16-24px sul lato destro per la scrollbar verticale. Risultato: header largo quanto tutto il frame, righe più strette → colonne disallineate.

#### Soluzione
Header con `padx=(8, 24)` — stesso offset sinistro, ma 24px a destra per compensare la scrollbar. Le colonne ora si allineano byte-per-byte tra header e righe.

### 🎨 FEAT-04 · Tab Qualità — sort per click header + frecce ▲▼
**File:** `gui/main_window.py` — nuovi metodi `_quality_sort_click()` e `_quality_refresh_header_arrows()`

Ogni label dell'header è ora cliccabile (`cursor="hand2"`, bind `<Button-1>`). Click:
- **Prima volta** su una colonna → sort ascendente, mostra ▲ accanto al nome
- **Seconda volta** sulla stessa → toggle a discendente, mostra ▼
- **Click su altra colonna** → reset direzione ad ascendente, sposta la freccia

Chiave di ordinamento intelligente — mappa la colonna visibile all'indice tupla dei dati:

```python
COL2TUP = {0: 0,  # File       → str
           1: 1,  # kbps       → int
           2: 1,  # Qualità    → int (derivato da kbps)
           3: 3,  # SampleRate → float
           4: 4,  # RG         → str
           5: 2}  # Cartella   → str
```

Colonne numeriche ordinate come numeri (con fallback `0.0` per valori mancanti tipo "—"). Colonne testuali `case-insensitive`. Stato del sort memorizzato in `self._quality_sort_col` e `self._quality_sort_dir`, così sopravvive alla paginazione (i click su "pagina 2" non resettano l'ordinamento).

### 🎨 FEAT-05 · Sidebar scrollabile verticalmente
**File:** `gui/main_window.py::_build_left_panel`

Pre-v1076: tutte le sezioni (Directory / Opzioni / Duplicati / Cover / Bottoni) erano `pack()`ate direttamente nel CTkFrame sinistro fisso di 400×(altezza finestra). Riducendo la finestra in altezza, le sezioni in fondo (Cover + Bottoni) venivano tagliate fuori e non c'era modo di raggiungerle.

**Nuova struttura a 3 zone**:

```
┌─────────────────────────┐
│ HEADER (logo + badge)   │  pack(side="top") — fisso
├─────────────────────────┤
│                         │
│  MIDDLE                 │  CTkScrollableFrame
│  - Directory            │  pack(fill="both", expand=True)
│  - Opzioni              │  scrollbar verticale automatica
│  - Duplicati            │
│  - Cover                │
│                         │
├─────────────────────────┤
│ FOOTER (Avvia/Ferma/    │  pack(side="bottom") — fisso
│         Pulisci Log)    │
└─────────────────────────┘
```

L'ordine dei `pack()` è importante: prima header (side top), poi **footer (side bottom)**, poi middle (fill both expand). Tkinter riserva prima lo spazio di top e bottom, il middle riempie il resto.

Quando la finestra è grande abbastanza, tutto è visibile senza scrollbar. Quando l'utente la rimpicciolisce verticalmente, la scrollbar appare automaticamente **solo nella zona middle** — i bottoni Avvia/Ferma in fondo e l'header in cima restano sempre visibili e non si spostano.

### 🎨 FEAT-06 · Menubar custom (fix barra bianca Windows)
**File:** `gui/main_window.py` — nuovo `_build_custom_menubar()`, rimosso `_build_menu()`

#### Il problema
La vecchia menubar era `tk.Menu` di sistema, agganciata via `self.root.config(menu=mb)`. Su Windows questa produce una barra di ~18px **sempre bianca**, non tematizzabile via tkinter (limite documentato del motore tk su Windows — i colori impostati su `tk.Menu(bg=...)` vengono ignorati per la top-level bar).

Erano state valutate due strade:
- **(A)** Toolbar custom CTk + popup `tk.Menu` al click
- **(B)** Titlebar completamente custom con `overrideredirect(True)` — refactor grande con rischi su drag/resize/snap/Aero Peek

Scelta **(A)** — 90% del risultato visivo al 20% del rischio di regressioni.

#### Implementazione
1. `self.root.config(menu=...)` **non più chiamato** — la barra bianca sparisce.
2. Nuovo `CTkFrame` alto 32px su `row=0, columnspan=2` del root grid, con tre pulsanti `CTkButton` (File, Strumenti, Help) stilizzati come il resto dell'app (`fg_color="transparent"`, `hover_color=surface2`, `corner_radius=0`, font uniforme).
3. Al click, una factory `_popup_menu()` crea on-demand un `tk.Menu(tearoff=0)` con i colori app e lo apre sotto il pulsante via `tk_popup()`. I popup **sì** onorano i colori impostati (il limite del bianco riguarda solo la menubar di sistema in alto).
4. I pannelli esistenti sono stati spostati da `row=0` a `row=1` del root grid per far spazio alla toolbar. `rowconfigure(1, weight=1)` garantisce che il contenuto continui ad espandersi normalmente.

Il sottomenu "Directory Recenti" viene ricostruito ad ogni popup (`_refresh_recent_menu` chiamato dentro `_build_file_menu`) — `self._recent_menu` punta sempre al `tk.Menu` corrente, nessun riferimento stale.

#### Effetto visivo
Prima: barra bianca Windows da 18px + menubar app da 0px = **18px bianchi**.
Dopo: 0px bianchi + toolbar app da 32px tematizzata = layout pulito, uniforme col resto.

### Bump versione
`v1075` → `v1076` · `filevers=(1,0,7,6)` · `FileVersion 1.0.76` · `ProductVersion 1.0.76`.

### File modificati in v1076
```
gui/main_window.py         ~250 righe toccate
  - _set_win_icon          riscritta (auto-retry)
  - _show_about            aggiunto _center_win
  - dialog Catalogazione   aggiunto _center_win
  - _build_quality_tab     padx header + click-sort
  + _quality_sort_click          NUOVO
  + _quality_refresh_header_arrows  NUOVO
  - _build_left_panel      ristrutturata in 3 zone
  + _build_custom_menubar  NUOVO (sostituisce _build_menu)
  - __init__               rimossa chiamata _build_menu
  - _build_layout          toolbar custom + row shift
  - left panel             row=0 → row=1
  - right panel            row=0 → row=1

version.py                 1 riga
version_info.txt           4 campi
UPGRADES.md                questa sezione
```

---

## v1077 — Opzione C, flyout profilo a 5 voci, duplicati batch, resize fix (2026-04-20)
**File:** `gui/main_window.py`, `gui/icons.py`, `icons/app/ph-*.png` (6 nuovi), `version.py`, `version_info.txt`

### Contesto
Dopo la v1076 rimangono aperti 5 punti:

1. **Menu bar custom da nascondere** (Opzione C concordata) — tutte le voci spostate nel flyout profilo o nel tab Avanzate.
2. **Profilo** — pulsante da spostare sotto il titolo (2 righe), con nome utente + icona user e badge piano separato. Sottomenu con 5 voci: Impostazioni, Lingua, Piani, Aiuto, Esci. Icone Phosphor a sinistra.
3. **Dialog duplicati** — con 85+ voci i bottoni "Mantieni questo" per-riga con singolo dialog di conferma diventano insostenibili: serve radio + batch.
4. **Resize lento** — quando si trascina il bordo della finestra, l'aggiornamento è molto lento (causato dal CTkScrollableFrame della sidebar introdotto in v1076).
5. **"Advanced" doppio** — la riga "Advanced v1076" sotto il titolo è fuorviante: "Advanced" è il nome del piano, non un sottotitolo del prodotto.

### 🎨 FEAT-01 · Opzione C — menubar custom rimossa
**File:** `gui/main_window.py`

#### Cosa è stato fatto
Il metodo `_build_custom_menubar()` introdotto in v1076 è stato eliminato (sostituito da un commento-lapide per tracciabilità). La toolbar custom in `row=0` del root non viene più creata. `_build_layout()` torna alla forma pre-v1076 con una sola riga (`rowconfigure(0, weight=1)`) e left/right panel di nuovo su `row=0`.

Le voci che erano nel menu custom sono state redistribuite:
- **File → Seleziona Directory** — già coperto dal pulsante "Sfoglia" nella sidebar
- **File → Directory Recenti** — già coperto dal pulsante freccia accanto a "Sfoglia"
- **File → Esci** — ora nel flyout profilo, voce "Esci"
- **Strumenti → Apri Cartella Log** — spostata nel tab **Avanzate → Manutenzione**
- **Strumenti → Test Configurazione** — spostata nel tab **Avanzate → Manutenzione**
- **Help → About** — ora nel flyout profilo, voce "Aiuto"

Nessuna funzionalità persa. Guadagnati ~32px di altezza a schermo (fine della toolbar custom v1076).

### 🎨 FEAT-02 · Icone Phosphor per il flyout
**File:** `icons/app/ph-*.png` (6 nuovi PNG), `gui/icons.py`

Generate 6 icone in stile Phosphor Regular (1.6pt stroke, round linecap, outline-only, canvas 32×32 trasparente):
- `ph-user.png` — silhouette utente (pulsante profilo)
- `ph-gear.png` — ingranaggio (Impostazioni)
- `ph-translate.png` — globo terrestre (Lingua)
- `ph-crown.png` — corona (Piani)
- `ph-question.png` — cerchio con "?" (Aiuto)
- `ph-sign-out.png` — porta+freccia (Esci)

Le icone sono state disegnate a mano in SVG perché l'ambiente di build ha network restrittivo su `raw.githubusercontent.com`. Stile coerente con le icone app esistenti. Se in futuro si vogliono sostituire con le originali Phosphor, basta droppare 6 PNG con lo stesso nome nella cartella `icons/app/`.

`gui/icons.py` aggiornato con 6 nuove chiavi di mapping:
```python
"profile":     "ph-user",
"settings_ph": "ph-gear",
"lang":        "ph-translate",
"plans":       "ph-crown",
"help_ph":     "ph-question",
"logout":      "ph-sign-out",
```

### 🎨 FEAT-03 · Header sidebar ridisegnato
**File:** `gui/main_window.py::_build_left_panel`

Il vecchio header aveva 2 righe di contenuto:
```
[icona] Music Cataloger         [badge Piano]
Advanced  v1076
```

Il doppio "Advanced" era confusivo. Nuovo header a 3 righe:
```
[icona] Music Cataloger
v1077
[ph-user  Nome utente          ][Piano]
```

- Titolo + icona title_icon sulla prima riga
- Versione (solo `v1077`, senza "Advanced") sulla seconda
- Pulsante profilo con icona `ph-user` + nome utente + badge piano compatto sulla terza

Il pulsante ora mostra il **nome utente** (letto da `plan.username` con fallback "User"), il **badge piano** (tag arrotondato a destra) mostra il nome del piano. I due callsite che facevano `self._profile_btn.configure(text=plan.display_name)` ora aggiornano `self._plan_badge` — separazione netta tra identità utente e piano attivo.

### 🎨 FEAT-04 · Flyout profilo a 5 voci con substitute+back
**File:** `gui/main_window.py::_show_profile_panel`

Completamente riscritto. Architettura:

**Pannello principale** (5 voci, icona Phosphor a sinistra):
```
[ph-user] <NomeUtente>
          Piano: <DisplayName>
─────────────────────────────────
 [gear]     Impostazioni
 [lang]     Lingua              ›
 [crown]    Piani abbonamento   ›
 [help]     Aiuto
 [logout]   Esci
```

**Pattern substitute + back**: quando l'utente clicca una voce con `›` (Lingua o Piani), il contenuto del flyout viene sostituito da un pannello secondario con pulsante **"← Indietro"** nell'header che riporta alla lista principale. Nessun flyout annidato — una sola `CTkToplevel` che ricicla lo stesso spazio.

**Sottomenu implementati**:
- **Impostazioni** — placeholder (messaggio "disponibile in una prossima versione")
- **Lingua** — lista 3 lingue (Italiano ✓, English, Español) con nota sul placeholder
- **Piani** — contenuto del vecchio flyout v1072d (feature list del piano attuale + pulsanti base/pro/advanced per cambiarlo)
- **Aiuto** — delega a `self._show_about()` dopo aver chiuso il flyout
- **Esci** — chiude il flyout e chiama `self._on_close()` (che a sua volta chiede conferma se c'è un processo in corso)

Ogni voce ha hover con cambio colore (`surface2` → `surface`). Click binding esteso a tutti i children del frame per evitare dead zones.

### 🎨 FEAT-05 · Duplicati — radio + conferma batch
**File:** `gui/main_window.py` (dialog in `_maint_find_duplicates` callback)

Pre-v1077: per ogni duplicato un bottone "✓ Mantieni questo" → un dialog di conferma per-riga → un `file.unlink()`. Con 85 gruppi di duplicati, 85 click + 85 conferme = UX non utilizzabile.

Nuovo layout:
```
┌─ ⚠️  85 file con nome duplicato ─────────────────────┐
│ Seleziona per ciascun gruppo il file da mantenere... │
├──────────────────────────────────────────────────────┤
│ 📄 File.mp3                                          │
│    ○ Latin/Salsa/…/File.mp3                         │
│    ● Jazz/…/File.mp3                                 │
│ 📄 OtherFile.mp3                                     │
│    ● Pop/…/OtherFile.mp3                            │
│    ○ Rock/…/OtherFile.mp3                           │
│    ...                                               │
├──────────────────────────────────────────────────────┤
│  12 di 85 gruppi selezionati         [Chiudi]       │
│                     [✓ Conferma selezioni]          │
└──────────────────────────────────────────────────────┘
```

- Ogni gruppo ha un `tk.StringVar` → una `CTkRadioButton` per ogni path del gruppo
- Nessuna selezione di default: forza scelta esplicita
- Contatore dinamico nel footer: `"N di M gruppi selezionati"` aggiornato via `var.trace_add("write", ...)`
- Un unico dialog di conferma con riepilogo (`"Mantieni N file, elimina M file"`) prima dell'esecuzione batch
- Esecuzione single-pass: per ogni gruppo selezionato elimina tutti i path non scelti, aggiorna `raw["files"]` del DB locale, scrive il JSON una sola volta alla fine
- Se ci sono errori vengono raccolti e mostrati in un `showwarning` finale (max 8 righe), altrimenti `showinfo` con il conteggio

Risultato: da 85 click + 85 conferme a **N click (uno per gruppo) + 1 conferma** finale.

### 🐛 FIX-06 · Resize lento della finestra principale
**File:** `gui/main_window.py::_build_left_panel`

#### Root cause
Il `CTkScrollableFrame` introdotto in v1076 per la sidebar ricalcola il viewport scroll region ad ogni evento `<Configure>` del canvas interno. Quando l'utente trascina il bordo della finestra, tkinter emette dozzine di eventi al secondo, ognuno fa repack dei 4 frame di sezione + ~30 widget ciascuno — ~120 widget rimisurati decine di volte al secondo = drag lentissimo.

#### Soluzione
Debounce del `<Configure>` a livello root:

```python
def _on_root_configure(e):
    if e.widget is not self.root:
        return
    if self._sidebar_resize_after is not None:
        self.root.after_cancel(self._sidebar_resize_after)
    def _recompute():
        self._sidebar_resize_after = None
        _middle.update_idletasks()
    self._sidebar_resize_after = self.root.after(120, _recompute)

self.root.bind("<Configure>", _on_root_configure, add="+")
```

Flow:
1. Utente inizia a trascinare → emette `<Configure>` 1, 2, 3, …
2. Ogni evento **cancella** l'`after` pendente e ne schedula uno nuovo a 120ms
3. Solo quando l'utente smette di trascinare per 120ms consecutivi, il `_recompute` scatta una volta sola
4. Il `CTkScrollableFrame` ricalcola il viewport con le dimensioni finali, non 60 volte durante il drag

Non sostituiamo il bind interno di CTk (rompe le sue internals), aggiungiamo un bind aggiuntivo sul root. Il trick `add="+"` garantisce che i bind esistenti continuino a funzionare normalmente.

### Bump versione
`v1076` → `v1077` · `filevers=(1,0,7,7)` · `FileVersion 1.0.77` · `ProductVersion 1.0.77`.

### File modificati in v1077
```
gui/main_window.py
  -   def _build_custom_menubar (~95 righe)         → rimossa (commento-lapide)
  - _build_layout               torna a row=0
  - _build_left_panel           nuovo header 3 righe + debounce resize
  - _show_profile_panel         ~130 righe, riscritto con 5 voci + substitute+back
  - dialog duplicati            radio + batch confirm
  - _tools (tab Avanzate)       +2 voci (Apri Cartella Log, Test Configurazione)
  - 2 callsite                  _profile_btn → _plan_badge per l'update

gui/icons.py                    +6 voci mapping Phosphor

icons/app/ph-user.png           nuovo
icons/app/ph-gear.png           nuovo
icons/app/ph-translate.png      nuovo
icons/app/ph-crown.png          nuovo
icons/app/ph-question.png       nuovo
icons/app/ph-sign-out.png       nuovo

version.py                       v1077
version_info.txt                 1.0.77

UPGRADES.md                      questa sezione
```

---

## v1078 — Fix regressioni v1077 e rifiniture UX (2026-04-21)
**File:** `gui/main_window.py`, `version.py`, `version_info.txt`

### Contesto
Dopo l'installazione della v1077 sono emerse 3 regressioni serie (AttributeError sulla selezione directory, flyout che non funziona come "pannello laterale", radio button quasi invisibili) e 4 rifiniture UX richieste dai test pilota:

1. **AttributeError `_recent_menu`** — selezione directory da Sfoglia e dallo storico non funzionano più (Tkinter callback fallisce).
2. **Flyout substitute+back non piace** — l'utente preferiva il pattern "pannello laterale che si estende a destra" (il `›` delle voci con sottomenu perdeva senso con substitute).
3. **Radio duplicati invisibili** — con il tema scuro il radio non si distingue, all'apertura sembra non ci siano controlli.
4. **Pulsante profilo "spento"** — dovrebbe essere blu pieno stile pill (come il vecchio badge piano v1072d).
5. **Badge piano doppio** — ormai è un refuso, il piano è già nel flyout voce "Piani".
6. **Versione sulla seconda riga** — andrebbe accanto al titolo come appendice.
7. **Scrollbar sidebar visibile all'apertura** — il layout compatto dovrebbe bastare.
8. **"Esci" chiude il programma** — dovrebbe essere un logout (placeholder finché non c'è login).
9. **Resize ancora lento** — la fix v1077 non era sufficiente.

### 🐛 FIX-01 · AttributeError `_recent_menu` (selezione directory)
**File:** `gui/main_window.py::_refresh_recent_menu`
**Severity:** Critica (blocca la selezione directory da Sfoglia e da storico)

#### Root cause
In v1076 la menubar custom aveva un sottomenu `File → Directory Recenti` rappresentato da `self._recent_menu` (un `tk.Menu` condiviso). Quando in v1077 ho applicato l'Opzione C rimuovendo la menubar (`_build_custom_menubar`), quell'attributo non viene più creato. Ma `_refresh_recent_menu()` continua a essere chiamato ogni volta che l'utente seleziona una directory:

```
_select_path → _add_recent_dir → _refresh_recent_menu → self._recent_menu.delete(...)
                                                         ^^^^^^^^^^^^^^^^^
                                                         AttributeError
```

Il dropdown della sidebar (`_show_recent_dropdown`, righe 1098-1123) **non** dipende da `self._recent_menu`: crea il suo `_recent_menu_widget` on-demand e lo svuota/ripopola ad ogni apertura leggendo direttamente `self._recent_dirs`. Quindi `_refresh_recent_menu` è diventato obsoleto.

#### Soluzione
`_refresh_recent_menu()` reso **no-op** con docstring esplicativa. I due chiamanti (`_add_recent_dir`, `_save_recent_dirs`) continuano a compilare; la lista `_recent_dirs` viene aggiornata correttamente e il dropdown al prossimo click la leggerà.

### 🎨 FEAT-02 · Flyout con pannello laterale a destra
**File:** `gui/main_window.py::_show_profile_panel` (riscritto)

#### Pre-v1078 (v1077 substitute+back)
Click su voce con sottomenu → stesso Toplevel principale sostituisce il contenuto con il sottomenu + pulsante "← Indietro" in header. Le voci con `›` perdevano senso perché non c'era nulla che si espandeva lateralmente.

#### Post-v1078 (side-panel toggle)
Click su voce con `›` → si apre un **secondo `CTkToplevel`** accanto a destra del principale (+4px di gap), mentre il principale resta visibile. Tre stati gestiti:

- `self._profile_flyout` → Toplevel principale (300×360)
- `self._profile_sub_flyout` → Toplevel laterale (340×420), opzionale
- `self._profile_active_sub` → id del sottomenu attivo (`"settings"` | `"lang"` | `"plans"` | `None`)

Comportamento toggle:
- Click **prima volta** su "Piani" → apre sub-flyout "Piani" + evidenzia row blu
- Click **stessa voce** → chiude sub + reset evidenza
- Click **altra voce con `›`** → chiude sub precedente + apre nuovo + sposta evidenza

Click esterno (fuori da entrambi i Toplevel) → cleanup completa via `_close_profile_flyout()` (nuovo helper che distrugge prima il sub, poi il main).

Il sub-flyout ha una sua mini header blu con il titolo della sezione e un pulsante `✕` per chiudere solo il laterale senza chiudere il principale. Il contenuto (settings, lang, plans) è lo stesso della v1077 — solo il contenitore cambia.

### 🎨 FEAT-03 · Header sidebar compatto a 2 righe
**File:** `gui/main_window.py::_build_left_panel`

Pre-v1078 (v1077 layout 3 righe):
```
[icona] Music Cataloger
v1077
[ph-user  Nome utente     ][Advanced]
```

Post-v1078 (layout 2 righe):
```
[icona] Music Cataloger  v1078
[ph-user  Nome utente               ]
```

- Versione `v1078` inline al titolo come piccola appendice (`FONT_SMALL[1]-1`, `text_dim`, `pady=(8,0), anchor="s"` per allinearla alla baseline del titolo)
- Riga "Advanced v107x" rimossa (era un duplicato della versione + refuso sul nome piano)
- Badge piano rimosso — era un refuso: il piano corrente si vede nell'header del flyout profilo e nel sottomenu "Piani"
- Pulsante profilo ora full-width, stile pill blu (`fg_color=primary`, `corner_radius=16`, `text_color=#ffffff`) — stesso aspetto del vecchio badge piano v1072d
- `self._plan_badge` mantenuto come widget nascosto (non-packed) per compatibilità coi 2 callsite esistenti che chiamano `_plan_badge.configure(text=...)` in blocchi `try/except`

Guadagno: ~28px verticali che contribuiscono a nascondere la scrollbar sidebar alla dimensione standard della finestra.

### 🎨 FIX-04 · Radio button duplicati — contorno bianco
**File:** `gui/main_window.py` (dialog in `_maint_find_duplicates` callback)

Pre-v1078:
```python
border_color=PALETTE["border"]       # #333 — si fonde col dark
radiobutton_width=16, height=16
text_color=PALETTE["text_dim"]
```
Risultato: radio quasi invisibile su sfondo scuro.

Post-v1078:
```python
border_color="#ffffff"               # contorno bianco netto
border_width_checked=5
border_width_unchecked=2
radiobutton_width=18, height=18      # +2px ciascun lato
text_color=PALETTE["text"]           # testo pieno, non dim
```

Il contorno bianco emula lo stile dei radio usati nella sezione "Gestione Duplicati" della sidebar — uniforme visivamente col resto dell'app.

### 🎨 FIX-05 · Spazio vuoto tra opzioni catalogazione
**File:** `gui/main_window.py::_build_options_section`

Tra "Rimuovi Cartelle Vuote" e "Abilita Sorgenti DB Online" c'era un buco visibile dovuto a un row vuoto nel grid (separatore era su row=4, il DB checkbox su row=5, saltando row=3) + padding generoso del separatore (`pady=(6,4)`).

Fix: separatore compattato su row=3 con `pady=(4,2)`, DB checkbox su row=4 con `pady=(2,3)`. Testo informativo sposato di conseguenza a row=5. Guadagno: ~10px verticali, aspetto più coeso.

### 🎨 FEAT-06 · Esci = logout placeholder
**File:** `gui/main_window.py` (callback flyout voce "Esci")

Pre-v1078: "Esci" chiamava `self._on_close()` → chiusura del programma con conferma.

Post-v1078: "Esci" chiude il flyout e mostra un `messagebox.showinfo` esplicativo:

```
Il logout non è ancora disponibile: il sistema di
autenticazione verrà implementato in una prossima versione.

Per chiudere il programma usa il pulsante X in alto a destra.
```

L'uscita del programma resta disponibile via X della finestra o Alt+F4 (comportamento standard Windows). Quando verrà implementato il sistema di login client/server sul NAS, qui si collegherà la vera logica di logout + schermata di login.

### 🐛 FIX-07 · Resize lento — strategia "freeze during drag"
**File:** `gui/main_window.py::_build_left_panel`

#### Perché la fix v1077 non bastava
La v1077 bindava `<Configure>` sul root con un debounce `after(120, _recompute)` che chiamava solo `_middle.update_idletasks()`. Due problemi:
1. `update_idletasks()` **forza** immediatamente tutti i processi idle pendenti — esattamente l'opposto di un debounce (che dovrebbe ritardarli)
2. Il canvas interno di `CTkScrollableFrame` riceveva comunque i `<Configure>` di ogni pixel di drag, facendo re-layout dei 120 widget

#### Soluzione v1078
State machine con tracciamento esplicito dell'inizio/fine drag:

```python
self._sidebar_dragging = False
self._sidebar_last_size = (0, 0)

def _on_root_configure(e):
    if e.widget is not self.root: return
    sz = (e.width, e.height)
    if sz == self._sidebar_last_size: return   # no-op su Configure duplicati
    self._sidebar_last_size = sz

    # Cancella il recompute pendente → estende la finestra di attesa
    if self._sidebar_resize_after:
        self.root.after_cancel(self._sidebar_resize_after)
    self._sidebar_dragging = True

    def _release():
        self._sidebar_resize_after = None
        self._sidebar_dragging = False
        _middle.update_idletasks()
        # Forza il ricalcolo della scroll region CON LE DIMENSIONI FINALI
        cv = _middle._parent_canvas
        bbox = cv.bbox("all")
        if bbox: cv.configure(scrollregion=bbox)
    self._sidebar_resize_after = self.root.after(160, _release)
```

Timing scelto: **160ms**. Un drag normale emette `<Configure>` ogni ~16ms (60fps), quindi 160ms mangia 10 frame consecutivi prima di rilasciare il ricalcolo.

**Nota trasparente**: questa è la strategia che si può implementare senza toccare le internals di CTk. Se il drag dovesse risultare ancora lento nei test utente, la v1079 potrebbe **rimuovere del tutto il `CTkScrollableFrame`** dalla sidebar: con l'header compatto v1078 e i 10px guadagnati dalle opzioni, le 4 sezioni + bottoni entrano in ~720px di altezza, che è lo spazio disponibile anche su laptop a 768p.

### Bump versione
`v1077` → `v1078` · `filevers=(1,0,7,8)` · `FileVersion 1.0.78` · `ProductVersion 1.0.78`.

### File modificati in v1078
```
gui/main_window.py
  - _refresh_recent_menu           → no-op (fix AttributeError)
  - _build_left_panel header       → 2 righe con versione inline, badge piano rimosso
  - _show_profile_panel            → ~350 righe, riscritto con side-panel
  + _close_profile_flyout          → nuovo helper di cleanup combinato
  - _handle_exit                   → placeholder logout (niente _on_close)
  - radio duplicati                → contorno bianco + stroke più spesso
  - _build_options_section grid    → row 3 compattato (no spazio vuoto)
  - resize debounce                → state machine con tracking last_size

version.py                          v1078
version_info.txt                    1.0.78
UPGRADES.md                         questa sezione
```

### Punti aperti per v1079 (se necessario dopo i test)
- Se il resize è ancora percettibilmente lento → rimuovere `CTkScrollableFrame` dalla sidebar
- Sistema di login con autenticazione client/server verso NAS Docker
- Implementazione reale del logout (ora placeholder)
- Traduzione UI (Italiano/English/Español) con backend gettext o dict-driven

---

## v1079 — Fix regressioni v1078 + rimozione scrollable sidebar (2026-04-21)
**File:** `gui/main_window.py`, `version.py`, `version_info.txt`

### Contesto
Cinque punti dai test pilota v1078:
1. **Crash radio duplicati** — TypeError `'StringVar' object is not subscriptable` al click di qualsiasi radio (il contatore a fondo pagina non si aggiorna).
2. **Hitbox voci flyout** — l'hover evidenzia solo la striscia al centro della riga; Aiuto/Esci (senza icona a sinistra) non si evidenziano affatto.
3. **Spazio opzioni residuo** — dopo v1078 il gap tra "Rimuovi Cartelle Vuote" e "Abilita Sorgenti DB Online" è ridotto ma ancora visibile.
4. **Scrollbar sidebar presente all'apertura** — ingombro visivo inutile.
5. **Resize ancora lentissimo** — anche la state-machine v1078 non risolve.
6. **Angoli flyout non rounded** — i Toplevel hanno angoli netti, stonano con il pulsante pill blu che è rounded.

### 🐛 FIX-01 · TypeError subscript su radio duplicati
**File:** `gui/main_window.py` (dialog `_maint_find_duplicates` → `_refresh_count`)
**Severity:** Bloccante per la feature duplicati

#### Root cause
```python
group_choices[fname] = (var, list(paths))   # values: (StringVar, [paths])

# v1078 (BUG):
def _refresh_count(*_):
    n = sum(1 for _v, _ in group_choices.values() if _v[0].get())
    #                                                  ^^^^^^ _v è già StringVar
```

Il loop unpack `_v, _` estrae correttamente la `StringVar` in `_v`, ma poi
indicizzo `_v[0]` come se fosse ancora una tupla — TypeError.

#### Fix v1079
```python
def _refresh_count(*_):
    n = sum(1 for _v, _ in group_choices.values() if _v.get())
```

Il contatore `"N di M gruppi selezionati"` ora si aggiorna correttamente
ad ogni click di radio.

### 🐛 FIX-02 · Hitbox voci flyout
**File:** `gui/main_window.py::_show_profile_panel::_mk_row`

#### Root cause
La funzione `_mk_row` creava:
```
row (CTkFrame, height=36)
 └─ inner_row (CTkFrame, transparent, expand=True)
     ├─ CTkLabel(icon+text, side="left", fill="y")   ← NON expand, NON fill="both"
     └─ CTkLabel("›", side="right")                   ← solo voci con submenu
```

Problemi:
1. Il label del testo usava `fill="y"` → occupava solo la sua larghezza naturale, lasciando lo spazio restante del frame **non coperto** dal label
2. Hover era bindato solo su `row` e `inner_row`, NON sui label children. Quando il mouse passava sopra il label, `<Leave>` del frame genitore scattava perché parte della riga non era più coperta
3. Per "Aiuto" ed "Esci" (senza `›` a destra) il comportamento era peggiore: il label stretto stava a sinistra, il resto della riga era "morto"

#### Fix v1079
- Label principale: `fill="both", expand=True` → si espande a coprire tutta la larghezza della riga
- `<Enter>`/`<Leave>`/`<Button-1>` bindati su **tutti i widget**: `row`, `inner_row`, `lbl` e (se presente) `arrow`
- Lista di bind esplicita invece di loop su `winfo_children()` (più chiaro e testabile)

Risultato: hover copre tutta la riga per tutte le voci, click funziona ovunque.

### 🎨 FIX-03 · Spazio residuo tra checkbox opzioni
**File:** `gui/main_window.py::_build_options_section`

#### Diagnosi
Dopo v1078 lo screenshot mostrava ancora una linea chiara di separazione tra "Rimuovi Cartelle Vuote" e "Abilita Sorgenti DB Online". Rivedendo, il colpevole **non era il padding** ma il **separatore orizzontale stesso** (`CTkFrame height=1, fg_color=border`) + i suoi `pady=(4,2)` + i `pady=(2,3)` del checkbox DB = ~11px di discontinuità visibile.

#### Fix v1079
Rimosso completamente il separatore interno. Le tre checkbox sono tutte "Opzioni Catalogazione" — non c'era un gruppo semantico diverso da dividere. Ora tutte e 3 hanno lo stesso `pady=3` e la griglia è lineare:

```
row 0: Titolo "Opzioni Catalogazione"
row 1: [  ] Solo Analisi              pady=3
row 2: [  ] Rimuovi Cartelle Vuote    pady=3
row 3: [  ] Abilita Sorgenti DB Online pady=3    ← era row 5 con separatore a row 4
row 4: "Altre opzioni → tab Avanzate" pady=(6,10)
```

Guadagno: ~12px verticali e aspetto coeso.

### 🚀 FIX-04+05 · Resize lento + scrollbar residua — SOLUZIONE DEFINITIVA
**File:** `gui/main_window.py::_build_left_panel`

#### Due tentativi falliti (v1077 debounce `after(120)`, v1078 state machine `after(160)`) → motivo
Sia il debounce v1077 che la state machine v1078 bindavano un handler aggiuntivo sul `<Configure>` del root con `add="+"`. Il mio handler sovrascriveva correttamente la propria prenotazione, ma **non impediva a CTk di processare gli eventi**: il `CTkScrollableFrame` ha i suoi bind interni sul canvas che fanno `update_idletasks()` + ricalcolo `scrollregion` ad ogni `<Configure>`, indipendentemente da cosa faccia il mio callback esterno. Il mio debounce si aggiungeva al lavoro di CTk invece di sostituirlo.

#### Fix v1079: rimozione totale del CTkScrollableFrame dalla sidebar
Ho misurato le altezze di tutte le sezioni post-compattazione (header 2-righe v1078 + opzioni senza separatore v1079):

| Sezione | Altezza |
|---|---:|
| Header (titolo+v+pulsante profilo+separatore) | ~104 px |
| Directory Musicale | ~78 px |
| Opzioni Catalogazione (3 ck) | ~134 px |
| Gestione Duplicati (3 radio) | ~108 px |
| Cover slim | ~58 px |
| Status + Footer (Avvia/Ferma/Pulisci) | ~92 px |
| **Totale** | **~574 px** |

Un laptop 1366×768 ha ~700px di altezza utile dopo taskbar e title bar. Un desktop 1920×1080 ne ha ~1000px. In entrambi casi 574px stanno senza scrollbar.

Sostituito `ctk.CTkScrollableFrame(...)` con `ctk.CTkFrame(left, fg_color="transparent")`. Rimossa tutta la state-machine `_sidebar_resize_after` / `_sidebar_dragging` / `_sidebar_last_size` / `_on_root_configure` (v1077+v1078, ~80 righe eliminate). Nessun bind aggiuntivo sul root.

**Trade-off documentato**: se l'utente riduce la finestra sotto ~600px di altezza le ultime sezioni verranno tagliate (behavior tk nativo, zero lag). I bottoni azione sono ancorati al bottom con `side="bottom"` quindi restano sempre raggiungibili. Nessuna regressione funzionale.

**Bonus**: la scrollbar visibile all'apertura in v1078 (anche quando non serviva) sparisce automaticamente.

### 🎨 FIX-06 · Angoli flyout rounded come il pulsante
**File:** `gui/main_window.py::_show_profile_panel`, creazione sub-flyout

Il pulsante profilo ha `corner_radius=16` (stile pill). I `CTkToplevel` con `overrideredirect(True)` invece mostravano 4 quadratini grigi agli angoli perché Windows non applica il clipping sui Toplevel undecorated.

#### Fix v1079
Uso `wm_attributes("-transparentcolor", "#010101")` con color-key: il colore `#010101` viene reso completamente trasparente dalla compositing di Windows. Impostando quel colore come `fg_color` del Toplevel e aumentando il `corner_radius` degli CTkFrame interni a 16 (outer) / 15 (inner), gli angoli rounded del frame "bucano" gli angoli altrimenti quadrati del Toplevel.

Risultato: flyout principale e sub-flyout ora hanno bordi rounded stile pill coerenti col pulsante profilo.

**Nota Windows-specific**: `wm_attributes("-transparentcolor", ...)` funziona solo su Windows. Su Linux/Mac il `try/except` fallisce silenziosamente e gli angoli restano leggermente quadrati (come prima). Dato che il target deployment è Windows, accettabile.

### Bump versione
`v1078` → `v1079` · `filevers=(1,0,7,9)` · `FileVersion 1.0.79` · `ProductVersion 1.0.79`.

### File modificati in v1079
```
gui/main_window.py
  - _refresh_count (dialog duplicati)    → fix _v[0].get() → _v.get()
  - _mk_row (flyout)                     → fill="both" + bind su tutti i widget
  - _build_options_section               → rimosso separatore interno
  - _build_left_panel                    → CTkScrollableFrame rimosso (+ state machine)
  - _show_profile_panel                  → transparentcolor + radius 16
  - sub-flyout (_open_sub)               → transparentcolor + radius 16

version.py                                v1079
version_info.txt                          1.0.79
UPGRADES.md                               questa sezione
```

### Pronto per il pilota
Dopo v1079, le regressioni v1077/v1078 sono chiuse. Prossimo salto naturale:
- Sistema di login con backend dedicato
- Containerizzazione Docker del server di catalogazione
- Deployment su NAS per test con il cliente-amico
- Client GUI che si autentica verso il server

---

## v1080 — Resize fluido, hover-to-open, rifiniture flyout (2026-04-21)
**File:** `gui/main_window.py`, `version.py`, `version_info.txt`

### Contesto
Dai test pilota v1079 sono emersi 6 punti:
1. **Resize ancora lento** — neanche la rimozione del CTkScrollableFrame sidebar ha risolto. L'utente conferma che il problema era presente anche in v1076 prima della barra custom, quindi non è correlato alla sidebar.
2. ✅ Scrollbar sidebar sparita correttamente
3. ✅ Duplicati funzionanti
4. **Hover Aiuto/Esci** — la sottolineatura non funziona su queste due voci a meno che non sia già aperto un sub-flyout laterale.
5. **Angoli flyout rettangolari** — solo le selezioni interne sono rounded, le finestre no.
6. ✅ Gap opzioni risolto

**Rifiniture UX richieste:**
- Pulsante profilo adattato alla lunghezza del nome utente (non a tutto il titolo)
- Dimensioni flyout adattate al contenuto (le finestre vuote di Impostazioni/Lingua avevano le stesse dimensioni di Piani, con scrollbar inutile)
- Hover-to-open dei sottomenu invece che click obbligatorio

### 🚀 FIX-01 · Resize fluido — "hide-tabview-during-drag"
**File:** `gui/main_window.py::_install_resize_handler` (nuovo metodo)

#### Nuova diagnosi
Dopo 3 tentativi falliti (v1077 debounce, v1078 state machine, v1079 rimozione CTkScrollableFrame), l'utente ha confermato che il resize è sempre stato lento, anche in v1076. Questo ha portato a una nuova diagnosi: **il collo di bottiglia non è la sidebar ma il CTkTabview del pannello destro.**

Il `CTkTabview` ha 7 tab (Log, DB Locale, Generi, Cache, Qualità, Caraibica, Avanzate) e ad ogni `<Configure>` ricalcola il layout di **tutte** le tab, non solo di quella visibile:
- Log: `CTkTextbox` grande con line wrapping
- DB Locale: tabella custom con CTkFrame per ogni riga
- Qualità: tabella con sort indicators + click-sort
- Caraibica: checkbox + tooltip singleton
- Avanzate: ~40 checkbox + maintenance tools

Questo è un problema noto di CTk documentato nel loro GitHub.

#### Soluzione v1080: hide-during-drag del tabview
Strategia: al primo `<Configure>` del root, `grid_forget()` del tabview e mostra un semplice `CTkLabel` placeholder "⋯ Ridimensionamento in corso ⋯" al suo posto. Finché arrivano `<Configure>`, il placeholder resta visibile. Dopo 180ms di quiete (release del drag), `grid_forget()` del placeholder e rimetti il tabview con una singola passata di layout.

```python
def _on_configure(e):
    if e.widget is not self.root: return
    sz = (e.width, e.height)
    if sz == self._last_size: return   # ignora Configure duplicati
    self._last_size = sz

    # Hide tabview alla prima variazione
    if not self._tabview_hidden:
        self._tabview.grid_forget()
        self._resize_placeholder.grid(row=1, column=0, padx=20, pady=(0,8), sticky="nsew")
        self._tabview_hidden = True

    # Cancella ripristino pendente, schedula nuovo
    if self._resize_after_id:
        self.root.after_cancel(self._resize_after_id)
    self._resize_after_id = self.root.after(180, _restore)
```

Trade-off onesto: durante il drag (pochi secondi) si vede il placeholder invece delle tab. Feedback visivo chiaro. Al rilascio tutto torna normale con una sola passata di layout.

**Perché funziona ora dove prima non funzionava**: le strategie precedenti debouncavano ma non impedivano al tabview di ricevere i `<Configure>`. Con `grid_forget()`, il widget non è nel layout manager → non riceve Configure → non fa lavoro → drag fluido.

### 🐛 FIX-02 · Hover Aiuto/Esci
**File:** `gui/main_window.py::_show_profile_panel::_mk_row`

#### Bug
```python
def _on_enter(e):
    if self._profile_active_sub != sub_id:   # BUG
        row.configure(fg_color=...)
```

Per Aiuto/Esci `sub_id=None`. Quando nessun sub è aperto `_profile_active_sub=None`. Il check `None != None` → **False** → no highlight. Dopo l'apertura di un sub (es. Piani con `_active_sub="plans"`), su Aiuto `None != "plans"` → True → hover funziona. Ecco perché l'utente vedeva l'hover funzionare solo dopo aver aperto un sub.

#### Fix v1080
```python
if sub_id is None or self._profile_active_sub != sub_id:
```

Voci senza `sub_id` (Aiuto/Esci) ottengono sempre l'highlight. Il check residuo serve solo a preservare il blu sulla voce con sub correntemente aperto.

### 🎨 FIX-03 · Flyout con angoli davvero rounded
**File:** `gui/main_window.py::_show_profile_panel` e `_open_sub`

#### Bug v1079
```python
fly.configure(fg_color="#010101")
fly.wm_attributes("-transparentcolor", "#010101")
```

`configure(fg_color=...)` di CTk agisce sul frame interno di `CTkToplevel`, non sul bg nativo tk. Il `-transparentcolor` richiede che il **bg tk nativo** del Toplevel sia esattamente `#010101`. Quindi gli angoli restavano quadrati perché il color-key non trovava pixel di quel colore.

#### Fix v1080
```python
fly.configure(bg="#010101")   # bg nativo tk.Toplevel, NON fg_color CTk
fly.wm_attributes("-transparentcolor", "#010101")
```

`configure(bg=...)` scrive direttamente sul `tk.Toplevel` sottostante. Ora i 4 pixel agli angoli hanno davvero il colore `#010101` e il color-key di Windows li rende trasparenti. Angoli rounded reali coerenti col pulsante pill.

### 🎨 FIX-04 · Pulsante profilo adattato al nome
**File:** `gui/main_window.py::_build_left_panel`

V1079: `self._profile_btn.grid(row=1, column=0, sticky="ew", pady=(10, 0))`
- `sticky="ew"` faceva espandere il pulsante a tutta la larghezza del `_hdr`

V1080: `self._profile_btn.grid(row=1, column=0, sticky="w", pady=(10, 0))`
- `width=0` e `sticky="w"` → il pulsante ha larghezza naturale del contenuto
- `text=f"  {_username}   "` con spazi extra per padding visivo interno
- Aspetto più "pill" coeso, non "bottone che occupa la sidebar"

### 🎨 FIX-05 · Dimensioni flyout adattate al contenuto
**File:** `gui/main_window.py::_show_profile_panel`

#### Principale
V1079: `300×360` (fisso per 5 voci)
V1080: `260×280` (calibrato: header 58 + 5 righe × 40 + margine ≈ 270)

#### Sub-flyout per-id
```python
sub_sizes = {
    "settings": (280, 160),   # solo placeholder "disponibile in prossima versione"
    "lang":     (280, 220),   # 3 lingue + nota
    "plans":    (320, 440),   # 11 feature + 3 bottoni piano (unico che scrolla)
}
```

Inoltre rimossa `height=300` fissa dal `CTkScrollableFrame` di `_fill_plans` → ora `fill="both", expand=True` riempie il sub senza scrollbar parassita.

Risultato: Impostazioni e Lingua ora sono finestre piccole, compatte, senza scrollbar. Solo Piani resta grande perché ha davvero tanto contenuto.

### 🎨 FEAT-06 · Hover-to-open sottomenu con delay 250ms
**File:** `gui/main_window.py::_show_profile_panel::_mk_row`

Il pattern tipico dei menu contestuali: sostando 250ms sopra una voce con `›` si apre il sottomenu. Se il mouse esce prima dei 250ms, niente apertura (evita flash su passaggi rapidi).

```python
def _on_enter(e):
    # ... highlight ...
    if sub_id is not None:
        # Cancella timer precedente
        if self._hover_open_after is not None:
            self.root.after_cancel(self._hover_open_after)
        # Nuovo timer 250ms
        def _trigger():
            self._hover_open_after = None
            if self._profile_active_sub != sub_id:
                _toggle_sub(sub_id)
        self._hover_open_after = self.root.after(250, _trigger)

def _on_leave(e):
    # ... reset highlight ...
    if sub_id is not None and self._hover_open_after is not None:
        self.root.after_cancel(self._hover_open_after)
        self._hover_open_after = None
```

Il click manuale resta comunque disponibile (il bind `<Button-1>` non è stato rimosso). Quando si passa rapidamente da una voce con sub a un'altra, il timer della prima viene cancellato e parte quello della seconda → cambio sub fluido.

Il timer viene anche cancellato in `_close_profile_flyout` per evitare che un sub si apra dopo che l'utente ha chiuso il menu.

### Bump versione
`v1079` → `v1080` · `filevers=(1,0,8,0)` · `FileVersion 1.0.80` · `ProductVersion 1.0.80`.

### File modificati in v1080
```
gui/main_window.py
  + _install_resize_handler           → nuovo: hide-tabview-during-drag
  - _build_layout                     → chiama _install_resize_handler
  - _build_right_panel                → +self._right_panel ref, +_resize_placeholder
  - _show_profile_panel
      ~ fly.configure(bg=...)         → bg tk nativo per angoli rounded
      ~ main_w/main_h 300/360 → 260/280
      + _hover_open_after state
  - _mk_row
      ~ _on_enter: fix sub_id=None    → highlight Aiuto/Esci
      + hover-to-open con after(250)
      ~ _on_leave: cancel timer hover
  - _open_sub
      ~ sub_sizes per-id              → settings/lang piccole, plans grande
      ~ sub.configure(bg=...)         → angoli rounded
  - _fill_plans                       → rimosso height=300 fissa
  - _close_profile_flyout             → cancel hover timer pendente
  - _build_left_panel
      ~ _profile_btn sticky="w"       → adattato al contenuto
      ~ text con padding interno spaziale

version.py                             v1080
version_info.txt                       1.0.80
UPGRADES.md                            questa sezione
```

### Piano dopo v1080
Se v1080 è stabile, inizia la fase **pilot** come definita con l'utente:
- **FastAPI** backend con endpoint REST per catalogazione
- **JWT** auth (login + refresh tokens)
- **Docker** containerization del server
- **Deploy su NAS** del cliente-amico
- **Client GUI limitato al piano di abbonamento** via server

Il piano dell'utente è già definito e coerente con l'architettura corrente: la separazione `core/cataloger.py` da `gui/main_window.py` rende naturale esporre il core dietro API REST senza cambi architetturali importanti.

---

## v1081 — Resize via minsize/maxsize (decisione architettonica) (2026-04-21)
**File:** `gui/main_window.py`, `version.py`, `version_info.txt`

### Contesto
Dopo 4 tentativi di fix del resize (v1077 debounce, v1078 state machine, v1079 rimozione CTkScrollableFrame, v1080 hide-tabview-during-drag), il problema persiste: la finestra resta congelata durante il drag per circa 3 secondi. La causa architetturale è il `CTkTabview` con 7 tab pesanti che ricalcola layout di tutte le tab ad ogni `<Configure>`, e CTk non espone API per sopprimere questo comportamento senza hackare le internals.

**Decisione dell'utente**: al momento si opta per fissare un range `min/max` stretto in cui il resize sia percepibilmente gestibile; la risoluzione definitiva (probabilmente sostituzione CTkTabview con `tk.Frame`-based custom tabs, oppure porting a un altro toolkit GUI) viene rimandata al post-pilot. Priorità: funzionalità del programma su Windows e macOS.

### 🔧 FIX-01 · Resize gestito via min/max size
**File:** `gui/main_window.py::__init__`

```python
self.root.geometry("1300x860")
self.root.minsize(1100, 720)   # laptop 1366×768 ok, niente scroll
self.root.maxsize(1500, 960)   # range stretto, drag lento dura poco
```

Rationale dei valori:
- **min 1100×720**: larghezza permette la sidebar (400px) + tab content con colonne leggibili; altezza supporta le 4 sezioni sidebar + azioni senza scroll su laptop 1366×768
- **max 1500×960**: limita il drag a ~400px massimi orizzontali, ~240px verticali → il lag dura meno di 1-2 secondi invece dei 3-5 attuali

Note:
- Il tasto **Maximize** della titlebar di Windows non è soggetto al `maxsize` e può ingrandire a tutto schermo (comportamento standard tk)
- Il placeholder v1080 è stato rimosso insieme a `_install_resize_handler`: non serviva, e creava rumore visivo non voluto durante il drag
- Il riferimento `self._right_panel` e `self._resize_placeholder` introdotti in v1080 sono stati rimossi

### Post-v1081 — Pilot
Da qui la roadmap passa al backend:
1. FastAPI + JWT auth
2. Docker container del server
3. Deploy su NAS dell'utente (da configurare — nessun dato in memoria)
4. Client GUI autentica via HTTP, JWT carries plan → feature gating
5. Piani ripensati con logica di business (vedi proposta)
6. Richiesta upgrade dal flyout "Piani" con dialog comparativo

### File modificati in v1081
```
gui/main_window.py
  ~ __init__: minsize/maxsize stretti
  - _install_resize_handler              rimosso
  - _build_layout: chiamata a _install   rimossa
  - _build_right_panel
      - self._right_panel = right        rimosso
      - self._resize_placeholder label   rimosso

version.py                                v1081
version_info.txt                          1.0.81
UPGRADES.md                               questa sezione
```

---

## v1082 — Integrazione client-server (pilot) (2026-04-22)
**File:** `run_gui.py`, `gui/login_window.py`, `gui/main_window.py`, `services/{api_client,jwt_store}.py`, `config/{app_config,user_plans}.py`, `requirements.txt`, `version.{py,info.txt}`

### Contesto
Primo deploy integrato client↔server per la fase pilot:
- Backend FastAPI già in produzione su Synology DS415+
- Client GUI ora autentica via HTTPS con JWT
- Flyout "Piani" mostra dati dal server, richiesta upgrade con dialog comparativo
- Fallback offline se NAS unreachable

### 🎯 FEAT-01 · Login window all'avvio
**File:** `gui/login_window.py` (nuovo), `run_gui.py` (riscritto)

Nuovo flusso avvio:
1. `run_gui.py` prova a leggere `data/session.json` (jwt_store)
2. Se presente → ping server
   - Server up + token valido → skip login, avvia main window
   - Server down + `offline_ok=True` → avvia main window in modalità offline (piano dall'ultimo JWT)
   - Server up + token scaduto → auto-refresh via refresh token; se fallisce → login
3. Login window: email + password + URL server configurabile + "Ricorda email"

Finestra 440×520, stile coerente con la main window, validazione client-side, threading per non bloccare UI durante la chiamata di rete.

### 🎯 FEAT-02 · api_client con refresh automatico
**File:** `services/api_client.py` (nuovo)

Wrapper su `requests.Session` con:
- Bearer token auto-injected su ogni chiamata
- Refresh trasparente al primo 401: chiama `/auth/refresh` con refresh token, aggiorna l'access, ritenta la chiamata originale. Se anche il refresh fallisce → `AuthError` (login richiesto).
- Timeout separati: 5s connect, 30s read
- 3 tipi di eccezioni chiare: `ApiError` (4xx/5xx), `AuthError` (401 o refresh fallito), `ServerUnreachableError` (connessione non stabilita)
- Helper `get_features_from_stored_token()` per modalità offline (decodifica payload JWT lato client, senza verifica firma — il server valida comunque su ogni chiamata reale)

### 🎯 FEAT-03 · jwt_store persistente
**File:** `services/jwt_store.py` (nuovo)

Salva `access_token` + `refresh_token` + metadati sessione in `data/session.json`. Permessi 0600 su Unix. Su Windows NTFS usa le ACL utente di default.

Onestà: i token sono in chiaro. Equivalente di un cookie "remember me" browser. Post-pilot si può integrare `keyring` (Windows Credential Manager / macOS Keychain) per cifratura nativa.

### 🎯 FEAT-04 · Dialog comparativo upgrade piani
**File:** `gui/main_window.py::_show_upgrade_dialog`

Dal flyout "Piani di abbonamento", se l'utente ha upgrade disponibili, un pulsante "⬆ Richiedi upgrade del piano" apre una finestra modale con:

- **Tabella comparativa** (piano corrente vs piani upgrade): 12 feature + 2 limiti. ✓/— per ogni cella. Piano corrente evidenziato in surface2, piani upgrade in blu primary.
- **Pulsanti "Richiedi <piano>"** sotto ogni colonna upgrade
- Click apre un secondo dialog per messaggio opzionale all'admin
- Invio richiesta via `/plans/upgrade-request` in thread separato
- Messaggio di conferma "Richiesta inviata, attendi approvazione"

Modalità automatica: se `api_client=None` (standalone dev), il flyout mostra i vecchi bottoni base/pro/advanced per switch locale — backward compat completa.

### 🎯 FEAT-05 · main_window retrocompatibile
**File:** `gui/main_window.py::__init__`

```python
def __init__(self, root, api_client=None, user_info=None):
```

Due modalità:
- **Server mode** (parametri valorizzati): piano dal JWT, feature dal server, flyout Piani con upgrade request
- **Locale mode** (parametri None): comportamento pre-v1082, piano da `config/user_plans.py`, flyout con switch diretto

Il `run_gui.py` nuovo passa sempre la modalità server; il vecchio `run_gui.py` (se qualcuno lo tiene) continua a funzionare come prima.

### 🎯 FEAT-06 · URL server configurabile
**File:** `config/app_config.py` (nuovo)

File `data/client_config.json` auto-generato al primo avvio. Campi:
- `server_url`: default `http://localhost:8000` (dev) oppure `https://api.choros27.synology.me` (configurabile)
- `offline_ok`: se True, parte la GUI anche con server unreachable usando last-known JWT
- `remember_email`, `last_email`: per la finestra di login

Override via env var `MCS_SERVER_URL` per packaging multi-target.

### 🐛 FIX-07 · Server email default (scoperto durante integrazione)
**File:** `music-cataloger-server/app/config.py`

Il default `ADMIN_EMAIL="admin@localhost"` veniva rifiutato da Pydantic `EmailStr` (manca TLD). Cambiato a `admin@example.com`. Non impattava Pedro che nel `.env` aveva già `admin@choros27.synology.me`, ma avrebbe crashato al login qualsiasi nuovo deploy che partisse dai soli default.

### Test end-to-end validati
```
✓ pedro login → plan=base
✓ pedro richiede upgrade a pro → pending
✓ admin vede richiesta pending
✓ admin approva → status=approved
✓ dopo approve, pedro plan=pro (era base, ora pro)
✓ pedro ora ha catalog_bpm=True, tab_caribbean=True
✓ doppia richiesta pending bloccata
✓ refresh trasparente al 401
✓ ServerUnreachableError su URL inesistente
```

### File modificati/nuovi in v1082
```
NUOVI:
  gui/login_window.py               ← finestra login modale
  services/api_client.py            ← HTTP client + JWT auto-refresh
  services/jwt_store.py             ← persistenza token su disco
  config/app_config.py              ← config runtime client (URL server)
  requirements.txt                  ← deps pip client

MODIFICATI:
  run_gui.py                        ← flow login → main window
  gui/main_window.py
      __init__                      ← +api_client, +user_info opzionali
      _fill_plans (flyout piani)    ← modalità server vs locale
      _show_upgrade_dialog          ← NUOVO: comparativo + request
  config/user_plans.py              ← piani v1081 confermati

version.py                           v1082
version_info.txt                     1.0.82
UPGRADES.md                          questa sezione
```

### Prossimi step
- Integrare `core/cataloger.py` come worker async sul server (endpoint `/catalog/start` oggi è scheletro)
- Streaming log via WebSocket dal server al client durante catalogazione
- Registrazione self-service utenti (`POST /auth/register`) + email di conferma
- Cambio password (`POST /auth/change-password`)
- UI admin integrata: tab Avanzate → nuova sezione "Richieste Upgrade" (solo per admin) per approvare senza curl

---

## v1083 — Fix integrazione GUI e UX (2026-04-23)
**File:** `run_gui.py`, `gui/main_window.py`, `gui/login_window.py`, `gui/app_icon.py` (nuovo), `music_cataloger.spec`

### 🐛 FIX-01 · UnicodeEncodeError su Windows (cp1252)
**File:** `run_gui.py`

All'avvio del client su Windows (EXE o Python nativo), il print con caratteri
come `✓`, `⚠`, `→` crashava con:
```
UnicodeEncodeError: 'charmap' codec can't encode character '\u2713'
```

Fix: forzato `sys.stdout`/`sys.stderr` a UTF-8 con `io.TextIOWrapper` come
primissima operazione di `run_gui.py`, prima di qualsiasi print o import.
Gestito anche il caso EXE `--windowed` dove `sys.stdout` è None (dummy sink).

### 🐛 FIX-02 · Icona finestre non attaccata (sviluppo + EXE)
**File:** `gui/app_icon.py` (nuovo), `gui/main_window.py`, `gui/login_window.py`

Il path dell'icona era hardcoded con `Path(__file__).parent.parent` che in
PyInstaller onedir/onefile non funziona (i file statici finiscono in
`sys._MEIPASS`). La login window non settava nemmeno l'icona.

Creato helper `gui/app_icon.py` con:
- `_resource_root()` che rileva `sys._MEIPASS` se presente
- `find_icon_file()` con fallback ordinato (.ico → .png)
- `set_window_icon(window)` stabile per CTk e Toplevel
- `get_title_icon_photo()` per il logo sopra "Music Cataloger" nella
  login window (sostituisce l'emoji `🎵`)

### 🎯 FEAT-03 · Logout funzionante
**File:** `gui/main_window.py::_handle_exit`

Sostituito placeholder con logout reale:
- Conferma con `messagebox.askyesno`
- `jwt_store.clear()` + `api_client.logout()`
- Chiusura main window con `root.quit() + destroy()`
- Messaggio conferma all'utente

In un prossimo step si può far riaprire automaticamente la LoginWindow
senza riavvio, ma per v0.0.2.2 il flusso "logout → riavvia app" è
sufficiente e chiaro.

### 🔧 spec — imports hidden v1082 + app_icon
`music_cataloger.spec` aggiornato con:
- `config.app_config`
- `services.api_client`, `services.jwt_store`
- `gui.login_window`, `gui.app_icon`

Senza questi hiddenimports, `pyinstaller --clean` costruiva l'EXE ma al
runtime dava `ModuleNotFoundError` sui nuovi moduli v1082.

---

## Server v0.1.2 — Fix permessi NAS (2026-04-23)
**File:** `music-cataloger-server/Dockerfile`

### 🐛 FIX — sqlite3.OperationalError: unable to open database file

Il container gira come user non-root `mcs` (UID random assegnato da
useradd). Quando monta `/volume1/docker/music-cataloger/data` dal NAS,
i permessi della cartella host (proprietà `Choros`, NO scrittura altri)
bloccavano `mcs` dal creare `app.db`.

Fix a due livelli:
1. **Dockerfile**: `useradd -u 1000 -g 1000` fissa UID/GID prevedibili
2. **Istruzione NAS**: `sudo chown -R 1000:1000 /volume1/docker/music-cataloger/{data,output}`

Aggiunto anche `mkdir -p /srv/app/data /output` nel Dockerfile per
creare le cartelle prima del chown e consentire a SQLite di scriverci
anche senza bind mount esterno (utile per test locali).

---

## v1084 — Self-service signup + admin panel + change password (2026-04-24)
**File:** `gui/login_window.py`, `gui/main_window.py`, `services/api_client.py`

### 🎯 FEAT-01 · /auth/register endpoint + dialog signup
**Server:** `app/api/auth.py::register` + `RegisterRequest` (email/username/password)
- Validazione: email valida, username 2-64 char, password min 8 char
- Email duplicata → 409 Conflict
- Nuovo utente sempre con piano `base`, `is_admin=False`, `is_active=True`
- Nessuna verifica email per il pilot (post-MVP: token via SMTP)

**Client:** `LoginWindow._open_register_dialog()`
- Link "Non hai un account? Registrati" sotto il bottone Accedi
- Dialog 420×440 con email + username + password + conferma
- Validazione client-side prima di chiamare il server (email format,
  password length, conferma match)
- Su successo: chiude dialog, pre-compila l'email nella login window,
  status verde "Account creato ✓ Accedi con email e password"

### 🎯 FEAT-02 · /auth/change-password endpoint + dialog
**Server:** `app/api/auth.py::change_password`
- Richiede password corrente per sicurezza
- Nuova password ≠ corrente, min 8 char
- 403 se current errata, 400 se nuova == corrente
- I JWT esistenti NON sono invalidati (scelta MVP)

**Client:** `MainWindow._show_change_password_dialog()`
- Voce "🔒 Cambia password" nella sezione "Account" del flyout
  Impostazioni (visibile solo in modalità server)
- Dialog 440×400 con password corrente + nuova + conferma
- Threading per non bloccare GUI durante chiamata server
- Mappatura errori: 403 → "Password corrente errata", 400 → "Non valida"

### 🎯 FEAT-03 · Pannello admin nel tab Avanzate
**File:** `gui/main_window.py::_build_admin_section`

Solo visibile se `user_info.is_admin == True`. Mostra:
- Header "👑 Pannello Amministratore — Richieste Upgrade"
- Conteggio richieste pending in tempo reale (verde se 0, blu se ≥1)
- Lista pending con per ogni riga:
  - User #ID, transizione `base → pro` con badge piano
  - Messaggio motivazione utente (troncato a 80 char)
  - Timestamp creazione
  - Bottone "✓ Approva" verde + "✗ Rifiuta" rosso
- Click Approva: `simpledialog.askstring` per nota opzionale, POST
  `/admin/upgrade-requests/{id}/approve`, refresh lista
- Click Rifiuta: motivazione obbligatoria, refresh lista
- Pulsante "🔄 Aggiorna" per ri-fetch manuale

Tutte le chiamate in thread separato per non bloccare GUI.

### 📊 Test E2E pilot completo (12 step validati)
```
✓ cliente registra account self-service → plan=base
✓ cliente login + change_password + re-login con nuova
✓ cliente richiede upgrade pro con messaggio
✓ admin vede pending nel pannello
✓ admin approva via API (simulato pannello GUI)
✓ cliente rilogga → plan=pro, catalog_bpm/tab_caribbean abilitati
✓ admin rifiuta seconda richiesta
✓ piano cliente non cambia dopo rifiuto
```

### Endpoint server v0.1.3
Riepilogo totale endpoint disponibili:
```
PUBLIC:
  GET  /                            - info server
  GET  /health                      - liveness
  GET  /plans                       - lista piani
  POST /auth/register               - signup (v0.1.3)

AUTH:
  POST /auth/login                  - email+password → JWT pair
  POST /auth/refresh                - refresh access token
  GET  /auth/me                     - utente corrente
  POST /auth/change-password        - cambio password (v0.1.3)

USER:
  GET  /plans/me                    - piano + features
  POST /plans/upgrade-request       - richiedi upgrade
  GET  /plans/my-requests           - storico mie richieste

ADMIN:
  GET  /admin/upgrade-requests      - elenco pending
  POST /admin/upgrade-requests/{id}/approve
  POST /admin/upgrade-requests/{id}/reject

CATALOG (placeholder MVP — worker da integrare):
  POST /catalog/start
  GET  /catalog/status/{id}
  GET  /catalog/results/{id}
  POST /catalog/cancel/{id}
```

### Prossimi step
- ⚡ Worker catalogazione: integrare `core/cataloger.py` come task async
  con aggiornamento Job in DB e streaming log
- 🔄 Auto-updater EXE: endpoint `/version/latest` + check al boot client
- 📧 Email notifica admin su nuova richiesta upgrade (SMTP)
- 🌐 WebSocket per streaming log catalogazione real-time
- 🍎 Test build PyInstaller su macOS e Linux

---

## v1085 — Worker catalogazione + Auto-update + Plan enforcement (2026-04-25)
**File:** `services/{api_client,catalog_reporter,updater}.py` (nuovo: 2),
`gui/main_window.py`, `run_gui.py`, `music_cataloger.spec`

### 🎯 FEAT-01 · Worker catalogazione client-side con tracking server
**Architettura scelta:** *client-side execution + server-side tracking*.

Motivazione: la libreria MP3 sta sul PC del cliente, non sul NAS. Far
catalogare il server richiederebbe upload massicci. Quindi:
- Il **client** esegue `MusicCataloger` localmente (come fa già)
- Il **server** riceve notifiche di start/progress/complete/fail
- Il server applica le **quote del piano** (max files, max runs/day)
- Il server tiene il **log centralizzato** dei job (admin lo vede)

#### Nuovi endpoint server (9):
```
POST  /catalog/start              → 201 {job_id, quota_remaining}
                                  → 402 se quota superata (files o runs)
                                  → 403 se opzione non concessa al piano
POST  /catalog/{id}/progress      → update files_done + log_chunk
POST  /catalog/{id}/complete      → marca completed + salva report
POST  /catalog/{id}/fail          → marca failed + error_message
POST  /catalog/{id}/cancel        → marca cancelled
GET   /catalog/{id}/status        → stato corrente (owner o admin)
GET   /catalog/{id}/logs?after=N  → log incrementale (polling-friendly)
GET   /catalog/{id}/results       → report JSON finale
GET   /catalog/my-jobs            → ultimi 20 job propri
GET   /admin/all-jobs             → admin: tutti i job di tutti
```

Schema DB: `Job` + `JobLog` (cascade delete su Job).

#### Test E2E validati: 18/18
```
✓ Base 600 file → 402 (quota max 500)
✓ Base 100 file → 201, quota_remaining=2
✓ Base con analyze_bpm → 403
✓ Pro 600 file con bpm+cover → 201
✓ Update progress 25%, status, logs incrementali
✓ Complete con report JSON, results endpoint
✓ Re-complete su completato → 409
✓ Cross-user → 403
✓ Admin all-jobs vede 2 job totali
✓ Pro my-jobs filtra correttamente
✓ Base: 4° run nello stesso giorno → 402 (quota max 3)
✓ Fail endpoint con error_message
✓ Cancel endpoint
```

#### Recovery automatico
Al boot, server marca come `failed` tutti i job rimasti in `running`
(server è stato riavviato durante un'esecuzione cliente). L'utente
può semplicemente rilanciare la catalogazione.

### 🎯 FEAT-02 · Client: catalog_reporter
**File:** `services/catalog_reporter.py` (nuovo)

Wrapper sopra `ApiClient` che gestisce il ciclo vita di un singolo job:
- `start(path, files_total, options)` → notifica server, ritorna job_id
  o None se rifiutato (con `last_error` leggibile per messagebox)
- `progress(...)` → push asincrono su coda interna, thread sender
  invia al server senza bloccare GUI/cataloger
- `complete(...)` / `fail(...)` → drain coda, notifica finale sincrona
- Errori di rete loggati ma **non interrompono** la catalogazione locale

In `MainWindow._run`:
- Quick scan ricorsivo `*.mp3` per stimare `files_total`
- Costruisce `options` dict per validazione server
- Se reporter ritorna None per quota → showerror + abort
- Se reporter è None per server offline → degrade graceful: cataloga
  comunque in locale senza tracking

In `MainWindow._poll_queue`:
- Ogni `log` line → `reporter.progress(log_chunk=line)`
- `done` (rc=0) → `reporter.complete(report={processed, moved, ...})`
- `aborted` → `reporter.cancel()`
- `error` → `reporter.fail(msg)`

### 🎯 FEAT-03 · Plan enforcement con overlay lock
**File:** `gui/main_window.py::_apply_plan_restrictions`

Riscrittura completa della funzione (prima era un `pass` con commento
"implementazione semplificata"). Ora:
- Legge features da `user_info.features` se in modalità server
  (decodificate dal JWT), fallback a `config.user_plans` in locale
- Per ogni tab non concesso (`tab_cache`, `tab_caribbean`, `tab_advanced`):
  nasconde i widget originali e mostra un **overlay lock** con:
  - 🔒 icona grande
  - Nome feature ("Cache metadati", "Classificazione Caraibica"...)
  - "Disponibile dal piano X"
  - "Il tuo piano attuale: Y"
  - Bottone "⬆ Richiedi upgrade" (solo se in modalità server) che apre
    direttamente il dialog comparativo upgrade
- Disabilita check `Abilita Sorgenti DB Online` se piano non lo supporta
- Auto-applicato al boot via `self.root.after(300, ...)` dopo che tutti
  i tab sono costruiti

L'overlay è non-distruttivo: i widget originali non vengono distrutti
(solo `pack_forget`). Se l'utente fa upgrade durante la sessione, basta
ri-chiamare `_apply_plan_restrictions()` e i tab tornano normali.

### 🎯 FEAT-04 · Auto-updater EXE
**File:** `services/updater.py` (nuovo), `app/api/updates.py` (nuovo)

#### Server (2 endpoint):
- `GET /version/latest` → legge `data/version.json` con metadati ultima
  release (version, filename, sha256, changelog, mandatory). 404 se
  non c'è nessuna release pubblicata.
- `GET /downloads/{filename}` → serve file EXE da `data/releases/`.
  Sanity: rifiuta path traversal e estensioni non consentite (solo
  `.exe`, `.zip`, `.dmg`, `.AppImage`, `.tar.gz`).

#### Client (`services/updater.py`):
- `check_and_offer_update(api_client, parent_window)` → entry point
  chiamato dal `run_gui.py` 1.5s dopo l'apertura della main window
- Check eseguito in thread separato (non blocca GUI)
- Confronto versioni con tuple semver-friendly (`v1085` > `v1084`,
  `v1.0.85` > `v1.0.84`)
- Se siamo in modalità script (`sys.frozen=False`): logga e skippa
  (in dev rebuild manuale è atteso)
- Se siamo EXE PyInstaller: mostra dialog 500×460 con changelog +
  bottoni "Aggiorna ora" / "Più tardi"
- Click "Aggiorna ora": scarica EXE in `%TEMP%/music_cataloger_update/`
  con verifica SHA256 (se fornito), genera batch `updater.bat` che
  attende chiusura EXE corrente, lo sostituisce, riavvia, infine
  chiude l'app corrente
- Supporto flag `mandatory`: se True, blocca pulsante "Più tardi" e
  intercetta WM_DELETE_WINDOW

#### Workflow rilascio (per Pedro):
```bash
# Sul NAS, dopo aver buildato il nuovo EXE su Windows:
scp Music_Cataloger_v1086.exe pedro@nas:/volume1/docker/music-cataloger/data/releases/
sha256sum /volume1/docker/music-cataloger/data/releases/Music_Cataloger_v1086.exe
# Edita /volume1/docker/music-cataloger/data/version.json:
{
  "version": "v1086",
  "filename": "Music_Cataloger_v1086.exe",
  "sha256": "<hash sopra>",
  "changelog": "- ...",
  "mandatory": false
}
```
Tutti i client esistenti al prossimo avvio vedranno il prompt.

### 🔧 spec PyInstaller aggiornato
Aggiunti `services.catalog_reporter` e `services.updater` agli
`hiddenimports`.

### Stato funzionalità v1085
| Funzione | Stato |
|---|---|
| Auth (login/refresh/me/change-pwd/register) | ✅ |
| Piani (list/me/upgrade-request/admin approve/reject) | ✅ |
| Catalog tracking server-side | ✅ |
| Client cataloga in locale + invia progress | ✅ |
| Quote piano (max files/run, max runs/day) | ✅ |
| Plan enforcement GUI con overlay lock | ✅ |
| Pannello admin in tab Avanzate | ✅ |
| Auto-update EXE (Windows) | ✅ |
| Email notifiche admin | ⏳ post-MVP |
| WebSocket streaming log | ⏳ post-MVP |
| Rate limiting Redis | ⏳ post-MVP |
| Build Mac/Linux | ⏳ TODO |

### Endpoint server v0.1.4 — riepilogo (24 endpoint)
```
PUBLIC:
  GET  /                              GET  /health
  GET  /plans                         POST /auth/register
  GET  /version/latest                GET  /downloads/{filename}

AUTH:
  POST /auth/login                    POST /auth/refresh
  GET  /auth/me                       POST /auth/change-password

USER:
  GET  /plans/me                      POST /plans/upgrade-request
  GET  /plans/my-requests
  POST /catalog/start                 POST /catalog/{id}/progress
  POST /catalog/{id}/complete         POST /catalog/{id}/fail
  POST /catalog/{id}/cancel           GET  /catalog/{id}/status
  GET  /catalog/{id}/logs             GET  /catalog/{id}/results
  GET  /catalog/my-jobs

ADMIN:
  GET  /admin/upgrade-requests        POST /admin/upgrade-requests/{id}/approve
  POST /admin/upgrade-requests/{id}/reject
  GET  /admin/all-jobs
```

---

## v1085b — Hotfix login bug + default produzione (2026-04-25)
**File:** `gui/login_window.py`, `config/app_config.py`

### 🐛 BUG-01 · `NameError: cannot access free variable 'e'` al login
**File:** `gui/login_window.py::_login_worker` + register dialog

Crash al login con qualunque errore lato server (es. `Server irraggiungibile`,
`AuthError`, `ApiError`):
```
File "login_window.py", line 245, in <lambda>
    "Email o password errate. Riprova.", detail=str(e)))
                                                    ^
NameError: cannot access free variable 'e' where it is not associated
with a value in enclosing scope
```

Causa: in Python 3.11+ le variabili catturate da `except ... as e:`
vengono **eliminate dallo scope al termine del blocco except**. Le
`lambda` chiuse sopra `e` falliscono al momento dell'esecuzione,
perché vengono chiamate via `root.after(0, ...)` *fuori* dallo scope.

Fix in 4 punti del file:
```python
# PRIMA (rotto):
except AuthError as e:
    self.root.after(0, lambda: self._on_error("...", detail=str(e)))

# DOPO (funzionante):
except AuthError as e:
    err_str = str(e)   # cattura il valore in variabile locale
    self.root.after(0, lambda: self._on_error("...", detail=err_str))
```

Audit completo eseguito su `main_window.py`, `catalog_reporter.py`,
`api_client.py`, `updater.py` — nessun altro pattern simile trovato.

### 🐛 BUG-02 · Default server_url ancora puntato a localhost
**File:** `config/app_config.py`

`DEFAULT_SERVER_DEV` (localhost:8000) era ancora il default in
`ClientConfig.server_url`. Doveva essere `DEFAULT_SERVER_PROD`
(`https://api.choros27.synology.me`) dato che siamo in produzione.

Inoltre la porta dev errata: era `:8000`, doveva essere `:8020` per
allinearsi alla configurazione NAS reale.

Fix:
- Default ClientConfig.server_url = DEFAULT_SERVER_PROD
- DEFAULT_SERVER_DEV corretto a `localhost:8020`

### 🎯 FEAT-03 · Auto-migrazione client_config.json legacy
**File:** `config/app_config.py::_load_or_create`

Gli utenti che hanno già un `data/client_config.json` salvato dalle
versioni iniziali (server=localhost:8000) sarebbero rimasti bloccati
sul localhost anche dopo l'aggiornamento, perché il loader privilegia
il file esistente sui default.

Fix: al caricamento, se `server_url` è uno degli URL legacy
conosciuti (`http://localhost:8000`, `http://localhost`,
`https://localhost:8000`) viene **sovrascritto** automaticamente
col DEFAULT_SERVER_PROD e il file viene riscritto su disco. Gli
altri campi (`last_email`, `remember_email`, `offline_ok`) sono
preservati.

Test verificato: legacy localhost:8000 + last_email='test@example.me'
→ post-migration server=NAS, last_email preservato.

---

## v1085c — Hotfix bug GUI + admin set-plan endpoint (2026-04-26)

### 🐛 BUG-01 · Crash al click "Pannello Amministratore"
**File:** `gui/main_window.py::_admin_refresh_requests` riga 4023

Stesso pattern Python 3.11+ già fixato in v1085b: `lambda` cattura
`e` di `except Exception as e:` ma `e` viene cancellato dallo scope
prima dell'esecuzione. Audit completo eseguito e trovati altri 2 punti
nello stesso file (`_admin_approve` e `_admin_reject`). Tutti fixati.

### 🐛 BUG-02 · Username "DJ" e piano sbagliato in modalità server
**File:** `gui/main_window.py::_show_profile_panel` + header card riga 1808

`_show_profile_panel` leggeva sempre `config.user_plans.get_plan()`
che ritorna il piano salvato localmente in `data/user_plan.json`,
ignorando completamente `self.user_info` ricevuto dal server.

Risultato: utenti loggati col server vedevano "DJ" come username
(default offline di user_plans.py) e il piano salvato in passato sul
filesystem locale invece del piano vero dal JWT.

Fix: in modalità server (`api_client != None`) costruisco un oggetto
SimpleNamespace virtuale con username/plan/display_name da
`self.user_info`. Stesso fix applicato al pulsante profilo nella
header card della sidebar (riga 1808).

### 🐛 BUG-03 · Spam `[CatalogReporter] progress send failed: HTTP 409`
**File:** `services/catalog_reporter.py`

Quando l'utente premeva "Stop" durante una catalogazione lunga, il
CMD si riempiva di centinaia di righe `progress send failed: HTTP 409
Job non in esecuzione (status=cancelled)`. Causa:
1. La coda interna del reporter conteneva ~100 update accodati
2. Quando l'utente preme stop, il sender thread svuota la coda
   provando a inviarli al server
3. Ma il job lato server è ormai `cancelled`, quindi ogni progress
   viene rifiutato con 409

Due fix complementari:
- **`cancel()` ora purge la coda PRIMA di chiamare `/cancel`** così
  gli update pending vengono scartati senza essere spediti
- **`_sender_loop` rileva "dead job" sui 409/404** e passa a "drain
  silenzioso": continua a svuotare la coda senza invocare il server
  (e senza spammare il log). Errori transitori di rete (timeout,
  ConnectionError) restano gestiti come prima

### 🐛 BUG-04 · `invalid command name "...update"` dopo login
**File:** `gui/login_window.py::_on_success`

Customtkinter accoda callback periodiche (DPI scaling, dimensioni
scrollbar) sul mainloop del Tk root. Quando la login window viene
distrutta, queste callback restano accodate ma non hanno più widget
su cui agire — Tk emette warning `invalid command name "...update"`.

Fix: prima della destroy, cancello esplicitamente tutti i pending
after-id con `tk.call('after', 'info')` + `after_cancel()`.

### 🎯 FEAT-01 · `POST /admin/users/{id}/set-plan`
**File:** `app/api/plans.py`

Nuovo endpoint admin per forzare il piano di un utente bypassando il
flusso upgrade-request. Casi d'uso:
- Ripristinare admin se per qualche motivo il piano nel DB è disallineato
- Test rapidi senza dover creare richieste upgrade
- Casi eccezionali fuori dal flusso normale

```http
POST /admin/users/1/set-plan
Authorization: Bearer <admin_jwt>
Content-Type: application/json

{ "plan": "advanced" }
```

Ritorna `{ok, user_id, username, email, old_plan, new_plan}`.
L'utente target dovrà rifare login per vedere il nuovo piano.

### 🎯 FEAT-02 · `GET /admin/users`
Lista tutti gli utenti registrati (per ottenere user_id da passare
a set-plan). Test verificati: 5/5.

### Endpoint server v0.1.5 — riepilogo (26 endpoint)
Aggiunti rispetto a v0.1.4:
```
GET  /admin/users
POST /admin/users/{user_id}/set-plan
```

---

## v1085d — Hotfix dialoghi + word boundary caraibica + UX (2026-04-27)

### 🐛 BUG-01 · Dialog upgrade pulsanti non cliccabili
**File:** `gui/main_window.py::_show_upgrade_dialog`

Il dialog di confronto piani aveva i pulsanti "Richiedi upgrade" non
visibili/non cliccabili e una scrollbar orizzontale inutile. Causa:
**pack order errato**. Il body veniva packato con `expand=True`
PRIMA del btn_bar, quindi si prendeva tutto lo spazio verticale e
spingeva il btn_bar fuori dalla finestra.

Fix completo (riscrittura della funzione):
- Pack order corretto: `titlebar TOP → btn_bar BOTTOM → header → body`
  (il bottom è pinnato PRIMA del body con `side="bottom"`)
- Titlebar custom (`overrideredirect=True`) → no barra Windows bianca
- `columnconfigure(uniform="cols")` su tutte le colonne piano →
  larghezza uniforme tra Base/Pro/Advanced
- `CTkScrollableFrame` con scrollbar customizzata (no orizzontale)
- Drag della finestra dalla titlebar (mouse trascinamento)
- Dimensioni ridotte: `680px` di altezza max (entra in 1080p)

### 🐛 BUG-02 · Dialog registrazione pulsante mancante in fondo
**File:** `gui/login_window.py::_open_register_dialog`

Stesso problema del dialog upgrade: btn_row con `side="bottom"`
packato dopo header/form/status che hanno già preso lo spazio.
Pedro doveva premere Invio per inviare.

Fix con stessa struttura: titlebar custom + btn_row pinnato bottom
PRIMA del body. Ora il pulsante "✓ Registrati" è sempre visibile.

### 🐛 BUG-03 · Flyout profilo non si chiude su mouse-leave
**File:** `gui/main_window.py::_show_profile_panel`

Il polling con `winfo_pointerx()` poteva fallire su Windows con DPI
scaling: il puntatore in coordinate scalate, i widget in coordinate
non-scalate → `_is_mouse_inside_flyouts()` ritornava sempre True.

Fix: aggiunto bind `<Leave>`/`<Enter>` direttamente sul frame `inner`
del flyout come fallback robusto. Funziona indipendentemente dalle
coordinate. Il polling esistente resta come secondo livello di sicurezza.

### 🐛 BUG-04 · Matching caraibica falsi positivi (Timbaland → Salsa)
**File:** `core/genre_classifier.py`

`if indicator in combined` matcha substring ovunque. "timba" come
indicatore Salsa matchava "Timbaland" → `Apologize` di Timbaland
finiva in `Latin/Salsa/`. Stesso problema con "salsa" che matchava
"salsacake" o "merengue" che matchava "merenguestyle".

Fix: tutti i matching ora usano word boundary regex `\bX\b`:
- `detect_latin_subgenre()` per `bachata_indicators` e `salsa_indicators`
- Match letterali "salsa"/"bachata" nel title
- `infer_genre_from_filename()` per `latin_indicators_generic`

I pattern sono compilati una sola volta per istanza e cachati in
`self._compiled_indicators` (efficienza per librerie con migliaia
di file). Test verificati:
```
timba       in 'timbaland - apologize'  → False (era True bug)
salsa       in 'salsacake recipe'       → False (era True bug)
salsa       in 'la salsa de mi tierra'  → True  (corretto)
el gran combo in 'el gran combo - achilipu' → True (corretto)
merengue    in 'merenguestyle - random' → False (era True bug)
```

### 🎨 FEAT-01 · Icona titolo principale ingrandita
**File:** `gui/main_window.py` riga 1910

Da `_get_icon("title_icon", 28)` a `_get_icon("title_icon", 44)`.
L'icona accanto a "Music Cataloger" nella header ora è 16px più
grande, più visibile, allineata col font 20pt del titolo.

### 📝 v1085d note operative
- Per generare un nuovo `.ico` da PNG su Windows: usare Python
  `from PIL import Image; Image.open("icon.png").save("icon.ico", sizes=[(16,16),(32,32),(48,48),(256,256)])`
- Per calcolare hash SHA256 su Windows: `certutil -hashfile file.exe SHA256`
- Per editare `version.json` su NAS Synology senza nano: usare `vi` (preinstallato)
  oppure via SSH: `echo '{"version":"...","filename":"..."}' > /volume1/.../version.json`

---

## v1085e — Hotfix critici dopo test pilot v1085d (2026-04-27)

### 🐛 BUG-01 · Dialog upgrade scompare e blocca app
**File:** `gui/main_window.py::_show_upgrade_dialog`

Aprendo il dialog upgrade dal flyout profilo, il dialog appariva
brevemente e poi scompariva, lasciando l'app bloccata (la finestra
principale non era più interattiva ma non c'era nessuna finestra
visibile in primo piano). Il dialog non era nella taskbar perché
`overrideredirect=True`.

Causa: il flyout profilo restava aperto sotto al dialog. Il polling
`_mouse_check` continuava a girare e, visto che il mouse era ora sopra
il dialog (fuori dalle coordinate del flyout), schedulava la chiusura
del flyout dopo 250ms. Questo a sua volta cancellava i bind
`<Button-1>` e portava il sistema in uno stato di Tk/Windows confuso
dove il dialog perdeva il grab e finiva nascosto.

Fix in 2 punti:
1. **Quando si clicca "⬆ Richiedi upgrade del piano"** dal flyout,
   il flyout viene chiuso esplicitamente PRIMA con
   `self._close_profile_flyout()`, poi `root.after(80, ...)`
   apre il dialog con un piccolo delay
2. **`_show_upgrade_dialog()` come prima cosa** chiude qualunque
   flyout profilo aperto (difesa in profondità per altri call site)

### 🐛 BUG-02 · KeyError 'user_id' apertura tab Avanzate
**File:** `gui/main_window.py::_admin_render_list`

Quando l'admin apriva il tab Avanzate, il rendering della lista
richieste pending crashava con `KeyError: 'user_id'`. Il rendering
usava `r['user_id']` aspettandosi che il server lo includesse, ma
lo schema Pydantic `UpgradeRequestOut` lato server NON aveva quel
campo nella response. Il modello DB ce l'ha, ma Pydantic emette solo
i campi dichiarati nello schema.

Fix doppio:
1. **Server** (`app/api/plans.py`): aggiunti `user_id`, `user_email`,
   `user_name` allo schema `UpgradeRequestOut`. L'endpoint
   `list_pending_requests` ora arricchisce ogni richiesta con
   `r.user.email` e `r.user.username` tramite la relationship
2. **Client** (`gui/main_window.py`): rendering robusto con `.get()`
   e fallback graceful: `username (email)` se entrambi presenti,
   altrimenti email, altrimenti username, altrimenti `User #ID`,
   altrimenti `Utente sconosciuto`. Funziona anche con server vecchi
   che non includono i nuovi campi

### 🐛 BUG-03 · Avvio catalogazione apre seconda finestra GUI
**File:** `gui/main_window.py::_build_command` + `run_gui.py`

In modalità EXE PyInstaller, `sys.executable` è il path dell'EXE GUI
stesso, NON il python interpreter. Quindi
`Popen([sys.executable, "run_cataloger.py", path])` lanciava una
nuova istanza del programma GUI principale invece di un python con
lo script cataloger. Il sintomo: cliccando Avvia compariva una
seconda finestra del programma e la prima si bloccava.

Fix con flag speciale:
- `_build_command`: in modalità frozen, comando =
  `[sys.executable, "--cataloger-mode", path, ...args]`
- `run_gui.py` al boot, prima di costruire la GUI, intercetta
  `--cataloger-mode` in `sys.argv`. Lo rimuove dagli argv e invoca
  `run_cataloger.main()` direttamente nel processo, con `sys.exit()`
  al termine
- In modalità script (sviluppo) tutto resta come prima: python +
  run_cataloger.py

### 🐛 BUG-04 · Updater 500 "Permission denied: 'data/version.json'"
**File:** `app/api/updates.py`

Errore lato server quando il client faceva check update al boot.
Il path `Path("./data/version.json")` era relativo al CWD del
processo (in container `/srv/app/`). Se il file esisteva ma con
permessi sbagliati, o era diventato una directory per errore (es.
`mkdir -p data/version.json`), Python sollevava `PermissionError`
o `IsADirectoryError` e l'endpoint rispondeva 500 invece di 404.

Fix:
- Path resi assoluti: `(settings.DATA_DIR / "version.json").resolve()`
- 404 graceful (con WARNING in console) per:
  - file inesistente
  - non è un file regolare (es. directory)
  - PermissionError / OSError di lettura
  - JSONDecodeError (file presente ma corrotto)

Il client tratta tutti questi casi come "nessuna release pubblicata"
e non insiste — niente più spam 500 al boot di ogni utente.

### 🛠️ FIX-05 · Race condition thread workers vs root distrutto
**File:** `gui/main_window.py`

In rari casi il messaggio `Exception in Tkinter callback ...
main thread is not in main loop` compariva nel CMD quando un thread
worker (refresh richieste/utenti) chiamava `self.root.after(0, ...)`
mentre la main window stava per essere distrutta.

Fix: nuovo helper `MainWindow._safe_after(ms, cb)` che verifica
`winfo_exists()` prima di chiamare `root.after`. Tutti i worker
admin (refresh users, refresh requests) ora usano questo invece
di `self.root.after` diretto.

### 🎨 UX-06 · Centratura dialog "Richiedi upgrade"
**File:** `gui/main_window.py`

Il dialog di conferma "Richiedi → Pro" che si apriva dal dialog
upgrade non era centrato sopra al parent. Ora calcola posizione
basata su `winfo_x/y` del dialog upgrade.

### 🎨 UX-07 · Flyout chiusura più reattiva (250ms)
**File:** `gui/main_window.py`

Ridotto il grace period di chiusura flyout da 400ms a 250ms come
richiesto. Cancellazione resta intatta se il mouse rientra.

### 📝 v1085e note operative

**Build EXE: icona vecchia in cache.** PyInstaller cachea le icone
in `%LOCALAPPDATA%\pyinstaller\`. Per forzare il rebuild dell'icona:
```cmd
rmdir /s /q "%LOCALAPPDATA%\pyinstaller"
rmdir /s /q build dist
pyinstaller music_cataloger.spec --clean
```
Inoltre Windows cachea l'icona dell'EXE in `IconCache.db`: anche dopo
il rebuild l'esploratore mostra la vecchia. Per forzare il refresh:
```cmd
ie4uinit.exe -show
```
oppure logout/login.

**Hash SHA256 su Windows** (no `sha256sum`):
```cmd
certutil -hashfile "dist\Music Cataloger Advanced.exe" SHA256
```

**Editare version.json su Synology** (no `nano`):
```bash
# Opzione 1: vi (preinstallato)
sudo vi /volume1/docker/music-cataloger/data/version.json

# Opzione 2: heredoc
sudo tee /volume1/docker/music-cataloger/data/version.json > /dev/null << 'EOF'
{
  "version": "v1086",
  "filename": "Music_Cataloger_v1086.exe",
  "sha256": "...",
  "changelog": "- Fix vari",
  "mandatory": false
}
EOF

# Permessi: il container gira come UID 1000:1000
sudo chown 1000:1000 /volume1/docker/music-cataloger/data/version.json
```

---

## v1085f / v0.1.7 — Hardening pre-pilot + caribbean condivisi (2026-04-28)

### 🐛 BUG-01 · Timbaland classificato Salsa nonostante word boundary
**File:** `core/genre_classifier.py::classify` priorità 3

In v1085d ho aggiunto word boundary su `detect_latin_subgenre` ma il
bug era altrove: quando il DB esterno (iTunes/Deezer) ritornava
direttamente "Salsa" come `external_metadata['genre']`, il codice
accettava ciecamente senza verificare:
```python
if raw_lower in ['salsa', 'bachata', 'merengue', 'cumbia', 'reggaeton']:
    genre = raw_lower.capitalize()  # ← bug: nessuna verifica
```

Caso reale: "Apologize - One Republic Ft. Timbaland.mp3" → iTunes
etichetta "Salsa" (per qualche album compilation latin?) → catalogato
in `Latin\Salsa\` invece che `Pop\` o `R&B\`.

Fix: anche quando il DB dice direttamente "salsa/bachata", chiamiamo
`detect_latin_subgenre`. Tre casi:
- detect e DB concordano → accetto
- detect dice X, DB dice Y → fido del detect
- detect ritorna None → DB ignorato (zero indicatori latini su
  artista/titolo/filename = il DB sta sbagliando), proseguo con
  priorità successive

### 🐛 BUG-02 · "PROCESSO TERMINATO CON ERRORI" su stop utente
**File:** `gui/main_window.py::_finish`

Quando l'utente premeva "Ferma" durante una catalogazione, il log
finale diceva "✗ PROCESSO TERMINATO CON ERRORI" (rosso/error) invece
di un messaggio chiaro che era stato l'utente a fermarlo.

Fix: il branch `aborted=True` ora aggiunge al log un blocco
"⚠ PROCESSO TERMINATO DALL'UTENTE" in giallo (level WARNING) e
imposta lo status a "Processo interrotto dall'utente". Niente più
messagebox d'errore.

### 🐛 BUG-03 · Flyout principale non chiude su voci senza sub
**File:** `gui/main_window.py::_show_profile_panel::_on_enter`

Quando l'utente passava il mouse da "Piani" (con sub-flyout) a
"Esci"/"Aiuto" (senza sub), il sub-flyout di Piani restava aperto
sotto, dando l'impressione che il menu non si chiudesse.

Fix: in `_on_enter` di una row con `sub_id=None`, se è attivo un
sub-flyout di un'altra voce, lo chiudo subito + cancello eventuali
timer di hover-to-open pendenti.

### 🎨 UX-04 · Icone messagebox + qualità barra Windows
**File:** `gui/app_icon.py::set_window_icon`

I messagebox di approva/rifiuta richiesta upgrade non avevano
l'icona dell'app (mostravano l'icona generica tkinter). Inoltre
l'icona in barra Windows era sgranata.

Fix: `set_window_icon` ora applica DUE strategie in cascata:
1. `iconbitmap()` con il `.ico` per la qualità nativa Windows
   (taskbar + titlebar)
2. `iconphoto(default=True, ...)` con un PNG 64×64 da
   `taskbar_active.png` per propagare l'icona ai messagebox figli
   creati da `tkinter.messagebox`

`default=True` è il flag chiave: applica l'icona al "wm class"
condiviso da tutti i Toplevel del processo, inclusi i messagebox
standard. L'icona 64×64 è un buon compromesso tra qualità
(nitida nei popup grandi) e dimensione runtime.

### 🎯 FEAT-05 · Caribbean settings condivisi server-side
**File server:** `app/api/plans.py` — endpoint nuovi:
```
GET  /caribbean-settings/defaults        (pubblico)
POST /admin/caribbean-settings           (admin only)
```

**File client:** `gui/main_window.py`, `services/api_client.py`

L'admin può ora pubblicare le sue impostazioni caraibiche (BPM range,
artisti, keyword salsa/bachata) come **default per tutti i clienti**.

Workflow:
1. Admin configura tab Caraibica come desidera
2. Click "📤 Pubblica come default per tutti" (bottone visibile solo
   in modalità admin server) → POST `/admin/caribbean-settings`
3. Server salva in `data/caribbean_defaults.json`

Per i client:
- Al boot, se `data/caribbean_settings.json` LOCALE non esiste,
  fa GET `/caribbean-settings/defaults` (no auth)
- Se l'admin ha pubblicato qualcosa, scarica e applica + salva
  localmente per non ripetere ad ogni boot
- Se l'utente ha già impostazioni locali, NON viene sovrascritto

Il fetch è non-bloccante (thread separato) per non rallentare il
boot della GUI. Se il tab Caraibica è già aperto, i widget vengono
ricaricati con i nuovi valori dopo il fetch.

### 🔒 FEAT-06 · Hardening sicurezza pre-pilot

**1. SECRET_KEY hard-fail in produzione**
**File:** `app/main.py::lifespan`

In produzione, se `SECRET_KEY` è vuoto, < 32 char, o contiene
prefissi placeholder (CHANGE-ME, INSERT-, REPLACE-ME, your-secret),
il server lancia `RuntimeError [FATAL]` e non parte. In dev
(ENV=development) emette solo un WARNING.

Senza questo controllo, un deployment con `.env` non configurato
avrebbe usato una chiave nota, permettendo a chiunque di forgiare
JWT validi.

**2. Audit log azioni admin**
**File:** `app/models/db.py` (modello), `app/api/plans.py` (logging)

Nuova tabella `admin_audit_log` con righe immutabili (nessun
endpoint per modificare/cancellare). Eventi tracciati:
- `upgrade_approved` (admin, target_user, old_plan→new_plan, note)
- `upgrade_rejected` (admin, target_user, plans, note)
- `plan_changed` (admin, target_user, old→new) - via set-plan diretto
- `caribbean_defaults_published` (admin, contatori indicatori)

Endpoint `GET /admin/audit-log?limit=N` (admin only, max 500 righe)
per visualizzazione successiva. Per il pilot questo è sufficiente
per accountability di chi-ha-fatto-cosa-quando.

### 📝 Note operative v1085f

**Build EXE — icona vecchia:**
La cache è in `%LOCALAPPDATA%\pyinstaller\` + cache shell di Windows.
```cmd
rmdir /s /q "%LOCALAPPDATA%\pyinstaller" 2>nul
rmdir /s /q build dist 2>nul
pyinstaller music_cataloger.spec --clean
:: Refresh cache shell Windows (icone in Esplora)
ie4uinit.exe -show
:: Oppure: logout/login
```

**File EXE deve match con version.json:**
Quando carichi un EXE su NAS in `data/releases/`, il `filename` in
`version.json` deve corrispondere ESATTAMENTE al nome file:
```bash
ls -la /volume1/docker/music-cataloger/data/releases/
# → Music_Cataloger_v1086.exe
```
Se l'EXE che PyInstaller produce si chiama `Music Cataloger Advanced.exe`
devi rinominarlo prima di caricare, oppure aggiornare il `filename`
in `version.json` per matchare.

**Inoltre il check si attiva solo se `version.version > version.py:APP_VERSION`.**
Esempio: se il client è v1085f e il `version.json` dice v1085f → no
update. Se dice v1086 → prompt update. Quindi quando carichi un nuovo
EXE devi sempre incrementare la versione in version.json oltre quella
del client già installato.

### Endpoint server v0.1.7 — riepilogo (29 endpoint)
Aggiunti rispetto a v0.1.6:
```
GET  /caribbean-settings/defaults       (pubblico)
POST /admin/caribbean-settings          (admin)
GET  /admin/audit-log                   (admin)
```

---

## v1085g / v0.1.8 — Hotfix critici + admin panel completo (2026-04-28)

### 🐛 BUG-01 · Timbaland ANCORA in Salsa (priorità 2 substring match)
**File:** `core/genre_classifier.py::classify` priorità 2

I fix v1085d e v1085f avevano coperto `detect_latin_subgenre` e
priorità 3 (DB esterni), ma c'era un terzo punto di matching:
```python
# Priorità 2 (artisti noti)
if len(indicator) > 4 and indicator in artist_lc:  # ← bug substring
    genre = 'Salsa'
```

`"timba" in "timbaland"` → match → `[P2-artist] Artista salsa noto: timba`.
Visto nei log che hai mandato:
```
[P2-artist] Artista salsa noto: timba
[Genere finale: 'Salsa' (raw: 'salsa')]
```

Fix: word boundary regex anche su priorità 2. Audit completo del file
ora confermato pulito (zero substring matching su nomi artisti).

### 🐛 BUG-02 · Voce sub flyout resta blu dopo chiusura
**File:** `gui/main_window.py::_show_profile_panel::_on_enter`

Quando l'utente passava il mouse da "Piani" (sub aperto) a "Esci",
il sub-flyout di Piani si chiudeva (fix v1085f) ma la voce "Piani"
rimaneva colorata blu come se il mouse fosse ancora sopra.

Fix: in `_on_enter` di una row con `sub_id=None`, prima di chiudere
il sub recupero `prev_sub_id = self._profile_active_sub` e ripristino
`fg_color="transparent"` sulla row corrispondente in `_profile_rows`.

### 🐛 BUG-03 · Rinomina file non funziona
**File:** `core/cataloger.py` riga ~614

La logica di rename usava una lookup in `_metadata_cache` con un
matching testuale fragile:
```python
for key, val in self._metadata_cache.items():
    if destination.stem.lower() in (t + " " + a + " " + key.lower()):
        meta = val; break
```
Funzionava in pochissimi casi → la maggior parte dei file era spostata
ma non rinominata. Il pattern `{artist} - {title}` era settato ma
l'azione di rename non avveniva.

Fix: usare `final_metadata` (passato come argomento alla `process_file`,
già contenente artist+title corretti dopo lookup DB esterni). Niente
più lookup nella cache. Codice molto più semplice. Aggiunto anche:
- supporto pattern custom (non solo `{artist} - {title}` e
  `{title} - {artist}`)
- check `if str(new_path) != str(destination)` per evitare rinomina
  no-op
- log esplicito `[RINOMINA SKIP]` se il target esiste già

### 🐛 BUG-04 · "Versione file 1.0.8.2" sgranata in Esplora
**File:** `version_info.txt`

`filevers=(1,0,8,2)` non concordava con `FileVersion=1.0.85` —
quando Pedro guardava le proprietà dell'EXE, Windows mostrava
`1.0.8.2` (la tupla ha precedenza).

Fix: file riscritto con tupla coerente `(1, 0, 85, 7)` e stringhe
corrispondenti `1.0.85.7`. La quarta cifra mappa la lettera (a=1,
b=2, ..., g=7) in modo che si possa distinguere `v1085f` da `v1085g`
anche guardando le proprietà Windows.

### 🎨 UX-05 · Icona barra Windows sgranata
**File:** `build_ico.py` (nuovo), `icons/music_cataloger.ico`

L'icona in taskbar appariva sfocata rispetto alle altre app perché
il `.ico` aveva una sola dimensione (256×256) → Windows scalava
malamente a 16×16 per la taskbar.

Soluzione: nuovo script `build_ico.py` da lanciare PRIMA del
`pyinstaller` build. Genera un `.ico` **multi-resolution** con TUTTE
le dimensioni che Windows usa nativamente:
```
16, 24, 32, 48, 64, 128, 256
```
Ogni size è generato con resize LANCZOS dal PNG sorgente
(`icons/app/taskbar_active.png`). Pillow li impacchetta nello stesso
file `.ico` e Windows sceglie la versione native a runtime — niente
più downscaling sgranato.

USAGE:
```cmd
:: Lancia PRIMA del pyinstaller, ogni volta che cambi l'icona
python build_ico.py
pyinstaller music_cataloger.spec --clean
```

### 🎯 FEAT-06 · Pannello Audit Log GUI
**File:** `gui/main_window.py::_admin_render_audit`

Pedro chiedeva dove vedere l'audit log. Ora c'è una sezione dedicata
nel tab Avanzate (solo admin), sotto "Utenti registrati":
```
📋  Pannello Amministratore  —  Audit Log azioni
```
Ogni riga mostra:
- Emoji + descrizione user-friendly dell'azione (✅ approvato,
  ❌ rifiutato, 🔄 piano modificato, 📤 caraibica pubblicata)
- Email utente target (o user_id come fallback)
- Email admin che ha eseguito l'azione + timestamp
- Dettagli parsificati dal JSON (es. "base → pro", motivo rifiuto,
  contatori indicatori caraibici pubblicati)

Carica le ultime 50 azioni in ordine cronologico inverso. Refresh
manuale via bottone "🔄 Aggiorna".

### 🎯 FEAT-07 · Toggle registrazione self-service (admin)
**Server:** `app/api/auth.py` 3 endpoint nuovi:
```
GET  /auth/registration/status              (pubblico)
POST /auth/admin/registration/disable       (admin)
POST /auth/admin/registration/enable        (admin)
```
**Stato:** persistito su file `data/registration_disabled.flag`
(presenza = disabilitata).

**Server logic:** `POST /auth/register` ora rispetta il flag e
risponde 403 con messaggio italiano se disabilitato.

**Client:** sezione admin nel tab Avanzate
```
🔐  Pannello Amministratore  —  Registrazione self-service
✓  Registrazione self-service ATTIVA  [🔒 Disabilita]
```
oppure (quando off):
```
🔒  Registrazione DISABILITATA  [🔓 Abilita]
```
Click sul bottone → conferma → chiamata API → refresh.

**Login window:** al boot fa GET `/auth/registration/status` in thread
separato. Se disabled, sostituisce il link "Registrati" con un avviso
"ℹ Registrazione self-service disabilitata. Contatta l'amministratore
per ottenere un account."

### 📝 v1085g note operative

**EXE auto-update non parte se versione locale = remota.**
`is_newer("v1085g", "v1085g") == False`. Per testare l'updater devi
aver caricato sul NAS un EXE *più recente* del client che stai
lanciando. Esempio:
- Client installato: v1085g
- `version.json` sul NAS: `"version": "v1086"` o `"v1086a"`, etc.
- Client al boot vede "v1086 > v1085g" → mostra prompt update

**Build EXE — sequenza completa per icona perfetta:**
```cmd
:: 1. Genera .ico multi-resolution (UNA TANTUM)
python build_ico.py

:: 2. Pulisci cache
rmdir /s /q "%LOCALAPPDATA%\pyinstaller" 2>nul
rmdir /s /q build dist 2>nul

:: 3. Build
pyinstaller music_cataloger.spec --clean

:: 4. Refresh icone shell Windows
ie4uinit.exe -show

:: 5. Hash per version.json
certutil -hashfile "dist\Music Cataloger Advanced.exe" SHA256
```

### Endpoint server v0.1.8 — riepilogo (32 endpoint)
Aggiunti rispetto a v0.1.7:
```
GET  /auth/registration/status              (pubblico)
POST /auth/admin/registration/disable       (admin)
POST /auth/admin/registration/enable        (admin)
```

---

## v1085h / v0.1.9 — Hotfix critici + roadmap pilot (2026-04-29)

### 🐛 BUG-01 · Catalogazione crash con `final_metadata is not defined`
**File:** `core/cataloger.py::_move_to_genre_folder`

Il fix di rinomina di v1085g referenziava `final_metadata` ma quella
variabile esiste solo nello scope di `process_file()`, NON di
`_move_to_genre_folder()` dove ho messo il codice. Risultato: ogni
file falliva con
```
ERROR - Errore spostamento Apologize - One Republic Ft. Timbaland.mp3:
        name 'final_metadata' is not defined
```
**Quindi nessun file veniva spostato/rinominato — bug critico.**

Fix: `_move_to_genre_folder` ora accetta `final_metadata` come
parametro opzionale; il caller `process_file` lo passa esplicitamente.
Aggiunto guard `if final_metadata` per sicurezza.

### 🐛 BUG-02 · Auto-update silenzioso quando 500/Permission denied
**File:** `services/updater.py`

Il check si chiudeva con `print()` su `sys.stdout`, ma in modalità
EXE windowed (PyInstaller) `sys.stdout` è None → niente log visibili.
Pedro non riusciva a capire perché non comparisse il prompt.

Fix:
- Nuovo helper `_log()` che scrive sia su stdout (se disponibile)
  sia su `data/updater.log` accanto all'EXE
- Tutti i `print("[updater] ...")` sostituiti con `_log(...)`
- 404 ora gestito in silenzio (= no release), 500/altro ora mostra
  popup informativo (se `silent=False`)
- Più verbose: log della versione locale + remota + flag frozen

### 🎯 FEAT-03 · Password policy enforcement
**File:** `app/services/password_policy.py` (nuovo)

Validazione password applicata su:
- `POST /auth/register`
- `POST /auth/change-password`
- `POST /auth/admin/users` (anche l'admin deve usare password decenti)

Regole (allineate NIST SP 800-63B):
- Min 8 / Max 128 caratteri
- NON in lista weak comuni (50 password top: `password`, `12345678`,
  `qwerty123`, ecc.)
- NON identica a email o username
- NON sequenza ovvia (`aaaaaaaa`, `12345678`, `abcdefgh`)

Niente regole "complessità" tipo "1 maiuscola + 1 simbolo": NIST le
sconsiglia (forzano pattern prevedibili tipo `P@ssw0rd!`).

### 🎯 FEAT-04 · Crea utente lato admin
**Server:** `POST /auth/admin/users` (admin only)
**Client:** Pannello admin → "👤 Crea utente" → form modale

Form: email, username, password, plan (base/pro/advanced),
checkbox is_admin. Sottomette al server, mostra credenziali generate
in popup confirmation. Audit log azione `user_created_by_admin`.

Use case: pilot privato dove la registrazione self-service è
disabilitata e l'admin crea manualmente gli account ai clienti.

### 🎯 FEAT-05 · Pannello statistiche admin
**Server:** `GET /admin/stats` (admin only)
**Client:** Pannello admin → griglia 2×4 KPI cards

KPI mostrate:
- 👥 Utenti totali  •  👑 Admin  •  ⚙️ Job totali  •  ✅ Job completati
- 🎵 File processati  •  ⏳ Job in corso  •  ✗ Job falliti  •  🔔 Upgrade pending

Sotto la grid:
- 📦 Versione server
- 💾 Dimensione DB
- 📊 Distribuzione utenti per piano

Refresh manuale via bottone "🔄 Aggiorna".

### 🎯 FEAT-06 · Backup script per Synology NAS
**File:** `scripts/backup-db.sh`

Script bash con:
- Backup `app.db` con `sqlite3 .backup` (transazione-safe) se
  disponibile, altrimenti `cp` con avviso
- Compressione gzip
- Rotation 30 giorni (configurabile)
- Snapshot extras (`registration_disabled.flag`,
  `caribbean_defaults.json`, `version.json`) in tarball
- Logging in `data/backups/backup.log`

Setup tramite **DSM Utilità di pianificazione** (no `crontab`
diretto su DSM 7+):
```
Pannello di controllo → Utilità di pianificazione → Crea
→ Attività pianificata → Script definito dall'utente
   Generale: nome "DB Backup", utente: root
   Pianificazione: Giornaliera, 03:30
   Comando: /volume1/docker/music-cataloger/scripts/backup-db.sh
```

Notifica email su errore configurabile dal pannello "Notifica" DSM.

### 🎯 FEAT-07 · Build cross-platform (Linux + macOS)
**File:** `music_cataloger_linux.spec`, `music_cataloger_macos.spec`,
`BUILD_CROSS_PLATFORM.md`

PyInstaller spec dedicati per Linux e macOS. Note importanti:
- **PyInstaller non fa cross-compile**: serve una macchina/VM/container
  dell'OS target per buildare
- Per pilot privato con amici tecnici raccomando di distribuire il
  sorgente Python (più facile, niente firme, niente Gatekeeper)
- macOS: bundle `.app` con `Info.plist`. Senza certificato Apple
  Developer ($99/anno) il primo avvio richiede "Click destro → Apri"
- Linux: binario singolo + istruzioni `.desktop` per integrazione
  GNOME/KDE
- L'auto-updater attuale è solo Windows. Implementare cross-platform
  post-pilot 1.

### 📝 Roadmap pilot — stato aggiornato

**Pronto per pilot 1 (cliente-amico):**
- ✅ Auth + JWT + bcrypt + password policy
- ✅ Plans + upgrade flow + admin approval
- ✅ Catalog tracking + quote
- ✅ Pannello admin completo (richieste, utenti, audit, registrazione,
   crea utente, statistiche)
- ✅ Auto-updater Windows con fallback log file
- ✅ Caribbean settings condivisi
- ✅ SECRET_KEY hardening produzione
- ✅ Audit log completo
- ✅ Backup script NAS
- ✅ Build script Linux/macOS

**Da implementare per pilot 2 (multi-cliente):**
- ⏳ Email verifica registrazione (richiede SMTP setup)
- ⏳ Notifiche email all'admin su nuove richieste
- ⏳ Rate limit su `/auth/login` (slowapi 5/15min/IP)
- ⏳ Migration PostgreSQL (postpone fino a >50 utenti concorrenti)

**Production (post pilot 2):**
- 2FA opzionale TOTP
- Stripe integrazione
- WebSocket logs
- Auto-update cross-platform
- Build pipeline CI/CD

### Endpoint server v0.1.9 — totale 35 endpoint

Aggiunti rispetto a v0.1.8:
```
POST /auth/admin/users               (admin: crea utente)
GET  /admin/stats                    (admin: KPI dashboard)
```

Modificati:
```
POST /auth/register                  (+ password policy)
POST /auth/change-password           (+ password policy)
```

---

## v1085m — Hotfix critici onefile + persistenza dati (2026-05-04)

### 🐛 BUG-01 · `Failed to load Python DLL python313.dll` post-update
**File:** `services/updater.py::_make_windows_updater_script`

Sintomo: dopo l'update, l'EXE veniva sostituito ma al rilancio crashava
con `LoadLibrary: Impossibile trovare il modulo specificato`.

Causa root: con PyInstaller ONEFILE, il vecchio EXE in chiusura tiene
ancora un lock file mentre il batch fa `copy /Y`. La copy avveniva con
il bootloader "agganciato" → EXE corrotto sui primi ~31 MB (visibile
da Properties Windows: dimensione corretta ma magic header invalido).

Fix: strategia **rename-and-replace** invece di `copy`:
1. Aspetta che `ren` (rename atomico) sul vecchio EXE riesca = file
   non più in lock
2. Rinomina vecchio in `*.exe.old` (operazione atomica filesystem)
3. `move /Y` del nuovo nella posizione del vecchio (atomica)
4. Lancia il nuovo
5. Al boot del nuovo, `cleanup_old_backup()` rimuove `*.exe.old`

Rename non legge/scrive il contenuto del file → niente race con il
bootloader in chiusura.

### 🐛 BUG-02 · Sessione e last_email persi a ogni boot
**File:** `services/jwt_store.py`, `config/app_config.py`

Sintomo Pedro: doveva sempre rifare login, last_email non veniva
ricordato, recent_dirs spariva.

Causa root: in modalità onefile, `__file__` di moduli Python è dentro
`%TEMP%\_MEI<random>` che PyInstaller cancella ad ogni avvio. La
cartella `data/` calcolata come `Path(__file__).parent.parent / "data"`
finiva DENTRO `_MEI` → tutto il contenuto si cancellava.

Fix: nuova logica path:
```python
if getattr(sys, "frozen", False):
    # Bundle PyInstaller — usa dir dell'EXE (persistente)
    base = Path(sys.executable).parent / "data"
else:
    # Script Python — usa parent del package (legacy)
    base = Path(__file__).parent.parent / "data"
```

Applicato a `app_config.py` (last_email, server_url, recent_dirs) e
`jwt_store.py` (session.json). Cataloger usava già pattern corretto.

### 🐛 BUG-03 · `_is_latin_file` AttributeError
**File:** `core/cataloger.py`

Sintomo Pedro: catalogazione lanciata da `python run_gui.py` esplodeva
con `'MusicCataloger' object has no attribute '_is_latin_file'`.
Da EXE non scattava (caribbean_dirty=False da bundle).

Causa: il codice v1073 in `process_mp3_file` chiamava `self._is_latin_file()`
ma il metodo non era mai stato implementato — bug presente da v1073 ma
non triggerato perché caribbean_dirty raramente true da EXE.

Fix: aggiunto metodo `_is_latin_file(file_path, metadata)` come
euristica conservativa: cerca keyword salsa/bachata/merengue/ecc nei
campi filename + metadata (artist, album, title, genre).
Falsi positivi sono OK (riclassificheremo un file pop tre volte invece
di una), falsi negativi sono il vero problema.

### 🐛 BUG-04 · Form crea utente nascosto sotto la main
**File:** `gui/main_window.py::_admin_show_create_user_dialog`

Sintomo Pedro: il dialog spariva sotto la main quando l'utente passava
ad altre app Windows e tornava.

Causa: `overrideredirect=True` + `transient(self.root)` rendono il
dialog "borderless" e legato alla main, ma Windows non emette
`<FocusOut>` su finestre senza bordo. Il dialog ereditava lo z-order
della main.

Fix: rimosso `overrideredirect=True` e `transient`. Il dialog ora è
una finestra Windows STANDALONE: ha la sua entry in taskbar, propria
icona, propria gestione minimize/restore — esattamente come la
finestra "Catalogazione Completata" che Pedro aveva notato funzionare
correttamente. Tolto anche il bottone X custom e i drag handlers (la
titlebar nativa Windows ha già tutto).

### 🐛 BUG-05 · .ico mancante in build pulito
**File:** `music_cataloger.spec`

Sintomo Pedro: dopo `pyinstaller --clean`, le icone della GUI sembravano
"fatte casino".

Causa: `.spec` referenziava `icons/music_cataloger.ico` ma quel file
viene generato da `build_ico.py` da `taskbar_active.png`. Se Pedro non
lanciava `build_ico.py` prima di pyinstaller, il `.ico` non esisteva e
PyInstaller usava un fallback bruttino.

Fix: il `.spec` ora controlla all'inizio se il `.ico` esiste e in caso
contrario lo genera al volo da PIL. Niente più step manuale obbligatorio
prima del build.

### 🎯 FEAT-06 · Workflow versioning con git + GitHub
**File:** `VERSIONING.md`, `.gitignore`

Pedro ha già il repo `https://github.com/PedroFerre27/MusicCatalogerAdvanced`.
Aggiunta documentazione completa per:
- Workflow main + tag stabili (`v1086-stable`)
- Branch per esperimenti rischiosi
- Rollback in 30 secondi (`git checkout v1085m-stable`)
- GitHub Releases per distribuire EXE multi-platform
- `.gitignore` con esclusioni corrette (data/, dist/, build/, secret)

Niente più ZIP sul desktop.

---

## v1085n — Hotfix robustness icone + UPX-free build (2026-05-04)

### 🚨 ATTENZIONE: questo ZIP NON contiene la cartella `icons/`

Pedro: ho rimosso `icons/` dal ZIP perché nei turni precedenti ho
incluso ICONE CORROTTE (PNG con header JPEG, no transparency). Causa:
i project files del workspace Claude avevano i PNG già corrotti, e
non l'ho rilevato. Risultato: la build con quelle icone produceva
sfondi neri ovunque.

**AZIONE PEDRO**: estrai il ZIP sopra `C:\dev\music-cataloger\` SENZA
sovrascrivere la cartella `icons/`. La tua copia locale degli icons
è quella buona, restano com'erano.

D'ora in poi i miei ZIP non includeranno più icone — sono tue, non
mie.

### 🐛 BUG-01 · Build EXE crash `Failed to load python313.dll`
**File:** `music_cataloger.spec`

Pedro test su tutte le versioni dalla v1085i in poi: l'EXE buildato
con onefile crasha al primo avvio con `LoadLibrary: Impossibile
trovare il modulo specificato. python313.dll`.

Causa root: UPX compression. UPX comprime python313.dll dentro l'EXE
self-extracting, ma molti antivirus aziendali (Defender + Carbon Black
+ SentinelOne + suite Indra-style) marcano i binari UPX come
"potential malware" perché tanti ransomware usano UPX per offuscarsi.
Risultato: AV mette python313.dll in quarantena post-extract → il
bootloader trova `_MEIxxx/` ma non `python313.dll` → crash.

Fix: `upx=False` nello spec. EXE ~30% più grande ma non più toccato
dall'AV. Aggiunto anche `optimize=0` esplicito per evitare problemi
con eyed3 che usa docstring runtime.

### 🐛 BUG-02 · Icone con sfondo nero / non trasparenti
**File:** `gui/icons.py`

Pedro: post-build le icone avevano sfondo nero invece di trasparente.

Causa root duplice:
1. Il path resolver usava `Path(__file__).parent.parent` ma in
   PyInstaller onefile `__file__` è in `_MEIxxx/gui/` quindi NON
   trovava le icone (path differente).
2. `Image.open(...).convert("RGBA")` falliva silenziosamente su file
   non-RGBA (es. PNG con header JPEG, o RGB pure senza alpha) e
   ritornava None → CustomTkinter renderizzava placeholder nero.

Fix:
1. `_resolve_icon_dir()` aware di `sys._MEIPASS` come nelle altre
   parti del codice.
2. Check esplicito `if img.mode != "RGBA"` prima di convert.
3. Catch `UnidentifiedImageError, OSError, ValueError` per file
   corrotti (capita con OneDrive sync interrotto durante build) →
   ritorna None invece di icona corrotta. CustomTkinter mostra solo
   testo del bottone.

### 🐛 BUG-03 · Form crea utente apre in background senza icona
**File:** `gui/main_window.py::_admin_show_create_user_dialog`

Pedro: la finestra crea utente è ora una finestra Windows standalone
(buono, da v1085m), MA si apre dietro la main + senza icona top-left.

Fix duplice:
1. `_apply_icon()` chiamata DUE volte: subito + dopo 250ms via
   `win.after()`. Il primo tentativo può essere ignorato da Windows
   se la finestra non è ancora "mappata"; il secondo passa quasi
   sempre. Stesso pattern già usato sulla main window.
2. `_bring_to_front()` con strategia "topmost momentaneo": setto
   `-topmost True`, lift, focus_force, e dopo 100ms tolgo il topmost.
   La finestra appare sopra TUTTE le altre app (non solo sopra la
   main) ma non ci resta — comportamento normale dopo il "boot".
   Chiamato a t=0, t=50ms, t=200ms per battere race con il window
   manager Windows.

### 🛟 FEAT-04 · Fallback manuale se update automatico fallisce
**File:** `services/updater.py`

Pedro problema: client v1085i ha updater rotto → ogni nuova versione
che pubblico non si applica → Pedro è in loop circolare ("per
aggiornare il client devo usare l'updater che è rotto").

Soluzione: quando `_do_update()` raise un'eccezione MA il file è
stato scaricato OK, mostro un nuovo dialog `_show_fallback_manual`
con:
- Path FROM (file scaricato in `%TEMP%\music_cataloger_update\`)
- Path TO (EXE corrente)
- Bottone "📂 Apri 'File scaricato'" → explorer.exe /select
- Bottone "📂 Apri 'App corrente'" → explorer.exe /select
- Istruzioni step-by-step

Pedro chiude il client, copia a mano, riapre. Funziona indipendentemente
da OneDrive/antivirus/permission/UPX. È UN fallback per emergenza —
non sostituisce l'updater automatico.

### 📦 Versioning: workflow git aggiornato
**File:** `VERSIONING.md`

Pedro ha già repo `https://github.com/PedroFerre27/MusicCatalogerAdvanced`.
Setup completato in questa sessione, anche se con merge conflict
risolti via `git merge --abort` + `git branch -M main` +
`git push --force-with-lease`. Rebase pulito, tag `v1085m-stable`
pushato.

---

## v1085o — Auto-update fix env vars + UX dialog standalone (2026-05-04)

### 🐛 BUG-01 · Auto-update: nuovo EXE crash dopo move atomic
**File:** `services/updater.py::_make_windows_updater_script`

Pedro test definitivo: rename+move atomic vanno OK (vecchio in `*.exe.old`,
nuovo in posizione, dimensioni 34555 KB conferma file integro). MA al
rilancio il NUOVO EXE crasha sempre `Failed to load Python DLL python313.dll`.
Lanciato a mano da explorer.exe lo stesso EXE funziona perfettamente.

**Causa root identificata** (non era UPX né AV come ipotizzato):
quando il client Python (vecchio EXE PyInstaller) lancia il batch via
`subprocess.Popen` con `DETACHED_PROCESS`, il batch eredita le env vars
del processo Python — incluse quelle che PyInstaller setta a runtime
per il proprio bootstrap interno: `_PYI_APPLICATION_HOME_DIR`,
`_MEIPASS2`, `_PYI_ARCHIVE_FILE`, etc.

Quando il batch fa `start "" "<new_exe>"`, il nuovo processo eredita
queste env vars stantii. Il bootloader del nuovo EXE le legge → pensa
di essere "un sub-step di un altro PyInstaller" → cerca DLL in path
inesistenti → crash con LoadLibrary fail.

Lanciato da explorer.exe queste env vars NON sono presenti, quindi il
bootloader fa estrazione fresh → tutto OK.

Fix: nel batch updater, prima di `start`, set a vuoto tutte le env
vars PyInstaller note:
```bat
set "_PYI_APPLICATION_HOME_DIR="
set "_MEIPASS2="
set "_PYI_ARCHIVE_FILE="
set "_PYIBOOT_USER_PYTHONPATH="
set "_PYI_SPLASH_IPC="
start "" /D "<exe_dir>" "<new_exe>"
```

Aggiunto anche `/D <exe_dir>` per forzare la cwd del nuovo processo
nella cartella dell'EXE (evita che cwd resti = %TEMP% del batch).

### 🎨 FEAT-02 · Helper `_setup_standalone_dialog`
**File:** `gui/main_window.py`

Pedro: "il form crea utente è standalone Windows ora; potresti
applicare la stessa fix agli altri dialog?"

Refactoring: creato helper `_setup_standalone_dialog(win, root, title,
w, h)` che applica la "ricetta" finestra Windows nativa (entry in
taskbar, icona top-left, bring-to-front con topmost momentaneo,
centratura sopra main).

Applicato a:
- `_admin_show_create_user_dialog` (deduplicato il codice da v1085m+n)
- `_show_change_password_dialog` (era transient + grab_set custom)
- `_show_about` (era transient + center_win custom)
- `_show_upgrade_dialog` (era overrideredirect=True + transient — questo
  era il caso più rischioso perché aveva titlebar custom; mantenuta la
  titlebar custom ma rimossa overrideredirect → ora ha sia titlebar
  Windows che titlebar custom; vedremo se cosmetico OK)

Flyout/tooltip/popup leggeri NON modificati (devono restare borderless).

### 🌴 FEAT-03 · Caraibica spostata a piano Advanced
**File:** `config/user_plans.py`

Pedro: "spostare il tab Caraibica nel piano Advanced".

Cambio: `pro.tab_caribbean` da `True` a `False`. Solo `advanced` ha
il tab.

NB: per gli utenti Pro che già lo usano, il tab apparirà bloccato
con overlay "Feature non disponibile, richiedi upgrade". Comportamento
corretto.

### 🪟 FEAT-04 · Titolo finestra principale
**File:** `gui/main_window.py`

Pedro: "Sulla barra di windows in cima alla pagina principale rimuovi
'advanced' e la versione che è già riportata dentro la finestra. Lì
invece dovrà apparire 'Music Cataloger | Piano attivo'".

Fix: `self.root.title("Music Cataloger  |  <Plan.display_name>")`.
Niente più "Advanced" né version string nel chrome OS — sono dentro
l'app.

### 📦 ZIP NON contiene icons/

Le icone restano sul disco di Pedro (le mie sono JPEG-truccate).
Pedro estrae il ZIP sopra C:\dev\music-cataloger\ senza sovrascrivere
icons/.

---

## v1085p — Auto-update encoding fix + log style + macrogenere fix (2026-05-04)

### 🐛 BUG-01 · Auto-update charmap codec error
**File:** `services/updater.py::_make_windows_updater_script`

Pedro test v1085o: dialog update si bloccava su "Preparo aggiornamento..."
con log `_do_update fallito: 'charmap' codec can't encode character
'\u2192' in position 2763`.

Causa: avevo aggiunto in v1085m+n+o ~50 righe di commenti REM Unicode
(con frecce `→`) per documentare le strategie. Quando Python scrive il
batch su disco usa cp1252 (richiesto da cmd.exe) — cp1252 non ha U+2192
quindi UnicodeEncodeError.

Fix duplice:
1. Riscritto batch v1085f-style — copy /Y semplice + retry, niente
   rename, niente .old backup. Le toppe v1085m introducevano complessità
   senza risolvere il bug di base (env vars pollution).
2. Try cp1252, fallback utf-8 con BOM se ci sono caratteri speciali nei
   path utente.
3. Tutti i commenti REM ora sono ASCII puro.

L'env cleanup PyInstaller (causa root del crash python313.dll) è
PRESERVATO — quella era la fix vera. Tutto il resto (.old, rename
atomic, fallback dialog) era teatro.

### 🐛 BUG-02 · Macrogenere "Hard Rock" non matchato
**File:** `core/cataloger.py::_get_parent_genre`

Pedro: "Hard Rock dovrebbe finire in Rock/, invece va in root".

Causa: `_PARENT_MAP` ha key `"Hard Rock"`, ma il caller fa
`raw_genre.strip().capitalize()` → "Hard rock" (lowercase la 'r' di
rock). La key non matcha → fallback ritorna il genere stesso → checking
"hard rock" lower == "hard rock" lower → True → considerato "subgenere
= macrogenere" → resta in root.

Fix: `_get_parent_genre` ora fa lookup case-insensitive (cache
`_parent_map_lower` su istanza). Aggiunti anche subgeneri mancanti nel
PARENT_MAP (Pachanga, Latin Jazz, Soca, Dancehall, Funk, Gospel,
Tropical House, Afrobeats, Bossa Nova). Caller ora passa
`raw_genre.strip()` invece di `.capitalize()`.

### 🎨 BUG-03 · Filtro log non rifiltra righe esistenti
**File:** `gui/main_window.py`

Pedro: "click sui pulsanti INFO/WARNING/ERROR svuota il log invece di
rifiltrare le righe già emesse".

Causa: `_log_apply_filter()` chiamava `clear()` poi ripopolava da
`_log_all_lines`. Ma in TUTTO il codice si chiama `self._log.append()`
diretto (non `self._log_append`), quindi il buffer restava vuoto.

Fix: monkey-patch su istanza LogViewer.append per popolare anche
`_log_all_lines`. Il filtro ora rifiltra tutta la storia. Mappo i
livelli "speciali" (DEBUG, SUCCESS) su INFO ai fini del filtro perché
i 3 toggle UI sono solo INFO/WARNING/ERROR.

### 🐛 BUG-04 · Warning finali colorati ROSSO
**File:** `gui/main_window.py::_classify_line`

Causa: `_classify_line` matcheva la parola "errore" nel CONTENUTO della
riga prima del livello dichiarato. Le righe `... - WARNING - Errore
spostamento ...` (parola "Errore" nel testo, level WARNING) venivano
classificate ERROR → colorate rosso.

Fix: priorità al pattern del logger Python (`" - LEVEL - "`). Solo
se non c'è pattern, fallback su contenuto.

### 🎨 FEAT-05 · Log style nuove convenzioni
**File:** `core/cataloger.py`

Pedro: "i tag tipo `[GENERE ESCLUSO]`, `[RINOMINA]`, `[SUBFOLDER ESCLUSO]`
sembrano debug; usa lo stile `>--` come gli altri".

Cambiato:
- `[GENERE ESCLUSO] X → macrogenere: Y` → `>-- Genere X Escluso -> Macrogenere: Y`
- `[SUBFOLDER ESCLUSO] X → ...` → `>-- Subfolder Escluso: X -> ...`
- `[RINOMINA] → name` → `>-- Rinomina: name`

### 🖼️ BUG-06 · About logo path in onefile
**File:** `gui/main_window.py::_show_about`

Causa: usava `Path(__file__).parent.parent / "icons" / "app" / "app_icon_256.png"`.
In onefile `__file__` è dentro `_MEIPASS` quindi punta correttamente, MA
se `app_icon_256.png` non c'è nel bundle (Pedro lo aveva sostituito con
una versione vecchia su disco), il dialog mostra fallback emoji.

Fix: MEIPASS-aware + fallback a `taskbar_active.png` se app_icon_256
non c'è. Convert RGBA esplicito.

### 🪟 BUG-07 · Cambia password: bottone Conferma fuori dal bordo
**File:** `gui/main_window.py::_show_change_password_dialog`

Pedro: "scomparso il pulsante di conferma".

Causa: dialog 440x400 era OK con titlebar custom (overrideredirect),
ma con titlebar Windows nativa (v1085o) la titlebar mangia ~30px e i
bottoni andavano sotto il bordo. CTk non scrolla automaticamente.

Fix: aumentato a 460x480.

### 📝 NOTA · Tab Cache info mancanti

Pedro: "alcuni file nella tab Cache non hanno tutti i metadati".

Conferma: i 2 file DB locali (`metadata_cache.json` per cache lookup
esterni + `music_library.json` per stato catalogo) sono separati per
scope. Pedro vuole unificarli in un nuovo branch.

DA FARE in branch `dev/unify-local-db` (post pilot 1):
- Schema unificato (es. `local_db.json` con sezioni cache/library)
- Migrazione automatica al primo boot v1086+
- Tab Cache + DB Locale + Generi interrogano stesso file

---

## v1086 — Pilot 1 RELEASE 🚀 (2026-05-04)

Prima release stabile del pilot. Tester:
- Pedro (Windows) — sviluppatore
- Amico Pedro (Linux Ubuntu) — primo tester esterno

### Tutti i bug critici v1085i...v1085p sono inclusi e fixati:

✅ Auto-update funzionante (fix env vars `_PYI_*` cleanup nel batch)
✅ Persistenza sessione/last_email in modalità onefile
✅ Form crea utente come finestra Windows standalone
✅ Form cambia password / About / Upgrade Plan tutti standalone
✅ Macrogeneri case-insensitive (Hard Rock → Rock/, Country Pop → Pop/)
✅ Filtro log ri-applica filtro a tutta la storia (no più svuotamento)
✅ Warning corretto colorato giallo (no rosso)
✅ Log style nuove convenzioni (`>--` invece di `[TAG]`)
✅ Tab Caraibica solo Advanced
✅ Titolo finestra `Music Cataloger | <Piano>`
✅ `_is_latin_file` metodo aggiunto

### Build Linux supportata
File `BUILD_LINUX.md` con guida per il tester. Spec `music_cataloger_linux.spec`
allineata allo Windows (onefile, niente UPX). File `.desktop` per
integrazione DE.

### Branch strategy post-v1086
- `main` = sempre v1086 stabile
- `dev/unify-local-db` = unificazione metadata_cache + music_library
- `dev/community-db` = pilot 2: DB community-driven
- Eventuali hotfix v1086 → tag `v1086.1`, `v1086.2`...

---

## v1086.1 — Sources priority funzionale + cmd window nascosta (2026-05-05)

Branch: `dev/sources-priority`

### 🐛 BUG-01 · Cmd window updater visibile e fastidiosa
**File:** `services/updater.py`

Pedro: "l'update apre una finestra cmd e non la chiude".

Causa: lancio batch via `cmd /c start "" /MIN <bat>` con
DETACHED_PROCESS (0x08). `/MIN` minimizza la console ma resta visibile
in taskbar e flasha all'avvio. Inoltre il `:FAIL` usava `pause`,
causando blocco indefinito se la console fosse riapparsa.

Fix:
- `subprocess.Popen([str(script)], creationflags=CREATE_NO_WINDOW |
  DETACHED_PROCESS)` — niente wrapper cmd /c, niente /MIN: la console
  e' completamente nascosta.
- Tolto `pause` dal :FAIL. Errori vanno solo nel %LOG%.
- Tolti `echo` verso stdout (la console e' nascosta, sono inutili).

### 🎯 FEAT-02 · Sources priority funzionale (era hardcoded)
**Files:** `services/external_apis.py`, `services/bpm_services.py`,
          `core/cataloger.py`, `run_cataloger.py`, `gui/main_window.py`

Pedro segnala: "le checkbox priorita' sorgenti nel tab Avanzate sono
solo visuali, deselezionarle non ha effetto sul flow".

**Vero**: la cascata `ExternalAPIs.search_all` era hardcoded:
musicbrainz → deezer → itunes → lastfm → discogs. Le BooleanVar
`self._meta_sources` esistevano nella UI ma non venivano mai propagate
al subprocess cataloger.

Pipeline implementata (UI → subprocess):

```
GUI._meta_sources (BooleanVar dict)
  └→ build_cmd() filtra per .get() → --metadata-sources [list ordinata]
                                      --bpm-sources [list]
                                      --no-external (se tutti deselezionati)
       └→ run_cataloger.py argparse
            └→ MusicCataloger(metadata_sources=..., bpm_sources=...)
                 └→ ExternalAPIs(enabled_sources=[...])
                 └→ BPMServices(enabled_sources=[...])
                      └→ search_all() / estimate_bpm() filtrano cascata
```

Sorgenti riconosciute:
- **Metadata** (search_all): musicbrainz, deezer, itunes, lastfm, discogs
- **BPM** (estimate_bpm): getsong, beatport (TuneBat e SongBPM sempre
  attivi come fallback automatici)
- **AcoustID/AudD**: fingerprint-only, gestiti separatamente per file

Casi limite:
- Lista vuota da UI → `--no-external` (= cascata DB esterni saltata)
- Typo / sorgente sconosciuta → filtrata silenziosamente
- Ordine UI preservato = priorita' cascata

### 💾 FEAT-03 · Persistenza preferenze sorgenti
**File:** `gui/main_window.py`

Le selezioni delle 9 checkbox sources ora persistono tra sessioni in
`data/sources_prefs.json`. Implementato via:
- `_load_sources_prefs()` chiamato all'init (default values fallback)
- `var.trace_add("write", _save_sources_prefs)` su ogni BooleanVar
  → autosave ad ogni click utente

### 🧪 Test pipeline
Smoke test in subprocess Python verifica:
- CLI accetta `--metadata-sources` e `--bpm-sources`
- ExternalAPIs valida lista (filtra typo, fallback a default se vuoto)
- BPMServices stesso
- Ordine preservato dalla UI alla cascata

Test reali (con Pedro):
- [ ] Catalogazione con TUTTI gli enabled → log mostra tutte le sorgenti
- [ ] Solo Discogs+AcoustID enabled → MusicBrainz/Deezer/iTunes/Lastfm SKIPPATI
- [ ] Tutti disabilitati → no_external, niente chiamate
- [ ] Riavvia client → checkbox restano come selezionate ultima volta

---

## v1086.1 — Round 2 (2026-05-05): cmd hidden + master sync + riordino frecce

Test feedback Pedro su v1086.1 round 1:
1. Cmd visibile durante update → CREATE_NO_WINDOW + DETACHED non bastava
2. Master "Abilita Sorgenti DB Online" non sincronizzata con checkbox Avanzate
3. Riordino priorita' tramite "ordine di selezione" non intuitivo → frecce ↑↓

### 🐛 BUG-04 · Cmd window visibile (v1086.1 round 1 fallito)
**File:** `services/updater.py`

Problema: `subprocess.Popen([bat], creationflags=CREATE_NO_WINDOW |
DETACHED_PROCESS)` produceva ancora finestra cmd visibile. Causa: per
i `.bat` file, DETACHED_PROCESS stacca dalla console del padre ma il
batch interpreter (cmd.exe) crea comunque la propria console. I due
flag insieme non interagiscono come previsto.

Fix: uso `STARTUPINFO` con `STARTF_USESHOWWINDOW` + `wShowWindow=SW_HIDE`,
piu' `creationflags=CREATE_NO_WINDOW`. Aggiunto reindirizzamento
stdin/stdout/stderr a DEVNULL per evitare che il batch erediti handle
del processo padre in chiusura. Lo `start "" /D ... <exe>` interno al
batch ottiene la sua propria configurazione di finestra (visibile),
quindi l'app relauchata appare normalmente.

### ✨ FEAT-05 · Master "Abilita Sorgenti DB Online" sincronizzata
**File:** `gui/main_window.py`

Problema: la master in pannello sinistro era indipendente dalle 9
checkbox in tab Avanzate. Pedro: "se disabilito tutti i DB online da
avanzate, la master rimane attiva".

Implementazione:
- `_on_ext_db_toggle()` ora setta effettivamente le 9 BooleanVar:
  - master OFF → salva stato corrente (`_sources_pre_disable_state`),
    spegne tutte le sources
  - master ON → ripristina lo stato salvato; se assente, riaccende
    solo le free-tier (token rimangono off di default)
- `_on_source_changed(key)` (nuovo trace handler) propaga al contrario:
  - Se utente accende una sorgente da Avanzate con master OFF → master ON
  - Se utente spegne l'ultima sorgente accesa → master OFF
- Guard `_syncing_master` evita loop infiniti durante la sincronizzazione
  programmatica master ↔ children

### ✨ FEAT-06 · Riordino priorita' tramite frecce ↑↓
**File:** `gui/main_window.py`

Pedro suggerimento (analogia con tab Caraibica): "sarebbe meglio
gestirlo come si è gestita la priorità di classificazione per la
caraibica, la differenza è che si sposta la checkbox in su o giù".

Implementazione:
- Nuovo attributo classe `_SOURCE_META`: dict {key → (nome, descrizione,
  richiede_token)} per tutte le 8 sorgenti
- Nuovo state `self._sources_order`: lista ordinata persistita in
  `data/sources_prefs.json` sotto chiave `_order`
- `_redraw_sources_list()`: ridisegna dinamicamente le righe sources.
  Ogni riga ha [▲][▼][☑] N. Nome — desc. Frecce ai bordi disabilitate
  (su per primo elemento, giu per ultimo). Token-tier (Discogs,
  AcoustID) marcate in verde.
- `_move_source(key, ±1)`: swap nella lista, salva, ridisegna
- `build_cmd()` aggiornato: `--metadata-sources` ora segue
  `self._sources_order` invece di iterazione fissa

Migration: il vecchio `sources_prefs.json` continua a funzionare; al
prossimo avvio il loader aggiunge `_order` con il default ordine.

### 🧪 Test logici (smoke)
Verificato in subprocess Python:
- Default order: produce ['musicbrainz', 'deezer', 'itunes', 'lastfm']
- Deezer in cima dopo riordino: produce ['deezer', 'musicbrainz', ...]
- Discogs in cima con token: produce ['discogs', 'musicbrainz', ...]
- Master OFF: produce lista vuota → --no-external
- Mix arbitrario: rispetta sempre l'ordine in self._sources_order

---

## v1086.1 — Round 3 (2026-05-06): fix regressione cascata BPM

### 🐛 BUG-07 · Cascata BPM ignorava UI quando tutto disabilitato
**Files:** `run_cataloger.py`, `services/external_apis.py`,
          `services/bpm_services.py`, `gui/main_window.py`

Pedro feedback dal log catalogazione test 1 (solo Discogs+AcoustID
abilitati): `BPMServices: cascata abilitata = ['getsong', 'beatport']`
nonostante UI mostrasse Beatport e GetSong DESELEZIONATI.

Root cause: catena di tre buchi:
1. `argparse --bpm-sources nargs="+"` non accetta lista vuota
2. GUI quando lista vuota OMETTEVA il flag (perche' "+", non "*")
3. Cataloger riceveva `args.bpm_sources = None` (= "non passato"),
   trattato come "usa default"

Risultato: utente non aveva modo di disabilitare TUTTI i BPM.
Stessa logica era latente per metadata, ma mascherata da
`--no-external` come fallback alternativo.

Fix sequenziale:
- argparse: `nargs="+"` → `nargs="*"` per `--metadata-sources` e
  `--bpm-sources` (accettano lista esplicitamente vuota)
- GUI build_cmd: passa SEMPRE entrambi i flag, anche se vuoti
- ExternalAPIs e BPMServices: `enabled_sources=None` → default,
  `enabled_sources=[]` → cascata vuota (NESSUN fallback)
- Rimosso il fallback `--no-external` quando metadata vuoti
  (non distingueva piu' meta-only off vs entrambi off)

Ora `BPMServices: cascata abilitata = []` e' uno stato valido che
significa "solo TuneBat/SongBPM/librosa fallback automatico".

### 🐛 BUG-08 · Discogs senza token: nessun warning
**File:** `services/external_apis.py`

Pedro test: Discogs viene chiamato ma non ritorna nulla. Causa:
`DISCOGS_TOKEN` mancante in `secrets.py`. Prima: silently None.

Fix: warning all'init di `ExternalAPIs` se Discogs e' in
`enabled_sources` ma il token manca. Stesso warning per AcoustID
(ACOUSTID_API_KEY) con nota aggiuntiva che non e' ancora integrato
nella cascata.

### 🎨 UI-09 · Label AcoustID piu' onesta
**File:** `gui/main_window.py`

AcoustID era etichettata "fingerprinting (richiede fpcalc.exe)" ma
la cascata `search_all()` non la chiama (e' un metodo file-based,
non query-based). Pedro l'ha selezionata aspettandosi che funzionasse.

Fix: label ora "fingerprint (non ancora integrato — pilot 2)".
Il toggle resta funzionale (stato persiste) ma e' chiaro all'utente
che oggi NON viene chiamata.

### 📝 TODO per branch dev/unify-local-db (rimandati)
1. **App non si riavvia dopo l'update**: il batch e' nascosto con
   STARTUPINFO+SW_HIDE, ma `start "" /D ... <new_exe>` interno al
   batch eredita il "show flag" del cmd.exe nascosto e l'EXE PyInstaller
   non appare. Possibili approcci: usare `cmd /c start "" ...` esplicito
   o sostituire `start` con una chiamata Python finale che faccia
   `subprocess.Popen([new_exe])` con startupinfo normale. Da indagare.
2. **`LocalMusicDB.upsert() got an unexpected keyword argument 'cataloged_at'`**:
   regressione dell'API DB locale. Visibile nei log delle sezioni
   "Correggi Metadati Cartelle Esistenti". Probabilmente firma
   `upsert()` cambiata senza aggiornare i call site.
3. **AcoustID integrazione fingerprint reale**: aggiungere
   `process_audio_fingerprint()` come fallback dopo che `search_all()`
   esaurisce le sorgenti query-based.
4. **DB merge**: unificare `metadata_cache.json` + `music_library.json`
   in `local_db.json` con sezioni `cache/library` + migration al primo boot.

---

## v1086.1 — Round 4 (2026-05-06): Discogs token caricato + AcoustID nascosta

### 🐛 BUG-10 · `_get_key()` argomenti scambiati per Discogs/AcoustID/AudD
**File:** `config/secrets.py`

Pedro: "il token Discogs è X, mettilo in secrets". Aprendo il file il
token era GIA' nel codice. Investigando, scoperto bug:

```python
# PRIMA (rotto)
self.DISCOGS_TOKEN = self._get_key('uDnXzYJqaiNqwclprniLgPlCsEqfoEzBaTyDPAiF', None)
#                                  ^^^ usato come ENV_VAR (1° arg)
```

`_get_key(env_var, default)` esegue `os.getenv(env_var, default)`.
Quindi cercava una env var nominata col valore del token → mai presente
→ ritornava `None` → token effettivamente disabilitato.

Stesso bug per `ACOUSTID_API_KEY` e `AUDD_API_KEY`. Le 3 righe
storicamente sbagliate erano in formato `(token, None)` invece di
`('NAME', token)`.

```python
# DOPO (corretto)
self.DISCOGS_TOKEN = self._get_key(
    'DISCOGS_TOKEN',
    'uDnXzYJqaiNqwclprniLgPlCsEqfoEzBaTyDPAiF'
)
```

Effetto collaterale del fix: Discogs ora funziona davvero in
catalogazione. Pedro vedra' `[Discogs]` nei log al prossimo run.
Il warning "DISCOGS_TOKEN mancante" introdotto in round 3 non
apparira' piu'.

### 🎨 UI-11 · AcoustID temporaneamente nascosta
**File:** `gui/main_window.py`

Pedro: "il piano free AcoustID e' scaduto, nascondila per ora".

Implementato come "soft hide": la BooleanVar `acoustid_enabled` resta
in `self._meta_sources` per compatibilita' con `sources_prefs.json`
esistenti (utenti che gia' avevano salvato lo stato non vedranno errori),
ma `_SOURCE_META` non la elenca, quindi `_redraw_sources_list` la
salta in fase di rendering.

`_move_source` aggiornato per fare swap col PROSSIMO ELEMENTO VISIBILE
saltando keys nascoste (altrimenti le frecce ↑↓ sembrerebbero rotte
quando il vicino e' nascosto). Numerazione (1, 2, 3...) e' separata
dall'indice nella lista — usa `visible_idx`.

Per ripristinare AcoustID in futuro: aggiungere la entry in
`_SOURCE_META` (e idealmente integrarla nella cascata fingerprint).

### Stato finale v1086.1 — pronto per merge in main
Round 1 (5 maggio):
- Cmd updater nascosta (CREATE_NO_WINDOW + DETACHED, fallito)
- Sources priority funzionale (era hardcoded)
- Persistenza preferenze sorgenti

Round 2 (5 maggio):
- Cmd updater fix (STARTUPINFO + SW_HIDE)
- Master "Abilita Sorgenti DB Online" sincronizzata
- Riordino con frecce ↑↓

Round 3 (6 maggio):
- Fix regressione cascata BPM (None vs [])
- Discogs warning se token mancante
- AcoustID label "non integrata"

Round 4 (6 maggio):
- Discogs token effettivamente caricato (bug pregresso _get_key)
- AcoustID nascosta dalla UI

### TODO portati a dev/unify-local-db (riconfermati)
1. **App restart post-update**: batch nascosto via SW_HIDE, ma `start ""`
   interno non riesce a rendere visibile l'EXE relauched.
2. **`LocalMusicDB.upsert() got an unexpected keyword argument 'cataloged_at'`**:
   firma upsert() cambiata, call site non aggiornato.
3. **Unificazione DB locali**: `metadata_cache.json` + `music_library.json`
   → `local_db.json` con migration al primo boot.
4. **AcoustID integrazione fingerprint** (verra' riattivata insieme alla
   reintroduzione UI quando Pedro avra' il token attivo).

---

## v1086.1 — Round 4 (continuato): fix admin first-load

### 🐛 BUG-12 · Sezioni admin "Caricamento…" al primo avvio
**File:** `gui/main_window.py`

Pedro: "nella round 3 hai reintrodotto il bug del caricamento delle
finestre admin al primo avvio, devo cliccare Aggiorna per vedere i dati".

Causa probabile: i 5 refresh admin erano schedulati con
`self.root.after(200..600, _admin_refresh_*)` separatamente. Al primo
avvio, alcuni timer scattavano prima che il widget fosse "mapped" sullo
schermo (Tk window non ancora visibile, init main loop non finito).
I worker thread partivano, le chiamate HTTP forse partivano anche, ma
il `_safe_after(0, render)` ricavato non aveva un widget attached
correttamente e il render falliva silenziosamente.

Fix: rimosse le 5 chiamate `after(N, _admin_refresh_*)` da dentro
`_build_admin_section`. Aggiunto un metodo unificato
`_admin_kickoff_initial_load()` che chiama tutti e 5 i refresh in
sequenza. Schedulato con due livelli di sicurezza:
- `self.root.after_idle(...)` → garantito quando Tk e' in idle
  (cioe' tutti i widget sono mount-completati)
- `self.root.after(2500, ...)` → safety net, riprova dopo 2.5s se
  per qualche motivo `after_idle` non scattasse (es. main loop
  bloccato da update_check)

Il kickoff e' idempotente: una seconda chiamata semplicemente rifa
i fetch HTTP, il widget viene aggiornato (e' lo stesso comportamento
del bottone Aggiorna).

### 📝 Stato finale v1086.1 — pronto per merge in main
Round 1 (5 maggio): cmd updater fix attempt 1, sources priority funzionale, persistenza prefs
Round 2 (5 maggio): cmd updater fix attempt 2 (SW_HIDE), master sync, frecce ↑↓
Round 3 (6 maggio): fix regressione cascata BPM, warning Discogs/AcoustID, label AcoustID
Round 4 (6 maggio): Discogs token caricato (bug pregresso _get_key), AcoustID nascosta UI, **admin first-load fix**

### 🚧 TODO portati a dev/unify-local-db (riconfermati, AcoustID rimandato)
1. **App restart post-update** (UX critica)
2. **`LocalMusicDB.upsert() cataloged_at`** (bug pregresso, log puliti)
3. **Unificazione DB locali** metadata_cache + music_library
4. ~~AcoustID integration~~ — rimandato finche' Pedro avra' il token attivo

---

## v1086.2 — dev/unify-local-db Task 1+2 (2026-05-06)

### 🐛 BUG-13 · `LocalMusicDB.upsert() got an unexpected keyword argument 'cataloged_at'`
**File:** `core/cataloger.py` (call site #2)

Causa: il chiamante in "Correggi Metadati Cartelle Esistenti" passava
`cataloged_at=_dt.now().strftime(...)` come kwarg, ma la firma di
`LocalMusicDB.upsert()` lo calcola internamente con
`datetime.now().isoformat()`. Errore loggato per OGNI file (rumore
log enorme nei test Pedro recenti).

Fix: rimosso il kwarg dal call site. `LocalMusicDB.upsert()` resta
single source of truth per il timestamp.

Note: c'erano 2 call site in cataloger.py — il primo (line 740) era
gia' corretto, solo il secondo (line 1198) era buggy.

### 🐛 BUG-14 · App non si riavvia post-update (window state hide propagato)
**File:** `services/updater.py`

Causa root: in v1086.1 round 2 ho introdotto `STARTUPINFO + SW_HIDE`
per nascondere la finestra cmd dell'updater. Funziona, ma su Windows
il flag SW_HIDE viene EREDITATO dai processi figli a meno di non
override esplicito. Quindi quando il batch chiamava
`start "" <new_exe>`, il nuovo EXE PyInstaller veniva avviato hidden
e l'utente non vedeva nulla.

Fix: nel batch sostituito `start ""` con `explorer.exe <exe>`. Il
trick classico Windows: quando explorer.exe lancia un processo, lo
fa nel contesto della shell utente (window state default = visible),
"staccato" dal contesto del padre nascosto. Mantenuto `start ""` come
fallback se explorer.exe non risponde (raro).

Riferimento: https://stackoverflow.com/q/29903706

### 🚧 TODO restanti per dev/unify-local-db
3. **Unificazione DB locali** (task grosso, prossimo turno):
   `metadata_cache.json` + `music_library.json` → `local_db.json`
   con sezioni `cache/library` + migration al primo boot.

---

## v1086.2 — Round 2 (2026-05-06): updater realmente fixato + UX critica

### 🐛 BUG-15 · Updater fail "30 tentativi in 1 secondo" (regression v1086.1)
**File:** `services/updater.py`

Pedro test: 30 retry in 1.37 secondi invece di 30 secondi. Ogni copia
falliva con "il file e' utilizzato da un altro processo".

Causa: in v1086.1 round 2 ho introdotto `stdin=subprocess.DEVNULL` per
nascondere la cmd. Effetto collaterale non previsto: il comando
`timeout /t N /nobreak` di Windows **richiede una stdin valida** (anche
se ridiretta a NUL non funziona) e fallisce immediatamente con errore
"il reindirizzamento dell'input non e' supportato; uscita immediata".
I 30 retry quindi non aspettavano il secondo previsto fra l'uno e
l'altro: tutto si concentrava in ~1 secondo, durante il quale l'EXE
vecchio non aveva tempo di rilasciare il lock sul file → copia rotta.

Fix: sostituito `timeout /t 1 /nobreak >nul` con `ping -n 2 127.0.0.1 >nul`.
`ping` non ha il problema con stdin ridiretto. E' il workaround standard
Windows per "sleep N secondi in batch script con stdin chiusa".
- `ping -n 3` → ~2 secondi (warmup iniziale)
- `ping -n 2` → ~1 secondo (fra retry)

### 🐛 BUG-16 · `bpm` come stringa rompe `LocalMusicDB.upsert()`
**File:** `services/local_db.py`

Pedro feedback: ancora `DEBUG - DB update err: type str doesn't define
__round__ method` per ogni file. Diverso dal bug round 1 (cataloged_at):
ora il problema e' che alcuni file hanno BPM nel tag ID3 come stringa
("128") invece che numero, e `round("128", 1)` rompe.

Fix: coercion difensiva all'inizio di upsert. `float(bpm)` con
try/except, e stessa cosa per `quality_kbps`. Test smoke verificato:
str/None/float/typo tutti producono il valore atteso.

### ✨ FEAT-17 · Single instance lock (Pedro UX request)
**Files:** `services/singleton.py` (nuovo), `run_gui.py`

Pedro: "se apro l'EXE una seconda volta, il programma si apre
tranquillamente. Non ci dovrebbe essere un controllo per impedire di
riaprire il programma o per lo meno portare in primo piano quello gia'
aperto?".

Implementazione:
- `services/singleton.py`: lock TCP socket su 127.0.0.1:47286.
  Vantaggio rispetto a lock file con PID: il SO rilascia
  automaticamente la porta quando il processo muore (anche su crash),
  no PID stale.
- All'avvio (PRIMA degli import pesanti come ctk), `acquire()` prova
  a bind. Se fallisce → un'altra istanza e' attiva.
- `bring_existing_to_front()`: scansiona tutte le finestre Windows
  via `EnumWindows`, cerca quella che inizia con "Music Cataloger",
  fa `ShowWindow(SW_RESTORE) + SetForegroundWindow`.
- `show_already_running_dialog()`: MessageBox nativo Windows
  (no Tk import per essere veloce).

Edge case: il lock va PRIMA del check `--cataloger-mode` per
permettere ai subprocess cataloger figli di aprirsi (sono "seconde
invocazioni legittime" dell'EXE durante la catalogazione).

[CORREZIONE]: in realta' il lock va DOPO `--cataloger-mode` perche'
il subprocess parte con `sys.executable` cioe' lo stesso EXE; senza
escluderlo, il subprocess non riuscirebbe ad acquisire il lock e
fallirebbe la catalogazione. Fix in run_gui.py: `_singleton_acquire`
e' chiamato solo se `--cataloger-mode` NON e' nei sys.argv.

### ✨ FEAT-18 · Modal dialog (blocco interazione main mentre dialog aperto)
**File:** `gui/main_window.py`

Pedro: "se c'e' una finestra attiva il programma principale rimanga
bloccato e non ti faccia aprire altre finestre o lavorare sulla
principale, cosi' non avviene".

Fix: `_setup_standalone_dialog()` ora supporta `modal=True` (default).
Applica `transient(root) + grab_set()` ritardato di 50ms (per evitare
fallimento se chiamato prima che la finestra sia mappata).

Effetto: dialog "Cambia password", "Crea utente", "Upgrade Plan",
"About" bloccano la main finche' non chiusi. L'utente non puo' piu'
premere "Avvia" mentre ha un dialog aperto.

### 🐛 BUG-19 · Generi esclusi nelle prefs non rispettati (diagnostico)
**File:** `gui/main_window.py`

Pedro: "ho escluso Alternative e World ma li cataloga". Verificato
con simulazione: la logica `_genre_prefs[macro::sub]` e' corretta
e produce la lista exclude attesa.

Il problema piu' probabile: o le prefs sono state salvate DOPO
l'avvio della catalogazione, oppure il path `_get_data_dir()` punta
a una location diversa fra dev e EXE (OneDrive sync, working dir).

Aggiunto log debug `[build_cmd] excluded_genres dalla UI: N user +
M always = K totali` con dettaglio della lista user-excluded. Al
prossimo run Pedro vedra' chiaramente cosa il client passa.

### ✨ FEAT-20 · Riepilogo distribuzione completo (no top-10)
**File:** `core/cataloger.py`

Pedro: "se ci sono generi con meno di 5 canzoni non lo segnala. Deve
essere una funzionalita' attiva per tutta la catalogazione".

Causa: `[:10]` slicing dopo sorted in `_print_summary`. Conferma del
report JSON: 17 generi totali, di cui solo i top 10 nel log.

Fix: rimosso `[:10]`. Tutti i generi rilevati ora sono mostrati,
ordinati per count desc. Cambiato titolo da "TOP GENERI" a
"DISTRIBUZIONE GENERI (N)" per chiarezza.

### 🚧 TODO restanti per dev/unify-local-db
3. **Unificazione DB locali** (task grosso, prossimo turno):
   `metadata_cache.json` + `music_library.json` → `local_db.json`
   con sezioni `cache/library` + migration al primo boot.

---

## v1086.3 — dev/unify-local-db (TASK 3 PRINCIPALE)

### 🎯 BIG-CHANGE-21 · DB locali UNIFICATI in `local_db.json` (Option B)
**Files:** `services/local_db.py` (riscritto), `services/cache_manager.py`
(eliminato), `core/cataloger.py`, `gui/main_window.py`, `config/settings.py`

Pedro: "L'opzione B mi sembra quella più pulita anche se più grossa,
inoltre fare questo refactoring dovrebbe sistemare alcuni metadati che
nel tab cache risultano vuoti perché sono nell'altro file. Avere un dato
unico per file aiuterà in futuro la logica di merging per community".

#### Schema v2
```json
{
  "version": 2,
  "last_updated": "...",
  "files": {
    "<rel_path>": {
      "artist": "...", "title": "...", "album": "...",
      "genre": "...", "subgenre": "...",
      "bpm": float | null, "quality_kbps": int | null,
      "external_lookup": {
        "source": "MusicBrainz" | "iTunes" | ...,
        "raw_genre": "...", "raw_bpm": float, "cached_at": "..."
      },
      "cataloged_at": "..."
    },
    "__orphan__:<artist>|||<title>": { ... }   # cache entries senza file reale
  },
  "lookup_by_query": {
    "<artist>|||<title>": "<rel_path>"   # indice inverso per cache lookup
  }
}
```

Un solo record per brano, con metadati di catalogazione + cache API
attaccata. L'indice `lookup_by_query` e' inverso (artist|title → path)
e permette alla cascata API di cercare "ho gia' visto questo (artist,
title)?" senza scansionare tutti i files.

#### Migration v1 → v2 (automatica al primo boot)
- Eseguita da `migrate_legacy_to_v2(data_dir)`
- Idempotente: no-op se `local_db.json` gia' esiste
- Inferenza euristica artist/title dal filename (pattern
  "Artist - Title.mp3") per collegare cache → record file
- Cache entries senza match diventano "record orfani"
  (`__orphan__:artist|||title`) — preservate ma escluse dalle
  viste library
- I file legacy `metadata_cache.json` e `music_library.json`
  vengono rinominati `.migrated_v2` (no perdita dati)

Testato con smoke test: 9 scenari coperti (upsert con coercion str→num,
cache attaccata a file esistente, cache orfana, promozione orfano →
file reale, save/load atomico, backcompat LocalMusicDB.upsert(),
migration con inferenza, idempotenza, scrittura .tmp+rename).

#### Eliminazione `services/cache_manager.py`
Era orfano (nessun import nel codebase). Le sue responsabilita' sono
ora dentro `LocalDB.cache_external_lookup()` /
`LocalDB.get_cached_metadata()`.

#### Aggiornamento call site
1. `core/cataloger.py`:
   - init: chiama `migrate_legacy_to_v2()` + apre `LocalDB(local_db.json)`
   - `load_cache()`: legge external_lookup dai records → popola
     `ExternalAPIs.metadata_cache` in-RAM
   - `save_cache()`: scrive `metadata_cache` in-RAM → external_lookup
     dei records via `cache_external_lookup()`
   - `backup_cache()`: copia `local_db.json` (era metadata_cache.json)
   - `upsert_file()` con artist/title/album passati dal `final_metadata`
     così l'indice lookup_by_query si popola correttamente

2. `gui/main_window.py`:
   - **Library tab** (`_db_reload`): legge `local_db.json`,
     filtra orfani (escludi keys `__orphan__:...`)
   - **Cache tab** (`_cache_reload`): RICOSTRUITA da zero. Scorri
     records → builda dict[query_key]→{source, genre, raw_genre, bpm,
     raw_bpm, artist, title, album, _path}. Pedro lamentava metadati
     vuoti nel tab cache: con questo schema unificato, il record file
     ha tutti i metadati ricchi (artist/album/genre/bpm)
   - **CSV export**: semplificato — niente piu' lookup ridondante in
     metadata_cache (era doppio file). Aggiunta colonna "Sorgente cache"
   - **Find duplicates / Quality scan**: filtrano orfani
   - **Clear cache**: ora svuota SOLO `external_lookup` + `lookup_by_query`,
     non tocca i record library (genere/bpm assegnati dal cataloger)

3. `config/settings.py`:
   - `cache_filename` aggiornato a `local_db.json` (era dead config
     non letto da nessuno, ma lo aggiorno per coerenza)

#### Backward compatibility
La classe `LocalMusicDB` resta come shim. Codice che chiama
`LocalMusicDB.upsert(path, genre, ...)` continua a funzionare,
internamente delega a `LocalDB.upsert_file()`.

#### Migration scenario per Pedro
Al primo avvio della v1086.3, l'EXE trovera':
- `data/metadata_cache.json` (~5 MB, cache esterna)
- `data/music_library.json` (1202 file dalla scorsa catalogazione)

Eseguira' la migration automatica:
- Crea `data/local_db.json` con tutti e due uniti
- I 1202 file della library + cache attaccata dove l'inferenza
  artist/title matcha
- Le entries cache senza match → orfani (preservati ma non visibili
  in library)
- Rinomina `metadata_cache.json` → `metadata_cache.json.migrated_v2`
  e idem per `music_library.json` (backup di sicurezza)

Log atteso:
```
DB legacy migrato → local_db.json: 1202 file, N cache entries (M orfani)
DB locale attivo: local_db.json (1202 file, N cache entries)
```

### 🚧 TODO post-task 3
1. **Test Pedro**: verifica migration + uso normale tab Library/Cache
2. **Merge in main + tag v1086.3-stable**
3. **Branch dev/security-audit** (priorita' 1: plan check server-side)
4. **Branch dev/community-db** (futuro: usa local_db.json come schema
   per il merge community)

---

## v1086.4 — dev/unify-local-db Round 4 (2026-05-11): cache roundtrip + UI fixes

### 🐛 BUG-22 · Tab Cache mostra record cancellati (Clear Cache "non funzionava")
**File:** `gui/main_window.py` (`_cache_reload`)

Pedro test 5: dopo Clear Cache, il tab cache mostrava ancora le righe.
Allegando i file `local_db.json` pre/post, ho visto che la cache ERA
stata svuotata correttamente (`lookup_by_query` da 12 → 0), ma il
`_cache_reload` mostrava tutti i record file che avessero artist+title,
indipendentemente dalla presenza di `external_lookup`.

Fix: `_cache_reload` ora skippa i record SENZA external_lookup.
La cache view rappresenta solo i record effettivamente cached, come
dovrebbe essere.

### 🐛 BUG-23 · Cache roundtrip povero (cover_url e altri campi persi)
**Files:** `core/cataloger.py` (`load_cache`, `save_cache`),
          `services/local_db.py` (migration cache)

Pedro test 3: "i metadati non sono tutti popolati e le cover album non
sono più visibili nè per i vecchi dati nè per i dati di nuova
catalogazione". Diagnosi: il roundtrip cache era impoverito.

- La cache in-RAM di `external_apis.py` contiene blob RICCHI con
  artist, title, album, genre, bpm, cover_url, year, source
- Ma `save_cache` v1086.3 round 1 estraeva SOLO `source/raw_genre/raw_bpm`
- E `load_cache` ricostruiva solo `{source, genre, bpm}` in-RAM
- E la migration legacy → v2 perdeva idem tutti gli extra campi

Risultato: ogni roundtrip cache (load → use → save) impoveriva
sistematicamente i metadati. Le cover URL, gli anni, gli album extra
venivano persi al primo restart.

Fix RICCO:
- `external_lookup` ora contiene il PAYLOAD COMPLETO della risposta API
- `save_cache` copia integralmente `metadata_cache[qk]` come
  `external_lookup`, aggiungendo solo `cached_at` timestamp
- `load_cache` ritorna il blob external_lookup intero in-RAM (con
  artist/title/album come fallback dal record file)
- Migration legacy preserva ALL i campi della cache vecchia
  (cover_url, year, etc.) invece di filtrare solo 3 campi

NOTA per Pedro: l'attuale `local_db.json` (post-migration con bug
round 1) ha gia' perso i cover_url ai miei dati cache. Per
recuperarli:
1. Chiudi l'app
2. Apri `data/`
3. Rinomina `local_db.json` → `local_db.json.bak`
4. Rinomina `metadata_cache.json.migrated_v2` → `metadata_cache.json`
5. Rinomina `music_library.json.migrated_v2` → `music_library.json`
6. Riavvia l'app — la migration v1086.4 partira' di nuovo, questa volta
   preservando TUTTO

### 🐛 BUG-24 · Statistiche dialog limitate a Top 8
**File:** `gui/main_window.py` (dialog "Catalogazione Completata")

Pedro test: il riepilogo nel log gia' mostrava tutti i generi (fix
v1086.2 round 2), ma il **dialog GUI** finale era ancora limitato a
`[:8]`. Lo stesso bug in un altro punto del codice.

Fix: rimosso `[:8]`. Dialog ora mostra TUTTI i generi rilevati con il
titolo aggiornato a "Distribuzione Generi (N)" per coerenza col log.
Il dialog e' gia' scrollable quindi nessun rischio di overflow.

### 🎨 UI-25 · Login email case-insensitive
**File:** `gui/login_window.py`

Pedro: "controlleresti anche la login, mi sembra che sia sensitive cap
la mail e non è necessario il campo sensitive per email/utente".

Fix duplice:
- `_do_login` normalizza email a lowercase prima di inviarla al server
- Entry email ha trace_add("write") che forza lowercase in tempo reale
  mentre l'utente digita (con preservazione cursor position).
  Se l'utente scrive "Mario@Gmail.com", vedra' "mario@gmail.com" man
  mano. Niente piu' confusione "ho scritto bene ma il server dice
  utente non trovato".
- Password resta case-sensitive (giusta).

### 🚧 Aperti
- Restano da identificare possibili altri punti che soffrono dello
  stesso pattern `Top N`/`[:N]` in altre viste GUI (audit futuro).
- Pedro chiedeva qualcosa su "permanenza delle impostazioni" ma il
  messaggio si era troncato. Da chiarire nel prossimo turno.

---

## v1086.5 — dev/unify-local-db Round 5 (2026-05-11): cache roundtrip FIX CRITICO

### 🐛 BUG-26 · Cache key format mismatch (causa root di tutto)
**Files:** `core/cataloger.py` (load/save_cache),
          `services/local_db.py` (migration)

**Bug critico scoperto** dal log di Pedro:
```
Cache caricata: 0 metadati
[...10 file processati con risposte iTunes valide...]
Cache salvata: 0 voci → local_db.json
```

Cause root: `external_apis.py` usa chiavi cache in formato
`"<provider>_<artist>_<title>"` (es. `"itunes_Akon_Lonely"`,
`"mb_Beatles_Yesterday_Help!"`). Da v1086.3, il refactor unificato
si aspettava chiavi `"artist|||title"` e splittava su `"|||"` →
TUTTE le entries scartate silenziosamente.

Sintomi visibili a Pedro:
1. Cache caricata 0 metadati ad ogni boot (ogni catalogazione rifa le
   query API → lente, sprecano rate limit)
2. Tab cache vuoto
3. Cover album mai visibili (cover_url nelle entries scartate)
4. CSV "Sorgente cache" vuota
5. Migration log: "421 errori non fatali" = le 421 entries cache che
   non riusciva a parsare con `_parse_legacy_query_key`

### Schema v1086.5 (aggregato per provider)
Il nuovo `external_lookup` aggrega le risposte di provider diversi
per stesso (artist, title):
```json
"external_lookup": {
  "primary": "itunes",
  "providers": {
    "itunes": { artist, title, genre, bpm, cover_url, year, ... },
    "musicbrainz": { artist, title, genre, ... }
  },
  "cached_at": "...",
  "source": "itunes",      // backcompat
  "raw_genre": "R&B/Soul", // backcompat (= primary.genre)
  "raw_bpm": 89            // backcompat (= primary.bpm)
}
```

**save_cache**: scorre `external_apis.metadata_cache` (chiavi
per-provider), estrae artist/title dai dati del payload (NON dalla
chiave, che e' ambigua), aggrega per stesso (artist, title), crea
external_lookup con sotto-sezione `providers`. Da n entries
per-provider → m records aggregati (con m ≤ n).

**load_cache**: per ogni record con external_lookup, ricostruisce le
chiavi per-provider che external_apis.py si aspetta:
`"itunes_Akon_Lonely"`, `"mb_Akon_Lonely"`, ecc. Cosi' al prossimo
boot la cache HIT funziona davvero.

**Migration**: stessa logica del save. Parsing del prefisso provider
dalla chiave legacy, aggregazione per (artist, title) preso dal payload.

### 🐛 BUG-27 · Orfani-candidati: Indie/Blues/Ambient inclusi
**File:** `gui/main_window.py`

Pedro: "vedi che non consiglia a sottogeneri con pochi file di spostarli
in macrogenere? (in questo caso Indie e Blues)". 

Causa: `_MACRO_GENRES` set conteneva `blues`, `indie`, `ambient` →
venivano esclusi dal filtro orfani anche se avevano 1 file ciascuno.

Fix: rimossi dai macrogeneri. Ora Indie con 1 file viene suggerito
come spostabile sotto Alternative, Blues sotto Jazz, Ambient sotto
Electronic (mapping gia' presente in `_SUB_TO_MACRO`).

### 🚧 Aperti (Pedro test)
- Permanenza impostazioni menu sinistra → rinviato al prossimo branch
- Recovery cache: Pedro deve rifare la procedura ren/restore con
  v1086.5 (la migration ora funziona correttamente)

---

## v1086.6 — dev/unify-local-db Round 6 (2026-05-11): tab Cache enrichment

### 🎨 UI-28 · Layout dettaglio cache a 2 colonne con metadati estesi
**File:** `gui/main_window.py`

Pedro request:
1. "voci 'Titolo' e 'Artisti Partecipanti' invece di stampare solo
    titolo e artista" → header del record con titolo + artisti come riga
    separata
2. "indentare la colonna dei titoli metadati a sinistra e la colonna
    dei metadati a destra" → grid 2 colonne con label sx, valori dx
3. "aggiungere altri metadati nelle cache: dimensione file, bitrate,
    altri campi utili" → carta bianca

Implementazione:
- `_cache_detail_var` (singolo StringVar testuale) → SOSTITUITO da
  un layout strutturato:
  - Header in alto: `_cache_detail_title_var` (bold) + `_cache_detail_artist_var`
  - Grid 2 colonne in `CTkScrollableFrame` per i metadati
  - I widget label+valore vengono creati al primo select e RIUTILIZZATI
    ai successivi (StringVar) — niente ricostruzione, niente flicker
- Campi dettaglio:
  - Album, Anno, Genere, BPM, Durata (mm:ss formattato)
  - **Qualità** (kbps) — dal record file
  - **Sample rate** (Hz) — best-effort via mutagen
  - **Dimensione** (MB/KB/B leggibile) — `stat().st_size` se file esiste
  - **Sorgente** con annotazione `+N` se piu' provider hanno risposto
    (es. `"itunes (+1)"` = iTunes + MusicBrainz)

Note tecniche:
- L'accesso al disco per leggere size/bitrate/sample_rate e' best-effort:
  se il file e' un orfano (`_path = None`) o e' su path non risolvibile
  rispetto a `_db_base_path_var`, i campi mostrano "—" senza errori
- Mutagen e' gia' una dipendenza del progetto, riuso esistente

### 📝 Sul "campi non popolati" di Pedro (analisi)
Pedro test 3: "i metadati non sono tutti popolati... Alberto Indio ha
trovato corrispondenza su iTunes ma da tab cache non vedo genere/BPM".

Investigato: il record "Alberto Indio - Quero-te Dizer" e' un ORFANO
(`__orphan__:alberto indio|||quero-te dizer`) perche' il cataloger l'ha
processato MA non l'ha spostato (log: "Errore spostamento o SKIP
duplicato"). Quando upsert_file non viene chiamato, save_cache non
trova un file corrispondente nell'indice `lookup_by_query` e crea un
orfano. Comportamento corretto, non bug: i metadati iTunes sono
preservati nel record orfano (visibile nel tab cache), e quando il
file verra' davvero spostato in una catalogazione futura,
upsert_file fara' la promozione automatica orfano → record file
(logica gia' presente).

Altri "campi vuoti" in cache di Pedro: record di catalogazioni
PASSATE (pre-v1086.5) che non hanno mai avuto external_lookup
perche' la cache era rotta allora. Si sistemano solo con una
ricatalogazione (la cache da Quero-te Dizer e altre risposte API
sono adesso in `external_apis.metadata_cache` e verranno persistite
al prossimo save_cache).

### 🚧 Permanenza impostazioni menu sinistra
Pedro chiedeva di rendere persistenti anche le impostazioni del menu
di sinistra (come quelle del tab Avanzate). Rinviato al prossimo
branch dedicato (`dev/persistent-settings` o nel `dev/security-audit`,
da decidere). Aggiunto alla todo list.

---

## v1086.7-wip — dev/security-audit (Fase 1 client, work in progress)

⚠️ Questa è una versione **DI LAVORO** — non distribuibile in produzione finché
gli endpoint server proxy non sono implementati. I cambiamenti di seguito
sono PARTE del refactoring security, non lo stato finale.

### Cosa è cambiato

**`config/secrets.py`** — rimossi i 5 token sensibili (Discogs, Last.fm,
GetSong, AcoustID, AudD). Spotify CLIENT_SECRET messo a None. Solo
CLIENT_ID (pubblico) e identificativi MusicBrainz (user-agent, contact)
restano. Le chiavi sensibili viaggeranno via server proxy.

**`config/user_plans.py`** — `_DEFAULT_PLAN = "base"` (era "advanced").
`has_feature()` default = False per feature sconosciute (era True). Nuovo
helper `set_plan_from_server()` per impostare il piano post-login senza
manipolare il file locale come fonte di verità.

**`gui/main_window.py`** — sostituiti i `features.get("...", True)`
permissivi con `features.get("...", False)`. Da ora un piano sconosciuto
o features mancanti DISABILITANO la feature invece di darla per scontata.

**`services/api_client.py`** — `get_stored_user_info()` ritorna SEMPRE
`is_admin=False` per la modalità offline. Le tab admin si vedono solo
dopo `me()` riuscito (server autoritativo). Niente più rischio di
modificare `session.json` con `is_admin=True` per vedere le tab.

**`services/external_apis.py`** — Spotify ora controlla anche None per
client_secret (prima solo "YOUR_SPOTIFY_CLIENT_ID"). Tutti i path
gestiscono graceful "no chiave → skip provider".

### Cosa NON è ancora cambiato (TODO Fase 2-4)

- [ ] Endpoint server proxy per Discogs/Last.fm/Spotify/GetSong
- [ ] Client che chiama il server invece delle API esterne dirette
- [ ] `@require_feature` decorator server-side su endpoint plan-gated
- [ ] Job quota tracker server-side (`max_runs_per_day`)
- [ ] Firma EXE digitale (Ed25519)
- [ ] Storage cifrato per `session.json` (Windows Credential Manager)
- [ ] SECURITY.md finale + README.md

### Test piano per v1086.7-wip

NON deployare in produzione. Test interno:
1. Login dovrebbe funzionare normalmente
2. Le tab Pro/Advanced si vedono ancora dopo login con utente del piano corretto (perché `features` viene dal server)
3. Le chiamate dirette a Discogs/Last.fm/Spotify/GetSong falliranno graceful con "no key, skip" nei log — i metadati useranno solo MusicBrainz/iTunes/Deezer
4. Se cancelli `session.json` o lo modifichi a mano, NON dovresti vedere tab admin

---

## v1086.7-wip — Round 2 (2026-05-12): tab Cache UX fix prima di security

⚠️ NB: questo round NON e' security audit. E' un fix UX rimandato dal
v1086.6 per il quale Pedro aveva testato e segnalato regressioni
visive prima di passare al server.

### 🎨 UI-29 · Tab Cache layout corretto (Pedro feedback)
**File:** `gui/main_window.py`

Pedro feedback sull'iterazione precedente del tab Cache:

1. "Voci 'Titolo' e 'Artisti Partecipanti' invece che stampare solo
    titolo e artista"
   → Aggiunte come PRIMI due elementi della grid 2 colonne, NON come
   header separati. Cosi' tutto e' allineato nella stessa colonna.

2. "Indentazione a sinistra completamente a sinistra"
   → CTkScrollableFrame con `padx=0`, label con `padx=(0, 4)` (sinistra
   0, gap 4px col valore). Prima c'era padx=(8, 6) — visualmente
   "sembrava al centro".

3. "Durata vuota nonostante iTunes la ritorni"
   → `_cache_reload` non propagava `duration` dal primary provider al
   cache_view. Aggiunto `"duration": primary_data.get("duration")`.

4. "Sample rate e Dimensione mancanti"
   → Il base path per risolvere il file fisico era `_db_base_path_var`,
   variabile inesistente nel codice (bug pregresso v1086.6). Sostituito
   con `self._selected_path` (StringVar del path entry GUI).
   Ora se il file e' presente sul disco, dimensione + sample rate
   vengono letti via `stat()` e `mutagen.MP3`.

5. "Sorgente: iTunes (+1) mancante"
   → `providers_count` non veniva propagato al cache_view per i record
   migrati. Aggiunto.

Layout finale (11 righe, allineate completamente a sinistra):
- Titolo:                Quero-te Dizer
- Artisti Partecipanti:  Alberto Indio
- Album:                 Acústico
- Anno:                  2013
- Genere:                World
- BPM:                   126.0
- Durata:                4:10
- Qualità:               256 kbps
- Sample rate:           44100 Hz
- Dimensione:            8.45 MB
- Sorgente:              iTunes

### 🔜 Prossimo: security audit FASE 2
File server arrivati da Pedro:
- `main.py` (FastAPI app, lifespan, CORS, seed admin)
- `auth.py` (API endpoints login/refresh/me/change-password/register/admin)
- `services/auth.py` (JWT bcrypt, get_current_user, require_admin)
- `models/db.py` (User, UpgradeRequest, Job, JobLog, AdminAuditLog)
- `requirements.txt` server
- `.env` + `.env.example`

Audit server-side parte nel prossimo turno.

---

## v1087.0 — dev/security-audit Round 3 (2026-05-12): tab Cache UX finale

⚠️ Bump major: passaggio v1086 → v1087 perche' branch nuovo
(dev/security-audit). Il fix UX tab cache resta marginale (Pedro ha
testato il refactor di backend security in v1086.7-wip e funziona;
questo round chiude la richiesta UX in sospeso prima di passare
all'audit server vero e proprio).

### 🎨 UI-30 · Tab Cache layout (Pedro feedback definitivo)
**File:** `gui/main_window.py`

Pedro screenshot test 1086.7-wip: "le label non sono tutte allineate
a sinistra e in più continuo a non vedere Sample rate, Dimensione,
Sorgente. Sospetto che dato lo spazio che hai introdotto tra le
voci in verticale si nascondono sotto la UI, anzi confermo,
espandendo in verticale i campi si vedono".

#### Cause
1. `pady=2` per ogni riga × 11 righe = 44px verticali di gap che
   sommavano oltre l'altezza visibile del frame dettaglio →
   gli ultimi 3 campi (Sample rate, Dimensione, Sorgente) finivano
   sotto la zona visibile senza scroll
2. `minsize=140` sulla colonna labels creava uno spazio fisso
   "Album:          [          ] R&B", facendo sembrare i valori
   centrati invece che subito dopo le label

#### Fix
- `pady=0` su tutte le righe — niente piu' gap inutile fra campi
- Spacer dedicato (8px) inserito SOLO dopo "Artisti Partecipanti"
  come richiesto da Pedro (separa header dai metadati dettaglio)
- `minsize=0` + `weight=0` sulla colonna 0 → la colonna si adatta
  al testo piu' lungo ("Artisti Partecipanti:") senza spazi inutilizzati
- "Titolo:" e "Artisti Partecipanti:" in GRASSETTO + colore
  `text` invece di `text_dim` per evidenziarli come header
- Le altre label restano in `text_dim` per gerarchia visiva

#### Campi aggiunti
- **Cartella** — directory del file dentro la music dir (es. "R&B/")
- **Catalogato il** — data ultima catalogazione formato "YYYY-MM-DD HH:MM"

Layout finale (13 righe):
```
Titolo:                Lonely               (bold)
Artisti Partecipanti:  Akon                 (bold)
                                             (8px gap)
Album:                 Trouble
Anno:                  2004
Genere:                R&B
BPM:                   89.0
Durata:                3:55
Qualità:               320 kbps
Sample rate:           44100 Hz
Dimensione:            8.45 MB
Cartella:              R&B
Catalogato il:         2026-05-12 09:50
Sorgente:              iTunes
```

### 🔢 Bump versione branch nuovo
Pedro: "mi spieghi poi perché hai fatto la versione 1086.7 se siamo
al branch nuovo? puoi progredire di versione".

Hai ragione, scelta sbagliata mia. Branch nuovo = bump minor.
- `dev/sources-priority` chiuso a v1086.1
- `dev/unify-local-db` chiuso a v1086.6
- `dev/security-audit` parte da **v1087.0** (era v1086.7-wip)

Da ora in poi:
- patch di fix dentro lo stesso branch: bump patch (v1087.0 → v1087.1)
- branch nuovo: bump minor (v1087.x → v1088.0)
- breaking changes: bump major (v1xxx → v2xxx)

### 🔜 Prossimo turno
Audit server completo sui file ricevuti (main.py, auth.py,
services/auth.py, models/db.py, requirements.txt, .env, .env.example).
Lista bug/lacune trovate + patch da applicare al server.

---

## v1087.1 — dev/security-audit Round 4 (2026-05-12): fix orfani + UI cache definitiva

⚠️ Sono ancora fix UX/correttezza che hanno la precedenza sull'audit
server vero. Il bug orfani impatta la qualità dei dati e va sistemato
prima di continuare.

### 🐛 BUG-31 · 13/16 file appena catalogati senza external_lookup
**Files:** `services/external_apis.py`, `core/cataloger.py`, `services/local_db.py`

Pedro test 17:19: 17 file catalogati, log dice "iTunes: Genere: X | BPM: Y"
per ognuno. Ma dump del local_db: solo 3 hanno `external_lookup`,
13 sono record file PURI senza cache e ci sono 71 ORFANI.

#### Causa root
Il cataloger fa `upsert_file()` usando `artist`/`title` dal **filename**
(o tag mp3), che e' la versione "grezza" usata dall'utente per
salvare i file. Esempio dal log:
```
*** Audiomachine - Kill 'Em All.mp3 ***
>-- iTunes: Genere: Hip Hop | BPM: 92
\-- Spostata in Hip Hop/
```
→ upsert_file: `artist="Audiomachine"`, `title="Kill 'Em All"`
→ lookup_by_query["audiomachine|||kill 'em all"] = "Hip Hop/Audiomachine - Kill 'Em All.mp3"

Poi a fine catalogazione `save_cache()` aggrega `external_apis.metadata_cache`:
```python
in_ram["itunes_Audiomachine_Kill 'Em All"] = {
    "artist": "Big Rob Savage",                   # nome canonico iTunes !
    "title":  "Kill Em' All (feat. Timothy)",     # nome canonico iTunes !
    ...
}
```
iTunes ha trovato una versione canonica DIVERSA del brano (succede
sempre: feat. mancanti, "ft." vs "feat.", apostrofi curly vs ASCII,
suffix tipo "(Radio Edit)", artisti aggiuntivi). `save_cache`
costruiva la qk dai campi canonici → `"big rob savage|||kill em' all (feat. timothy)"`
→ non match in `lookup_by_query` → ORFANO creato.

#### Fix
**Lato external_apis.py (search_all)**: il payload ritornato ora include
`_query_artist` e `_query_title` con i parametri **originali** della
ricerca (= quelli passati dal cataloger, = artist/title del filename,
= chiave in `lookup_by_query`).

**Lato cataloger.py (save_cache)**: usa `_query_artist`/`_query_title`
quando presenti per costruire qk. Fallback ai canonici se manca
(record migrati legacy).

**Lato local_db.py (migration legacy)**: la cache vecchia v1086.x aveva
chiavi `<prefix>_<query_artist>_<query_title>[_<album>]`. Il parser
estrae query_artist/title dalla chiave invece di usare i canonici
dal payload. Cosi' i record migrati dal vecchio `metadata_cache.json`
si collegano correttamente alla library.

#### Smoke test
Scenario reale: file "Audiomachine - Kill 'Em All.mp3", iTunes ritorna
"Big Rob Savage" come canonical artist.
- Prima (v1086.6 e v1087.0): orfano `__orphan__:big rob savage|||...`
- Dopo (v1087.1): external_lookup attaccato a "Hip Hop/Audiomachine - Kill 'Em All.mp3" (corretto)

#### Impatto su DB esistenti
Il `local_db.json` di Pedro attuale ha 71 orfani inutili. Saranno
ripuliti automaticamente alle prossime catalogazioni (i record reali
saranno popolati con external_lookup; gli orfani inutili non
verranno piu' creati). Per pulizia immediata si puo' usare il
bottone "Svuota Cache" — gli orfani si cancellano insieme alla
cache, e al prossimo run vengono ricreati correttamente collegati
ai file reali.

### 🎨 UI-32 · Tab Cache: niente scrollbar, valori espansi a tutta larghezza
**File:** `gui/main_window.py`

Pedro feedback: "il valore non occupa tutto lo spazio disponibile in
orizzontale risultando non realmente indentato a sinistra. Inoltre
non è necessaria la scroolbar dovrebbe riuscire ad entrare tutto
nella finestra."

#### Cause
- `CTkScrollableFrame` con 13 righe stretto mostrava scrollbar
- Valori con `wraplength=160` → larghezza fissa, testo breve
  ("Antonio José") sembrava centrato in spazio bianco

#### Fix
- `CTkScrollableFrame` → `CTkFrame` (niente scroll, niente scrollbar)
- `columnconfigure(1, weight=1)` + `sticky="ew"` sui valori → la
  colonna valori SI ESPANDE a riempire tutto lo spazio residuo
- `wraplength=0` sui valori → niente wrap

### 🚧 Prossimo turno
Quando il fix orfani e' verificato OK, audit server completo.

---

## v1087.2 — dev/security-audit Round 5 (2026-05-16): ROLLBACK layout tab cache

### ↩️ UI-33 · Rollback grid layout → testo semplice (Pedro)
**File:** `gui/main_window.py`

Pedro: "da quando ti ho chiesto di indentare le colonne nel tab cache
hai creato questo spazio verticale tra le voci che prima non c'era e
inoltre il campo non è comunque indentato a sinistra, quindi ti chiedo
torna indietro alle modifiche sulla parte delle cache e aggiungi solo
le voci in più che ti ho chiesto."

Lezione appresa: ho preso una richiesta semplice ("aggiungi metadati,
indenta il valore") e ho ricostruito tutto il layout con grid 2-colonne
attraverso 4 round (v1086.6 → v1086.7 → v1087.0 → v1087.1), ognuno
introducendo nuovi problemi (spazio verticale, scrollbar, valori che
sembravano centrati). Era over-engineering.

Fix v1087.2: ROLLBACK al singolo `CTkLabel` con `StringVar` di testo
multi-riga (la struttura ORIGINALE pre-v1086.7). Niente grid, niente
spazio verticale extra, niente scrollbar. Aggiunti SOLO i campi extra
richiesti:
- Titolo, Artisti Partecipanti (prime due righe)
- Album, Anno, Genere, BPM, Durata
- Qualità, Sample rate, Dimensione   ← extra concordati
- Cartella, Catalogato il            ← extra concordati
- Sorgente

L'indentazione perfetta del valore a destra NON e' risolvibile con un
singolo Label di testo. Pedro ha confermato: "Se non si trova una
soluzione sull'indentazione a destra, per il momento lasciamola così".
Accettato come limite noto.

### ✅ BUG-31 (orfani) — CONFERMATO RISOLTO
Pedro test 16/05: catalogazione 11 file. Dump local_db: i file
appena catalogati hanno `external_lookup` correttamente collegato
(32 record con external_lookup recenti; quelli senza sono di run
PRE-fix). I 138 orfani residui sono legacy — si puliscono con
"Svuota Cache" o restano innocui (non vengono piu' creati nuovi
orfani nei run post-fix).

Il fix `_query_artist`/`_query_title` in v1087.1 funziona.

### 🔜 Prossimo turno (finalmente): AUDIT SERVER
Tutti i blocchi UX/correttezza chiusi. Si parte con l'audit di
sicurezza server-side sui file ricevuti da Pedro.

---

## v1087.3 — dev/security-audit (Fase 2 Step 2/3): client usa proxy

### 🔐 Client collegato al proxy server-side
**Files:** `services/api_client.py`, `services/external_apis.py`,
          `core/cataloger.py`

Completa la Fase 2 lato client. I token Discogs/Last.fm/Spotify/
GetSong NON sono piu' nel client (rimossi v1087.0); ora il client
li usa attraverso il server.

- `api_client.lookup(provider, artist, title)`: nuovo metodo che
  chiama `GET /api/v1/lookup` sul server. Gestione errori robusta:
  server offline / sessione scaduta / qualsiasi eccezione → ritorna
  None senza propagare (la catalogazione non si ferma mai per il
  proxy).
- `external_apis._proxy_lookup()`: helper che inoltra al server.
- `search_lastfm` / `search_discogs` / `get_spotify_metadata`:
  proxy-first. Se il server risponde usa quello; altrimenti
  fallback al codice diretto (che con token client rimossi
  ritorna None → cascata passa al provider pubblico successivo:
  iTunes/MusicBrainz/Deezer).
- `ExternalAPIs.__init__`: nuovo param opzionale `api_client`
  (retrocompatibile, default None).
- `cataloger`: costruisce l'ApiClient da `app_config.server_url`
  + jwt_store (run_cataloger gira come subprocess separato, ma i
  token sono su disco dal login GUI). Se manca sessione → None →
  provider pubblici.

Server-side: v0.2.3 con `/api/v1/lookup` testato e funzionante
(Last.fm ha restituito dati reali per "Daft Punk - Get Lucky").

### Smoke test
- proxy hit → usa dati server ✓
- proxy miss → None graceful, cascata continua ✓
- no api_client → None graceful, nessun crash ✓

### 🔜 Prossimo: Step 3/3 — test end-to-end catalogazione reale

---

## v1088.0 — dev/security-audit CHIUSURA BRANCH (2026-05-18)

Bump minor: chiusura del branch security-audit, pronto per merge in
main + tag v1088.0-stable.

### 📄 Documentazione finale
**Files:** `README.md`, `SECURITY.md` (nuovi)

- **SECURITY.md**: documento completo del modello di sicurezza.
  Descrive cosa è protetto (auth bcrypt+JWT, rate limit, secrets
  server-side, plan enforcement, HTTPS) e — con piena trasparenza —
  cosa NON è protetto e perché (firma EXE e storage cifrato saltati
  come rischio consapevolmente accettato per il pilot privato).
  Include gestione incidenti e buone pratiche operative.
- **README.md**: doppio scopo GitHub + utenti. Spiega cosa fa il
  programma, perché, l'architettura client/server, la tabella piani,
  requisiti, privacy, stato pilot.

### Decisioni di scope (Pedro)
- **Fase 3 (firma EXE Ed25519)**: SALTATA. Rischio accettato per
  pilot fra amici (richiede compromissione preventiva del NAS).
  Candidata a branch dedicato prima di distribuzione pubblica.
- **Fase 4 (storage cifrato session.json)**: SALTATA. Classificata
  security theater in fase di audit (chi ha accesso al PC vede
  comunque tutto, come i cookie browser). Limite documentato in
  SECURITY.md §3.2.

### Riepilogo COMPLETO del branch dev/security-audit

| Item | Esito |
|------|-------|
| Audit client completo | ✅ SECURITY_AUDIT.md |
| Audit server completo | ✅ SECURITY_AUDIT_SERVER.md |
| Fase 1: secrets rimossi dal client | ✅ v1087.0 |
| Fase 1: plan defaults sicuri (no fallback permissivo) | ✅ v1087.0 |
| Fase 1: is_admin offline = False | ✅ v1087.0 |
| Server S1: rate limit login (slowapi) | ✅ v0.2.2 deploy |
| Server S2: .env production (no dev/debug) | ✅ verificato NAS |
| Server S3: require_plan dependency | ✅ v0.2.2 |
| Server S4: token_version (invalida JWT) | ✅ v0.2.2 deploy |
| Server S5: revoke-sessions endpoint | ✅ v0.2.2 |
| Server S6: CORS ristretto | ✅ v0.2.2 |
| Server S7: email normalizzata | ✅ v0.2.2 |
| Server S8: require_admin dependency | ✅ v0.2.2 |
| Fase 2: proxy lookup server-side | ✅ v0.2.3 + v1087.3 |
| Fase 2: test end-to-end (Last.fm via proxy) | ✅ confermato 18/05 |
| Fase 3: firma EXE | ⏭️ saltata (scelta) |
| Fase 4: storage cifrato | ⏭️ saltata (scelta) |
| README.md + SECURITY.md | ✅ v1088.0 |

Versioni client del branch: v1086.6 → v1087.0 → v1087.1 → v1087.2 →
v1087.3 → **v1088.0** (chiusura).
Versioni server del branch: v0.2.1 → v0.2.2 → **v0.2.3** (in prod).

### Bug risolti durante il branch (oltre alla security)
- BUG-26: cache key format mismatch (roundtrip cache rotto)
- BUG-31: orfani da nome canonico API ≠ nome filename
- UI tab cache: rollback a layout semplice + campi extra
- Macrogeneri: Indie/Blues/Ambient ora suggeribili come orfani
