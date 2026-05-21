# Manuale Utente — Music Cataloger Advanced

Benvenuto. Questo manuale è pensato per chi prende in mano il
programma per la prima volta e non sa cosa fa. Le sezioni vanno
nell'ordine in cui le incontri all'uso.

**Indice**

- 1. Cos'è e cosa fa
- 2. Primo avvio — login e account
- 3. La schermata principale a colpo d'occhio
- 4. Catalogazione: il flusso passo per passo
- 5. I 7 tab e a cosa servono
- 6. Strumenti di manutenzione
- 7. Logica di classificazione (cosa decide il programma)
- 8. Piani utente — cosa cambia
- 9. Problemi comuni
- 10. Glossario rapido

---

## 1. Cos'è e cosa fa

Music Cataloger Advanced organizza automaticamente librerie di file
MP3. Lo fa in quattro modi che si combinano:

- **Classifica i brani per genere** (es. Salsa, Bachata, Rock, Pop…)
- **Arricchisce i metadati ID3** (titolo, artista, album, anno,
  genere, cover) recuperandoli da database musicali online
- **Stima il BPM** dei brani che non ce l'hanno
- **Sposta i file** in una struttura di cartelle coerente per
  genere e sottogenere

È particolarmente adatto a chi ha collezioni di musica da ballo
latino (Salsa e Bachata) — la classificazione delle sottocategorie
di ballo è uno dei punti di forza — ma funziona per qualunque
collezione MP3.

L'app è composta da due parti: un programma che gira sul tuo
computer (dove stanno i file MP3) e un server che gestisce gli
account utente e l'accesso sicuro ad alcuni database musicali. Per
te utente significa solo che devi fare login con un account.

---

## 2. Primo avvio — login e account

All'apertura compare una finestra di login.

- Se hai già le credenziali: **email + password** → Accedi.
- Se non ce l'hai: **Registrati** (se l'amministratore ha abilitato
  la registrazione self-service). Altrimenti contatta
  l'amministratore per ottenere un account.

Dopo il login l'app ricorda la sessione: i prossimi avvii non
chiederanno di nuovo email/password fino alla scadenza o finché non
fai logout esplicito.

**Cambio password**: nelle impostazioni del profilo (badge in alto
con il tuo nome). Quando cambi la password, tutte le altre sessioni
attive del tuo account vengono disconnesse automaticamente — è una
misura di sicurezza.

---

## 3. La schermata principale a colpo d'occhio

```
┌────────────────────────────────────────────────────────────┐
│  [logo]  Music Cataloger Advanced            [badge profilo]│
├──────────────┬──────────────────────────────────────────────┤
│              │                                              │
│  MENU SX     │              AREA TAB (7 tab)                │
│              │                                              │
│  • Opzioni   │   Log · DB Locale · Generi · Cache ·         │
│    catalog.  │   Qualità · Caraibica · Avanzate             │
│              │                                              │
│  • Sorgenti  │                                              │
│              │                                              │
│  • Cartella  │                                              │
│              │                                              │
│              │                                              │
│  [▶ Avvia]   │                                              │
│  [■ Ferma]   │                                              │
│  [🗑 Pulisci]│                                              │
└──────────────┴──────────────────────────────────────────────┘
```

- **Menu di sinistra**: dove imposti tutte le scelte PRIMA di
  lanciare la catalogazione (cartella musicale, opzioni, sorgenti).
  Filosofia del programma: tutte le scelte si fanno prima, non a
  metà processo.
- **Area tab (centro/destra)**: 7 tab che mostrano viste diverse —
  log in tempo reale, database locale, statistiche, ecc.
- **Pulsanti Avvia / Ferma / Pulisci Log** in basso a sinistra.
- **Badge profilo** in alto a destra: nome, piano attivo, accesso a
  profilo/logout.

---

## 4. Catalogazione: il flusso passo per passo

### Passo 1 — Scegli la cartella musicale
Nel menu di sinistra c'è il pulsante per selezionare la cartella che
contiene i tuoi MP3. Può essere disordinata: l'app si occupa di
metterla in ordine.

### Passo 2 — Imposta le opzioni
Nel menu di sinistra trovi le opzioni principali:

- **Modalità simulazione (dry-run)**: ATTIVA al primo uso. Il
  programma fa finta di catalogare ma non sposta niente — ti
  permette di vedere cosa farebbe senza rischi. **Usala sempre la
  prima volta su una cartella nuova.**
- **Pulisci cartelle vuote**: dopo aver spostato i file, rimuove le
  cartelle rimaste vuote.
- **Usa database online**: attiva il recupero metadati da Internet
  (più lento ma molto più accurato). Richiede piano Pro/Advanced.
- **Recupera copertine**: scarica le cover mancanti. Richiede piano
  Pro/Advanced.
- **Analizza BPM**: stima i BPM dei brani che non ce l'hanno.
  Richiede piano Pro/Advanced.
- **Correggi cartelle**: riclassifica anche i brani che sono già in
  cartelle, non solo quelli sciolti.
- **Classifica Salsa per BPM**: applica la sottoclassificazione
  Romantica/Lenta/Media/Veloce/Crazy.
- **Gestione duplicati**: cosa fare se trova due file con lo stesso
  titolo (tienili entrambi, sovrascrivi, salta).

### Passo 3 — Scegli le sorgenti (priorità DB online)
Se hai attivato il database online, puoi decidere QUALI sorgenti
usare e in che ordine. Le sorgenti disponibili sono iTunes,
MusicBrainz, Deezer, Last.fm, Discogs, Spotify. Le prime tre sono
pubbliche; le ultime tre passano dal server (sicurezza).

Suggerimento: lascia l'ordine di default a meno che tu non abbia un
motivo specifico. Funziona bene così.

### Passo 4 — Avvia
Premi **▶ Avvia**. La barra di progresso parte. Nel tab **Log**
vedi tutto quello che il programma sta facendo, file per file. Per
ogni brano compare un blocco tipo:

```
*** nome_file.mp3 ***
 >-- Last.fm (proxy server): metadati trovati
 >-- BPM stimato: 96
 \-- nome_file.mp3 → Latin/Salsa/3 - Media/
```

### Passo 5 — Controlla i risultati
Quando finisce, controlla il **DB Locale** (tab) per vedere tutti i
file catalogati. Se eri in dry-run, ora puoi disattivarlo e
rilanciare per fare lo spostamento reale.

### Fermare a metà
Il pulsante **■ Ferma** interrompe in modo pulito alla fine del
file corrente. I file già processati restano dove li ha messi (se
non eri in dry-run).

---

## 5. I 7 tab e a cosa servono

### Log
Output in tempo reale di tutto quello che il programma fa. Ogni
file ha un suo blocco con i passi intermedi (`>--`) e il risultato
finale (`\--`). Lo trovi anche salvato su disco in `output/`.

### DB Locale
La libreria catalogata dal programma. Ogni riga è un file MP3 con i
suoi metadati arricchiti (titolo, artista, album, anno, genere,
BPM, qualità). Cliccando su un brano vedi tutti i dettagli — incluse
le sorgenti da cui sono arrivati i metadati. Da qui puoi anche
esportare in CSV (piani Pro/Advanced).

### Generi
Statistiche e gestione generi. Vedi quanti brani hai per ogni
genere, quali generi sono attivi (cioè usati per la
classificazione) e quali disattivati. Puoi attivare/disattivare
generi per non vederli più nelle statistiche o nella destinazione
dei file.

### Cache
Visualizza la cache dei metadati online (`metadata_cache.json`). La
cache evita di richiamare le API per lo stesso brano due volte. Da
qui puoi svuotarla se per qualche motivo contiene dati sbagliati
(piani Pro/Advanced).

### Qualità
Analisi della qualità tecnica della libreria: bitrate (kbps),
sample rate, dimensione media, quanti file sono di qualità bassa
(<128 kbps). Utile per capire se hai file da rimasterizzare o da
ricercare in qualità migliore.

### Caraibica
Tab dedicato alla classificazione fine della musica caraibica
(Salsa per velocità BPM, Bachata per stile). Qui puoi vedere e
modificare le soglie BPM dei livelli Salsa, la lista degli artisti
noti per il riconoscimento Bachata Dominicana, e altre regole
specifiche. Piano Pro/Advanced.

### Avanzate
Opzioni avanzate per utenti esperti: percorsi personalizzati,
parametri di analisi, scelte di sviluppo. Piano Advanced.

---

## 6. Strumenti di manutenzione

Oltre alla catalogazione, l'app include strumenti per tenere in
ordine la libreria.

### 📋 Esporta CSV
Esporta tutto il DB Locale in un file CSV (separatore `;`),
apribile con Excel o LibreOffice. Colonne: file, titolo, artista,
album, anno, genere, sottogenere, BPM, qualità (kbps), data
catalogazione. Utile per analisi esterne o backup.

### 🔍 Trova Duplicati
Scansiona il DB e raggruppa i file con lo **stesso nome** in
cartelle diverse — risultato tipico di catalogazioni multiple o
spostamenti manuali. Una finestra mostra i gruppi; per ogni gruppo
scegli quale copia mantenere e le altre vengono eliminate.

⚠️ L'eliminazione è permanente. Controlla sempre quale copia
tieni prima di confermare.

### 🗑 Svuota Cache
Cancella il contenuto di `metadata_cache.json`. La prossima
catalogazione richiamerà le API da zero (più lenta). Non tocca i
file MP3 né il DB Locale. Usalo se la cache contiene dati sbagliati.

### 📂 Apri Cartella Dati
Apre in Esplora File la cartella `data/` del programma. Contiene:
- `music_library.json` (DB Locale)
- `metadata_cache.json` (cache)
- `genre_prefs.json` (generi attivi)
- `caribbean_settings.json` (regole Salsa/Bachata)
- altre preferenze

Utile per backup manuale o ispezione dei dati.

### 🎵 Playlist M3U per Genere
Scansiona la cartella musicale e crea un file `.m3u` per ogni
genere trovato. I file M3U sono compatibili con VLC, foobar2000,
Winamp e tutti i DJ software moderni.

### ✂️ Rinomina Batch con Pattern
Rinomina i file MP3 secondo un pattern personalizzato. Variabili
disponibili: `{title}`, `{artist}`, `{album}`, `{year}`, `{bpm}`.

Esempi:
- `{artist} - {title}` → `Hector Lavoe - El Cantante.mp3`
- `{year} - {artist} - {title}` → `1975 - Hector Lavoe - El Cantante.mp3`

⚠️ Operazione irreversibile. Fare backup prima.

### 🔊 Normalizza Volume (ReplayGain)
Analizza i brani e scrive nei tag ID3 il valore di guadagno per
equalizzare il volume percepito a -89 dBFS (standard ReplayGain
2.0). I lettori compatibili (VLC, foobar2000) usano questo valore
per evitare salti di volume tra brani. **Non modifica i dati
audio**, è completamente reversibile.

Requisiti: `mp3gain` installato (Windows:
https://mp3gain.sourceforge.net/ o `winget install mp3gain`).

### 🛡 Verifica Integrità File MP3
Legge l'header audio di ogni file MP3 e segnala quelli con header
corrotti, illeggibili, o non MP3 nonostante l'estensione. Utile
dopo operazioni di copia di massa o per file scaricati da fonti
incerte.

---

## 7. Logica di classificazione (cosa decide il programma)

Capire come il programma decide il genere di un brano ti aiuta a
prevedere e correggere i risultati.

### L'ordine di analisi (importantissimo)
Per ogni brano il programma controlla nell'ordine:

1. **Nome del file** (`Hector Lavoe - El Cantante.mp3` → "Salsa"
   probabile)
2. **Tag ID3** già nel file
3. **BPM** (se disponibile, aiuta a distinguere stili)
4. **Database online** (iTunes, MusicBrainz, Last.fm, ecc.)

Quest'ordine non è casuale: i database online taggano spesso la
musica latina in modo generico ("Latin Music", "Latina"). Il nome
del file e gli artisti noti danno informazioni più precise.

### Salsa — divisa per velocità BPM
La Salsa viene smistata in 5 livelli, **non per difficoltà didattica
ma per velocità**:

| Livello       | BPM       |
|---------------|-----------|
| 1 - Romantica | < 80      |
| 2 - Lenta     | 80 – 94   |
| 3 - Media     | 95 – 99   |
| 4 - Veloce    | 100 – 119 |
| 5 - Crazy     | ≥ 120     |

Boogaloo e Cha-cha-cha hanno cartelle separate.

### Bachata — divisa per stile
La Bachata viene smistata in 3 categorie **per stile, non per
livello**:

- **Dominicana** — riconosciuta tramite combinazione di tag
  metadati, lista di artisti noti, parole chiave nel titolo/album,
  e soglia BPM
- **Fusion**
- **Sensual**

### Struttura cartelle finale
```
Musica/
├── Latin/
│   ├── Salsa/
│   │   ├── 1 - Romantica/
│   │   ├── 2 - Lenta/
│   │   ├── 3 - Media/
│   │   ├── 4 - Veloce/
│   │   └── 5 - Crazy/
│   ├── Bachata/
│   ├── Reggaeton/
│   └── …
├── Pop/
├── Rock/
├── Electronic/
└── …
```

---

## 8. Piani utente — cosa cambia

| Funzionalità                   | Base | Pro | Advanced |
|--------------------------------|:----:|:---:|:--------:|
| Catalogazione locale           | ✅  | ✅  |   ✅    |
| Pulizia cartelle vuote         | ✅  | ✅  |   ✅    |
| Modalità simulazione (dry-run) | ✅  | ✅  |   ✅    |
| Database online + cover + BPM  | —   | ✅  |   ✅    |
| Export CSV / M3U               | —   | ✅  |   ✅    |
| Strumenti manutenzione         | —   | parz. |  ✅   |
| Tab Avanzate / Caraibica       | —   | —   |   ✅    |

Esistono anche **limiti d'uso** per piano (file per sessione,
sessioni giornaliere) applicati dal server. Quando arrivi al limite
ricevi un messaggio chiaro.

---

## 9. Problemi comuni

**"Il programma non parte / errore al login"**
Verifica la connessione internet — il login passa dal server. Se il
server è temporaneamente offline puoi continuare ad usare le
funzioni offline (catalogazione locale senza database online).

**"La catalogazione si è bloccata a metà"**
Controlla il tab Log: di solito c'è un messaggio. Cause comuni:
file MP3 corrotto, cartella di destinazione protetta. Premi Ferma,
correggi, e rilancia: il programma riprende dai file non ancora
fatti.

**"I generi non vengono riconosciuti correttamente"**
- Verifica di aver attivato il **database online** nelle opzioni
- Apri il tab Generi e controlla se il genere che ti aspetti è
  abilitato
- Per la musica latina: il programma usa molti segnali combinati
  perché i tag online sono spesso generici. Risultato migliore se
  i nomi file sono nella forma `Artista - Titolo.mp3`

**"Spotify / Discogs / Last.fm non danno risultati"**
Quei provider passano dal server. Se sei offline o il server è
irraggiungibile, vengono saltati e il programma usa solo i provider
pubblici (iTunes, MusicBrainz, Deezer). La catalogazione continua
comunque.

**"Voglio annullare uno spostamento"**
Sfortunatamente lo spostamento non è annullabile dall'app stessa.
Per questo c'è la **modalità simulazione**: usala sempre la prima
volta su una cartella nuova per vedere cosa farebbe il programma
PRIMA di farglielo fare davvero.

**Aggiornamenti**
Su Windows l'aggiornamento è automatico al lancio. Se segnala una
nuova versione, lascia che si aggiorni e riapri.

---

## 10. Glossario rapido

- **BPM** (Beats Per Minute): velocità del brano in battiti al
  minuto. Determina la classificazione Salsa per velocità.
- **Tag ID3**: i metadati incorporati nei file MP3 (titolo,
  artista, album, anno, genere, ecc.).
- **Dry-run / Simulazione**: il programma fa finta di catalogare
  senza spostare nulla, mostrando solo cosa farebbe.
- **Cache metadati**: salvataggio locale delle risposte dei
  database online per non richiamarli due volte.
- **DB Locale**: il database personale del programma con tutti i
  brani che hai catalogato.
- **Sottogenere**: una sottocategoria dentro un genere (es. Salsa
  Romantica è un sottogenere di Salsa).
- **Cover**: la copertina dell'album incorporata nel file MP3.

---

*Per problemi tecnici o segnalazioni di bug, contatta
l'amministratore. Per domande sul modello di sicurezza vedi
SECURITY.md.*
