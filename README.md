# TrackLab

Applicazione desktop per organizzare automaticamente librerie musicali
MP3 — pensata in particolare per la musica da ballo latino-americana
(Salsa e Bachata), ma utilizzabile per qualunque collezione.

Classifica i brani per genere, ne arricchisce i metadati, analizza i
BPM e riordina i file in una struttura di cartelle coerente, con il
minimo intervento manuale.

---

## A cosa serve

Chi ha migliaia di MP3 accumulati nel tempo conosce il problema: tag
incompleti o sbagliati, nomi file incoerenti, generi generici
("Latina" per qualunque cosa), nessuna idea del BPM utile per
costruire scalette da ballo. Sistemare tutto a mano è impraticabile.

TrackLab automatizza questo lavoro:

- **Classificazione di genere** con strategia multi-segnale: prima il
  nome del file, poi i tag ID3, poi l'analisi del BPM, infine i
  database musicali online. Questo ordine è importante perché i
  database online taggano spesso la musica latina in modo generico,
  quindi servono più segnali combinati.
- **Sottocategorie di ballo**: la Salsa è suddivisa per velocità BPM
  (Romantica, Lenta, Media, Veloce, Crazy); la Bachata per stile
  (Dominicana, Fusion, Sensual).
- **Arricchimento metadati** da iTunes, MusicBrainz, Deezer e — via
  server — Discogs, Last.fm, Spotify.
- **Analisi BPM** per stimare la velocità reale del brano.
- **Recupero copertine** mancanti.
- **Riorganizzazione file** in cartelle per genere, con possibilità
  di simulazione (dry-run) prima di spostare qualunque cosa.

Tutte le scelte (quali sorgenti usare, come gestire i duplicati,
ecc.) si impostano **prima** di avviare la catalogazione, non a
metà processo.

---

## Come funziona (in breve)

L'applicazione è composta da due parti:

- **Client desktop** (questo software): gira sul computer
  dell'utente, dove stanno i file MP3. Fa la catalogazione vera e
  propria in locale.
- **Server**: gestisce gli account, i piani di utilizzo, e fa da
  intermediario sicuro verso i database musicali che richiedono
  chiavi private. Il client non contiene mai chiavi segrete.

L'utente accede con un account, sceglie la cartella musicale e le
opzioni, e avvia. Il client lavora in locale; il server tiene traccia
del progresso e applica i limiti del piano.

Per i dettagli sul modello di sicurezza, cosa è protetto e cosa no,
vedi [SECURITY.md](SECURITY.md).

---

## Piani di utilizzo

| Funzionalità                    | Base | Pro | Advanced |
|---------------------------------|:----:|:---:|:--------:|
| Catalogazione locale            |  ✅  | ✅  |    ✅    |
| Pulizia cartelle vuote          |  ✅  | ✅  |    ✅    |
| Modalità simulazione (dry-run)  |  ✅  | ✅  |    ✅    |
| Database online + cover + BPM   |  —   | ✅  |    ✅    |
| Export CSV / M3U                |  —   | ✅  |    ✅    |
| Strumenti di manutenzione       |  —   | parz.|   ✅    |
| Tab Avanzate / Caraibica        |  —   | —   |    ✅    |

I limiti d'uso (numero di file per sessione, sessioni giornaliere)
dipendono dal piano e sono applicati dal server.

---

## Requisiti

- Windows (build EXE distribuita) — il sorgente gira anche su
  Linux/macOS con Python 3.13.
- Connessione a internet per i metadati online e l'autenticazione.
- Un account sul server (creazione gestita dall'amministratore o via
  registrazione self-service, se abilitata).

Per chi esegue dai sorgenti:
```
pip install -r requirements.txt
python run_gui.py
```

---

## Privacy

- I file musicali **non lasciano mai il computer dell'utente**: la
  catalogazione è locale. Al server vengono inviati solo i metadati
  necessari al funzionamento (progresso del job, query di lookup come
  artista/titolo).
- I dettagli su autenticazione, protezione dei dati e limiti noti
  sono in [SECURITY.md](SECURITY.md).

---

## Stato del progetto

Progetto personale in fase di **pilot privato** (uso fra utenti
fidati). Funzionalità e API possono cambiare. Non è destinato, allo
stato attuale, a distribuzione pubblica di massa.

## Segnalazioni

Per problemi di sicurezza: contattare privatamente l'autore (vedi
[SECURITY.md](SECURITY.md) §6). Per bug o suggerimenti: canali
indicati dall'autore.

## Licenza

Progetto personale. Tutti i diritti riservati salvo diversa
indicazione dell'autore.
