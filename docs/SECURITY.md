# Security — TrackLab

Questo documento descrive il modello di sicurezza di TrackLab
Advanced: cosa è protetto, come, e — altrettanto importante — cosa
**non** è protetto e perché. La trasparenza sui limiti fa parte della
sicurezza: sapere cosa il sistema non garantisce evita falsi sensi di
protezione.

Stato: pilot privato (distribuzione tra utenti fidati).
Ultima revisione: maggio 2026 (branch `dev/security-audit`).

---

## 1. Architettura in breve

```
┌─────────────────┐         HTTPS          ┌──────────────────────┐
│  Client desktop │ ───────────────────────│  Server FastAPI       │
│  (Python/EXE)   │   JWT Bearer auth      │  (Synology NAS DS415+)│
│                 │                        │                      │
│  - UI           │                        │  - Auth (JWT+bcrypt) │
│  - Catalogazione│                        │  - Plan enforcement  │
│    locale MP3   │                        │  - Proxy API esterne │
│  - Cache locale │                        │  - Quote/rate limit  │
└─────────────────┘                        └──────────────────────┘
                                                      │
                                            token segreti nel .env
                                            (mai nel client)
                                                      ▼
                                          Discogs / Last.fm /
                                          Spotify / GetSong
```

**Principio guida**: la sicurezza vera è server-side. Il client è
software che gira sulla macchina dell'utente, quindi è ispezionabile
e modificabile da chi lo possiede. Per questo nessuna decisione di
sicurezza dipende dal client: il server valida tutto in modo
indipendente.

---

## 2. Cosa è protetto

### 2.1 Autenticazione

- Password utente con **bcrypt** (cost factor 12, default passlib).
  Le password non sono mai salvate in chiaro né loggate.
- **JWT firmati HS256** con `SECRET_KEY` robusta (lunghezza > 80
  caratteri) iniettata da variabile d'ambiente, mai committata.
  Il server fa hard-fail all'avvio in produzione se la SECRET_KEY è
  ancora un placeholder.
- Access token a vita breve (15 minuti), refresh token 7 giorni.
- Il server valida firma, scadenza, tipo del token e stato
  dell'utente (attivo/disattivato) a ogni richiesta autenticata.
- **Rate limiting login**: massimo 5 tentativi al minuto per IP
  (mitigazione brute-force). Funziona anche dietro il reverse proxy
  DSM leggendo `X-Forwarded-For`.
- **Invalidazione sessioni**: al cambio password tutte le sessioni
  esistenti (access + refresh) vengono invalidate via `token_version`.
  Un endpoint admin permette di revocare manualmente le sessioni di
  un utente compromesso.
- Email normalizzate server-side (lowercase) per evitare account
  duplicati e ambiguità di login.

### 2.2 Segreti delle API esterne

I token di Discogs, Last.fm, Spotify e GetSong **non sono nel
client**. Prima (≤ v1086) erano hardcoded in `config/secrets.py`:
chiunque decompilasse l'EXE poteva estrarli e abusarne a tuo nome.

Ora il client chiama l'endpoint server `GET /api/v1/lookup`, e il
**server** esegue la chiamata all'API esterna con i propri token
(presenti solo nel `.env` del NAS, gitignored). Vantaggi:
- I token non sono mai esposti sul dispositivo dell'utente.
- L'endpoint richiede JWT valido: niente uso anonimo dei token.
- Rate limit tecnico (120/min per IP) contro l'abuso.

Se il server è irraggiungibile, il client prosegue con i provider
pubblici che non richiedono token (iTunes, MusicBrainz, Deezer): la
catalogazione non si interrompe mai per indisponibilità del proxy.

### 2.3 Autorizzazione per piano (Base / Pro / Advanced)

Il gating delle funzionalità è **applicato dal server**, non solo
nascosto nella UI:
- Una dependency `require_plan(...)` permette di proteggere endpoint
  riservati a piani superiori.
- Le opzioni di catalogazione plan-gated (DB online, analisi BPM,
  recupero cover) sono validate server-side: un client modificato
  che le richiede senza il piano adeguato riceve `403 Forbidden`.
- Le quote (file per run, run giornaliere) sono contate server-side.

Il client mantiene un gating "soft" della UI (nascondere bottoni non
disponibili) solo per esperienza utente: non è una barriera di
sicurezza e non pretende di esserlo.

### 2.4 Trasporto e altre misure

- Tutta la comunicazione client↔server è su **HTTPS** (reverse proxy
  DSM con certificato), con validazione del certificato attiva.
- CORS ristretto ai soli domini Synology previsti, con metodi e
  header espliciti.
- Nessuna `shell=True`, nessun `eval`/`exec` su input; i sottoprocessi
  usano liste di argomenti (no command injection).
- ORM SQLAlchemy parametrizzato: nessuna SQL injection.
- Log delle azioni amministrative immutabile (accountability).
- In produzione il server gira con `DEBUG=false` e Swagger `/docs`
  disabilitato (non espone la superficie API pubblicamente).

---

## 3. Cosa NON è protetto (limiti noti e accettati)

Questi limiti sono **scelte consapevoli** per la fase di pilot
privato fra utenti fidati. Sono documentati qui per onestà: vanno
rivalutati prima di un'eventuale distribuzione pubblica.

### 3.1 Integrità dell'eseguibile (firma EXE) — NON implementata

L'auto-update scarica l'EXE dal server e ne verifica l'hash SHA256,
ma l'hash proviene dallo stesso server. Se il NAS venisse
compromesso, un attaccante potrebbe servire un EXE malevolo con hash
coerente.

**Mitigazione presente**: il canale è HTTPS, il NAS è una macchina
privata non esposta pubblicamente oltre l'API, l'accesso DSM è
protetto.

**Rischio residuo accettato**: per il pilot fra amici, lo scenario
richiede che un attaccante comprometta prima il NAS — vettore non
banale. Una firma digitale Ed25519 dell'EXE (chiave privata offline,
pubblica nel client) chiuderebbe del tutto questo rischio ed è il
candidato naturale per un branch dedicato prima della distribuzione
pubblica.

### 3.2 Cifratura dello storage locale — NON implementata

I token di sessione (`session.json`) e il database locale
(`local_db.json`) sono salvati in chiaro nella cartella dati
dell'utente.

**Razionale**: chi ha accesso fisico o amministrativo alla macchina
dell'utente può comunque leggere la memoria del processo, installare
keylogger, ecc. Cifrare i file con una chiave comunque presente sul
client (o derivata) darebbe un falso senso di sicurezza senza
cambiare il modello di minaccia — esattamente come i cookie di
sessione di qualunque browser, che sono in chiaro sul disco.

**Rischio residuo accettato**: un token di sessione rubato è
utilizzabile fino alla scadenza (access 15 min; refresh 7 giorni).
Mitigazioni disponibili: il cambio password invalida tutte le
sessioni; l'admin può revocare le sessioni di un utente.

### 3.3 Il client è ispezionabile

Per natura, un'app desktop distribuita può essere decompilata e
modificata da chi la possiede. Non si tenta offuscamento o anti-
tampering: sarebbe security theater. La protezione reale è che
**nessuna decisione di sicurezza dipende dal client** — il server
rivalida tutto in modo indipendente (vedi §2.3).

---

## 4. Gestione degli incidenti

### Token API esterno compromesso (Discogs, Last.fm, ecc.)
1. Rigenera il token sul portale del provider.
2. Aggiorna il valore nel `.env` del NAS.
3. Riavvia il container del server.
Il client non va toccato (non contiene token).

### Password o sessione utente compromessa
- L'utente cambia la password: tutte le sue sessioni vengono
  invalidate automaticamente.
- In alternativa, l'admin revoca le sessioni dell'utente tramite
  l'endpoint amministrativo dedicato.

### SECRET_KEY del server compromessa
Evento grave: tutti i JWT esistenti sono da considerare non fidati.
1. Genera una nuova SECRET_KEY
   (`python -c "import secrets; print(secrets.token_urlsafe(64))"`).
2. Aggiornala nel `.env` e riavvia il server.
3. Tutti gli utenti dovranno rifare login (comportamento atteso).

---

## 5. Buone pratiche operative

- Il file `.env` del server **non va mai committato** (è in
  `.gitignore`). Contiene SECRET_KEY, password admin iniziale e i
  token delle API esterne.
- Non incollare mai token o password in chat, issue, log pubblici o
  screenshot. Se accade, considerarli compromessi e rigenerarli.
- Mantenere DSM e i container aggiornati.
- In produzione verificare periodicamente che il server sia in
  `ENV=production` e `DEBUG=false`
  (`docker exec <container> env | grep -E "ENV=|DEBUG="`).

---

## 6. Come segnalare un problema di sicurezza

Trattandosi di un progetto personale in fase di pilot, segnalare
privatamente all'autore (non aprire issue pubbliche per
vulnerabilità). Indicare descrizione, passi di riproduzione e impatto
potenziale.
