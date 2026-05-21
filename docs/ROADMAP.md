# ROADMAP — Music Cataloger Advanced

Documento ufficiale e versionato della roadmap, mantenuto come
UPGRADES.md. Ogni voce ha: stato, priorità, note di scope.

Legenda stato: 🔲 da fare · 🟡 in corso · ✅ fatto · ⏸️ rimandato
Legenda priorità: P1 (prossimo) · P2 (medio termine) · P3 (lungo termine)

Ultima revisione: maggio 2026 — dopo chiusura branch `dev/security-audit`.

---

## Completato (storico sintetico)

- ✅ Catalogazione locale multi-segnale (filename→ID3→BPM→online)
- ✅ Classificazione Salsa per BPM / Bachata per stile
- ✅ DB locale unificato schema v2 (`local_db.json`) — branch unify-local-db
- ✅ Priorità sorgenti configurabile — branch sources-priority
- ✅ Server FastAPI su NAS (auth JWT, piani, job tracking)
- ✅ Security audit completo client+server — branch security-audit
  - secrets rimossi dal client, proxy lookup server-side
  - 8 fix server (rate limit, plan enforcement, token_version, ecc.)
- ✅ README.md + SECURITY.md

---

## P1 — Prossimo (pilot in corso)

### R1 · Permanenza impostazioni 🔲
**Richiesto più volte da Pedro.** Le impostazioni del menu di
sinistra e alcune voci del tab Avanzate non sopravvivono alla
chiusura del programma.
- Scope: persistere su `data/ui_prefs.json` lo stato di: opzioni
  catalogazione (menu sx), voci tab Avanzate selezionate, ultima
  directory, gestione duplicati, cover on/off.
- Note: già esiste un pattern simile per `caribbean_settings.json` e
  `genre_prefs.json` — riusare quell'approccio. Attenzione al timing
  di init dei widget (bug pregresso noto: Caribbean non caricava al
  boot per ordine di creazione widget).
- Branch suggerito: `dev/persistent-settings`

### R2 · Finestra Help con pulsanti contestuali 🔲
Rifare la finestra Help/About in stile MobaXterm: pulsanti
Changelog · Email · Updates · Documentazione.
- Changelog → mostra UPGRADES.md (o sezione recente) in-app
- Email → apre client mail verso l'indirizzo supporto
- Updates → forza check `/version/latest`
- Documentazione → link a README/manuale (locale o GitHub)
- Branch suggerito: stesso di R1 (`dev/persistent-settings`) o
  dedicato `dev/help-window` se si vuole tenerlo isolato

### R3 · Email transazionali 🔲
- Notifica all'admin (Pedro) quando si registra un nuovo utente
- Email di conferma all'utente alla registrazione
- Scope server: serve un meccanismo SMTP. Decisione aperta: usare
  un servizio (es. SMTP del dominio Synology, o un provider
  transazionale) — DA DEFINIRE prima di implementare.
- Branch suggerito: `dev/email-notifications` (server-side)

### R4 · Integrazione Spotify lato utente 🔲
Dare all'utente con account Spotify a pagamento la possibilità di
collegare il proprio account:
- Voce in Impostazioni: "Collega Spotify" → OAuth, il token resta
  **lato client** (scelta di privacy esplicita di Pedro: è il token
  dell'utente, non del sistema).
- Una volta collegato, "Spotify" diventa selezionabile tra le
  sorgenti DB online (oggi è server-proxy con token di sistema; qui
  invece è il token personale dell'utente).
- Nota architetturale: questo CONVIVE col proxy server. Se l'utente
  ha collegato il suo Spotify → usa quello client-side; altrimenti →
  niente Spotify (il proxy di sistema resta per Discogs/Last.fm).
- Branch suggerito: `dev/spotify-user-oauth`

---

## P2 — Medio termine

### R5 · Re-branding 🔲
Cambio nome prodotto (era postponed post-pilot). Nome candidato da
decidere (in passato citato "TrackLab" — DA CONFERMARE).
- Scope: nome app, icone, titoli finestre, README, version string,
  eventualmente dominio/endpoint. Operazione trasversale, va fatta
  quando il nome è deciso definitivamente per non rifarla due volte.
- Branch suggerito: `dev/rebrand`

### R6 · Internazionalizzazione IT/EN 🔲
Traduzione interfaccia in inglese (oggi è solo IT).
- Scope: estrarre le stringhe UI in un dizionario i18n, switch
  lingua in impostazioni. Lavoro meccanico ma esteso (centinaia di
  stringhe in main_window.py).
- Decisione aperta: serve davvero per il pilot tra amici
  (presumibilmente italiani)? Valutare se P2 o P3.
- Branch suggerito: `dev/i18n`

### R7 · Auto-update Linux/macOS 🔲
Oggi l'auto-update è solo Windows (batch swap EXE). Linux/macOS
devono aggiornare a mano.
- Scope: meccanismo di replace binario atomico + restart per
  Unix. Dipende anche da come si distribuisce (vedi R9).
- Branch suggerito: `dev/cross-update`

### R8 · Firma EXE Ed25519 🔲
Rimandata consapevolmente da security-audit (documentato in
SECURITY.md §3.1). Da fare PRIMA di qualunque distribuzione
pubblica oltre il pilot.
- Scope: chiave privata offline, pubblica hardcoded nel client,
  server serve EXE + firma, client verifica.
- Branch suggerito: `dev/exe-signing`

---

## P3 — Lungo termine

### R9 · Build & distribuzione macOS 🔲
Pedro non ha accesso a un Mac. Vedi sezione dedicata sotto
("Build macOS senza Mac") per le opzioni reali.
- Branch suggerito: parte di `dev/cross-update` o dedicato

### R10 · Community DB (pilot 2) 🔲
Database metadati centralizzato e condiviso tra utenti. Lo schema
`local_db.json` v2 è già stato progettato come base predisposta.
- Scope grande: cache server-side, dedup, contributi utente,
  moderazione qualità. Merita il suo ciclo di pianificazione.
- Branch suggerito: progetto a sé `dev/community-db`

### R11 · Spotify — ricatalogazione libreria utente 🔲
**Pedro stesso indica: in fondo alla roadmap, struttura complessa.**
Dato l'account Spotify collegato (R4), leggere brani salvati /
preferiti / playlist dell'utente e generare playlist coerenti per
genere.
- Limiti: dipende da cosa le API Spotify permettono (no download
  audio; si lavora su metadati e playlist). Scope da studiare
  quando si arriva qui.
- Dipende da: R4 completato.
- Branch suggerito: `dev/spotify-library-sync`

### R12 · Roadmap "lunga" originale 🔲
Elementi citati storicamente, mai iniziati, validità da
riconfermare: Web API layer estesa, Plugin System, WebApp
React/PWA, deploy Android. Da rivalutare se ancora pertinenti
dopo il pilot.

---

## Documentazione — riordino (vedi anche sezione apposita)

### D1 · Consolidamento documenti 🔲
Pedro ha 13+ file .md, alcuni datati/ridondanti. Piano di
consolidamento proposto in fondo a questo documento.

---

## Note su build macOS senza accesso a un Mac

Pedro chiede se esiste una VM per creare ambiente Apple. Risposta
onesta e completa:

- **VM macOS su hardware non-Apple**: tecnicamente esistono progetti
  (es. immagini "macOS in QEMU/KVM", o soluzioni tipo OSX-KVM), ma
  eseguire macOS su hardware non Apple **viola l'EULA Apple**.
  Funziona tecnicamente ma non è una via legale né affidabile per
  distribuire software.
- **Vie legittime**:
  1. **CI cloud con runner macOS**: GitHub Actions offre runner
     macOS. Si può configurare una pipeline che builda l'app
     `.app`/`.dmg` su un runner Mac Apple-hosted ad ogni tag. È la
     via raccomandata: nessun Mac fisico necessario, legale,
     ripetibile. Richiede solo configurare un workflow.
  2. **Servizi di build macOS a noleggio** (es. MacStadium,
     MacInCloud): Mac reali in cloud, a pagamento.
  3. **Un amico/tester con un Mac** che builda da sorgente
     (`BUILD_CROSS_PLATFORM.md` documenta già la procedura).
  4. **Distribuire il sorgente** invece del .app: per un pilot tra
     amici tecnici è la via più semplice (già documentato in
     BUILD_CROSS_PLATFORM.md Opzione A).

Raccomandazione: per il pilot, opzione 4 (sorgente) o 3 (tester con
Mac). Per distribuzione seria, opzione 1 (GitHub Actions macOS
runner) abbinata a R8 (firma) e R9. Da pianificare nel branch
`dev/cross-update`.

---

## Ordine di lavoro suggerito (proposta)

1. **R1 + R2** insieme (`dev/persistent-settings`) — richieste utente
   concrete, a basso rischio, alto valore percepito
2. **R4** (`dev/spotify-user-oauth`) — feature utente di valore
3. **R3** (`dev/email-notifications`) — quando deciso il meccanismo SMTP
4. **R5** (`dev/rebrand`) — quando il nome è deciso
5. Il resto secondo priorità e decisioni di prodotto

Questo ordine è una proposta: la priorità reale la decide Pedro.
