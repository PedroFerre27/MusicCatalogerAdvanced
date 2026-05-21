# CLAUDE.md — Onboarding per Claude Code

> Questo file è il punto di ingresso per Claude Code. Leggilo per
> primo a ogni sessione, poi consulta i documenti linkati secondo il
> task. È mantenuto aggiornato come gli altri documenti vivi.

## Cos'è questo progetto

**Music Cataloger Advanced**: applicazione desktop (Python 3.13 +
CustomTkinter) per catalogare automaticamente librerie MP3 di musica
da ballo latino (Salsa/Bachata) — classificazione genere, metadati,
BPM, riorganizzazione file. Ha un backend FastAPI su NAS Synology per
auth, piani utente, e proxy sicuro verso API musicali.

Autore: Pedro (captainjoker27). Fase: **pilot privato** tra utenti
fidati. Non distribuzione pubblica di massa (ancora).

## Architettura — DUE codebase distinte

⚠️ Importante: il progetto è diviso in due parti separate.

1. **Client desktop** — questo repo (`C:\dev\music-cataloger`)
   - GitHub: github.com/PedroFerre27/MusicCatalogerAdvanced
   - Versione attuale: **v1088.0** (vedi `version.py`)
   - Gira sulla macchina dell'utente, fa la catalogazione locale

2. **Server FastAPI** — repo/cartella separata, deployata su NAS
   - NAS Synology DS415+, container Docker
   - Endpoint produzione: `https://api.choros27.synology.me`
   - Versione attuale: **v0.2.3** (in produzione)
   - Path sul NAS: `/volume1/docker/music-cataloger-server/`
   - Auth JWT, gestione piani, proxy lookup API esterne

Quando lavori, sii sempre consapevole di QUALE codebase stai
toccando. Una modifica al plan enforcement tocca ENTRAMBI (il client
ha il gating soft UI, il server quello reale).

## Da dove iniziare a leggere (in ordine)

1. **Questo file** (CLAUDE.md) — onboarding
2. **CONTEXT.md** — architettura tecnica dettagliata, flusso,
   struttura moduli, convenzioni
3. **ROADMAP.md** — cosa c'è da fare, prioritizzato
4. **UPGRADES.md** — changelog cumulativo (7900+ righe; leggi le
   ultime ~200 per lo stato recente, cerca per keyword il resto)
5. **SECURITY.md** — modello sicurezza, cosa è protetto e cosa no
6. **VERSIONING.md** — workflow git/release, come si pubblica un EXE

## Convenzioni del progetto (rispettale sempre)

### Versioning
- Patch nel branch: bump patch (v1088.0 → v1088.1)
- Nuovo branch feature: bump minor (v1088.x → v1089.0)
- Breaking: bump major
- Aggiorna SEMPRE `version.py` e `version_info.txt` insieme
- Ogni release stabile = tag git `vXXXX-stable`

### Codice
- Preservare codice/commenti esistenti salvo richiesta esplicita di
  rimozione (Pedro è attento alle regressioni)
- Niente over-engineering: Pedro segnala quando si ricostruisce
  qualcosa che già funziona
- BTN_H=36 per i bottoni, estetica dark minimale
- Log: `>--` step intermedi, `\--` risultati (ASCII-safe);
  metadati verbosi a DEBUG, INFO pulito una riga per file

### Sicurezza (non negoziabile)
- `secrets.py` lato client: MAI con token reali (rimossi in v1087.0)
- `.env` del server: MAI committato, MAI in uno ZIP
- Token API stanno SOLO nel `.env` del NAS, mai nel client
- Audit pre-commit server: niente `ENV=development`/`DEBUG=true`/
  path relativi DB in produzione
- Nessuna decisione di sicurezza dipende dal client (il server
  rivalida tutto)

### Classificazione musicale (dominio)
- Ordine: filename → ID3 → BPM → API online (NON cambiare l'ordine)
- Salsa per velocità BPM: Romantica <80, Lenta 80-94, Media 95-99,
  Veloce 100-119, Crazy 120+
- Bachata per stile: Dominicana, Fusion, Sensual (NON livelli di
  difficoltà)
- Musica latina raramente taggata bene online → multi-segnale
  essenziale

## Stato attuale (maggio 2026)

- Branch `dev/security-audit` CHIUSO e mergeato: audit completo,
  secrets rimossi dal client, proxy lookup server-side funzionante
  end-to-end, 8 fix server deployati
- Server v0.2.3 in produzione sul NAS, verificato
- Client v1088.0, documentazione (README/SECURITY/ROADMAP) scritta
- Prossimo: vedi ROADMAP.md P1 (R1 persistenza impostazioni + R2
  finestra Help sono i candidati immediati)

## Workflow git essenziale

```
# Feature nuova
git checkout -b dev/<nome-feature>
# ... lavoro, commit frequenti ...
git checkout main && git merge dev/<nome-feature>
git tag vXXXX-stable && git push origin main --tags
```

Dettagli completi (build EXE, pubblicazione manifest, rollback) in
VERSIONING.md.

## Cose che Pedro NON vuole

- ZIP con tutti i file quando ha chiesto solo i modificati
- Rifare/riscrivere codice che già funziona
- Token o password nei deliverable
- Cambiare l'ordine della cascata di classificazione
- Assumere che una cosa serva senza verificare nel codice reale

## Quando sei incerto

Chiedi prima di fare modifiche estese. Pedro preferisce un task
piccolo e verificato che un grande blocco rischioso. Procedi a step
verificabili (è il pattern che ha funzionato finora).
