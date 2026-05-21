# CONTEXT.md — Contesto tecnico

> Sostituisce i vecchi CLAUDE_CONTEXT.md e STRUCTURE.md (datati a
> v0.0.2.x). Aggiornato a **v1088.0** (client) / **v0.2.3** (server).
> Documento vivo: aggiornare quando l'architettura cambia.

## Visione d'insieme

Sistema client-server:

```
┌──────────────────────────┐        HTTPS         ┌────────────────────┐
│ CLIENT desktop (v1088.0)  │ ──── JWT Bearer ──── │ SERVER FastAPI      │
│ Python 3.13 + CustomTk    │                      │ (v0.2.3, NAS Docker)│
│                           │                      │                    │
│ run_gui.py → GUI          │                      │ auth JWT + bcrypt   │
│   ↓ subprocess            │                      │ piani/quote          │
│ run_cataloger.py → core/  │                      │ proxy /api/v1/lookup │
│   usa services/, config/  │                      │ job tracking         │
└──────────────────────────┘                      └────────────────────┘
                                                            │ token nel .env
                                                            ▼
                                              Discogs/Last.fm/Spotify/GetSong
```

## CLIENT — struttura moduli

```
Music Cataloger/
├── run_gui.py              # launcher GUI (login → main window)
├── run_cataloger.py        # ENTRY POINT CLI catalogazione
│                           # (lanciato dalla GUI via subprocess)
├── version.py              # APP_VERSION (fonte di verità versione)
├── version_info.txt        # metadati versione per PyInstaller
├── build_ico.py            # generazione .ico multi-risoluzione
│
├── config/
│   ├── secrets.py          # API keys — SVUOTATO dei token (v1087.0):
│   │                       #   ora solo placeholder, token sul server
│   ├── settings.py         # generi (190+ map), range BPM, bachata
│   ├── user_plans.py       # PLAN_FEATURES (specchio del server);
│   │                       #   default sicuri: base, has_feature False
│   └── app_config.py       # config client (server_url, ecc.);
│                           #   singleton `config` esportato
│
├── core/
│   ├── cataloger.py        # logica catalogazione; costruisce
│   │                       #   ApiClient per il proxy (v1087.3)
│   ├── file_manager.py     # scan/move file, struttura cartelle
│   ├── genre_classifier.py # classificazione genere multi-segnale
│   └── metadata_extractor.py  # ID3 via eyed3/mutagen
│
├── gui/
│   ├── main_window.py      # GUI principale (grande file; PALETTE
│   │                       #   steel-blue dentro qui, non styles.py)
│   ├── login_window.py     # finestra login/registrazione
│   ├── dialogs.py          # dialoghi vari
│   ├── icons.py            # gestione set icone PNG
│   ├── app_icon.py         # icona app/taskbar
│   └── styles.py           # (non in uso attivo)
│
├── services/
│   ├── api_client.py       # client HTTP verso il server FastAPI;
│   │                       #   metodo lookup() = proxy (v1087.3)
│   ├── external_apis.py    # cascata metadati; proxy-first per
│   │                       #   Last.fm/Discogs/Spotify (v1087.3)
│   ├── bpm_services.py     # cascata BPM (GetSong + altri)
│   ├── cover_service.py    # recupero copertine
│   ├── local_db.py         # DB locale unificato (schema v2)
│   ├── jwt_store.py        # persistenza token sessione su disco
│   ├── catalog_reporter.py # report JSON catalogazione
│   ├── singleton.py        # helper singleton
│   └── updater.py          # auto-update EXE (solo Windows)
│
├── icons/app/              # PNG trasparenti (NON nei deliverable ZIP)
└── output/                 # log + report (creata a runtime)
```

NOTA: il vecchio monolite `MusicCatalogerAdvanced_v0020.py` non è
più rilevante — la GUI usa la struttura modulare sopra. Se presente
nel repo è legacy storico, non toccarlo né basarti su di esso.

## CLIENT — flusso di esecuzione

1. `run_gui.py` → login (verso server) → finestra principale
2. Utente sceglie cartella musica + opzioni, preme Avvia
3. `main_window.py` costruisce comando, lancia `run_cataloger.py`
   via `subprocess.Popen`
4. `run_cataloger.py` istanzia `core/cataloger.py`
5. cataloger:
   - costruisce un `ApiClient` (da `app_config.config.server_url`
     + token su disco da jwt_store) per il proxy lookup
   - per ogni MP3: extract ID3 → metadati esterni (cascata) →
     BPM → classificazione genere → update tag → move file
6. stdout letto dalla GUI in tempo reale, parsato per progress bar

### Cascata metadati (services/external_apis.py)
Ordine classificazione (NON modificare): filename → ID3 → BPM →
API online. Per i provider che richiedono token (Last.fm, Discogs,
Spotify) → `_proxy_lookup()` chiama il SERVER; fallback ai provider
pubblici (iTunes, MusicBrainz, Deezer) se il proxy non risponde.
La catalogazione non si ferma MAI per indisponibilità del proxy.

### Protocollo progress (GUI ↔ cataloger)
`run_cataloger` emette righe `PROGRESS: X/Y` che la GUI intercetta.
Tre fasi: scan_and_catalog → correct_existing_folders →
classify_salsa_by_bpm. Il pattern `*** filename.mp3 ***` delimita i
blocchi file — logica sensibile, già rotta da rewrite passati.

## CLIENT — dominio classificazione

### Salsa — per velocità BPM (NON difficoltà didattica)
Romantica <80 · Lenta 80-94 · Media 95-99 · Veloce 100-119 ·
Crazy 120+ · (Boogaloo, Cha-cha-cha separati)

### Bachata — per stile (NON livelli)
Dominicana · Fusion · Sensual
(Dominicana = combinazione scored di tag metadati + lista artisti
noti + keyword titolo/album + soglia BPM)

### Struttura cartelle target
```
Musica/
├── Latin/{Salsa/{1-Romantica..5-Crazy}, Bachata, Reggaeton, ...}
├── Pop/ Rock/ Electronic/ Soundtrack/ ...
```
Salsa usa prefissi numerati di difficoltà nelle cartelle; Bachata no.

## SERVER — sintesi (repo separato)

FastAPI su NAS Synology, Docker. Endpoint produzione porta 8020
dietro reverse proxy DSM (`https://api.choros27.synology.me`).

Struttura `app/`:
- `main.py` — entrypoint, registra router, CORS, rate limiter
- `config.py` — Settings da .env (SECRET_KEY, token API, ecc.)
- `models/db.py` — SQLAlchemy (User, Job, JobLog, AdminAuditLog);
  migrations idempotenti in `_run_migrations()`
- `api/auth.py` — login/register/refresh/me + admin endpoints
- `api/catalog.py` — start/progress/complete/fail job + quote piano
- `api/updates.py` — /version/latest + pubblicazione manifest
- `api/lookup.py` — proxy `/api/v1/lookup` (v0.2.3)
- `services/auth.py` — JWT, get_current_user, require_admin,
  require_plan
- `services/plans.py` — PLAN_FEATURES (specchio del client)
- `services/ratelimit.py` — limiter slowapi condiviso
- `services/music_lookup.py` — logica 4 provider (token server-side)

Sicurezza chiave (post audit, vedi SECURITY.md):
- bcrypt + JWT HS256, access 15min / refresh 7gg
- `token_version` invalida i JWT al cambio password
- rate limit login 5/min per IP (X-Forwarded-For aware)
- plan enforcement server-side (require_plan + check opzioni)
- `.env` production: ENV=production, DEBUG=false, DB path assoluto

## Stack tecnico

- **Client**: Python 3.13, CustomTkinter, eyed3, mutagen, librosa
  (opz. BPM), musicbrainzngs, requests; PyInstaller per build
- **Server**: FastAPI, SQLAlchemy, python-jose, passlib[bcrypt],
  slowapi, requests; Docker (python:3.11-slim)
- **Auth**: JWT Bearer; token client persistiti via jwt_store
- **DB**: SQLite (server, path assoluto); JSON (client local_db v2)

## Percorsi di riferimento

- Repo client: `C:\dev\music-cataloger` → GitHub PedroFerre27
- Server NAS: `/volume1/docker/music-cataloger-server/`
- Musica NAS: `/volume1/Multimedia/Musica`
- Admin: captainjoker27@gmail.com

## Riferimenti incrociati

- Cosa fare → ROADMAP.md
- Storia decisioni → UPGRADES.md
- Sicurezza → SECURITY.md
- Build/release → VERSIONING.md, BUILD.md
- Onboarding AI → CLAUDE.md
