# Music Cataloger Server

FastAPI backend per Music Cataloger Advanced.
Gestisce autenticazione JWT, piani utente, richieste upgrade, catalogazione remota.

---

## Architettura

```
Client GUI (Windows/macOS)
      │ HTTPS + JWT
      ▼
Reverse Proxy DSM  ──  https://choros27.synology.me/mcs/*
      │
      ▼
Container Docker su DS415+
      │
      ├── FastAPI (uvicorn)
      ├── SQLite (./data/app.db)
      └── Libreria musicale montata (/music)
```

---

## Test locale (prima di andare sul NAS)

Su qualsiasi macchina con Docker Desktop:

```bash
cd music-cataloger-server/
cp .env.example .env

# Genera una SECRET_KEY reale
python -c "import secrets; print(secrets.token_urlsafe(64))"
# copia l'output in .env → SECRET_KEY=...

docker compose up --build
```

Aperti:
- API: http://localhost:8020
- Docs interattive: http://localhost:8020/docs
- Health check: http://localhost:8020/health

Primo login (come admin):

```bash
curl -X POST http://localhost:8020/auth/login \
  -d "username=admin@choros27.synology.me" \
  -d "password=ChangeMeAt-FirstLogin-123"
```

Riceverai un `access_token` e `refresh_token`. Passa l'access in header:

```bash
curl http://localhost:8020/auth/me \
  -H "Authorization: Bearer <access_token>"
```

---

## Deploy sul NAS Synology DS415+

### 1) Preparazione filesystem NAS

Via SSH o File Station, crea le cartelle:

```bash
mkdir -p /volume1/docker/music-cataloger/data
mkdir -p /volume1/docker/music-cataloger/output
# Cartella musica esistente (es. /volume1/Multimedia/Musica) resta dov'è
```

### 2) Upload del progetto sul NAS

```bash
# Dal PC
scp -r music-cataloger-server/ pedro@choros27.synology.me:/volume1/docker/
```

Oppure tramite File Station: trascina la cartella in `/volume1/docker/`.

### 3) Configura `.env` per produzione

```bash
ssh pedro@choros27.synology.me
cd /volume1/docker/music-cataloger-server
cp .env.example .env
nano .env
```

Metti:

```
ENV=production
DEBUG=false
SECRET_KEY=<genera con python secrets.token_urlsafe(64)>
ADMIN_EMAIL=<tua email>
ADMIN_PASSWORD=<password robusta>
```

### 4) Modifica `docker-compose.yml` — volumi NAS

Edita la sezione `volumes:`:

```yaml
    volumes:
      - /volume1/docker/music-cataloger/data:/srv/app/data
      - /volume1/Multimedia/Musica:/music:rw
      - /volume1/docker/music-cataloger/output:/output
```

E rimuovi la sezione `ports:` (il reverse proxy DSM gestirà l'esposizione).

### 5) Deploy via Portainer (raccomandato)

Portainer già installato nel tuo NAS:

1. Login Portainer
2. **Stacks** → **Add stack**
3. Nome: `music-cataloger`
4. **Build method**: *Upload* → seleziona il `docker-compose.yml` modificato
5. **Environment variables**: carica il file `.env`
6. **Deploy the stack**

Attendi il build (~2 minuti prima volta).

### 6) Reverse Proxy DSM

**Pannello di controllo** → **Portale di accesso** → **Scheda Proxy inverso** → **Crea**

| Campo | Valore |
|---|---|
| Origine Protocollo | HTTPS |
| Origine Nome host | `api.choros27.synology.me` (o sottopath) |
| Origine Porta | 443 |
| Destinazione Protocollo | HTTP |
| Destinazione Nome host | `localhost` (o nome container) |
| Destinazione Porta | 8020 |

Tab **Intestazioni personalizzate**:

| Nome | Valore |
|---|---|
| Host | `$host` |
| X-Real-IP | `$remote_addr` |
| X-Forwarded-For | `$proxy_add_x_forwarded_for` |
| X-Forwarded-Proto | `$scheme` |

Tab **Avanzate** → spunta:
- [x] HSTS abilitato
- [x] HTTP/2 abilitato
- [x] WebSocket (non serve ora, comodo per futuro streaming log)

### 7) Test accesso esterno

```bash
curl https://api.choros27.synology.me/health
# {"status":"ok"}

curl https://api.choros27.synology.me/docs
# redirect alla UI di Swagger
```

### 8) Primo login e cambio password admin

```bash
curl -X POST https://api.choros27.synology.me/auth/login \
  -d "username=<tua email>" \
  -d "password=<password iniziale>"
```

Cambia subito la password (endpoint da aggiungere nel prossimo turno).

---

## Endpoint disponibili (MVP)

### Pubblici
- `GET /` — info server
- `GET /health` — liveness probe
- `GET /plans` — lista piani + feature

### Auth
- `POST /auth/login` — form-encoded `username`+`password`
- `POST /auth/refresh` — JSON `{refresh_token}`
- `GET /auth/me` — info utente corrente

### Utente autenticato
- `GET /plans/me` — piano corrente + feature + upgrade disponibili
- `POST /plans/upgrade-request` — `{to_plan, message?}`
- `GET /plans/my-requests` — elenco richieste upgrade proprie
- `POST /catalog/start` — avvia catalogazione (placeholder MVP)
- `GET /catalog/status/{id}` — stato job
- `GET /catalog/results/{id}` — risultato job
- `POST /catalog/cancel/{id}` — annulla job

### Admin
- `GET /admin/upgrade-requests` — elenco pending
- `POST /admin/upgrade-requests/{id}/approve` — approva + cambia piano utente
- `POST /admin/upgrade-requests/{id}/reject` — rifiuta

---

## TODO post-MVP

1. **Worker catalogazione**: asyncio background task che esegue il core cataloger (integrazione con `core/cataloger.py` del client come libreria condivisa)
2. **Endpoint /auth/change-password**: cambio password lato utente
3. **Endpoint /auth/register**: registrazione cliente (con conferma email)
4. **Email notifiche**: admin riceve email quando un utente richiede upgrade
5. **Streaming log**: WebSocket per far vedere al client il log della catalogazione in tempo reale
6. **Rate limiting**: slowapi + Redis per prevenire abusi
7. **HTTPS interno**: il container parla HTTP al reverse proxy DSM (ok), ma per Zero Trust certificato self-signed interno

---

## Troubleshooting

**Container non parte**: `docker compose logs server`
**401 sempre**: verifica `SECRET_KEY` identica fra restart (se cambi, tutti i JWT esistenti sono invalidati)
**CORS error dal client**: aggiungi il dominio del client in `CORS_ORIGINS` del `.env`
**DB corrotto (SQLite)**: stop container, rename `data/app.db` in `app.db.broken`, riparti (l'admin viene ri-seedato, gli utenti sono persi)
