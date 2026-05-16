# SECURITY AUDIT — Music Cataloger Advanced
**Branch:** `dev/security-audit`
**Stato all'avvio:** v1086.6 (post unify-local-db)
**Data:** 11 maggio 2026
**Autore audit:** Claude
**Scope:** client desktop Python + interfaccia verso server FastAPI NAS

---

## TL;DR — Sommario per Pedro

Il client ha **5 categorie di problemi**:

| # | Problema | Severità | Note |
|---|----------|----------|------|
| 1 | Secrets API hardcoded nel client (Discogs, Last.fm, GetSong, Spotify, AudD) | 🔴 ALTA | Estraibili da chiunque legga l'EXE |
| 2 | Plan/feature gating completamente client-side (`PLAN_FEATURES` hardcoded) | 🔴 ALTA | Bypass banale modificando `user_plans.py` |
| 3 | `is_admin` letto dal JWT non firmato lato client | 🟡 MEDIA | Il JWT è firmato server-side, ma il client decodifica senza verifica |
| 4 | EXE update senza verifica firma digitale | 🟡 MEDIA | Se il server è compromesso, malware injectabile |
| 5 | Storage locale in chiaro (`session.json`, `local_db.json`) | 🟢 BASSA | Accettabile per pilot, ma da documentare |

Tutto il resto del codice è **ragionevolmente pulito**: subprocess senza shell=True, niente eval/exec, niente SQL injection (no DB), HTTPS di default verso il NAS.

**Il punto chiave** è che la security del programma dipende quasi interamente dal **server FastAPI**. Se il server fa correttamente plan check, hashing password e rate limiting, il client può anche essere reversed senza che l'attaccante guadagni vantaggi.

---

## 1. Secrets nel client

**File:** `config/secrets.py`

### Chiavi esposte
```python
SPOTIFY_CLIENT_ID       = '682cb59a3f5743cbad34c1ac22b4229d'
SPOTIFY_CLIENT_SECRET   = 'b9f26e830df94d138ad0f382158a6c91'
GETSONG_API_KEY         = 'c1d3052529a15b51c20932a4283db3f1'
LASTFM_API_KEY          = '8b79bf6197a85dc2ff9e076da46792c5'
ACOUSTID_API_KEY        = '4c23noZ1UA'
DISCOGS_TOKEN           = 'uDnXzYJqaiNqwclprniLgPlCsEqfoEzBaTyDPAiF'
AUDD_API_KEY            = 'ebfab499d0b0fd6a7add88d50352f0d9'
```

### Rischio
- Chiunque scarichi l'EXE può estrarre il bytecode con `pyinstxtractor` + `decompyle3` e leggere queste chiavi in chiaro
- Le chiavi possono essere abusate: rate limit consumato per conto tuo, account sospeso dal provider, charge se l'API è a pagamento
- **In particolare il `DISCOGS_TOKEN`** è personale tuo: se viene abusato, Discogs sospende il TUO account

### Severità per provider

| Provider | Severità | Motivo |
|----------|----------|--------|
| Discogs | 🔴 ALTA | Token personale, rate limit attribuito a te, sospensione account possibile |
| Last.fm | 🟡 MEDIA | Tier free generoso ma è comunque la tua chiave |
| Spotify | 🟡 MEDIA | Client secret esposto = chiunque può fare auth come "Music Cataloger" |
| GetSong | 🟢 BASSA | Web scraping non ufficiale, niente API key vincolata a te (verifica) |
| AcoustID | 🟢 BASSA | Trial scaduto come da memoria |
| AudD | 🟢 BASSA | Trial scaduto come da memoria |

### Soluzione proposta — Proxy server-side

Tutte le chiamate ad API esterne passano attraverso il server NAS:

```
PRIMA (client diretto):
  client → discogs.com [con DISCOGS_TOKEN del client]

DOPO (server proxy):
  client → /api/v1/lookup/discogs?artist=X&title=Y  [con JWT del cliente]
  server  → discogs.com [con DISCOGS_TOKEN del server]
  server ← discogs.com
  client ← server [risultato passato through]
```

Vantaggi:
- Token mai esposti al client
- Server può fare rate limiting per utente (es. base = 10 lookup/giorno, pro = 1000)
- Server può cache le risposte centralmente (futuro community DB)
- Server può audit log (chi chiede cosa, quando)

Lavoro stimato lato client: ~2 ore (sostituire chiamate dirette con `api_client.lookup_discogs()`).
Lavoro stimato lato server: ~3 ore (aggiungere endpoint proxy + token in `.env` server + rate limiter).

### Soluzione alternativa parziale — Mantenere alcune chiavi client

Se vuoi accettare il rischio per **Spotify/Last.fm** (che hanno tier free generosi e non sono critici), si può:
- Spostare server-side: **Discogs** (token personale tuo, critico)
- Lasciare client-side: **Last.fm, Spotify**, ma con possibilità futura di migration server-side

---

## 2. Plan/feature gating completamente client-side

**File:** `config/user_plans.py` (PLAN_FEATURES dict), `gui/main_window.py` (decine di `if features.get(...)`)

### Il problema in concreto

```python
# config/user_plans.py
PLAN_FEATURES = {
    "base":     { "catalog_external_db": False, "tab_advanced": False, ... },
    "pro":      { "catalog_external_db": True,  "tab_advanced": False, ... },
    "advanced": { "catalog_external_db": True,  "tab_advanced": True,  ... },
}
```

Un attaccante:
1. Estrae l'EXE con `pyinstxtractor`
2. Decompila `user_plans.py` con `decompyle3`
3. Cambia `"base"` per avere tutto `True`
4. Ricompatta l'EXE (o usa l'EXE estratto come Python)
5. **Ora ha tutte le feature di Advanced** senza pagare

In più: in `gui/main_window.py` molti check sono:
```python
if features.get("catalog_external_db", True):  # default TRUE!
```
Con default `True`, basta passare un piano sconosciuto e tutte le feature sono attive.

### Auditato: punti di plan check nel client

Trovati ~15 `features.get(...)` in `gui/main_window.py` (linee 1044, 1059, 1075, 1681, 1714, 1744, 1756, 2111, ecc.) e in `config/user_plans.py:has_feature()`.

### Cosa è realmente protetto OGGI

**Niente lato client**. La protezione che hai oggi è puramente cosmetica/UX — nascondere bottoni a chi non ha il piano. Ma chi smanetta vede tutto.

### Soluzione proposta — Server come fonte di verità

**Principio:** Il client può ANCHE mostrare/nascondere bottoni in base al plan (UX), ma ogni azione che usa una feature plan-gated deve **passare per il server**, che fa il check.

Esempi concreti:

#### Feature: BPM analysis (Pro+)
- **Oggi:** client invoca `librosa` localmente → niente check possibile
- **Domani:** client chiede al server `/api/v1/bpm?file_hash=X` che fa il check plan e poi computa o ritorna 403

⚠️ **Problema con BPM**: la libreria `librosa` gira sul CLIENT, non sul server. Non possiamo "spostarla" senza farne re-engineering grosso.

**Compromesso pragmatico**: il client mantiene la feature ma deve "registrare" l'uso al server prima di partire. Server verifica plan + rate limit + audit log. Se il server dice OK, il client procede. Bypass possibile patchando il client per skippare la registrazione, ma rate limit per `max_runs_per_day` resta efficace perché viene contato server-side.

#### Feature: tab_advanced (Advanced only)
- **Oggi:** client nasconde il tab se `tab_advanced=False`
- **Domani:** client può ancora "mostrare il tab" se uno smanetta, ma ogni endpoint chiamato dal tab Advanced (es. `/api/v1/batch_rename`, `/api/v1/integrity_check`) richiede plan=advanced server-side

#### Feature: max_files_per_run / max_runs_per_day
- **Oggi:** check client-side, bypass banale
- **Domani:** client chiede al server `POST /api/v1/jobs/start` con `n_files`. Server controlla quota giorno + plan limits, ritorna `job_id` o 429 Too Many Requests.

### Stima lavoro
- ~5 ore lato server per aggiungere `@require_plan("pro")` decorator + audit endpoints
- ~3 ore lato client per sostituire chiamate locali con chiamate server-mediated
- Resta lato client la UI gating (nascondere bottoni) per UX, ma è "soft" — accettato che si possa bypassare

---

## 3. `is_admin` letto dal JWT senza verifica firma

**File:** `services/api_client.py:386`

### Il problema

```python
# Client decodifica JWT SENZA verificare la firma
payload = decode_jwt_payload(s.access_token)
return {
    ...
    "is_admin": payload.get("is_admin", False),
}
```

Il client legge `is_admin` dal payload del JWT, ma **non verifica la firma**. Il commento dice:
> *"il client si fida del payload solo per info non-sensibili come piano e username — il server comunque valida il token su ogni chiamata"*

Questo è **parzialmente vero**:
- ✅ **Il server valida il JWT su ogni chiamata** (Authorization Bearer)
- ❌ **Il client mostra le tab admin solo guardando `is_admin` dal payload locale**

Concretamente: chi smanetta può modificare `session.json` cambiando l'access_token con un JWT (anche fake) che dichiara `is_admin=True`. La GUI mostra le tab admin. Quando l'utente clicca le funzioni admin, il server respinge perché il JWT è invalido. **L'utente vede le tab ma non funzionano.**

### Severità

🟡 **MEDIA**. Non c'è vero bypass perché il server respinge. Ma:
- L'utente vede l'UI admin (information disclosure: scopre che esistono funzioni admin)
- Esperienza utente confusa: tab visibili ma ogni click dà errore
- Compromessa "security by obscurity" delle funzioni admin

### Soluzione proposta

Il server deve **anche includere `is_admin` come campo separato della risposta `/auth/login` e `/auth/me`**, e il client si fida solo di quello. Se l'utente modifica `session.json`, dopo qualche minuto il refresh token verrà invalidato dal server e l'utente verrà sloggato.

Alternativa più robusta: il client chiama `/auth/me` all'avvio (oltre a leggere il JWT) e usa la risposta del server come autoritativa per la UI.

### Stima lavoro
1 ora client + 30 min server.

---

## 4. EXE update senza verifica firma digitale

**File:** `services/updater.py`

### Stato attuale

```python
def _download_to_temp(url: str, expected_sha256: Optional[str] = None, ...):
    ...
    if expected_sha256:
        actual = _sha256_file(dest)
        if actual.lower() != expected_sha256.lower():
            raise ValueError("SHA256 mismatch...")
```

✅ Il client **supporta** SHA256 check
❌ Ma usa lo SHA256 che ARRIVA DAL SERVER stesso. Se il server è compromesso, l'attaccante può servire malware con il suo SHA256 corrispondente.

Il server è il tuo NAS Synology — è un'assunzione ragionevole considerarlo trusted. Ma:
- Se uno ti hackera il NAS (es. password admin debole, vulnerabilità DSM non patchata), può pushare un EXE malevolo a tutti i client
- L'EXE malevolo gira con i permessi dell'utente che fa l'update → può fare tutto sull'utente

### Soluzione proposta — Firma digitale

Schema: tu firmi gli EXE con una chiave privata che vive **fuori dal NAS** (sul tuo PC o un hardware token). Il client ha la public key hardcoded. Il server serve sia l'EXE sia la signature. Il client verifica.

Implementazione concreta:
```python
# Client genera all'install (una volta) o tu generi e committi:
private_key, public_key = ed25519_keypair_generate()
# public_key (32 bytes) hardcoded nel client

# Workflow release:
# 1. Builda EXE
# 2. Firma: signature = ed25519_sign(private_key, sha256(EXE))
# 3. Server pubblica: EXE + signature
# 4. Client scarica entrambi, verifica signature con public_key
```

Pro: anche se il NAS è compromesso, l'attaccante non può forgiare signature valide senza la chiave privata.
Contro: tu devi proteggere la chiave privata (offline, password manager).

### Severità

🟡 **MEDIA**. È un rischio reale ma il vettore (compromissione NAS) richiede skills. Per un pilot di amici è accettabile rimandare. Per distribuzione pubblica è quasi obbligatorio.

### Stima lavoro
3 ore (Ed25519 disponibile in `cryptography` library, già pulita da pip).

---

## 5. Storage locale in chiaro

**File:** `data/session.json`, `data/local_db.json`, `data/client_config.json`, `data/genre_prefs.json`, ecc.

### Contenuto sensibile

```json
// session.json
{
  "access_token": "eyJhbGc...",      // JWT con plan, is_admin
  "refresh_token": "eyJhbGc...",     // 7 giorni
  "user_email": "user@example.com"
}
```

```json
// local_db.json
{
  "files": {
    "Pop/Beatles - Yesterday.mp3": { ... },
    ...
  }
}
```

### Rischio

- Chiunque acceda al profilo Windows può leggere i token JWT in chiaro
- I JWT possono essere usati per fare richieste al server fino allo scadere del refresh (7 giorni)
- `local_db.json` rivela cosa l'utente ascolta (privacy)

### Soluzione possibile

`keyring` (Python lib, integrata con Windows Credential Manager):
```python
import keyring
keyring.set_password("music-cataloger", "access_token", access_token)
keyring.set_password("music-cataloger", "refresh_token", refresh_token)
```

Pro: token cifrati nativamente da Windows, accessibili solo dall'utente loggato.
Contro: aggiunge una dipendenza, leggermente più complesso per debug ("non vedo il file token").

### Severità

🟢 **BASSA**. Tutti i browser fanno lo stesso (cookie session in chiaro su filesystem). Per pilot è accettabile, da documentare in `SECURITY.md`.

### Stima lavoro
2 ore.

---

## 6. Cose che NON sono problemi (audit clean)

- ✅ Niente `shell=True` in subprocess → no command injection
- ✅ Niente `eval()` o `exec()` su input utente
- ✅ Subprocess `cmd` costruita come **list** non come stringa
- ✅ HTTPS di default verso il NAS (`https://api.choros27.synology.me`)
- ✅ Cert validation di default (non c'è `verify=False`)
- ✅ Refresh token con rotation gestita server-side
- ✅ Password mai loggata
- ✅ Login UI ha campo password con `show="*"`
- ✅ Email normalizzata lowercase (v1086.4)
- ✅ Singleton lock per istanza (v1086.2)

---

## 7. Cose che dipendono dal server (non auditabili da qui)

Devo verificare con te o sui file server:

| Item | Dove verificare | Importanza |
|------|-----------------|------------|
| Password hashing algoritmo (bcrypt? argon2?) | Server `auth.py` o `models.py` | 🔴 Critico |
| Bcrypt cost factor (≥12) | Server config | 🔴 Critico |
| Rate limit login (es. 5/min per IP) | Server middleware o endpoint `/auth/login` | 🔴 Critico |
| `SECRET_KEY` JWT in `.env` lato server (NON in repo) | Server `.env` / `.env.example` | 🔴 Critico |
| `is_admin` aggiornato dal server, non auto-promote | Server endpoint `/auth/admin/users` | 🟡 Importante |
| Audit log immutabile | Server DB | 🟢 Nice-to-have |
| CORS configurato strict (no `allow_origins=["*"]`) | Server `main.py` | 🟡 Importante |

---

## 8. Piano operativo proposto

### Fase 1 — Server-side hardening (prerequisito a tutto)
**Quello che devo ricevere da te per procedere:**
- File `auth.py` / `main.py` / `models.py` del server
- File `.env.example` (per vedere quali secret sono già server-side)
- O alternativamente: ti consegno una **patch testuale** che applichi al server

**Cosa faccio:**
1. Audit password hashing (verifica bcrypt cost)
2. Aggiungo `@require_plan("pro")` / `@require_plan("advanced")` decorator
3. Implemento rate limit login (5/min per IP)
4. Verifico CORS settings
5. Endpoint proxy per Discogs/Last.fm/Spotify
6. Endpoint `/api/v1/jobs/start` per quota check
7. Endpoint `/auth/me` ritorna `is_admin` esplicito

### Fase 2 — Client refactoring
1. `config/secrets.py` → svuotato (solo MUSICBRAINZ_USER_AGENT che è pubblico)
2. `services/external_apis.py` → tutte le chiamate API passano da `api_client` (server proxy)
3. `config/user_plans.py` → `PLAN_FEATURES` lasciato per UI hint, ma **default `False`** invece di `True` per non avere fallback permissivo
4. `gui/main_window.py` → check plan diventano "soft" (UX only), il vero check è server-side
5. `services/api_client.py` → `is_admin` letto da `/auth/me` non solo dal JWT

### Fase 3 — Firma EXE (opzionale, rimandabile)
1. Generazione chiave Ed25519 (una volta)
2. Workflow di build firma l'EXE
3. Client verifica con public key hardcoded

### Fase 4 — Storage cifrato (opzionale, rimandabile)
1. Migration `session.json` → Windows Credential Manager via `keyring`

---

## 9. Decisioni che mi servono da te

1. **Discogs token**: server-side proxy SUBITO, o teniamo client per ora? (consiglio: subito, è personale tuo)
2. **Spotify/Last.fm**: server-side proxy o accettiamo client? (consiglio: per ora client, in futuro server)
3. **Plan check server-side**: facciamo il refactor adesso o lo fai tu sul server e io adatto il client?
4. **Firma EXE**: in questo branch o in uno futuro?
5. **Storage cifrato**: in questo branch o in uno futuro?
6. **Codice server**: me lo passi qui o mi dai patch da farti applicare manualmente?

Dopo le tue risposte, posso partire con la **Fase 2** (client) in parallelo, anche senza tutti i fix server, lasciando dei TODO documentati nel codice. Quando hai pronto il server, faremo il merge finale.
