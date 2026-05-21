# SECURITY AUDIT — SERVER (FastAPI NAS)
**Branch:** `dev/security-audit`
**Data:** 16 maggio 2026
**File analizzati:** main.py, api/auth.py, services/auth.py, models/db.py, requirements.txt, .env, .env.example
**Server:** Synology DS415+ — https://api.choros27.synology.me

---

## TL;DR — Sommario

Il server è **strutturalmente più solido del client**. Buone pratiche già presenti:
- ✅ bcrypt via passlib (cost 12 default) per password hashing
- ✅ JWT firmato HS256, access 15min + refresh 7d
- ✅ `get_current_user` verifica firma + scadenza + tipo token
- ✅ `require_admin` dependency esiste
- ✅ Hard-fail in produzione se SECRET_KEY è placeholder
- ✅ Password policy su register/change-password/admin-create
- ✅ Audit log immutabile per azioni admin
- ✅ Recovery job orfani al boot

Problemi trovati: **8**, di cui 2 ALTA, 3 MEDIA, 3 BASSA.

| # | Problema | Severità |
|---|----------|----------|
| S1 | Nessun rate limiting login → brute force possibile | 🔴 ALTA |
| S2 | `.env` reale in modalità development su server produzione | 🔴 ALTA |
| S3 | Nessun plan-check enforcement server-side (manca `require_plan`) | 🟡 MEDIA |
| S4 | `change-password` non invalida i JWT esistenti | 🟡 MEDIA |
| S5 | Refresh token non ruotato + nessuna revoca | 🟡 MEDIA |
| S6 | CORS `allow_methods/headers=["*"]` con `allow_credentials=True` | 🟢 BASSA |
| S7 | Email non normalizzata lato server (case-sensitive lookup) | 🟢 BASSA |
| S8 | Endpoint admin senza `require_admin` dependency (check manuale) | 🟢 BASSA |

---

## S1 — Nessun rate limiting login 🔴 ALTA

**File:** `api/auth.py` endpoint `/auth/login`, `requirements.txt`

`requirements.txt` non contiene `slowapi` né altri rate limiter.
L'endpoint `/auth/login` accetta tentativi illimitati.

### Rischio
Un attaccante può fare brute-force sulla password dell'admin
(`admin@choros27.synology.me` — email nota dal `.env.example`
committato!) con migliaia di tentativi al minuto. bcrypt rallenta
(circa 100ms per verifica), ma senza rate limit restano ~600
tentativi/minuto, sufficienti per password deboli.

### Fix proposto
Aggiungere `slowapi` (rate limiter per FastAPI):

```txt
# requirements.txt — aggiungere
slowapi==0.1.9
```

```python
# main.py — aggiungere
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
```

```python
# api/auth.py — decorare login
from slowapi import Limiter
from slowapi.util import get_remote_address
from ..main import limiter   # o importarlo da un modulo condiviso

@router.post("/login", response_model=TokenPair)
@limiter.limit("5/minute")   # max 5 tentativi/min per IP
def login(request: Request, form: OAuth2PasswordRequestForm = Depends(),
          db: Session = Depends(get_db)):
    ...
```

NOTA: `slowapi` richiede `request: Request` come primo parametro
dell'endpoint. Va aggiunto alla firma di `login`.

### Considerazione NAS
Il server è dietro reverse proxy DSM. `get_remote_address` potrebbe
vedere sempre l'IP del proxy (127.0.0.1) invece dell'IP reale del
client. Va verificato che DSM passi `X-Forwarded-For` e configurare
slowapi per leggerlo:

```python
def real_ip(request: Request) -> str:
    xff = request.headers.get("X-Forwarded-For")
    return xff.split(",")[0].strip() if xff else get_remote_address(request)

limiter = Limiter(key_func=real_ip)
```

---

## S2 — `.env` in development su server produzione 🔴 ALTA

**File:** `.env` (quello reale che hai caricato)

Il `.env` caricato ha:
```
ENV=development
DEBUG=true
DATABASE_URL=sqlite:///./data/app.db          # path RELATIVO
CORS_ORIGINS=[...,"http://localhost",...]      # localhost in prod!
```

Mentre `.env.example` (il template) è correttamente production-ready:
```
ENV=production
DEBUG=false
DATABASE_URL=sqlite:////srv/app/data/app.db   # path ASSOLUTO
CORS_ORIGINS=["https://choros27...",...]       # solo domini reali
```

### Rischio
- `DEBUG=true` → `engine = create_engine(..., echo=True)` → **TUTTE le
  query SQL loggate**, comprese quelle che contengono dati utente.
  Su un server esposto è information disclosure.
- `ENV=development` → `/docs` (Swagger) **esposto pubblicamente**
  (in `main.py`: `docs_url="/docs" if settings.ENV != "production"`).
  Un attaccante vede tutta la API surface.
- `CORS` con `localhost` → una pagina web malevola in locale potrebbe
  chiamare l'API se l'utente è loggato.
- Path DB relativo → se il container parte da una working dir diversa,
  crea un DB nuovo VUOTO invece di usare quello sul NAS. Potenziale
  perdita apparente di tutti gli account.

### Fix
Il `.env` REALE sul NAS deve essere production. **Verifica sul NAS**:
```bash
docker exec <container> env | grep -E "ENV|DEBUG|DATABASE_URL"
# DEVE mostrare ENV=production, DEBUG=false, path assoluto
```

Se il `.env` che mi hai caricato è quello realmente in uso sul NAS,
**è un problema serio da correggere subito**. Se invece è solo la tua
copia di sviluppo locale e sul NAS c'è un `.env` diverso (production),
allora è OK — ma chiariamolo.

Coerente con la tua nota di memoria: *"per OGNI ZIP server fai un
audit con grep -rn '8000|/volume1/Musica|development' prima di
zippare"*. Aggiungerei al grep anche `ENV=development|DEBUG=true`.

---

## S3 — Nessun plan-check enforcement server-side 🟡 MEDIA

**File:** `services/auth.py`, `api/catalog.py` (non fornito ma dedotto)

`services/auth.py` ha `get_current_user` e `require_admin`, ma
**NON c'è `require_plan(...)`** o equivalente. Gli endpoint
`/catalog/*`, `/plans/*` (non forniti) probabilmente non verificano
il piano dell'utente.

Questo è IL punto centrale dell'intera security audit. Tutto il
discorso "plan check deve essere server-side" (di cui parlavamo a
v1086.1) si concretizza qui.

### Rischio
Se il server non verifica il piano, un client modificato (PLAN_FEATURES
patchato, già fixato lato client v1086.7 ma bypassabile) può invocare
endpoint plan-gated e il server li esegue comunque. La protezione
client è solo UX.

### Fix proposto — dependency `require_plan`

```python
# services/auth.py — aggiungere

# Gerarchia piani: advanced ⊃ pro ⊃ base
_PLAN_RANK = {"base": 0, "pro": 1, "advanced": 2}

def require_plan(minimum: str):
    """Dependency factory: l'utente deve avere `minimum` o superiore.

    Uso:
        @router.post("/catalog/start")
        def start(user: User = Depends(require_plan("pro"))):
            ...
    """
    min_rank = _PLAN_RANK.get(minimum, 0)

    def _checker(user: User = Depends(get_current_user)) -> User:
        user_rank = _PLAN_RANK.get(user.plan, 0)
        if user_rank < min_rank:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Funzionalità riservata al piano '{minimum}' o superiore. "
                       f"Il tuo piano attuale è '{user.plan}'.",
            )
        # v0.x: controllo scadenza piano
        if user.plan_expires_at is not None:
            from datetime import datetime
            if user.plan_expires_at < datetime.utcnow():
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Il tuo piano è scaduto. Rinnova per continuare.",
                )
        return user

    return _checker
```

Poi sugli endpoint catalog (file non fornito, da applicare quando lo
condividi):

```python
@router.post("/catalog/start")
def catalog_start(
    body: CatalogStartRequest,
    user: User = Depends(require_plan("pro")),   # ← gating server-side
    db:   Session = Depends(get_db),
):
    # Quota check anche qui (max_runs_per_day dal piano)
    ...
```

### Lavoro necessario da te
Devi condividere `api/catalog.py` e `api/plans.py` perché lì stanno
gli endpoint da proteggere. Senza quei file posso solo darti la
dependency `require_plan`; l'applicazione ai singoli endpoint la
faremo quando li vedo.

---

## S4 — change-password non invalida JWT esistenti 🟡 MEDIA

**File:** `api/auth.py` (commento esplicito nel codice lo ammette)

Il codice stesso documenta:
> *"NOTA: non invalida i JWT esistenti. Per farlo dovremmo memorizzare
> la versione della password in un claim del JWT..."*

### Rischio
Se un attaccante ruba un access token (15 min) o refresh token (7
giorni), e l'utente cambia password per sicurezza, **i token rubati
restano validi** fino a scadenza naturale. La password change non
"chiude le sessioni".

### Fix proposto — token version
Aggiungere `token_version` a User, includerlo nel JWT, verificarlo:

```python
# models/db.py — User
token_version = Column(Integer, default=0, nullable=False)

# services/auth.py — create_access_token
payload = {..., "tv": user.token_version}

# services/auth.py — get_current_user
if payload.get("tv") != user.token_version:
    raise HTTPException(401, "Token invalidato (password cambiata)")

# api/auth.py — change_password, dopo aver aggiornato l'hash:
user.token_version += 1   # invalida tutti i token vecchi
```

Va aggiunta una micro-migration in `_run_migrations()` per la colonna
`token_version` (segui il pattern esistente di `last_progress_at`).

Severità MEDIA perché richiede prima un furto di token (vettore non
banale su HTTPS).

---

## S5 — Refresh token non ruotato + nessuna revoca 🟡 MEDIA

**File:** `api/auth.py` endpoint `/auth/refresh`

`/auth/refresh` accetta un refresh token valido e ritorna un nuovo
access token, ma:
- NON ruota il refresh token (best practice: refresh rotation)
- NON tiene una blacklist/whitelist → un refresh token rubato è
  utilizzabile per 7 giorni senza possibilità di revoca

### Rischio
Refresh token rubato = accesso per 7 giorni, non revocabile nemmeno
da admin. Combinato con S4, una sessione compromessa è difficile da
chiudere.

### Fix proposto (pragmatico per pilot)
Il fix completo (rotation + DB store dei refresh token) è oneroso.
Per il pilot propongo il minimo efficace: il `token_version` di S4
copre anche questo caso. Quando l'utente cambia password,
`token_version += 1` invalida anche i refresh. Aggiungiamo un
endpoint admin per forzare il logout di un utente:

```python
@router.post("/admin/users/{user_id}/revoke-sessions")
def admin_revoke_sessions(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """Invalida TUTTI i token (access+refresh) dell'utente
    incrementando il suo token_version. L'utente dovrà rifare login."""
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(404, "Utente non trovato")
    target.token_version += 1
    db.commit()
    # audit log...
    return {"ok": True, "message": "Sessioni revocate"}
```

Refresh rotation completo → rimandabile a post-pilot.

---

## S6 — CORS troppo permissivo 🟢 BASSA

**File:** `main.py`

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,    # ✅ ristretto (buono)
    allow_credentials=True,
    allow_methods=["*"],                     # ⚠️ tutti i metodi
    allow_headers=["*"],                     # ⚠️ tutti gli header
)
```

`allow_origins` è correttamente ristretto (buono). Ma
`allow_methods=["*"]` + `allow_headers=["*"]` + `allow_credentials=True`
è più permissivo del necessario.

### Rischio
Basso: dato che `allow_origins` è ristretto ai domini Synology, il
rischio reale è minimo. È hardening difensivo.

### Fix
```python
allow_methods=["GET", "POST", "PUT", "DELETE"],
allow_headers=["Authorization", "Content-Type"],
```

---

## S7 — Email non normalizzata lato server 🟢 BASSA

**File:** `api/auth.py` login/register

Lato client v1086.4 abbiamo normalizzato l'email a lowercase. Ma il
server fa `db.query(User).filter(User.email == form.username)` — match
case-sensitive. Se due flussi salvano email con case diverso, o se un
client diverso dal nostro non normalizza, si creano account
duplicati / login falliti.

### Fix
Normalizzare lato server a registrazione e login:

```python
# register
email_normalized = body.email.strip().lower()
existing = db.query(User).filter(User.email == email_normalized).first()
new_user = User(email=email_normalized, ...)

# login
user = db.query(User).filter(
    User.email == form.username.strip().lower()).first()
```

E migration one-shot per normalizzare gli email esistenti nel DB.

---

## S8 — Endpoint admin con check manuale invece di dependency 🟢 BASSA

**File:** `api/auth.py`

`services/auth.py` definisce `require_admin` come dependency pulita,
ma gli endpoint admin in `api/auth.py` fanno il check manualmente:

```python
@router.post("/admin/registration/disable")
def admin_disable_registration(user: User = Depends(get_current_user)):
    if not user.is_admin:           # ← check manuale ripetuto
        raise HTTPException(403, "Solo admin")
```

Funziona, ma è error-prone: se aggiungi un endpoint admin e dimentichi
il check manuale, è aperto a tutti. Meglio la dependency.

### Fix
```python
from ..services.auth import require_admin

@router.post("/admin/registration/disable")
def admin_disable_registration(admin: User = Depends(require_admin)):
    # niente più check manuale — la dependency garantisce admin
    ...
```

Applicare a tutti gli endpoint `/admin/*`: `admin_disable_registration`,
`admin_enable_registration`, `admin_create_user`.

Severità BASSA perché i check manuali attuali SONO presenti e
corretti. È robustezza preventiva.

---

## Cose che NON sono problemi (server pulito)

- ✅ Password hashing bcrypt cost 12 (default passlib) — adeguato
- ✅ JWT firma HS256 con SECRET_KEY robusta (len 86 nel .env)
- ✅ `get_current_user` valida firma + exp + type + utente attivo
- ✅ `decode_token` gestisce JWTError → 401 pulito
- ✅ SECRET_KEY hard-fail in produzione se placeholder
- ✅ Password policy applicata (register, change, admin-create)
- ✅ Audit log immutabile per azioni admin
- ✅ SQLAlchemy ORM parametrizzato → no SQL injection
- ✅ Refresh token type-checked (rifiuta access usato come refresh)
- ✅ `is_active` verificato → account disattivati non passano
- ✅ Recovery job orfani robusto

---

## Piano operativo proposto

### Priorità 1 (questo branch, ALTA)
- [ ] S1 — slowapi rate limit login (richiede: requirements.txt, main.py, auth.py)
- [ ] S2 — verifica `.env` reale sul NAS (azione TUA: comando docker exec)

### Priorità 2 (questo branch, MEDIA)
- [ ] S3 — `require_plan` dependency + applicazione su endpoint catalog
       (richiede: mi mandi `api/catalog.py` e `api/plans.py`)
- [ ] S4 — token_version per invalidare JWT su change-password
- [ ] S5 — endpoint admin revoke-sessions (usa token_version di S4)

### Priorità 3 (questo branch, BASSA)
- [ ] S6 — CORS methods/headers ristretti
- [ ] S7 — email normalizzata server-side + migration
- [ ] S8 — `require_admin` dependency su tutti gli endpoint admin

### Poi: Fase 2-4 client (dal SECURITY_AUDIT.md precedente)
- [ ] Endpoint proxy `/api/v1/lookup/{discogs,lastfm,spotify}` + `/api/v1/bpm/getsong`
- [ ] Client che chiama il server invece delle API dirette
- [ ] Firma EXE Ed25519
- [ ] Storage cifrato session.json (keyring)

### Infine
- [ ] SECURITY.md (documentazione finale)
- [ ] README.md (GitHub + utenti)

---

## Cosa mi serve da te per procedere

1. **Conferma S2**: il `.env` che mi hai dato è quello reale sul NAS o
   è la tua copia locale di sviluppo? Esegui sul NAS:
   ```bash
   docker exec <nome-container> env | grep -E "ENV=|DEBUG=|DATABASE_URL="
   ```
   e incollami l'output (non contiene secret).

2. **File mancanti** per completare l'audit + i fix:
   - `api/catalog.py` (endpoint catalogazione — dove applicare require_plan)
   - `api/plans.py` (endpoint piani/upgrade)
   - `api/updates.py` (endpoint /version/latest — per firma EXE)
   - `config.py` o `config/__init__.py` del server (la classe Settings)
   - `services/plans.py` (get_features, PLAN_DISPLAY_NAMES)

3. **Decisione**: i fix server li applico come **patch testuali** che
   incolli tu, oppure ti **riscrivo i file interi** e te li consegno
   come ZIP da deployare? (consiglio: file interi, meno errori di
   applicazione manuale)

Dopo le tue risposte parto con i fix in ordine di priorità.
