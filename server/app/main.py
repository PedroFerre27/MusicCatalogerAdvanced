"""
main.py — Entrypoint FastAPI

Monta i router, configura CORS, esegue init_db e seed admin al startup.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

# v0.2.2 (security-audit S1): rate limiting login anti brute-force.
# `limiter` vive in services.ratelimit per evitare import circolare
# (main → api.auth → main).
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from .config import settings
from .models.db import init_db, SessionLocal, User
from .services.auth import hash_password
from .services.ratelimit import limiter

from .api import auth as auth_api
from .api import plans as plans_api
from .api import catalog as catalog_api
from .api import updates as updates_api
from .api import lookup as lookup_api


def _seed_admin(db: Session) -> None:
    """
    Al primo avvio, se il DB è vuoto, crea un account admin con le
    credenziali definite in settings.ADMIN_EMAIL / ADMIN_PASSWORD.
    L'utente dovrà cambiare la password al primo login.
    """
    if db.query(User).first() is not None:
        return
    admin = User(
        email=settings.ADMIN_EMAIL,
        username="Admin",
        password_hash=hash_password(settings.ADMIN_PASSWORD),
        plan="advanced",
        is_admin=True,
        is_active=True,
    )
    db.add(admin)
    db.commit()
    print(f"[seed] Creato account admin: {settings.ADMIN_EMAIL}")
    print(f"[seed] Password temporanea: {settings.ADMIN_PASSWORD}")
    print(f"[seed] ⚠️  Cambia la password al primo login!")


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[startup] {settings.APP_NAME} v{settings.APP_VERSION} — env={settings.ENV}")

    # v0.1.7: hard-fail in produzione se SECRET_KEY è ancora il placeholder
    # del template. In dev (ENV=development) emette solo un warning per
    # non rompere il flusso di sviluppo locale.
    PLACEHOLDER_PREFIXES = (
        "CHANGE-ME",
        "INSERT-",
        "REPLACE-ME",
        "your-secret",
    )
    sk = (settings.SECRET_KEY or "").strip()
    is_placeholder = (
        not sk
        or len(sk) < 32
        or any(sk.upper().startswith(p.upper()) for p in PLACEHOLDER_PREFIXES)
    )
    if is_placeholder:
        msg = (
            "SECRET_KEY non configurato o ancora al placeholder.\n"
            "In produzione genera una chiave sicura con:\n"
            "  python -c \"import secrets; print(secrets.token_urlsafe(64))\"\n"
            "e impostala in .env come SECRET_KEY=..."
        )
        if settings.ENV == "production":
            raise RuntimeError(f"[FATAL] {msg}")
        else:
            print(f"[WARNING] {msg}")

    init_db()
    db = SessionLocal()
    try:
        _seed_admin(db)
    finally:
        db.close()

    # v0.1.4: Recovery job interrotti da restart server.
    # Architettura: il worker reale è il CLIENT (cataloga in locale).
    # Se il server è restartato mentre un client stava catalogando,
    # quei job restano in 'running' ma non avranno più update — li
    # marchiamo come 'failed' al boot per non bloccare le quote.
    from datetime import datetime as _dt
    db = SessionLocal()
    try:
        from .models.db import Job
        stuck = db.query(Job).filter(Job.status == "running").all()
        for j in stuck:
            j.status = "failed"
            j.error_message = "Server riavviato durante l'esecuzione"
            j.completed_at = _dt.utcnow()
        if stuck:
            db.commit()
            print(f"[startup] {len(stuck)} job 'running' marcati come 'failed' (recovery)")
    finally:
        db.close()

    # v0.2.0: avvia task cleanup periodico job orfani
    _cleanup_orphan_jobs()
    print("[startup] cleanup orfani attivo (check ogni 5 min)")

    yield
    print("[shutdown] Server fermato")


# ── v0.2.0 — Cleanup orfani periodico ────────────────────────────
def _cleanup_orphan_jobs():
    """Task in background che ogni 5 minuti marca come 'failed' i
    job 'running' senza progress da più di STUCK_MINUTES.

    Casi tipici:
    - Client crasha senza notificare /complete o /cancel
    - Connessione persa permanentemente durante catalogazione
    - Utente chiude l'app brutalmente (kill task manager)

    Senza questo cleanup il job resta 'running' indefinitamente, le
    quote restano consumate e il prossimo /catalog/start può fallire
    con "limite max raggiunto".
    """
    import threading, time
    from datetime import datetime as _dt, timedelta as _td
    from .models.db import Job

    STUCK_MINUTES   = 30      # senza update da 30+ min → marca failed
    CHECK_INTERVAL  = 300     # check ogni 5 min

    def _loop():
        while True:
            try:
                time.sleep(CHECK_INTERVAL)
                cutoff = _dt.utcnow() - _td(minutes=STUCK_MINUTES)
                db = SessionLocal()
                try:
                    stuck = (db.query(Job)
                             .filter(Job.status == "running")
                             .filter(Job.last_progress_at < cutoff)
                             .all())
                    for j in stuck:
                        j.status = "failed"
                        j.error_message = (f"Nessun update da "
                            f"{STUCK_MINUTES} min — client probabilmente "
                            f"disconnesso")
                        j.completed_at = _dt.utcnow()
                    if stuck:
                        db.commit()
                        print(f"[cleanup] {len(stuck)} job orfani "
                              f"marcati come failed")
                finally:
                    db.close()
            except Exception as e:
                print(f"[cleanup] errore ciclo: {e}")

    t = threading.Thread(target=_loop, daemon=True, name="orphan-cleanup")
    t.start()


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan,
    docs_url="/docs" if settings.ENV != "production" else None,
    redoc_url=None,
)

# v0.2.2 (S1): registra il rate limiter sull'app + handler 429
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    # v0.2.2 (S6): metodi/header espliciti invece di ["*"]. Con
    # allow_credentials=True il wildcard e' sconsigliato. Restringo
    # ai soli metodi/header realmente usati dal client.
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Admin-Token"],
)

# ── Router ────────────────────────────────────────────────────────
app.include_router(auth_api.router)
app.include_router(plans_api.router)
app.include_router(catalog_api.router)
app.include_router(updates_api.router)
app.include_router(lookup_api.router)   # v0.2.3 proxy lookup


@app.get("/", tags=["system"])
def root():
    return {
        "name":    settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status":  "running",
    }


@app.get("/health", tags=["system"])
def health():
    return {"status": "ok"}
