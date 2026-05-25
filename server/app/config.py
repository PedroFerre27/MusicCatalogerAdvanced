"""
config.py — Configurazione centralizzata letta da env var / .env

Tutte le impostazioni sensibili (SECRET_KEY, DB URL) passano da qui,
mai hardcoded. Su Docker vengono iniettate via docker-compose env_file.
"""
from pathlib import Path
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

    # ── Generale ──────────────────────────────────────────────────
    APP_NAME: str = "Music Cataloger Server"
    APP_VERSION: str = "0.2.4"
    ENV: str = "production"    # development | production
    DEBUG: bool = False

    # ── Server ────────────────────────────────────────────────────
    HOST: str = "0.0.0.0"
    PORT: int = 8020

    # ── CORS ──────────────────────────────────────────────────────
    # Default: dominio Synology in produzione. Il client GUI chiama via
    # HTTPS reverse proxy. Override via env CORS_ORIGINS se servono altri
    # origin (es. sviluppo locale aggiungendo "http://localhost:3000").
    CORS_ORIGINS: List[str] = [
        "https://choros27.synology.me",
        "https://api.choros27.synology.me",
    ]

    # ── Database ──────────────────────────────────────────────────
    # v0.2.2 (S2): default production-ready (path ASSOLUTO). In
    # sviluppo locale si sovrascrive via .env con
    # sqlite:///./data/app.db. Il default sicuro evita che, se il
    # .env manca o ha un path relativo errato, il server crei un DB
    # vuoto in una working dir imprevista perdendo gli account.
    DATABASE_URL: str = "sqlite:////srv/app/data/app.db"

    # ── JWT Auth ──────────────────────────────────────────────────
    # SECRET_KEY deve essere generata con: python -c "import secrets; print(secrets.token_urlsafe(64))"
    # In produzione va OBBLIGATORIAMENTE passata da env var, mai committata.
    SECRET_KEY: str = "CHANGE-ME-IN-PRODUCTION-WITH-SECRETS-TOKEN-URLSAFE-64"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # ── API musicali esterne (v0.2.3 — security-audit Fase 2) ──────
    # Token spostati QUI dal client config/secrets.py. Mai committati:
    # vanno nel .env del NAS (gitignored). Se mancano, il proxy
    # /api/v1/lookup ritorna None per quel provider e il client fa
    # fallback ai provider pubblici (iTunes/MusicBrainz/Deezer).
    LASTFM_API_KEY: str = ""
    SPOTIFY_CLIENT_ID: str = ""
    SPOTIFY_CLIENT_SECRET: str = ""
    DISCOGS_TOKEN: str = ""
    GETSONG_API_KEY: str = ""

    # ── SMTP (v0.2.4 — R3: email transazionali) ────────────────────
    # Config per email di benvenuto + notifiche admin alla
    # registrazione. Default: Gmail su 587 (STARTTLS). Per Gmail serve
    # una App Password (https://myaccount.google.com/apppasswords),
    # NON la password normale dell'account. Se SMTP_HOST o SMTP_USER
    # sono vuoti, il server logga un warning e salta l'invio: la
    # registrazione utente NON fallisce mai per problemi SMTP.
    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    SMTP_FROM: str = ""    # default: stesso valore di SMTP_USER
    ADMIN_NOTIFY_EMAIL: str = ""    # destinatario notifica nuova registrazione

    # ── Storage ───────────────────────────────────────────────────
    # Cartelle dati del server (montate come volumi in Docker)
    DATA_DIR: Path = Path("./data")
    MUSIC_DIR: Path = Path("/music")   # sul NAS = bind mount della libreria
    OUTPUT_DIR: Path = Path("./output")

    # ── Job queue ─────────────────────────────────────────────────
    # Numero massimo di catalogazioni concorrenti. Su DS415+ con
    # Atom C2538 (4 core) consigliato max 2 per non saturare.
    MAX_CONCURRENT_JOBS: int = 2

    # ── Admin iniziale ────────────────────────────────────────────
    # Al primo avvio viene creato un admin con queste credenziali
    # se non esistono già utenti nel DB.
    # Default con TLD valido (EmailStr di pydantic rifiuta "user@host"
    # senza dominio con punto). In produzione va comunque sovrascritto.
    ADMIN_EMAIL: str = "admin@example.com"
    ADMIN_PASSWORD: str = "change-me-on-first-login"


settings = Settings()
