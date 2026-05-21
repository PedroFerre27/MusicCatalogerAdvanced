"""
models/db.py — Modelli SQLAlchemy

Schema minimo:
- users: utenti registrati con piano corrente
- upgrade_requests: richieste di cambio piano pending
- jobs: catalogazioni in corso o completate
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Text, ForeignKey,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from ..config import settings

Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id            = Column(Integer, primary_key=True, index=True)
    email         = Column(String(255), unique=True, nullable=False, index=True)
    username      = Column(String(64), nullable=False)
    password_hash = Column(String(255), nullable=False)
    plan          = Column(String(16), nullable=False, default="base")
    # "base" | "pro" | "advanced"
    is_admin      = Column(Boolean, default=False, nullable=False)
    is_active     = Column(Boolean, default=True,  nullable=False)
    created_at    = Column(DateTime, default=datetime.utcnow, nullable=False)
    plan_expires_at = Column(DateTime, nullable=True)   # None = senza scadenza
    # v0.2.2 (security-audit S4/S5): versione dei token. Inserita come
    # claim "tv" nel JWT. get_current_user rifiuta token con tv diverso
    # da quello corrente dell'utente. Incrementata al change-password
    # (invalida tutte le sessioni) e dall'endpoint admin revoke-sessions.
    token_version = Column(Integer, default=0, nullable=False)

    upgrade_requests = relationship("UpgradeRequest", back_populates="user")
    jobs             = relationship("Job", back_populates="user")


class UpgradeRequest(Base):
    __tablename__ = "upgrade_requests"
    id         = Column(Integer, primary_key=True)
    user_id    = Column(Integer, ForeignKey("users.id"), nullable=False)
    from_plan  = Column(String(16), nullable=False)
    to_plan    = Column(String(16), nullable=False)
    status     = Column(String(16), default="pending", nullable=False)
    # "pending" | "approved" | "rejected"
    message    = Column(Text, nullable=True)   # motivazione utente
    admin_note = Column(Text, nullable=True)   # nota admin al momento dell'approvazione
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    resolved_at= Column(DateTime, nullable=True)

    user = relationship("User", back_populates="upgrade_requests")


class Job(Base):
    """
    Una singola richiesta di catalogazione. Il worker la esegue in background,
    il client fa polling su /catalog/status/{job_id}.
    """
    __tablename__ = "jobs"
    id            = Column(Integer, primary_key=True)
    user_id       = Column(Integer, ForeignKey("users.id"), nullable=False)
    path          = Column(String(500), nullable=False)   # path relativo a MUSIC_DIR
    options       = Column(Text, nullable=False)          # JSON serializzato
    status        = Column(String(16), default="queued", nullable=False)
    # "queued" | "running" | "completed" | "failed" | "cancelled"
    progress_pct  = Column(Integer, default=0)
    files_total   = Column(Integer, default=0)
    files_done    = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    result_json   = Column(Text, nullable=True)    # report finale serializzato
    created_at    = Column(DateTime, default=datetime.utcnow)
    started_at    = Column(DateTime, nullable=True)
    completed_at  = Column(DateTime, nullable=True)
    # v0.2.0: timestamp dell'ultimo progress ricevuto. Usato dal task
    # cleanup orfani: se passa più di N minuti senza update il job è
    # considerato abbandonato (client crashato o disconnesso).
    last_progress_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="jobs")
    logs = relationship("JobLog", back_populates="job",
                        cascade="all, delete-orphan",
                        order_by="JobLog.id")


class JobLog(Base):
    """
    Linea di log di un job. Il worker scrive qui durante l'esecuzione,
    il client fa polling con offset per ricevere gli incrementi.

    Schema minimal — non indicizzato per timestamp perché query sempre per job_id.
    """
    __tablename__ = "job_logs"
    id        = Column(Integer, primary_key=True)
    job_id    = Column(Integer, ForeignKey("jobs.id"), nullable=False, index=True)
    level     = Column(String(8), default="INFO", nullable=False)
    # "DEBUG" | "INFO" | "WARNING" | "ERROR"
    message   = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)

    job = relationship("Job", back_populates="logs")


# ── Engine & Session ──────────────────────────────────────────────
_connect_args = {"check_same_thread": False} if settings.DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(settings.DATABASE_URL, connect_args=_connect_args, echo=settings.DEBUG)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


# v0.1.7 — Audit log azioni admin
class AdminAuditLog(Base):
    """Traccia ogni azione admin sensibile per accountability nel pilot.

    Eventi registrati:
      - upgrade_approved / upgrade_rejected (plans.py)
      - plan_changed (admin_set_plan in plans.py)
      - caribbean_defaults_published (plans.py)

    Volutamente immutabile: nessun endpoint per modificare/cancellare.
    """
    __tablename__ = "admin_audit_log"
    id          = Column(Integer, primary_key=True)
    admin_id    = Column(Integer, ForeignKey("users.id"), nullable=False)
    admin_email = Column(String(255), nullable=False)   # denormalizzato per resistere a cancellazione user
    action      = Column(String(64),  nullable=False)   # es. "upgrade_approved"
    target_id   = Column(Integer, nullable=True)        # user_id o request_id target
    target_email= Column(String(255), nullable=True)
    details     = Column(Text, nullable=True)           # JSON con dettagli (old_plan, new_plan, note...)
    created_at  = Column(DateTime, default=datetime.utcnow, nullable=False)


def _run_migrations() -> None:
    """v0.2.1: micro-migrations per upgrade DB esistenti.

    SQLAlchemy `Base.metadata.create_all()` crea solo tabelle nuove,
    NON aggiunge colonne a tabelle esistenti. Quando aggiungo una
    colonna al modello (es. Job.last_progress_at in v0.2.0) i deployment
    esistenti vanno in errore al primo SELECT.

    Per evitare di introdurre Alembic ora (overhead troppo grande per
    un pilot), faccio le migration manualmente con SQL diretto. Ogni
    blocco è idempotente: controlla se la modifica è già stata
    applicata prima di tentarla.

    Piano lungo termine: quando avrò più di 3-4 migration introdurrò
    Alembic con baseline corrente.
    """
    from sqlalchemy import text
    with engine.begin() as conn:
        # ── v0.2.0 → v0.2.1: jobs.last_progress_at ──────────────
        try:
            cols = {row[1] for row in conn.execute(
                text("PRAGMA table_info(jobs)"))}
            if "last_progress_at" not in cols:
                # SQLite supporta ALTER TABLE ADD COLUMN ma NON con
                # default non-costante (es. CURRENT_TIMESTAMP). Usiamo
                # un default NULL e popoliamo subito i record esistenti
                # con created_at (che esiste già).
                conn.execute(text(
                    "ALTER TABLE jobs ADD COLUMN last_progress_at DATETIME"))
                conn.execute(text(
                    "UPDATE jobs SET last_progress_at = "
                    "COALESCE(started_at, created_at) "
                    "WHERE last_progress_at IS NULL"))
                print("[migration] aggiunta colonna jobs.last_progress_at")
        except Exception as e:
            print(f"[migration] WARNING jobs.last_progress_at: {e}")

        # ── v0.2.1 → v0.2.2: users.token_version (security-audit S4) ──
        try:
            cols = {row[1] for row in conn.execute(
                text("PRAGMA table_info(users)"))}
            if "token_version" not in cols:
                conn.execute(text(
                    "ALTER TABLE users ADD COLUMN token_version "
                    "INTEGER NOT NULL DEFAULT 0"))
                print("[migration] aggiunta colonna users.token_version")
        except Exception as e:
            print(f"[migration] WARNING users.token_version: {e}")

        # Spazio per future migrations qui


def init_db() -> None:
    """Crea le tabelle se non esistono. Chiamato all'avvio server."""
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(bind=engine)
    # v0.2.1: applica eventuali migration pending PRIMA che qualunque
    # query sia eseguita
    _run_migrations()


def get_db():
    """Dependency FastAPI per injection della session DB."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
