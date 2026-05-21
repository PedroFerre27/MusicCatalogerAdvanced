"""
api/catalog.py — Endpoint catalogazione (v0.1.4)

ARCHITETTURA: il *cliente* esegue la catalogazione localmente sulla sua
macchina (è dove sono i file MP3). Il *server* tiene la coda dei job,
applica le quote del piano, e raccoglie progress + log centralizzati.

Endpoint principali:
  POST  /catalog/start          → cliente notifica "sto partendo"
                                  → server crea Job, applica quote piano,
                                    risponde con job_id
  POST  /catalog/{id}/progress  → cliente invia update progresso
                                  (files_done, files_total, status, log_chunk)
  POST  /catalog/{id}/complete  → cliente notifica fine + invia report JSON
  POST  /catalog/{id}/fail      → cliente notifica errore
  GET   /catalog/{id}/status    → leggi stato (utente o admin)
  GET   /catalog/{id}/logs      → leggi log incrementale (?after=N)
  GET   /catalog/my-jobs        → ultimi N job dell'utente
  GET   /admin/all-jobs         → admin: tutti i job di tutti gli utenti

Quote (applicate da plans.PLAN_FEATURES):
  - max_files_per_run: rifiuta /start se files_total > limit
  - max_runs_per_day:  rifiuta /start se conteggio job nelle ultime 24h ≥ limit
"""
import json
from datetime import datetime, timedelta
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import desc

from ..models.db import User, Job, JobLog, get_db
from ..services.auth import get_current_user, require_admin
from ..services.plans import get_features

router = APIRouter(tags=["catalog"])


# ── Schemas ───────────────────────────────────────────────────────
class CatalogOptions(BaseModel):
    """Opzioni di catalogazione passate dal client al server."""
    dry_run:           bool = True
    cleanup_empty:     bool = False
    use_external_db:   bool = False
    analyze_bpm:       bool = False
    fetch_cover:       bool = False
    correct_folders:   bool = False
    classify_salsa:    bool = False
    duplicate_action:  str  = "keep_both"


class StartRequest(BaseModel):
    """Cliente notifica intent di iniziare catalogazione."""
    path:        str = Field(..., description="Path locale della cliente (informativo)")
    options:     CatalogOptions = CatalogOptions()
    files_total: int = Field(0, ge=0, description="Numero di file rilevati (post-scan)")


class ProgressUpdate(BaseModel):
    """Cliente invia update di avanzamento."""
    files_done:    int = Field(..., ge=0)
    files_total:   Optional[int] = None
    progress_pct:  int = Field(..., ge=0, le=100)
    log_chunk:     Optional[str] = Field(None, max_length=10000,
                                         description="Log testuale da appendere")
    log_level:     str = Field("INFO", pattern="^(DEBUG|INFO|WARNING|ERROR)$")


class CompleteRequest(BaseModel):
    """Cliente notifica fine — invia il report JSON finale."""
    files_done:  int = Field(..., ge=0)
    report:      dict = Field(default_factory=dict,
                              description="Report finale (cataloger.generate_report)")


class FailRequest(BaseModel):
    error_message: str = Field(..., max_length=2000)


class JobOut(BaseModel):
    id:            int
    user_id:       int
    path:          str
    status:        str
    progress_pct:  int
    files_total:   int
    files_done:    int
    error_message: Optional[str]
    created_at:    datetime
    started_at:    Optional[datetime]
    completed_at:  Optional[datetime]

    class Config:
        from_attributes = True


class StartResponse(BaseModel):
    job_id:           int
    status:           str
    quota_remaining:  int = Field(..., description="-1 = illimitato; altrimenti runs ancora disponibili oggi")


class LogLine(BaseModel):
    id:        int
    level:     str
    message:   str
    timestamp: datetime

    class Config:
        from_attributes = True


# ── Helpers ───────────────────────────────────────────────────────
def _check_options_against_plan(opts: CatalogOptions, features: dict) -> None:
    """Rifiuta opzioni non concesse dal piano dell'utente."""
    if opts.use_external_db and not features["catalog_external_db"]:
        raise HTTPException(403, "Il tuo piano non include il DB online")
    if opts.analyze_bpm and not features["catalog_bpm"]:
        raise HTTPException(403, "Il tuo piano non include l'analisi BPM")
    if opts.fetch_cover and not features["catalog_cover"]:
        raise HTTPException(403, "Il tuo piano non include il recupero cover")


def _runs_today(db: Session, user_id: int) -> int:
    """Conta job creati dall'utente nelle ultime 24h."""
    cutoff = datetime.utcnow() - timedelta(hours=24)
    return (db.query(Job)
              .filter(Job.user_id == user_id, Job.created_at >= cutoff)
              .count())


def _get_job_or_403(db: Session, job_id: int, user: User) -> Job:
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(404, "Job non trovato")
    if job.user_id != user.id and not user.is_admin:
        raise HTTPException(403, "Non puoi accedere a questo job")
    return job


# ── Endpoint cliente ──────────────────────────────────────────────
@router.post("/catalog/start", response_model=StartResponse,
             status_code=status.HTTP_201_CREATED)
def start_catalog(
    req:  StartRequest,
    user: User = Depends(get_current_user),
    db:   Session = Depends(get_db),
):
    """
    Cliente notifica "sto per iniziare" PRIMA di catalogare. Server:
      1. Valida opzioni vs piano
      2. Verifica quote (max_files_per_run, max_runs_per_day)
      3. Crea Job in stato 'running'
      4. Risponde job_id + quota residua

    Se il client non chiama /complete o /fail entro X ore, il job
    resta in 'running' (cleaner periodico → marcare 'failed' — TODO).
    """
    features = get_features(user.plan)

    # Quota 1: file totali per run
    max_files = features.get("max_files_per_run", -1)
    if max_files > 0 and req.files_total > max_files:
        raise HTTPException(
            status.HTTP_402_PAYMENT_REQUIRED,
            f"Il tuo piano consente max {max_files} file per catalogazione "
            f"(richiesti: {req.files_total}). Richiedi un upgrade per il "
            f"limite più alto."
        )

    # Quota 2: run giornaliere
    max_runs = features.get("max_runs_per_day", -1)
    runs_used = _runs_today(db, user.id)
    if max_runs > 0 and runs_used >= max_runs:
        raise HTTPException(
            status.HTTP_402_PAYMENT_REQUIRED,
            f"Hai già usato {runs_used}/{max_runs} catalogazioni nelle "
            f"ultime 24 ore (limite del tuo piano). Riprova più tardi o "
            f"richiedi un upgrade."
        )
    quota_remaining = -1 if max_runs <= 0 else max_runs - runs_used - 1

    # Validazione opzioni vs piano
    _check_options_against_plan(req.options, features)

    # Crea job in stato running
    job = Job(
        user_id=user.id,
        path=req.path[:500],
        options=json.dumps(req.options.model_dump()),
        status="running",
        files_total=req.files_total,
        files_done=0,
        progress_pct=0,
        started_at=datetime.utcnow(),
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return StartResponse(job_id=job.id, status=job.status,
                         quota_remaining=quota_remaining)


@router.post("/catalog/{job_id}/progress")
def update_progress(
    job_id: int,
    upd:    ProgressUpdate,
    user:   User = Depends(get_current_user),
    db:     Session = Depends(get_db),
):
    """Cliente invia update progresso. Append eventuale log_chunk in JobLog."""
    job = _get_job_or_403(db, job_id, user)
    if job.status not in ("running", "queued"):
        raise HTTPException(409, f"Job non in esecuzione (status={job.status})")
    if job.user_id != user.id:
        raise HTTPException(403, "Solo il proprietario può aggiornare il progresso")

    job.files_done = upd.files_done
    if upd.files_total is not None:
        job.files_total = upd.files_total
    job.progress_pct = upd.progress_pct
    job.last_progress_at = datetime.utcnow()    # v0.2.0: per cleanup orfani
    if upd.log_chunk:
        # Splitta su newline per avere righe singole — facilita il display
        for line in upd.log_chunk.splitlines():
            line = line.strip()
            if not line:
                continue
            db.add(JobLog(job_id=job.id, level=upd.log_level,
                          message=line[:2000]))
    db.commit()
    return {"ok": True}


@router.post("/catalog/{job_id}/complete", response_model=JobOut)
def complete_job(
    job_id: int,
    body:   CompleteRequest,
    user:   User = Depends(get_current_user),
    db:     Session = Depends(get_db),
):
    """Cliente notifica fine — server marca completato e salva il report."""
    job = _get_job_or_403(db, job_id, user)
    if job.user_id != user.id:
        raise HTTPException(403)
    if job.status not in ("running", "queued"):
        raise HTTPException(409, f"Job non in esecuzione (status={job.status})")

    job.status = "completed"
    job.files_done = body.files_done
    job.progress_pct = 100
    job.completed_at = datetime.utcnow()
    job.result_json = json.dumps(body.report)[:50000]   # safety cap
    db.commit()
    db.refresh(job)
    return job


@router.post("/catalog/{job_id}/fail", response_model=JobOut)
def fail_job(
    job_id: int,
    body:   FailRequest,
    user:   User = Depends(get_current_user),
    db:     Session = Depends(get_db),
):
    """Cliente notifica errore."""
    job = _get_job_or_403(db, job_id, user)
    if job.user_id != user.id:
        raise HTTPException(403)
    if job.status in ("completed", "failed", "cancelled"):
        raise HTTPException(409, f"Job già {job.status}")

    job.status = "failed"
    job.error_message = body.error_message[:2000]
    job.completed_at = datetime.utcnow()
    db.commit()
    db.refresh(job)
    return job


@router.post("/catalog/{job_id}/cancel", response_model=JobOut)
def cancel_job(
    job_id: int,
    user:   User = Depends(get_current_user),
    db:     Session = Depends(get_db),
):
    """Utente o admin cancella job in esecuzione."""
    job = _get_job_or_403(db, job_id, user)
    if job.status in ("completed", "failed", "cancelled"):
        raise HTTPException(409, f"Job già {job.status}")
    job.status = "cancelled"
    job.completed_at = datetime.utcnow()
    db.commit()
    db.refresh(job)
    return job


# ── Lettura stato e log ───────────────────────────────────────────
@router.get("/catalog/{job_id}/status", response_model=JobOut)
def job_status(
    job_id: int,
    user:   User = Depends(get_current_user),
    db:     Session = Depends(get_db),
):
    return _get_job_or_403(db, job_id, user)


@router.get("/catalog/{job_id}/logs", response_model=List[LogLine])
def job_logs(
    job_id: int,
    after:  int = 0,
    limit:  int = 500,
    user:   User = Depends(get_current_user),
    db:     Session = Depends(get_db),
):
    """Polling incrementale dei log: client passa l'id dell'ultimo log
    già visto, server ritorna solo quelli più recenti."""
    _get_job_or_403(db, job_id, user)
    return (db.query(JobLog)
              .filter(JobLog.job_id == job_id, JobLog.id > after)
              .order_by(JobLog.id)
              .limit(min(limit, 1000))
              .all())


@router.get("/catalog/{job_id}/results")
def job_results(
    job_id: int,
    user:   User = Depends(get_current_user),
    db:     Session = Depends(get_db),
):
    job = _get_job_or_403(db, job_id, user)
    if job.status != "completed":
        raise HTTPException(409, f"Job non ancora completato (status={job.status})")
    return json.loads(job.result_json or "{}")


@router.get("/catalog/my-jobs", response_model=List[JobOut])
def my_jobs(
    limit: int = 20,
    user:  User = Depends(get_current_user),
    db:    Session = Depends(get_db),
):
    """Ultimi N job dell'utente, ordinati dal più recente."""
    return (db.query(Job)
              .filter(Job.user_id == user.id)
              .order_by(desc(Job.created_at))
              .limit(min(limit, 100))
              .all())


# ── Admin ─────────────────────────────────────────────────────────
@router.get("/admin/all-jobs", response_model=List[JobOut])
def admin_all_jobs(
    limit: int = 50,
    admin: User = Depends(require_admin),
    db:    Session = Depends(get_db),
):
    """Admin: tutti i job di tutti gli utenti."""
    return (db.query(Job)
              .order_by(desc(Job.created_at))
              .limit(min(limit, 500))
              .all())
