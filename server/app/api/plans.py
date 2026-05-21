"""
api/plans.py — Endpoint piani e richieste upgrade

GET  /plans                   → lista di tutti i piani con feature (pubblico)
GET  /plans/me                → features del piano dell'utente corrente
POST /plans/upgrade-request   → crea richiesta upgrade (client → admin)
GET  /plans/my-requests       → richieste upgrade dell'utente corrente

ADMIN:
GET  /admin/upgrade-requests                → elenco richieste pending
POST /admin/upgrade-requests/{id}/approve   → approva + cambia piano utente
POST /admin/upgrade-requests/{id}/reject    → rifiuta con motivazione
"""
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..models.db import User, UpgradeRequest, get_db
from ..services.auth import get_current_user, require_admin
from ..services.plans import (
    PLAN_FEATURES, PLAN_DISPLAY_NAMES, PLAN_ORDER,
    get_features, can_upgrade_to, upgrades_available,
)

router = APIRouter(tags=["plans"])


class PlanInfo(BaseModel):
    name:         str
    display_name: str
    features:     dict


class UpgradeRequestCreate(BaseModel):
    to_plan: str
    message: Optional[str] = None


class UpgradeRequestOut(BaseModel):
    id:           int
    user_id:      int                 # v0.1.6: per visualizzazione admin
    user_email:   Optional[str] = None
    user_name:    Optional[str] = None
    from_plan:    str
    to_plan:      str
    status:       str
    message:      Optional[str]
    admin_note:   Optional[str]
    created_at:   datetime
    resolved_at:  Optional[datetime]

    class Config:
        from_attributes = True


# ── Endpoint pubblici / utente ────────────────────────────────────
@router.get("/plans", response_model=List[PlanInfo])
def list_plans():
    """Restituisce tutti i piani con le loro feature (utile per pagina upgrade)."""
    return [
        PlanInfo(
            name=p,
            display_name=PLAN_DISPLAY_NAMES[p],
            features=PLAN_FEATURES[p],
        )
        for p in PLAN_ORDER
    ]


@router.get("/plans/me")
def my_plan(user: User = Depends(get_current_user)):
    return {
        "plan":              user.plan,
        "display_name":      PLAN_DISPLAY_NAMES.get(user.plan, user.plan),
        "features":          get_features(user.plan),
        "plan_expires_at":   user.plan_expires_at,
        "upgrades_available": upgrades_available(user.plan),
    }


@router.post("/plans/upgrade-request", response_model=UpgradeRequestOut)
def create_upgrade_request(
    body: UpgradeRequestCreate,
    user: User = Depends(get_current_user),
    db:   Session = Depends(get_db),
):
    """L'utente richiede l'upgrade. Rimane in pending finché l'admin non approva."""
    if body.to_plan not in PLAN_FEATURES:
        raise HTTPException(status_code=400, detail="Piano di destinazione non valido")
    if not can_upgrade_to(user.plan, body.to_plan):
        raise HTTPException(
            status_code=400,
            detail=f"Non puoi fare upgrade da '{user.plan}' a '{body.to_plan}'",
        )
    # Evita duplicati pending
    existing = (
        db.query(UpgradeRequest)
        .filter(
            UpgradeRequest.user_id == user.id,
            UpgradeRequest.status == "pending",
        )
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=409,
            detail="Hai già una richiesta upgrade in attesa. Attendi la risposta prima di inviarne altre.",
        )
    req = UpgradeRequest(
        user_id=user.id,
        from_plan=user.plan,
        to_plan=body.to_plan,
        message=body.message,
        status="pending",
    )
    db.add(req)
    db.commit()
    db.refresh(req)
    # TODO: inviare email admin (task post-MVP)
    return req


@router.get("/plans/my-requests", response_model=List[UpgradeRequestOut])
def my_upgrade_requests(
    user: User = Depends(get_current_user),
    db:   Session = Depends(get_db),
):
    return (
        db.query(UpgradeRequest)
        .filter(UpgradeRequest.user_id == user.id)
        .order_by(UpgradeRequest.created_at.desc())
        .all()
    )


# ── Endpoint ADMIN ────────────────────────────────────────────────
@router.get("/admin/upgrade-requests", response_model=List[UpgradeRequestOut])
def list_pending_requests(
    admin: User = Depends(require_admin),
    db:    Session = Depends(get_db),
):
    # v0.1.6: arricchisco con user_email/user_name per visualizzazione GUI
    rows = (
        db.query(UpgradeRequest)
        .filter(UpgradeRequest.status == "pending")
        .order_by(UpgradeRequest.created_at.asc())
        .all()
    )
    out = []
    for r in rows:
        d = {
            "id": r.id,
            "user_id": r.user_id,
            "from_plan": r.from_plan,
            "to_plan": r.to_plan,
            "status": r.status,
            "message": r.message,
            "admin_note": r.admin_note,
            "created_at": r.created_at,
            "resolved_at": r.resolved_at,
            "user_email": r.user.email if r.user else None,
            "user_name":  r.user.username if r.user else None,
        }
        out.append(d)
    return out


class AdminResolve(BaseModel):
    admin_note: Optional[str] = None


# v0.1.7 — Helper per registrare audit log delle azioni admin
def _log_admin_action(
    db:           Session,
    admin:        User,
    action:       str,
    target_id:    Optional[int] = None,
    target_email: Optional[str] = None,
    details:      Optional[dict] = None,
):
    """Inserisce una riga in admin_audit_log. Errori loggati ma non
    interrompono il flusso: l'azione admin è la cosa importante,
    l'audit è secondario."""
    import json as _json
    try:
        from ..models.db import AdminAuditLog
        entry = AdminAuditLog(
            admin_id=admin.id,
            admin_email=admin.email,
            action=action,
            target_id=target_id,
            target_email=target_email,
            details=_json.dumps(details, ensure_ascii=False) if details else None,
        )
        db.add(entry)
        db.commit()
    except Exception as e:
        print(f"[audit] log failed: {e}")
        db.rollback()


@router.post("/admin/upgrade-requests/{req_id}/approve",
             response_model=UpgradeRequestOut)
def approve_request(
    req_id: int,
    body:   AdminResolve,
    admin:  User = Depends(require_admin),
    db:     Session = Depends(get_db),
):
    req = db.query(UpgradeRequest).filter(UpgradeRequest.id == req_id).first()
    if not req:
        raise HTTPException(status_code=404, detail="Richiesta non trovata")
    if req.status != "pending":
        raise HTTPException(status_code=400,
                            detail=f"Richiesta già {req.status}")

    target_user = db.query(User).filter(User.id == req.user_id).first()
    if not target_user:
        raise HTTPException(status_code=404, detail="Utente non trovato")

    old_plan = target_user.plan
    # Esegui l'upgrade
    target_user.plan = req.to_plan
    req.status = "approved"
    req.admin_note = body.admin_note
    req.resolved_at = datetime.utcnow()
    db.commit()
    db.refresh(req)

    # v0.1.7: audit log
    _log_admin_action(
        db, admin, "upgrade_approved",
        target_id=target_user.id,
        target_email=target_user.email,
        details={
            "request_id": req.id,
            "old_plan": old_plan,
            "new_plan": req.to_plan,
            "admin_note": body.admin_note,
        },
    )
    # NOTA: l'utente dovrà refresh il JWT per vedere il nuovo piano
    #       (l'access token corrente contiene ancora il piano vecchio)
    return req


@router.post("/admin/upgrade-requests/{req_id}/reject",
             response_model=UpgradeRequestOut)
def reject_request(
    req_id: int,
    body:   AdminResolve,
    admin:  User = Depends(require_admin),
    db:     Session = Depends(get_db),
):
    req = db.query(UpgradeRequest).filter(UpgradeRequest.id == req_id).first()
    if not req:
        raise HTTPException(status_code=404, detail="Richiesta non trovata")
    if req.status != "pending":
        raise HTTPException(status_code=400,
                            detail=f"Richiesta già {req.status}")
    req.status = "rejected"
    req.admin_note = body.admin_note
    req.resolved_at = datetime.utcnow()
    db.commit()
    db.refresh(req)

    # v0.1.7: audit log
    target_user = db.query(User).filter(User.id == req.user_id).first()
    _log_admin_action(
        db, admin, "upgrade_rejected",
        target_id=req.user_id,
        target_email=target_user.email if target_user else None,
        details={
            "request_id": req.id,
            "from_plan": req.from_plan,
            "to_plan":   req.to_plan,
            "admin_note": body.admin_note,
        },
    )
    return req


# ── v0.1.5 — Admin: cambia piano direttamente ─────────────────────
class AdminSetPlan(BaseModel):
    plan: str = Field(..., pattern="^(base|pro|advanced)$")


@router.post("/admin/users/{user_id}/set-plan", response_model=dict)
def admin_set_plan(
    user_id: int,
    body:    AdminSetPlan,
    admin:   User = Depends(require_admin),
    db:      Session = Depends(get_db),
):
    """
    Forza il piano di un utente (anche se stesso). Utile per:
    - Ripristinare admin a 'advanced' se il seed iniziale ha messo altro
    - Test rapidi di transizioni di piano senza creare richieste upgrade
    - Gestire casi eccezionali fuori dal flusso upgrade-request

    Restituisce il nuovo plan + nome utente. L'utente target dovrà
    rifare il login (o aspettare il refresh del JWT) per vedere il
    nuovo piano applicato lato client.
    """
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "Utente non trovato")
    old_plan = user.plan
    user.plan = body.plan
    db.commit()

    # v0.1.7: audit log
    _log_admin_action(
        db, admin, "plan_changed",
        target_id=user.id, target_email=user.email,
        details={"old_plan": old_plan, "new_plan": body.plan},
    )

    return {
        "ok":       True,
        "user_id":  user.id,
        "username": user.username,
        "email":    user.email,
        "old_plan": old_plan,
        "new_plan": user.plan,
    }


@router.get("/admin/users", response_model=list)
def admin_list_users(
    admin: User = Depends(require_admin),
    db:    Session = Depends(get_db),
):
    """Lista tutti gli utenti — utile per ottenere user_id da passare
    a /admin/users/{id}/set-plan."""
    return [
        {
            "id":         u.id,
            "email":      u.email,
            "username":   u.username,
            "plan":       u.plan,
            "is_admin":   u.is_admin,
            "is_active":  u.is_active,
            "created_at": u.created_at.isoformat() if u.created_at else None,
        }
        for u in db.query(User).order_by(User.id).all()
    ]


# ── v0.1.7 — Caribbean settings condivise ─────────────────────────
class CaribbeanSettings(BaseModel):
    """
    Default caraibici condivisi tra tutti gli utenti.
    Pubblicati dall'admin tramite POST /admin/caribbean-settings,
    letti da chiunque tramite GET /caribbean-settings/defaults.

    Il client li scarica al primo avvio (o se data/caribbean_settings.json
    non esiste localmente) e li applica come baseline. L'utente resta
    libero di modificarli localmente — non vengono mai sovrascritti
    automaticamente in seguito.
    """
    bachata_bpm_range:  list = Field(default_factory=lambda: [120, 140])
    salsa_bpm_range:    list = Field(default_factory=lambda: [180, 220])
    salsa_artists:      list = Field(default_factory=list)
    salsa_keywords:     list = Field(default_factory=list)
    bachata_artists:    list = Field(default_factory=list)
    bachata_keywords:   list = Field(default_factory=list)


# Path per il file JSON con i default condivisi
def _caribbean_settings_path():
    from ..config import settings
    return (settings.DATA_DIR / "caribbean_defaults.json").resolve()


@router.get("/caribbean-settings/defaults", response_model=CaribbeanSettings)
def get_caribbean_defaults():
    """
    Pubblico: ritorna i default caraibici condivisi pubblicati dall'admin.
    Se l'admin non ha ancora pubblicato nulla, ritorna struttura vuota
    (il client userà i suoi default locali). Sempre 200, mai 404, per
    semplificare la logica di boot del client.
    """
    import json as _json
    from pathlib import Path
    p = _caribbean_settings_path()
    if not p.exists() or not p.is_file():
        return CaribbeanSettings()
    try:
        data = _json.loads(p.read_text(encoding="utf-8"))
        # Filtra solo i campi noti (evita injection di campi extra)
        clean = {k: v for k, v in data.items()
                 if k in CaribbeanSettings.model_fields}
        return CaribbeanSettings(**clean)
    except (PermissionError, OSError, _json.JSONDecodeError) as e:
        print(f"[caribbean] WARNING: leggere {p}: {e}")
        return CaribbeanSettings()


@router.post("/admin/caribbean-settings", response_model=CaribbeanSettings)
def set_caribbean_defaults(
    body:  CaribbeanSettings,
    admin: User = Depends(require_admin),
    db:    Session = Depends(get_db),
):
    """
    Admin pubblica i nuovi default caraibici. Tutti gli utenti li
    riceveranno al prossimo avvio (boot del client) o quando premono
    "Reset ai default" nella sezione Caribbean.
    """
    import json as _json
    p = _caribbean_settings_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = body.model_dump()
    import datetime as _dt
    payload["_saved_at"] = _dt.datetime.utcnow().isoformat()
    payload["_saved_by"] = admin.email
    p.write_text(_json.dumps(payload, indent=2, ensure_ascii=False),
                  encoding="utf-8")

    # v0.1.7: audit log
    _log_admin_action(
        db, admin, "caribbean_defaults_published",
        details={
            "n_salsa_artists":    len(body.salsa_artists),
            "n_salsa_keywords":   len(body.salsa_keywords),
            "n_bachata_artists":  len(body.bachata_artists),
            "n_bachata_keywords": len(body.bachata_keywords),
            "salsa_bpm_range":    body.salsa_bpm_range,
            "bachata_bpm_range":  body.bachata_bpm_range,
        },
    )
    return body


# ── v0.1.7 — Audit log read endpoint ───────────────────────────────
class AuditLogEntry(BaseModel):
    id:           int
    admin_email:  str
    action:       str
    target_id:    Optional[int]
    target_email: Optional[str]
    details:      Optional[str]
    created_at:   datetime

    class Config:
        from_attributes = True


@router.get("/admin/audit-log", response_model=List[AuditLogEntry])
def admin_audit_log(
    limit: int = 100,
    admin: User = Depends(require_admin),
    db:    Session = Depends(get_db),
):
    """Storico immutabile delle azioni admin (max 500 righe per chiamata).
    Ordinato dal più recente al più vecchio."""
    from ..models.db import AdminAuditLog
    from sqlalchemy import desc
    rows = (db.query(AdminAuditLog)
              .order_by(desc(AdminAuditLog.created_at))
              .limit(min(limit, 500))
              .all())
    return rows


# ── v0.1.9 — Statistiche admin ──────────────────────────────────
class AdminStats(BaseModel):
    n_users_total:        int
    n_users_admin:        int
    n_users_by_plan:      dict
    n_jobs_total:         int
    n_jobs_completed:     int
    n_jobs_running:       int
    n_jobs_failed:        int
    n_files_processed:    int   # somma di processed_files su tutti i job completati
    n_pending_upgrades:   int
    db_size_kb:           int
    server_version:       str


@router.get("/admin/stats", response_model=AdminStats)
def admin_stats(
    admin: User = Depends(require_admin),
    db:    Session = Depends(get_db),
):
    """Snapshot delle statistiche per il pannello admin GUI."""
    from ..config import settings
    from ..models.db import Job
    from sqlalchemy import func

    # Utenti
    n_users = db.query(func.count(User.id)).scalar() or 0
    n_admins = db.query(func.count(User.id)).filter(User.is_admin == True).scalar() or 0

    # Per plan
    by_plan = {}
    for plan in ("base", "pro", "advanced"):
        by_plan[plan] = db.query(func.count(User.id)).filter(
            User.plan == plan).scalar() or 0

    # Job
    n_jobs = db.query(func.count(Job.id)).scalar() or 0
    n_completed = db.query(func.count(Job.id)).filter(
        Job.status == "completed").scalar() or 0
    n_running = db.query(func.count(Job.id)).filter(
        Job.status == "running").scalar() or 0
    n_failed = db.query(func.count(Job.id)).filter(
        Job.status == "failed").scalar() or 0

    # File processati totali
    n_files = db.query(func.coalesce(
        func.sum(Job.files_done), 0)).filter(
        Job.status == "completed").scalar() or 0

    # Richieste pending
    n_pending = db.query(func.count(UpgradeRequest.id)).filter(
        UpgradeRequest.status == "pending").scalar() or 0

    # DB size (solo SQLite)
    db_size_kb = 0
    if settings.DATABASE_URL.startswith("sqlite"):
        try:
            from pathlib import Path as _P
            db_path = settings.DATABASE_URL.replace("sqlite:///", "").replace("sqlite:////", "/")
            db_p = _P(db_path)
            if db_p.exists():
                db_size_kb = db_p.stat().st_size // 1024
        except Exception:
            pass

    return AdminStats(
        n_users_total=n_users, n_users_admin=n_admins,
        n_users_by_plan=by_plan,
        n_jobs_total=n_jobs, n_jobs_completed=n_completed,
        n_jobs_running=n_running, n_jobs_failed=n_failed,
        n_files_processed=int(n_files),
        n_pending_upgrades=n_pending,
        db_size_kb=db_size_kb,
        server_version=settings.APP_VERSION,
    )
