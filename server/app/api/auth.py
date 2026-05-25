"""
api/auth.py — Endpoint autenticazione

POST /auth/login              → {access_token, refresh_token, user}
POST /auth/refresh            → {access_token}
GET  /auth/me                 → {user}
POST /auth/change-password    → {ok: true}   (v0.1.3)
POST /auth/register           → {user}       (v0.1.3, self-service signup)
"""
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.orm import Session

from ..models.db import User, get_db
from ..services.auth import (
    verify_password, hash_password,
    create_access_token, create_refresh_token,
    decode_token, get_current_user, require_admin,
)
from ..services.email_service import send_email
from ..services.plans import get_features, PLAN_DISPLAY_NAMES
from ..services.ratelimit import limiter

router = APIRouter(prefix="/auth", tags=["auth"])


class UserPublic(BaseModel):
    id: int
    email: EmailStr
    username: str
    plan: str
    plan_display: str
    is_admin: bool
    features: dict

    class Config:
        from_attributes = True


class TokenPair(BaseModel):
    access_token:  str
    refresh_token: str
    token_type:    str = "bearer"
    user:          UserPublic


class RefreshRequest(BaseModel):
    refresh_token: str


class AccessOnly(BaseModel):
    access_token: str
    token_type:   str = "bearer"


def _user_to_public(user: User) -> UserPublic:
    return UserPublic(
        id=user.id, email=user.email, username=user.username,
        plan=user.plan,
        plan_display=PLAN_DISPLAY_NAMES.get(user.plan, user.plan),
        is_admin=user.is_admin,
        features=get_features(user.plan),
    )


@router.post("/login", response_model=TokenPair)
@limiter.limit("5/minute")
def login(request: Request,
          form: OAuth2PasswordRequestForm = Depends(),
          db: Session = Depends(get_db)):
    # v0.2.2 (S1): rate limit 5 tentativi/min per IP (anti brute-force).
    #   `request: Request` e' OBBLIGATORIO come primo parametro per
    #   slowapi (legge l'IP da li').
    # v0.2.2 (S7): email normalizzata (strip + lowercase) per evitare
    #   account duplicati / login case-sensitive falliti.
    # OAuth2PasswordRequestForm usa 'username' come campo → l'utente manda email
    email_norm = (form.username or "").strip().lower()
    user = db.query(User).filter(User.email == email_norm).first()
    if not user or not verify_password(form.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Email o password errate",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account disattivato")

    return TokenPair(
        access_token=create_access_token(user),
        refresh_token=create_refresh_token(user),
        user=_user_to_public(user),
    )


@router.post("/refresh", response_model=AccessOnly)
def refresh(body: RefreshRequest, db: Session = Depends(get_db)):
    payload = decode_token(body.refresh_token)
    if payload.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Token non è di tipo refresh")
    user_id = payload.get("sub")
    user = db.query(User).filter(User.id == int(user_id)).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="Utente non trovato o disattivato")
    # v0.2.2 (S5): il refresh token porta il claim "tv". Se non combacia
    # col token_version corrente, e' stato revocato (password cambiata o
    # admin revoke-sessions) → rifiuta, l'utente deve rifare login.
    if payload.get("tv", 0) != getattr(user, "token_version", 0):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token revocato. Effettua di nuovo il login.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return AccessOnly(access_token=create_access_token(user))


@router.get("/me", response_model=UserPublic)
def me(user: User = Depends(get_current_user)):
    return _user_to_public(user)


# ── v0.1.3 — Change password ──────────────────────────────────────
class ChangePasswordRequest(BaseModel):
    current_password: str = Field(..., min_length=1)
    new_password:     str = Field(..., min_length=8,
                                  description="Minimo 8 caratteri")


class OkResponse(BaseModel):
    ok: bool = True
    message: str = ""


@router.post("/change-password", response_model=OkResponse)
def change_password(
    body: ChangePasswordRequest,
    user: User = Depends(get_current_user),
    db:   Session = Depends(get_db),
):
    """
    Cambia la password dell'utente autenticato. Richiede la password
    corrente per sicurezza (standard pratica).

    NOTA: non invalida i JWT esistenti. Per farlo dovremmo memorizzare
    la versione della password in un claim del JWT e rifiutare i token
    con versione vecchia. Implementazione post-MVP.
    """
    if not verify_password(body.current_password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Password corrente errata",
        )
    if body.current_password == body.new_password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La nuova password deve essere diversa dalla corrente",
        )
    # v0.1.9: password policy
    from ..services.password_policy import validate_password
    pwd_error = validate_password(
        body.new_password, email=user.email, username=user.username)
    if pwd_error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=pwd_error)
    user.password_hash = hash_password(body.new_password)
    # v0.2.2 (S4): invalida TUTTE le sessioni esistenti (access+refresh).
    # I token rubati prima del cambio password smettono di funzionare.
    user.token_version = getattr(user, "token_version", 0) + 1
    db.commit()
    return OkResponse(
        message="Password aggiornata. Le altre sessioni sono state disconnesse.")


# ── v0.1.3 — Self-service register ────────────────────────────────
class RegisterRequest(BaseModel):
    email:    EmailStr
    username: str = Field(..., min_length=2, max_length=64)
    password: str = Field(..., min_length=8,
                          description="Minimo 8 caratteri")


@router.post("/register", response_model=UserPublic,
             status_code=status.HTTP_201_CREATED)
def register(body: RegisterRequest,
             background_tasks: BackgroundTasks,
             db: Session = Depends(get_db)):
    """
    Registrazione self-service. Il nuovo utente parte sempre con piano
    'base' e deve richiedere l'upgrade tramite il normale flusso
    /plans/upgrade-request.

    v0.1.8: rispetta il flag `REGISTRATION_OPEN` (file
    `data/registration_open.flag`) gestito dall'admin via endpoint
    POST /admin/registration/{enable|disable}. Quando disabilitato,
    risponde 403 — utile per pilot privato dove solo l'admin crea
    account ai clienti.

    NOTA MVP: non c'è email di verifica. L'utente appena creato può
    loggarsi immediatamente.
    """
    # v0.1.8: check registration enabled
    if not _is_registration_open():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="La registrazione self-service è temporaneamente "
                   "disabilitata. Contatta l'amministratore per ottenere "
                   "un account.",
        )

    # v0.1.9: password policy
    from ..services.password_policy import validate_password
    pwd_error = validate_password(
        body.password, email=body.email, username=body.username)
    if pwd_error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=pwd_error)

    # v0.2.2 (S7): normalizza email (strip + lowercase) per evitare
    # account duplicati con case diverso
    email_norm = (str(body.email) or "").strip().lower()

    # Verifica email non in uso
    existing = db.query(User).filter(User.email == email_norm).first()
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email già registrata",
        )

    new_user = User(
        email=email_norm,
        username=body.username,
        password_hash=hash_password(body.password),
        plan="base",
        is_admin=False,
        is_active=True,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    # v0.2.4 (R3): email transazionali asincrone via BackgroundTasks.
    # send_email cattura gia' tutte le eccezioni SMTP: la registrazione
    # NON fallisce mai se il server di posta e' giu' o malconfigurato.
    from ..config import settings as _s

    # 1) Email di benvenuto all'utente
    welcome_subject = "Benvenuto su TrackLab"
    welcome_body = (
        f"Ciao {new_user.username},\n\n"
        f"Il tuo account su TrackLab e' stato creato "
        f"correttamente.\n\n"
        f"  Email: {new_user.email}\n"
        f"  Piano: Base\n\n"
        f"Da ora puoi accedere all'app e iniziare a catalogare la tua "
        f"libreria musicale. Per richiedere l'upgrade a Pro o Advanced "
        f"usa il menu Account dentro l'app.\n\n"
        f"Buon ballo!\n"
        f"— TrackLab"
    )
    background_tasks.add_task(
        send_email, new_user.email, welcome_subject, welcome_body)

    # 2) Notifica admin (solo se ADMIN_NOTIFY_EMAIL e' configurata)
    if _s.ADMIN_NOTIFY_EMAIL:
        admin_subject = f"Nuovo utente registrato: {new_user.email}"
        admin_body = (
            f"Nuova registrazione self-service su TrackLab Server:\n\n"
            f"  Email:    {new_user.email}\n"
            f"  Username: {new_user.username}\n"
            f"  Piano:    {new_user.plan}\n"
            f"  ID:       {new_user.id}\n"
        )
        background_tasks.add_task(
            send_email, _s.ADMIN_NOTIFY_EMAIL, admin_subject, admin_body)

    return _user_to_public(new_user)


# ── v0.1.8 — Registration open/closed flag ──────────────────────
# Gestito tramite file sentinella `data/registration_disabled.flag`.
# Default: registrazione APERTA (file assente). L'admin può chiudere
# o riaprire tramite endpoint dedicati.
def _registration_flag_path():
    from ..config import settings
    return (settings.DATA_DIR / "registration_disabled.flag").resolve()


def _is_registration_open() -> bool:
    """True se la registrazione self-service è abilitata."""
    return not _registration_flag_path().exists()


class RegistrationStatus(BaseModel):
    enabled: bool
    message: str = ""


@router.get("/registration/status", response_model=RegistrationStatus)
def registration_status():
    """Pubblico: indica se la registrazione è aperta. Usato dal client
    per mostrare/nascondere il link 'Registrati' nella login window."""
    if _is_registration_open():
        return RegistrationStatus(enabled=True,
            message="Registrazione self-service attiva")
    return RegistrationStatus(enabled=False,
        message="Registrazione disabilitata. Contatta l'amministratore.")


@router.post("/admin/registration/disable", response_model=RegistrationStatus)
def admin_disable_registration(
    admin: User = Depends(require_admin),
):
    """Admin: disabilita la registrazione self-service. I client non
    potranno più creare nuovi account. L'admin può sempre creare
    account manualmente via DB o tramite endpoint admin.

    v0.2.2 (S8): usa require_admin dependency invece del check manuale
    (piu' robusto: impossibile dimenticare il check)."""
    p = _registration_flag_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    import datetime as _dt
    p.write_text(
        f"Disabilitata da {admin.email} il "
        f"{_dt.datetime.utcnow().isoformat()}\n",
        encoding="utf-8",
    )
    return RegistrationStatus(enabled=False,
        message="Registrazione disabilitata")


@router.post("/admin/registration/enable", response_model=RegistrationStatus)
def admin_enable_registration(
    admin: User = Depends(require_admin),
):
    """Admin: riabilita la registrazione self-service.
    v0.2.2 (S8): require_admin dependency."""
    p = _registration_flag_path()
    if p.exists():
        try:
            p.unlink()
        except Exception as e:
            raise HTTPException(500, f"Impossibile abilitare: {e}")
    return RegistrationStatus(enabled=True,
        message="Registrazione abilitata")


# ── v0.1.9 — Admin: creazione utente manuale + statistiche ─────
class AdminCreateUser(BaseModel):
    """Schema per creazione utente da pannello admin (bypass register
    self-service). Pratico per pilot privato dove l'admin crea gli
    account ai clienti senza far passare da self-service."""
    email:    EmailStr
    username: str = Field(..., min_length=2, max_length=64)
    password: str = Field(..., min_length=8, max_length=128)
    plan:     str = Field("base", pattern="^(base|pro|advanced)$")
    is_admin: bool = False


@router.post("/admin/users", response_model=UserPublic,
             status_code=status.HTTP_201_CREATED)
def admin_create_user(
    body:  AdminCreateUser,
    admin: User = Depends(require_admin),
    db:    Session = Depends(get_db),
):
    """Admin crea un utente con piano e ruolo arbitrari.
    v0.2.2 (S8): require_admin dependency."""

    # Anche qui validiamo la password (l'admin potrebbe pigramente
    # mettere "password" come default). La policy si applica a tutti.
    from ..services.password_policy import validate_password
    pwd_error = validate_password(
        body.password, email=body.email, username=body.username)
    if pwd_error:
        raise HTTPException(422, pwd_error)

    # v0.2.2 (S7): normalizza email
    email_norm = (str(body.email) or "").strip().lower()
    if db.query(User).filter(User.email == email_norm).first():
        raise HTTPException(409, "Email già registrata")

    new_user = User(
        email=email_norm,
        username=body.username,
        password_hash=hash_password(body.password),
        plan=body.plan,
        is_admin=body.is_admin,
    )
    db.add(new_user); db.commit(); db.refresh(new_user)

    # Audit log per accountability
    try:
        from ..models.db import AdminAuditLog
        import json as _json
        entry = AdminAuditLog(
            admin_id=admin.id, admin_email=admin.email,
            action="user_created_by_admin",
            target_id=new_user.id, target_email=new_user.email,
            details=_json.dumps({
                "plan": new_user.plan, "is_admin": new_user.is_admin,
            }),
        )
        db.add(entry); db.commit()
    except Exception as e:
        print(f"[audit] log failed: {e}"); db.rollback()

    # v0.1.9: usa l'helper esistente per costruire UserPublic completo
    return _user_to_public(new_user)


# ── v0.2.2 (security-audit S5) — Admin: revoca sessioni utente ──
class RevokeSessionsResponse(BaseModel):
    ok: bool = True
    message: str = ""
    user_id: int
    new_token_version: int


@router.post("/admin/users/{user_id}/revoke-sessions",
             response_model=RevokeSessionsResponse)
def admin_revoke_sessions(
    user_id: int,
    admin: User = Depends(require_admin),
    db:    Session = Depends(get_db),
):
    """Admin: invalida TUTTI i token (access + refresh) di un utente
    incrementando il suo token_version. L'utente dovra' rifare login.

    Caso d'uso: account compromesso, dipendente che lascia, sospetto
    furto credenziali. Prima di v0.2.2 non c'era modo di revocare un
    refresh token (valido 7 giorni) — questo colma la lacuna S5.
    """
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(404, "Utente non trovato")
    target.token_version = getattr(target, "token_version", 0) + 1
    db.commit()
    db.refresh(target)

    # Audit log per accountability
    try:
        from ..models.db import AdminAuditLog
        import json as _json
        entry = AdminAuditLog(
            admin_id=admin.id, admin_email=admin.email,
            action="sessions_revoked",
            target_id=target.id, target_email=target.email,
            details=_json.dumps({"new_token_version": target.token_version}),
        )
        db.add(entry); db.commit()
    except Exception as e:
        print(f"[audit] log failed: {e}"); db.rollback()

    return RevokeSessionsResponse(
        message="Tutte le sessioni dell'utente sono state revocate",
        user_id=target.id,
        new_token_version=target.token_version,
    )
