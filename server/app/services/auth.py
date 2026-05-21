"""
services/auth.py — Autenticazione JWT + bcrypt

Due tipi di token:
- access_token: 15 min, contiene {sub, email, plan, is_admin}
- refresh_token: 7 giorni, contiene {sub, token_type="refresh"}

Il client chiama /auth/login con email+password → riceve entrambi.
Quando l'access scade (401), il client chiama /auth/refresh col refresh.
Se anche il refresh scade, l'utente fa login di nuovo.
"""
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from ..config import settings
from ..models.db import User, get_db


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)


def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_access_token(user: User) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    payload = {
        "sub":      str(user.id),
        "email":    user.email,
        "username": user.username,
        "plan":     user.plan,
        "is_admin": user.is_admin,
        # v0.2.2 (S4): token version — invalidato al change-password
        "tv":       getattr(user, "token_version", 0),
        "exp":      expire,
        "type":     "access",
    }
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def create_refresh_token(user: User) -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    payload = {
        "sub":  str(user.id),
        # v0.2.2 (S5): anche il refresh porta tv → revocabile
        "tv":   getattr(user, "token_version", 0),
        "exp":  expire,
        "type": "refresh",
    }
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token non valido: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ── Dependency FastAPI ────────────────────────────────────────────
def get_current_user(
    token: Optional[str] = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
) -> User:
    """Inject: estrae l'utente corrente dal JWT passato in Authorization header."""
    if token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenziali richieste",
            headers={"WWW-Authenticate": "Bearer"},
        )
    payload = decode_token(token)
    if payload.get("type") != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Questo endpoint richiede un access token, non un refresh",
        )
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Token malformato")
    user = db.query(User).filter(User.id == int(user_id)).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="Utente non trovato o disattivato")
    # v0.2.2 (S4): se il token_version del JWT non combacia con quello
    # corrente dell'utente, il token e' stato invalidato (password
    # cambiata o sessioni revocate da admin). Forza re-login.
    token_tv = payload.get("tv", 0)
    if token_tv != getattr(user, "token_version", 0):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sessione scaduta (credenziali modificate). Effettua di nuovo il login.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    """Dependency per endpoint admin-only."""
    if not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Permessi amministratore richiesti",
        )
    return user


# ── v0.2.2 (security-audit S3) — Plan gating server-side ──────────
# Gerarchia: advanced ⊃ pro ⊃ base
_PLAN_RANK = {"base": 0, "pro": 1, "advanced": 2}


def require_plan(minimum: str):
    """Dependency factory: l'utente deve avere il piano `minimum` o
    superiore, altrimenti 403.

    Questo e' il CUORE dell'intera security audit: il plan check
    client-side e' solo UX (bypassabile). La vera autorita' e' qui.
    Un client manomesso che chiama un endpoint plan-gated viene
    respinto dal server.

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
                detail=(f"Funzionalità riservata al piano '{minimum}' o "
                        f"superiore. Il tuo piano attuale è '{user.plan}'."),
            )
        # Controllo scadenza piano (plan_expires_at None = no scadenza)
        if user.plan_expires_at is not None:
            if user.plan_expires_at < datetime.utcnow().replace(tzinfo=None):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Il tuo piano è scaduto. Rinnova per continuare.",
                )
        return user

    return _checker
