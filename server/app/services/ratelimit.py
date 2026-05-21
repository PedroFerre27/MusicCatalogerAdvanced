"""
services/ratelimit.py — Rate limiter condiviso (v0.2.2, security-audit S1)

Estratto in un modulo separato per evitare l'import circolare:
main.py importa api.auth (per i router), e api.auth ha bisogno del
`limiter` per decorare /login. Se `limiter` vivesse in main.py si
creerebbe main → auth → main.

Il server e' dietro reverse proxy DSM Synology. get_remote_address
vedrebbe sempre 127.0.0.1 (il proxy). Leggiamo X-Forwarded-For per
avere l'IP reale del client.
"""
from fastapi import Request
from slowapi import Limiter
from slowapi.util import get_remote_address


def _real_ip(request: Request) -> str:
    """IP reale del client anche dietro reverse proxy DSM."""
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        # Primo IP della catena = client originale
        return xff.split(",")[0].strip()
    return get_remote_address(request)


# Istanza condivisa importata sia da main.py (registrazione) sia da
# api/auth.py (decoratore @limiter.limit su /login)
limiter = Limiter(key_func=_real_ip)
