# Dockerfile — Music Cataloger Server
#
# Target: Synology DS415+ (Intel Atom C2538, amd64, DSM 7.1.1)
# Container Manager di DSM è basato su Docker standard — nessuna
# modifica necessaria. Build & run via Portainer o CLI SSH.
#
# Dimensione finale attesa: ~150 MB (python:3.11-slim + deps minime)

FROM python:3.11-slim

# ── Metadata OCI ──────────────────────────────────────────────────
LABEL org.opencontainers.image.title="Music Cataloger Server"
LABEL org.opencontainers.image.description="FastAPI backend per Music Cataloger Advanced"
LABEL org.opencontainers.image.source="https://github.com/PedroFerre27"

# ── System deps (bcrypt, psycopg2 se un domani si migra a postgres) ──
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps ───────────────────────────────────────────────────
WORKDIR /srv/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── App code ──────────────────────────────────────────────────────
COPY app/ ./app/

# ── Runtime config ────────────────────────────────────────────────
# Non-root user per sicurezza (DSM spesso lo raccomanda).
# UID/GID fissi a 1000 così chown sul NAS è prevedibile:
#   sudo chown -R 1000:1000 /volume1/docker/music-cataloger/data
RUN groupadd -r -g 1000 mcs \
    && useradd --create-home --shell /bin/bash -u 1000 -g 1000 mcs \
    && mkdir -p /srv/app/data /output \
    && chown -R mcs:mcs /srv/app /output
USER mcs

# Volumi dove il container scrive
# /srv/app/data   → SQLite + upload utente
# /music          → libreria musica (read-write per catalogazione)
# /output         → report JSON e CSV generati
VOLUME ["/srv/app/data", "/music", "/output"]

EXPOSE 8020

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8020/health || exit 1

# Single worker in MVP — il DS415+ con 4 core Atom regge bene; per
# aumentare concorrenza più avanti si userà gunicorn con 2 worker.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8020"]
