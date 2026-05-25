#!/bin/bash
#
# deploy-nas.sh — Build + run del container TrackLab Server sul NAS
# Synology DS415+
#
# Questo script non usa docker-compose (che potrebbe non essere
# installato su DSM 7.1.1 standard). Usa solo "docker" V1.
#
# Prerequisiti:
#   - Stare nella cartella server/ del repo TrackLab
#   - Aver creato /volume1/docker/music-cataloger/{data,output}
#   - File .env presente con SECRET_KEY, ADMIN_EMAIL, ADMIN_PASSWORD
#
# Uso:
#   chmod +x deploy-nas.sh
#   sudo ./deploy-nas.sh
#
# v0.2.5 (R5 rebrand "TrackLab"): IMAGE_NAME e CONTAINER_NAME cambiano
# da "music-cataloger-server" a "tracklab-server". Al PRIMO deploy
# dopo l'upgrade, il vecchio container resta orfano:
#   docker stop music-cataloger-server || true
#   docker rm   music-cataloger-server || true
#   docker rmi  music-cataloger-server:latest || true
# Lo script nuovo creera' il container "tracklab-server" che monta
# gli stessi path "/volume1/docker/music-cataloger/..." — il DB e
# i dati utente NON vengono toccati (path filesystem invariati).
# La migrazione dei path da "music-cataloger" a "tracklab" sotto
# /volume1/docker/ e' rimandata a un branch dedicato (vedi ROADMAP).

set -euo pipefail

IMAGE_NAME="tracklab-server:latest"
CONTAINER_NAME="tracklab-server"

# Path sul NAS — INTENZIONALMENTE invariati nel rebrand R5 perche'
# contengono il DB e i dati degli utenti esistenti. Migrazione a
# /volume1/docker/tracklab/... rimandata a un branch dedicato.
DATA_DIR="/volume1/docker/music-cataloger/data"
OUTPUT_DIR="/volume1/docker/music-cataloger/output"
MUSIC_DIR="/volume1/Multimedia/Musica"

# ── 1. Verifica prerequisiti ──────────────────────────────────────
if [ ! -f .env ]; then
    echo "ERRORE: file .env mancante. Copia da .env.example e compila."
    exit 1
fi
if [ ! -f Dockerfile ]; then
    echo "ERRORE: eseguire dentro la cartella music-cataloger-server/"
    exit 1
fi

echo "── [1/4] Creazione cartelle host (se non esistono) ──"
mkdir -p "$DATA_DIR" "$OUTPUT_DIR"
chmod 755 "$DATA_DIR" "$OUTPUT_DIR"

echo "── [2/4] Build immagine ${IMAGE_NAME} ──"
docker build -t "$IMAGE_NAME" .

echo "── [3/4] Stop + rimozione container esistente (se presente) ──"
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    docker stop "$CONTAINER_NAME" || true
    docker rm "$CONTAINER_NAME"  || true
    echo "  Container precedente rimosso"
fi

echo "── [4/4] Avvio nuovo container ──"
docker run -d \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    --env-file .env \
    -v "${DATA_DIR}:/srv/app/data" \
    -v "${MUSIC_DIR}:/music:rw" \
    -v "${OUTPUT_DIR}:/output" \
    -p 8020:8020 \
    "$IMAGE_NAME"

echo ""
echo "=== Container avviato ==="
echo ""
docker ps --filter "name=${CONTAINER_NAME}" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
echo ""
echo "Test:"
echo "  curl http://localhost:8020/health"
echo ""
echo "Log:"
echo "  docker logs -f ${CONTAINER_NAME}"
echo ""
