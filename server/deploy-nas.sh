#!/bin/bash
#
# deploy-nas.sh — Build + run del container sul NAS Synology DS415+
#
# Questo script non usa docker-compose (che potrebbe non essere
# installato su DSM 7.1.1 standard). Usa solo "docker" V1.
#
# Prerequisiti:
#   - Stare nella cartella music-cataloger-server/
#   - Aver creato /volume1/docker/music-cataloger/{data,output}
#   - File .env presente con SECRET_KEY, ADMIN_EMAIL, ADMIN_PASSWORD
#
# Uso:
#   chmod +x deploy-nas.sh
#   sudo ./deploy-nas.sh

set -euo pipefail

IMAGE_NAME="music-cataloger-server:latest"
CONTAINER_NAME="music-cataloger-server"

# Path sul NAS — cambia se diversi
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
