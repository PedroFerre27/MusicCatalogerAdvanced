#!/bin/bash
# backup-db.sh — Backup quotidiano DB TrackLab Server su Synology NAS.
#
# v0.2.6 (R14): migrazione path NAS completata.
#   /volume1/docker/music-cataloger -> /volume1/docker/tracklab
#
# Cosa fa:
#   1. Copia /volume1/docker/tracklab/data/app.db
#      in /volume1/docker/tracklab/data/backups/app-YYYY-MM-DD.db
#   2. Comprime con gzip per risparmiare spazio
#   3. Cancella backup più vecchi di KEEP_DAYS (default 30)
#   4. Logga ogni run in /volume1/docker/tracklab/data/backups/backup.log
#
# Setup cron (DSM Synology) — DOPO la migrazione path:
#   1. Carica questo file in /volume1/docker/tracklab/scripts/backup-db.sh
#   2. chmod +x /volume1/docker/tracklab/scripts/backup-db.sh
#   3. DSM → Pannello di controllo → Utilità di pianificazione →
#      MODIFICA il task "DB Backup TrackLab" esistente:
#         Impostazioni attività: comando =
#            /volume1/docker/tracklab/scripts/backup-db.sh
#         (era /volume1/docker/music-cataloger/scripts/backup-db.sh)
#   4. (opzionale) Email su errore: nel pannello "Notifica" inserisci
#      la tua email per ricevere alert se il backup fallisce.
#
# Test manuale:
#   sudo /volume1/docker/tracklab/scripts/backup-db.sh
#   ls -lh /volume1/docker/tracklab/data/backups/

set -euo pipefail

# Config (v0.2.6 — R14 migrazione path completata)
DATA_DIR="/volume1/docker/tracklab/data"
DB_FILE="${DATA_DIR}/app.db"
BACKUP_DIR="${DATA_DIR}/backups"
LOG_FILE="${BACKUP_DIR}/backup.log"
KEEP_DAYS=30        # giorni di retention dei backup

# Setup
mkdir -p "${BACKUP_DIR}"
TODAY=$(date +%Y-%m-%d)
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
DEST="${BACKUP_DIR}/app-${TODAY}.db"

log() {
    echo "[${TIMESTAMP}] $*" | tee -a "${LOG_FILE}"
}

# Check sorgente esiste
if [ ! -f "${DB_FILE}" ]; then
    log "ERROR: ${DB_FILE} non esiste — backup saltato"
    exit 1
fi

# Per backup consistente di un DB SQLite in uso, l'ideale sarebbe
# usare il comando `sqlite3 .backup` che gestisce le transazioni.
# Senza sqlite3 installato sul NAS Synology di base usiamo cp:
# il rischio è basso perché il server FastAPI fa singoli commit
# rapidi (<10ms), quindi una copia bytewise è quasi sempre coerente.
# Se vuoi essere paranoico puoi installare sqlite3 via Entware.
if command -v sqlite3 >/dev/null 2>&1; then
    log "Backup con sqlite3 .backup (transazione-safe)"
    sqlite3 "${DB_FILE}" ".backup '${DEST}'"
else
    log "Backup con cp (sqlite3 non installato — rischio basso accettato)"
    cp "${DB_FILE}" "${DEST}"
fi

# Comprimi
gzip -f "${DEST}"
DEST_GZ="${DEST}.gz"
SIZE=$(du -h "${DEST_GZ}" | cut -f1)
log "Backup creato: ${DEST_GZ} (${SIZE})"

# Pulisci vecchi
DELETED=$(find "${BACKUP_DIR}" -name "app-*.db.gz" -type f -mtime +${KEEP_DAYS} -print -delete | wc -l)
if [ "${DELETED}" -gt 0 ]; then
    log "Rimossi ${DELETED} backup più vecchi di ${KEEP_DAYS} giorni"
fi

# Anche backup di altri file critici (snapshot completo)
# - registration_disabled.flag (stato registrazione)
# - caribbean_defaults.json (default condivisi)
# - version.json (release manifest)
# Questi sono piccoli, li includiamo in un tarball giornaliero
EXTRAS_TAR="${BACKUP_DIR}/extras-${TODAY}.tar.gz"
EXTRAS=()
for f in registration_disabled.flag caribbean_defaults.json version.json; do
    if [ -f "${DATA_DIR}/${f}" ]; then
        EXTRAS+=("${f}")
    fi
done
if [ ${#EXTRAS[@]} -gt 0 ]; then
    tar -czf "${EXTRAS_TAR}" -C "${DATA_DIR}" "${EXTRAS[@]}" 2>/dev/null || true
    log "Snapshot extras: ${EXTRAS_TAR} (${#EXTRAS[@]} file)"
fi
find "${BACKUP_DIR}" -name "extras-*.tar.gz" -type f -mtime +${KEEP_DAYS} -delete 2>/dev/null || true

log "Backup OK"
exit 0
