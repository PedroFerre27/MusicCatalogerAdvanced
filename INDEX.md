# 📦 Music Cataloger Advanced - Docker Package

## 🎯 Benvenuto!

Questo package contiene tutto il necessario per eseguire Music Cataloger Advanced in Docker.

---

## 📁 Contenuto Package

### 🔧 File di Configurazione (ESSENZIALI)

1. **Dockerfile** - Definizione immagine Docker
2. **docker-compose.yml** - ⚠️ DA MODIFICARE con il tuo path musica!
3. **docker-entrypoint.sh** - Script avvio container
4. **requirements.txt** - Dipendenze Python
5. **.dockerignore** - File esclusi dal build

### 🚀 Script Helper

6. **docker-helper.bat** - Helper Windows per comandi semplificati
7. **test-docker-setup.bat** - Test setup Windows
8. **test-docker-setup.sh** - Test setup Linux/Mac
9. **Makefile** - Helper Linux/Mac (opzionale)

### 📖 Documentazione

10. **QUICKSTART_DOCKER.md** - ⭐ **INIZIA DA QUI!** Setup in 3 passi
11. **README_DOCKER.md** - Documentazione completa e dettagliata
12. **DOCKER_ARCHITECTURE.md** - Architettura e diagrammi tecnici
13. **CHECKLIST_DOCKER.md** - Checklist per verificare setup
14. **INDEX.md** - Questo file (navigazione)

---

## 🚀 Quick Start (3 Passi)

### 1️⃣ Estrai tutto nella root del progetto

```
C:\Users\pmarquesf\Desktop\Pedro\Music Cataloger\
├── Dockerfile              ← Dal package
├── docker-compose.yml      ← Dal package
├── docker-entrypoint.sh    ← Dal package
├── requirements.txt        ← Dal package (o usa quello esistente)
├── .dockerignore          ← Dal package
├── docker-helper.bat      ← Dal package
├── test-docker-setup.bat  ← Dal package
│
├── MusicCatalogerAdvanced_v0020.py  ← Esistente
├── run_gui.py                        ← Esistente
├── config/                           ← Esistente
├── core/                             ← Esistente
├── gui/                              ← Esistente
├── services/                         ← Esistente
└── utils/                            ← Esistente
```

### 2️⃣ Configura Path Musica

Apri `docker-compose.yml` e cambia:
```yaml
volumes:
  # CAMBIA QUESTO!
  - C:/Users/pmarquesf/Music:/music:ro
```

### 3️⃣ Build & Run

```cmd
REM Test setup
test-docker-setup.bat

REM Build
docker-helper.bat build

REM Run (simulazione)
docker-helper.bat run
```

✅ **Fatto!** Vai a `./output/` per vedere i risultati.

---

## 📖 Quale Documentazione Leggere?

### 🆕 Prima Volta?
→ **QUICKSTART_DOCKER.md** - Setup rapido in 3 passi

### 🔍 Vuoi tutti i dettagli?
→ **README_DOCKER.md** - Guida completa con esempi

### 🏗️ Vuoi capire l'architettura?
→ **DOCKER_ARCHITECTURE.md** - Diagrammi e struttura tecnica

### ✅ Vuoi verificare tutto?
→ **CHECKLIST_DOCKER.md** - Checklist passo-passo

### ❓ Problemi o dubbi?
→ **README_DOCKER.md** sezione Troubleshooting

---

## 🎯 Workflow Consigliato

```
1. Leggi: QUICKSTART_DOCKER.md
   ↓
2. Estrai file nella root progetto
   ↓
3. Esegui: test-docker-setup.bat
   ↓
4. Modifica: docker-compose.yml (path musica)
   ↓
5. Esegui: docker-helper.bat build
   ↓
6. Esegui: docker-helper.bat run (dry-run)
   ↓
7. Verifica: ./output/
   ↓
8. Se OK: docker-helper.bat run-real
   ↓
9. ✅ Usa la checklist per verifiche finali
```

---

## 🛠️ Comandi Più Usati

### Windows (con docker-helper.bat)

```cmd
docker-helper.bat help         # Mostra tutti i comandi
docker-helper.bat build        # Build immagine
docker-helper.bat run          # Dry-run (simulazione)
docker-helper.bat run-real     # Catalogazione reale
docker-helper.bat shell        # Shell interattiva
docker-helper.bat salsa        # Solo Salsa/Bachata
docker-helper.bat clean        # Pulizia
```

### Linux/Mac (con Makefile)

```bash
make help          # Mostra tutti i comandi
make build         # Build immagine
make run           # Dry-run (simulazione)
make run-real      # Catalogazione reale
make shell         # Shell interattiva
make salsa         # Solo Salsa/Bachata
make clean         # Pulizia
```

### Docker Compose Manuale

```bash
docker-compose build                                      # Build
docker-compose run --rm music-cataloger cli /music --dry-run -v  # Dry-run
docker-compose run --rm music-cataloger cli /music -v            # Run reale
docker-compose run --rm music-cataloger bash                     # Shell
```

---

## 📊 Struttura Output

Dopo l'esecuzione troverai:

```
./output/
├── cataloging_report_YYYYMMDD_HHMMSS.json   # Report statistiche
├── mp3_cataloger_advanced_YYYYMMDD.log      # Log dettagliato
└── ... (altri file generati)

./cache/
└── metadata_cache.json                       # Cache per velocizzare
```

---

## ⚠️ Note Importanti

### Prima di Iniziare
- ✅ Fai **BACKUP** della cartella musica
- ✅ Installa **Docker Desktop**
- ✅ Assicurati che Docker sia **in esecuzione**

### Durante l'Uso
- 🔒 `/music` è **read-only** per sicurezza
- ⚡ Prima esecuzione **lenta** (download immagini)
- 🖥️ **GUI non supportata** su Docker Windows

### Limitazioni
- ❌ GUI Tkinter non funziona in Docker Windows
- ❌ Richiede Docker Desktop (non funziona con podman out-of-the-box)
- ⚠️ Prima build richiede ~3-4 minuti

---

## 🆘 Problemi Comuni

### "Docker daemon not running"
→ Avvia Docker Desktop

### "Cannot access /music"
→ Verifica path in `docker-compose.yml`
→ Docker Desktop > Settings > File Sharing

### "Build failed"
→ Esegui `docker system prune -a`
→ Riprova con `docker-compose build --no-cache`

### Container lento
→ Usa `--no-external-apis` per velocizzare
→ Aumenta CPU/RAM in Docker Desktop Settings

**Per altri problemi**: Consulta sezione Troubleshooting in `README_DOCKER.md`

---

## 📚 Risorse Aggiuntive

- **Repository GitHub**: https://github.com/PedroFerre27/MusicCatalogerAdvanced
- **Docker Docs**: https://docs.docker.com
- **Docker Compose**: https://docs.docker.com/compose/

---

## 🎉 Setup Completato?

Usa la **CHECKLIST_DOCKER.md** per verificare che tutto sia OK!

---

## 📞 Supporto

Per problemi o domande:
1. Consulta `README_DOCKER.md` (Troubleshooting)
2. Verifica `CHECKLIST_DOCKER.md` 
3. Rivedi `QUICKSTART_DOCKER.md`
4. Apri issue su GitHub

---

## 📝 Version Info

- **Package Version**: 1.0.0
- **App Version**: 0.0.2.0
- **Docker Base**: Python 3.11-slim
- **Data Package**: 2024-11-19

---

**Buon lavoro con Music Cataloger Advanced! 🎵**
