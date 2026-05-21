# Versioning & rollback — Music Cataloger Advanced

Pedro ha già un repo GitHub:
**https://github.com/PedroFerre27/MusicCatalogerAdvanced**

Questo documento ti dice come usarlo bene per gestire le versioni in
produzione (= che il tuo amico Linux usa) senza più tenere ZIP sul
desktop.

## Strategia in breve

- **Branch `main`**: ultima versione che funziona, sempre buildabile
- **Tag `v1085m-stable`**: snapshot di una release pubblicata sul NAS
  (= che gli utenti vedono via auto-update)
- **Branch `dev/<feature>`**: lavoro sperimentale, NON visto dagli utenti
- **GitHub Releases**: pacchetti EXE (Windows/Linux/macOS) attaccati al tag

## Setup primo volta (se non l'hai già fatto)

```cmd
cd C:\dev\music-cataloger
git init
git remote add origin https://github.com/PedroFerre27/MusicCatalogerAdvanced.git
git config user.name "Pedro"
git config user.email "captainjoker27@gmail.com"

REM .gitignore — escludi cose che NON vanno nel repo
(
echo dist/
echo build/
echo data/
echo __pycache__/
echo *.pyc
echo *.spec.bak
echo .venv/
echo /icons/music_cataloger.ico
echo *.log
) > .gitignore

REM Verifica cosa stai per committare
git status

git add .
git commit -m "v1085m baseline pilot"
git push -u origin main
git tag v1085m-stable
git push origin v1085m-stable
```

## Workflow per ogni release riuscita

Una "release" = una versione che hai testato, buildato, caricato sul
NAS, e che il tuo amico (o gli utenti del pilot) può usare.

```cmd
REM 1. Verifica di essere su main e di aver committato tutto
git status
git checkout main

REM 2. Bump versione nei file
REM    Modifica version.py:        APP_VERSION = "v1086"
REM    Modifica version_info.txt:  filevers=(1, 0, 86, 0)
REM                                 FileVersion u'1.0.86.0'

REM 3. Build e test locale
python build_ico.py
pyinstaller music_cataloger.spec --clean --noconfirm
REM Lancia dist\Music Cataloger Advanced.exe e verifica funzioni

REM 4. Commit + tag
git add .
git commit -m "v1086 - <breve descrizione delle fix>"
git tag v1086-stable -m "Pilot release v1086: fix X, Y, Z"
git push origin main
git push origin v1086-stable

REM 5. Carica su NAS
copy "dist\Music Cataloger Advanced.exe" \\NAS\path\Music_Cataloger_v1086.exe
REM Calcola SHA256
certutil -hashfile "dist\Music Cataloger Advanced.exe" SHA256

REM 6. Pubblica manifest via API admin
SET SECRET=<la SECRET_KEY del server>
curl -X POST https://api.choros27.synology.me/admin/version/publish ^
  -H "X-Admin-Token: %SECRET%" ^
  -H "Content-Type: application/json" ^
  -d "{\"version\":\"v1086\",\"filename\":\"Music_Cataloger_v1086.exe\",\"sha256\":\"...\",\"changelog\":\"- ...\",\"mandatory\":false}"
```

## Rollback in caso di regressione

Caso tipico: hai pubblicato v1086 e l'amico segnala che è rotta.

```cmd
REM Vedi i tag stabili
git tag

REM Torna a versione precedente che sai funzionare
git checkout v1085m-stable

REM Rebuild EXE da quella versione
python build_ico.py
pyinstaller music_cataloger.spec --clean --noconfirm

REM Ri-pubblica come "v1086_hotfix" (NON sovrascrivere v1086, lasciala per debug)
copy "dist\Music Cataloger Advanced.exe" \\NAS\...\Music_Cataloger_v1086_hotfix.exe

REM Pubblica manifest con stessa version v1086 ma puntando al nuovo file
curl -X POST .../admin/version/publish -d "{\"version\":\"v1086\",\"filename\":\"Music_Cataloger_v1086_hotfix.exe\", ...}"

REM Quando torni a sviluppare:
git checkout main
```

## Diff tra due versioni

```cmd
git log --oneline                      REM lista commit
git diff v1085m-stable v1086-stable    REM cosa è cambiato in totale
git diff v1085m-stable -- gui/main_window.py    REM solo un file
```

## Branch per esperimenti rischiosi

Quando devi fare un cambio rischioso (refactor major, nuova feature,
migrazione DB), usa un branch così se va male torni indietro
istantaneamente:

```cmd
git checkout -b dev/postgresql-migration
REM ... lavora, committa frequentemente ...

REM Build dal branch e testa in isolato
pyinstaller music_cataloger.spec --clean --noconfirm
REM (NON pubblicare sul NAS — questa è ancora WIP)

REM Quando sei sicuro:
git checkout main
git merge dev/postgresql-migration
git push origin main

REM Se vuoi buttare via tutto:
git checkout main
git branch -D dev/postgresql-migration
```

## GitHub Releases (opzionale ma utile)

Per ogni tag stabile puoi creare una "Release" su GitHub via web:
1. https://github.com/PedroFerre27/MusicCatalogerAdvanced/releases
2. "Draft a new release"
3. Tag: `v1086-stable`
4. Title: "Music Cataloger v1086"
5. Description: changelog
6. **Attach binaries**: trascina il `.exe` Windows + binario Linux + .app macOS
7. "Publish release"

Vantaggio: il tuo amico Linux può scaricare il binario direttamente
da GitHub senza passare dal NAS, e tu hai una storia visiva delle
release.

## Cosa NON committare mai

- `data/` — dati personali, sessioni, cache
- `dist/`, `build/` — output PyInstaller (rigenerati da source)
- `*.exe`, `*.dll` — binari (vanno in GitHub Releases, non in repo)
- File con secret/password
- `.venv/` — virtual environment

Il `.gitignore` di sopra li copre.

## Backup di sicurezza

GitHub stesso è il tuo backup. Però tieni una copia ZIP del tuo
ultimo build EXE su disco esterno o cloud (es. OneDrive personale,
NON quello aziendale Indra) — se GitHub va giù mentre stai per
fare un demo critico hai una via di fuga.

## Mai più ZIP sul desktop

Una volta che hai questo workflow, butta gli ZIP che hai accumulato
sul desktop. Ogni versione precedente è recuperabile con:

```cmd
git checkout v1085i-stable
```

E in 30 secondi hai il sorgente di v1085i pronto per buildare.
