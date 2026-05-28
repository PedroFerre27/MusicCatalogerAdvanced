# tracklab_macos.spec — Build macOS (Intel + Apple Silicon)
#
# USAGE su una macchina macOS:
#   pip install pyinstaller customtkinter Pillow eyed3 mutagen requests pyjwt
#   pyinstaller tracklab_macos.spec --clean --noconfirm
#
# Output:
#   dist/TrackLab.app   (bundle .app)
#
# Lo spec genera anche l'icona .icns da PNG se manca (vedi blocco
# inizio file). Su Linux/macOS si usa `iconutil` (macOS) o Pillow
# (cross-platform). Il workflow GitHub Actions `build-macos.yml`
# automatizza tutto.
#
# IMPORTANTE per gli amici di Pedro (R9 v1095.0):
# - L'app NON è firmata col certificato Apple Developer ($99/anno).
#   Al primo avvio macOS bloccherà con "App di sviluppatore non
#   identificato". Per aprirla la prima volta:
#       Click destro sull'app → Apri → Apri (conferma)
#   Da lì in poi macOS la ricorda.
# - Firma + notarization saranno aggiunte in R19 (pagamenti) quando
#   il costo Apple Dev (~99 €/anno) è giustificato dal lancio
#   commerciale.
#
# Apple Silicon vs Intel:
# - PyInstaller fa build per l'architettura della macchina su cui gira
# - GitHub Actions usa `macos-14` runner → Apple Silicon (arm64).
#   Per supportare anche Intel servirebbe un secondo job con
#   `macos-13` (Intel) o usare `target_arch="universal2"` (richiede
#   tutte le wheel universal2, fragile).
# - Per ora: build single-arch arm64. Se servirà Intel, aggiungere
#   un job parallelo nel workflow.

import sys
from pathlib import Path

block_cipher = None
project_root = Path('.').absolute()

# v1095.0 (R9): legge la versione da version.py per non hard-coding
# nel bundle Info.plist.
try:
    _version_globals = {}
    exec((project_root / 'version.py').read_text(encoding='utf-8'),
         _version_globals)
    _APP_VERSION = _version_globals.get('APP_VERSION', 'v0.0.0').lstrip('v')
    # CFBundleShortVersionString richiede formato N.N.N(.N)
    # version.py usa "v1095.0" → estraggo i numeri
    _parts = _APP_VERSION.replace('.', '_').split('_')
    if len(_parts) >= 2 and _parts[0].isdigit():
        _SHORT_VER = f"1.0.{_parts[0]}.{_parts[1]}"
    else:
        _SHORT_VER = "1.0.0.0"
except Exception:
    _APP_VERSION = "0.0.0"
    _SHORT_VER = "1.0.0.0"

# v1095.0 (R9): genera icons/tracklab.icns dal PNG sorgente se manca.
# Pillow funziona cross-platform (no `iconutil` macOS-only required).
_icns_path = project_root / 'icons' / 'tracklab.icns'
_src_png = project_root / 'icons' / 'app' / 'app_icon_256.png'
if not _icns_path.exists() and _src_png.exists():
    print(f"[spec macos] {_icns_path} non trovato, lo genero da {_src_png}")
    try:
        from PIL import Image
        img = Image.open(str(_src_png)).convert("RGBA")
        # Pillow supporta save in .icns con dimensioni multiple.
        # macOS richiede 16, 32, 64, 128, 256, 512, 1024 (subset OK)
        sizes = [(16,16), (32,32), (64,64), (128,128), (256,256), (512,512)]
        _icns_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(str(_icns_path), format='ICNS', sizes=sizes)
        print(f"[spec macos] {_icns_path} generato OK")
    except Exception as e:
        print(f"[spec macos] WARNING: impossibile generare .icns: {e}")
        _icns_path = None
elif not _icns_path.exists():
    print(f"[spec macos] WARNING: nessuna icona disponibile "
          f"({_icns_path} e {_src_png} mancanti)")
    _icns_path = None

# v1095.1 (R9 hotfix): UPGRADES.md e' gitignored (doc interna). Sul
# cloud runner GitHub Actions il file NON esiste → pyinstaller falliva
# con "Unable to find docs/UPGRADES.md". Soluzione: bundlalo solo se
# esiste sul filesystem (in locale per Pedro c'e', sul runner no).
# Il dialog Help → Changelog gestisce gia' graceful il file mancante
# con un messaggio fallback (vedi _show_help_changelog in main_window).
_datas_list = [
    ('icons',                  'icons'),
    ('config',                 'config'),
    ('services',               'services'),
    ('core',                   'core'),
    ('gui',                    'gui'),
    ('translations',           'translations'),   # R6.0: bundle i18n
    ('run_cataloger.py',       '.'),
    ('version.py',             '.'),
]
_upgrades_md = project_root.parent / 'docs' / 'UPGRADES.md'
if _upgrades_md.exists():
    _datas_list.append(('../docs/UPGRADES.md', '.'))
    print(f"[spec macos] UPGRADES.md bundlato")
else:
    print(f"[spec macos] UPGRADES.md non trovato ({_upgrades_md}) → "
          f"skip (il dialog Help mostrera' messaggio 'changelog non "
          f"disponibile')")

a = Analysis(
    ['run_gui.py'],
    pathex=[],
    binaries=[],
    datas=_datas_list,
    hiddenimports=[
        'customtkinter',
        'PIL._tkinter_finder',
        'PIL.Image',
        'PIL.ImageTk',
        'eyed3',
        'mutagen',
        'mutagen.mp3',
        'requests',
        'urllib3',
        # 'jwt' rimosso in v1096.2 — il client decodifica JWT manuale
        # (base64+json), non usa PyJWT. Era residuo morto.
        'services.i18n',                    # R6.0
        'services.spotify_oauth',
        'services.spotify_store',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='TrackLab',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,         # UPX talvolta rompe binari macOS firmati
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    # target_arch=None → architettura della macchina build
    # Per universal2: target_arch="universal2"
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

# ── BUNDLE .app (specifico macOS) ─────────────────────────────
# Senza questo blocco PyInstaller produce solo un binario, non un
# bundle aprible da Finder. Il BUNDLE wrappa l'EXE in una struttura
# TrackLab.app/
#   Contents/
#     Info.plist
#     MacOS/TrackLab
#     Resources/icon.icns
app = BUNDLE(
    exe,
    name='TrackLab.app',
    icon=str(_icns_path) if _icns_path else None,
    bundle_identifier='com.pedromarques.tracklab',
    info_plist={
        'CFBundleShortVersionString': _SHORT_VER,
        'CFBundleVersion':            _SHORT_VER,
        'CFBundleName':               'TrackLab',
        'CFBundleDisplayName':        'TrackLab',
        'NSHighResolutionCapable':    True,
        'LSMinimumSystemVersion':     '11.0',   # Big Sur+ (2020+)
        # Necessario per Tk + accesso alla cartella musica utente
        'NSAppleEventsUsageDescription':
            'TrackLab needs to access music files to organize them.',
        # macOS 13+ usa la chiave nuova; vecchie 12- la ignorano
        'LSApplicationCategoryType':  'public.app-category.music',
    },
)
