# music_cataloger_linux.spec — Build Linux (Ubuntu, Fedora, Arch, ...)
# v1085p: allineato allo spec Windows (onefile + niente UPX)
#
# USAGE su una macchina Linux:
#   pip install pyinstaller customtkinter Pillow eyed3 mutagen mutagen[mp3] requests pyjwt
#   pyinstaller music_cataloger_linux.spec --clean --noconfirm
#
# IMPORTANTE: PyInstaller NON fa cross-compile. Devi buildare su una
# macchina Linux per ottenere un binario Linux. Tre alternative:
#   1. WSL2 su Windows (gratis, raccomandato per Pedro)
#   2. VM VirtualBox con Ubuntu 22.04
#   3. La macchina Linux dell'amico (lui builda da source)
#
# Output: dist/music-cataloger-advanced (singolo binario, ~80-130MB)
#
# Per compatibilita' max-distro: build su Ubuntu 22.04 LTS
# (glibc 2.35) → compatibile con Ubuntu 22.04+, Debian 12+, Fedora 36+

import sys
from pathlib import Path

block_cipher = None
project_root = Path('.').absolute()

# Auto-genera .ico/icona se manca (per coerenza col Windows spec)
ico_png = project_root / 'icons' / 'app' / 'taskbar_active.png'
if not ico_png.exists():
    print(f"[spec] WARNING: {ico_png} non trovato. Build proseguira' senza icona.")

datas = [
    ('icons',                  'icons'),
    ('config',                 'config'),
    ('services',               'services'),
    ('core',                   'core'),
    ('gui',                    'gui'),
    ('run_cataloger.py',       '.'),
    ('version.py',             '.'),
    ('../docs/UPGRADES.md',    '.'),   # R2: bundlato per Help → Changelog
]

hiddenimports = [
    'config.secrets', 'config.settings', 'config.user_plans',
    'config.app_config',
    'services.external_apis', 'services.bpm_services',
    'services.cover_service', 'services.local_db',
    'services.api_client', 'services.jwt_store',
    'services.catalog_reporter', 'services.updater',
    'services.cache_manager',
    'core.cataloger', 'core.genre_classifier',
    'core.file_manager', 'core.metadata_extractor',
    'gui.main_window', 'gui.login_window', 'gui.app_icon',
    'gui.icons',
    'customtkinter',
    'PIL', 'PIL._tkinter_finder', 'PIL.Image', 'PIL.ImageTk',
    'eyed3', 'mutagen', 'mutagen.mp3',
    'musicbrainzngs', 'requests', 'urllib3', 'jwt',
]

a = Analysis(
    ['run_gui.py'],
    pathex=[str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['librosa', 'numpy', 'scipy', 'torch'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# ── ONEFILE Linux ──────────────────────────────────────────────────
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='music-cataloger-advanced',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    # v1085p: UPX disabilitato anche su Linux per coerenza e per
    # evitare false positivi degli AV su distro che li installano
    # (es. ClamAV, Sophos)
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # Linux: icona via .desktop file separato (vedi music-cataloger.desktop)
)
