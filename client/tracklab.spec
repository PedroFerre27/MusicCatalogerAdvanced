# -*- mode: python ; coding: utf-8 -*-
# TrackLab — PyInstaller spec
# v1085m: passato a ONEFILE mode per supportare auto-update via copy
#
# Modalità precedente (onedir COLLECT): EXE+_internal/ in cartella.
# L'auto-updater faceva copy del solo .exe loader; le DLL in _internal/
# restavano vecchie → upgrade silenziosamente fallito.
#
# Modalità onefile (questa): un singolo .exe self-extracting che a
# runtime si decomprime in una temp dir. Boot è ~1-2s più lento ma
# auto-update funziona con un singolo copy/move.

import sys
from pathlib import Path

block_cipher = None
project_root = Path('.').absolute()

# v1085m: assicuro che .ico esista prima della build. Se manca, lo
# genero dal PNG sorgente con build_ico.py — evita che pyinstaller
# fallisca silenziosamente sul `icon=` del blocco EXE.
ico_path = project_root / 'icons' / 'tracklab.ico'
src_png  = project_root / 'icons' / 'app' / 'taskbar_active.png'
if not ico_path.exists() and src_png.exists():
    print(f"[spec] {ico_path} non trovato, lo genero da {src_png}")
    try:
        from PIL import Image
        sizes = [(16,16), (24,24), (32,32), (48,48), (64,64), (128,128), (256,256)]
        img = Image.open(str(src_png)).convert("RGBA")
        ico_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(str(ico_path), format='ICO', sizes=sizes)
        print(f"[spec] {ico_path} generato OK")
    except Exception as e:
        print(f"[spec] WARNING: impossibile generare {ico_path}: {e}")

datas = [
    ('config',    'config'),
    ('services',  'services'),
    ('core',      'core'),
    ('gui',       'gui'),
    ('icons',     'icons'),
    ('../docs/UPGRADES.md', '.'),   # R2: bundlato per Help → Changelog
]

hiddenimports = [
    'config.secrets', 'config.settings', 'config.user_plans',
    'config.app_config',
    'services.external_apis', 'services.bpm_services',
    'services.cover_service', 'services.local_db',
    'services.api_client', 'services.jwt_store',
    'services.catalog_reporter', 'services.updater',
    'core.cataloger', 'core.genre_classifier',
    'core.file_manager', 'core.metadata_extractor',
    'gui.main_window', 'gui.login_window', 'gui.app_icon',
    'gui.icons', 'gui.widgets', 'gui.styles',
    'customtkinter', 'eyed3', 'mutagen', 'mutagen.mp3',
    'PIL', 'PIL.Image', 'PIL.ImageTk',
    'musicbrainzngs', 'requests', 'jwt',
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
    # v1085n: bytecode optimization 0 = no -O.
    # Il default è già 0 ma lo metto esplicito per essere chiaro che
    # NON vogliamo -O (rimuove docstring e assert: alcune librerie
    # tipo eyed3 si rompono perché usano docstring runtime).
    optimize=0,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# ── ONEFILE EXE: tutto bundlato in un singolo .exe self-extracting ─
# (NO blocco COLLECT)
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
    # v1085n: UPX disabilitato. UPX comprime le DLL (incluso python313.dll)
    # ma molti antivirus aziendali (Defender + suite enterprise tipo
    # Carbon Black, SentinelOne) marcano i binari UPX come "potential
    # malware" perché tanti ransomware usano UPX per offuscarsi.
    # Risultato: l'AV mette python313.dll in quarantena → al primo
    # boot il bootloader trova `_MEIxxxx/` ma non `python313.dll` →
    # "Failed to load Python DLL". Senza UPX l'EXE è ~30% più grande
    # ma non ha questo problema.
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='icons/tracklab.ico',
    version='version_info.txt',
)
