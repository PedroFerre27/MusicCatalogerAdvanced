# music_cataloger_linux.spec — Build Linux (Ubuntu, Fedora, Arch, ...)
#
# USAGE su una macchina Linux:
#   pip install pyinstaller customtkinter Pillow eyed3 mutagen requests
#   pyinstaller music_cataloger_linux.spec --clean
#
# IMPORTANTE: PyInstaller NON fa cross-compile. Devi buildare su una
# macchina Linux per ottenere un binario Linux. Stesse 3 alternative
# se non hai una macchina Linux:
#   1. WSL2 su Windows (gratis)
#   2. VM VirtualBox con Ubuntu (gratis)
#   3. Docker container (vedi Dockerfile.build-linux)
#
# Output: dist/music-cataloger-advanced (binary singolo)
#
# NOTA distribuzione: per essere usabile su tutti gli amici Linux
# con distribuzioni diverse, raccomandiamo:
#   - Build su Ubuntu 22.04 LTS (glibc 2.35) → compatibile con
#     Ubuntu 22.04+, Debian 12+, Fedora 36+, Arch corrente
#   - Per supportare distro più vecchie: usa una build su CentOS 7
#     in container, ottieni glibc 2.17 → compatibile con quasi tutto

block_cipher = None

a = Analysis(
    ['run_gui.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('icons',                  'icons'),
        ('config',                 'config'),
        ('services',               'services'),
        ('core',                   'core'),
        ('gui',                    'gui'),
        ('run_cataloger.py',       '.'),
        ('version.py',             '.'),
    ],
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
        'jwt',
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
    name='music-cataloger-advanced',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,        # equivalente a Windows --windowed
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # Linux non supporta icona embedded nell'EXE come Windows;
    # l'icona si associa via .desktop file (vedi build-linux.sh)
)
