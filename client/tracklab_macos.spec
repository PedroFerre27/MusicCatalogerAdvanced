# tracklab_macos.spec — Build macOS (Intel + Apple Silicon)
#
# USAGE su una macchina macOS:
#   pip install pyinstaller customtkinter Pillow eyed3 mutagen requests
#   pyinstaller tracklab_macos.spec --clean
#
# Output:
#   dist/TrackLab.app   (bundle .app)
#   dist/TrackLab       (binary nudo, dentro al .app)
#
# IMPORTANTE per gli amici di Pedro:
# - L'app NON è firmata col certificato Apple Developer ($99/anno).
#   Al primo avvio macOS bloccherà con "App di sviluppatore non
#   identificato". Per aprirla la prima volta:
#       Click destro sull'app → Apri → Apri (conferma)
#   Da lì in poi macOS la ricorda.
# - Se vuoi distribuirla "professionalmente" servono certificato Apple
#   Developer + notarizzazione (post-MVP).
#
# Apple Silicon vs Intel:
# - PyInstaller fa build per l'architettura della macchina su cui gira
# - Su un Mac M1/M2/M3 ottieni un binario arm64
# - Su un Mac Intel ottieni un binario x86_64
# - Per UN bundle universal (entrambe arch) serve:
#   `target_arch="universal2"` MA tutte le wheel pip devono essere
#   universal2 — molte non lo sono. Più semplice fare 2 build.

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
        ('../docs/UPGRADES.md',    '.'),   # R2: bundlato per Help → Changelog
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
    icon='icons/tracklab.icns',  # se presente, altrimenti None
    bundle_identifier='com.pedromarques.musiccataloger',
    info_plist={
        'CFBundleShortVersionString': '1.0.85',
        'CFBundleVersion': '1.0.85.7',
        'NSHighResolutionCapable': 'True',
        'LSMinimumSystemVersion': '10.13.0',
        # Necessario per Tk + accesso alla cartella musica utente
        'NSAppleEventsUsageDescription':
            'TrackLab needs to access music files to organize them.',
    },
)
