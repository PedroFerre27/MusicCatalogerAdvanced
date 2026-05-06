#!/usr/bin/env python3
"""
run_cataloger.py — Entry point CLI per Music Cataloger Advanced
v1057: fix unicode cp1252, aggiunto --excluded-genres
"""

import argparse
import sys
from pathlib import Path

# v1057: forza stdout UTF-8 prima di qualsiasi output (fix cp1252 su Windows)
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# Aggiunge la root del progetto al path
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))

from core.cataloger import MusicCataloger


def _load_caribbean_settings_from_json():
    """
    v1075: Carica `data/caribbean_settings.json` (se esiste) e applica le
    preferenze utente ai settings runtime prima che MusicCataloger venga
    istanziato. Necessario perché il cataloger gira in subprocess separato
    dalla GUI e altrimenti userebbe solo i default hardcoded di settings.py.

    Aggiorna:
      - settings.bpm.bachata_bpm_range
      - settings.bpm.salsa_bpm_range
      - settings.genre.salsa_indicators   (salsa_artists + salsa_keywords)
      - settings.genre.bachata_indicators (bachata_artists + bachata_keywords +
                                           core bachata/bachatero/bachatera)

    Il file JSON è scritto dalla GUI in `gui/main_window.py::_save_caribbean_settings`.
    Se il file manca o è corrotto, si procede in silenzio con i default.
    """
    import json
    try:
        from config.settings import settings as _s
    except Exception:
        return  # settings non disponibili → niente da fare

    # Percorso data directory: <project_root>/data/
    data_dir = project_root / "data"
    carib_file = data_dir / "caribbean_settings.json"
    if not carib_file.exists():
        return

    try:
        data = json.loads(carib_file.read_text(encoding="utf-8"))
    except Exception as ex:
        print(f"⚠ caribbean_settings.json illeggibile ({ex}) — uso i default",
              flush=True)
        return

    try:
        # BPM range
        if "bachata_bpm_range" in data:
            _s.bpm.bachata_bpm_range = tuple(data["bachata_bpm_range"])
        if "salsa_bpm_range" in data:
            _s.bpm.salsa_bpm_range = tuple(data["salsa_bpm_range"])

        # Salsa indicators = artisti noti + keyword testuali
        sal_artists = data.get("salsa_artists", []) or []
        sal_kw      = data.get("salsa_keywords", []) or []
        if sal_artists or sal_kw:
            _s.genre.salsa_indicators = [
                x.strip().lower() for x in (sal_artists + sal_kw) if x and x.strip()
            ]

        # Bachata indicators = artisti noti + keyword testuali + core obbligatori
        bac_artists = data.get("bachata_artists", []) or []
        bac_kw      = data.get("bachata_keywords", []) or []
        core_bach   = ["bachata", "bachatero", "bachatera"]
        merged_bac  = [x.strip().lower() for x in (bac_artists + bac_kw)
                       if x and x.strip()]
        # append dei core che non sono già presenti
        for c in core_bach:
            if c not in merged_bac:
                merged_bac.append(c)
        if merged_bac:
            _s.genre.bachata_indicators = merged_bac

        print(
            f"✔ Caribbean settings caricate: "
            f"{len(_s.genre.salsa_indicators)} indicatori salsa, "
            f"{len(_s.genre.bachata_indicators)} indicatori bachata",
            flush=True,
        )
    except Exception as ex:
        print(f"⚠ Errore applicando caribbean_settings.json ({ex}) — uso i default",
              flush=True)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_cataloger",
        description="Music Cataloger Advanced — Catalogazione automatica MP3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("path", help="Directory contenente i file MP3")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-external", action="store_true")
    parser.add_argument("--analyze-only", action="store_true")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--correct-folders", action="store_true")
    parser.add_argument("--classify-salsa", action="store_true")
    parser.add_argument("--duplicate-action",
                        choices=["keep_both", "skip", "overwrite"],
                        default="keep_both")
    parser.add_argument("--cover", action="store_true", default=False)
    parser.add_argument("--no-cover", action="store_true", default=False)
    parser.add_argument("--cover-strategy",
                        choices=["largest", "first_available"],
                        default="largest")
    parser.add_argument("--cover-overwrite", action="store_true", default=False)
    parser.add_argument("--cover-sources", nargs="+",
                        choices=["spotify", "musicbrainz", "lastfm", "deezer", "itunes"],
                        default=["musicbrainz", "lastfm", "deezer", "itunes"])
    # v1086.1 (revisione 3): priorita' sorgenti METADATA (cascata search_all).
    # Ordine = priorita'. Solo le sorgenti elencate vengono usate.
    # nargs="*" ammette lista esplicitamente vuota (= cascata disattivata),
    # mentre default=None significa "argomento non passato" (= usa default).
    # Distinguere None da [] e' fondamentale: con nargs="+" la lista vuota
    # non era esprimibile e finiva sempre nel fallback default.
    parser.add_argument("--metadata-sources", nargs="*",
                        default=None,
                        help="Sorgenti metadata in ordine priorita' "
                             "(es. musicbrainz deezer itunes lastfm discogs). "
                             "Lista vuota = nessuna sorgente metadata.")
    parser.add_argument("--bpm-sources", nargs="*",
                        default=None,
                        help="Sorgenti BPM abilitate UI: getsong beatport. "
                             "Lista vuota = solo TuneBat/SongBPM/librosa fallback.")
    parser.add_argument("--excluded-genres", nargs="*", default=[])
    parser.add_argument("--update-local-db", action="store_true", default=False)
    parser.add_argument("--rename-pattern", type=str, default=None,
                        help="Pattern rinomina file: '{artist} - {title}' o '{title} - {artist}'")
    return parser


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    music_path = Path(args.path)
    if not music_path.exists():
        print(f"ERRORE: La directory non esiste: {music_path}", flush=True)
        sys.exit(1)
    if not music_path.is_dir():
        print(f"ERRORE: Il percorso non e' una directory: {music_path}", flush=True)
        sys.exit(1)

    # v1075: carica caribbean_settings.json PRIMA di istanziare il cataloger.
    # Il subprocess è separato dalla GUI e senza questo step il GenreClassifier
    # userebbe solo i default hardcoded di settings.py, ignorando gli artisti
    # e le keyword aggiunti dall'utente nel tab Caraibica.
    _load_caribbean_settings_from_json()

    cover_enabled = True
    if args.no_cover:
        cover_enabled = False
    elif args.cover:
        cover_enabled = True

    cataloger = MusicCataloger(
        base_path=str(music_path),
        dry_run=args.dry_run,
        use_external_db=not args.no_external,
        verbose=args.verbose,
        duplicate_action=args.duplicate_action,
        cover_enabled=cover_enabled,
        cover_strategy=args.cover_strategy,
        cover_source_priority=args.cover_sources,
        cover_overwrite=args.cover_overwrite,
        update_local_db=args.update_local_db,
        excluded_genres=args.excluded_genres,
        rename_pattern=args.rename_pattern,
        # v1086.1: priorita' sorgenti UI
        metadata_sources=args.metadata_sources,
        bpm_sources=args.bpm_sources,
    )

    try:
        cataloger.load_cache()

        if args.analyze_only:
            cataloger.analyze_collection()
        else:
            cataloger.scan_and_catalog()

        if args.correct_folders:
            cataloger.correct_existing_folders()

        if args.classify_salsa:
            cataloger.classify_salsa_by_bpm()

        if args.cleanup:
            cataloger.cleanup_empty_folders()

        cataloger.generate_report()

        if cataloger.api_calls > 0 or cataloger.updated_files > 0:
            cataloger.save_cache()

    except KeyboardInterrupt:
        print("\nInterrotto dall'utente.", flush=True)
        sys.exit(1)
    except Exception as e:
        print(f"ERRORE critico: {e}", flush=True)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
