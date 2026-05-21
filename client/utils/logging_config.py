def setup_logging(self, level):
        """Configura logging con dual handler: file UTF-8 + console cp1252-safe"""
        import io
        log_filename = f"MusicCatalogerAdvanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_path = self.script_dir / log_filename

        # Rimuovi handler esistenti
        logging.getLogger().handlers.clear()

        fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # ── Handler 1: FILE .log — UTF-8 completo, salva ├── └── originali ──
        try:
            file_handler = logging.FileHandler(log_path, encoding='utf-8')
            file_handler.setLevel(level)
            file_handler.setFormatter(fmt)
        except Exception as e:
            file_handler = None
            print(f"Avviso: Impossibile creare file di log: {e}")

        # ── Handler 2: CONSOLE/pipe — safe per cp1252, mai crashare ──
        # Converte caratteri problematici prima di scrivere su stdout.
        # v1029: aggiunge colori ANSI per WARNING (giallo) ed ERROR (rosso).
        # I colori si attivano solo su terminale reale (non pipe verso GUI).
        _IS_TTY = hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
        if _IS_TTY:
            try:
                import colorama
                colorama.init()
            except ImportError:
                pass

        _ANSI_RESET  = "\033[0m"  if _IS_TTY else ""
        _ANSI_YELLOW = "\033[33m" if _IS_TTY else ""   # WARNING → giallo
        _ANSI_LRED   = "\033[91m" if _IS_TTY else ""   # ERROR   → rosso brillante

        class SafeFormatter(logging.Formatter):
            _SUBS = {
                '\u251c\u2500\u2500': '|--',   # ├──
                '\u2514\u2500\u2500': '\\--',  # └──
                '\u2502':            '|',      # │
                '\u2500':            '-',      # ─
            }
            def format(self, record):
                # v1057: normalizza il messaggio per rimuovere BOM e NUL
                if isinstance(record.msg, str):
                    record.msg = record.msg.replace('\x00', '').replace('\ufeff', '')
                msg = super().format(record)
                for orig, safe in self._SUBS.items():
                    msg = msg.replace(orig, safe)
                # v1057: encode con errors='replace' per cp1252 safe
                try:
                    msg = msg.encode('cp1252', errors='replace').decode('cp1252')
                except Exception:
                    msg = msg.encode('ascii', errors='replace').decode('ascii')
                # v1029: colora WARNING (giallo) ed ERROR (rosso) su terminale
                if _IS_TTY:
                    if record.levelno >= logging.ERROR:
                        msg = _ANSI_LRED + msg + _ANSI_RESET
                    elif record.levelno == logging.WARNING:
                        msg = _ANSI_YELLOW + msg + _ANSI_RESET
                return msg

        try:
            safe_stream = io.TextIOWrapper(
                sys.stdout.buffer,
                encoding='utf-8',
                errors='replace',
                line_buffering=True
            )
        except AttributeError:
            safe_stream = sys.stdout  # fallback per IDLE / ambienti senza .buffer

        console_handler = logging.StreamHandler(safe_stream)
        console_handler.setLevel(level)
        console_handler.setFormatter(SafeFormatter('%(asctime)s - %(levelname)s - %(message)s'))

        # Configura logger principale
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level)
        self.logger.addHandler(console_handler)
        if file_handler:
            self.logger.addHandler(file_handler)

        # Logger root
        root_logger = logging.getLogger()
        root_logger.setLevel(level)

        # Messaggi iniziali
        if self.dry_run:
            self.logger.info("=== MODALIT\u00c0 SIMULAZIONE ATTIVA ===")
            self.logger.info("Nessun file sar\u00e0 spostato o modificato")
        self.logger.info(f"Avvio catalogazione MP3 avanzata in: {self.base_path}")
        self.logger.info(f"Database esterni: {'ABILITATI' if self.use_external_db else 'DISABILITATI'}")
        self.logger.info(f"Livello logging: {logging.getLevelName(level)}")
        if file_handler:
            self.logger.info(f"Log salvato in: {log_path}")

def _suppress_musicbrainz_warnings(self):
        """NUOVO: Soppressione globale dei warning MusicBrainz"""
        import logging
        
        # Soppressione warning musicbrainz specifici
        musicbrainz_logger = logging.getLogger('musicbrainzngs')
        musicbrainz_logger.setLevel(logging.ERROR)
        
        # Soppressione warning XML parsing
        xml_logger = logging.getLogger('xml')
        xml_logger.setLevel(logging.ERROR)
        
        # Filtro personalizzato per warning specifici
        class MusicBrainzWarningFilter(logging.Filter):
            def filter(self, record):
                unwanted_messages = [
                    'uncaught attribute',
                    'uncaught <first-release-date>',
                    'in <ws2:',
                ]
                return not any(msg in record.getMessage() for msg in unwanted_messages)
        
        # Applica il filtro a tutti i logger
        for logger_name in ['musicbrainzngs', 'xml', 'root']:
            logger = logging.getLogger(logger_name)
            logger.addFilter(MusicBrainzWarningFilter())
