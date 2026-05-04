"""
services/local_db.py — Music Cataloger Advanced v1046
DB locale mappatura file→genere (music_library.json)
"""
from __future__ import annotations
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)
DB_VERSION = 1

class LocalMusicDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._data: dict = {"version": DB_VERSION, "last_updated": "", "files": {}}

    def load(self) -> bool:
        if not self.db_path.exists():
            return False
        try:
            with open(self.db_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._data = data
            logger.info(f"DB locale caricato: {len(self._data.get('files', {}))} file ({self.db_path.name})")
            return True
        except Exception as e:
            logger.warning(f"DB locale non leggibile: {e}")
            return False

    def save(self) -> bool:
        try:
            self._data["last_updated"] = datetime.now().isoformat(timespec="seconds")
            self._data["version"] = DB_VERSION
            with open(self.db_path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=2)
            logger.info(f"DB locale salvato: {len(self._data.get('files', {}))} file → {self.db_path.name}")
            return True
        except Exception as e:
            logger.error(f"Errore salvataggio DB locale: {e}")
            return False

    def upsert(self, relative_path: str, genre: str,
               subgenre: Optional[str] = None,
               bpm: Optional[float] = None,
               quality_kbps: Optional[int] = None) -> None:
        self._data.setdefault("files", {})[relative_path] = {
            "genre": genre,
            "subgenre": subgenre,
            "bpm": round(bpm, 1) if bpm else None,
            "quality_kbps": quality_kbps,
            "cataloged_at": datetime.now().isoformat(timespec="seconds"),
        }

    def count(self) -> int:
        return len(self._data.get("files", {}))
