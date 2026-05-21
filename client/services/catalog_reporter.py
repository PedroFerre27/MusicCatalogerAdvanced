"""
services/catalog_reporter.py — Tracking server-side della catalogazione.

Il client esegue la catalogazione localmente con MusicCataloger. Questo
modulo si occupa di notificare al server:
  - inizio job (con applicazione quote piano)
  - aggiornamenti periodici (files_done, progress_pct, log)
  - fine job (con report)
  - errori

Usage tipico nel main_window:

    reporter = CatalogReporter(api_client)
    job_id = reporter.start(
        path=str(music_dir),
        files_total=files_count,
        options={...},
    )
    if job_id is None:
        # Quota superata o piano insufficiente: messaggio già loggato
        return
    try:
        # ... esecuzione catalogazione ...
        reporter.progress(files_done=N, files_total=M, log_chunk="...")
        # ...
        reporter.complete(files_done=N, report={...})
    except Exception as e:
        reporter.fail(str(e))

Il reporter è "best-effort": errori di rete vengono loggati ma NON
interrompono la catalogazione locale. Se il server è irraggiungibile,
l'utente ottiene comunque la sua libreria catalogata.
"""
from __future__ import annotations
import threading
import queue
from typing import Optional


class CatalogReporter:
    """
    Wrapper sopra ApiClient per il tracking di una singola catalogazione.

    L'invio dei `progress` è asincrono via thread + coda: chiamare
    `progress()` non blocca la GUI/cataloger anche se la rete è lenta.
    `complete()` e `fail()` invece sono sincroni (blocking) perché
    rappresentano lo stato finale.
    """

    def __init__(self, api_client):
        self.api_client = api_client
        self.job_id: Optional[int] = None
        self.quota_remaining: int = -1
        self._enabled = api_client is not None
        self._queue: queue.Queue = queue.Queue()
        self._sender_thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()

    # ── Public API ────────────────────────────────────────────────
    def start(self, path: str, files_total: int,
              options: Optional[dict] = None) -> Optional[int]:
        """
        Notifica server. Ritorna il job_id, o None se:
          - reporter disattivato (modalità offline / no api_client)
          - server ha rifiutato (quota / piano)

        In caso di rifiuto, popola self.last_error con un messaggio
        leggibile da mostrare all'utente.
        """
        self.last_error: str = ""
        if not self._enabled:
            return None
        try:
            resp = self.api_client.catalog_start(
                path=path, files_total=files_total, options=options or {})
            self.job_id = resp["job_id"]
            self.quota_remaining = resp.get("quota_remaining", -1)
            # Avvia il thread sender
            self._sender_thread = threading.Thread(
                target=self._sender_loop, daemon=True)
            self._sender_thread.start()
            return self.job_id
        except Exception as e:
            # Distinguish quota errors (402) da altri
            msg = str(e)
            if "402" in msg or "Payment Required" in msg:
                self.last_error = "Quota del piano superata: " + self._extract_detail(msg)
            elif "403" in msg:
                self.last_error = "Opzione non disponibile nel tuo piano: " + self._extract_detail(msg)
            else:
                self.last_error = f"Server non raggiungibile o errore: {msg}"
            return None

    def progress(self, files_done: int, progress_pct: int,
                 files_total: Optional[int] = None,
                 log_chunk: str = "", log_level: str = "INFO") -> None:
        """Accoda un update progress per invio asincrono."""
        if not self._enabled or self.job_id is None:
            return
        self._queue.put({
            "type":         "progress",
            "files_done":   files_done,
            "progress_pct": progress_pct,
            "files_total":  files_total,
            "log_chunk":    log_chunk,
            "log_level":    log_level,
        })

    def complete(self, files_done: int, report: Optional[dict] = None) -> bool:
        """Notifica fine + report. Aspetta che la coda progress sia vuota.
        Ritorna True se inviato correttamente."""
        if not self._enabled or self.job_id is None:
            return False
        # Drain coda
        self._wait_queue_drain(timeout=10.0)
        self._stop_flag.set()
        try:
            self.api_client.catalog_complete(
                job_id=self.job_id, files_done=files_done, report=report or {})
            return True
        except Exception as e:
            print(f"[CatalogReporter] complete failed: {e}")
            return False

    def fail(self, error_message: str) -> bool:
        """Notifica errore. Aspetta drain coda progress prima."""
        if not self._enabled or self.job_id is None:
            return False
        self._wait_queue_drain(timeout=5.0)
        self._stop_flag.set()
        try:
            self.api_client.catalog_fail(
                job_id=self.job_id, error_message=error_message)
            return True
        except Exception as e:
            print(f"[CatalogReporter] fail-notify failed: {e}")
            return False

    def cancel(self) -> bool:
        """Marca il job come 'cancelled' lato server.

        v1085c: PRIMA di chiamare /cancel, svuotiamo la coda di update
        accodati così il sender thread non li invia più. Senza questo,
        si aveva uno spam di 'progress send failed: HTTP 409' perché
        gli ultimi N progress accodati venivano spediti dopo che il
        server aveva già messo il job in stato 'cancelled'.
        """
        if not self._enabled or self.job_id is None:
            return False
        # Purge coda PRIMA del cancel, in modo che gli update pending
        # non vengano più tentati (anche perché tanto sarebbero rifiutati)
        try:
            while True:
                self._queue.get_nowait()
                self._queue.task_done()
        except queue.Empty:
            pass
        self._stop_flag.set()
        try:
            self.api_client.catalog_cancel(job_id=self.job_id)
            return True
        except Exception as e:
            print(f"[CatalogReporter] cancel failed: {e}")
            return False

    # ── Internal: sender thread ──────────────────────────────────
    def _sender_loop(self):
        """Thread che svuota la coda e invia gli update al server.

        v1085c: detect "dead job". Se il server risponde 409 (job non
        in esecuzione, es. cancellato/completato/fallito) o 404, smette
        di tentare gli invii successivi e svuota la coda silenziosamente.
        Continuare a spammare era il pattern visto in produzione quando
        l'utente preme "Stop" durante una catalogazione lunga.

        Errori di rete TRANSITORI invece (timeout, ConnectionError) sono
        loggati ma non interrompono il loop: la prossima update tenta
        comunque, in caso il server si riprenda.
        """
        dead_job = False
        while not self._stop_flag.is_set() or not self._queue.empty():
            try:
                msg = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if dead_job:
                # Drain silenzioso: continua a svuotare la coda senza
                # tentare invii (il job lato server non accetta più update)
                self._queue.task_done()
                continue

            if msg["type"] == "progress":
                try:
                    self.api_client.catalog_progress(
                        job_id=self.job_id,
                        files_done=msg["files_done"],
                        progress_pct=msg["progress_pct"],
                        files_total=msg.get("files_total"),
                        log_chunk=msg.get("log_chunk", ""),
                        log_level=msg.get("log_level", "INFO"),
                    )
                except Exception as e:
                    err = str(e)
                    # 409 = job non più in esecuzione (cancelled/completed/failed)
                    # 404 = job inesistente (eliminato manualmente)
                    if "HTTP 409" in err or "HTTP 404" in err:
                        # Una sola riga di log, poi muto
                        print(f"[CatalogReporter] job non più attivo lato "
                              f"server, drain silenzioso ({err[:60]})")
                        dead_job = True
                    else:
                        # Errore transitorio: log e prosegui
                        print(f"[CatalogReporter] progress send failed: {err[:120]}")
            self._queue.task_done()

    def _wait_queue_drain(self, timeout: float):
        """Aspetta che il thread sender finisca di mandare gli update
        accodati. Best-effort: se scade il timeout, prosegue."""
        import time
        deadline = time.time() + timeout
        while not self._queue.empty() and time.time() < deadline:
            time.sleep(0.1)

    def _extract_detail(self, error_text: str) -> str:
        """Estrae il 'detail' dall'eccezione ApiError formattata."""
        if "HTTP " in error_text:
            try:
                # ApiError: "HTTP 402: Il tuo piano consente max 500..."
                _, detail = error_text.split(":", 1)
                return detail.strip()
            except ValueError:
                pass
        return error_text
