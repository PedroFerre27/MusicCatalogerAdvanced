"""
services/email_service.py — Invio email transazionali via SMTP

v0.2.4 (R3): supporto email di benvenuto e notifiche admin.
Robusto a errori SMTP: tutte le eccezioni vengono loggate ma MAI
propagate al chiamante (l'endpoint che chiama send_email non deve
fallire se il server SMTP e' giu' o male configurato).

Usato tipicamente dentro FastAPI BackgroundTasks per non bloccare
la risposta HTTP sul tempo di handshake SMTP.
"""
from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from typing import Optional

from ..config import settings

logger = logging.getLogger(__name__)


def send_email(
    to: str,
    subject: str,
    body: str,
    html: Optional[str] = None,
) -> bool:
    """
    Invia un'email via SMTP configurato in `settings`.

    Args:
        to:       indirizzo destinatario
        subject:  oggetto
        body:     corpo plain text (sempre presente)
        html:     opzionale, parte alternative HTML (multipart)

    Returns:
        True se l'invio e' andato a buon fine, False altrimenti.
        Non solleva MAI eccezioni — la robustezza e' qui dentro,
        il chiamante puo' ignorare il valore di ritorno.
    """
    if not settings.SMTP_HOST or not settings.SMTP_USER:
        logger.warning(
            "[email] SMTP non configurato (SMTP_HOST/SMTP_USER vuoti) — "
            "skip invio a %s (subject=%r)", to, subject)
        return False

    sender = settings.SMTP_FROM or settings.SMTP_USER

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body)
    if html:
        msg.add_alternative(html, subtype="html")

    try:
        with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT,
                          timeout=15) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
            smtp.send_message(msg)
        logger.info("[email] inviata a %s | subject=%r", to, subject)
        return True
    except smtplib.SMTPAuthenticationError as e:
        logger.error(
            "[email] autenticazione SMTP fallita (user=%s): %s "
            "— per Gmail usa una App Password, non la password normale",
            settings.SMTP_USER, e)
    except smtplib.SMTPException as e:
        logger.error("[email] errore SMTP inviando a %s: %s", to, e)
    except OSError as e:
        # connessione rifiutata, DNS fail, timeout
        logger.error("[email] errore di rete inviando a %s: %s", to, e)
    except Exception as e:
        logger.exception("[email] errore inatteso inviando a %s: %s", to, e)
    return False
