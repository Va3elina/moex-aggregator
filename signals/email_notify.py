"""
E-mail доставка алертов (noreply) — ГЕЙТ инфраструктуры.

Реальная отправка включается ТОЛЬКО когда в /opt/frame/.env заданы SMTP-креды:
  SMTP_HOST, SMTP_PORT (587), SMTP_USER, SMTP_PASS, SMTP_FROM (noreply@…).
Без них send_email возвращает 'skip' — канал 'email' в UI помечен «проверяется»,
а Telegram + сайт работают без этого. См. план chart-alerts (инфра-подзадача:
Вадим подтверждает noreply-ящик + SPF/DKIM-доставляемость).

send_email(to, subject, html) → 'ok' | 'skip' | 'error'
  skip  — SMTP не настроен (канал молчит, доставку берут другие каналы);
  ok    — письмо принято сервером;
  error — временный сбой (как TG 'error' — этот канал не доставил).
"""
import os
import smtplib
import ssl
from email.message import EmailMessage


def _smtp_config() -> dict | None:
    host = os.getenv("SMTP_HOST", "").strip()
    sender = os.getenv("SMTP_FROM", "").strip()
    if not host or not sender:
        return None
    return {
        "host": host,
        "port": int(os.getenv("SMTP_PORT", "587") or "587"),
        "user": os.getenv("SMTP_USER", "").strip(),
        "password": os.getenv("SMTP_PASS", "").strip(),
        "sender": sender,
    }


def send_email(to_addr: str, subject: str, html_body: str) -> str:
    cfg = _smtp_config()
    if cfg is None:
        return "skip"
    if not to_addr:
        return "skip"
    try:
        msg = EmailMessage()
        msg["From"] = cfg["sender"]
        msg["To"] = to_addr
        msg["Subject"] = subject
        # Текстовая версия — грубый strip тегов; HTML — как есть.
        import re
        msg.set_content(re.sub(r"<[^>]+>", "", html_body))
        msg.add_alternative(html_body, subtype="html")
        ctx = ssl.create_default_context()
        with smtplib.SMTP(cfg["host"], cfg["port"], timeout=15) as s:
            s.starttls(context=ctx)
            if cfg["user"]:
                s.login(cfg["user"], cfg["password"])
            s.send_message(msg)
        return "ok"
    except Exception as e:
        print(f"[email_notify] send failed to {to_addr}: {e}")
        return "error"
