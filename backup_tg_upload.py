#!/usr/bin/env python3
"""Заливка дампа БД в Telegram ОДНИМ файлом — от аккаунта Вадима (Premium), не ботом.

Bot API режет файлы до 50 МБ независимо от Premium получателя, поэтому раньше
дамп (~900 МБ) уходил 19 частями. Пользовательский аккаунт с Premium по MTProto
грузит до 4 ГБ — файл приходит целиком в «Избранное» (Saved Messages).

Сессия — та же, что у сканера хайпа (signals/mtp_session, аккаунт Вадима).
Работаем с КОПИЕЙ файла сессии: оригинал — SQLite, его каждые 2 минуты держит
tg_hype_scan, одновременная запись дала бы «database is locked». Ключ
авторизации один, параллельные соединения с ним Telegram допускает.

Запуск (из backup_db.sh):
  /opt/frame/signals/.venv/bin/python /opt/frame/backup_tg_upload.py FILE CAPTION
Код возврата 0 — файл доставлен, иначе backup_db.sh откатывается на части через бота.
"""
import os
import shutil
import sys
import tempfile
import time

from telethon.sync import TelegramClient

SESSION = "/opt/frame/signals/mtp_session.session"
ENV = "/opt/frame/.env"


def read_env():
    env = {}
    with open(ENV) as fh:
        for line in fh:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k] = v.strip().strip('"')
    return env


def main():
    path, caption = sys.argv[1], sys.argv[2]
    env = read_env()
    tmpdir = tempfile.mkdtemp(prefix="bk_mtp_")
    session = os.path.join(tmpdir, "s")
    shutil.copy(SESSION, session + ".session")
    started = time.time()
    try:
        with TelegramClient(session, int(env["MTP_API_ID"]), env["MTP_API_HASH"],
                            use_ipv6=True, connection_retries=10, request_retries=10) as client:
            if not client.get_me().premium:
                print("ERROR: у аккаунта нет Premium — файл >2 ГБ не пройдёт", file=sys.stderr)
            msg = client.send_file("me", path, caption=caption, force_document=True, part_size_kb=512)
            if msg.file.size != os.path.getsize(path):
                print(f"ERROR: размер в Telegram {msg.file.size} != {os.path.getsize(path)}", file=sys.stderr)
                return 1
            print(f"sent msg {msg.id}, {msg.file.size} bytes in {round(time.time() - started)}s")
            return 0
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
