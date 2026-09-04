#!/usr/bin/env python3
"""
Клиент FinanceMarker: запрос → слой сырья → разбор.

⚠️ ТОКЕН ТОЛЬКО В ЗАПРОСЕ, НИКОГДА В ЛОГЕ. Заголовок Bearer их API не понимает —
авторизация идёт query-параметром `?api_token=`, то есть секрет физически попадает в
URL. Логируем поэтому не URL, а логический ключ («stocks/MOEX:SBER»); в исключениях
токен вырезается.

⚠️ ЛИМИТ — ЖИВОЙ СЧЁТЧИК, А НЕ КОНСТАНТА. token_info отдаёт `day_limit` — это
ОСТАТОК, он уменьшается с каждым запросом (сам token_info не считается). Перед
обходом читаем остаток и останавливаемся, не доходя до нуля: упереться в лимит на
середине обхода значит получить половину компаний обновлёнными, а половину нет — и
не узнать об этом, потому что ошибок не будет.

⚠️ ОСТАНОВКА ПРИ 429/403 — как у smart-lab. Продолжать обход после того, как источник
попросил уйти, — прямой путь к блокировке. Один раз этот урок уже оплачен MOEX.
"""

import gzip
import hashlib
import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

from sqlalchemy import text

BASE = "https://financemarker.ru/api/fm/v2"
LOG_DIR = Path(__file__).resolve().parent / "logs"
log = logging.getLogger("fm_client")

# Запас, ниже которого обход не начинаем и прерываем. Не ноль: между нашей проверкой
# и концом обхода лимит могут потратить другие процессы или ручные вызовы.
ЗАПАС_ЗАПРОСОВ = 20
ПАУЗА = 0.3          # вежливость: 3 запроса в секунду — сильно ниже любых разумных лимитов
ПОВТОРОВ = 3
БЭКОФФ = 2.0


class ЛимитИсчерпан(RuntimeError):
    """Запросов не осталось. Не ошибка сети — причина остановиться до завтра."""


class ИсточникПросилУйти(RuntimeError):
    """429/403. Продолжать обход нельзя."""


def _без_токена(s: str) -> str:
    return re.sub(r"api_token=[^&\s]+", "api_token=<REDACTED>", str(s))


def setup_logging(verbose: bool = False):
    """Двойное логирование, как у фетчеров проекта: полный лог и лог ошибок."""
    LOG_DIR.mkdir(exist_ok=True)
    day = date.today().strftime("%Y%m")
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    for path, level in ((LOG_DIR / f"fm_{day}.log", logging.DEBUG),
                        (LOG_DIR / f"fm_errors_{day}.log", logging.WARNING)):
        h = logging.FileHandler(path, encoding="utf-8")
        h.setLevel(level)
        h.setFormatter(fmt)
        log.addHandler(h)
    import sys
    c = logging.StreamHandler(sys.stderr)
    c.setLevel(logging.DEBUG if verbose else logging.INFO)
    c.setFormatter(fmt)
    log.addHandler(c)


class FMClient:
    def __init__(self, token: str = None, db=None):
        self.token = token or os.environ.get("FM_API_TOKEN", "")
        if not self.token:
            raise RuntimeError("нет FM_API_TOKEN в окружении")
        self.db = db
        self.потрачено = 0

    # ── лимит ────────────────────────────────────────────────────────────────
    def остаток(self) -> int:
        """Сколько запросов осталось. Сам этот вызов не считается."""
        данные, _ = self._raw("token_info", {}, учитывать=False)
        return int(данные.get("day_limit", 0))

    # ── сеть ─────────────────────────────────────────────────────────────────
    def _raw(self, путь: str, params: dict, учитывать: bool = True):
        q = dict(params or {})
        q["api_token"] = self.token
        url = "%s/%s?%s" % (BASE, путь, urllib.parse.urlencode(q))
        для_лога = путь + ("?" + urllib.parse.urlencode(params) if params else "")

        последняя = None
        for попытка in range(1, ПОВТОРОВ + 1):
            try:
                with urllib.request.urlopen(url, timeout=30) as r:
                    тело = r.read()
                if учитывать:
                    self.потрачено += 1
                data = json.loads(тело.decode("utf-8"))
                # ⚠️ Их API отдаёт отказ КОДОМ 200 с телом {"code":403,...}. Без этой
                # проверки «out_of_limit» уехал бы в разбор как обычные данные и
                # выглядел бы как «источник вернул пусто».
                if isinstance(data, dict) and data.get("code") in (400, 403):
                    m = data.get("message", "")
                    if "limit" in m:
                        raise ЛимитИсчерпан(m)
                    raise ИсточникПросилУйти("%s: %s" % (data.get("code"), m))
                log.debug("получено %d Б: %s", len(тело), для_лога)
                return data, тело
            except (ЛимитИсчерпан, ИсточникПросилУйти):
                raise
            except urllib.error.HTTPError as ex:
                if ex.code in (429, 403):
                    log.error("ИСТОЧНИК ПРОСИТ ОСТАНОВИТЬСЯ: HTTP %s на %s", ex.code, для_лога)
                    raise ИсточникПросилУйти("HTTP %s" % ex.code)
                последняя = ex
            except Exception as ex:
                последняя = ex
            if попытка < ПОВТОРОВ:
                пауза = БЭКОФФ ** попытка
                log.warning("сбой на %s (%s) — повтор %d/%d через %.0f с",
                            для_лога, _без_токена(последняя), попытка, ПОВТОРОВ, пауза)
                time.sleep(пауза)
        log.error("не получилось: %s — %s", для_лога, _без_токена(последняя))
        raise последняя

    def get(self, путь: str, params: dict = None, doc_key: str = None):
        """Запрос + сохранение сырого ответа в слой сырья."""
        data, тело = self._raw(путь, params or {})
        if self.db is not None:
            self._в_сырьё(doc_key or путь, путь, params or {}, тело)
        time.sleep(ПАУЗА)
        return data

    # ── слой сырья ───────────────────────────────────────────────────────────
    def _в_сырьё(self, doc_key: str, путь: str, params: dict, тело: bytes):
        h = hashlib.sha256(тело).hexdigest()
        # ⚠️ URL пишем БЕЗ токена. Он попадёт в базу, в бэкапы и в дашборд.
        url = "%s/%s?%s" % (BASE, путь, urllib.parse.urlencode(params))
        try:
            self.db.execute(text("""
                INSERT INTO source_documents (source, doc_key, url, http_status,
                       content_type, content_hash, body, body_bytes)
                VALUES ('financemarker', :k, :u, 200, 'application/json', :h, :b, :n)
                ON CONFLICT (source, doc_key, content_hash)
                DO UPDATE SET last_seen = now()
            """), {"k": doc_key[:300], "u": url, "h": h,
                   "b": gzip.compress(тело, 6), "n": len(тело)})
        except Exception as ex:
            # Слой сырья — наблюдение. Он не может быть причиной, по которой данные
            # не забраны: пишем предупреждение и работаем дальше.
            log.warning("сырьё не сохранено (%s): %s", doc_key, _без_токена(ex))
