#!/usr/bin/env python3
"""
Клиент FinanceMarker: запрос → слой сырья → разбор.

⚠️ ТОКЕН ТОЛЬКО В ЗАПРОСЕ, НИКОГДА В ЛОГЕ. Заголовок Bearer их API не понимает —
авторизация идёт query-параметром `?api_token=`, то есть секрет физически попадает в
URL. Логируем поэтому не URL, а логический ключ («stocks/MOEX:SBER»); в исключениях
токен вырезается.

⚠️ ЛИМИТ СЧИТАЕМ САМИ, НА ДИСКЕ. `token_info.day_limit` НЕ предохранитель: 04.09.2026
он показал 388 в момент, когда месячный пул (400) был уже выбран и ушёл в −3. Число
живое и правдоподобное, но меряет не то, что списывается. Оно осталось в логе одной
справочной строкой — и только.
Настоящий предохранитель — таблица api_budget (миграции 076/078): расход переживает
перезапуск процесса и считается ПОСУТОЧНО — лимит у них 400 в день и обнуляется
каждые сутки. Прошлый счётчик жил в объекте клиента, и три прогона подряд каждый
считал с нуля.

⚠️ ЦЕНА ВЫЗОВА НЕ РАВНА ЕДИНИЦЕ. 145 вызовов компаний списали ~403 единицы — около
2.8 за вызов с include из восьми разделов. Точное правило биллинга неизвестно, а
выяснять его можно только новыми запросами, поэтому ЦЕНА округлена вверх: упереться
в потолок рано дешевле, чем поздно.

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
# Суточная квота (обнуляется каждый день) и оценка цены вызова. Обе — env.
СУТОЧНАЯ_КВОТА = int(os.environ.get("FM_СУТ_КВОТА", os.environ.get("FM_DAILY_QUOTA", "400")))
ЦЕНА_ВЫЗОВА = float(os.environ.get("FM_ЦЕНА_ВЫЗОВА", os.environ.get("FM_CALL_COST", "3")))
# Сколько суточной квоты НЕ трогает обход карточек. Ту же квоту тратит сканер
# раскрытия (каждые 15 мин = 96 вызовов в сутки) и ручные проверки; без явной брони
# обход карточек съедал бы всё до последней единицы и сканеру не оставалось бы ничего.
ДОЛЯ_ДРУГИМ = int(os.environ.get("FM_ДОЛЯ_ДРУГИМ", os.environ.get("FM_RESERVE_OTHERS", "120")))
# ⚠️ ДВЕ СЕКУНДЫ, А НЕ 0,3. Тот 403 на 98-й компании я сперва объяснил защитой от
# частоты («остаток же 388») и пошёл вторым проходом — это и добило квоту. 403 был
# про ЛИМИТ. Пауза всё равно остаётся: три запроса в секунду к API с квотой 400 в
# месяц не нужны никому, 123 компании укладываются в 20 минут вместо 12.
ПАУЗА = 2.0
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
    def __init__(self, token: str = None, db=None, engine=None):
        self.token = token or os.environ.get("FM_API_TOKEN", "")
        if not self.token:
            raise RuntimeError("нет FM_API_TOKEN в окружении")
        self.db = db
        # ⚠️ ОТДЕЛЬНОЕ подключение под учёт расхода. Данные компании пишутся в
        # транзакцию вызывающего и могут откатиться — расход откатываться не должен:
        # запрос-то уже ушёл и деньги списаны. Без engine клиент не работает вовсе,
        # потому что делать платные запросы, не умея их посчитать, — это ровно то,
        # чем 04.09.2026 была выбрана вся месячная квота.
        if engine is None:
            raise RuntimeError("FMClient требует engine: без учёта расхода запросы запрещены")
        self.engine = engine
        self.потрачено = 0

    # ── бюджет ───────────────────────────────────────────────────────────────
    @staticmethod
    def _день() -> date:
        return date.today()

    def бюджет(self) -> tuple:
        """(списано, квота) за СЕГОДНЯ по НАШЕМУ счёту, не по данным источника."""
        with self.engine.connect() as c:
            строка = c.execute(text("""
                SELECT spent, limit_total FROM api_budget
                WHERE source = 'financemarker' AND period_day = :m
            """), {"m": self._день()}).first()
        if строка is None:
            return 0.0, СУТОЧНАЯ_КВОТА
        return float(строка[0]), int(строка[1])

    def установить_квоту(self, квота: int) -> None:
        """Сменился тариф — правим потолок суток. Расход не трогаем."""
        with self.engine.connect() as c:
            c.execute(text("""
                INSERT INTO api_budget (source, period_day, spent, limit_total, calls)
                VALUES ('financemarker', :m, 0, :кв, 0)
                ON CONFLICT (source, period_day) DO UPDATE
                   SET limit_total = EXCLUDED.limit_total, updated_at = now()
            """), {"m": self._день(), "кв": квота})
            c.commit()

    def состояние_уведомления(self) -> str:
        """О каком состоянии бюджета уже сообщали ('' — ещё ни о каком)."""
        with self.engine.connect() as c:
            r = c.execute(text("""
                SELECT COALESCE(notified_state, '') FROM api_budget
                 WHERE source = 'financemarker' AND period_day = :m
            """), {"m": self._день()}).first()
        return r[0] if r else ''

    def запомнить_уведомление(self, состояние: str) -> None:
        with self.engine.connect() as c:
            c.execute(text("""
                UPDATE api_budget SET notified_state = :с, notified_at = now()
                 WHERE source = 'financemarker' AND period_day = :m
            """), {"с": состояние, "m": self._день()})
            c.commit()

    def _списать(self, цена: float) -> None:
        """Пишем расход СРАЗУ и отдельной транзакцией — до разбора ответа."""
        with self.engine.connect() as c:
            c.execute(text("""
                INSERT INTO api_budget (source, period_day, spent, limit_total, calls)
                VALUES ('financemarker', :m, :ц, :кв, 1)
                ON CONFLICT (source, period_day) DO UPDATE
                   SET spent = api_budget.spent + EXCLUDED.spent,
                       calls = api_budget.calls + 1,
                       updated_at = now()
            """), {"m": self._день(), "ц": цена, "кв": СУТОЧНАЯ_КВОТА})
            c.commit()

    def проверить_бюджет(self, цена: float = None) -> None:
        """Бросает ЛимитИсчерпан, если следующий вызов уводит за суточный запас."""
        цена = ЦЕНА_ВЫЗОВА if цена is None else цена
        списано, квота = self.бюджет()
        if списано + цена > квота - ЗАПАС_ЗАПРОСОВ:
            raise ЛимитИсчерпан(
                "суточный бюджет исчерпан: списано %.0f из %d (запас %d), цена вызова %.1f"
                % (списано, квота, ЗАПАС_ЗАПРОСОВ, цена))

    # ── справка источника (НЕ предохранитель) ────────────────────────────────
    def справка_источника(self) -> int:
        """token_info.day_limit — только в лог. Предохранитель — проверить_бюджет()."""
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
                    # Списываем ФАКТ отправки, а не факт успешного разбора: источник
                    # уже посчитал этот вызов, даже если тело нам не понравится.
                    self._списать(ЦЕНА_ВЫЗОВА)
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
                if учитывать:
                    self._списать(ЦЕНА_ВЫЗОВА)   # ответ пришёл — значит, у них он посчитан
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
