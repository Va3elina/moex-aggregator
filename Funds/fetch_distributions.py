"""
Периодический фетчер выплат дохода по паям БПИФ/ОПИФ → таблица fund_distributions
(для total-return доходности). Запуск по cron в контейнере frame-api-1 раз в неделю —
подхватывает новые квартальные выплаты автоматически.

Источник: HTML профиля ETF на сайте Cbonds —
    https://cbonds.ru/etf/{cbonds_share_id}/
В страницу встроен JS-блоб `var dividends = {...};` с массивами
`expected` и `archive`. Каждый объект выплаты содержит:
    etf_share_classes_id(.numeric)  — id класса пая (= cbonds_share_id)
    record_date / record_date.numeric  — дата фиксации (DD.MM.YYYY / unix-сек)
    payable_date / payable_date.numeric — дата выплаты
    distribution_amount.numeric        — ₽ на пай (число)
    currency_name                      — валюта (обычно RUB)

ВАЖНО про даты: unix-секунды (`*.numeric`) проставлены на ПОЛНОЧЬ МСК
(UTC+3). Конвертация в UTC сдвигает дату на день назад (31.03 → 30.03).
Поэтому конвертируем в фиксированном поясе Москвы — тогда дата совпадает
и с текстовым полем record_date, и с эталонными значениями.

БД: коннект через DB_URL (как Funds/fetch_funds_realtime.py), UPSERT напрямую.
Запуск в контейнере frame-api-1 (там DB_URL=прод):

    docker exec frame-api-1 python3 /app/Funds/fetch_distributions.py

Локально (DB_URL=localhost из .env) тоже запустится, но писать будет в локальную БД.

Идемпотентно: ON CONFLICT (fund_id, record_date) DO UPDATE.
Rate-limit: пауза между фондами (Cbonds блокирует при частых запросах).
"""
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta

from sqlalchemy import create_engine, text

# ─── Настройки ─────────────────────────────────────────────────
SOURCE = "cbonds"


def get_engine():
    """Коннект к БД через DB_URL (как Funds/fetch_funds_realtime.py). В контейнере
    frame-api-1 DB_URL указывает на прод-БД — скрипт рассчитан на запуск там по cron."""
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise SystemExit("DB_URL не установлен (.env). В контейнере frame-api-1 он есть.")
    return create_engine(db_url, connect_args={"ssl_context": False})

CBONDS_URL = "https://cbonds.ru/etf/{sid}/"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
SLEEP_BETWEEN = 0.8  # сек между фондами — rate-limit Cbonds

# Cbonds проставляет unix-секунды на полночь по Москве (UTC+3).
MSK = timezone(timedelta(hours=3))


# ─── Список фондов из БД ───────────────────────────────────────
def fetch_funds(engine) -> list[tuple[int, str, int]]:
    """SELECT fund_id, ticker, cbonds_share_id FROM funds WHERE cbonds_share_id IS NOT NULL."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT fund_id, ticker, cbonds_share_id FROM funds "
            "WHERE cbonds_share_id IS NOT NULL ORDER BY fund_id"
        )).fetchall()
    return [(int(r[0]), r[1], int(r[2])) for r in rows]


# ─── HTTP ──────────────────────────────────────────────────────
def http_get(url: str, timeout: int = 30) -> str | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [http err] {url}: {e}", file=sys.stderr)
        return None


# ─── Парсинг ───────────────────────────────────────────────────
_DIVIDENDS_ANCHOR = re.compile(r"var\s+dividends\s*=\s*")


def _brace_match(s: str, start: int) -> str | None:
    """Возвращает подстроку сбалансированного {...} начиная с s[start]=='{'.

    Уважает строковые литералы и escape, чтобы скобки внутри строк не сбивали
    счётчик глубины.
    """
    if start >= len(s) or s[start] != "{":
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(s)):
        c = s[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return s[start:i + 1]
    return None


def _to_date(unix_sec, text_val):
    """unix-секунды (полночь МСК) → date. Fallback на текст DD.MM.YYYY."""
    if unix_sec is not None:
        try:
            return datetime.fromtimestamp(int(unix_sec), tz=MSK).date()
        except (ValueError, OSError, OverflowError):
            pass
    if text_val:
        try:
            return datetime.strptime(text_val.strip(), "%d.%m.%Y").date()
        except ValueError:
            pass
    return None


def parse_distributions(html: str, cbonds_share_id: int) -> tuple[list[dict], str | None]:
    """Извлекает выплаты из HTML, фильтруя по etf_share_classes_id.

    Возвращает (rows, error). Стратегия:
      1. Якорь `var dividends = {...}` → brace-match → json.loads. Объединяем
         expected+archive.
      2. Fallback: если глобальный JSON не распарсился — собираем объекты
         поштучно вокруг якоря "distribution_amount.numeric" brace-matching’ом.
    """
    rows: list[dict] = []

    # ── Стратегия 1: цельный блоб var dividends = {...} ──────────
    m = _DIVIDENDS_ANCHOR.search(html)
    if m:
        blob = _brace_match(html, m.end())
        if blob:
            try:
                div = json.loads(blob)
                items = (div.get("expected") or []) + (div.get("archive") or [])
                rows = _items_to_rows(items, cbonds_share_id)
                if rows:
                    return rows, None
            except (json.JSONDecodeError, AttributeError):
                pass  # упадём в fallback

    # ── Стратегия 2: пообъектный сбор вокруг якоря суммы ─────────
    objs = _collect_objects_by_anchor(html, "distribution_amount.numeric")
    rows = _items_to_rows(objs, cbonds_share_id)
    if rows:
        return rows, None

    # Различаем «накопительный фонд (нет выплат)» и «парсинг не удался».
    if "distribution_amount.numeric" not in html:
        return [], None  # на странице вообще нет выплат — норма
    return [], "anchor present but no rows parsed"


def _collect_objects_by_anchor(html: str, anchor: str) -> list[dict]:
    """Для каждого вхождения anchor находит охватывающий {...} и json.loads его."""
    out = []
    seen_spans = set()
    idx = 0
    while True:
        a = html.find(anchor, idx)
        if a == -1:
            break
        idx = a + len(anchor)
        # Назад до ближайшей открывающей '{' на верхнем уровне относительно anchor.
        # Идём влево, считая баланс, пока не выйдем в depth=+1 «снаружи слева».
        depth = 0
        in_str = False
        start = None
        i = a
        while i >= 0:
            c = html[i]
            # Грубое уважение строк: считаем неэкранированные кавычки.
            if c == '"' and not (i > 0 and html[i - 1] == "\\"):
                in_str = not in_str
            if not in_str:
                if c == "}":
                    depth += 1
                elif c == "{":
                    if depth == 0:
                        start = i
                        break
                    depth -= 1
            i -= 1
        if start is None:
            continue
        if start in seen_spans:
            continue
        seen_spans.add(start)
        blob = _brace_match(html, start)
        if not blob:
            continue
        try:
            out.append(json.loads(blob))
        except json.JSONDecodeError:
            continue
    return out


def _items_to_rows(items: list[dict], cbonds_share_id: int) -> list[dict]:
    """Фильтр по etf_share_classes_id + нормализация в наши поля."""
    rows = []
    sid = int(cbonds_share_id)
    for o in items:
        # Фильтр по классу пая (на странице бывают родственные классы).
        oid = o.get("etf_share_classes_id.numeric")
        if oid is None:
            oid = o.get("etf_share_classes_id")
        try:
            if oid is None or int(oid) != sid:
                continue
        except (ValueError, TypeError):
            continue

        amt = o.get("distribution_amount.numeric")
        if amt is None:
            continue
        try:
            amt = float(amt)
        except (ValueError, TypeError):
            continue

        rd = _to_date(o.get("record_date.numeric"), o.get("record_date"))
        if rd is None:
            continue  # record_date — часть PK, без неё строку не записать
        pd = _to_date(o.get("payable_date.numeric"), o.get("payable_date"))
        cur = (o.get("currency_name") or "RUB").strip() or "RUB"

        rows.append({
            "record_date": rd,
            "payable_date": pd,
            "amount_per_unit": amt,
            "currency": cur,
        })

    # Дедуп по record_date (PK) — последнее значение выигрывает.
    dedup = {}
    for r in rows:
        dedup[r["record_date"]] = r
    return sorted(dedup.values(), key=lambda r: r["record_date"], reverse=True)


# ─── Main ──────────────────────────────────────────────────────
UPSERT = text(
    "INSERT INTO fund_distributions"
    "(fund_id, record_date, payable_date, amount_per_unit, currency, source) "
    "VALUES (:fund_id, :record_date, :payable_date, :amount, :currency, :source) "
    "ON CONFLICT (fund_id, record_date) DO UPDATE SET "
    "amount_per_unit=EXCLUDED.amount_per_unit, payable_date=EXCLUDED.payable_date, "
    "currency=EXCLUDED.currency, updated_at=now()"
)


def main():
    engine = get_engine()
    funds = fetch_funds(engine)
    print(f"Фондов с cbonds_share_id: {len(funds)} · Cbonds → upsert fund_distributions\n")

    summary_with = []   # (ticker, n)
    summary_empty = []  # ticker — накопительные/без выплат (норма)
    summary_err = []    # (ticker, sid, error)
    total_rows = 0

    for fund_id, ticker, sid in funds:
        html = http_get(CBONDS_URL.format(sid=sid))
        if html is None:
            summary_err.append((ticker, sid, "HTTP fetch failed"))
            time.sleep(SLEEP_BETWEEN)
            continue

        rows, err = parse_distributions(html, sid)
        if err:
            summary_err.append((ticker, sid, err))
        elif not rows:
            summary_empty.append(ticker)
            print(f"  {ticker:14} (sid {sid}): 0 выплат (накопительный?)")
        else:
            with engine.begin() as conn:
                for r in rows:
                    conn.execute(UPSERT, {
                        "fund_id": fund_id,
                        "record_date": r["record_date"],
                        "payable_date": r["payable_date"],
                        "amount": r["amount_per_unit"],
                        "currency": r["currency"],
                        "source": SOURCE,
                    })
            summary_with.append((ticker, len(rows)))
            total_rows += len(rows)
            newest = rows[0]
            print(f"  {ticker:14} (sid {sid}): {len(rows):2} выплат "
                  f"(свежая {newest['record_date']} → {newest['amount_per_unit']:.4f} {newest['currency']})")

        time.sleep(SLEEP_BETWEEN)

    print("\n" + "=" * 64)
    print("СВОДКА")
    print("=" * 64)
    print(f"  Обработано фондов:        {len(funds)}")
    print(f"  С выплатами:              {len(summary_with)} ({total_rows} строк, upsert применён)")
    print(f"  Без выплат (накопит.):    {len(summary_empty)}")
    print(f"  Ошибки парсинга/HTTP:     {len(summary_err)}")
    if summary_empty:
        print(f"\n  Накопительные (0 выплат): {', '.join(summary_empty)}")
    if summary_err:
        print("\n  Проблемные фонды:")
        for t, s, e in summary_err:
            print(f"    - {t} (sid {s}): {e}")


if __name__ == "__main__":
    main()
