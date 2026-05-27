"""
Cbonds reconstruct — восстанавливает positions/amount_rub из Cbonds weight + NAV.

Формула:
    amount_rub = NAV × weight / 100
    positions  = amount_rub / price_at_date

Где price_at_date берётся с MOEX ISS:
  - Акции:    /history/.../shares/securities/{secid}    → CLOSE (TQBR)
  - Облигации: /history/.../bonds/securities/{secid}     → (LEGALCLOSEPRICE/100)*FACEVALUE + ACCINT

Результат пишется в fund_holdings_history с source='cbonds_calc'.

Использование:
    docker exec frame-api-1 python3 /app/Funds/cbonds_reconstruct.py
"""
import sys
import json
import time
import urllib.request
import urllib.parse
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from sqlalchemy import text
from api.database import get_engine

log = logging.getLogger(__name__)


# ─── Override mapping: cbonds_name → MOEX secid ─────────────────
# Для МКПАО (новые тикеры) и неоднозначных названий
NAME_OVERRIDE = {
    "МКПАО Яндекс, акция об.": "YDEX",
    "Банк ВТБ (ПАО), акция об.": "VTBR",
    "МКПАО Озон, акция об.": "OZON",
    "Сбербанк России, акция прив.": "SBERP",
    "Сургутнефтегаз, акция прив.": "SNGSP",
    "Татнефть, акция прив.": "TATNP",
    "Транснефть, акция прив.": "TRNFP",
    "Россети, акция прив.": "MRKP",
    "Банк Санкт-Петербург, акция об.": "BSPB",
    "Группа Позитив, акция об.": "POSI",
    "МКПАО Хэдхантер, акция об.": "HEAD",
    "МКПАО ВК, акция об.": "VKCO",
    "МКПАО Лента, акция об.": "LENT",
    "МД Медикал Груп Инвестментс, акция об.": "MDMG",
    "Циан, акция об.": "CIAN",
    "Самолёт, акция об.": "SMLT",
    "Группа компаний Самолет, акция об.": "SMLT",
}


# In-memory кеши.
_secid_cache = {}    # cbonds_name → (secid, isin, market_type)
_price_cache = {}    # (secid, date, market_type) → price_per_unit


def _http_get_json(url, timeout=10):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "frame-cbonds-reconstruct/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        log.debug(f"http err {url[:60]}: {e}")
        return None


def resolve_secid(cbonds_name: str):
    """
    cbonds_name → (secid, isin, market_type).
    market_type: 'shares' | 'bonds' | None
    """
    if cbonds_name in _secid_cache:
        return _secid_cache[cbonds_name]

    # 1. Hardcoded override
    override = NAME_OVERRIDE.get(cbonds_name)
    if override:
        d = _http_get_json(f"https://iss.moex.com/iss/securities.json?q={override}&iss.meta=off&limit=5")
        if d:
            secs = d.get("securities", {})
            cols = secs.get("columns", [])
            rows = secs.get("data", [])
            ci = {c: i for i, c in enumerate(cols)}
            for row in rows:
                if row[ci["secid"]] == override:
                    market = _market_for_type(row[ci.get("type", 0)])
                    res = (override, row[ci["isin"]], market)
                    _secid_cache[cbonds_name] = res
                    return res

    # 2. Очистка имени: убираем суффиксы типа "акция об."
    is_preferred = "прив." in cbonds_name or "привилегированная" in cbonds_name
    is_bond = "облигаци" in cbonds_name.lower() or " бо-" in cbonds_name.lower()
    cleaned = (
        cbonds_name
        .replace(", акция об.", "")
        .replace(", акция привилегированная", "")
        .replace(", акция прив.", "")
        .strip()
    )

    # 3. Поиск через MOEX ISS
    d = _http_get_json(
        f"https://iss.moex.com/iss/securities.json?q={urllib.parse.quote(cleaned)}&iss.meta=off&limit=10"
    )
    if d:
        secs = d.get("securities", {})
        cols = secs.get("columns", [])
        rows = secs.get("data", [])
        ci = {c: i for i, c in enumerate(cols)}

        wanted_type = "preferred_share" if is_preferred else ("bond" if is_bond else "common_share")
        # Сначала по типу + is_traded=1
        for row in rows:
            if row[ci.get("is_traded", 0)] and row[ci.get("type", 0)] == wanted_type:
                market = _market_for_type(row[ci.get("type", 0)])
                res = (row[ci["secid"]], row[ci["isin"]], market)
                _secid_cache[cbonds_name] = res
                return res
        # Fallback: любой traded
        for row in rows:
            if row[ci.get("is_traded", 0)]:
                market = _market_for_type(row[ci.get("type", 0)])
                res = (row[ci["secid"]], row[ci["isin"]], market)
                _secid_cache[cbonds_name] = res
                return res

    _secid_cache[cbonds_name] = (None, None, None)
    return None, None, None


def _market_for_type(t):
    if t in ("common_share", "preferred_share", "depositary_receipt"):
        return "shares"
    if t in ("bond", "exchange_bond", "ofz_bond", "subfederal_bond", "corporate_bond"):
        return "bonds"
    return None


def get_price(secid: str, date: str, market: str = "shares"):
    """Стоимость одной единицы в ₽ на указанную дату."""
    key = (secid, date, market)
    if key in _price_cache:
        return _price_cache[key]

    # Поищем точно на дату и в окне ±5 дней (вдруг выходной)
    from datetime import date as _date, timedelta
    target = _date.fromisoformat(date)
    for offset in [0, -1, -2, -3, 1, 2, 3, -5, -7]:
        d_str = (target + timedelta(days=offset)).isoformat()
        url = (
            f"https://iss.moex.com/iss/history/engines/stock/markets/{market}"
            f"/securities/{secid}.json?iss.meta=off&from={d_str}&till={d_str}"
        )
        d = _http_get_json(url)
        if not d:
            continue
        hist = d.get("history", {})
        cols = hist.get("columns", [])
        rows = hist.get("data", [])
        if not rows:
            continue
        ci = {c: i for i, c in enumerate(cols)}

        if market == "shares":
            # Берём TQBR + CLOSE
            for row in rows:
                if row[ci.get("BOARDID", 0)] == "TQBR" and row[ci.get("CLOSE", 0)]:
                    _price_cache[key] = row[ci["CLOSE"]]
                    return row[ci["CLOSE"]]
            for row in rows:
                if row[ci.get("CLOSE", 0)]:
                    _price_cache[key] = row[ci["CLOSE"]]
                    return row[ci["CLOSE"]]
        else:  # bonds
            for row in rows:
                lcp = row[ci.get("LEGALCLOSEPRICE", 0)]
                face = row[ci.get("FACEVALUE", 0)]
                acc = row[ci.get("ACCINT", 0)] or 0
                if lcp and face:
                    dirty = (lcp / 100.0) * face + acc
                    _price_cache[key] = dirty
                    return dirty
    _price_cache[key] = None
    return None


def reconstruct():
    engine = get_engine()
    with engine.connect() as conn:
        # Все cbonds snapshots в БД (Cbonds + cbonds_baseline)
        snapshots = conn.execute(text("""
            SELECT DISTINCT h.fund_id, h.snapshot_date, f.ticker, f.name
            FROM fund_holdings_history h
            JOIN funds f ON f.fund_id = h.fund_id
            WHERE h.source IN ('cbonds', 'cbonds_baseline')
            ORDER BY f.ticker, h.snapshot_date
        """)).mappings().all()

        log.info(f"Найдено {len(snapshots)} (fund × date) combos для реконструкции")

        total_inserted = 0
        total_skipped = 0
        for snap in snapshots:
            fund_id = snap["fund_id"]
            snap_date = snap["snapshot_date"]
            ticker = snap["ticker"]

            # NAV на эту дату из fund_data
            nav_row = conn.execute(text("""
                SELECT nav FROM fund_data
                WHERE fund_id = :fid AND trade_date <= :d
                ORDER BY trade_date DESC LIMIT 1
            """), {"fid": fund_id, "d": snap_date}).first()
            if not nav_row or not nav_row[0]:
                log.warning(f"  [SKIP] {ticker} {snap_date}: no NAV in fund_data")
                continue
            nav = float(nav_row[0])

            # Cbonds позиции
            cbonds_rows = conn.execute(text("""
                SELECT asset_name, weight
                FROM fund_holdings_history
                WHERE fund_id = :fid AND snapshot_date = :d AND source IN ('cbonds', 'cbonds_baseline')
            """), {"fid": fund_id, "d": snap_date}).mappings().all()

            log.info(f"  {ticker} {snap_date}: NAV={nav/1e9:.2f}B, {len(cbonds_rows)} positions")

            for row in cbonds_rows:
                name = row["asset_name"]
                weight = float(row["weight"]) if row["weight"] is not None else None
                if weight is None or weight <= 0:
                    continue
                amount = nav * weight / 100
                secid, isin, market = resolve_secid(name)
                if not secid or not market:
                    total_skipped += 1
                    continue
                price = get_price(secid, snap_date.isoformat(), market)
                if not price or price <= 0:
                    total_skipped += 1
                    continue
                positions = int(round(amount / price))

                # Используем asset_name из Cbonds (имена короче и стабильнее)
                conn.execute(text("""
                    INSERT INTO fund_holdings_history
                        (fund_id, asset_name, isin, weight, positions, amount_rub,
                         snapshot_date, source, created_at)
                    VALUES (:fid, :name, :isin, :weight, :positions, :amount, :d, 'cbonds_calc', NOW())
                    ON CONFLICT (fund_id, asset_name, snapshot_date) DO UPDATE SET
                        isin = EXCLUDED.isin,
                        positions = EXCLUDED.positions,
                        amount_rub = EXCLUDED.amount_rub,
                        weight = EXCLUDED.weight
                """), {
                    "fid": fund_id,
                    "name": name[:255],
                    "isin": isin,
                    "weight": weight,
                    "positions": positions,
                    "amount": amount,
                    "d": snap_date,
                })
                total_inserted += 1

            conn.commit()
            time.sleep(0.05)  # rate-limit MOEX ISS

        log.info(f"\nИтого вставлено: {total_inserted}, скипнуто (no price/secid): {total_skipped}")
        log.info(f"Кеш ISIN: {len(_secid_cache)} имен, кеш цен: {len(_price_cache)} запросов")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    reconstruct()


if __name__ == "__main__":
    main()
