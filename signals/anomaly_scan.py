"""
Маркет-вайд скан публичных аномалий → таблица `anomalies` (лента сайта: тосты +
колокол). Запуск по cron на ХОСТЕ (как alerts_run): нужен доступ к БД на
127.0.0.1 и pg_notify для SSE. Telegram НЕ трогает.
  /opt/frame/signals/anomaly_scan.sh   (cron 5 * * * *)

Переиспользует ТЕ ЖЕ detector-функции, что и личные алерты (compute_position_atr /
compute_fund_flow_atr), поэтому «механизм бота» сохранён — меняется лишь то, что
гоняем их по всем активам, а не по подпискам. Пишет ОДНУ строку на
(тип·актив·сторона·день): ON CONFLICT DO NOTHING + уникальный индекс гарантируют,
что пере-скан в тот же день не плодит дублей (зеркало гейта «новый день» алертов).

Видимость публичных аномалий НЕ делится по силе/тарифу (решение Вадима) — порог
единый PUBLIC_RATIO_MIN. Тариф влияет только на КЛИК (диплинк/подписка) — это
считает feed-API/фронт, не продюсер.

OI сканируем ТОЛЬКО по FIZ: net-позиция зеркальна (net физлиц = −net юрлиц), скан
обеих групп задвоил бы одно движение. FIZ — канонически «измеренная» сторона
(совпадает с текстом сигналов и диплинком clgroup=FIZ).
"""
import os
import json
from datetime import datetime, timezone

from dotenv import load_dotenv
from sqlalchemy import text

# ── .env + DB_URL override ДО импорта api.database (host-side, как alerts_run) ──
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from api.database import SessionLocal                      # noqa: E402
from api.services.oi_screener import low_activity_set       # noqa: E402
from signals import config                                 # noqa: E402
from signals.db import get_asset_name, FUND_CATEGORIES     # noqa: E402
from signals.detectors.oi import compute_position_atr      # noqa: E402
from signals.detectors.funds import compute_fund_flow_atr  # noqa: E402

_FUND_CAT_NAME = {
    "money_market": "Денежный рынок",
    "stocks": "Акции",
    "bonds": "Облигации",
    "gold": "Золото",
    "yuan": "Юань",
}


def _oi_candidate(sectype: str) -> dict | None:
    """Публичная OI-аномалия по физлицам (FIZ) или None. Порог — PUBLIC_RATIO_MIN;
    ликвидность/материальность/ATR-floor уже внутри compute_position_atr."""
    r = compute_position_atr(sectype, "FIZ")          # interval=24 (дневная) по умолчанию
    if not r:
        return None
    ratio, _last_signed, _net, direction, sig_date, _legs = r
    if ratio < config.PUBLIC_RATIO_MIN:
        return None
    word = "нарастили" if direction == "up" else "сократили"
    return {
        "type": "oi_move",
        "asset_id": sectype,
        "asset_name": get_asset_name(sectype) or sectype,
        "clgroup": "FIZ",
        "direction": direction,
        "headline": f"Физлица резко {word} позицию",
        "context": f"×{ratio:g} к обычному дневному шагу",
        "severity_value": ratio,
        "signal_date": sig_date,
        "deep_link": {"route": "/oi", "secid": sectype, "clgroup": "FIZ",
                      "interval": 24, "mode": "positions", "variant": "net"},
    }


def _fund_candidate(category: str) -> dict | None:
    """Публичная fund-аномалия по категории или None. Порог — PUBLIC_RATIO_MIN
    (+ опц. NAV-floor PUBLIC_FUND_MIN_NAV_FRAC, по умолчанию выключен)."""
    r = compute_fund_flow_atr(None, category)
    if not r:
        return None
    ratio, direction, _last_flow, sig_date = r
    if ratio < config.PUBLIC_RATIO_MIN:
        return None
    name = _FUND_CAT_NAME.get(category, category)
    word = "приток" if direction == "up" else "отток"
    return {
        "type": "funds_flow",
        "asset_id": category,
        "asset_name": name,
        "clgroup": None,
        "direction": direction,
        "headline": f"Резкий {word} — {name}",
        "context": f"×{ratio:g} к обычному дневному потоку",
        "severity_value": ratio,
        "signal_date": sig_date,
        "deep_link": {"route": "/funds-money", "category": category},
    }


_INSERT = text("""
  INSERT INTO anomalies (scope, user_id, type, asset_id, asset_name, clgroup,
                         direction, headline, context, severity_value, signal_date, deep_link)
  VALUES ('public', NULL, :type, :asset_id, :asset_name, :clgroup, :direction,
          :headline, :context, :severity_value, :signal_date, CAST(:deep_link AS JSONB))
  ON CONFLICT DO NOTHING
  RETURNING id
""")


def run_once() -> dict:
    summary = {"scanned": 0, "found": 0, "inserted": 0, "errors": 0}
    candidates: list[dict] = []

    # Малоактивные активы (мало физлиц-трейдеров) прячем из ленты ТЕМ ЖЕ принципом,
    # что пикер и скринер сигналов (services/oi_screener.low_activity): резкое
    # движение в фьючерсе, которым торгует 2-3 физлица (напр. индекс RVI), — шум,
    # а не сигнал. Единый источник порога, иначе тост покажет то, что скрыто в /oi.
    try:
        with SessionLocal() as _s:
            low_set = low_activity_set(_s)
    except Exception as e:
        low_set = set()
        print(f"[anomaly_scan] low_activity_set failed, no relevance filter: "
              f"{type(e).__name__}: {e}")

    # OI — только FIZ (net зеркален, YUR задвоил бы), все РЕЛЕВАНТНЫЕ активы.
    for sectype in config.OI_ASSETS:
        if sectype in low_set:
            continue
        summary["scanned"] += 1
        try:
            c = _oi_candidate(sectype)
            if c:
                candidates.append(c)
        except Exception as e:
            summary["errors"] += 1
            print(f"[anomaly_scan] OI {sectype} skipped: {type(e).__name__}: {e}")

    # Фонды — по категориям. Kill-switch (config.FUNDS_FLOW_ALERTS_ENABLED):
    # при выключенных fund-сигналах категории не сканируются и в публичную
    # ленту не попадают; OI-часть ленты продолжает работать как обычно.
    if not config.FUNDS_FLOW_ALERTS_ENABLED:
        print("[anomaly_scan] fund-аномалии выключены (FUNDS_FLOW_ALERTS_ENABLED=0)")
    else:
        for category in FUND_CATEGORIES:
            summary["scanned"] += 1
            try:
                c = _fund_candidate(category)
                if c:
                    candidates.append(c)
            except Exception as e:
                summary["errors"] += 1
                print(f"[anomaly_scan] funds {category} skipped: {type(e).__name__}: {e}")

    summary["found"] = len(candidates)
    if not candidates:
        return summary

    db = SessionLocal()
    try:
        for c in candidates:
            params = dict(c, deep_link=json.dumps(c["deep_link"], ensure_ascii=False))
            row = db.execute(_INSERT, params).fetchone()
            if row:    # None → ON CONFLICT (уже была сегодня) — не плодим и не нотифаим
                summary["inserted"] += 1
                # SSE: notify_listener (Phase 2) форвардит канал 'anomaly' в sse_manager.
                db.execute(
                    text("SELECT pg_notify('anomaly', :p)"),
                    {"p": json.dumps({"id": row[0], "type": c["type"],
                                      "asset_id": c["asset_id"]}, ensure_ascii=False)},
                )
        db.commit()
    except Exception as e:
        db.rollback()
        summary["errors"] += 1
        print(f"[anomaly_scan] fatal: {e}")
    finally:
        db.close()
    return summary


def main():
    s = run_once()
    print(f"[{datetime.now(timezone.utc)}] anomaly_scan: {s}")


if __name__ == "__main__":
    main()
