"""
Скан «вышли новые данные» → уведомления в Telegram (всем, кто привязал бота) и
в ленту сайта (тост + колокол, строка type='data_release'). Host-cron раз в час:
  17 * * * * /opt/frame/signals/data_release_scan.sh >> /opt/frame/logs/data_release_scan.log 2>&1

Датасеты (оба обновляются редко и ВРУЧНУЮ — пользователь сам их не отследит):
  • fund_trades — месячные SCHA-составы фондов (/fund-trades). Релиз месяца M
    считаем состоявшимся, когда у КАЖДОГО фонда, имевшего ПОЛНЫЙ снапшот за
    предыдущий месяц, появился полный снапшот за M (полный = Σ weight ≥ wmin,
    зеркало FT_COMPLETE_WSUM_MIN). Так уведомление уходит один раз — когда
    месячная загрузка ЗАВЕРШЕНА, а не на первом же фонде.
  • cbr_flows — потоки участников торгов из ОРФР Банка России (/cbr-flows).
    Релиз = новый MAX(period_end_date) месячных строк.

Стейт — таблица data_releases (dataset, period), миграция 051: INSERT ... ON
CONFLICT DO NOTHING = гейт «анонсировать однажды». Первый прогон по датасету
СИДИРУЕТ текущий период молча — иначе анонсировали бы задним числом старый релиз.

Резерв-перед-отправкой (как в alerts_run): гейт коммитится ДО рассылки. Упавшая
рассылка НЕ повторяется (иначе те, кому уже дошло, получили бы дубль) — частичные
сбои видны в summary лога.
"""
import os
import json
import time
import urllib.parse
from datetime import datetime, timezone, date

from dotenv import load_dotenv
from sqlalchemy import text

# ── .env + DB_URL override ДО импорта api.database (host-side, как alerts_run) ──
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from api.database import SessionLocal          # noqa: E402
from signals.alert_notify import send_message  # noqa: E402

SITE = "https://framedata.ru"

# Зеркало констант api/routers/fund_trades.py (роутер тянет api.cache/redis,
# которых нет в host-venv signals — импортировать его отсюда нельзя, см.
# memory «Signals venv без redis»). При правке там — поправить и здесь.
FT_WSUM_MIN = 80                                # FT_COMPLETE_WSUM_MIN
FT_FLOOR = date(2021, 9, 1)                     # FT_HISTORY_FLOOR
FT_SOURCES = ["vim_sdr", "interfax_manual"]     # MONTHLY_SOURCES

# Сколько фондов из прошлого месяца могут ОТСУТСТВОВАТЬ в новом, чтобы релиз всё
# равно считался состоявшимся. 0 = строго «все». Поднять через env, если какой-то
# фонд перестал публиковаться и месяц застрял (missing-список печатается в лог).
FT_MAX_MISSING = int(os.getenv("DR_FT_MAX_MISSING", "0"))

# Пауза между Telegram-сообщениями рассылки (лимит Bot API ~30 msg/s).
BROADCAST_SLEEP_S = float(os.getenv("DR_BROADCAST_SLEEP_S", "0.1"))

_MONTHS_RU = ("", "январь", "февраль", "март", "апрель", "май", "июнь",
              "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь")


# Кастом-эмодзи Frame (тот же пак, что в alerts_run; ph — фолбэк не-Premium).
def _ce(eid: str, ph: str) -> str:
    return f'<tg-emoji emoji-id="{eid}">{ph}</tg-emoji>'


_EMO_SIGNAL = ("5454063456045012758", "🔔")
_EMO_FUNDS  = ("5454114055054728462", "💼")
_EMO_UP     = ("5454034507965444800", "📈")


def _month_label(period: date) -> str:
    return f"{_MONTHS_RU[period.month]} {period.year}"


# ─── Описание датасетов ───────────────────────────────────────────────────────
# tg_text/headline/context — функции от периода релиза; deep_link — plain-роут
# (buildAnomalyUrl на фронте отдаёт его как есть).

def _fund_tg(period: date) -> str:
    m = _month_label(period)
    return (f"{_ce(*_EMO_SIGNAL)} <b>ФРЕЙМ · НОВЫЕ ДАННЫЕ</b>\n"
            f"<b>Сделки фондов</b>\n"
            f"{_ce(*_EMO_FUNDS)} Фонды отчитались за {m}: составы и сделки "
            f"обновлены по всем фондам.\n"
            f'<a href="{SITE}/fund-trades">открыть →</a>')


def _cbr_tg(period: date) -> str:
    m = _month_label(period)
    return (f"{_ce(*_EMO_SIGNAL)} <b>ФРЕЙМ · НОВЫЕ ДАННЫЕ</b>\n"
            f"<b>Потоки ЦБ</b>\n"
            f"{_ce(*_EMO_UP)} Банк России выпустил обзор рисков: покупки и "
            f"продажи по группам участников обновлены за {m}.\n"
            f'<a href="{SITE}/cbr-flows">открыть →</a>')


DATASETS = {
    "fund_trades": {
        "name": "Сделки фондов",
        "route": "/fund-trades",
        "tg_text": _fund_tg,
        "headline": lambda p: f"Фонды отчитались за {_month_label(p)}",
        "context": lambda p: "Составы и сделки обновлены по всем фондам",
    },
    "cbr_flows": {
        "name": "Потоки ЦБ",
        "route": "/cbr-flows",
        "tg_text": _cbr_tg,
        "headline": lambda p: f"ЦБ обновил потоки за {_month_label(p)}",
        "context": lambda p: "Покупки и продажи по группам участников торгов",
    },
}


# ─── Детекторы ────────────────────────────────────────────────────────────────

_FT_SQL = text("""
  WITH snap_ok AS (
    -- Полные снапшоты (зеркало snap_ok из fund_trades.py): группировка по
    -- (fund_id, snapshot_date), чтобы два ЧАСТИЧНЫХ снапшота одного месяца не
    -- складывались в «полный».
    SELECT fund_id, snapshot_date
    FROM fund_holdings_history
    WHERE source = ANY(:sources)
      AND snapshot_date >= :floor
    GROUP BY fund_id, snapshot_date
    HAVING COALESCE(SUM(weight), 0) >= :wmin
  ),
  last_months AS (
    SELECT DISTINCT date_trunc('month', snapshot_date)::date AS m
    FROM snap_ok
    ORDER BY m DESC
    LIMIT 2
  )
  SELECT date_trunc('month', s.snapshot_date)::date AS m, s.fund_id
  FROM snap_ok s
  WHERE date_trunc('month', s.snapshot_date)::date IN (SELECT m FROM last_months)
  GROUP BY 1, 2
""")


def detect_fund_release(db):
    """(период-месяц, missing_fund_ids) свежего ЗАВЕРШЁННОГО релиза SCHA или None.
    Ростер = фонды с полным снапшотом за предыдущий месяц; новые фонды релиз не
    блокируют (войдут в ростер со следующего месяца)."""
    rows = db.execute(_FT_SQL, {
        "sources": FT_SOURCES, "floor": FT_FLOOR, "wmin": FT_WSUM_MIN,
    }).fetchall()
    if not rows:
        return None
    by_month: dict = {}
    for m, fid in rows:
        by_month.setdefault(m, set()).add(fid)
    months = sorted(by_month)
    curr = months[-1]
    if len(months) < 2:
        return curr, []
    missing = sorted(by_month[months[-2]] - by_month[curr])
    if len(missing) > FT_MAX_MISSING:
        print(f"[data_release] fund_trades {curr}: ждём {len(missing)} фондов "
              f"из прошлого месяца, fund_id={missing}")
        return None
    return curr, missing


def detect_cbr_release(db):
    """Последний месячный период ОРФР или None."""
    d = db.execute(text(
        "SELECT MAX(period_end_date) FROM cbr_flows WHERE period_kind = 'month'"
    )).scalar()
    return (d, []) if d else None


# ─── Гейт data_releases ───────────────────────────────────────────────────────

def _gate(db, dataset: str, period: date) -> str:
    """'new' — релиз новый, анонсируем; 'seed' — первый прогон по датасету,
    записали молча; 'old' — уже анонсирован. Коммитит резерв ДО отправки."""
    has_any = db.execute(text(
        "SELECT 1 FROM data_releases WHERE dataset = :d LIMIT 1"
    ), {"d": dataset}).scalar()
    inserted = db.execute(text("""
        INSERT INTO data_releases (dataset, period) VALUES (:d, :p)
        ON CONFLICT DO NOTHING RETURNING dataset
    """), {"d": dataset, "p": period}).fetchone()
    db.commit()
    if not inserted:
        return "old"
    return "new" if has_any else "seed"


# ─── Доставка ─────────────────────────────────────────────────────────────────

_ANOMALY_SQL = text("""
  INSERT INTO anomalies (scope, user_id, type, asset_id, asset_name, clgroup,
                         direction, headline, context, severity_value, signal_date, deep_link)
  VALUES ('public', NULL, 'data_release', :asset_id, :asset_name, NULL, NULL,
          :headline, :context, NULL, :signal_date, CAST(:deep_link AS JSONB))
  ON CONFLICT DO NOTHING
  RETURNING id
""")


def _site_notify(db, dataset: str, spec: dict, period: date) -> int:
    """Строка в ленту сайта (тост + колокол) + pg_notify для SSE."""
    row = db.execute(_ANOMALY_SQL, {
        "asset_id": dataset, "asset_name": spec["name"],
        "headline": spec["headline"](period), "context": spec["context"](period),
        "signal_date": period,
        "deep_link": json.dumps({"route": spec["route"]}, ensure_ascii=False),
    }).fetchone()
    if row:
        db.execute(text("SELECT pg_notify('anomaly', :p)"),
                   {"p": json.dumps({"source": "anomaly", "id": row[0]})})
    db.commit()
    return 1 if row else 0


def _tg_broadcast(db, spec: dict, period: date) -> dict:
    """Рассылка всем привязавшим бота. blocked → авто-отвязка (как в alerts_run)."""
    stats = {"ok": 0, "blocked": 0, "error": 0}
    users = db.execute(text(
        "SELECT id, telegram_chat_id FROM users WHERE telegram_chat_id IS NOT NULL"
    )).fetchall()
    msg = spec["tg_text"](period)
    kb = {"inline_keyboard": [[{
        "text": "📈 Открыть",
        "url": SITE + spec["route"],
    }]]}
    for uid, chat_id in users:
        result = send_message(chat_id, msg, reply_markup=kb)
        stats[result] = stats.get(result, 0) + 1
        if result == "blocked":
            db.execute(text("""
                UPDATE users SET telegram_chat_id = NULL, telegram_username = NULL,
                                 telegram_linked_at = NULL WHERE id = :id
            """), {"id": uid})
            db.commit()
        time.sleep(BROADCAST_SLEEP_S)
    return stats


# ─── Прогон ───────────────────────────────────────────────────────────────────

def run_once() -> dict:
    summary: dict = {}
    db = SessionLocal()
    try:
        for dataset, detect in (("fund_trades", detect_fund_release),
                                ("cbr_flows", detect_cbr_release)):
            spec = DATASETS[dataset]
            try:
                r = detect(db)
                if not r:
                    summary[dataset] = "no-data"
                    continue
                period, _missing = r
                verdict = _gate(db, dataset, period)
                if verdict != "new":
                    summary[dataset] = f"{verdict}:{period}"
                    continue
                site = _site_notify(db, dataset, spec, period)
                tg = _tg_broadcast(db, spec, period)
                summary[dataset] = {"period": str(period), "site": site, "tg": tg}
            except Exception as e:
                db.rollback()
                summary[dataset] = f"error:{e}"
                print(f"[data_release] {dataset} error: {e}")
    finally:
        db.close()
    return summary


def main():
    s = run_once()
    print(f"[{datetime.now(timezone.utc)}] data_release_scan: {s}")


if __name__ == "__main__":
    main()
