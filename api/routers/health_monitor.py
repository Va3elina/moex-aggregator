"""
Health/data — барометр свежести/целостности ингеста для автомониторинга.

READ-ONLY, admin-only. Отдаёт ТОЛЬКО даты свежести + целые счётчики по
таблицам-ИСТОЧНИКАМ. Никаких значений (цен/NAV/весов/сумм), никаких
строк-записей — только агрегаты MAX()/COUNT(). НЕ касается ни одной
user/billing/auth таблицы: список источников захардкожен ниже.

Назначение: облачная Routine (Anthropic) дёргает по HTTPS с X-API-Key и
оценивает целостность данных без SSH-доступа к серверу.

ВАЖНО: лежит в ОТДЕЛЬНОМ роутере (а не в public_api.py), потому что
публичный API под kill-switch'ом PUBLIC_API_CSV_ENABLED. Этот роутер
монтируется БЕЗУСЛОВНО — мониторинг не зависит от запуска публичного API.

Оси целостности:
  • свежесть       — max_date, лаг в ТОРГОВЫХ днях (holiday-proof: candles
                     содержит только торг.дни, поэтому лаг = число торг.дней
                     ПОСЛЕ max_date данной таблицы);
  • полнота дня    — сущности на свежей дате vs на предыдущей;
  • охват          — distinct сущностей vs всего;
  • каденс         — kind задаёт ожидание (intraday/close/t_plus_1/...).
"""
from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.security.api_key import get_user_by_api_key

router = APIRouter(prefix="/api/health", tags=["health-monitor"])


# Мировое сырьё (обновляется в тот же день T-0). Остальные ряды index_data —
# биржевые MOEX (закрытие T-1).
_COMMODITY_SECIDS = {
    "BRENT", "GOLD", "SILVER", "COPPER", "ALUMINUM", "NICKEL",
    "PALLADIUM", "PLATINUM", "WHEAT", "NATGAS_HH", "LIT", "TTF_GAS",
}

# Серии, которые MOEX публикует в ISS history только ночью T+1 (MCFTR — TR-индекс
# считается после дивидендов; EUR — после остановки биржевых торгов остались
# только поздние history-строки, candles пустые). Ингест добирает их утренним
# прогоном 09:00, поэтому лаг 2 торговых дня для них — норма, не stale.
_T_PLUS_1_SECIDS = {"MCFTR", "EUR_RUB__TOM"}

# macro_data: только ЖИВЫЕ (используемые сайтом) индикаторы.
# Мёртвые ZCYC/ОФЗ/IPO сознательно исключены.
_MACRO = [
    ("MARKET_CAP_TOTAL", "SMARTLAB", "close"),
    ("M2_MONTHLY", None, "monthly"),
    ("GDP_QUARTERLY", None, "quarterly"),
    # KEY_RATE — дневной ряд ЦБ (рабочие дни ≈ торговые), с 2026-07-21 ингестится
    # ежедневно в macro_daily. Раньше kind=event (= всегда ok) прятал 40-дневный
    # застой, включая пропущенное снижение ставки.
    ("KEY_RATE", None, "t_plus_1"),
]

# Пайплайн, переставший ЗАПУСКАТЬСЯ, вечно хранит последний last_status='ok'
# (heartbeat пишет сам скрипт) — мёртвый cron выглядел зелёным. Реальный случай
# 07.2026: distributions пропустил 3 понедельника после переезда имени
# api-контейнера (frame-api-1 → frame-api-N), монитор молчал. Ловим по возрасту
# last_run_at: дефолт 48ч, недельным — явный override.
_PIPELINE_MAX_AGE_H = {"distributions": 9 * 24}  # пн-еженедельный + запас
_PIPELINE_DEFAULT_MAX_AGE_H = 48


def _status(kind, max_date, lag_td, today):
    """Грубый хинт ok/stale по типу каденса. Routine может переопределить."""
    if max_date is None:
        return "missing"
    if kind in ("intraday", "close"):
        return "ok" if (lag_td or 0) <= 1 else "stale"
    if kind == "t_plus_1":
        return "ok" if (lag_td or 0) <= 2 else "stale"
    if kind == "monthly":
        return "ok" if (today - max_date).days <= 40 else "stale"
    if kind == "quarterly":
        # Худший легитимный разрыв: квартал (92д) + лаг публикации Росстата
        # (~78д: Q1 31.03 → 17.06). Перед выходом следующего квартала данным
        # ~170 дней — это норма, не stale. 180 = запас + тревога ~через 2
        # недели после реально пропущенной публикации.
        return "ok" if (today - max_date).days <= 180 else "stale"
    if kind == "weekly":
        # Каденс = пн-еженедельный cron; 12 = неделя + запас на праздники.
        return "ok" if (today - max_date).days <= 12 else "stale"
    # event / manual — информационно, без тревоги (ручной ингест по определению)
    return "ok"


@router.get("/data")
def health_data(
    response: Response,
    user: User = Depends(get_user_by_api_key),
    db: Session = Depends(get_db),
):
    """
    Барометр свежести/целостности данных-источников (admin-only, X-API-Key).
    Только даты и счётчики — см. модуль-docstring. Мутаций нет by construction.
    """
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="admin only")

    response.headers["Cache-Control"] = "no-store"
    today = date.today()

    # Пул торговых дней (holiday-proof). Берём из index_data по IMOEX: он
    # торгуется каждый торговый день, а таблица маленькая (быстро). НЕ из
    # candles — там нет индекса под begin_time-only, полный скан = десятки сек.
    tdays = [
        r.d for r in db.execute(text(
            "SELECT DISTINCT trade_date AS d FROM index_data "
            "WHERE secid = 'IMOEX' AND trade_date >= (CURRENT_DATE - INTERVAL '40 days') "
            "ORDER BY d"
        )).all()
    ]
    market_ref = tdays[-1] if tdays else None

    def lag_td(md):
        if md is None or not tdays:
            return None
        return sum(1 for d in tdays if d > md)

    def entry(name, kind, max_date, **extra):
        lt = lag_td(max_date)
        e = {
            "name": name,
            "kind": kind,
            "max_date": max_date.isoformat() if max_date else None,
            "lag_trading_days": lt,
            "status": _status(kind, max_date, lt, today),
        }
        e.update(extra)
        return e

    sources = []

    # candles (спот) — свежесть свечного пайплайна. Меряем по ОДНОМУ ликвидному
    # secid (SBER) через индекс (secid, begin_time) — 19мс. Глобальные агрегаты
    # по candles (MAX/COUNT DISTINCT без secid) = 8-29с (нет нужного индекса),
    # поэтому НЕ используем. SBER торгуется каждый день → надёжный прокси.
    md = db.execute(text(
        "SELECT MAX(begin_time)::date FROM candles WHERE secid = 'SBER'"
    )).scalar()
    sources.append(entry("candles:SBER", "intraday", md))

    # open_interest (физ/юр OI) — только max (COUNT(*) по дате ~2с, не нужен)
    md = db.execute(text("SELECT MAX(tradedate) FROM open_interest")).scalar()
    sources.append(entry("open_interest", "intraday", md))

    # fund_data — NAV (T+1, прогрессивный догон)
    md = db.execute(text("SELECT MAX(trade_date) FROM fund_data")).scalar()
    prev = db.execute(text(
        "SELECT MAX(trade_date) FROM fund_data WHERE trade_date < :d"
    ), {"d": md}).scalar()
    sources.append(entry(
        "fund_data", "t_plus_1", md,
        entities_on_max=db.execute(text(
            "SELECT COUNT(DISTINCT fund_id) FROM fund_data WHERE trade_date = :d"
        ), {"d": md}).scalar(),
        entities_on_prev=(db.execute(text(
            "SELECT COUNT(DISTINCT fund_id) FROM fund_data WHERE trade_date = :d"
        ), {"d": prev}).scalar() if prev else None),
        total_entities=db.execute(text("SELECT COUNT(DISTINCT fund_id) FROM fund_data")).scalar(),
    ))

    # stock_market_cap — кап по акциям
    md = db.execute(text("SELECT MAX(period_date) FROM stock_market_cap")).scalar()
    sources.append(entry(
        "stock_market_cap", "close", md,
        entities_on_max=db.execute(text(
            "SELECT COUNT(DISTINCT sec_id) FROM stock_market_cap WHERE period_date = :d"
        ), {"d": md}).scalar(),
    ))

    # breadth_history
    md = db.execute(text("SELECT MAX(trade_date) FROM breadth_history")).scalar()
    sources.append(entry("breadth_history", "close", md))

    # index_data — по каждому secid (close для MOEX, intraday для сырья,
    # t_plus_1 для серий с ночной публикацией ISS)
    for row in db.execute(text(
        "SELECT secid, MAX(trade_date) AS md FROM index_data GROUP BY secid ORDER BY secid"
    )).all():
        if row.secid in _COMMODITY_SECIDS:
            kind = "intraday"
        elif row.secid in _T_PLUS_1_SECIDS:
            kind = "t_plus_1"
        else:
            kind = "close"
        sources.append(entry(f"index:{row.secid}", kind, row.md))

    # macro_data — только живые индикаторы
    for indicator, src, kind in _MACRO:
        if src:
            md = db.execute(text(
                "SELECT MAX(period_date) FROM macro_data WHERE indicator = :i AND source = :s"
            ), {"i": indicator, "s": src}).scalar()
        else:
            md = db.execute(text(
                "SELECT MAX(period_date) FROM macro_data WHERE indicator = :i"
            ), {"i": indicator}).scalar()
        sources.append(entry(f"macro:{indicator}", kind, md))

    # fund_distributions / fund_holdings — свежесть ингеста по updated_at
    md = db.execute(text("SELECT MAX(updated_at)::date FROM fund_distributions")).scalar()
    sources.append(entry("fund_distributions", "weekly", md))
    md = db.execute(text("SELECT MAX(updated_at)::date FROM fund_holdings")).scalar()
    sources.append(entry("fund_holdings", "manual", md))

    # cbr_flows — ручной квартальный ингест
    md = db.execute(text("SELECT MAX(period_end_date) FROM cbr_flows")).scalar()
    sources.append(entry("cbr_flows", "manual", md))

    # ── Pipeline heartbeats — статус ЗАПУСКА скриптов (не только данных) ──
    # Пишется оркестратором/демонами через pipeline_heartbeat.record_pipeline_run.
    # Таблицы может ещё не быть (до первого пульса) → пустой список, не падаем.
    pipelines = []
    silent_pipelines = []
    now_utc = datetime.now(timezone.utc)
    try:
        for r in db.execute(text(
            "SELECT pipeline, last_run_at, last_success_at, last_status, "
            "last_note, last_duration_sec FROM pipeline_runs ORDER BY pipeline"
        )).all():
            run_at = r.last_run_at
            if run_at is not None and run_at.tzinfo is None:
                run_at = run_at.replace(tzinfo=timezone.utc)
            age_h = ((now_utc - run_at).total_seconds() / 3600
                     if run_at is not None else None)
            silent = (age_h is not None and age_h > _PIPELINE_MAX_AGE_H.get(
                r.pipeline, _PIPELINE_DEFAULT_MAX_AGE_H))
            if silent:
                silent_pipelines.append(r.pipeline)
            pipelines.append({
                "pipeline": r.pipeline,
                "last_run_at": r.last_run_at.isoformat() if r.last_run_at else None,
                "last_success_at": r.last_success_at.isoformat() if r.last_success_at else None,
                "status": "silent" if silent else r.last_status,
                "note": (r.last_note or "")[:200],
                "duration_sec": (round(r.last_duration_sec, 1)
                                 if r.last_duration_sec is not None else None),
            })
    except Exception:
        pipelines = []  # таблица ещё не создана (до первого heartbeat)
        silent_pipelines = []

    stale = [s["name"] for s in sources if s["status"] == "stale"]
    failed_pipelines = [p["pipeline"] for p in pipelines
                        if p["status"] not in ("ok", "silent")]
    # overall: fail (пайплайн упал/замолчал) > stale (данные протухли) > ok.
    # Раньше падение пайплайна НЕ отражалось в overall (ключевал только по
    # свежести данных) → упавший скрипт прятался, пока его данные не протухнут.
    # silent (см. _PIPELINE_MAX_AGE_H) — та же лига, что fail: скрипт не бегает.
    overall = ("fail" if (failed_pipelines or silent_pipelines)
               else ("stale" if stale else "ok"))
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "market_ref_date": market_ref.isoformat() if market_ref else None,
        "overall": overall,
        "stale_sources": stale,
        "failed_pipelines": failed_pipelines,
        "silent_pipelines": silent_pipelines,
        "sources": sources,
        "pipelines": pipelines,
    }
