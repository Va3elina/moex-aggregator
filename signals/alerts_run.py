"""
Alert eval-loop (Phase 3) — один проход: проверяет активные алерты, шлёт пуши.
Запуск по cron на ХОСТЕ (Telegram через IPv6). Каждые ~N минут:
  /opt/frame/signals/alerts_run.sh   (cron */10 * * * *)

Для каждого active-алерта считает текущее значение (цена фьючерса = последняя
свеча непрерывного контракта; OI z-score = compute_oi_z), сравнивает с условием
(с учётом last_value для cross), при срабатывании шлёт пуш в user.telegram_chat_id,
пишет alert_fires, обновляет last_fired_at / last_value; mode='once' → status='fired'.

MVP: метрики 'price' (фьючерсы) + 'oi_zscore'. Когда 🔔 появится на акциях/индексах —
добавить ветки цены (candles по sec_id / index_data) в compute_value.
"""
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

# ── .env + DB_URL override ДО импорта api.database (host-side, как alert_bot) ──
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from api.database import SessionLocal               # noqa: E402
from api.models import User, Alert, AlertFire        # noqa: E402
from signals.db import get_latest_price              # noqa: E402
from signals.detectors.oi import (  # noqa: E402
    compute_oi_z, compute_position_atr, compute_participants_atr,
)
from signals.alert_notify import send_message        # noqa: E402

SITE = "https://xn--80aklbnczmv.xn--p1ai"  # punycode таймфрейм.рф (надёжно в TG)

_OP_PRICE = {"cross_up": "↑ пересекла", "cross_down": "↓ пересекла",
             "gt": "выше", "lt": "ниже"}

# Таймфрейм алерта → interval бара-источника «net/npart сейчас». 1d — текущее
# поведение (дневная публикация); 5m/1h — раннее срабатывание ТОГО ЖЕ дневного
# сигнала по последнему внутридневному бару (свежесть ≤5мин / ≤1ч).
_TF_INTERVAL = {"5m": 5, "1h": 60, "1d": 24}


def eval_op(op: str, value: float, prev, threshold: float) -> bool:
    if op == "gt":
        return value > threshold
    if op == "lt":
        return value < threshold
    if op == "cross_up":
        return prev is not None and prev < threshold <= value
    if op == "cross_down":
        return prev is not None and prev > threshold >= value
    return False


def compute_value(a: Alert):
    """(value, ctx) или (None, None). ctx — доп. данные для текста уведомления."""
    if a.indicator == "price":
        # Самая свежая цена (intraday по 5м/60м, EOD по дневной у неликвидных).
        r = get_latest_price(a.asset)
        if not r:
            return None, None
        close, ts, interval = r
        return close, {"interval": interval, "price_ts": ts}
    if a.indicator == "oi_zscore":
        r = compute_oi_z(a.asset, a.clgroup or "FIZ")
        if not r:
            return None, None
        z, last_diff, current_net = r
        return float(z), {"last_diff": last_diff, "current_net": current_net}
    if a.indicator == "oi_move":
        # «Резкое движение позиции» — во сколько раз дневной сдвиг больше обычного (ATR14).
        # clgroup ALL: net зеркален (физ net = −юр net), считаем по FIZ как источнику
        # net, но текст нейтральный («Позиции», без субъекта физ/юр) — флаг neutral.
        # interval по таймфрейму алерта: 1d=дневная публикация, 5m/1h=ранний сигнал
        # по последнему внутридневному бару (тот же дневной сигнал, свежий источник).
        clg = a.clgroup or "FIZ"
        neutral = clg == "ALL"
        interval = _TF_INTERVAL.get(a.timeframe or "1d", 24)
        r = compute_position_atr(a.asset, "FIZ" if neutral else clg, interval=interval)
        if not r:
            return None, None
        ratio, last_diff, current_net, direction, sig_date, legs = r
        return float(ratio), {"last_diff": last_diff, "current_net": current_net,
                              "direction": direction, "neutral": neutral,
                              "signal_date": sig_date, "legs": legs}
    if a.indicator == "oi_participants":
        # «Резко изменилось число участников» — ATR14 по числу участников (npart).
        # FIZ/YUR — независимые счётчики (НЕ зеркальны), самостоятельные сигналы.
        interval = _TF_INTERVAL.get(a.timeframe or "1d", 24)
        r = compute_participants_atr(a.asset, a.clgroup or "FIZ", interval=interval)
        if not r:
            return None, None
        ratio, last_diff, current_npart, direction, sig_date = r
        return float(ratio), {"last_diff": last_diff, "current_npart": current_npart,
                              "direction": direction, "signal_date": sig_date}
    return None, None


def build_keyboard(a: Alert) -> dict:
    """inline_keyboard для пуша. callback_data по общему контракту (<=64б):
    p:<id> пауза, r:<id> возобновить, dx:<id> удалить.
    Строка 1 — ссылка на график; строка 2 — действия (once → 'Включить снова',
    repeat → 'Пауза'), + 'Удалить'."""
    open_btn = {"text": "📈 Открыть график",
                "url": f"{SITE}/oi?instrument={a.asset}"}
    if a.mode == "once":
        # после срабатывания once-алерт станет status='fired' → нужно r:<id>
        action_btn = {"text": "🔔 Включить снова", "callback_data": f"r:{a.id}"}
    else:
        action_btn = {"text": "⏸ Пауза", "callback_data": f"p:{a.id}"}
    delete_btn = {"text": "🗑 Удалить", "callback_data": f"dx:{a.id}"}
    return {"inline_keyboard": [[open_btn], [action_btn, delete_btn]]}


# Нейтральный плейсхолдер-маркер вместо эмодзи/стикеров (фирменный добавим позже).
# Одна строка-заголовок без эмодзи и хэштегов — единый «шильдик» всех алертов.
_MARK = "СИГНАЛ ФРЕЙМ"


def _leg_phrase(direction: str, legs: dict) -> str:
    """Какая нога двинула чистую позицию — когда это однозначно. legs = дневные Δ
    {'long': Δдлинной (+), 'short': Δкороткой (знаковая, короткая хранится −)}.

    net вырос (direction='up') либо за счёт прироста длинной (Δlong>0), либо за счёт
    закрытия короткой (Δshort>0 = модуль шорта уменьшился). net упал — наоборот.
    Берём доминирующую по модулю ногу и описываем честно; если ноги почти равны или
    данных нет — пустая строка (текст останется про чистую позицию, без выдумки)."""
    if not legs:
        return ""
    dl = legs.get("long", 0)
    ds = legs.get("short", 0)
    # Доминирующая нога — у которой больше |Δ|. Требуем заметного перевеса (×1.5),
    # иначе «обе ноги сразу» — не приписываем одну.
    if abs(dl) < 1 and abs(ds) < 1:
        return ""
    if abs(dl) >= abs(ds) * 1.5:
        if dl > 0:
            return "нарастили длинную сторону"
        return "сократили длинную сторону"
    if abs(ds) >= abs(dl) * 1.5:
        # короткая нога: Δshort>0 → шорт закрывают (модуль падает); <0 → шорт наращивают
        if ds > 0:
            return "закрывали короткую сторону"
        return "нарастили короткую сторону"
    return ""


def format_msg(a: Alert, value: float, ctx: dict) -> str:
    name = a.asset_name or a.asset
    link = f'<a href="{SITE}/oi?instrument={a.asset}">открыть график →</a>'
    head = f"<b>{_MARK}</b>\n<b>{name} · {a.asset}</b>"
    thr = float(a.threshold)
    # Таймфрейм-пометка для OI-сигналов: при 5m/1h уточняем, что net взят из
    # внутридневного бара (иначе при разных ТФ на один актив — путаница). И
    # «Δ за день» для интрадей неточно → «Δ с прошлого закрытия».
    tf = a.timeframe or "1d"
    tf_note = {"5m": " · данные 5-мин", "1h": " · данные часовые"}.get(tf, "")
    diff_label = "Δ за день" if tf == "1d" else "Δ с прошлого закрытия"
    if a.indicator == "price":
        word = _OP_PRICE.get(a.op, a.op)
        eod = "\n<i>(дневная свеча — у актива нет внутридневных данных)</i>" \
            if ctx.get("interval") == 24 else ""
        return (f"{head}\n"
                f"Цена {word} отметку {thr:g} ₽ — сейчас {value:g} ₽{eod}\n{link}")
    if a.indicator == "oi_zscore":
        clg = "физлиц" if (a.clgroup or "FIZ") == "FIZ" else "юрлиц"
        diff = ctx.get("last_diff", 0)
        verb = "нарастили чистую позицию" if diff > 0 else "сократили чистую позицию"
        return (f"{head} — открытые позиции\n"
                f"Аномалия в позициях {clg}: за день {verb} резче обычного.\n"
                f"Чистая позиция изменилась на {diff:+,} контрактов за день.\n{link}")
    if a.indicator == "oi_move":
        diff = ctx.get("last_diff", 0)
        direction = ctx.get("direction", "up")
        leg = _leg_phrase(direction, ctx.get("legs") or {})
        leg_note = f" ({leg})" if leg else ""
        if ctx.get("neutral"):
            # clgroup ALL — нейтральный текст, без субъекта физ/юр в роли действующего.
            move = "выросла" if direction == "up" else "снизилась"
            return (f"{head} — открытые позиции{tf_note}\n"
                    f"Чистая позиция {move} резче обычного — в {value:g}× "
                    f"сильнее среднего дневного шага за 14 дней (ваш порог {thr:g}×).\n"
                    f"{diff_label}: {diff:+,} контрактов{leg_note}.\n{link}")
        clg = "Физлица" if (a.clgroup or "FIZ") == "FIZ" else "Юрлица"
        word = "резко нарастили чистую позицию" if direction == "up" \
            else "резко сократили чистую позицию"
        return (f"{head} — открытые позиции{tf_note}\n"
                f"{clg} {word} — сдвиг в {value:g}× сильнее обычного дневного шага "
                f"за 14 дней (ваш порог {thr:g}×).\n"
                f"{diff_label}: {diff:+,} контрактов{leg_note}.\n{link}")
    if a.indicator == "oi_participants":
        clg = "физлиц" if (a.clgroup or "FIZ") == "FIZ" else "юрлиц"
        diff = ctx.get("last_diff", 0)
        npart = ctx.get("current_npart")
        flow = "Приток" if ctx.get("direction") == "up" else "Отток"
        ctx_line = f"\nВсего в позиции сейчас {npart:,} участников." if npart else ""
        return (f"{head} — открытые позиции{tf_note}\n"
                f"{flow} {clg} в фьючерсе резче обычного — в {value:g}× сильнее "
                f"среднего дневного шага за 14 дней (ваш порог {thr:g}×).\n"
                f"{diff_label}: {diff:+,} участников.{ctx_line}\n{link}")
    return f"{head}\nАлерт сработал.\n{link}"


def run_once() -> dict:
    summary = {"checked": 0, "fired": 0, "skipped": 0, "unlinked": 0, "errors": 0}
    db = SessionLocal()
    try:
        alerts = db.query(Alert).filter(Alert.status == "active").all()
        now = datetime.now(timezone.utc)
        # Префетч юзеров одним запросом (а не по одному на алерт).
        uids = {a.user_id for a in alerts}
        users = {u.id: u for u in db.query(User).filter(User.id.in_(uids)).all()} if uids else {}
        # Дедуп: значение метрики считаем ОДИН раз на (indicator, asset, clgroup,
        # timeframe) за проход — 50 алертов на SR-цену = 1 запрос, а не 50. timeframe
        # в ключе обязателен: 1d и 5m на один актив дают РАЗНЫЕ значения (дневное
        # закрытие vs бегущий внутридневной бар) — иначе перепутались бы.
        value_cache: dict = {}
        for a in alerts:
            summary["checked"] += 1
            try:
                user = users.get(a.user_id)
                if not user or not user.telegram_chat_id:
                    summary["skipped"] += 1   # не привязан Telegram — доставить некуда
                    continue
                ck = (a.indicator, a.asset, a.clgroup or "", a.timeframe or "1d")
                if ck in value_cache:
                    value, ctx = value_cache[ck]
                else:
                    value, ctx = compute_value(a)
                    value_cache[ck] = (value, ctx)
                if value is None:
                    summary["skipped"] += 1
                    continue
                # Гейт «новый торговый день» для дневных OI-метрик: данные
                # обновляются раз в день (лаг + выходные). Не пере-выстреливаем
                # ОДНУ И ТУ ЖЕ дату данных — иначе застрявший дневной экстремум
                # долбил бы каждые cooldown_hours, пока не придут новые. У цены
                # (intraday) signal_date нет → её это не трогает.
                sig_date = (ctx or {}).get("signal_date")
                if sig_date is not None and a.last_fired_date is not None \
                        and sig_date <= a.last_fired_date:
                    a.last_value = value
                    summary["skipped"] += 1
                    continue
                # Cooldown для mode='repeat': не чаще раза в cooldown_hours. last_value
                # обновляем (трекаем prev для cross), но НЕ шлём, пока кулдаун не вышел.
                if a.mode == "repeat" and a.last_fired_at is not None:
                    elapsed = (now - a.last_fired_at).total_seconds()
                    if elapsed < (a.cooldown_hours or 24) * 3600:
                        a.last_value = value
                        continue
                prev = float(a.last_value) if a.last_value is not None else None
                send_failed = False
                if eval_op(a.op, value, prev, float(a.threshold)):
                    text = format_msg(a, value, ctx or {})
                    result = send_message(user.telegram_chat_id, text,
                                          reply_markup=build_keyboard(a))
                    if result == "ok":
                        db.add(AlertFire(alert_id=a.id, value=value, message_text=text))
                        a.last_fired_at = now
                        if sig_date is not None:
                            a.last_fired_date = sig_date   # гейт «новый день»
                        if a.mode == "once":
                            a.status = "fired"
                        summary["fired"] += 1
                    elif result == "blocked":
                        # юзер забанил бота / чат мёртв → авто-отвязка, чтобы не
                        # долбить впустую. Алерт остаётся, но без chat_id eval его
                        # скипнет до повторной привязки на сайте.
                        user.telegram_chat_id = None
                        user.telegram_username = None
                        user.telegram_linked_at = None
                        summary["unlinked"] += 1
                        send_failed = True
                        print(f"[alerts_run] user {user.id} blocked bot → auto-unlinked")
                    else:  # 'error' — временная ошибка отправки
                        send_failed = True
                # last_value НЕ двигаем при неудачной отправке — иначе cross_up/down
                # не пере-сработает (prev уже за порогом). При успехе/без срабатывания — двигаем.
                if not send_failed:
                    a.last_value = value
            except Exception as e:
                summary["errors"] += 1
                print(f"[alerts_run] alert {a.id} error: {e}")
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"[alerts_run] fatal: {e}")
    finally:
        db.close()
    return summary


def main():
    s = run_once()
    print(f"[{datetime.now(timezone.utc)}] alerts_run: {s}")


if __name__ == "__main__":
    main()
