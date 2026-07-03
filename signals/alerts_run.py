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
import json
import urllib.parse
from datetime import datetime, timezone, date

from dotenv import load_dotenv
from sqlalchemy import text, bindparam

# ── .env + DB_URL override ДО импорта api.database (host-side, как alert_bot) ──
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from api.database import SessionLocal, engine       # noqa: E402
from api.models import User, Alert, AlertFire        # noqa: E402
from signals.db import get_latest_price              # noqa: E402
from signals.detectors.oi import (  # noqa: E402
    compute_oi_z, compute_position_atr, compute_participants_atr,
)
from signals.detectors.funds import compute_fund_flow_atr  # noqa: E402
from signals.alert_notify import send_message        # noqa: E402
from signals.email_notify import send_email          # noqa: E402

SITE = "https://xn--80aklbnczmv.xn--p1ai"  # punycode таймфрейм.рф (надёжно в TG)

_OP_PRICE = {"cross_up": "↑ пересекла", "cross_down": "↓ пересекла",
             "gt": "выше", "lt": "ниже"}

# Таймфрейм алерта → interval бара-источника «net/npart сейчас». 1d — текущее
# поведение (дневная публикация); 5m/1h — раннее срабатывание ТОГО ЖЕ дневного
# сигнала по последнему внутридневному бару (свежесть ≤5мин / ≤1ч).
_TF_INTERVAL = {"5m": 5, "1h": 60, "1d": 24}


def _oi_url(a) -> str:
    """Диплинк на /oi с контекстом сигнала: актив + физ/юр + таймфрейм + режим/
    вариант — чтобы страница (вкл. мобилку) открылась ровно в том виде, о котором
    пришёл сигнал, а не на дефолтном Сбербанке. Фронт читает эти параметры в
    parseOiDeepLink (frontend/src/utils/oiDeepLink.ts)."""
    params = [("instrument", a.asset)]
    if a.clgroup in ("FIZ", "YUR"):
        params.append(("clgroup", a.clgroup))
    elif a.clgroup == "ALL":
        # «В целом» (ALL) считается от физлиц и текст ведёт от них → и график
        # открываем на физлицах, чтобы направление совпало с тем, что в сигнале.
        params.append(("clgroup", "FIZ"))
    iv = _TF_INTERVAL.get(a.timeframe or "1d")
    if iv:
        params.append(("interval", str(iv)))
    if a.indicator == "oi_participants":
        params.append(("mode", "participants"))
    elif a.indicator == "price":
        params.append(("mode", "price"))
    else:  # oi_move / oi_zscore / oi_level — чистые позиции
        params.append(("mode", "positions"))
        params.append(("variant", "net"))
    return f"{SITE}/oi?" + urllib.parse.urlencode(params)


_FUND_CATEGORIES = {"money_market", "stocks", "bonds", "gold", "yuan"}


def _funds_resolve(fund_ids):
    """Набор fund_id → (доминирующая категория, fund_id'ы ИЗ этой категории). Страница
    /funds-money категорийная — ведём на категорию с наибольшим числом фондов набора
    и изолируем именно их. (None, []) если БД недоступна / фонды не найдены."""
    try:
        stmt = (text("SELECT fund_id, category FROM funds WHERE fund_id IN :ids")
                .bindparams(bindparam("ids", expanding=True)))
        with SessionLocal() as s:
            rows = s.execute(stmt, {"ids": fund_ids}).fetchall()
    except Exception:
        return None, []
    by_cat: dict = {}
    for fid, cat in rows:
        by_cat.setdefault(cat, []).append(fid)
    if not by_cat:
        return None, []
    dom = max(by_cat, key=lambda c: len(by_cat[c]))
    return dom, by_cat[dom]


def _funds_url(a) -> str:
    """Диплинк на /funds-money. Страница категорийная (один раздел за раз):
    - сигнал по категории → ?category=<cat>;
    - 'custom'-набор → доминирующая категория + ?funds= (изоляция именно сигнальных
      фондов через пофондовую видимость; кросс-категорийный хвост набора показать
      нельзя — берём самую представленную категорию);
    - 'all' / нерезолвимое → голый /funds-money (агрегатного «все фонды» вида нет).
    Фронт читает ?category=/?funds= на маунте обеих страниц фондов."""
    if a.asset in _FUND_CATEGORIES:
        return f"{SITE}/funds-money?category={a.asset}"
    fund_ids = _parse_fund_ids(a.fund_ids)
    if a.asset == "custom" and fund_ids:
        cat, in_cat = _funds_resolve(fund_ids)
        if cat:
            funds_q = ("&funds=" + ",".join(str(i) for i in in_cat)) if in_cat else ""
            return f"{SITE}/funds-money?category={cat}{funds_q}"
    return f"{SITE}/funds-money"


def _parse_fund_ids(raw) -> list | None:
    """CSV из fund_id (колонка alerts.fund_ids) → list[int]; пусто/None → None.
    None = таргет задаётся через a.asset (категория или 'all'), а не явный набор.
    Невалидные элементы тихо пропускаем; если ничего не осталось → None."""
    if not raw:
        return None
    ids = []
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            continue
    return ids or None


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
    if a.indicator == "oi_level":
        # Уровень открытого интереса — чистая позиция (контракты, знаковая) для
        # алерта на пересечение/выше/ниже (TradingView-стиль на ПРАВОЙ оси графика
        # ОИ). clgroup FIZ/YUR (ALL→FIZ, канонический источник net). interval по
        # таймфрейму: 1d — дневная публикация, 5m/1h — «net сейчас» по последнему бару.
        from signals.db import get_position_series
        clg = a.clgroup or "FIZ"
        if clg == "ALL":
            clg = "FIZ"
        interval = _TF_INTERVAL.get(a.timeframe or "1d", 24)
        series = get_position_series(a.asset, clg, days=7, interval=interval)
        if not series:
            return None, None
        sig_date, net = series[-1][0], series[-1][1]
        return float(net), {"current_net": net, "signal_date": sig_date}
    if a.indicator == "funds_flow":
        # «Аномальный поток» — во сколько раз дневной net_flow набора фондов больше
        # обычного (ATR14). Таргет (контракт fund-алерта):
        #   • a.fund_ids задан (CSV) → именно эти фонды (asset='custom');
        #   • a.asset ∈ {'all','custom'} → category=None (все фонды; для custom
        #     fund_ids уже задают набор, category игнорируется);
        #   • иначе a.asset = ключ категории (money_market/stocks/bonds/gold) →
        #     вся категория (backward-compat со старыми fund-алертами).
        # direction: 'up' приток / 'down' отток. Дневная метрика → signal_date для
        # гейта «новый торговый день» (как у oi_move).
        fund_ids = _parse_fund_ids(a.fund_ids)
        category = None if a.asset in ("all", "custom") else a.asset
        r = compute_fund_flow_atr(fund_ids, category)
        if not r:
            return None, None
        ratio, direction, last_flow, sig_date = r
        return float(ratio), {"direction": direction, "last_flow": last_flow,
                              "asset": a.asset, "fund_ids": fund_ids,
                              "signal_date": sig_date}
    return None, None


def build_keyboard(a: Alert) -> dict:
    """inline_keyboard для пуша. callback_data по общему контракту (<=64б):
    p:<id> пауза, r:<id> возобновить, dx:<id> удалить.
    Строка 1 — ссылка на график; строка 2 — действия (once → 'Включить снова',
    repeat → 'Пауза'), + 'Удалить'."""
    # Ссылка кнопки по источнику: fund-алерт ведёт на /funds-money, OI — на /oi.
    open_url = (_funds_url(a) if a.indicator == "funds_flow"
                else _oi_url(a))
    open_btn = {"text": "📈 Открыть график", "url": open_url}
    if a.mode == "once":
        # после срабатывания once-алерт станет status='fired' → нужно r:<id>
        action_btn = {"text": "🔔 Включить снова", "callback_data": f"r:{a.id}"}
    else:
        action_btn = {"text": "⏸ Пауза", "callback_data": f"p:{a.id}"}
    delete_btn = {"text": "🗑 Удалить", "callback_data": f"dx:{a.id}"}
    return {"inline_keyboard": [[open_btn], [action_btn, delete_btn]]}


# Фирменный «шильдик» всех алертов: кастом-эмодзи Frame + текст.
_MARK = "СИГНАЛ ФРЕЙМ"


# Кастом-эмодзи Frame в HTML-режиме (alert_notify шлёт с parse_mode=HTML).
# ph — emoji-фолбэк, который видят не-Premium получатели. id из пака
# t.me/addemoji/Frametool (вытащены через getStickerSet, 2026-06-17).
def _ce(eid: str, ph: str) -> str:
    return f'<tg-emoji emoji-id="{eid}">{ph}</tg-emoji>'


_EMO_SIGNAL = ("5454063456045012758", "🔔")   # «сигнал» — заголовок
_EMO_UP     = ("5454034507965444800", "📈")   # рост / приток
_EMO_DOWN   = ("5454074292247499750", "⛔️")   # падение / отток
_EMO_OI     = ("5454099499410561256", "🔄")   # открытые позиции
_EMO_FUNDS  = ("5454114055054728462", "💼")   # фонды
_EMO_PRICE  = ("5454013243582356952", "💵")   # цена

# Человекочитаемые имена категорий фондов (зеркало CATEGORY_INDEX_MAP в API).
_FUND_CAT_NAME = {
    "money_market": "Денежный рынок",
    "stocks": "Акции",
    "bonds": "Облигации",
    "gold": "Золото",
    "yuan": "Юань",
}

# Тип-«плашка» в шапке сигнала — ЖИРНЫМ словом, чтобы тип (ОИ / цена / фонды)
# читался с первого взгляда. Кастом-эмодзи у нас одноцветные (оранжевый пак Frame)
# и тип НЕ различают — значки 🔄/💼 рядом не контрастируют. Поэтому различаем
# именно жирным ТЕКСТОМ, а не значком.
_TYPE_OI    = "ОТКРЫТЫЙ ИНТЕРЕС"
_TYPE_PRICE = "ЦЕНА ФЬЮЧЕРСА"
_TYPE_FUNDS = "ДЕНЬГИ В ФОНДАХ"


def _head(type_label: str, subtitle: str, note: str = "") -> str:
    """Шапка сигнала: бренд + ЖИРНЫЙ тег типа первой строкой, предмет сигнала
    (актив/категория) — второй. Тип различаем текстом, а не эмодзи."""
    return (f"{_ce(*_EMO_SIGNAL)} <b>{_MARK} · {type_label}</b>\n"
            f"<b>{subtitle}</b>{note}")


_MONTHS_RU = ("", "января", "февраля", "марта", "апреля", "мая", "июня",
              "июля", "августа", "сентября", "октября", "ноября", "декабря")


def _date_note(ctx: dict) -> str:
    """Отдельная строка с торговым днём, за который посчитан сигнал (signal_date из
    ctx) — курсивом. СЧА фондов и позиции МосБиржи публикуются с лагом (~T+1),
    поэтому важно показать, за какой именно день аномалия, а не «сегодня». Пусто,
    если даты в ctx нет (price — реальное время; oi_zscore — без даты)."""
    d = (ctx or {}).get("signal_date")
    if not d:
        return ""
    return f"\n<i>по данным за {d.day} {_MONTHS_RU[d.month]}</i>"


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
    link = f'<a href="{_oi_url(a)}">открыть график →</a>'
    subj = f"{name} · {a.asset}"            # предмет OI/цены: имя · тикер
    thr = float(a.threshold)
    # Таймфрейм-пометка для OI-сигналов: при 5m/1h уточняем, что net взят из
    # внутридневного бара (иначе при разных ТФ на один актив — путаница).
    tf = a.timeframe or "1d"
    tf_note = {"5m": " · данные 5-мин", "1h": " · данные часовые"}.get(tf, "")
    if a.indicator == "price":
        word = _OP_PRICE.get(a.op, a.op)
        eod = "\n<i>(дневная свеча — у актива нет внутридневных данных)</i>" \
            if ctx.get("interval") == 24 else ""
        return (f"{_head(_TYPE_PRICE, subj)}\n"
                f"{_ce(*_EMO_PRICE)} Цена {word} отметку {thr:g} ₽ — сейчас {value:g} ₽{eod}\n{link}")
    if a.indicator == "oi_level":
        # Уровень открытого интереса (чистая позиция) пересёк/выше/ниже отметки.
        clg = "физлиц" if (a.clgroup or "FIZ") in ("FIZ", "ALL") else "юрлиц"
        word = _OP_PRICE.get(a.op, a.op)
        return (f"{_head(_TYPE_OI, subj, tf_note)}\n"
                f"{_ce(*_EMO_OI)} Чистая позиция {clg} {word} отметку {thr:,.0f} — "
                f"сейчас {value:,.0f} контрактов.{_date_note(ctx)}\n{link}")
    # Принцип текста (правка Вадима): ГЛАВНОЕ — насколько аномально (×N к обычному
    # дневному шагу) и в какую сторону. Тип сигнала («Открытый интерес») вынесен в
    # ЖИРНУЮ плашку шапки — раньше был значок 🔄 + «открытые позиции», но эмодзи
    # одноцветные и тип не различали.
    if a.indicator == "oi_zscore":
        clg = "Физлица" if (a.clgroup or "FIZ") == "FIZ" else "Юрлица"
        diff = ctx.get("last_diff", 0)
        word = "резко нарастили позицию" if diff > 0 else "резко сократили позицию"
        dir_emo = _ce(*_EMO_UP) if diff > 0 else _ce(*_EMO_DOWN)
        return (f"{_head(_TYPE_OI, subj)}\n"
                f"{dir_emo} {clg} {word} — аномально резкий дневной сдвиг.\n{link}")
    if a.indicator == "oi_move":
        direction = ctx.get("direction", "up")
        dir_emo = _ce(*_EMO_UP) if direction == "up" else _ce(*_EMO_DOWN)
        head = _head(_TYPE_OI, subj, tf_note)
        if ctx.get("neutral"):
            # clgroup ALL («в целом»). Чистую позицию считаем со стороны физлиц
            # (net зеркален: net физлиц = −net юрлиц). Называем ТОЛЬКО физлиц (тех,
            # кого измерили); зеркальную сторону юрлиц НЕ утверждаем. Диплинк ведёт
            # на физлиц, совпадая с текстом.
            word = "резко нарастили позицию" if direction == "up" else "резко сократили позицию"
            return (f"{head}\n"
                    f"{dir_emo} Физлица {word} — в {value:g}× резче обычного (порог {thr:g}×).{_date_note(ctx)}\n{link}")
        clg = "Физлица" if (a.clgroup or "FIZ") == "FIZ" else "Юрлица"
        word = "резко нарастили позицию" if direction == "up" else "резко сократили позицию"
        return (f"{head}\n"
                f"{dir_emo} {clg} {word} — в {value:g}× резче обычного (порог {thr:g}×).{_date_note(ctx)}\n{link}")
    if a.indicator == "oi_participants":
        clg = "физлиц" if (a.clgroup or "FIZ") == "FIZ" else "юрлиц"
        npart = ctx.get("current_npart")
        up = ctx.get("direction") == "up"
        flow = "Приток" if up else "Отток"
        dir_emo = _ce(*_EMO_UP) if up else _ce(*_EMO_DOWN)
        ctx_line = f" Сейчас {npart:,} участников." if npart else ""
        return (f"{_head(_TYPE_OI, subj, tf_note)}\n"
                f"{dir_emo} {flow} {clg} — в {value:g}× резче обычного (порог {thr:g}×).{ctx_line}{_date_note(ctx)}\n{link}")
    if a.indicator == "funds_flow":
        # «Аномальный поток» по выбранному набору фондов. Метка выбора в шапку
        # (a.asset_name: «Денежный рынок» / «Все фонды» / «N фондов»), с fallback
        # на имя категории. Тип — ЖИРНАЯ плашка «ДЕНЬГИ В ФОНДАХ» (раньше был значок
        # 💼 + «притоки-оттоки»). Главное в тексте — ×N + направление.
        label = a.asset_name or _FUND_CAT_NAME.get(a.asset, a.asset)
        funds_link = f'<a href="{_funds_url(a)}">открыть график →</a>'
        up = ctx.get("direction") == "up"
        flow_word = "ПРИТОК" if up else "ОТТОК"
        dir_emo = _ce(*_EMO_UP) if up else _ce(*_EMO_DOWN)
        return (f"{_head(_TYPE_FUNDS, label)}\n"
                f"{dir_emo} Резкий {flow_word} — в {value:g}× больше обычного (порог {thr:g}×).{_date_note(ctx)}\n{funds_link}")
    return (f"{_ce(*_EMO_SIGNAL)} <b>{_MARK}</b>\n<b>{subj}</b>\n"
            f"Алерт сработал.\n{link}")


# ─── Phase 2b: write-through личного fire в ленту аномалий сайта ──────────────
# При срабатывании личного алерта пишем scope='personal'-строку в `anomalies` →
# в колоколе/тосте появляется бейдж «ваш сигнал», и личные аномалии вне публичного
# скана тоже попадают в ленту. Только ×N-типы (oi_move/oi_participants/funds_flow);
# price и oi_zscore (другая единица измерения) — НЕ в ленту. НИКОГДА не ломает
# доставку алерта: отдельная сессия + try/except (см. _write_personal_anomaly).
_ANOMALY_TYPES = {"oi_move", "oi_participants", "funds_flow"}

_PERSONAL_ANOMALY_SQL = text("""
  INSERT INTO anomalies (scope, user_id, type, asset_id, asset_name, clgroup,
                         direction, headline, context, severity_value, signal_date, deep_link)
  VALUES ('personal', :user_id, :type, :asset_id, :asset_name, :clgroup, :direction,
          :headline, :context, :severity_value, :signal_date, CAST(:deep_link AS JSONB))
  ON CONFLICT DO NOTHING
""")


def _anomaly_clgroup(a) -> str | None:
    """clgroup строки ленты, канонизированный КАК ПУБЛИЧНЫЙ СКАН: OI меряется со
    стороны физлиц (net зеркален, см. anomaly_scan), поэтому ALL/None → FIZ.
    Иначе DISTINCT ON в /feed не свернёт личную (alert.clgroup часто = 'ALL') и
    публичную (всегда 'FIZ') строки одного движения — и пользователь видит дубль
    «мой + системный сигнал» (баг 2026-06-26). Явный YUR — отдельная сторона
    (другой текст/направление), его не схлопываем. Фонды clgroup не имеют → None."""
    if a.indicator == "funds_flow":
        return None
    return a.clgroup if a.clgroup in ("FIZ", "YUR") else "FIZ"


def _anomaly_deep_link(a) -> dict:
    if a.indicator == "funds_flow":
        cat = a.asset if a.asset in _FUND_CATEGORIES else None
        return {"route": "/funds-money", "category": cat} if cat else {"route": "/funds-money"}
    clg = _anomaly_clgroup(a)
    iv = _TF_INTERVAL.get(a.timeframe or "1d", 24)
    mode = "participants" if a.indicator == "oi_participants" else "positions"
    return {"route": "/oi", "secid": a.asset, "clgroup": clg, "interval": iv,
            "mode": mode, "variant": "net"}


def _anomaly_text(a, value: float, ctx: dict) -> tuple:
    direction = (ctx or {}).get("direction")
    if a.indicator == "funds_flow":
        name = a.asset_name or _FUND_CAT_NAME.get(a.asset, a.asset)
        word = "приток" if direction == "up" else "отток"
        return f"Резкий {word} — {name}", f"×{value:g} к обычному дневному потоку"
    if a.indicator == "oi_participants":
        clg = "физлиц" if (a.clgroup or "FIZ") == "FIZ" else "юрлиц"
        flow = "Приток" if direction == "up" else "Отток"
        return f"{flow} участников ({clg})", f"×{value:g} к обычному"
    # oi_move
    clg = "Физлица" if (a.clgroup or "FIZ") in ("FIZ", "ALL") else "Юрлица"
    word = "резко нарастили позицию" if direction == "up" else "резко сократили позицию"
    return f"{clg} {word}", f"×{value:g} к обычному дневному шагу"


def _write_personal_anomaly(a, value: float, ctx: dict, sig_date) -> None:
    """Личный fire → строка anomalies (scope='personal') для ленты сайта. Пишем в
    ОТДЕЛЬНОЙ сессии (полная изоляция) + try/except: ошибка ленты физически НЕ может
    затронуть транзакцию алерта (fire/AlertFire/доставку в Telegram)."""
    if a.indicator not in _ANOMALY_TYPES:
        return  # price / oi_zscore — не аномалия для ленты
    direction = (ctx or {}).get("direction") or ("up" if value > 0 else "down")
    headline, context = _anomaly_text(a, value, ctx)
    params = {
        "user_id": a.user_id, "type": a.indicator, "asset_id": a.asset,
        "asset_name": a.asset_name, "clgroup": _anomaly_clgroup(a), "direction": direction,
        "headline": headline, "context": context, "severity_value": value,
        "signal_date": sig_date or date.today(),
        "deep_link": json.dumps(_anomaly_deep_link(a), ensure_ascii=False),
    }
    try:
        with SessionLocal() as s2:
            s2.execute(_PERSONAL_ANOMALY_SQL, params)
            s2.execute(text("SELECT pg_notify('anomaly', :p)"),
                       {"p": json.dumps({"source": "anomaly", "id": 0})})
            s2.commit()
    except Exception as e:
        print(f"[alerts_run] personal anomaly write failed (alert {a.id}): {e}")


_ALERT_CHANNELS = ("telegram", "site", "email")


def _alert_channels(a) -> set:
    """CSV alerts.channels → множество каналов. Пусто → {'telegram'} (совместимость
    со старыми алертами без колонки)."""
    raw = getattr(a, "channels", None) or "telegram"
    present = {p.strip() for p in str(raw).split(",") if p.strip()}
    sel = {c for c in _ALERT_CHANNELS if c in present}
    return sel or {"telegram"}


def _email_ok(user) -> bool:
    email = (getattr(user, "email", "") or "")
    return bool(getattr(user, "is_verified", False)) and not email.endswith("@oauth.local")


def _notify_alert_fire(a) -> None:
    """Site-канал: nudge фронту через SSE, что у юзера сработал алерт (сам fire уже
    durable в alert_fires). Broadcast несёт ТОЛЬКО user_id — контент фронт тянет
    сам user-scoped запросом (/api/alerts/recent-fires), без утечки в общий SSE.
    Отдельная сессия + try/except — не влияет на доставку."""
    try:
        with SessionLocal() as s2:
            s2.execute(text("SELECT pg_notify('alert_fire', :p)"),
                       {"p": json.dumps({"source": "alert_fire", "user_id": a.user_id})})
            s2.commit()
    except Exception as e:
        print(f"[alerts_run] alert_fire notify failed (alert {a.id}): {e}")


_RUN_LOCK_KEY = 0x616C7274  # 'alrt' — id сессионного advisory-lock прогона alerts_run


def run_once() -> dict:
    summary = {"checked": 0, "fired": 0, "skipped": 0, "unlinked": 0, "errors": 0}
    # ЗАЩИТА ОТ НАЛОЖЕНИЯ ПРОГОНОВ (PR #208). reserve-перед-send (PR #207) спасает от
    # ПОСЛЕДОВАТЕЛЬНОГО сбоя коммита, но два ОДНОВРЕМЕННЫХ прогона (cron-tick поверх
    # затянувшегося >5мин прогона, либо ручной `python -m signals.alerts_run` во время
    # крона) прочли бы старый last_fired_date ДО любого коммита и оба отправили бы дубль.
    # Сессионный advisory-lock на ВЫДЕЛЕННОМ соединении: держим его открытым весь прогон
    # независимо от mid-loop commit'ов основной сессии; закрытие соединения = авто-релиз
    # замка даже при падении процесса. Не взяли → другой прогон активен → пропускаем тик
    # (следующий подхватит). exec_driver_sql, т.к. `text` шадоумится в цикле (text=format_msg).
    try:
        lock_conn = engine.connect()
    except Exception as e:
        print(f"[alerts_run] cannot connect for run-lock: {e}")
        return summary
    got = lock_conn.exec_driver_sql(
        f"SELECT pg_try_advisory_lock({_RUN_LOCK_KEY})").scalar()
    lock_conn.commit()   # завершаем tx; session-level замок живёт на соединении до close
    if not got:
        lock_conn.close()
        print("[alerts_run] overlap: another run holds the lock → skip this tick")
        summary["skipped_locked"] = 1
        return summary
    db = SessionLocal()
    try:
        alerts = db.query(Alert).filter(Alert.status == "active").all()
        now = datetime.now(timezone.utc)
        # Префетч юзеров одним запросом (а не по одному на алерт).
        uids = {a.user_id for a in alerts}
        users = {u.id: u for u in db.query(User).filter(User.id.in_(uids)).all()} if uids else {}
        # Дедуп: значение метрики считаем ОДИН раз на (indicator, asset, clgroup,
        # timeframe, fund_ids) за проход — 50 алертов на SR-цену = 1 запрос, а не 50.
        # timeframe в ключе обязателен: 1d и 5m на один актив дают РАЗНЫЕ значения
        # (дневное закрытие vs бегущий внутридневной бар) — иначе перепутались бы.
        # fund_ids в ключе: два custom-fund-алерта с asset='custom', но разными
        # наборами фондов — РАЗНЫЕ значения, не должны делить кэш (для не-fund
        # алертов fund_ids=NULL → нормализуется в "" → ключ как раньше).
        value_cache: dict = {}
        for a in alerts:
            summary["checked"] += 1
            try:
                user = users.get(a.user_id)
                if not user:
                    summary["skipped"] += 1
                    continue
                # Куда реально можем доставить: site — всегда; telegram — если
                # привязан; email — если подтверждён. Нет ни одного пригодного
                # канала (напр. только telegram, но не привязан) → пропускаем.
                channels = _alert_channels(a)
                can_deliver = ("site" in channels) \
                    or ("telegram" in channels and user.telegram_chat_id) \
                    or ("email" in channels and _email_ok(user))
                if not can_deliver:
                    summary["skipped"] += 1
                    continue
                ck = (a.indicator, a.asset, a.clgroup or "", a.timeframe or "1d",
                      (a.fund_ids or "") if a.indicator == "funds_flow" else "")
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
                    # РЕЗЕРВ-ПЕРЕД-ОТПРАВКОЙ (фикс дубля 2026-06-26). send необратим и
                    # НЕ трогает БД. Раньше выстрел фиксировался ПОСЛЕ send одним
                    # батч-коммитом в конце прогона — и если тот срывался (БД-блип на
                    # 4ГБ-хосте / OOM-kill / «ядовитый» алерт ронял коммит всей пачки),
                    # сообщение уже ушло, а гейт last_fired_date оставался пуст →
                    # следующий 5-мин прогон слал ДУБЛЬ (VI пришёл 06:12 и 06:17, fire
                    # записан ОДИН; ни AlertFire, ни personal-anomaly за 06:12 — значит
                    # БД была недоступна, а relay-send прошёл). Теперь пишем гейт и
                    # коммитим ДО send: reserve не прошёл → НЕ отправляем (повтор позже,
                    # без дубля); отправка не удалась → откатываем резерв (БД жива, раз
                    # reserve закоммитился) → честный повтор без потери сигнала.
                    prev_state = (a.last_fired_at, a.last_fired_date, a.status)
                    fire = AlertFire(alert_id=a.id, value=value, message_text=text)
                    db.add(fire)
                    a.last_fired_at = now
                    if sig_date is not None:
                        a.last_fired_date = sig_date   # гейт «новый день»
                    if a.mode == "once":
                        a.status = "fired"
                    try:
                        db.commit()                    # резерв durable ДО отправки
                    except Exception as e:
                        db.rollback()                  # БД недоступна → НЕ шлём, повтор позже
                        summary["errors"] += 1
                        print(f"[alerts_run] reserve failed (alert {a.id}): {e}")
                        continue
                    # МУЛЬТИКАНАЛ. Резерв уже durable → доставляем по каждому каналу
                    # из a.channels. delivered = хотя бы один канал доставил; если ВСЕ
                    # провалились → откат резерва (честный повтор), как раньше при
                    # неудачном TG-send. Инвариант дубля цел: гейт закоммичен ДО отправки.
                    delivered = False
                    # site — самый надёжный: fire уже в alert_fires, шлём nudge фронту.
                    if "site" in channels:
                        _notify_alert_fire(a)
                        delivered = True
                    # telegram — как раньше; blocked → авто-отвязка (даже если site/email
                    # доставили: бан бота — устойчивое состояние, чистим привязку).
                    if "telegram" in channels and user.telegram_chat_id:
                        result = send_message(user.telegram_chat_id, text,
                                              reply_markup=build_keyboard(a))
                        if result == "ok":
                            delivered = True
                        elif result == "blocked":
                            user.telegram_chat_id = None
                            user.telegram_username = None
                            user.telegram_linked_at = None
                            summary["unlinked"] += 1
                            print(f"[alerts_run] user {user.id} blocked bot → auto-unlinked")
                        # result == 'error' → этот канал не доставил (delivered от него не растёт)
                    # email — за гейтом SMTP: 'skip' если не настроен (не влияет на delivered).
                    if "email" in channels and _email_ok(user):
                        if send_email(user.email, "Сработал алерт · Фрейм", text) == "ok":
                            delivered = True

                    if delivered:
                        _write_personal_anomaly(a, value, ctx or {}, sig_date)
                        summary["fired"] += 1
                    else:
                        # ни один канал не доставил → откатываем резерв (не занятый
                        # гейт, не статус 'fired' у mode='once') для честного повтора.
                        db.delete(fire)
                        a.last_fired_at, a.last_fired_date, a.status = prev_state
                        db.commit()
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
        lock_conn.close()   # авто-релиз session advisory-lock (даже при падении прогона)
    return summary


def main():
    s = run_once()
    print(f"[{datetime.now(timezone.utc)}] alerts_run: {s}")


if __name__ == "__main__":
    main()
