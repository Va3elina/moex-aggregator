"""Находки движка → кандидаты завода постов: жанр «пост от наших данных», без новости.

Раз на торговый день, как только его дневные позиции в базе (cron 30 7-17 * * * UTC, сканер ждёт день и не
повторяется — signals/insights/fresh.py; пятница приходит в субботу): детекторы
signals/insights/detect.py по рядам базы → рейтинг → лучшие находки трёх типов (позиции физлиц,
потоки в фонды, сезонность) → фильтры повторов и значимости → карточка находки и график
(signals/insights/cards.py) → content_candidates: source='insight', status='draft_ready' сразу —
Шаги А и Б не нужны, повод и данные поста и есть сама находка. Черновик пишет отдельный Routine
(content_ai.py, TRIGGER_ID_STEP_C_INSIGHT) строго по карточке, проверка — кодом при приёмке
(api/services/insight_check.py), дальше — обычный бот ревью.

Замеры и происхождение — research/content_pipeline_v2/insights/ (HANDOFF.md): на истории 69%
постов FRAME от данных — в топ-10 находок дня; в слепом сравнении итоговый рецепт (карточка +
второй мозг + голос автора + запрет прогноза) интереснее автора в 8 парах из 15.

    python3 -m signals.insight_scan --dry-run     # что создал бы — без записи в базу
    python3 -m signals.insight_scan               # создать кандидатов
"""
import os

# Хост ходит в Postgres через localhost: имя docker-сети «db» с хоста не резолвится
# (тот же приём, что в content_ai.py) — до импорта api.database.
_url = os.environ.get("DB_URL", "")
if "@db:" in _url:
    os.environ["DB_URL"] = _url.replace("@db:", "@127.0.0.1:")

import argparse  # noqa: E402
import re  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

import pandas as pd  # noqa: E402
from sqlalchemy import text  # noqa: E402

from api.database import SessionLocal  # noqa: E402
from signals.insights import cards, data, fresh  # noqa: E402
from signals.insights import detect as det  # noqa: E402

# сколько находок каждого типа в день; фондов — две: у канала за 14–25.09 треть постов про потоки в фонды (золото, юань
# дважды, денежный рынок, облигации), а один слот отдавал рекорд оттока из золота рекорду юаня (реплей 27.09)
PER = {"positions": 3, "funds": 2, "seasonality": 1}
FAMILY = {"positions": "позиции", "funds": "фонды", "seasonality": "сезонность"}
HASHTAG = {"positions": r"#открыт\w+", "funds": r"#деньгивфондах", "seasonality": r"#сезонность"}
# Хэштеги рубрик канала. Пост другой рубрики — не повтор находки: «Покупки/продажи фондов» (#сделкифондов, ребаланс
# индекса с Газпромом) 23–24.09 отсекал рекорд шорта физлиц по Газпрому и позиции по индексу, «Толпа охладела к юаню»
# (#деньгивфондах) — рекордный шорт по фьючерсу на юань (реплей завода 27.09).
RUBRICS = r"#открыт\w+|#деньгивфондах|#сезонность|#\w*делкифондов|#потоккапитала|#силарынка|#индикаторбаффетта"
MEDIA_DIR = os.environ.get("CONTENT_MEDIA_DIR", "/opt/frame/data/content_media")
REPEAT_DAYS, SKIP_DAYS = 14, 3
FUNDS_LAG_DAYS = 4        # потоки фондов приходят с опозданием: день-два плюс выходные
# «Про позиции» — только слова срочного рынка: в channel_posts текст обрезан (~500 знаков), хэштег рубрики в конце
# поста туда не доходит, и по «покупки/продажи», «толпа» посты о ребалансе индекса («Покупки/продажи фондов») и о
# юаневых фондах («Толпа охладела к юаню») 23–24.09 отсекали рекорды шорта по Газпрому, индексу и юаню (реплей 27.09).
TOPIC = {"positions": r"шорт|лонг|позици|фьючерс|контракт",
         "funds": r"фонд|приток|отток|БПИФ", "seasonality": r"сезонн"}
FUND_INST = {"bonds": r"облигац|ОФЗ", "stocks": r"фонд\w* акци", "money_market": r"денежн\w* рынк|ликвидност",
             "gold": r"золот", "yuan": r"юан", "all": r"фонд"}

_EXISTS = text("""
    SELECT 1 FROM content_candidates
    WHERE source = 'insight' AND headline = :headline AND created_at > now() - interval '14 days'
    LIMIT 1
""")
_INSERT = text("""
    INSERT INTO content_candidates
        (status, source, headline, raw_text, tickers, futures_ticker, event_type,
         importance_1_5, reasoning, media_filename, thread_key)
    VALUES ('draft_ready', 'insight', :headline, :raw_text, CAST(:tickers AS text[]), :futures_ticker,
            :event_type, 3, :reasoning, :media_filename, :thread_key)
    RETURNING id
""")


def detect_window(until: pd.Timestamp) -> list:
    """Находки за 30 дней до `until`: новизне в рейтинге нужны предыдущие дни."""
    out = det.Out(until - pd.Timedelta(days=30), until)
    P = det.load_positions()
    names, groups, to_stock = det.instruments()
    idx, stk, perp = det.prices()
    usd = det.usd_series(idx, perp)
    det.detect_positions(out, P, names, groups, to_stock, idx, stk, perp)
    det.detect_funds(out)
    out.ctx = {}
    det.detect_breadth(out)
    det.detect_buffett(out)
    det.detect_seasonality(out, idx, usd)
    det.detect_seasonal_curve(out, idx, usd)
    det.detect_fund_trades(out)
    det.detect_prices(out, idx, stk, perp, usd)
    return det.rank(out.items)


def channel_posts(as_of, days=REPEAT_DAYS) -> list:
    t = pd.Timestamp(as_of)
    df = data.read("channel_posts")
    if not len(df):
        return []
    df["d"] = pd.to_datetime(df.posted_at, utc=True).dt.tz_localize(None)
    df = df[(df.d >= t - pd.Timedelta(days=days)) & (df.d <= t + pd.Timedelta(days=1))]
    return [(r.d, r.text or "") for r in df.sort_values("d", ascending=False).itertuples()]


def repeat_of(spec, as_of):
    """Писал ли канал о той же теме недавно: тот же инструмент + тот же тип находки."""
    kind = spec["kind"]
    if kind == "funds":
        inst, case = FUND_INST.get(spec["cat"], r"фонд"), False
    elif kind == "seasonality":
        inst, case = (r"доллар|валют" if spec["code"] == "Si" else r"индекс\w* мосбирж|IMOEX|акци"), False
    else:
        code = det.CODE.get(spec["sec"])
        if code in ("MIX", "RI"):
            inst, case = r"индекс\w* мосбирж|IMOEX|индекс\w* РТС", False
        elif code in ("Si", "CNY", "Eu"):
            inst, case = r"доллар|валют|юан|евро", False
        else:   # акция: имя с заглавной или тикер — «Самолет» не должен ловить «самолетов Boeing»
            P, names, groups, to_stock, *_ = cards.data()
            nm = str(names.get(spec["sec"], "")).replace(" (вечн)", "").split(" ")[0]
            inst = "|".join(x for x in (re.escape(nm) if nm else "", to_stock.get(spec["sec"]) or "") if x)
            case = True
    for d, txt in channel_posts(as_of):
        if not (inst and re.search(inst, txt, 0 if case else re.I)):
            continue
        своя = re.search(HASHTAG[kind], txt, re.I)
        чужая = not своя and re.search(RUBRICS, txt, re.I)
        if своя or (not чужая and re.search(TOPIC[kind], txt, re.I)):
            return {"date": d, "title": txt.strip().splitlines()[0][:90] if txt.strip() else ""}
    return None


def all_time_record(x) -> bool:
    """Рекорд за всё время наших данных — пост и при скромной сумме: «Рекордные продажи золота» (700 млн ₽, первый
    месяц оттока за год) — эталонный пост канала 14.09, а порог «мало для читателя» его отсекал (реплей 27.09)."""
    return x.get("type") == "рекорд_или_экстремум" and "за всё время" in (x.get("title") or "")


def fund_significant(cat, as_of) -> bool:
    """Поток с начала месяца или за 20 дней ≥ 3 млрд ₽ или ≥ 3% активов категории:
    «отток 679 млн из фондов золота» (1% активов) — новость для движка, не для читателя."""
    daily, nav = cards.funds_data()
    d = cards.upto(daily[cat], as_of)
    t = d.index[-1]
    mtd = abs(float(d[d.index.to_period("M") == t.to_period("M")].sum()))
    s20 = abs(float(d.iloc[-20:].sum()))
    aum = float(cards.upto(nav[cat], t).iloc[-1])
    return max(mtd, s20) >= (min(3e9, 0.03 * aum) if aum > 0 else 3e9)


def drop_low_activity(items: list, log=print) -> list:
    """Малоактивные контракты на сайте скрыты из открытых позиций — мало физлиц-трейдеров (правило
    low_activity_set скринера). #2126: пост про шорт в Baidu, которого читатель на сервисе не видит."""
    try:
        from api.services.oi_screener import low_activity_set
        db = SessionLocal()
        try:
            low = low_activity_set(db)
        finally:
            db.close()
    except Exception as e:  # noqa: BLE001 — без фильтра лучше, чем без находок
        log(f"нет фильтра малоактивных: {type(e).__name__}: {e}")
        return items
    return [x for x in items if not (x.get("family") == "позиции" and (x.get("facts") or {}).get("sec") in low)]


def drop_expiry_days(items: list, log=print) -> list:
    """Находки по позициям на день экспирации и ±1 торговый день — искажение перехода в следующий
    контракт, а не сигнал (#2419 Мечел 17.09). Цена, фонды и сезонность не трогаются."""
    from signals.insights.expiry import near_expiry
    keep = [x for x in items if not (x.get("family") == "позиции" and near_expiry(x["date"]))]
    if len(keep) < len(items):
        log(f"экспирация: отложено находок по позициям - {len(items) - len(keep)}")
    return keep


def pick(items: list, log=print, until=None) -> list:
    """Лучшие находки дня по типам, по разным инструментам, с фильтрами."""
    res = []
    for kind, fam in FAMILY.items():
        pool = [x for x in items if x["family"] == fam]
        if kind == "positions":
            pool = [x for x in pool if x["type"] in ("рекорд_или_экстремум", "уровень_к_истории")
                    and (x.get("facts") or {}).get("leg") in ("long", "short", "nl", "ns", "net", "long_low", "nl_low")
                    and (x.get("facts") or {}).get("sec")]
        if kind == "funds":
            pool = [x for x in pool if (x.get("facts") or {}).get("cat")]
        if kind == "seasonality":
            pool = [x for x in pool if x["instrument"] in ("Si", "MIX")]
        if not pool:
            continue
        last = max(x["date"] for x in pool)
        # После экспирации дни позиций отложены, и «последний день» пула откатывался назад: находка
        # недельной давности уходила как свежая (#2491 у связок). Позиции и сезонность — только за
        # последний день данных, фонды отстают на день-два.
        if until is not None:
            lag = (pd.Timestamp(until).normalize() - pd.Timestamp(last)).days
            if lag > (FUNDS_LAG_DAYS if kind == "funds" else 0):
                log(f"пропуск {kind}: последняя находка от {last}, данные по {pd.Timestamp(until):%Y-%m-%d}")
                continue
        seen, cands = set(), []
        # позиции: кандидатов больше тройки — из них посты новых типов (angle_jobs), тренду остаются три бумаги
        cap = max(PER[kind], ANGLE_POOL) if kind == "positions" else PER[kind]
        for x in sorted((x for x in pool if x["date"] == last), key=lambda z: -z["score"]):
            if x["instrument"] in seen or len(seen) >= cap:
                continue
            f = x.get("facts") or {}
            spec = {"positions": {"kind": "positions", "sec": f.get("sec"), "leg": f.get("leg")},
                    "funds": {"kind": "funds", "cat": f.get("cat")},
                    "seasonality": {"kind": "seasonality", "code": x["instrument"]}}[kind]
            if kind == "funds" and not all_time_record(x) and not fund_significant(spec["cat"], last):
                log(f"пропуск, мало для читателя: {x['title'][:90]}")
                continue
            rep = repeat_of(spec, last)
            if rep and (pd.Timestamp(last) - rep["date"]).days <= SKIP_DAYS:
                log(f"пропуск, канал писал {rep['date']:%d.%m} «{rep['title'][:50]}»: {x['title'][:70]}")
                continue
            seen.add(x["instrument"])
            job = {"kind": kind, "spec": spec, "date": last, "title": x["title"], "repeat": rep,
                   "instrument": x["instrument"], "score": x["score"]}
            (cands if kind == "positions" else res).append(job)
        if kind == "positions":
            ang = angle_jobs(cands, log)
            used = {j["instrument"] for j in ang}
            res += [j for j in cands if j["instrument"] not in used][:PER["positions"]] + ang
    return res


# Посты новых типов по позициям (Вадим 28.09: «трендовые посты нужны, новыми типами разбавить»): концентрация, повод
# дня, доля — cards.position_angles. Сверху трёх трендовых, не больше двух в день; одна бумага — один пост в день.
ANGLE_PER_DAY = 2
ANGLE_POOL = 12      # сколько лучших находок по позициям проверять на поворот


def angle_jobs(cands: list, log=print) -> list:
    found = []
    for j in cands[:ANGLE_POOL]:
        if str(j["spec"].get("leg", "")).endswith("_low"):
            continue
        try:
            card = cards.build_card(j["spec"], j["date"])
        except Exception as e:  # noqa: BLE001 — без поворота находка остаётся трендовой
            log(f"поворот не посчитан: {j['title'][:70]}: {type(e).__name__}: {e}")
            continue
        for a in card.get("angles") or []:
            if a["strength"] >= cards.ANGLE_MIN[a["type"]]:
                found.append((a["strength"] / cards.ANGLE_MIN[a["type"]], j, a))
    out, used = [], set()
    for _, j, a in sorted(found, key=lambda z: (cards.ANGLE_PRIORITY[z[2]["type"]], -z[0])):
        if len(out) >= ANGLE_PER_DAY or j["instrument"] in used:
            continue
        used.add(j["instrument"])
        out.append({**j, "spec": {**j["spec"], "angle": a["type"]}, "title": f"{a['type']}: {j['title']}"})
        log(f"пост нового типа «{a['type']}»: {j['title'][:80]}")
    return out


def _inst_rx(spec):
    """Регэксп инструмента находки и чувствительность к регистру — общий для повторов и
    «что канал уже писал»."""
    kind = spec["kind"]
    if kind == "funds":
        return FUND_INST.get(spec["cat"], r"фонд"), False
    if kind == "seasonality":
        return (r"доллар|валют" if spec["code"] == "Si" else r"индекс\w* мосбирж|IMOEX|акци"), False
    code = det.CODE.get(spec["sec"])
    if code in ("MIX", "RI"):
        return r"индекс\w* мосбирж|IMOEX|индекс\w* РТС", False
    if code in ("Si", "CNY", "Eu"):
        return r"доллар|валют|юан|евро", False
    # акция: имя с заглавной или тикер — «Самолет» не должен ловить «самолетов Boeing»
    P, names, groups, to_stock, *_ = cards.data()
    nm = str(names.get(spec["sec"], "")).replace(" (вечн)", "").split(" ")[0]
    return "|".join(x for x in (re.escape(nm) if nm else "", to_stock.get(spec["sec"]) or "") if x), True


def own_posts(spec, as_of, k=2) -> list:
    """«Что канал уже писал по этому ряду» — посты той же рубрики И о том же инструменте. Одной
    рубрики мало: в карточке «АФК Системы» стояли посты про шорт по индексу и по валюте."""
    if spec["kind"] not in HASHTAG:     # сделки фондов и макро: свой отбор повторов (fund_trades_job)
        return []
    inst, case = _inst_rx(spec)
    rows = [(d, t) for d, t in channel_posts(as_of, days=45)
            if re.search(HASHTAG[spec["kind"]], t) and inst and re.search(inst, t, 0 if case else re.I)]
    return [f"{cards.d_ru(d, as_of)}: «{t.strip().splitlines()[0][:90]}»" for d, t in rows[:k]]


def related_lines(spec, as_of) -> list:
    """Связанные компании из второго мозга — тот же блок, что в брифе новостей (content_ai._related_context): граф
    владения, сила связи по совместным новостям за 30 дней, что о компании писали. Вадим 27.09 про #1638: «часть про
    Сегежу и Озон мы получили только за счёт второго мозга» — у находок этого блока не было. Прямые связи — фактурой,
    слабые (одна отрасль: Русал владеет 27,8% Норникеля) — вопросом с ответом «нет» по умолчанию, связь из одного
    графа владения до писателя не доходит — как в новостях."""
    if spec["kind"] != "positions":
        return []
    stock = cards.data()[3].get(spec.get("sec"))
    if not stock:
        return []
    from signals.content_ai import _related_context   # тяжёлый импорт — только когда есть бумага
    db = SessionLocal()
    try:
        strong, _weak = _related_context(db, {"tickers": [stock], "headline": None, "raw_text": None},
                                         pd.Timestamp(as_of).date())
    except Exception as e:  # noqa: BLE001 — без блока карточка работает как раньше
        print(f"[insight_scan] связанные компании {stock}: {type(e).__name__}: {e}")
        return []
    finally:
        db.close()
    lines = []
    for tk, item in strong.items():
        if not isinstance(item, dict):
            continue
        parts = [item.get("связь") or tk]
        if item.get("из_архива"):
            parts.append("что писали: " + "; ".join(item["из_архива"]))
        if item.get("цена_за_месяц"):
            parts.append(f"цена за месяц: {item['цена_за_месяц']}")
        lines.append(" · ".join(parts))
    weak = [item for item in _weak.values() if isinstance(item, dict)]
    for item in weak:
        lines.append(f"под вопросом: {item.get('связь')} ({'; '.join(item.get('почему_она_здесь') or [])})")
    if lines:
        lines.append("бери, только если связь помогает понять находку, иначе не упоминай; «под вопросом» - по умолчанию "
                     "НЕ упоминай; долю владения - с датой снимка; причину не утверждай, показывай последовательность")
    return lines


# Досье мозга в карточку: кто компания и где она в мире. Внутренние поля завода (прошлые кандидаты, аномалии) и
# события, которые уже есть в brain_lines (раскрытия, отчёты, фонды, индексы), сюда не идут.
_DOSSIER = (("компания", "компания"), ("сектор", "отрасль"), ("владельцы_по_снимку", "владельцы"),
            ("владеет", "владеет"), ("фонды_держатели", "фонды"), ("в_индексах", "в индексах"),
            ("о_компании_писали", "о компании писали"), ("часто_рядом_в_новостях", "рядом в новостях"))


def company_lines(spec, as_of) -> dict:
    """Кто эта компания и что вокруг неё — тот же второй мозг и архив, что в брифе новостей (content_ai._brain_block,
    _news_around). Вадим 27.09: «здесь больше речь про компании и контекст: что это за компания — строительный сектор,
    IT — и как это может косвенно повлиять; мы сделали второй мозг, чтобы был контекст понимания того, что произошло».
    До 27.09 у находок этого не было: карточка знала ряд позиций, но не компанию."""
    if spec["kind"] != "positions":
        return {}
    P, names, groups, to_stock, *_ = cards.data()
    stock = to_stock.get(spec.get("sec"))
    if not stock:
        return {}
    from signals.content_ai import _brain_block, _news_around   # тяжёлый импорт — только когда есть бумага
    row = {"id": None, "tickers": [stock], "asset_id": spec.get("sec"), "created_at": None,
           "asset_name": str(names.get(spec.get("sec"), "")).replace(" (вечн)", ""), "headline": None, "raw_text": None}
    db = SessionLocal()
    try:
        блок = (_brain_block(db, row) or {}).get(stock) or {}
        around = _news_around(db, row, pd.Timestamp(as_of).date())
    except Exception as e:  # noqa: BLE001 — без контекста карточка работает как раньше
        print(f"[insight_scan] контекст компании {stock}: {type(e).__name__}: {e}")
        return {}
    finally:
        db.close()
    dossier = []
    for key, label in _DOSSIER:
        val = next((v for k, v in блок.items() if k.startswith(key)), None)
        if val:
            dossier.append(f"{label}: " + ("; ".join(val) if isinstance(val, list) else str(val)))
    if dossier:
        dossier.append("[A]/[B] — утверждать с датой; [C] — «по нашей разметке»; [D] — не утверждать. "
                       "Бери один факт, если он помогает понять находку")
    out = {"о компании (второй мозг)": dossier}
    if around:
        out["что было у компании и в отрасли в эти дни (цена за 2 часа после)"] = around
    return out


def build(job: dict, now_iso: str) -> dict:
    spec, date = job["spec"], job["date"]
    card = cards.build_card(spec, date)
    ctx = cards.context_for(card, spec, now_iso)
    ctx["что канал уже писал по этому ряду"] = own_posts(spec, card["as_of"])
    ctx.update(company_lines(spec, card["as_of"]))
    ctx["связанные компании (второй мозг)"] = related_lines(spec, card["as_of"])
    if job["repeat"]:
        r = job["repeat"]
        ctx["повтор темы - подай как продолжение"] = [
            f"{cards.d_ru(r['date'], card['as_of'])} канал уже писал об этом: «{r['title']}». Начни со ссылки на "
            f"тот пост и скажи, что изменилось с тех пор; не пересказывай его заново"]
    brief = cards.brief_text(card, focus=True, context=ctx)
    return {"card": card, "brief": brief}


# Сделки фондов за месяц (Вадим 28.09: «вводи в эксплуатацию», шаг 2): раз в месяц, когда вышел новый срез составов
# фондов (август виден в сентябре), — один черновик. Месяц уже был у завода или канал уже писал о сделках фондов после
# конца месяца (август: посты 22.09 и 25.09) — пропуск; срез старше FT_MAX_AGE_DAYS — не новость.
FT_MAX_AGE_DAYS = 50
FT_CHANNEL_RX = r"делкифондов|сделк\w* фондов|фонды (продали|купили|продадут|докупят)"
_FT_EXISTS = text("SELECT 1 FROM content_candidates WHERE thread_key = :k LIMIT 1")


def fund_trades_job(now, db, log=print) -> dict | None:
    try:
        mv = cards._ft_get("movers", period="1m", sort="amount", limit=1, scope="portfolio")
    except Exception as e:  # noqa: BLE001 — страница недоступна: сделок фондов сегодня нет, остальное идёт
        log(f"сделки фондов: API недоступно: {type(e).__name__}: {e}")
        return None
    if not mv.get("resolved_month") or not mv.get("top_accumulated") or not mv.get("top_reduced"):
        return None
    month = pd.Timestamp(mv["resolved_month"])
    end = month + pd.offsets.MonthEnd(0)
    now = pd.Timestamp(now).tz_localize(None) if pd.Timestamp(now).tzinfo else pd.Timestamp(now)
    if (now - end).days > FT_MAX_AGE_DAYS:
        log(f"сделки фондов: последний срез за {month:%m.%Y} — старше {FT_MAX_AGE_DAYS} дней, не новость")
        return None
    thread = f"insight:fund_trades:{month:%Y-%m}"
    if db.execute(_FT_EXISTS, {"k": thread}).first():
        return None
    for d, txt in channel_posts(now, days=45):
        if d > end and re.search(FT_CHANNEL_RX, txt, re.I):
            log(f"сделки фондов за {month:%m.%Y}: канал уже писал {d:%d.%m} «{txt.strip().splitlines()[0][:50]}»")
            return None
    return {"kind": "fund_trades", "spec": {"kind": "fund_trades", "month": str(month.date())}, "date": now,
            "title": f"Сделки фондов за {month:%m.%Y}", "repeat": None, "instrument": "FUNDS", "score": 0,
            "thread_key": thread}

def run_once(dry_run: bool = False) -> dict:
    oi = data.read("oi_daily", parse_dates=["tradedate"])
    until = oi.tradedate.max()
    items = drop_expiry_days(drop_low_activity(detect_window(until)))
    jobs = pick(items, until=until)
    now_iso = datetime.now(timezone.utc).isoformat()
    _db = SessionLocal()
    try:
        ft = fund_trades_job(datetime.now(timezone.utc), _db)
    finally:
        _db.close()
    if ft:
        jobs.append(ft)
    summary = {"data_until": str(until.date()), "found": len(items), "picked": len(jobs), "created": 0,
               "skipped_exists": 0}
    db = SessionLocal()
    try:
        for n, job in enumerate(jobs, 1):
            b = build(job, now_iso)
            card = b["card"]
            chg = card.get("intraday_change")
            if chg is not None and chg <= cards.INTRADAY_SKIP:
                print(f"[insight_scan] пропуск, к утру позиция уже {chg:+.0%} к закрытию: {card['headline'][:80]}")
                summary["skipped_intraday"] = summary.get("skipped_intraday", 0) + 1
                continue
            head = card["headline"][:300]
            if db.execute(_EXISTS, {"headline": head}).first():
                summary["skipped_exists"] += 1
                continue
            spec = job["spec"]
            sec = spec.get("sec")
            tick = (cards.data()[3].get(sec) if sec else None) or job["instrument"] or spec.get("code") or "MIX"
            media = f"insight_{card['as_of']:%Y%m%d}_{spec['kind']}_{n}.png"
            print(f"[insight_scan] {spec['kind']}: {head}" + (" (продолжение темы)" if job["repeat"] else ""))
            if dry_run:
                print(b["brief"][:1500] + "\n")
                continue
            os.makedirs(MEDIA_DIR, exist_ok=True)
            cards.draw_chart(card, os.path.join(MEDIA_DIR, media))
            row = db.execute(_INSERT, {
                "headline": head, "raw_text": b["brief"], "tickers": [str(tick)],
                "futures_ticker": sec, "event_type": f"insight_{spec['kind']}",
                "reasoning": f"движок находок: {job['title'][:200]} (балл {job['score']})",
                "media_filename": media,
                "thread_key": job.get("thread_key") or f"insight:{spec['kind']}:{job['instrument'] or tick}",
            }).first()
            db.commit()
            summary["created"] += 1
            print(f"[insight_scan] кандидат {row[0]} создан")
    finally:
        db.close()
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="показать находки и карточки, ничего не писать")
    ap.add_argument("--force", action="store_true", help="прогнать сейчас: не ждать свежий день и не смотреть отметку")
    a = ap.parse_args()
    why = None if (a.dry_run or a.force) else fresh.wait_reason("insight_scan")
    if why:
        print(f"[insight_scan] пропуск: {why}")
    else:
        summary = run_once(dry_run=a.dry_run)
        if not a.dry_run:
            fresh.mark_done("insight_scan", summary["data_until"])
        print(f"[insight_scan] {summary}")
