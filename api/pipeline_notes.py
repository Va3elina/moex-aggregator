"""
Перевод заметки прогона (pipeline_runs.last_note) на человеческий.

⚠️ ГЛАВНОЕ ЗДЕСЬ — НЕ КРАСОТА, А ТРИ РАЗНЫХ НУЛЯ. Прогон с нулём записей — это
либо «выходной, не работали», либо «работали, ничего нового не нашли», либо
«заблокированы источником и вышли». Все три выглядят одинаково зелёными в
статусе, а различаются они ТОЛЬКО по содержимому заметки. Поэтому каждая
формулировка ниже кроме фразы отдаёт флаг тревоги: True — «зелёный, но по сути
ничего не сделано», None — судить не по чему.

⚠️ «OK» — ЭТО ОТСУТСТВИЕ ИНФОРМАЦИИ. Оркестратор писал его на любой код выхода
ноль; для 21 пайплайна из 27 заметка не несла ничего. Теперь оркестратор
подхватывает JSON-итог из stdout, но только у тех скриптов, которые его печатают.
Остальным «OK» переводится честно: «завершился без ошибок, что именно сделал —
не сообщил» — это правда, и она полезнее любой выдуманной фразы.

Форматы взяты из кода скриптов (аудит 04.09.2026), не из головы. Новый формат
добавлять сюда же, рядом с именем пайплайна.
"""

import ast
import json
import re


def _словарь(note: str) -> dict | None:
    """Заметка может быть JSON'ом (новый путь) или str(dict) (шесть старых).

    Для второго случая — ast.literal_eval: он разбирает ТОЛЬКО литералы (числа,
    строки, словари, списки) и не исполняет код. Это штатный способ прочитать
    то, что Python напечатал через str(dict)."""
    if not note:
        return None
    т = note.strip()
    if not т.startswith("{"):
        return None
    try:
        return json.loads(т)
    except ValueError:
        pass
    try:
        d = ast.literal_eval(т)
        return d if isinstance(d, dict) else None
    except (ValueError, SyntaxError):
        return None


def _n(d: dict, *ключи, default=0):
    for k in ключи:
        if k in d and d[k] is not None:
            return d[k]
    return default


def _склон(n: int, один: str, два: str, пять: str) -> str:
    n = abs(int(n))
    if 11 <= n % 100 <= 14:
        return пять
    r = n % 10
    if r == 1:
        return один
    if 2 <= r <= 4:
        return два
    return пять


# ── переводчики по пайплайнам ────────────────────────────────────────────────

def _content_match(d: dict):
    chk, m, exp, pend = _n(d, "checked"), _n(d, "matched"), _n(d, "expired"), _n(d, "still_pending")
    fired = _n(d, "step_c_fired")
    err = _n(d, "errors") + _n(d, "step_c_fire_errors")
    if chk == 0:
        return "Кандидатов на сверку не было", None
    части = [f"проверено {chk}"]
    части.append(f"совпало {m}" if m else "совпадений нет")
    if exp:
        части.append(f"протухло {exp}")
    if pend:
        части.append(f"ждут {pend}")
    if fired:
        части.append(f"писатель запущен {fired} раз")
    if err:
        части.append(f"ошибок {err}")
    # Тревога только при ошибках: ноль совпадений в одном прогоне — норма.
    return ", ".join(части).capitalize(), bool(err)


def _tg_hype(d: dict):
    f, nw, pr = _n(d, "fetched"), _n(d, "new_watched"), _n(d, "promoted")
    err = _n(d, "errors") + _n(d, "step_a_fire_errors") + _n(d, "hype_filter_fire_errors")
    if f == 0:
        return "Ни одного поста не прочитано — каналы молчат или связь оборвана", True
    части = [f"прочитано {f}", f"взято на карандаш {nw}"]
    части.append(f"хайпом признано {pr}" if pr else "до хайпа никто не дошёл")
    if err:
        части.append(f"ошибок {err}")
    return ", ".join(части).capitalize(), bool(err)


def _backstop(d: dict):
    if _n(d, "skipped_no_token"):
        return "Страховка не работала: нет токена", True
    gave = sum(v for k, v in d.items() if k.endswith("_gave_up") and isinstance(v, int))
    fired = sum(v for k, v in d.items() if k.endswith("_fired") and isinstance(v, int))
    if gave:
        return f"Сдался на {gave} — три попытки не помогли", True
    if fired:
        return f"Добил зависших: {fired}", False
    # Все нули — здоровье: основной путь справился сам.
    return "Зависших задач нет — всё прошло с первого раза", False


def _news_web(note: str):
    m = re.search(r"каналов (\d+), постов (\d+)", note)
    if not m:
        return note, None
    к, п = int(m.group(1)), int(m.group(2))
    доп = []
    r = re.search(r"повторов (\d+)", note)
    if r:
        доп.append(f"с {r.group(1)} повторами")
    e = re.search(r"ПУСТЫХ (\d+)", note)
    if e:
        доп.append(f"{e.group(1)} пустых")
    фраза = (f"Обошли {к} {_склон(к, 'канал', 'канала', 'каналов')}, "
             f"забрали {п} {_склон(п, 'пост', 'поста', 'постов')}")
    if доп:
        фраза += " (" + ", ".join(доп) + ")"
    return фраза, (к == 0 or п == 0 or bool(e))


def _distributions(note: str):
    m = re.search(r"(\d+) funds, (\d+) rows, (\d+) err", note)
    if not m:
        return note, None
    f, r, e = map(int, m.groups())
    if f == 0 and r == 0:
        return "Ни один фонд не отдал выплат — все страницы пустые", True
    фраза = (f"{f} {_склон(f, 'фонд заплатил', 'фонда заплатили', 'фондов заплатили')}, "
             f"загружено {r} {_склон(r, 'выплата', 'выплаты', 'выплат')}")
    if e:
        фраза += f", ошибок {e}"
    return фраза, bool(e)


def _fm_cards(d: dict):
    if "остановлен" in d:
        if d["остановлен"] == "бюджет":
            return f"Обход не запущен: суточная квота исчерпана (списано {_n(d, 'списано'):.0f})", True
        return f"Остановлен: {d['остановлен']}", True
    if "обход" in d:
        return "Все компании свежие — качать нечего", False
    if "план" in d:
        return f"Проба без сети: взяли бы {len(d['план'])}", None
    к, м, н = _n(d, "компаний"), _n(d, "метрик"), _n(d, "новых")
    if к == 0:
        return "Компаний обработано 0", True
    фраза = f"{к} {_склон(к, 'компания', 'компании', 'компаний')}, {м} метрик"
    if н:
        фраза += f" ({н} новых)"
    уп = _n(d, "упало")
    if уп:
        фраза += f", упало {уп}"
    return фраза, bool(уп)


def _company_cards(d: dict):
    if d.get("остановлен_на"):
        return f"Источник попросил остановиться на {d['остановлен_на']} — обошли {_n(d, 'успех')}", True
    у, п, уп = _n(d, "успех"), _n(d, "пусто"), _n(d, "упало")
    if у == 0 and п == 0:
        return "Ни одной бумаги не обработано", True
    фраза = f"{у} {_склон(у, 'бумага', 'бумаги', 'бумаг')} обработано"
    if _n(d, "метрик"):
        фраза += f", метрик {d['метрик']}"
    if _n(d, "документов"):
        фраза += f", документов {d['документов']}"
    if п:
        фраза += f", пустых {п}"
    if уп:
        фраза += f", упало {уп}"
    return фраза, bool(уп)


def _ownership(d: dict):
    н, нов, повт = _n(d, "найдено"), _n(d, "новых"), _n(d, "повторов")
    if н == 0:
        return "Упоминаний о владении за окно не нашлось", False
    фраза = f"Просмотрено {н} упоминаний, новых сигналов {нов}"
    if повт:
        фраза += f", повторов {повт}"
    return фраза, False


def _contract_calendar(d: dict):
    n = _n(d, "активных")
    if n == 0:
        return "Биржа отдала ПУСТОЙ список — все контракты помечены снятыми с торгов", True
    return f"Активных контрактов {n}", False


def _funds(d: dict):
    if d.get("итог") == "авторизация не прошла":
        return "Cbonds не пустил: авторизация не прошла, СЧА не обновлены", True
    ф, з = _n(d, "фондов"), _n(d, "записей")
    if з == 0:
        return f"Опрошено {ф} фондов, новых записей СЧА нет", True
    return f"{ф} {_склон(ф, 'фонд', 'фонда', 'фондов')}, {з} {_склон(з, 'запись СЧА', 'записи СЧА', 'записей СЧА')}", False


def _candles_spot(d: dict):
    a, b, c = _n(d, "5м"), _n(d, "60м"), _n(d, "24ч")
    if a == b == c == 0:
        return "Свечи акций: ни одной новой строки", True
    return f"Свечи акций: +{a} пятиминуток, +{b} часовых, +{c} дневных", False


def _breadth(d: dict):
    б, в = _n(d, "бумаг"), _n(d, "вселенных")
    if б == 0:
        return "Ширина рынка: тикеров в базе не нашлось — ничего не посчитано", True
    фраза = f"Ширина рынка по {б} бумагам, {в} {_склон(в, 'вселенная', 'вселенные', 'вселенных')}"
    if not d.get("состав_imoex", True):
        фраза += " — состава IMOEX нет, история считана по сегодняшнему"
        return фраза, True
    return фраза, False


def _market_cap(d: dict):
    # update_market_cap отдаёт {"MARKET_CAP_TOTAL": n_строк, ...}. Ноль у якоря =
    # smart-lab сменил вёрстку (инцидент 17.06.2026) — и это выглядело как «OK».
    t = _n(d, "MARKET_CAP_TOTAL")
    if t == 0:
        return "Капитализация рынка НЕ обновлена — smart-lab не распознан, смотреть вёрстку", True
    return "Капитализация рынка за сегодня записана", False


def _index_composition(d: dict):
    n = _n(d, "сохранено_дней")
    if n == 0:
        return "База расчёта IMOEX: новых дней нет", False
    return f"База расчёта IMOEX: +{n} {_склон(n, 'день', 'дня', 'дней')}", False


def _oi_5min(d: dict):
    a, i = _n(d, "опрошено"), _n(d, "вставлено")
    if a == 0:
        return "Открытый интерес: ни одного инструмента не опрошено", True
    return f"Открытый интерес: опрошено {a}, +{i} записей" + ("" if i else " (уже актуально)"), False


def _oi_daily(d: dict):
    k, i = _n(d, "инструментов"), _n(d, "вставлено")
    if k == 0:
        return "Дневной ОИ: список инструментов пуст", True
    return f"Дневной ОИ: {k} инструментов, +{i} записей" + ("" if i else " (день уже был)"), False


def _candles_futures(d: dict):
    a, b, c = _n(d, "5м"), _n(d, "60м"), _n(d, "24ч")
    if a == b == c == 0:
        return "Свечи фьючерсов: ни одной новой строки", True
    return f"Свечи фьючерсов: +{a} пятиминуток, +{b} часовых, +{c} дневных", False


def _macro(d: dict):
    # ⚠️ Число строк здесь показывать НЕЛЬЗЯ: M2 и ВВП перезаписываются целиком
    # каждый прогон (404 + 125 строк при нулевом фактическом обновлении). Только факт.
    m2, gdp, rate = _n(d, "M2"), _n(d, "GDP"), _n(d, "KEY_RATE")
    части, тревога = [], False
    for имя, v in (("M2", m2), ("ВВП", gdp)):
        if v:
            части.append(f"{имя} перезаписан")
        else:
            части.append(f"{имя} НЕ получен"); тревога = True
    части.append("ставка обновлена" if rate else "ставка без изменений")
    return "Макро: " + ", ".join(части), тревога


def _zcyc(d: dict):
    if d.get("ошибка"):
        return "Кривая ОФЗ: не получена ни от ЦБ, ни от биржи", True
    новых, последняя, ист = _n(d, "новых_дней"), d.get("последняя"), d.get("источник")
    откуда = "через API биржи" if ист == "iss" else "с ЦБ"
    if новых == 0:
        return f"Кривая ОФЗ: новых дней нет, последняя точка {последняя or '—'}", False
    return f"Кривая ОФЗ: {новых} новых дн. {откуда}, до {последняя or '—'}", False


def _brain(d: dict):
    узлов, рёбер = _n(d, "узлов"), _n(d, "рёбер")
    новых = _n(d, "новостей") + _n(d, "кандидатов") + _n(d, "документов") + _n(d, "аномалий") + _n(d, "сигналов")
    if d.get("режим") == "полный":
        return f"Карта нодов пересобрана целиком: {узлов} узлов, {рёбер} рёбер", False
    if новых == 0:
        return f"Карта нодов: нового нет, {узлов} узлов, {рёбер} рёбер", False
    return f"Карта нодов: догнала {новых} новых, всего {узлов} узлов, {рёбер} рёбер", False


def _freefloat(d: dict):
    n = _n(d, "месяцев")
    if n == 0:
        return "Free-float: текущий месяц уже есть — пропуск", False
    return f"Free-float: обновлено {n} {_склон(n, 'месяц', 'месяца', 'месяцев')}", False


def _commodity(d: dict):
    r, п = _n(d, "строк"), _n(d, "пустых")
    не = d.get("не_отдали") or []
    if r == 0:
        return "Сырьё: ни один тикер не отдал данных", True
    фраза = f"Сырьё: {r} строк"
    if п:
        фраза += f", не отдали {п}: {', '.join(не[:4])}"
    return фраза, bool(п)


def _turnover(d: dict):
    с, к = _n(d, "свечей"), _n(d, "контрактов")
    if с == 0:
        return f"Оборот не проставлен ни одной свече (контрактов с оборотом {к}) — дневных свечей ещё нет?", True
    return f"Рублёвый оборот проставлен {с} свечам из {к} контрактов", False


def _dividends(d: dict):
    н, о, п = _n(d, "новых"), _n(d, "обновлено"), _n(d, "покрытие", default=None)
    фраза = f"Дивиденды: +{н} новых, {о} обновлено"
    if п is not None:
        фраза += f", покрытие {п * 100:.0f}%"
    # Ноль новых — норма: дивиденды меняются редко. Тревога только по покрытию.
    return фраза, (п is not None and п < 0.5)


def _aggregate(d: dict):
    if "вставлено" in d:
        n = _n(d, "вставлено")
        return (f"Свёрнуто часовых записей ОИ: {n}" if n else "Часовой ОИ: нет данных для свёртки (нерабочий час)"), False
    return "Часовая свёртка ОИ выполнена", False


def _indices(d: dict):
    if "сохранено" in d:
        n = _n(d, "сохранено")
        return (f"Индексы: +{n} записей" if n else "Индексы: новых записей нет"), False
    if "индексы" in d:
        return "Индексы обновлены", False
    return ", ".join(f"{k} {v}" for k, v in list(d.items())[:6]), None


def _index_hourly(d: dict):
    if "строк" in d:
        n = _n(d, "строк")
        return (f"Часовые свечи индексов: +{n}" if n else "Часовые свечи индексов: новых нет"), False
    return "Часовые свечи индексов обновлены", False


def _index_intraday(d: dict):
    n = d.get("обновлено")
    if n is None:
        return "Текущие значения индексов: прогон завершён", None
    if n == 0:
        return "Вне сессии — текущих значений нет", False
    return f"Текущие значения: обновлено {n} из 3 индексов", False


_ПО_ИМЕНИ = {
    "oi_5min": ("dict", _oi_5min),
    "oi_daily": ("dict", _oi_daily),
    "candles_futures": ("dict", _candles_futures),
    "macro_daily": ("dict", _macro),
    "zcyc_daily": ("dict", _zcyc),
    "brain_sync": ("dict", _brain),
    "freefloat_cap_daily": ("dict", _freefloat),
    "commodity_daily": ("dict", _commodity),
    "futures_turnover": ("dict", _turnover),
    "dividends_daily": ("dict", _dividends),
    "oi_aggregate": ("dict", _aggregate),
    "indices_daily": ("dict", _indices),
    "index_candles_hourly": ("dict", _index_hourly),
    "index_intraday": ("dict", _index_intraday),
    "contract_calendar": ("dict", _contract_calendar),
    "funds_daily": ("dict", _funds),
    "candles_spot": ("dict", _candles_spot),
    "breadth_daily": ("dict", _breadth),
    "market_cap_daily": ("dict", _market_cap),
    "index_composition_daily": ("dict", _index_composition),
    "content_match": ("dict", _content_match),
    "content_tg_hype_scan": ("dict", _tg_hype),
    "content_ai_backstop": ("dict", _backstop),
    "news_web_scan": ("str", _news_web),
    "distributions": ("str", _distributions),
    "fm_cards_daily": ("dict", _fm_cards),
    "company_cards": ("dict", _company_cards),
    "ownership_detect": ("dict", _ownership),
}


def человеческая(pipeline: str, status: str | None, note: str | None,
                 duration_sec: float | None = None) -> dict:
    """
    → {"фраза": str, "тревога": bool | None, "сырое": str}

    тревога=True — «зелёный, но по сути ничего не сделано»; None — по заметке не
    судить. Сырая заметка возвращается всегда: перевод не должен прятать оригинал.
    """
    сырое = note or ""
    if status and status not in ("ok", "degraded"):
        # Сбой — оркестратор уже положил хвост stderr; это и есть человеческий текст.
        return {"фраза": сырое[:200] or "Упал без сообщения", "тревога": None, "сырое": сырое}

    if not сырое or сырое.strip() == "OK":
        # ⚠️ ЗДЕСЬ БЫЛА ЭВРИСТИКА «меньше секунды — похоже на ранний выход». На
        # живых данных она подсветила contract_calendar (штатно 1 с) и
        # index_intraday (три строки, 0,7 с) — то есть врала чаще, чем помогала.
        # Быстрый-нормальный от быстрого-сломанного отличает только история
        # прогонов, а её пока нет (журнал прогонов — отдельный этап). Не выдумываем.
        return {"фраза": "Завершился без ошибок; что именно сделал — не сообщил",
                "тревога": None, "сырое": сырое}

    # ── общие формы, не зависящие от пайплайна ──────────────────────────────
    d = _словарь(сырое)
    if d is not None and "пропуск" in d:
        # Третий из «трёх нулей»: не работали, потому что выходной. Это норма,
        # и тревога здесь — ложь; но и «ok» без слов прятал бы, что данных нет.
        return {"фраза": f"Пропуск: {d['пропуск']}", "тревога": False, "сырое": сырое}

    вид, fn = _ПО_ИМЕНИ.get(pipeline, (None, None))
    try:
        if вид == "dict":
            d = _словарь(сырое)
            if d is None:
                return {"фраза": сырое[:200], "тревога": None, "сырое": сырое}
            фраза, тревога = fn(d)
        elif вид == "str":
            фраза, тревога = fn(сырое)
        else:
            # Незнакомый пайплайн, но заметка есть: если это JSON — покажем как
            # пары ключ-значение, это уже лучше словаря в фигурных скобках.
            d = _словарь(сырое)
            if d:
                фраза = ", ".join(f"{k} {v}" for k, v in list(d.items())[:6])
                тревога = None
            else:
                фраза, тревога = сырое[:200], None
    except Exception:
        фраза, тревога = сырое[:200], None
    return {"фраза": фраза, "тревога": тревога, "сырое": сырое}
