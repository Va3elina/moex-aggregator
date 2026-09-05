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
    # результат update_market_cap: словарь шагов → число; «0» у якоря = вёрстка сменилась
    нули = [k for k, v in d.items() if v in (0, "0")]
    if нули and len(нули) == len(d):
        return "Капитализация: ничего не обновлено — смотреть вёрстку smart-lab", True
    return ", ".join(f"{k} {v}" for k, v in list(d.items())[:5]), False


def _index_composition(d: dict):
    n = _n(d, "сохранено_дней")
    if n == 0:
        return "База расчёта IMOEX: новых дней нет", False
    return f"База расчёта IMOEX: +{n} {_склон(n, 'день', 'дня', 'дней')}", False


_ПО_ИМЕНИ = {
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
