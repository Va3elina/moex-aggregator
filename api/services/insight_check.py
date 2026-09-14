"""Проверка черновика «от находки» кодом — при приёмке Шага В (apply_step_c) и по запросу
писателя (POST /api/internal/content-news/insight-check).

Для жанра «находка» у каждого числа есть источник — карточка находки (content_candidates.raw_text).
Поэтому здесь не LLM-судья, а детерминированная сверка: каждое число поста должно стоять в
карточке, прогноза цены быть не должно, финал — не заготовка, форма — как у канала.

Происхождение правил — research/content_pipeline_v2/insights (HANDOFF.md):
• прогноз цены запрещён: forecast_backtest — с 2023 года сигналы угадывают направление в 51%
  случаев, прогнозы автора не лучше правила «всегда вниз»;
• потолок 6 чисел: у канала 0,48 числа на 100 знаков, у первой версии завода было 2,66;
• «Посмотрим-увидим», «покажет следующий месяц» проскочили в свежих постах 13.09.
"""
import re

NUM = re.compile(r"\d+(?:[  ]\d{3})*(?:[.,]\d+)?")
FORECAST = re.compile(r"\b(?:вырастет|вырастут|упадёт|упадет|упадут|подорожает|подешевеет|будет расти|"
                      r"будет падать|развернётся|развернется|дно близко|дно уже)\b", re.I)
BOILERPLATE = re.compile(r"стоит последить|покажут ближайшие|время покажет|не является инвест|посмотрим|"
                         r"покажет (?:уже )?(?:следующ|ближайш)|поживём|поживем|не просто так", re.I)
# Правка Вадима к #2104 (14.09): «кто писали?», «медиана слишком сложная», шорт не объяснять
STYLE = ((re.compile(r"(?:^|[.!?◽️]\s*)(?:писали|сообщали),? что", re.I | re.M), "«писали, что» - подай новость событием"),
         (re.compile(r"медиан", re.I), "«медиана» - скажи проще: «в 2 случаях из 3»"),
         (re.compile(r"без вычета (?:лонгов|шортов)|не чистая позиция", re.I), "объяснение индикатора - читатель его знает"))
CODES = re.compile(r"\b(?:MX|IMOEXF|USDRUBF|CNYRUBF|CR|Si)\b")
TITLE_EMOJI = re.compile(r"[\U0001F300-\U0001FAFF☀-➿]️?\s*$")


def numbers(text: str) -> list:
    out = []
    for m in NUM.finditer(text or ""):
        try:
            out.append((m.group(0), round(float(m.group(0).replace(" ", "").replace(" ", "")
                                                 .replace(",", ".")), 6)))
        except ValueError:
            pass
    return out


def check(draft: str, card: str) -> dict:
    """→ {"verdict": годится|спорно|брак, "failed": [...], "defects": [...], "note": "..."}.
    Брак — число не из карточки или прогноз цены: это факты. Форма — дефекты, не ворота."""
    lines = [x for x in (draft or "").strip().splitlines() if x.strip()]
    title = lines[0] if lines else ""
    tags = [x for x in lines if x.strip().startswith("#")]
    body = "\n".join(x for x in lines[1:] if not x.strip().startswith("#"))
    have = {v for _, v in numbers(card)}
    foreign = [raw for raw, v in numbers(body) if v not in have]
    failed, defects = [], []
    if foreign:
        failed.append("числа не из карточки: " + ", ".join(dict.fromkeys(foreign)))
    fw = FORECAST.findall(body)
    if fw:
        failed.append(f"прогноз цены: «{fw[0]}» - перепиши как историю или условие «если…»")
    if BOILERPLATE.search(draft or ""):
        defects.append("заготовка вместо вывода")
    defects += [msg for rx, msg in STYLE if rx.search(body)]
    n = len(re.sub(r"\s+", " ", body))
    if not 450 <= n <= 1000:
        defects.append(f"длина {n} знаков, цель 600-800")
    nn = len(numbers(body))
    if nn > 6:
        defects.append(f"чисел {nn} - потолок 5, считая даты и годы")
    if len(tags) != 1 or not lines or not lines[-1].strip().startswith("#"):
        defects.append("нужен ровно один хэштег последней строкой")
    if re.search(r"\b(сегодня|вчера|завтра)\b", draft or "", re.I):
        defects.append("«сегодня/вчера» - нужна дата")
    if re.search(r"\b(Frame|Фрейм)\b", draft or ""):
        defects.append("канал назван по имени")
    if "(" in body:
        defects.append("вставка в скобках")
    if CODES.search(body):
        defects.append("код инструмента вместо слов")
    if not TITLE_EMOJI.search(title):
        defects.append("заголовок без эмодзи в конце")
    verdict = "брак" if failed else ("спорно" if len(defects) >= 2 else "годится")
    note = "проверка кодом: " + ("; ".join(failed + defects) if failed or defects else "числа из карточки, форма ок")
    return {"verdict": verdict, "failed": failed, "defects": defects, "note": note, "chars": n, "numbers": nn}
