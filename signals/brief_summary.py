"""Короткая выжимка карточки находки или связки — для человека в боте ревью.

Вадим 17.09 про «🔎 Контекст #2321»: «это перебор, очень много текста, читать невозможно». Кнопка
отдавала карточку писателя целиком — ЦИФРЫ, ТРЕНД, ЧТО БЫЛО ПОСЛЕ, АНАЛОГИЯ, КОНТЕКСТ, ОГРАНИЧЕНИЯ. Писателю
она нужна полностью, человеку — только то, на чём стоит пост: находка, тренд, одна история, итог,
итог толпы, цена, экспирация. Модуль без зависимостей: разбирает текст карточки по заголовкам.
"""
import re

# «ГЛАВНОЕ - строй пост вокруг этого:» — заголовок без значения (внутри бывают «…: …» в кавычках-ёлочках)
_HEADER = re.compile(r"^([А-ЯЁ][А-ЯЁ ]+?)(?: - .*)?:$")
# «НАХОДКА: …», «ДАТА ДАННЫХ: 16 сентября» — заголовок со значением
_KEY_VALUE = re.compile(r"^([А-ЯЁ][А-ЯЁ ]{2,}):\s*(.+)$")
_PREFIX = re.compile(r"^(находка(, данные на [^:]+)?|тренд|одна история|опора для вывода|"
                     r"подтверждение из других данных \([^)]*\)(, данные на [^:]+)?)\s*:\s*")


def _short(s: str, n: int = 220) -> str:
    s = s.strip()
    return s if len(s) <= n else s[:n - 1].rstrip() + "…"


def _sections(raw: str) -> dict:
    blocks, sec = {}, ""
    for line in raw.splitlines():
        s = line.strip()
        if not s:
            continue
        if not s.startswith("-"):
            m = _HEADER.match(s)
            if m:
                sec = m.group(1).strip()
                continue
            m = _KEY_VALUE.match(s)
            if m:
                sec = m.group(1).strip()
                blocks.setdefault(sec, []).append(m.group(2).strip())
                continue
        blocks.setdefault(sec, []).append(s.lstrip("- ").strip())
    return blocks


def summarize(raw: str) -> str:
    b = _sections(raw or "")
    title = (b.get("НАХОДКА") or b.get("СЮЖЕТ") or [""])[0]
    date = (b.get("ДАТА ДАННЫХ") or [""])[0]
    out = [_short(title, 260) + (f" · данные на {date}" if date else "")]
    out += [f"📰 {_short(x, 160)}" for x in (b.get("ПОВОД") or [])[:2]]
    for x in b.get("ГЛАВНОЕ") or []:
        if x.startswith("как читать") or (x.startswith("находка:") and title and title in x):
            continue
        x = _PREFIX.sub("", x).replace("главное - тренд, а не прошлый пик: ", "")
        out.append(f"• {_short(x)}")
    crowd = next((x for x in b.get("ЦИФРЫ") or [] if x.startswith("итог толпы")), None)
    if crowd:
        out.append(f"• {_short(crowd.split(' - оценка')[0])}")
    price = (b.get("ЦЕНА И ФОН") or [None])[0]
    if price:
        out.append(f"• {_short(price)}")
    expiry = next((x for x in b.get("ОГРАНИЧЕНИЯ") or [] if "экспирац" in x), None)
    if expiry:
        out.append(f"⚠️ {_short(expiry.split(':')[0])}")
    return "\n".join(out)
