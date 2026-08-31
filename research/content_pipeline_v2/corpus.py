#!/usr/bin/env python3
"""Корпус реальных постов канала из выгрузки Telegram Desktop.

Читать готовый корпус: gzip.open(dataset/corpus.json.gz, "rt") → json.load.

Зачем: положительного класса у нас нет — «published» означало «одобрено в тестовый
канал», и Вадим 31.08 подтвердил, что июльские одобрения были снисходительными
(см. EVAL_RUN1.md, прогон 2). Эталон приходится строить заново из реальных постов.
Корпус нужен трижды:
  1. положительный класс для калибровки судьи (Фаза 2);
  2. динамические few-shot — 2-3 ситуативно похожих поста вместо 4 статичных
     образцов в промпте (Фаза 4б);
  3. ⭐ проверка правил промпта по факту, а не по декларации. Два правила Шага В
     уже опровергнуты собственными образцами промпта («одна-две цифры на абзац»,
     «Данные с платформы Frame»). Корпус говорит, как канал пишет НА САМОМ ДЕЛЕ.

⚠️ Технический потолок выгрузки Telegram Desktop: в JSON НЕТ views/forwards/reactions
(проверено в июле 2026). Отранжировать эталон по охвату по этим данным нельзя —
для этого нужна MTProto-сессия.

Запуск (пути ищутся сами в ~/Downloads/ChatExport*):
  python3 research/content_pipeline_v2/corpus.py
  python3 research/content_pipeline_v2/corpus.py --paths ~/Downloads/ChatExport_2026-08-31
"""
import argparse
import glob
import gzip
import json
import os
import re
import statistics
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
# .gz, а не сырой json: 1459 постов = 3,6 МБ текстом против 0,75 МБ сжатым.
# Корпус — снимок, который меняется редко и целиком, поэтому потеря дельт в git
# ничего не стоит, а 3 МБ на каждый пересбор — стоят.
OUT = os.path.join(HERE, "dataset", "corpus.json.gz")

# Маркеры абзаца: Frame использует ◽️, у Thor встречается ещё 🔽/🔼 (формат-анализ
# 13.07). Полный список — чтобы n_paras не занулялся молча на чужом канале.
PARA_MARKERS = ("◽", "🔽", "🔼", "▪", "◾", "•")

# Те же штампы и бренд, что проверяет judge.py — специально ОДИН список смыслов,
# чтобы сравнение «наши черновики против реального канала» было честным.
STOCK_PHRASES = [
    (r"[Оо]дин сигнал\s*[-—]\s*не приговор", "один сигнал - не приговор"),
    (r"покажут\s+(?:не\s+)?ближайшие дни", "покажут ближайшие дни"),
    (r"стоит последить", "стоит последить"),
    (r"[Оо]днозначно (?:не )?трактов", "однозначно трактовать"),
    (r"совпадение по времени ничего не говорит", "совпадение по времени ничего не говорит"),
    (r"[Нн]е панацея", "не панацея"),
]
BRAND_RE = (r"(?:данн\w*\s+(?:с\s+платформы\s+)?Frame|платформ\w*\s+Frame"
            r"|индикатор\w*\s+Frame|Frame\s+показ\w+|Frame\s+отмеча\w+|Frame\s+зафиксирова\w+)")


def flatten_text(msg: dict) -> str:
    """В выгрузке поле text — либо строка, либо список из строк и объектов-сущностей
    (ссылки, жирный, хэштеги). text_entities надёжнее: там всегда список словарей.
    Берём его, а на text падаем только если entities отсутствуют."""
    ents = msg.get("text_entities")
    if isinstance(ents, list) and ents:
        return "".join(e.get("text", "") for e in ents)
    t = msg.get("text")
    if isinstance(t, str):
        return t
    if isinstance(t, list):
        return "".join(p if isinstance(p, str) else p.get("text", "") for p in t)
    return ""


def split_paras(text: str) -> list:
    """Абзацы по маркеру, а если маркеров нет — по пустым строкам. Без фоллбэка
    посты Thor и старые посты Frame дали бы n_paras=0 и испортили статистику."""
    lines = [l.strip() for l in text.split("\n")]
    marked = [l for l in lines if l and l.lstrip().startswith(PARA_MARKERS)]
    if marked:
        return marked
    return [b.strip() for b in re.split(r"\n\s*\n", text) if b.strip()]


def count_numbers(s: str) -> int:
    """Только показатели: проценты, множители, суммы, уровни. Даты и время НЕ
    считаем — иначе «15 июля» и «10:24» раздувают плотность и метрика врёт."""
    s = re.sub(r"\b\d{1,2}[:.]\d{2}\b", " ", s)                      # время
    s = re.sub(r"\b\d{1,2}\s+(?:янв|фев|мар|апр|ма[йя]|июн|июл|авг|сен|окт|ноя|дек)\w*", " ", s)
    # ⚠️ Годы вырезаем ТОЛЬКО в правдоподобном диапазоне 1990-2039. Наивное
    # `(?:19|20)\d{2}` съедало уровень индекса: IMOEX торгуется ровно в 2000-2999,
    # и «IMOEX у 2060,51» терял число (поймано на фикстуре). Уровни выше 2039
    # теперь остаются показателями, как и должны.
    s = re.sub(r"\b(?:199\d|20[0-2]\d|203\d)\b", " ", s)
    return len(re.findall(r"\d+(?:[.,]\d+)?", s))


def parse_export(path: str) -> list:
    """path — каталог выгрузки или сам result.json."""
    jf = path if path.endswith(".json") else os.path.join(path, "result.json")
    if not os.path.exists(jf):
        raise FileNotFoundError(f"нет {jf}")
    raw = json.load(open(jf, encoding="utf-8"))
    # ⚠️ Защита от личной переписки. Telegram Desktop экспортирует и каналы, и
    # ЛС одним и тем же форматом, различаются только полем type. Живой случай
    # 31.08: вместо канала «Thor | Alexandr Toria» пришёл personal_chat «Тория»
    # на 25 647 сообщений — переписка с коллегой. Для корпуса постов она
    # бесполезна, а складывать её в репозиторий нельзя. Пропускаем громко.
    ctype = raw.get("type", "")
    if "channel" not in ctype:
        raise ValueError(
            f"это не канал, а {ctype!r} («{raw.get('name')}», "
            f"{len(raw.get('messages', []))} сообщений) — пропущено. "
            f"Нужен экспорт КАНАЛА, не личного чата.")
    channel = raw.get("name") or os.path.basename(os.path.dirname(jf))
    posts = []
    for m in raw.get("messages", []):
        if m.get("type") != "message":
            continue                       # сервисные сообщения (создание канала и пр.)
        text = flatten_text(m).strip()
        if len(text) < 40:
            continue                       # репост-заглушки, одиночные картинки, «👍»
        paras = split_paras(text)
        lines = [l for l in text.split("\n") if l.strip()]
        posts.append({
            "channel": channel,
            "msg_id": m.get("id"),
            "date": (m.get("date") or "")[:10],
            "text": text,
            "title": lines[0] if lines else "",
            "conclusion": paras[-1] if paras else "",
            "length": len(text),
            "n_paras": len(paras),
            "has_photo": bool(m.get("photo") or m.get("media_type") == "photo"),
            "hashtags": re.findall(r"#\w+", text),
            "max_numbers_in_para": max((count_numbers(p) for p in paras), default=0),
            "stock_phrases": [n for pat, n in STOCK_PHRASES if re.search(pat, text)],
            "brand_mentions": re.findall(BRAND_RE, text, re.I),
        })
    return posts


def report(posts: list, label: str) -> None:
    if not posts:
        print(f"\n{label}: пусто")
        return
    n = len(posts)
    lens = sorted(p["length"] for p in posts)
    paras = [p["n_paras"] for p in posts]
    nums = [p["max_numbers_in_para"] for p in posts]
    print(f"\n=== {label}: {n} постов, {min(p['date'] for p in posts)} … {max(p['date'] for p in posts)}")
    print(f"  длина:      медиана {statistics.median(lens):.0f}, "
          f"10-90%% [{lens[int(.1*n)]}; {lens[int(.9*n)-1 if n>1 else 0]}]")
    print(f"  абзацев:    {dict(sorted(Counter(paras).items()))}")
    print(f"  максимум показателей в одном абзаце: {dict(sorted(Counter(nums).items()))}")
    print(f"  со скриншотом: {sum(p['has_photo'] for p in posts)/n:.0%}")
    st = sum(1 for p in posts if p["stock_phrases"])
    br = sum(1 for p in posts if p["brand_mentions"])
    print(f"  ⭐ со штампом-концовкой: {st}/{n} = {st/n:.0%}")
    print(f"  ⭐ с именем бренда в теле: {br}/{n} = {br/n:.0%}")
    tags = Counter(t for p in posts for t in p["hashtags"])
    print(f"  топ хэштегов: {dict(tags.most_common(6))}")
    op = Counter(" ".join(p["conclusion"].lstrip("".join(PARA_MARKERS) + "️ ").split()[:4])
                 for p in posts)
    rep = {k: v for k, v in op.most_common(5) if v > 1}
    print(f"  повторяющиеся начала концовки: {rep or 'нет — все разные'}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--paths", nargs="*", help="каталоги выгрузок; по умолчанию ~/Downloads/ChatExport*")
    args = ap.parse_args()
    paths = args.paths or sorted(glob.glob(os.path.expanduser("~/Downloads/ChatExport*")))
    if not paths:
        print("выгрузок не найдено. Сделай экспорт Telegram Desktop (формат JSON, "
              "медиа выключить) — файлы лягут в ~/Downloads/ChatExport_<дата>/",
              file=sys.stderr)
        sys.exit(1)

    all_posts = []
    for p in paths:
        try:
            got = parse_export(os.path.expanduser(p))
        except Exception as e:
            print(f"  {p}: не разобрано — {type(e).__name__}: {e}", file=sys.stderr)
            continue
        print(f"  {p}: {len(got)} постов", file=sys.stderr)
        all_posts.extend(got)

    if not all_posts:
        print("ни одного поста не разобрано", file=sys.stderr)
        sys.exit(1)
    with gzip.open(OUT, "wt", encoding="utf-8", compresslevel=9) as f:
        json.dump(all_posts, f, ensure_ascii=False)
    print(f"\nкорпус записан: {OUT} ({len(all_posts)} постов, "
          f"{os.path.getsize(OUT)/1e6:.2f} МБ)")

    for ch in sorted({p["channel"] for p in all_posts}):
        report([p for p in all_posts if p["channel"] == ch], ch)

    # Сравнение с нашими черновиками — главный смысл упражнения: правила промпта
    # проверяются по реальному каналу, а не по декларации внутри самого промпта.
    dpath = os.path.join(HERE, "dataset", "drafts.json")
    if os.path.exists(dpath):
        drafts = [d for d in json.load(open(dpath, encoding="utf-8")) if d.get("draft_text")]
        ours = []
        for d in drafts:
            t = d["draft_text"]
            paras = split_paras(t)
            ours.append({
                "channel": "НАШИ ЧЕРНОВИКИ (ИИ)", "date": d["created_at"][:10],
                "text": t, "title": t.split("\n")[0], "conclusion": paras[-1] if paras else "",
                "length": len(t), "n_paras": len(paras), "has_photo": False,
                "hashtags": re.findall(r"#\w+", t),
                "max_numbers_in_para": max((count_numbers(p) for p in paras), default=0),
                "stock_phrases": [n for pat, n in STOCK_PHRASES if re.search(pat, t)],
                "brand_mentions": re.findall(BRAND_RE, t, re.I),
            })
        report(ours, "НАШИ ЧЕРНОВИКИ (ИИ) — для сравнения")


if __name__ == "__main__":
    main()
