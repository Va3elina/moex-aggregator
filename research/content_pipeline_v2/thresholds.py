#!/usr/bin/env python3
"""Аудит стилевых правил промпта Шага В против реального корпуса канала.

Зачем: правила в промпте накапливались по одному после каждого плохого поста и
НИ ОДНО не проверялось по факту. Уже два из них опровергнуты собственными
образцами промпта («одна-две цифры на абзац», «Данные с платформы Frame»).
Здесь каждое проверяемое правило сверяется с 82 постами FRAME — что канал делает
НА САМОМ ДЕЛЕ. Выход: рекомендованные пороги для RUBRIC.md.

Запуск: python3 research/content_pipeline_v2/thresholds.py
"""
import gzip
import json
import os
import re
import statistics
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
CORPUS = os.path.join(HERE, "dataset", "corpus.json.gz")
DRAFTS = os.path.join(HERE, "dataset", "drafts.json")

TITLE_EMOJI = "🔥📣💡🔍⛔️📋🔔📈📉🏆🚨💰⚡️🤔😱🎯🧨🔽🔼"


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return 0
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


def paras_of(text):
    lines = [l.strip() for l in text.split("\n")]
    marked = [l for l in lines if l and l.lstrip().startswith(("◽", "🔽", "🔼", "▪", "◾", "•"))]
    return marked or [b.strip() for b in re.split(r"\n\s*\n", text) if b.strip()]


def count_numbers(s):
    s = re.sub(r"\b\d{1,2}[:.]\d{2}\b", " ", s)
    s = re.sub(r"\b\d{1,2}\s+(?:янв|фев|мар|апр|ма[йя]|июн|июл|авг|сен|окт|ноя|дек)\w*", " ", s)
    s = re.sub(r"\b(?:199\d|20[0-2]\d|203\d)\b", " ", s)
    return len(re.findall(r"\d+(?:[.,]\d+)?", s))


def measure(posts):
    """Каждая метрика — ровно то, что утверждает какое-то правило промпта."""
    n = len(posts)
    # ⚠️ Первая строка — заголовок ТОЛЬКО у многоабзацных постов с короткой первой
    # строкой. Иначе одноабзацный пост целиком уезжал в «заголовок» и давал 49 слов.
    titles = []
    for p in posts:
        lines = [l for l in p["text"].split("\n") if l.strip()]
        first = lines[0].strip() if lines else ""
        if len(lines) >= 2 and len(first) <= 100:
            titles.append(first)
    if not titles:
        titles = [""]
    m = {
        "n": n,
        "длина": [p["length"] for p in posts],
        "абзацев": [len(paras_of(p["text"])) for p in posts],
        "показателей в абзаце (макс)": [max((count_numbers(x) for x in paras_of(p["text"])), default=0)
                                         for p in posts],
        "слов в заголовке": [len(re.sub(r"[^\w\s]", " ", t).split()) for t in titles],
    }
    m["доли"] = {
        "заголовок кончается эмодзи": sum(1 for t in titles if t and t[-1] in TITLE_EMOJI or
                                            (len(t) > 1 and t[-2] in TITLE_EMOJI)) / n,
        "есть подпись @FrameTool": sum(1 for p in posts if "@FrameTool" in p["text"]) / n,
        "есть хэштег #открытыйинтерес": sum(1 for p in posts if "#открытыйинтерес" in p["text"]) / n,
        "есть ХОТЬ КАКОЙ хэштег": sum(1 for p in posts if "#" in p["text"]) / n,
        "ДЛИННОЕ тире «—» в тексте": sum(1 for p in posts if "—" in p["text"]) / n,
        "только одинарное «-»": sum(1 for p in posts if "—" not in p["text"] and "-" in p["text"]) / n,
        "маркер списка 📌": sum(1 for p in posts if "📌" in p["text"]) / n,
        "маркер абзаца ◽️": sum(1 for p in posts if "◽" in p["text"]) / n,
        "слово «сегодня»/«вчера»": sum(1 for p in posts
                                        if re.search(r"\b(сегодня|вчера)\b", p["text"], re.I)) / n,
        # Аббревиатуры (ОФЗ/ВВП/РСБУ) — не крик. Крик = ДВА капс-слова подряд
        # («АКЦИИ ГАЗПРОМА ПРОБИЛИ»), именно это запрещает правило промпта.
        "капс-крик (2+ слова подряд)": sum(
            1 for p in posts if re.search(r"\b[А-ЯЁ]{3,}\b[\s«»\"]+\b[А-ЯЁ]{3,}\b", p["text"])) / n,
        "маркдаун **жирный**": sum(1 for p in posts if "**" in p["text"]) / n,
    }
    return m


def show(label, m):
    print(f"\n═══ {label} (n={m['n']})")
    for k in ("длина", "абзацев", "показателей в абзаце (макс)", "слов в заголовке"):
        xs = m[k]
        print(f"  {k:<30} медиана {statistics.median(xs):>6.0f}   "
              f"p10 {pct(xs,10):>5}  p90 {pct(xs,90):>5}  p95 {pct(xs,95):>5}  макс {max(xs):>5}")
    print("  ── доли постов ──")
    for k, v in m["доли"].items():
        print(f"  {k:<32} {v:>5.0%}")


def main():
    with gzip.open(CORPUS, "rt", encoding="utf-8") as f:
        corpus = json.load(f)
    frame_all = [p for p in corpus if p["channel"].upper().startswith("FRAME")]
    # ⚠️ ЖАНРОВЫЙ СРЕЗ. Канал пишет и новостные посты по ОИ, и обучающие лонгриды
    # про методологию, и сезонность. Конвейер производит ровно ОДИН жанр — «новость ×
    # позиции толпы». Пороги надо брать по нему, иначе обучающий лонгрид на 2780
    # знаков разрешит генератору писать вдвое длиннее, чем принято в его жанре.
    GENRE = ("#открытыйинтерес", "#открытыепозиции")
    frame = [p for p in frame_all if any(t in p["hashtags"] for t in GENRE)]
    thor = [p for p in corpus if p["channel"].startswith("Thor")]
    drafts = [{"text": d["draft_text"], "length": len(d["draft_text"])}
              for d in json.load(open(DRAFTS, encoding="utf-8")) if d.get("draft_text")]

    mf, mt, md = measure(frame), measure(thor), measure(drafts)
    print(f"\nFRAME всего {len(frame_all)} постов; жанровый срез "
          f"(#открытыйинтерес/#открытыепозиции) — {len(frame)}")
    show("FRAME, жанр «позиции толпы» — ЭТАЛОН для конвейера", mf)
    show("FRAME, все жанры — для контраста", measure(frame_all))
    show("Thor — расширение голоса", mt)
    show("НАШИ ЧЕРНОВИКИ (ИИ)", md)

    print("\n\n═══ ПРАВИЛА ПРОМПТА ШАГА В против факта (FRAME) ═══\n")
    verdicts = [
        ("«Заголовок: 2-5 слов»",
         f"медиана {statistics.median(mf['слов в заголовке']):.0f}, "
         f"p90 {pct(mf['слов в заголовке'],90)}, макс {max(mf['слов в заголовке'])}",
         pct(mf["слов в заголовке"], 90) <= 5),
        ("«один эмодзи в конце заголовка»",
         f"{mf['доли']['заголовок кончается эмодзи']:.0%} постов", 
         mf["доли"]["заголовок кончается эмодзи"] >= 0.7),
        ("«Тело: 2-5 абзацев»",
         f"факт 1-{max(mf['абзацев'])}, мода {statistics.mode(mf['абзацев'])}, "
         f"p90 {pct(mf['абзацев'],90)}",
         pct(mf["абзацев"], 90) <= 5),
        ("«Целься в 400-1200 знаков»",
         f"p10 {pct(mf['длина'],10)}, медиана {statistics.median(mf['длина']):.0f}, "
         f"p90 {pct(mf['длина'],90)}",
         pct(mf["длина"], 90) <= 1200),
        ("«Одна-две цифры на абзац — потолок»",
         f"мода {statistics.mode(mf['показателей в абзаце (макс)'])}, "
         f"p90 {pct(mf['показателей в абзаце (макс)'],90)}, "
         f"нарушают {sum(1 for x in mf['показателей в абзаце (макс)'] if x>2)/mf['n']:.0%} постов",
         pct(mf["показателей в абзаце (макс)"], 90) <= 2),
        ("«Тире одинарное -, не длинное —»",
         f"длинное тире у {mf['доли']['ДЛИННОЕ тире «—» в тексте']:.0%} постов канала",
         mf["доли"]["ДЛИННОЕ тире «—» в тексте"] <= 0.1),
        ("«Последние строки: 😀😀😀/@FrameTool»",
         f"подпись есть у {mf['доли']['есть подпись @FrameTool']:.0%} постов",
         mf["доли"]["есть подпись @FrameTool"] >= 0.8),
        ("«хэштег #открытыйинтерес»",
         f"у {mf['доли']['есть хэштег #открытыйинтерес']:.0%}; "
         f"хоть какой хэштег у {mf['доли']['есть ХОТЬ КАКОЙ хэштег']:.0%}",
         mf["доли"]["есть хэштег #открытыйинтерес"] >= 0.8),
        ("«каждый абзац с ◽️»",
         f"у {mf['доли']['маркер абзаца ◽️']:.0%} постов",
         mf["доли"]["маркер абзаца ◽️"] >= 0.8),
        ("«Перечисления через 📌»",
         f"📌 встречается у {mf['доли']['маркер списка 📌']:.0%} постов",
         mf["доли"]["маркер списка 📌"] >= 0.1),
        ("«Не писать сегодня/вчера»",
         f"канал пишет это в {mf['доли']['слово «сегодня»/«вчера»']:.0%} постов",
         mf["доли"]["слово «сегодня»/«вчера»"] <= 0.1),
        ("«Не использовать капс»",
         f"капс-крик у {mf['доли']['капс-крик (2+ слова подряд)']:.0%} постов "
         f"(аббревиатуры ОФЗ/ВВП не считаются)",
         mf["доли"]["капс-крик (2+ слова подряд)"] <= 0.15),
        ("«Не использовать маркдаун»",
         f"** у {mf['доли']['маркдаун **жирный**']:.0%} постов",
         mf["доли"]["маркдаун **жирный**"] <= 0.05),
    ]
    for rule, fact, ok in verdicts:
        print(f"  {'✅ ПОДТВЕРЖДЕНО' if ok else '❌ ОПРОВЕРГНУТО '}  {rule}")
        print(f"      факт: {fact}")

    print("\n\n═══ РЕКОМЕНДОВАННЫЕ ПОРОГИ ДЛЯ РУБРИКИ (по FRAME, p5-p95) ═══")
    print(f"  длина:                    {pct(mf['длина'],5)} … {pct(mf['длина'],95)} знаков "
          f"(было в рубрике 400-1200)")
    print(f"  абзацев:                  {pct(mf['абзацев'],5)} … {pct(mf['абзацев'],95)} "
          f"(было 2-5), мода {statistics.mode(mf['абзацев'])}")
    print(f"  показателей в абзаце:     ≤ {pct(mf['показателей в абзаце (макс)'],95)} "
          f"(было ≤2, потом ≤3)")
    print(f"  слов в заголовке:         {pct(mf['слов в заголовке'],5)} … "
          f"{pct(mf['слов в заголовке'],95)} (было 2-5)")

    print("\n\n═══ РАЗРЫВ: наши черновики против жанрового эталона ═══")
    import statistics as st
    gaps = [
        ("длина, медиана", st.median(md["длина"]), st.median(mf["длина"])),
        ("абзацев, медиана", st.median(md["абзацев"]), st.median(mf["абзацев"])),
        ("показателей в абзаце, медиана", st.median(md["показателей в абзаце (макс)"]),
         st.median(mf["показателей в абзаце (макс)"])),
        ("слов в заголовке, медиана", st.median(md["слов в заголовке"]),
         st.median(mf["слов в заголовке"])),
    ]
    for name, ours, ref in gaps:
        rel = f"×{ours/ref:.1f}" if ref else "—"
        print(f"  {name:<32} мы {ours:>6.0f}   эталон {ref:>6.0f}   {rel}")
    for k in ("ДЛИННОЕ тире «—» в тексте", "маркер абзаца ◽️", "есть подпись @FrameTool"):
        print(f"  {k:<32} мы {md['доли'][k]:>5.0%}   эталон {mf['доли'][k]:>5.0%}")


if __name__ == "__main__":
    main()
