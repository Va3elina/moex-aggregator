#!/usr/bin/env python3
"""Слепой тест: посты человека против постов ИИ, перемешанные.

Запрос Вадима: «хочу прогнать тест и посмотреть вслепую, что написали мы и что
напишут агенты». Изначально искали ПАРЫ «одно событие — две версии», но их нашлось
всего три: канал FRAME за весь период — 82 поста, и он почти всегда пишет про свой
индикатор, а не про новость, тогда как черновики ИИ всегда привязаны к инфоповоду.
Пересечения возникают только на новостях, которые канал не мог пропустить.

Поэтому тест устроен иначе и это статистически сильнее: N постов канала и N
черновиков ИИ перемешиваются без привязки к событиям, Вадим размечает каждый.
Именно точность его разметки и есть ответ на вопрос «отличимо ли».

⚠️ НОРМАЛИЗАЦИЯ ОБЯЗАТЕЛЬНА. Без неё тест измерит внимательность к форматированию,
а не качество текста: у постов канала в конце всегда стоит подпись, а у части
черновиков её нет; длинное тире у ИИ встречается 2,5 раза на пост против 0,12 у
человека — это ловится глазом мгновенно и обнуляет слепоту. Снимаем подпись,
хэштеги и приводим тире к одному виду.

⚠️ Один черновик (744) исключён: пост канала на ту же историю вышел на следующий
день с почти дословным заголовком — похоже, человек переписывал черновик ИИ, а не
писал независимо. В тесте на отличимость такая пара сломала бы вывод.

Запуск:
  python3 blind_test.py --build          # собрать набор + ключ
  python3 blind_test.py --key            # показать ключ (после ответов!)
"""
import argparse
import gzip
import json
import os
import random
import re

HERE = os.path.dirname(os.path.abspath(__file__))
CORPUS = os.path.join(HERE, "dataset", "corpus.json.gz")
DRAFTS = os.path.join(HERE, "dataset", "drafts.json")
OUT = os.path.join(HERE, "dataset", "blind_test.json")
GENRE = ("#открытыйинтерес", "#открытыепозиции")
SEED = 20260831
EXCLUDE_DRAFTS = {744}   # см. докстринг: не независимая пара


def normalize(text: str) -> str:
    """Снять всё, что выдаёт источник механически, не трогая сам текст."""
    t = text
    t = re.sub(r"😀😀😀\s*/?\s*@\w+", "", t)          # подпись канала
    t = re.sub(r"(?m)^\s*#[\wА-Яа-яёЁ]+(\s+#[\wА-Яа-яёЁ]+)*\s*$", "", t)  # строки хэштегов
    t = t.replace("—", "-").replace("–", "-")          # тире к одному виду
    t = re.sub(r"[ \t]+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--key", action="store_true")
    ap.add_argument("-n", type=int, default=10, help="сколько с каждой стороны")
    a = ap.parse_args()

    if a.key:
        data = json.load(open(OUT, encoding="utf-8"))
        print("КЛЮЧ (не смотреть до ответов):")
        for item in data["items"]:
            print(f"  #{item['n']:>2}  {'ЧЕЛОВЕК' if item['human'] else 'ИИ     '}  "
                  f"{item['origin']}")
        return

    rnd = random.Random(SEED)
    with gzip.open(CORPUS, "rt", encoding="utf-8") as f:
        corpus = json.load(f)
    human = [p for p in corpus
             if p["channel"].upper().startswith("FRAME")
             and any(t in p["hashtags"] for t in GENRE)]
    drafts = [d for d in json.load(open(DRAFTS, encoding="utf-8"))
              if d.get("draft_text") and d["id"] not in EXCLUDE_DRAFTS]

    # Из постов канала берём только те, что попадают в диапазон длин черновиков —
    # иначе тест решится по длине, а не по тексту.
    lo = min(len(d["draft_text"]) for d in drafts)
    hi = max(len(d["draft_text"]) for d in drafts)
    human_ok = [p for p in human if lo <= p["length"] <= hi]

    n = min(a.n, len(human_ok), len(drafts))
    pick_h = rnd.sample(human_ok, n)
    pick_a = rnd.sample(drafts, n)

    items = []
    for p in pick_h:
        items.append({"human": True, "origin": f"канал FRAME {p['date']}",
                      "text": normalize(p["text"])})
    for d in pick_a:
        items.append({"human": False, "origin": f"черновик ИИ id={d['id']} ({d['status']})",
                      "text": normalize(d["draft_text"])})
    rnd.shuffle(items)
    for i, it in enumerate(items, 1):
        it["n"] = i

    json.dump({"seed": SEED, "n_each": n, "items": items},
              open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"собрано {len(items)} текстов ({n} человек + {n} ИИ), перемешано seed={SEED}")
    print(f"диапазон длин черновиков: {lo}-{hi} знаков; постов канала в диапазоне: "
          f"{len(human_ok)} из {len(human)}")
    print(f"→ {OUT}")


if __name__ == "__main__":
    main()
