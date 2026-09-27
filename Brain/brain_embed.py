#!/usr/bin/env python3
"""Эмбеддинги узлов второго мозга → brain_embeddings.

⚠️ ВЕКТОР — ВХОД ПО СМЫСЛУ, НЕ РЕБРО. Рёбра остаются структурными (владеет, держит,
упоминает…); вектор отвечает «что похоже» и даёт агенту якорь для новости без
тикера: ближайшие компании и похожие поводы. Из близости рёбра не строятся.

⚠️ МОДЕЛЬ СТАТИЧЕСКАЯ (model2vec, potion-multilingual-128M, 256 измерений): инференс
на numpy, без torch и GPU, тысячи текстов в секунду. Веса — 512 МБ float32 на
диске (/opt/frame/models), в память грузятся int8 (~130 МБ). Путь — EMBED_MODEL_DIR.

Что эмбеддим — единый формат для всех источников (Вадим 27.09: «машиночитаемо, под
векторную базу»): «тип события · компании · отрасль · суть». Тип и отрасль — из единой
разметки (Brain/vocab.py, рёбра «тип»/«отрасль»), суть — чистый текст узла (без хэштегов,
эмодзи, ссылок). Новость про дивиденды Сбера и раскрытие FinanceMarker о них ложатся рядом,
хотя пришли из разных источников и разными словами. У компании — название, сектор, полное
имя и тикеры. Документы («Отчёт · 2022») не эмбеддим — в заголовке нет смысла.
Пересчёт — только когда текст изменился (text_hash): сменился тег — синк сдвигает
updated_at узла, текст для вектора другой, вектор пересчитывается.

Запуск: python Brain/brain_embed.py [--full] [--limit N]
Итог — JSON последней строкой для оркестратора.
"""
import argparse
import hashlib
import json
import os
import sys
import time

import numpy as np
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import vocab  # noqa: E402 — рёбра от события к компании

DB_URL = os.getenv("DB_URL")
MODEL_DIR = os.getenv("EMBED_MODEL_DIR", "/app/models/potion-multilingual-128M-int8")
MODEL_NAME = "potion-multilingual-128M"
ВИДЫ = ("company", "news", "candidate", "post", "fact", "signal", "anomaly", "fund", "index", "holder",
        "disclosure", "exchange", "report",   # официальные события — чтобы находил поиск по смыслу
        "index_event", "fund_event")
ПАЧКА = 2000

_модель = None


def модель():
    global _модель
    if _модель is None:
        from model2vec import StaticModel
        # квантованная копия на диске (…-int8) грузится 3,5 с; fp32 с квантованием — 10 с
        _модель = StaticModel.from_pretrained(MODEL_DIR) if MODEL_DIR.endswith("-int8") else StaticModel.from_pretrained(MODEL_DIR, quantize_to="int8")
    return _модель


def текст_узла(kind: str, title: str, summary, payload, тип=None, компании=None, отрасль=None) -> str:
    if kind == "company":
        части = [title or ""]
        if payload:
            части += [str(payload.get("sector") or ""), str(payload.get("name_full") or ""),
                      " ".join(payload.get("secids") or [])]
        return " · ".join(x for x in части if x).strip()
    суть = [title or ""]
    if summary:
        # У новости суть — весь чистый текст, заголовок — его начало: второй раз не повторяем.
        if str(summary).startswith((title or "").rstrip("…")[:60]):
            суть = [str(summary)[:600]]
        else:
            суть.append(str(summary)[:600])
    return " · ".join(x for x in [тип, компании, отрасль, *суть] if x).strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="пересчитать все, не только изменившиеся")
    ap.add_argument("--limit", type=int, default=50000)
    args = ap.parse_args()
    t0 = time.time()
    eng = create_engine(DB_URL)
    with eng.connect() as conn:
        строки = conn.execute(text(f"""
            SELECT n.id, n.kind, n.title, n.summary, n.payload, e.text_hash,
                   (SELECT string_agg(t.title, ', ') FROM brain_edges x JOIN brain_nodes t ON t.id = x.dst
                     WHERE x.src = n.id AND x.kind = 'тип') AS тип,
                   (SELECT string_agg(DISTINCT c.title, ', ') FROM brain_edges x JOIN brain_nodes c ON c.id = x.dst
                     WHERE x.src = n.id AND x.kind = ANY(:рёбра) AND starts_with(x.dst, 'company:')) AS компании,
                   (SELECT string_agg(s.title, ', ') FROM brain_edges x JOIN brain_nodes s ON s.id = x.dst
                     WHERE x.src = n.id AND x.kind = 'отрасль') AS отрасль
              FROM brain_nodes n
              LEFT JOIN brain_embeddings e ON e.node_id = n.id
             WHERE n.kind = ANY(string_to_array(:kinds, ','))
               AND ({'TRUE' if args.full else 'e.node_id IS NULL OR e.updated_at < n.updated_at'})
             ORDER BY n.ts DESC NULLS LAST
             LIMIT :lim
        """), {"kinds": ",".join(ВИДЫ), "lim": args.limit, "рёбра": list(vocab.РЁБРА_К_КОМПАНИИ)}).all()
    задачи = []
    for id_, kind, title, summary, payload, старый_хэш, тип, компании, отрасль in строки:
        т = текст_узла(kind, title, summary, payload, тип, компании, отрасль)
        if not т:
            continue
        h = hashlib.md5(т.encode()).hexdigest()
        if h == старый_хэш and not args.full:
            # updated_at узла сдвинулся (пересинк), а текст тот же — вектор актуален
            continue
        задачи.append((id_, т, h))
    if not задачи:
        print(json.dumps({"новых": 0, "всего": _всего(eng), "сек": round(time.time() - t0, 1)}, ensure_ascii=False))
        return 0
    m = модель()
    записано = 0
    for i in range(0, len(задачи), ПАЧКА):
        пачка = задачи[i:i + ПАЧКА]
        векторы = m.encode([т for _, т, _ in пачка], show_progress_bar=False)
        векторы = np.asarray(векторы, dtype=np.float32)
        нормы = np.linalg.norm(векторы, axis=1, keepdims=True)
        векторы = векторы / np.where(нормы == 0, 1, нормы)
        with eng.begin() as conn:
            conn.execute(text("""
                INSERT INTO brain_embeddings (node_id, model, embedding, text_hash, updated_at)
                VALUES (:id, :m, CAST(:v AS vector), :h, NOW())
                ON CONFLICT (node_id) DO UPDATE SET embedding = EXCLUDED.embedding, text_hash = EXCLUDED.text_hash,
                                                    model = EXCLUDED.model, updated_at = NOW()
            """), [{"id": id_, "m": MODEL_NAME, "v": "[" + ",".join(f"{x:.6f}" for x in v) + "]", "h": h}
                   for (id_, _, h), v in zip(пачка, векторы)])
        записано += len(пачка)
    print(json.dumps({"новых": записано, "всего": _всего(eng), "сек": round(time.time() - t0, 1),
                      "режим": "полный" if args.full else "инкремент"}, ensure_ascii=False))
    return 0


def _всего(eng) -> int:
    with eng.connect() as conn:
        return int(conn.execute(text("SELECT COUNT(*) FROM brain_embeddings")).scalar() or 0)


if __name__ == "__main__":
    sys.exit(main())
