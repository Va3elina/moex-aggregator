#!/usr/bin/env python3
"""Тесты парсера корпуса. Запуск: python3 research/content_pipeline_v2/test_corpus.py

Зачем тесты у research-скрипта: парсер разбирает ЧУЖОЙ формат (выгрузка Telegram
Desktop), у которого есть неочевидные ловушки, и отлаживать его на живой выгрузке
поздно. Каждый кейс ниже — реальная ловушка, а не гипотетическая.
"""
import importlib.util
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("corpus", os.path.join(HERE, "corpus.py"))
C = importlib.util.module_from_spec(spec)
spec.loader.exec_module(C)

fails = []


def check(cond, msg):
    print(("  ✓ " if cond else "  ✗ ") + msg)
    if not cond:
        fails.append(msg)


print("count_numbers — считаем показатели, но не даты/время/годы:")
# ⚠️ Главная ловушка: наивное (?:19|20)\d{2} съедало уровень индекса. IMOEX
# торгуется ровно в 2000-2999, и «IMOEX у 2060,51» терял число.
check(C.count_numbers("Индекс теряет 2,4% и уходит к 2060. Моя цель 72₽") == 3,
      "уровень индекса 2060 считается показателем, а не годом")
check(C.count_numbers("IMOEX снижается на 2,4%, до 2 060,51") == 3,
      "уровень с пробелом-разделителем разрядов")
check(C.count_numbers("приостановка дивидендов до 2030 года") == 0,
      "2030 распознан как год")
check(C.count_numbers("15 июля в 10:24 акции упали на 8,5%") == 1,
      "дата и время не считаются показателями")
check(C.count_numbers("взлетело на 172% всего за 12 часов, вырос лишь на 42%") == 3,
      "три показателя в абзаце (это реальный Образец 2 из промпта Шага В)")

print("split_paras — маркеры обоих каналов и фоллбэк:")
check(len(C.split_paras("◽️раз\n◽️два")) == 2, "маркер Frame ◽️")
check(len(C.split_paras("🔽раз\n🔽два\n🔽три")) == 3, "маркер Thor 🔽")
check(len(C.split_paras("без маркеров\n\nвторой блок")) == 2,
      "фоллбэк по пустым строкам, когда маркеров нет вообще")

print("parse_export — формат выгрузки Telegram Desktop:")
export = {"name": "Frame", "type": "public_channel", "messages": [
    {"id": 1, "type": "service", "action": "create_channel"},
    {"id": 2, "type": "message", "date": "2026-05-14T10:24:00", "photo": "photos/1.jpg",
     "text": "Заголовок 📈\n\n◽️Первый абзац достаточной длины для фильтра.\n\n"
             "◽️Второй абзац, тоже содержательный.\n\n#открытыйинтерес"},
    {"id": 3, "type": "message", "date": "2026-05-20T11:00:00",
     "text": [{"type": "plain", "text": "Второй пост\n\n◽️Данные с платформы Frame "
                                          "показывают: стоит последить.\n\n"},
              {"type": "hashtag", "text": "#открытыйинтерес"}],
     "text_entities": [{"type": "plain", "text": "Второй пост\n\n◽️Данные с платформы Frame "
                                                   "показывают: стоит последить.\n\n"},
                       {"type": "hashtag", "text": "#открытыйинтерес"}]},
    {"id": 4, "type": "message", "date": "2026-06-01T12:00:00", "text": "👍"},
]}
with tempfile.TemporaryDirectory() as td:
    json.dump(export, open(os.path.join(td, "result.json"), "w"), ensure_ascii=False)
    posts = C.parse_export(td)

check(len(posts) == 2, "сервисное сообщение и короткое «👍» отброшены (осталось 2 из 4)")
check(posts[0]["has_photo"] is True, "фото распознано")
check(posts[0]["n_paras"] == 2, "абзацы по маркеру посчитаны")
check(posts[0]["hashtags"] == ["#открытыйинтерес"], "хэштеги вытащены")
# text как СПИСОК сущностей — вторая ловушка формата: в выгрузке поле text бывает
# и строкой, и списком из строк и объектов (ссылки, хэштеги, жирный).
check("платформы Frame" in posts[1]["text"], "text-как-список сущностей склеен в строку")
check(posts[1]["brand_mentions"] != [], "имя бренда в теле поймано")
check(posts[1]["stock_phrases"] == ["стоит последить"], "штамп концовки пойман")
check(posts[0]["stock_phrases"] == [], "у чистого поста штампов нет")

print()
if fails:
    print(f"ПРОВАЛОВ: {len(fails)}")
    sys.exit(1)
print("все тесты парсера корпуса прошли ✅")
