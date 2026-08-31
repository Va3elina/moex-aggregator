#!/usr/bin/env python3
"""Тесты импортёра новостного архива.
Запуск: research/content_pipeline_v2/.venv/bin/python research/content_pipeline_v2/test_news_import.py

Каждый кейс — реальная ловушка, а не гипотетическая: экранирование под COPY ломается
молча (строка просто уезжает не в ту колонку), а часовой пояс расходится между маком
и прод-контейнером.
"""
import importlib.util
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("ni", os.path.join(HERE, "news_import.py"))
N = importlib.util.module_from_spec(spec)
spec.loader.exec_module(N)

fails = []


def check(cond, msg):
    print(("  ✓ " if cond else "  ✗ ") + msg)
    if not cond:
        fails.append(msg)


print("esc — экранирование под COPY FORMAT text:")
# Обратный слэш ПЕРВЫМ, иначе экранируем собственные экранирующие последовательности.
check(N.esc("a\\b") == "a\\\\b", "обратный слэш удвоен")
check(N.esc("a\tb") == "a\\tb", "табуляция не разорвёт строку на колонки")
check(N.esc("a\nb") == "a\\nb", "перенос строки не создаст лишнюю запись")
check(N.esc("a\\tb") == "a\\\\tb", "литерал '\\t' в тексте не превратится в табуляцию")

print("pg_array — литерал массива:")
check(N.pg_array([]) == "{}", "пустой массив")
check(N.pg_array(["#GAZP"]) == '{"#GAZP"}', "элемент в кавычках")
check(N.pg_array(["a,b"]) == '{"a,b"}', "запятая внутри элемента не разорвёт массив")
check(N.pg_array(['a"b']) == '{"a\\"b"}', "кавычка внутри элемента экранирована")

print("flatten — текст и ссылки:")
t, l = N.flatten({"text_entities": [{"type": "plain", "text": "раз "},
                                     {"type": "text_link", "text": "тут", "href": "https://x/y"}]})
check(t == "раз тут", "text_entities склеены")
check(l == ["https://x/y"], "href ссылки вытащен, а не её подпись")
t2, _ = N.flatten({"text": [{"type": "plain", "text": "из "}, "списка"]})
check(t2 == "из списка", "text-как-список из строк и словарей")

print("parse_export — формат и время:")
with tempfile.TemporaryDirectory() as td:
    json.dump({"name": "MarketTwits", "type": "public_channel", "messages": [
        {"id": 1, "type": "service", "action": "create_channel"},
        {"id": 10, "type": "message", "date": "2026-08-30T10:00:00", "text": "#GAZP новость достаточной длины"},
        {"id": 11, "type": "message", "date": "2026-08-30T11:00:00",
         "date_unixtime": "1756540800", "text": "#SBER другая новость достаточной длины"},
        {"id": 12, "type": "message", "date": "2026-08-30T12:00:00", "text": "👍"},
    ]}, open(os.path.join(td, "result.json"), "w"), ensure_ascii=False)
    rows = list(N.parse_export(td, tz_suffix="+03:00"))

check(len(rows) == 2, "сервисное сообщение и «👍» отброшены")
f10 = rows[0].split("\t")
check(f10[2] == "2026-08-30T10:00:00+03:00",
      "без date_unixtime — наивное время получает ЯВНОЕ смещение из --tz")
f11 = rows[1].split("\t")
check(f11[2].endswith("+00:00") and "T" in f11[2],
      "с date_unixtime — время приведено к UTC (однозначно на маке и на проде)")
check(f11[6] == '{"SBER"}', "тикер распознан из хэштега")
check(f10[7] == "tg_export", "источник помечен")

print("entities — JSON с переносом строки внутри ссылки (живой сбой на проде):")
with tempfile.TemporaryDirectory() as td:
    # Реальный случай: смартлаб, строка 13329 — href с переносом строки. Ручная
    # сборка JSON экранировала только кавычки и слэши, перенос доезжал внутрь
    # JSON-строки и COPY падал на «invalid input syntax for type json».
    json.dump({"name": "СМАРТЛАБ", "type": "public_channel", "messages": [
        {"id": 1, "type": "message", "date": "2026-08-30T10:00:00",
         "text_entities": [{"type": "plain", "text": "новость достаточной длины тут "},
                            {"type": "text_link", "text": "ссылка",
                             "href": "https://smartlab.news/i/36911\n"}]},
    ]}, open(os.path.join(td, "result.json"), "w"), ensure_ascii=False)
    row = list(N.parse_export(td))[0]
ents_field = row.split("\t")[5]
check("\\n" in ents_field or "\\u000a" in ents_field.lower(),
      "перенос строки внутри href экранирован, а не пролез в JSON сырым")
# То, что реально проверяет Postgres: после разэкранирования COPY это валидный JSON.
# ⚠️ Разэкранировать НАДО одним проходом слева направо. Наивная цепочка
# .replace(r"\t","\t").replace(r"\n","\n").replace(r"\\","\\") ломается на
# последовательности \\n: она сначала съест хвостовые \n и оставит висячий слэш.
# Это ровно та же ошибка порядка, от которой предостерегает docstring esc().
def copy_unescape(v: str) -> str:
    out, i = [], 0
    m = {"t": "\t", "n": "\n", "r": "\r", "\\": "\\"}
    while i < len(v):
        if v[i] == "\\" and i + 1 < len(v):
            out.append(m.get(v[i + 1], v[i + 1]))
            i += 2
        else:
            out.append(v[i])
            i += 1
    return "".join(out)

try:
    parsed = json.loads(copy_unescape(ents_field))
    check(parsed["links"][0].endswith("\n"), "после разэкранирования JSON валиден и ссылка цела")
except Exception as e:
    check(False, f"после разэкранирования JSON невалиден: {type(e).__name__}: {e}")

print("parse_export — защита от личной переписки:")
with tempfile.TemporaryDirectory() as td:
    json.dump({"name": "Тория", "type": "personal_chat",
               "messages": [{"id": 1, "type": "message", "date": "2026-01-01T00:00:00",
                             "text": "личная переписка не должна попасть в архив"}]},
              open(os.path.join(td, "result.json"), "w"), ensure_ascii=False)
    try:
        list(N.parse_export(td))
        check(False, "personal_chat отброшен с ошибкой")
    except ValueError as e:
        check("не канал" in str(e), "personal_chat отброшен с внятной ошибкой")

print()
if fails:
    print(f"ПРОВАЛОВ: {len(fails)}")
    sys.exit(1)
print("все тесты импортёра прошли ✅")
