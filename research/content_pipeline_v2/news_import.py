#!/usr/bin/env python3
"""Импорт архива новостных Telegram-каналов из выгрузки Telegram Desktop.

Схема: db/migrations/055_news_archive.sql. Зачем архив — MACRO_BRAIN.md.

⚠️ ПОТОКОВОЕ чтение (ijson), а не json.load. У markettwits на 31.08 message_id
дошёл до 383013, у newssmartlab — до 129768: выгрузка на сотни тысяч сообщений
даёт файл в сотни мегабайт, и json.load на нём съедает несколько гигабайт RAM.
ijson читает по одному сообщению, память постоянная.

⚠️ ВЫВОД — TSV под `COPY`, а не INSERT'ы. На 500 тыс. строк построчная вставка
через ORM идёт минутами и держит транзакцию; COPY загружает тот же объём за
секунды. Побочная выгода: скрипту не нужен драйвер БД вообще, значит он живёт в
research-venv без sqlalchemy/pg8000.

Использование:
  # 1. разобрать выгрузку в TSV (можно несколько каталогов сразу)
  .venv/bin/python news_import.py --export ~/Downloads/ChatExport_markettwits --out /tmp/news.tsv

  # 2. залить на прод (idempotent — повторный запуск ничего не сломает)
  cat /tmp/news.tsv | ssh root@103.88.243.232 "docker exec -i frame-db-1 psql -U postgres -d moex_db \\
     -c \\"COPY news_archive (channel,message_id,posted_at,text,hashtags,entities,tickers,source) \\
     FROM STDIN WITH (FORMAT text, NULL '\\\\N')\\""

  # предпросмотр без записи
  .venv/bin/python news_import.py --export DIR --dry-run
"""
import argparse
import datetime as _dt
import glob
import os
import re
import sys

TICKER_RE = re.compile(r"^#([A-Z]{3,6})$")
HASHTAG_RE = re.compile(r"#[\w\u0400-\u04FF]+")


def flatten(msg) -> tuple:
    """Текст + ссылки. Поле text бывает строкой или списком из строк и объектов;
    text_entities надёжнее — там всегда список словарей."""
    ents = msg.get("text_entities")
    links = []
    if isinstance(ents, list) and ents:
        parts = []
        for e in ents:
            parts.append(e.get("text", ""))
            if e.get("type") in ("link", "text_link"):
                links.append(e.get("href") or e.get("text") or "")
        return "".join(parts), links
    t = msg.get("text")
    if isinstance(t, str):
        return t, links
    if isinstance(t, list):
        out = []
        for p in t:
            if isinstance(p, str):
                out.append(p)
            else:
                out.append(p.get("text", ""))
                if p.get("type") in ("link", "text_link"):
                    links.append(p.get("href") or p.get("text") or "")
        return "".join(out), links
    return "", links


def esc(s: str) -> str:
    """Экранирование для COPY FORMAT text. Порядок значим: обратный слэш ПЕРВЫМ,
    иначе экранируем собственные экранирующие последовательности."""
    return (s.replace("\\", "\\\\").replace("\t", "\\t")
             .replace("\n", "\\n").replace("\r", "\\r"))


def pg_array(items) -> str:
    """Литерал массива Postgres. Каждый элемент в кавычках — иначе запятая или
    пробел внутри значения разорвёт массив."""
    if not items:
        return "{}"
    inner = ",".join('"' + i.replace("\\", "\\\\").replace('"', '\\"') + '"' for i in items)
    return "{" + inner + "}"


def parse_export(path: str, min_len: int = 15, tz_suffix: str = "+03:00"):
    """Отдаёт готовые строки TSV. min_len отсекает «👍» и репост-заглушки, но
    порог НИЖЕ, чем у корпуса постов (40): новость «ЦБ поднял ставку до 14%»
    короткая и при этом ценнее многих длинных."""
    import ijson
    jf = path if path.endswith(".json") else os.path.join(path, "result.json")
    if not os.path.exists(jf):
        raise FileNotFoundError(f"нет {jf}")

    # Имя и тип канала лежат в начале файла — читаем их отдельным быстрым проходом,
    # не загружая messages.
    channel, ctype = None, None
    with open(jf, "rb") as f:
        for prefix, event, value in ijson.parse(f):
            if prefix == "name" and event == "string":
                channel = value
            elif prefix == "type" and event == "string":
                ctype = value
            if prefix == "messages":
                break
    if not channel:
        channel = os.path.basename(os.path.dirname(jf))
    if ctype and "channel" not in ctype:
        raise ValueError(f"это не канал, а {ctype!r} («{channel}») — пропущено. "
                          f"Нужен экспорт КАНАЛА, не личного чата.")

    kept = skipped = 0
    with open(jf, "rb") as f:
        for msg in ijson.items(f, "messages.item"):
            if msg.get("type") != "message":
                skipped += 1
                continue
            text, links = flatten(msg)
            text = text.strip()
            if len(text) < min_len:
                skipped += 1
                continue
            mid = msg.get("id")
            # ⚠️ ВРЕМЯ. Поле `date` в выгрузке — НАИВНАЯ строка в часовом поясе той
            # машины, где делали экспорт. Загруженная локально (мак в Берлине) и на
            # проде (контейнер в UTC) она даёт РАЗНОЕ абсолютное время — а весь смысл
            # архива в том, «что было верно на дату». Поэтому предпочитаем
            # `date_unixtime`: он однозначен. Наивную строку берём только как
            # запасной вариант и помечаем явным смещением из --tz.
            unix = msg.get("date_unixtime")
            if unix:
                # В ISO с Z, а не «epoch:N»: COPY кладёт значение в timestamptz как
                # есть, преобразовывать на стороне БД нечем.
                date = _dt.datetime.fromtimestamp(int(unix), _dt.timezone.utc)\
                          .strftime("%Y-%m-%dT%H:%M:%S+00:00")
                is_unix = True
            else:
                date = msg.get("date") or ""
                is_unix = False
            if mid is None or not date:
                skipped += 1
                continue
            tags = HASHTAG_RE.findall(text)
            tickers = sorted({m.group(1) for t in tags if (m := TICKER_RE.match(t.upper()))})
            ents = "{}" if not links else \
                '{"links": [' + ",".join('"' + l.replace('\\', '\\\\').replace('"', '\\"') + '"'
                                          for l in links) + "]}"
            kept += 1
            stamp = date if is_unix else f"{date}{tz_suffix}"
            yield "\t".join([
                esc(channel), str(mid), stamp, esc(text),
                pg_array(tags), esc(ents), pg_array(tickers), "tg_export",
            ])
    print(f"  {os.path.basename(path)}: канал «{channel}», взято {kept}, "
          f"пропущено {skipped}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--export", nargs="+", required=True,
                    help="каталоги выгрузок или пути к result.json")
    ap.add_argument("--out", help="файл TSV; без него — в stdout")
    ap.add_argument("--dry-run", action="store_true", help="только посчитать и показать примеры")
    ap.add_argument("--min-len", type=int, default=15)
    ap.add_argument("--tz", default="+03:00",
                    help="смещение для сообщений БЕЗ date_unixtime (наивная строка "
                          "в поясе машины-экспортёра). По умолчанию Москва +03:00 — "
                          "каналы российские, и Telegram Desktop у Вадима, скорее всего, "
                          "экспортирует в московском времени. Проверь на первых строках.")
    a = ap.parse_args()

    paths = []
    for p in a.export:
        p = os.path.expanduser(p)
        paths.extend(sorted(glob.glob(p)) if any(c in p for c in "*?[") else [p])

    total = 0
    sink = open(a.out, "w", encoding="utf-8") if a.out else sys.stdout
    try:
        for p in paths:
            try:
                for i, row in enumerate(parse_export(p, a.min_len, a.tz)):
                    total += 1
                    if a.dry_run:
                        if i < 3:
                            print("  пример: " + row[:220], file=sys.stderr)
                        continue
                    sink.write(row + "\n")
            except Exception as e:
                print(f"  {p}: не разобрано — {type(e).__name__}: {e}", file=sys.stderr)
    finally:
        if a.out:
            sink.close()
    where = a.out if a.out else "stdout"
    print(f"\nвсего строк: {total}" + ("" if a.dry_run else f" → {where}"), file=sys.stderr)


if __name__ == "__main__":
    main()
