"""Живой сбор новостных постов через ВЕБ-ПРЕВЬЮ Telegram (t.me/s/<канал>).

⚠️ ЗАЧЕМ НЕ MTProto. С 11.08.2026 Telegram недоступен с прод-сервера, и проверка
31.08 показала, что блокировка ИЗБИРАТЕЛЬНАЯ: MTProto мёртв по всем проверенным
дата-центрам (149.154.167.51, .91, 91.108.56.130 — таймаут), а обычный HTTPS до
t.me и api.telegram.org проходит. Значит `tg_hype_scan.py` (Telethon) с сервера
работать не может в принципе, а этот путь — может.

⚠️ ЧТО ТЕРЯЕТСЯ. Веб-превью отдаёт ПРОСМОТРЫ, но НЕ репосты. Весь детектор хайпа
(`fwd_3`/`fwd_15`, порог ×3 к медиане репостов) калиброван именно на репостах —
подменить одно поле другим нельзя, порог придётся выводить заново на просмотрах.
Поэтому этот скрипт СЕЙЧАС наполняет только архив (news_archive) — макро-базу,
второй мозг. Ветка «завод постов» (создание content_candidates) не трогается:
конвейер выключен с 11.08, и включать его на непроверенной метрике нельзя.

⚠️ ГЛУБИНА. Одна страница отдаёт ~20 последних постов. Крон раз в 2-5 минут
покрывает поток обоих каналов с большим запасом (у markettwits ~120 постов/сутки).
Для добора истории есть `?before=<message_id>` — параметр --before.

Запуск (host-side, как anomaly_scan — нужен доступ к БД на 127.0.0.1):
  /opt/frame/signals/news_web_scan.sh
"""
import argparse
import html as _html
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser

import requests
from dotenv import load_dotenv
from sqlalchemy import text

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

import pipeline_heartbeat                  # noqa: E402
from api.database import SessionLocal      # noqa: E402
from signals import config                 # noqa: E402

WEB_ROOT = "https://t.me/s/{channel}"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
TICKER_RE = re.compile(r"^#([A-Z]{3,6})$")
HASHTAG_RE = re.compile(r"#[\wЀ-ӿ]+")

# Просмотры растут со временем, поэтому НЕ DO NOTHING: обновляем, но только вверх.
# Иначе повторный проход по странице, где Telegram ещё не пересчитал счётчик,
# затёр бы уже большее значение меньшим.
_UPSERT = text("""
    INSERT INTO news_archive
        (channel, message_id, posted_at, text, views, hashtags, entities, tickers, source)
    VALUES (:channel, :message_id, :posted_at, :text, :views, :hashtags,
            CAST(:entities AS jsonb), :tickers, 'tg_web')
    ON CONFLICT (channel, message_id) DO UPDATE
       SET views = GREATEST(COALESCE(news_archive.views, 0), EXCLUDED.views)
     WHERE EXCLUDED.views IS NOT NULL
""")


class _Preview(HTMLParser):
    """Разбор t.me/s/<канал>. Стандартная библиотека, а не bs4: в прод-venv
    (signals/.venv) bs4 нет, а тащить туда зависимость ради одной страницы
    несоразмерно. Разметка превью простая и стабильная.

    Устройство страницы: обёртка сообщения несёт data-post="канал/ид"; внутри —
    div.tgme_widget_message_text с текстом, <time datetime> со временем и
    span.tgme_widget_message_views со счётчиком просмотров."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.posts = []
        self._cur = None
        self._depth_text = 0
        self._in_views = False

    @staticmethod
    def _cls(attrs):
        return dict(attrs).get("class", "") or ""

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        cls = a.get("class", "") or ""
        if "tgme_widget_message" in cls and a.get("data-post"):
            self._cur = {"data_post": a["data-post"], "text": [], "views": None,
                          "dt": None, "links": []}
            self.posts.append(self._cur)
        if self._cur is None:
            return
        if "tgme_widget_message_text" in cls:
            self._depth_text = 1
        elif self._depth_text:
            self._depth_text += 1
        if tag == "a" and a.get("href") and self._depth_text:
            self._cur["links"].append(a["href"])
        if tag == "br" and self._depth_text:
            self._cur["text"].append("\n")
        if tag == "time" and a.get("datetime") and not self._cur.get("dt"):
            self._cur["dt"] = a["datetime"]
        if "tgme_widget_message_views" in cls:
            self._in_views = True

    def handle_endtag(self, tag):
        if self._depth_text:
            self._depth_text -= 1
        if self._in_views and tag == "span":
            self._in_views = False

    def handle_data(self, data):
        if self._cur is None:
            return
        if self._depth_text:
            self._cur["text"].append(data)
        elif self._in_views and self._cur["views"] is None:
            v = _parse_views(data.strip())
            if v is not None:
                self._cur["views"] = v


def _parse_views(s: str):
    """«1.2K» / «3,4M» / «812» → int. Telegram сокращает счётчик, и без разбора
    суффикса 1.2K превратилось бы в 1 — ошибка в три порядка."""
    if not s:
        return None
    s = s.replace(",", ".").strip()
    m = re.match(r"^([\d.]+)\s*([KMkm])?$", s)
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    mult = {"k": 1_000, "m": 1_000_000}.get((m.group(2) or "").lower(), 1)
    return int(val * mult)


# ⚠️ ОДНА СЕССИЯ НА ВСЕ КАНАЛЫ. t.me с прод-сервера отвечает не всегда: за 1152
# прогона 115 отказов у markettwits и 121 у newssmartlab — то есть примерно каждый
# десятый запрос не доводит до конца TCP-рукопожатие (ConnectTimeout, до чтения
# ответа дело не доходит). Канал и порядок ни при чём, оба страдают поровну.
# Keep-alive избавляет второй канал от повторного рукопожатия.
_СЕССИЯ = requests.Session()

ПОВТОРОВ = 3
ПАУЗЫ = (1.0, 3.0, 7.0)


def fetch(channel: str, before: int | None = None, timeout: int = 20,
          счётчик: dict | None = None) -> str:
    """Забрать веб-превью канала. Повторяет только СЕТЕВЫЕ сбои.

    ⚠️ ПОВТОР НЕ ПРЯЧЕТ ПРОБЛЕМУ, А ГАСИТ ШУМ. До этого попытка была одна, и
    десятипроцентное мигание сети превращалось в качели «сломан / восстановился» по
    пять раз за день — канал уведомлений от такого перестают читать. Число повторов
    возвращается наверх и попадает в заметку прогона: если t.me начнёт мигать вдвое
    чаще, это будет видно, а не растворится в тихих успехах.

    HTTP-ответ с кодом ошибки НЕ повторяем: 404 у переименованного канала или 429
    от самого t.me повтором не лечится, а 429 ещё и усугубится.
    """
    url = WEB_ROOT.format(channel=channel)
    params = {"before": before} if before else None
    последняя = None
    for попытка in range(ПОВТОРОВ):
        try:
            r = _СЕССИЯ.get(url, params=params, headers={"User-Agent": UA},
                            timeout=timeout)
            r.raise_for_status()
            return r.text
        except (requests.ConnectionError, requests.Timeout) as e:
            последняя = e
            if попытка + 1 < ПОВТОРОВ:
                пауза = ПАУЗЫ[попытка]
                if счётчик is not None:
                    счётчик["retries"] = счётчик.get("retries", 0) + 1
                print(f"[news_web_scan] {channel}: {type(e).__name__}, "
                      f"повтор {попытка + 1}/{ПОВТОРОВ - 1} через {пауза:.0f} с")
                time.sleep(пауза)
    raise последняя


def parse(page_html: str, channel_hint: str = "") -> list:
    p = _Preview()
    p.feed(page_html)
    out = []
    for raw in p.posts:
        chan, _, mid = raw["data_post"].partition("/")
        if not mid.isdigit():
            continue
        body = _html.unescape("".join(raw["text"])).strip()
        if len(body) < 15:
            continue
        dt = raw["dt"]
        if not dt:
            continue
        try:
            posted = datetime.fromisoformat(dt.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            continue
        tags = HASHTAG_RE.findall(body)
        out.append({
            "channel": chan or channel_hint,
            "message_id": int(mid),
            "posted_at": posted,
            "text": body,
            "views": raw["views"],
            "hashtags": tags,
            "entities": json.dumps({"links": raw["links"]}, ensure_ascii=False)
                        if raw["links"] else "{}",
            "tickers": sorted({m.group(1) for t in tags if (m := TICKER_RE.match(t.upper()))}),
        })
    return out


def run_once(channels=None, before=None) -> dict:
    channels = channels or config.TG_HYPE_CHANNELS
    summary = {"fetched": 0, "written": 0, "errors": 0, "retries": 0,
               "пустых": 0, "by_channel": {}}
    db = SessionLocal()
    try:
        for ch in channels:
            try:
                posts = parse(fetch(ch, before=before, счётчик=summary), channel_hint=ch)
            except Exception as e:
                summary["errors"] += 1
                print(f"[news_web_scan] {ch}: {type(e).__name__}: {e}")
                continue
            # ⚠️ ПУСТОЙ КАНАЛ — ЭТО СБОЙ, А НЕ ТИШИНА. t.me отдаёт 200 и обычную
            # страницу на НЕСУЩЕСТВУЮЩИЙ канал (9919 байт, ноль постов), поэтому
            # raise_for_status молчит. Переименуй кто-нибудь канал — и мы бы каждые
            # пять минут писали «успех, каналов 2» с нулём постов от одного из них.
            # У живого канала на превью всегда ~20 постов, ноль не бывает.
            if not posts:
                summary["пустых"] += 1
                print(f"[news_web_scan] {ch}: НОЛЬ ПОСТОВ — канал переименован, "
                      f"удалён или разметка превью изменилась")
                summary["by_channel"][ch] = 0
                continue

            summary["fetched"] += len(posts)
            for p in posts:
                db.execute(_UPSERT, p)
            db.commit()
            summary["written"] += len(posts)
            summary["by_channel"][ch] = len(posts)
            newest = max((p["posted_at"] for p in posts), default=None)
            print(f"[news_web_scan] {ch}: {len(posts)} постов, свежайший {newest}")
    except Exception as e:
        db.rollback()
        summary["errors"] += 1
        print(f"[news_web_scan] сбой: {type(e).__name__}: {e}")
    finally:
        db.close()
    return summary


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--channels", nargs="*")
    ap.add_argument("--before", type=int, help="добор истории: посты ДО этого message_id")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if a.dry_run:
        for ch in (a.channels or config.TG_HYPE_CHANNELS):
            posts = parse(fetch(ch, before=a.before), channel_hint=ch)
            print(f"{ch}: {len(posts)} постов")
            for p in posts[-3:]:
                print(f"  [{p['posted_at']}] id={p['message_id']} views={p['views']} "
                      f"tickers={p['tickers']} :: {p['text'][:90]!r}")
        return
    s = run_once(a.channels, a.before)
    print(f"[news_web_scan] итог: {s}")
    заметка = f"каналов {len(s['by_channel'])}, постов {s['written']}"
    if s.get("пустых"):
        заметка += f", ПУСТЫХ {s['пустых']}"
    # Повторы держим на виду: успех после трёх попыток — не то же самое, что успех
    # с первой, и молчать об этом значит потерять раннее предупреждение.
    if s.get("retries"):
        заметка += f", повторов {s['retries']}"
    pipeline_heartbeat.record_pipeline_run(
        "news_web_scan",
        success=s["errors"] == 0 and s["пустых"] == 0,
        note=заметка,
        degraded=(s["errors"] > 0 or s["пустых"] > 0) and s["written"] > 0,
    )


if __name__ == "__main__":
    main()
