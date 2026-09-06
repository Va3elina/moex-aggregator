"""
Документы компаний: ссылка из company_documents (CDN FinanceMarker) → файл на диске →
версия по хэшу → текст и таблицы постранично (document_versions / document_pages).

⚠️ ХОСТ, НЕ КОНТЕЙНЕР. pypdf и pdfplumber стоят в signals/.venv (06.09.2026); в образе api/
оркестратора их нет, а файлы лежат на диске сервера (/opt/frame/docs). Зовётся кроном раз
в сутки (--since-days) и сканером раскрытия сразу после обновления карточки (--secid).

⚠️ ИМЯ ФАЙЛА У FM НЕСЁТ СМЫСЛ: sber_2026_6_6m_msfo.pdf / SBER_2025_3_Q_МСФО.pdf /
…_press.pdf — год, месяц, код периода (q / 6m / 9m / y), стандарт, презентация.
В company_documents этого нет (там только year), поэтому разбираем имя здесь.

⚠️ СКАНЫ. У части форм РСБУ текста нет вовсе — text_chars = 0; такие документы ИИ не
читает, цифры по ним берутся из карточки FM. Это не ошибка извлечения, это свойство файла.
"""

import argparse
import base64
import hashlib
import json
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from sqlalchemy import text

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

import pipeline_heartbeat                  # noqa: E402
from api.database import SessionLocal      # noqa: E402

КАТАЛОГ = os.environ.get("DOCS_DIR", "/opt/frame/docs")
UA = "Mozilla/5.0 (compatible; FrameBot/1.0; +https://framedata.ru)"
ТАЙМАУТ = (10, 120)
МАКС_СТРАНИЦ_ТАБЛИЦ = 150     # pdfplumber ≈ 0.16 с/стр; годовой МСФО на 300 стр — 50 с, дальше не ждём
ПАУЗА = 0.5

_ИМЯ = re.compile(r"^(?P<code>[A-Za-z0-9]+)_(?P<year>\d{4})_(?P<month>\d{1,2})_(?P<per>[0-9A-Za-z]+)_(?P<std>[^_.]+?)(?P<press>_press)?\.pdf$", re.I)
_СТАНДАРТ = {"msfo": "МСФО", "мсфо": "МСФО", "rsbu": "РСБУ", "рсбу": "РСБУ"}

_SELECT_QUEUE = text("""
    SELECT d.issuer_id, d.doc_type, d.period, d.url, s.secid
      FROM company_documents d
      JOIN issuer_securities s ON s.issuer_id = d.issuer_id AND s.share_class = 'common'
     WHERE d.url LIKE '%financemarker.ru/cdn/%'
       AND (CAST(:secid AS text) IS NULL OR s.secid = CAST(:secid AS text))
       AND (CAST(:since AS timestamptz) IS NULL OR d.created_at >= CAST(:since AS timestamptz))
       AND (CAST(:period_from AS text) IS NULL OR d.period >= CAST(:period_from AS text))
       AND NOT EXISTS (SELECT 1 FROM document_versions v WHERE v.url = d.url AND v.superseded_at IS NULL AND v.fetched_at > now() - interval '1 day')
     ORDER BY d.period DESC, s.secid, d.doc_type
     LIMIT :limit
""")
_SELECT_LIVE = text("SELECT id, sha256 FROM document_versions WHERE url = :url AND superseded_at IS NULL")
_SUPERSEDE = text("UPDATE document_versions SET superseded_at = now() WHERE url = :url AND superseded_at IS NULL")
_INSERT_VERSION = text("""
    INSERT INTO document_versions (url, issuer_id, secid, doc_type, standard, period_code, year, month, file_name, file_path,
                                   sha256, bytes, pages, text_chars, tables, note)
    VALUES (:url, :issuer_id, :secid, :doc_type, :standard, :period_code, :year, :month, :file_name, :file_path,
            :sha256, :bytes, :pages, :text_chars, :tables, :note)
    RETURNING id
""")
_INSERT_PAGE = text("INSERT INTO document_pages (version_id, page, text, tables) VALUES (:v, :p, :t, CAST(:tb AS jsonb))")
_MARK_PARSED = text("UPDATE company_documents SET parsed = TRUE WHERE url = :url")


def имя_из_ссылки(url: str) -> str:
    """file_id в CDN-ссылке — base64 пути /reports/2026/MOEX/S/sber_2026_6_6m_msfo.pdf."""
    q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get("file_id", [""])[0]
    try:
        путь = base64.urlsafe_b64decode(q + "=" * (-len(q) % 4)).decode("utf-8", "replace")
        return urllib.parse.unquote(os.path.basename(путь)) or "document.pdf"
    except Exception:  # noqa: BLE001
        return "document.pdf"


def разобрать_имя(имя: str) -> dict:
    m = _ИМЯ.match(имя)
    if not m:
        return {}
    per = m.group("per").lower()
    return {"year": int(m.group("year")), "month": int(m.group("month")),
            "period_code": {"q": "q", "6m": "6m", "9m": "9m", "y": "y", "12": "y"}.get(per, per[:8]),
            "standard": _СТАНДАРТ.get(m.group("std").lower()), "press": bool(m.group("press"))}


def скачать(url: str, путь: str) -> tuple[bytes, str]:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=ТАЙМАУТ)
    r.raise_for_status()
    данные = r.content
    if not данные.startswith(b"%PDF"):
        raise ValueError(f"не PDF ({r.headers.get('content-type')}, {len(данные)} байт)")
    return данные, hashlib.sha256(данные).hexdigest()


def извлечь(путь: str) -> tuple[list[dict], int, int]:
    """[{page, text, tables}] + сумма знаков + число таблиц."""
    import pypdf
    import pdfplumber
    страницы = []
    reader = pypdf.PdfReader(путь)
    for i, p in enumerate(reader.pages, 1):
        try:
            t = p.extract_text() or ""
        except Exception:  # noqa: BLE001 — битая страница не роняет документ
            t = ""
        страницы.append({"page": i, "text": t.strip(), "tables": None})
    таблиц = 0
    try:
        with pdfplumber.open(путь) as pdf:
            for i, p in enumerate(pdf.pages[:МАКС_СТРАНИЦ_ТАБЛИЦ], 1):
                try:
                    tb = p.extract_tables()
                except Exception:  # noqa: BLE001
                    tb = []
                tb = [t for t in tb if t and len(t) >= 2 and any(any(c for c in row) for row in t)]
                if tb:
                    страницы[i - 1]["tables"] = tb
                    таблиц += len(tb)
    except Exception as e:  # noqa: BLE001 — таблицы вторичны: текст уже есть
        print(f"[fetch_documents] {путь}: таблицы не извлечены: {type(e).__name__}: {e}")
    return страницы, sum(len(s["text"]) for s in страницы), таблиц


def run_once(secid, since_days, period_from, limit, dry_run) -> dict:
    итог = {"в_очереди": 0, "скачано": 0, "новых_версий": 0, "без_изменений": 0, "страниц": 0, "сканов": 0, "ошибок": 0}
    since = (datetime.now(timezone.utc).replace(hour=0, minute=0) - __import__("datetime").timedelta(days=since_days)) if since_days else None
    db = SessionLocal()
    try:
        очередь = db.execute(_SELECT_QUEUE, {"secid": secid, "since": since, "period_from": period_from, "limit": limit}).mappings().all()
        итог["в_очереди"] = len(очередь)
        for d in очередь:
            имя = имя_из_ссылки(d["url"])
            разбор = разобрать_имя(имя)
            if dry_run:
                print(f"  {d['secid']:<6} {d['doc_type']:<17} {d['period']:<5} {имя} → {разбор or 'имя не разобрано'}")
                continue
            папка = os.path.join(КАТАЛОГ, d["secid"])
            os.makedirs(папка, exist_ok=True)
            try:
                данные, sha = скачать(d["url"], папка)
            except Exception as e:  # noqa: BLE001
                итог["ошибок"] += 1
                print(f"[fetch_documents] {d['secid']} {имя}: {type(e).__name__}: {e}")
                time.sleep(ПАУЗА)
                continue
            итог["скачано"] += 1
            живая = db.execute(_SELECT_LIVE, {"url": d["url"]}).first()
            if живая and живая[1] == sha:
                итог["без_изменений"] += 1
                db.execute(_MARK_PARSED, {"url": d["url"]})
                db.commit()
                continue
            путь = os.path.join(папка, f"{sha[:8]}_{имя}")
            with open(путь, "wb") as f:
                f.write(данные)
            страницы, знаков, таблиц = извлечь(путь)
            if живая:
                db.execute(_SUPERSEDE, {"url": d["url"]})
            vid = db.execute(_INSERT_VERSION, {
                "url": d["url"], "issuer_id": d["issuer_id"], "secid": d["secid"],
                "doc_type": "presentation" if разбор.get("press") else d["doc_type"],
                "standard": разбор.get("standard"), "period_code": разбор.get("period_code"),
                "year": разбор.get("year") or (int(d["period"]) if str(d["period"]).isdigit() else None),
                "month": разбор.get("month"), "file_name": имя, "file_path": путь,
                "sha256": sha, "bytes": len(данные), "pages": len(страницы), "text_chars": знаков, "tables": таблиц,
                "note": "скан: текста нет" if знаков < 200 * max(1, len(страницы)) // 10 else None,
            }).scalar()
            for s in страницы:
                db.execute(_INSERT_PAGE, {"v": vid, "p": s["page"], "t": s["text"] or None,
                                          "tb": json.dumps(s["tables"], ensure_ascii=False) if s["tables"] else None})
            db.execute(_MARK_PARSED, {"url": d["url"]})
            db.commit()
            итог["новых_версий"] += 1
            итог["страниц"] += len(страницы)
            if знаков < 200:
                итог["сканов"] += 1
            print(f"[fetch_documents] {d['secid']} {имя}: версия #{vid}, {len(страницы)} стр, {знаков} зн, таблиц {таблиц}"
                  + (" (заменила прежнюю)" if живая else ""))
            time.sleep(ПАУЗА)
    except Exception as e:  # noqa: BLE001
        db.rollback()
        итог["ошибок"] += 1
        print(f"[fetch_documents] сбой: {type(e).__name__}: {e}")
    finally:
        db.close()
    return итог


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--secid", help="только эта компания (вызов из сканера раскрытия)")
    ap.add_argument("--since-days", type=int, help="документы, появившиеся в company_documents за N дней")
    ap.add_argument("--period-from", help="отчёты с этого года, напр. 2025")
    ap.add_argument("--limit", type=int, default=60)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    t0 = time.time()
    итог = run_once(a.secid, a.since_days, a.period_from, a.limit, a.dry_run)
    print(f"[fetch_documents] итог: {итог} за {time.time() - t0:.0f} с")
    if not a.dry_run:
        pipeline_heartbeat.record_pipeline_run(
            "documents_fetch", success=итог["ошибок"] == 0 or итог["новых_версий"] > 0,
            note=f"очередь {итог['в_очереди']}, новых версий {итог['новых_версий']}, без изменений {итог['без_изменений']}, ошибок {итог['ошибок']}",
            duration_sec=time.time() - t0, degraded=итог["ошибок"] > 0 and итог["новых_версий"] > 0)


if __name__ == "__main__":
    main()
