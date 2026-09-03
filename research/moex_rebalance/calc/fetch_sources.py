#!/usr/bin/env python
"""Скачать ВСЕ первоисточники по ребалансу индексов МосБиржи в sources/.

Зачем скрипт, а не «скачал руками»: песочница чистится, ссылки на документы
МосБиржи не угадываются (методика лежит под /ru/documents/3344, а сами файлы —
под непрозрачными id вида 41pt4c5kay64t3ek2thf66kjdq), и на поиск уходит больше
времени, чем на весь расчёт. Запуск идемпотентный: уже скачанное не перекачивает,
если не передать --force.

⚠️ TLS. fs.moex.com отдаёт цепочку Минцифры, которой нет в certifi, поэтому
python падает с CERTIFICATE_VERIFY_FAILED там, где браузер открывает файл молча.
Бандл собирается здесь же из certifi + certs/russian_trusted_{root,sub}_ca.pem
(см. память ru_tls_mincifry).

Запуск:  .venv/bin/python research/moex_rebalance/calc/fetch_sources.py [--force]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import ssl
import sys
import tempfile
import urllib.request
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[3]
SRC = pathlib.Path(__file__).resolve().parents[1] / "sources"
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}

# Что качаем. Ключ — имя файла в sources/, значение — (url, описание).
# id методик сняты со страницы moex.com/ru/documents/3344 2026-09-02.
#
# ⚠️ ГРАБЛИ, на которые я уже наступил. На странице ДВЕНАДЦАТЬ редакций методики
# под непрозрачными id, и по имени файла не видно, какая из них какая. 01.09 я взял
# id «41pt4c5kay64…», решив что это новая редакция, — а это редакция от 18.01.2021,
# то есть пятилетней давности. Правильное сопоставление читается только из порядка
# блоков на самой странице (Новая редакция → Действующая → предыдущие), и он
# воспроизводится так:
#   re.finditer(r'href="(/files/[0-9a-z]{20,})"|Утверждена[^<]{0,90}', html)
# Поэтому: НИКОГДА не подставляй id методики на глаз — сверь протокол и дату
# в шапке скачанного файла (первые 200 знаков .txt) с тем, что обещала страница.
SOURCES: dict[str, tuple[str, str]] = {
    "press_release_n103733.html": (
        "https://www.moex.com/n103733",
        "Пресс-релиз «Об изменении баз расчета индексов акций» от 28.08.2026"),
    "lw_table_18.09.2026.pdf": (
        "https://fs.moex.com/f/23954/ssylka-dlja-reliza-baz-dannyh-28-avg-2026.pdf",
        "Приложение к релизу: таблица дополнительных весовых коэффициентов LW на 18.09.2026"),
    "methodology_from_18.09.2026.docx": (
        "https://www.moex.com/files/4cwygv8qxdjwy5xhd57r5pw8mp",
        "Методика индексов акций — НОВАЯ редакция, утв. 13.08.2026 (Протокол № 57), "
        "вступает в силу 18.09.2026"),
    "methodology_current.docx": (
        "https://www.moex.com/files/41g1eqct6het86zjhsa7h5ex4d",
        "Методика индексов акций — ДЕЙСТВУЮЩАЯ редакция, утв. 04.04.2025 (Протокол № 28)"),
    "methodology_2021_obsolete.docx": (
        "https://www.moex.com/files/41pt4c5kay64t3ek2thf66kjdq",
        "Методика индексов акций — редакция от 18.01.2021 (Протокол № 2). УСТАРЕЛА, "
        "лежит здесь только как памятник ошибке 01.09: её приняли за новую редакцию"),
    "freefloat_methodology.docx": (
        "https://www.moex.com/files/400dcxg7kvb0q2tajp9n87vr50",
        "Методика определения Коэффициента free-float (отдельный документ)"),
    "doc3344_indices.html": (
        "https://www.moex.com/ru/documents/3344",
        "Страница-указатель редакций методики индексов акций"),
    "doc4540_freefloat.html": (
        "https://www.moex.com/ru/documents/4540",
        "Страница-указатель редакций методики free-float"),
}


def ru_ssl_context() -> ssl.SSLContext:
    """certifi + корень и Sub CA Минцифры одним временным бандлом."""
    import certifi
    bundle = tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False)
    bundle.write(pathlib.Path(certifi.where()).read_text())
    for name in ("russian_trusted_root_ca.pem", "russian_trusted_sub_ca.pem"):
        p = ROOT / "certs" / name
        if p.exists():
            bundle.write("\n" + p.read_text())
    bundle.close()
    return ssl.create_default_context(cafile=bundle.name)


def docx_to_text(path: pathlib.Path) -> str:
    """Текст из .docx без внешних зависимостей: document.xml + сноски."""
    import re
    out = []
    with zipfile.ZipFile(path) as z:
        for part in ("word/document.xml", "word/footnotes.xml", "word/endnotes.xml"):
            if part not in z.namelist():
                continue
            xml = z.read(part).decode("utf-8", "replace")
            xml = re.sub(r"</w:p>", "\n", xml)
            xml = re.sub(r"<w:tab[^>]*/>", "\t", xml)
            out.append(re.sub(r"<[^>]+>", "", xml))
    import html as _html
    return _html.unescape("\n".join(out))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="перекачать уже скачанное")
    args = ap.parse_args()

    SRC.mkdir(parents=True, exist_ok=True)
    ctx = ru_ssl_context()
    manifest = {}
    for name, (url, note) in SOURCES.items():
        path = SRC / name
        if path.exists() and not args.force:
            raw = path.read_bytes()
            status = "уже есть"
        else:
            try:
                raw = urllib.request.urlopen(
                    urllib.request.Request(url, headers=UA), timeout=120, context=ctx).read()
            except Exception as e:                       # noqa: BLE001
                print(f"  ✗ {name}: {type(e).__name__} {e}")
                continue
            path.write_bytes(raw)
            status = "скачан"
        manifest[name] = {"url": url, "описание": note, "байт": len(raw),
                          "sha256": hashlib.sha256(raw).hexdigest()[:16]}
        print(f"  {status:9} {name:36} {len(raw):>9,} байт")
        # у .docx рядом кладём .txt — чтобы grep-ать формулировки без распаковки
        if name.endswith(".docx"):
            txt = path.with_suffix(".txt")
            if not txt.exists() or args.force:
                txt.write_text(docx_to_text(path), encoding="utf-8")
                print(f"  {'текст':9} {txt.name:36} {txt.stat().st_size:>9,} байт")

    (SRC / "MANIFEST.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nманифест: {SRC / 'MANIFEST.json'} ({len(manifest)} источников)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
