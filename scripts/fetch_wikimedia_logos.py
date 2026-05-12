#!/usr/bin/env python3
"""
fetch_wikimedia_logos.py — скачивает SVG лого с Wikimedia Commons.

Подход:
  1. Search по имени компании в namespace File (ns=6).
  2. Берём первый .svg (или .png если svg нет).
  3. Через imageinfo API получаем прямой URL файла.
  4. Скачиваем → frontend/public/logos/<TICKER>.svg.

Wikipedia не блокирует автоматизированные запросы (User-Agent
достаточно). Лого там качественные, векторные.

Не перезаписывает существующие файлы (чтобы не затереть то что
уже скачано из apple-touch-icon).
"""
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

# Кандидаты "company name" для поиска в Wikimedia Commons.
# Первое совпадение (с расширением svg) побеждает.
TICKER_SEARCH = {
    'SBER':  'Sberbank logo',
    'SBERP': 'Sberbank logo',
    'GAZP':  'Gazprom logo',
    'LKOH':  'Lukoil logo',
    'ROSN':  'Rosneft logo',
    'NVTK':  'Novatek logo',
    'GMKN':  'Norilsk Nickel logo',
    'TATN':  'Tatneft logo',
    'TATNP': 'Tatneft logo',
    'PLZL':  'Polyus logo',
    'YDEX':  'Yandex logo',
    'T':     'T-Bank logo',
    'SIBN':  'Gazprom Neft logo',
    'MTSS':  'MTS logo 2023',
    'MGNT':  'Magnit logo',
    'AFLT':  'Aeroflot logo',
    'MOEX':  'Moscow Exchange logo',
    'MAGN':  'MMK logo',
    'CHMF':  'Severstal logo',
    'NLMK':  'NLMK logo',
    'PIKK':  'PIK Group logo',
    'FEES':  'Rosseti logo',
    'AFKS':  'Sistema PJSFC logo',
    'VTBR':  'VTB logo',
    'RUAL':  'Rusal logo',
    'IRAO':  'Inter RAO logo',
    'ALRS':  'Alrosa logo',
    'RTKM':  'Rostelecom logo',
    'HYDR':  'RusHydro logo',
    'PHOR':  'PhosAgro logo',
    'BSPB':  'Bank Saint Petersburg logo',
    'BANE':  'Bashneft logo',
    'TRNFP': 'Transneft logo',
    'UPRO':  'Unipro logo',
    'ENPG':  'En+ Group logo',
    'VSMO':  'VSMPO-AVISMA logo',
    'KMAZ':  'Kamaz logo',
    'POSI':  'Positive Technologies logo',
    'VKCO':  'VK Company logo',
    'OZON':  'Ozon logo',
    'X5':    'X5 Retail Group logo',
    'LENT':  'Lenta company logo',
    'HEAD':  'HeadHunter logo',
    'IRKT':  'Yakovlev logo',
    'FLOT':  'Sovcomflot logo',
    'MSNG':  'Mosenergo logo',
}

LOGOS_DIR = Path(__file__).resolve().parent.parent / 'frontend' / 'public' / 'logos'
LOGOS_DIR.mkdir(parents=True, exist_ok=True)

UA = 'FrameLogoFetch/1.0 (research; contact: support@frame.example)'


def wiki_search(query: str) -> list[str]:
    """Ищет файлы в Commons. Возвращает список имен (без префикса 'File:')."""
    url = (
        'https://commons.wikimedia.org/w/api.php'
        f'?action=query&list=search&srsearch={urllib.parse.quote(query)}'
        '&srnamespace=6&srlimit=10&format=json'
    )
    try:
        req = urllib.request.Request(url, headers={'User-Agent': UA})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.load(resp)
        return [item['title'].removeprefix('File:') for item in data['query']['search']]
    except Exception as e:
        print(f'  search error: {e}')
        return []


def wiki_file_url(filename: str) -> str | None:
    """Получает прямой URL к файлу через imageinfo API."""
    url = (
        'https://commons.wikimedia.org/w/api.php'
        f'?action=query&titles=File:{urllib.parse.quote(filename)}'
        '&prop=imageinfo&iiprop=url&format=json'
    )
    try:
        req = urllib.request.Request(url, headers={'User-Agent': UA})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.load(resp)
        pages = data['query']['pages']
        for page in pages.values():
            if 'imageinfo' in page:
                return page['imageinfo'][0]['url']
    except Exception as e:
        print(f'  imageinfo error: {e}')
    return None


def already_exists(ticker: str) -> bool:
    for ext in ('svg', 'png', 'jpg', 'webp'):
        if (LOGOS_DIR / f'{ticker}.{ext}').exists():
            return True
    return False


def download(url: str, path: Path) -> int:
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = resp.read()
    path.write_bytes(data)
    return len(data)


def pick_best_filename(results: list[str]) -> str | None:
    """Из результатов поиска выбирает лучшее имя файла (svg > png > _else_)."""
    # Фильтруем очевидно нерелевантное — логотипы истории / мелкие файлы
    blocked_keywords = ['old', 'outline', 'small', '1px', 'icon_16', 'icon_32']
    candidates = [
        f for f in results
        if not any(k in f.lower() for k in blocked_keywords)
    ]
    svg = [f for f in candidates if f.lower().endswith('.svg')]
    if svg:
        return svg[0]
    png = [f for f in candidates if f.lower().endswith('.png')]
    if png:
        return png[0]
    return None


def fetch_ticker(ticker: str, query: str) -> str:
    if already_exists(ticker):
        # Если SVG уже есть — пропускаем. Если PNG из предыдущего скрипта —
        # перезаписываем потенциально более качественным SVG.
        if (LOGOS_DIR / f'{ticker}.svg').exists():
            return 'skip (svg exists)'

    results = wiki_search(query)
    if not results:
        return 'not found'

    filename = pick_best_filename(results)
    if not filename:
        return f'no svg/png in {len(results)} results'

    url = wiki_file_url(filename)
    if not url:
        return f'no url for {filename}'

    # Определяем расширение из filename
    ext = filename.lower().rsplit('.', 1)[-1]
    if ext not in ('svg', 'png', 'jpg', 'webp'):
        ext = 'svg'

    path = LOGOS_DIR / f'{ticker}.{ext}'
    # Если PNG уже есть и мы скачиваем SVG — удаляем PNG (SVG лучше)
    if ext == 'svg':
        for old_ext in ('png', 'jpg', 'webp'):
            old = LOGOS_DIR / f'{ticker}.{old_ext}'
            if old.exists():
                old.unlink()

    try:
        size = download(url, path)
        return f'ok[{filename[:40]}] ({size} B)'
    except Exception as e:
        return f'download err: {e}'


def main():
    ok, skip, fail = 0, 0, 0
    for ticker in sorted(TICKER_SEARCH):
        query = TICKER_SEARCH[ticker]
        status = fetch_ticker(ticker, query)
        if status.startswith('ok'):
            ok += 1
        elif status.startswith('skip'):
            skip += 1
        else:
            fail += 1
        print(f'{ticker:6} [{query:30}] → {status}', flush=True)
        time.sleep(0.4)  # не спамим Wikipedia

    print()
    print(f'Итого: ok={ok}  skip={skip}  fail={fail}')


if __name__ == '__main__':
    main()
