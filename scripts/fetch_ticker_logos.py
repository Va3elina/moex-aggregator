#!/usr/bin/env python3
"""
fetch_ticker_logos.py — массово скачивает лого акций MOEX.

Стратегия (по убыванию качества):
  1. `https://{domain}/apple-touch-icon.png` — 180×180 PNG (iOS Safari).
     Большинство корпоративных сайтов публикуют их.
  2. `https://{domain}/apple-touch-icon-180x180.png` — вариант с суффиксом.
  3. `https://{domain}/apple-touch-icon-152x152.png`
  4. `https://{domain}/favicon-192x192.png` — Android.
  5. Google Favicon sz=256 (fallback, качество непредсказуемо).

Лого кладутся в frontend/public/logos/<TICKER>.png.

Уже существующие SVG/PNG НЕ перезаписываются (чтобы ручные upgrade
top-компаний не терялись).

Запуск:
    python3 scripts/fetch_ticker_logos.py
"""
import io
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

# ─── Mapping тикер → домен ─────────────────────────────────────────
# Составлено вручную на основе знаний о российских эмитентах MOEX.
# '' (пустая строка) — компания без известного домена / очень мелкая,
# fallback circle остаётся.
TICKER_DOMAINS = {
    # Топ крупные
    'SBER':   'sberbank.ru',
    'SBERP':  'sberbank.ru',
    'GAZP':   'gazprom.ru',
    'LKOH':   'lukoil.ru',
    'ROSN':   'rosneft.ru',
    'NVTK':   'novatek.ru',
    'GMKN':   'nornickel.com',
    'TATN':   'tatneft.ru',
    'TATNP':  'tatneft.ru',
    'PLZL':   'polyus.com',
    'YDEX':   'ya.ru',
    'T':      'tbank.ru',
    'SIBN':   'gazprom-neft.ru',
    'MTSS':   'mts.ru',
    'MGNT':   'magnit.com',
    'AFLT':   'aeroflot.ru',
    'MOEX':   'moex.com',
    'MAGN':   'mmk.ru',
    'CHMF':   'severstal.com',
    'NLMK':   'nlmk.com',
    'PIKK':   'pik.ru',
    'FEES':   'rosseti.ru',
    'AFKS':   'sistema.ru',
    'VTBR':   'vtb.ru',
    'RUAL':   'rusal.ru',
    'IRAO':   'interrao.ru',
    'ALRS':   'alrosa.ru',
    'RTKM':   'rt.ru',
    'HYDR':   'rushydro.ru',
    'PHOR':   'phosagro.ru',
    'BSPB':   'bspb.ru',
    # Средние
    'AKRN':   'acron.ru',
    'ASTR':   'astragroup.ru',
    'BANE':   'bashneft.ru',
    'BELU':   'novabev.com',
    'CBOM':   'mkb.ru',
    'CNRU':   'cian.ru',
    'DOMRF':  'domrf.ru',
    'ELMT':   'element.ru',
    'ENPG':   'enplusgroup.com',
    'FESH':   'fesco.ru',
    'FIXR':   'fix-price.com',
    'FLOT':   'sovcomflot.ru',
    'GCHE':   'cherkizovo.com',
    'GEMC':   'emcmos.ru',
    'HEAD':   'hh.ru',
    'IRKT':   'yak.ru',
    'KAZT':   'kuazot.ru',
    'KMAZ':   'kamaz.ru',
    'KZOS':   'kazanorgsintez.ru',
    'LEAS':   'europlan.ru',
    'LENT':   'lenta.com',
    'LSRG':   'lsrgroup.ru',
    'MBNK':   'mtsbank.ru',
    'MDMG':   'mdmg.com',
    'MGTS':   'mgts.ru',
    'MSNG':   'mosenergo.ru',
    'MSRS':   'rossetimr.ru',
    'NKNC':   'nknh.ru',
    'NMTP':   'nmtp.info',
    'OGKB':   'ogk2.ru',
    'OZON':   'ozon.ru',
    'OZPH':   'ozonpharm.com',
    'POSI':   'ptsecurity.com',
    'PRMD':   'promomed.ru',
    'RAGR':   'rusagrogroup.ru',
    'RASP':   'raspadskaya.com',
    'RENI':   'renlife.com',
    'RGSS':   'rgs.ru',
    'SELG':   'seligdar.ru',
    'SFIN':   'sfi-group.com',
    'SMLT':   'samolet.ru',
    'SNGS':   'surgutneftegas.ru',
    'SNGSP':  'surgutneftegas.ru',
    'SVCB':   'sovcombank.ru',
    'TNSE':   'tns-e.ru',
    'TRMK':   'tmk-group.ru',
    'TRNFP':  'transneft.ru',
    'UGLD':   'ugold.ru',
    'UNAC':   'uacrussia.ru',
    'UPRO':   'unipro.energy',
    'USBN':   'uralsib.ru',
    'UTAR':   'utair.ru',
    'UWGN':   'uniwagon.com',
    'VKCO':   'vk.company',
    'VSMO':   'vsmpo-avisma.ru',
    'X5':     'x5.ru',
    # Россети-дочки
    'LSNG':   'rosseti-lenenergo.ru',
    'MRKK':   'rosseti-sk.ru',
    'MRKP':   'rossetimr.ru',
    'MRKS':   'rosseti-sib.ru',
    'MRKU':   'rosseti-ural.ru',
    # Неизвестные / мелкие — fallback circle останется
    'APTK':   '',  # Аптеки 36.6 — нет чёткого домена
    'AVAN':   '',  # Авангард
    'BTBR':   '',  # В2В
    'FIAI':   '',  # Фабрика ПО
    'JNOS':   '',  # СлавнфтЯНОС
    'MFGS':   '',  # СлавнфтМНГЗ
    'VJGZ':   '',  # Варьеганнефтегаз
}

LOGOS_DIR = Path(__file__).resolve().parent.parent / 'frontend' / 'public' / 'logos'
LOGOS_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/120.0.0.0 Safari/537.36'
)

# Порядок попыток — от лучшего к худшему
URL_TEMPLATES = [
    'https://{d}/apple-touch-icon.png',
    'https://{d}/apple-touch-icon-180x180.png',
    'https://{d}/apple-touch-icon-precomposed.png',
    'https://{d}/apple-touch-icon-152x152.png',
    'https://{d}/favicon-192x192.png',
    'https://{d}/favicon-96x96.png',
    # Последний fallback — Google Favicon
    'https://www.google.com/s2/favicons?sz=256&domain={d}',
]

MIN_SIZE_BYTES = 500  # меньше = почти наверняка 16×16 favicon, бесполезно


def fetch(url: str) -> bytes | None:
    try:
        req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
        with urllib.request.urlopen(req, timeout=8) as resp:
            if resp.status != 200:
                return None
            data = resp.read()
            # Проверить что это не HTML error page (некоторые сайты 200 на 404)
            if data.startswith(b'<!DOCTYPE') or data.startswith(b'<html'):
                return None
            if len(data) < MIN_SIZE_BYTES:
                return None
            return data
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ConnectionResetError, ConnectionRefusedError):
        return None
    except Exception:
        return None


def detect_ext(data: bytes) -> str:
    if data[:4] == b'\x89PNG':
        return 'png'
    if data[:4] == b'GIF8':
        return 'gif'
    if data[:3] == b'\xff\xd8\xff':
        return 'jpg'
    if data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        return 'webp'
    if b'<svg' in data[:500]:
        return 'svg'
    # Default — PNG (Google favicon обычно PNG)
    return 'png'


def already_exists(ticker: str) -> bool:
    for ext in ('svg', 'png', 'jpg', 'webp'):
        if (LOGOS_DIR / f'{ticker}.{ext}').exists():
            return True
    return False


def fetch_ticker(ticker: str, domain: str) -> tuple[str, int]:
    """Возвращает (status, size)."""
    if already_exists(ticker):
        return 'skip', 0
    if not domain:
        return 'no-domain', 0

    for tmpl in URL_TEMPLATES:
        url = tmpl.format(d=domain)
        data = fetch(url)
        if data:
            ext = detect_ext(data)
            path = LOGOS_DIR / f'{ticker}.{ext}'
            path.write_bytes(data)
            return f'ok[{tmpl.split("/")[-1][:20]}→{ext}]', len(data)
        time.sleep(0.05)  # небольшая пауза между попытками одного тикера
    return 'fail', 0


def main():
    ok, skip, fail, nodomain = 0, 0, 0, 0
    total_bytes = 0
    for ticker in sorted(TICKER_DOMAINS):
        domain = TICKER_DOMAINS[ticker]
        status, size = fetch_ticker(ticker, domain)
        total_bytes += size
        if status == 'skip':
            skip += 1
        elif status == 'no-domain':
            nodomain += 1
        elif status.startswith('ok'):
            ok += 1
        else:
            fail += 1
        size_str = f'{size:>6} B' if size else '      '
        print(f'{ticker:6} {domain:32} → {status:30} {size_str}', flush=True)
        time.sleep(0.2)

    print()
    print(f'Итого: ok={ok}  skip={skip}  fail={fail}  no-domain={nodomain}  '
          f'total={total_bytes/1024:.1f} KB')


if __name__ == '__main__':
    main()
