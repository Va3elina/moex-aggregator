"""
TLS-контекст для API Московской биржи (цепочка Минцифры).

ЗАЧЕМ
    24.08.2026 зарубежный УЦ отозвал сертификат, которым жили сервисы MOEX, и
    биржа переехала на Russian Trusted Root CA. Этого корня нет ни в certifi,
    ни в системном store Debian, поэтому запросы к Algopack легли с
    CERTIFICATE_VERIFY_FAILED:

        apim.moex.com  → issuer Russian Trusted Sub CA   (уже переключён)
        iss.moex.com   → issuer ZeroSSL RSA DV SSL CA 2  (пока международный)

    Контекст ниже = certifi + корень Минцифры, то есть надмножество обычного
    доверия. Старые сертификаты продолжают проходить, поэтому его безопасно
    вешать и на iss.moex.com — переключение оставшихся доменов биржи ничего
    не сломает.

ПОЧЕМУ ОТДЕЛЬНЫЙ КОНТЕКСТ, А НЕ ГЛОБАЛЬНЫЙ TRUST STORE
    Ровно та же причина, что и в api/ru_tls.py (T-Bank, VK ID, Росстат):
    корень Минцифры технически может выпустить валидный сертификат на любой
    домен, и update-ca-certificates распространил бы это доверие на весь
    исходящий трафик — Telegram, GitHub, платежи. Поэтому доверие точечное:
    контекст подключается явным ssl= там, где ходим к бирже.

ПОЧЕМУ ТОЛЬКО КОРЕНЬ, БЕЗ SUB CA
    Промежуточный биржа присылает в хендшейке сама (проверено вживую:
    certifi + корень валидирует apim.moex.com). Пин интермедиата стал бы
    лишней точкой отказа при следующей ротации Минцифры.

ПОЧЕМУ ОТДЕЛЬНЫЙ МОДУЛЬ, А НЕ api/ru_tls.py
    Фетчеры (OI/, Candles/) живут вне пакета api и импортируют только корневые
    модули (moex_calendar.py и т. п.). Тянуть в них пакет api — значит тянуть
    его зависимости (redis и прочее), которых в host-side venv может не быть.

Модуль работает и вне контейнера: если собранного бандла нет, доверие
собирается из certs/ в репозитории.
"""

from __future__ import annotations

import functools
import logging
import os
import ssl
from pathlib import Path

log = logging.getLogger(__name__)

# Бандл, собранный на этапе docker build (certifi + корень Минцифры), —
# тот же, что используют T-Bank и VK ID. См. Dockerfile.
_BUNDLE_PATH = Path(
    os.getenv("FRAME_RU_CA_BUNDLE", "/etc/ssl/frame/ru-trusted-bundle.pem")
)

# Фоллбэки на голый корень: сначала образ (Dockerfile кладёт PEM-ы в
# /etc/ssl/frame/), затем репозиторий — для запуска вне контейнера.
_ROOT_PEMS = (
    Path("/etc/ssl/frame/russian_trusted_root_ca.pem"),
    Path(__file__).resolve().parent / "certs" / "russian_trusted_root_ca.pem",
)


@functools.lru_cache(maxsize=1)
def moex_ssl_context() -> ssl.SSLContext:
    """SSLContext для запросов к MOEX — с ПОЛНОЙ проверкой сертификата.

    Годится и для aiohttp (``TCPConnector(ssl=...)``), и для стандартного
    ssl-клиента. Контекст кэшируется: разбор бандла на ~150 сертификатов
    стоит заметно дороже, чем один запрос свечей.

    Raises:
        FileNotFoundError: нет ни собранного бандла, ни корня в репозитории.
            Падаем громко: тихий откат на ssl=False — ровно та дыра, которую
            этот модуль закрывает (до 24.08.2026 фетчер OI 5min ходил без
            проверки сертификата вообще).
    """
    if _BUNDLE_PATH.is_file():
        return ssl.create_default_context(cafile=str(_BUNDLE_PATH))

    root = next((p for p in _ROOT_PEMS if p.is_file()), None)
    if root is None:
        raise FileNotFoundError(
            "Нет корня Минцифры для MOEX: бандл "
            f"{_BUNDLE_PATH} отсутствует (норма вне контейнера), и ни один из "
            f"{[str(p) for p in _ROOT_PEMS]} не найден. Внутри контейнера это "
            "означает сломанный docker build."
        )

    ctx = ssl.create_default_context()
    try:
        import certifi

        ctx.load_verify_locations(cafile=certifi.where())
    except ImportError:
        pass  # системного store хватит: корень Минцифры добавляем ниже
    ctx.load_verify_locations(cafile=str(root))

    log.info("MOEX TLS: бандл не найден, доверие собрано из %s", root)
    return ctx
