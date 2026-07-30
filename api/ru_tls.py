"""
TLS-доверие к корню Минцифры для российских внешних сервисов.

ЗАЧЕМ
    T-Bank (эквайринг) и VK ID (OAuth) переводят TLS-сертификаты на Russian
    Trusted Root CA. Этот корень не входит в Mozilla CA Program, поэтому его
    нет ни в certifi, ни в системном CA-store Debian. Как только вендор
    переключит боевой домен, наши исходящие запросы лягут с
    CERTIFICATE_VERIFY_FAILED — то есть отвалятся приём платежей и вход через VK.

    На 30.07.2026 вендоры уже переключили «передние» контуры:
    partners.api.vk.ru и rest-api-test.tinkoff.ru отдают Минцифры-цепочку,
    боевые securepay.tinkoff.ru / id.vk.com — пока международные CA.
    Правка обратно совместима: бандл = certifi + корень, старые сертификаты
    продолжают проходить.

ПОЧЕМУ ОТДЕЛЬНЫЙ БАНДЛ, А НЕ СИСТЕМНЫЙ TRUST STORE
    1. httpx и requests читают certifi, а НЕ системный /usr/lib/ssl/cert.pem.
       Инструкции T-Bank и VK советуют update-ca-certificates — для нас этого
       мало: curl и aiohttp починятся, а платёжные запросы (httpx) всё равно
       упадут. Апгрейд certifi тоже не поможет: он зеркалит Mozilla CA Program,
       куда этот CA не принят.
    2. Корень технически может выпустить валидный сертификат на ЛЮБОЙ домен.
       Глобальное доверие распространило бы это на MOEX, Telegram, GitHub и
       весь прочий исходящий трафик. Поэтому бандл подключается явным verify=
       ровно там, где нужен: api/billing/tbank.py и api/routers/oauth.py.

ПОЧЕМУ ТОЛЬКО КОРЕНЬ, БЕЗ SUB CA
    Промежуточный сертификат сервер присылает сам в TLS-хендшейке. Вдобавок
    Sub CA с gu-st.ru уже разошёлся с тем, что реально отдают T-Bank и VK —
    Минцифры его ротировали. Пиннинг интермедиата = поломка при следующей
    ротации. Проверено вживую: certifi + корень валидирует обоих вендоров.

ИСКЛЮЧЕНИЕ — РОССТАТ (rosstat.gov.ru)
    Там правило выше не работает: сервер отдаёт ТОЛЬКО листовой сертификат,
    без промежуточного (openssl: `Verify return code: 21`). Достроить путь до
    корня нечем, поэтому для него собран второй бандл — с Sub CA внутри
    (rosstat-bundle.pem, см. rosstat_ssl_context ниже). Бандлы держим врозь,
    чтобы пин интермедиата не протёк в платёжный путь.

ЛОКАЛЬНАЯ РАЗРАБОТКА
    Бандл собирается на этапе docker build (см. Dockerfile). Вне контейнера
    файла нет → молча откатываемся на обычный certifi. Боевые домены пока на
    международных CA, поэтому локальная разработка не страдает.

Проверка состояния миграции: python scripts/check_ru_tls.py
"""

from __future__ import annotations

import logging
import os
import ssl
from pathlib import Path

log = logging.getLogger(__name__)

# Путь задаётся Dockerfile'ом. Env-override — для нестандартных окружений
# (запуск вне контейнера, отладка, смена раскладки образа).
RU_CA_BUNDLE_PATH = Path(
    os.getenv("FRAME_RU_CA_BUNDLE", "/etc/ssl/frame/ru-trusted-bundle.pem")
)

# Значение для httpx `verify=`: путь к бандлу либо True (обычный certifi).
# httpx падает с IOError, если передать путь к несуществующему файлу, поэтому
# наличие проверяем здесь один раз на импорте, а не на каждом запросе.
RU_TLS_VERIFY: str | bool = (
    str(RU_CA_BUNDLE_PATH) if RU_CA_BUNDLE_PATH.is_file() else True
)

if RU_TLS_VERIFY is True:
    log.warning(
        "Бандл с корнем Минцифры не найден (%s) — запросы к T-Bank и VK ID идут "
        "на обычном certifi. Вне контейнера это норма; ВНУТРИ контейнера "
        "означает сломанный docker build, и приём платежей ляжет, как только "
        "T-Bank переключит боевой сертификат.",
        RU_CA_BUNDLE_PATH,
    )
else:
    log.info("TLS-бандл с корнем Минцифры активен: %s", RU_CA_BUNDLE_PATH)


# ══════════════════════════════════════════════════════════════════════
#   Росстат — второй бандл, с промежуточным сертификатом
# ══════════════════════════════════════════════════════════════════════

ROSSTAT_CA_BUNDLE_PATH = Path(
    os.getenv("FRAME_ROSSTAT_CA_BUNDLE", "/etc/ssl/frame/rosstat-bundle.pem")
)

# Фоллбэк для запуска вне контейнера: тот же набор CA, но из репозитория.
# Внутрь образа certs/ не копируется (там whitelist-COPY только кода), так
# что в проде всегда выигрывает бандл, собранный на этапе docker build.
_REPO_CERTS_DIR = Path(__file__).resolve().parent.parent / "certs"
_MINCIFRY_PEMS = (
    _REPO_CERTS_DIR / "russian_trusted_root_ca.pem",
    _REPO_CERTS_DIR / "russian_trusted_sub_ca.pem",
)


def rosstat_ssl_context() -> ssl.SSLContext:
    """SSLContext для rosstat.gov.ru — с ПОЛНОЙ проверкой сертификата.

    Нужен отдельный контекст, а не общий бандл, по двум причинам:

    1. Корня Минцифры нет в certifi (как и для T-Bank/VK).
    2. Росстат не присылает промежуточный сертификат — в хендшейке ровно
       один `BEGIN CERTIFICATE`, openssl отвечает `Verify return code: 21
       (unable to verify the first certificate)`. Поэтому Sub CA лежит в
       бандле у нас; для вендоров так делать НЕ надо, они его шлют сами.

    Клиент здесь — стандартный `urllib` (фетчер ВВП), которому нужен именно
    SSLContext, а не строка для httpx `verify=`. Пути к бандлу хватило бы и
    в виде cafile, но вызывающему коду удобнее готовый контекст.

    Raises:
        FileNotFoundError: нет ни собранного бандла, ни PEM-ов в репозитории.
            Падаем громко и осознанно: тихий откат на `verify=False` — ровно
            та дыра, которую эта функция закрывает (до 30.07.2026 фетчер ВВП
            ходил с CERT_NONE).
    """
    if ROSSTAT_CA_BUNDLE_PATH.is_file():
        return ssl.create_default_context(cafile=str(ROSSTAT_CA_BUNDLE_PATH))

    missing = [str(p) for p in _MINCIFRY_PEMS if not p.is_file()]
    if missing:
        raise FileNotFoundError(
            "Нет бандла с цепочкой Минцифры для rosstat.gov.ru: "
            f"{ROSSTAT_CA_BUNDLE_PATH} отсутствует (норма вне контейнера), "
            f"и в репозитории не найдено: {', '.join(missing)}. "
            "Внутри контейнера это означает сломанный docker build."
        )

    ctx = ssl.create_default_context()
    try:
        import certifi

        ctx.load_verify_locations(cafile=certifi.where())
    except ImportError:
        pass  # системного store хватит: цепочку Минцифры добавляем ниже
    for pem in _MINCIFRY_PEMS:
        ctx.load_verify_locations(cafile=str(pem))

    log.info("Росстат: бандл не найден, доверие собрано из %s", _REPO_CERTS_DIR)
    return ctx
