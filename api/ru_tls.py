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

ЛОКАЛЬНАЯ РАЗРАБОТКА
    Бандл собирается на этапе docker build (см. Dockerfile). Вне контейнера
    файла нет → молча откатываемся на обычный certifi. Боевые домены пока на
    международных CA, поэтому локальная разработка не страдает.

Проверка состояния миграции: python scripts/check_ru_tls.py
"""

from __future__ import annotations

import logging
import os
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
