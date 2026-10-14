"""
Откуда человек пришёл к оплате — метка для subscriptions.source.

Фронт (frontend/src/utils/purchaseSource.ts) помечает переход на тарифы с
призыва: окно «Доступно на тарифе», плашка задержки данных, кнопка Plus и т.д.
Без метки — прямой заход на тарифы ('pricing'). Пишется только на первую
строку подписки из checkout / СБП / триала; продления и подарочный Pro
остаются с NULL.
"""
import re

SOURCES = {"paywall", "delay_banner", "header", "landing", "profile", "pricing"}

# Название функции из окна замка — русский текст, но в базу пускаем только
# короткую строку без управляющих символов: значение приходит от клиента.
_FEATURE_BAD = re.compile(r"[\x00-\x1f\x7f]")


def clean_source(source: str | None, feature: str | None) -> dict:
    """Поля для Subscription(**...). Неизвестный источник — пусто (NULL)."""
    if source not in SOURCES:
        return {}
    feat = _FEATURE_BAD.sub("", feature or "").strip()[:64] or None
    return {"source": source, "source_feature": feat}
