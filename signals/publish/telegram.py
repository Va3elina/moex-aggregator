"""Telegram publisher for signal posts.

Использует sendPhoto (с JPEG-сжатием) — у нас нет alpha в чарте,
а inline-preview важен для канала. Caption limit 1024 chars, наши ~150.
"""
from __future__ import annotations
import html
import os
from pathlib import Path
from typing import Optional, Tuple

import requests

from signals import config

# TELEGRAM_API_ROOT — релей (Cloudflare Worker) для обхода РКН без зависимости от IPv6.
# Дефолт = прямой Telegram (поведение не меняется, пока env не задан на проде).
_API_ROOT = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org")

# Фирменные custom-emoji Frame (пак t.me/addemoji/Frametool, Telegram Premium —
# не-Premium получатели видят plain-эмодзи фолбэк автоматически, тег не ломается).
# Полный инвентарь пака снят через getStickerSet 2026-07-14; отобраны эмодзи из
# словаря настроения Шага А/В синтеза (signals/content_ai.py, Anweisungen Routine).
# 🔽🔼🚨 в паке НЕТ — Шаг В их не использует (см. Routine-инструкцию), поэтому
# карта ниже покрывает ровно то, что реально может появиться в draft_text.
_CUSTOM_EMOJI_MAP = {
    "🔥": "5454087765559909583",
    "⛔️": "5454074292247499750",
    "📣": "5453896063989620863",
    "💡": "5454396453449408225",
    "🔍": "5454392549324136501",
    "◽️": "5454003420992151507",
}

# Фирменная подпись Frame — «😀😀😀» это НЕ буквальные смайлики, а плейсхолдер-
# эмодзи для ТРЁХ РАЗНЫХ custom-emoji (в этом порядке): скобка-логотип + «FRA» +
# «ME» → вместе рендерятся как ⌐FRAME. Опознано вживую 2026-07-14 (Вадим сверил
# 6 кандидатов из пака по номерам, см. содержимое реальных постов канала —
# «😀😀😀/@FrameTool» в конце). В паке ДВА варианта каждого символа — этот
# набор БЕЗ белой подложки (тёмный/прозрачный фон, годится под тёмную тему TG),
# второй набор (белый фон) НЕ использовать. ⚠️ НЕ просить ИИ писать эту строку
# самому — id непрозрачны, легко перепутать порядок/число; подпись добавляется
# ЗДЕСЬ, на уровне публикации, одинаково для каждого поста.
_LOGO_SIGNATURE_IDS = (
    "5456376948768936418",  # скобка (без белого фона)
    "5456183761139961304",  # «FRA» (без белого фона)
    "5454037394183462308",  # «ME» (без белого фона)
)
FRAME_SIGNATURE = "😀😀😀/@FrameTool"


def apply_custom_emoji(text: str) -> str:
    """HTML-escape пользовательский/AI-сгенерированный текст, затем заменить
    известные plain-эмодзи на <tg-emoji> (премиальные custom-emoji Frame).
    ВАЖНО: escape ДО подстановки — иначе сама подстановленная разметка
    заэкранируется. Emoji-символы escape не трогает, коллизий нет.

    «😀😀😀» — особый случай (позиционная тройка, не 1:1 маппинг, см.
    _LOGO_SIGNATURE_IDS) — обрабатывается ПЕРЕД обычной подстановкой."""
    escaped = html.escape(text)
    logo = "".join(f'<tg-emoji emoji-id="{eid}">😀</tg-emoji>' for eid in _LOGO_SIGNATURE_IDS)
    escaped = escaped.replace("😀😀😀", logo)
    for emoji, emoji_id in _CUSTOM_EMOJI_MAP.items():
        escaped = escaped.replace(emoji, f'<tg-emoji emoji-id="{emoji_id}">{emoji}</tg-emoji>')
    return escaped


def with_data_annotation(draft_text: str, annotation: str | None) -> str:
    """Приклеить строку «на основании таких-то данных на такое-то число».

    ⚠️ СОБИРАЕТ КОД, НЕ МОДЕЛЬ, и живёт это на слое публикации — ровно по той же
    причине, что и фирменная подпись ниже. С подписью урок уже оплачен: промпт просил
    модель писать её самой, она писала, и на публикации подпись добавлялась второй раз
    (18 черновиков из 23). С числами цена ошибки выше: выдуманное число неотличимо от
    настоящего, и проверить его читателю нечем.

    Идемпотентно: если аннотация уже в тексте, второй раз не добавляем.
    """
    if not annotation:
        return draft_text
    body = (draft_text or "").rstrip()
    if annotation.strip() in body:
        return body
    # ⚠️ ПЕРЕД ХЭШТЕГОМ, а не в конец. Шаг В кладёт один хэштег последней строкой,
    # и приклеенная в хвост аннотация разрывала порядок: текст, хэштег, аннотация,
    # подпись. Поймано на первом же тестовом рендере. Порядок в посте:
    # тело → аннотация → подпись → хэштег.
    lines = body.split("\n")
    if lines[-1].strip().startswith("#"):
        head = "\n".join(lines[:-1]).rstrip()
        return f"{head}\n\n{annotation.strip()}\n\n{lines[-1].strip()}"
    return f"{body}\n\n{annotation.strip()}"


def with_frame_signature(draft_text: str) -> str:
    """Добавить фирменную подпись «⌐FRAME/@FrameTool» — ВСЕГДА, на уровне
    публикации/превью, а не в тексте черновика (см. FRAME_SIGNATURE).

    Порядок как в реальных постах канала: подпись ПЕРЕД хэштегом, не после
    (Шаг В сам кладёт один хэштег последней строкой — см. Anweisungen Routine
    "В конце — один хэштег..."). Если последняя непустая строка похожа на
    хэштег — подпись вставляется перед ней; иначе просто в конец (fallback,
    напр. если модель хэштег не написала)."""
    # ⚠️ ИДЕМПОТЕНТНОСТЬ. Подпись принадлежит слою публикации, но промпты Шага В
    # (и старый, и новый) требовали от модели писать её самой — и она писала.
    # Итог: 18 черновиков из 23 уже содержат подпись, и на превью/публикации она
    # добавлялась ВТОРОЙ раз. Поймано Вадимом на карточке кандидата 1357.
    # Промпты исправлены, но защита нужна и здесь: 18 старых черновиков никуда не
    # делись, а модель может сбиться и снова дописать подпись.
    if FRAME_SIGNATURE in draft_text:
        return draft_text.rstrip()
    body = draft_text.rstrip()
    lines = body.split("\n")
    last = lines[-1].strip()
    if last.startswith("#"):
        head = "\n".join(lines[:-1]).rstrip()
        return f"{head}\n\n{FRAME_SIGNATURE}\n\n{last}"
    return f"{body}\n\n{FRAME_SIGNATURE}"


def send_signal_post(
    *,
    image_path: Path,
    caption: str,
) -> Tuple[bool, Optional[int], Optional[str]]:
    """Отправить (фото + caption) в SIGNALS_CHANNEL_ID.

    Returns:
        (ok, message_id, error_text)
        - ok=True  → message_id заполнен, error_text=None
        - ok=False → message_id=None, error_text содержит описание
    """
    url = f"{_API_ROOT}/bot{config.BOT_TOKEN}/sendPhoto"
    try:
        with image_path.open("rb") as f:
            resp = requests.post(
                url,
                data={
                    "chat_id": str(config.SIGNALS_CHANNEL_ID),
                    "caption": caption,
                },
                files={"photo": (image_path.name, f, "image/png")},
                timeout=60,
            )
    except requests.exceptions.RequestException as e:
        # Sanitize: requests печатает full URL (с токеном) в repr ошибки.
        safe = str(e).replace(config.BOT_TOKEN, "<REDACTED>") if config.BOT_TOKEN else str(e)
        return False, None, safe

    try:
        data = resp.json()
    except ValueError:
        return False, None, f"non-JSON response (HTTP {resp.status_code})"
    if data.get("ok"):
        return True, data["result"]["message_id"], None
    return False, None, data.get("description") or str(data)


def send_text_post(*, text: str, channel_id: int) -> Tuple[bool, Optional[int], Optional[str]]:
    """Отправить ЧИСТО ТЕКСТОВЫЙ пост (content-пайплайн, Этап 9 — черновики без
    авто-сгенерированного скриншота графика). channel_id параметризован (не
    захардкожен на SIGNALS_CHANNEL_ID) — тестовый канал на старте, см.
    signals/config.CONTENT_CHANNEL_ID. Добавляет фирменную подпись
    (with_frame_signature) и применяет фирменные custom-emoji (apply_custom_emoji)
    — HTML-режим, поэтому text ДОЛЖЕН прийти как plain-текст (не
    пред-эскейпленный), функция сама эскейпит. Возврат — тот же контракт,
    что у send_signal_post."""
    url = f"{_API_ROOT}/bot{config.BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            data={"chat_id": str(channel_id),
                  "text": apply_custom_emoji(with_frame_signature(text)),
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=30,
        )
    except requests.exceptions.RequestException as e:
        safe = str(e).replace(config.BOT_TOKEN, "<REDACTED>") if config.BOT_TOKEN else str(e)
        return False, None, safe

    try:
        data = resp.json()
    except ValueError:
        return False, None, f"non-JSON response (HTTP {resp.status_code})"
    if data.get("ok"):
        return True, data["result"]["message_id"], None
    return False, None, data.get("description") or str(data)
