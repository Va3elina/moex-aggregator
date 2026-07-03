"""
Telegram Alert-Bot (Frame) — host-side long-poll поллер. Phase 1: привязка аккаунта.

ЗАПУСК НА ХОСТЕ (не в контейнере!): РКН блокирует IPv4 Telegram → нужен IPv6 хоста.
  /opt/frame/signals/.venv/bin/python -m signals.alert_bot
Под systemd (frame-alert-bot.service, auto-restart). ENV (.env корня): ALERT_BOT_TOKEN.

DB: используем общий api.database.SessionLocal. На хосте DB_URL указывает на @db:5432
(docker-network) — переопределяем на 127.0.0.1 ДО импорта api.database (как делает
signals/run.sh через sed, только in-process т.к. процесс долгоживущий).

Команды: /start <token> — привязка; /menu — меню; /alerts /stop /resume /help.
Phase B: inline-навигация (меню → список → карточка → пауза/возобновить/удалить)
через callback_query + editMessageText (правим то же сообщение).
"""
import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

# ── .env + DB_URL override ДО импорта api.database ──────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # /opt/frame
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:  # host-side: docker-hostname → localhost
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from sqlalchemy import text  # noqa: E402
from api.database import SessionLocal  # noqa: E402  (импорт ПОСЛЕ DB_URL override)

BOT_TOKEN = os.environ["ALERT_BOT_TOKEN"]
# TELEGRAM_API_ROOT — релей (Cloudflare Worker) для обхода РКН без зависимости от IPv6
# хоста. Дефолт = прямой Telegram (поведение не меняется, пока env не задан на проде).
API_ROOT = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org")
API_BASE = f"{API_ROOT}/bot{BOT_TOKEN}"

SITE = "https://xn--80aklbnczmv.xn--p1ai"  # punycode таймфрейм.рф (надёжно в TG)

HELP_TEXT = (
    "🔔 <b>Frame Signal</b> — алерты по рынку MOEX.\n\n"
    "Новые алерты создаются на сайте таймфрейм.рф (Личный кабинет → Алерты),\n"
    "а уведомления приходят сюда. Управлять уже созданными можно прямо тут.\n\n"
    "Команды:\n"
    "/menu — меню · /alerts — список\n"
    "/stop — пауза всех · /resume — возобновить всех\n"
    "/help — помощь"
)

# ── ярлыки операторов/метрик/статусов для карточек (зеркало alerts_run.py) ──
_OP_PRICE = {"cross_up": "↑ пересекла", "cross_down": "↓ пересекла",
             "gt": "выше", "lt": "ниже"}
_OP_SHORT = {"cross_up": "↑✕", "cross_down": "↓✕", "gt": ">", "lt": "<"}
_STATUS_DOT = {"active": "🟢", "paused": "⏸", "fired": "✅"}
_STATUS_WORD = {"active": "активен", "paused": "на паузе", "fired": "сработал (once)"}
# Таймфрейм OI-алерта (раннее срабатывание дневного сигнала по интрадей-бару).
_TF_LABEL = {"5m": "5 мин", "1h": "1 час", "1d": "день"}

# Источник раздела для выбора по индикатору (зеркало _alert_source в api/routers/alerts).
# Два «индикатора» в боте: ОИ (открытые позиции/цена) и Фонды (потоки).
def _source_of(indicator: str) -> str:
    return "funds" if indicator == "funds_flow" else "oi"

_SOURCE_LABEL = {"oi": "🔓 Открытые позиции", "funds": "💰 Фонды"}

# Человеко-имя fund-таргета, когда asset_name пуст (старые fund-алерты по категории
# без метки выбора). Для новых asset_name приходит с фронта («Все фонды»/«N фондов»).
_FUND_ASSET_NAME = {
    "all": "Все фонды", "custom": "Набор фондов",
    "money_market": "Денежный рынок", "stocks": "Акции",
    "bonds": "Облигации", "gold": "Золото",
}


def _num(v) -> str:
    """Аккуратный вывод числа: целое без .0, дробное компактно (как :g)."""
    try:
        return f"{float(v):g}"
    except (TypeError, ValueError):
        return str(v)


def _redact(s) -> str:
    """Не дать токену утечь в логи (RequestException печатает full URL)."""
    return str(s).replace(BOT_TOKEN, "<REDACTED>")


def send(chat_id, text_msg: str) -> None:
    try:
        requests.post(
            f"{API_BASE}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": text_msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
    except requests.RequestException as e:
        print(f"[alert_bot] send error: {_redact(e)}")


def send_kb(chat_id, text_msg: str, inline_keyboard: list) -> None:
    """sendMessage с inline-клавиатурой (новое сообщение)."""
    try:
        requests.post(
            f"{API_BASE}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text_msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "reply_markup": {"inline_keyboard": inline_keyboard},
            },
            timeout=15,
        )
    except requests.RequestException as e:
        print(f"[alert_bot] send_kb error: {_redact(e)}")


# ── Постоянная reply-клавиатура (нижний бар, висит ВСЕГДА — без повторного /start) ──
# Кнопки шлют свой ТЕКСТ обычным сообщением → ловим в process_update по тексту.
# URL-кнопок reply-клавиатура НЕ поддерживает (только inline) → сайт остаётся
# ссылкой в тексте. is_persistent=True держит бар постоянно даже когда поле ввода
# в фокусе; resize_keyboard подгоняет высоту. Подписи — как в inline-меню (единообразно).
_REPLY_KB = {
    "keyboard": [[{"text": "📋 Мои алерты"}, {"text": "❓ Помощь"}]],
    "resize_keyboard": True,
    "is_persistent": True,
}


def send_persistent(chat_id, text_msg: str) -> None:
    """sendMessage + постоянная нижняя клавиатура (устанавливает/обновляет бар).
    Бар сохраняется для всех последующих сообщений (вкл. сигналы с inline-кнопками,
    они на него не влияют) — пока не заменить другой reply-клавиатурой."""
    try:
        requests.post(
            f"{API_BASE}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text_msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "reply_markup": _REPLY_KB,
            },
            timeout=15,
        )
    except requests.RequestException as e:
        print(f"[alert_bot] send_persistent error: {_redact(e)}")


def edit_kb(chat_id, message_id, text_msg: str, inline_keyboard: list) -> None:
    """editMessageText — правим то же сообщение (in-place навигация)."""
    try:
        requests.post(
            f"{API_BASE}/editMessageText",
            json={
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text_msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "reply_markup": {"inline_keyboard": inline_keyboard},
            },
            timeout=15,
        )
    except requests.RequestException as e:
        print(f"[alert_bot] edit_kb error: {_redact(e)}")


def answer_cb(callback_query_id, text_msg: str = "") -> None:
    """answerCallbackQuery — обязателен на каждый тап (убирает «часики»)."""
    try:
        data = {"callback_query_id": callback_query_id}
        if text_msg:
            data["text"] = text_msg
        requests.post(f"{API_BASE}/answerCallbackQuery", data=data, timeout=15)
    except requests.RequestException as e:
        print(f"[alert_bot] answer_cb error: {_redact(e)}")


def link_account(token: str, chat_id: int, username) -> bool:
    """Привязать chat_id к юзеру по одноразовому link-токену. True если успешно."""
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT user_id FROM telegram_link_tokens "
                 "WHERE token = :t AND expires_at > now()"),
            {"t": token},
        ).fetchone()
        if not row:
            return False
        user_id = row[0]
        # один Telegram-чат = один аккаунт: снять чат с других юзеров
        db.execute(
            text("UPDATE users SET telegram_chat_id = NULL "
                 "WHERE telegram_chat_id = :c AND id <> :u"),
            {"c": chat_id, "u": user_id},
        )
        db.execute(
            text("UPDATE users SET telegram_chat_id = :c, telegram_username = :un, "
                 "telegram_linked_at = now() WHERE id = :u"),
            {"c": chat_id, "un": username, "u": user_id},
        )
        # все link-токены юзера отработали — чистим
        db.execute(text("DELETE FROM telegram_link_tokens WHERE user_id = :u"), {"u": user_id})
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        print(f"[alert_bot] link error: {e}")
        return False
    finally:
        db.close()


# ── DB helpers (host-side, через text() как остальной файл) ─────────────────
def _user_id_by_chat(db, chat_id: int):
    row = db.execute(
        text("SELECT id FROM users WHERE telegram_chat_id = :c"),
        {"c": chat_id},
    ).fetchone()
    return row[0] if row else None


def _list_alerts(db, user_id: int, source=None):
    """Алерты юзера (свежие сверху), включая 'fired' (сработавшие once) — чтобы их
    можно было найти в боте и нажать «Возобновить»/«Удалить» (иначе fired-алерт был
    бы недостижим из бот-навигации). source задан → фильтр по разделу (oi|funds);
    COALESCE(source,'oi') — старые алерты без source трактуем как ОИ."""
    q = ("SELECT id, indicator, asset, asset_name, metric, clgroup, op, "
         "threshold, mode, status, last_value, last_fired_at, timeframe "
         "FROM alerts WHERE user_id = :u AND status IN ('active','paused','fired')")
    params = {"u": user_id}
    if source:
        q += " AND COALESCE(source, 'oi') = :src"
        params["src"] = source
    q += " ORDER BY created_at DESC, id DESC"
    return db.execute(text(q), params).fetchall()


def _source_counts(db, user_id: int) -> dict:
    """{source: count} по активным/paused/fired — для picker'а выбора индикатора."""
    rows = db.execute(
        text("SELECT COALESCE(source, 'oi') AS src, count(*) FROM alerts "
             "WHERE user_id = :u AND status IN ('active','paused','fired') "
             "GROUP BY 1"),
        {"u": user_id},
    ).fetchall()
    return {r[0]: int(r[1]) for r in rows}


def _get_alert(db, user_id: int, alert_id: int):
    return db.execute(
        text("SELECT id, indicator, asset, asset_name, metric, clgroup, op, "
             "threshold, mode, status, last_value, last_fired_at, timeframe "
             "FROM alerts WHERE id = :a AND user_id = :u"),
        {"a": alert_id, "u": user_id},
    ).fetchone()


# ── текстовые форматтеры карточек ───────────────────────────────────────────
def _alert_unit(indicator: str) -> str:
    if indicator == "oi_zscore":
        return "σ"
    if indicator in ("oi_move", "oi_participants", "funds_flow"):
        return "×"        # множитель ATR «во сколько раз больше обычного»
    if indicator == "oi_level":
        return ""         # уровень чистой позиции — контракты, без символа
    return "₽"


def _alert_short_line(a) -> str:
    """Одна строка для кнопки списка: статус + актив + op + порог (+группа для OI,
    +таймфрейм для интрадей OI-алертов). Fund-алерт — метка выбора + «поток ×N»."""
    _, indicator, asset, asset_name, _metric, clgroup, op, threshold = a[:8]
    status = a[9]
    timeframe = (a[12] if len(a) > 12 else None) or "1d"
    dot = _STATUS_DOT.get(status, "•")
    opn = _OP_SHORT.get(op, op)
    if indicator == "funds_flow":
        name = asset_name or _FUND_ASSET_NAME.get(asset, asset)
        return f"{dot} {name} · поток {opn}{_num(threshold)}×"
    unit = _alert_unit(indicator)
    grp = ""
    if indicator == "oi_zscore":
        grp = " физ" if (clgroup or "FIZ") == "FIZ" else " юр"
    elif indicator == "oi_move":
        grp = {"FIZ": " движ·физ", "YUR": " движ·юр"}.get(clgroup or "ALL", " движ")
    elif indicator == "oi_participants":
        grp = " уч·физ" if (clgroup or "FIZ") == "FIZ" else " уч·юр"
    # Таймфрейм показываем только для интрадей OI-алертов (1d — поведение по умолчанию).
    tf = ""
    if indicator in ("oi_move", "oi_participants") and timeframe != "1d":
        tf = f" · {_TF_LABEL.get(timeframe, timeframe)}"
    return f"{dot} {asset}{grp} {opn} {_num(threshold)}{unit}{tf}"


def _alert_card_text(a) -> str:
    """Подробная карточка алерта (HTML)."""
    (_id, indicator, asset, asset_name, _metric, clgroup, op,
     threshold, mode, status, last_value, last_fired_at, timeframe) = a
    timeframe = timeframe or "1d"
    unit = _alert_unit(indicator)

    if indicator == "oi_zscore":
        name = asset_name or asset
        head = "Открытые позиции"
        clg = "физлица" if (clgroup or "FIZ") == "FIZ" else "юрлица"
        cond = f"z-score ({clg}) {_OP_PRICE.get(op, op)} {_num(threshold)}{unit}"
    elif indicator == "oi_move":
        name = asset_name or asset
        head = "Резкое движение позиции"
        clg = {"FIZ": "физлица", "YUR": "юрлица"}.get(clgroup or "ALL", "в целом")
        cond = f"движение позиции ({clg}) больше обычного в {_num(threshold)}{unit}"
    elif indicator == "oi_participants":
        name = asset_name or asset
        head = "Резкое изменение числа участников"
        clg = "физлица" if (clgroup or "FIZ") == "FIZ" else "юрлица"
        cond = f"число участников ({clg}) меняется резче обычного в {_num(threshold)}{unit}"
    elif indicator == "oi_level":
        name = asset_name or asset
        head = "Открытый интерес"
        clg = "физлица" if (clgroup or "FIZ") in ("FIZ", "ALL") else "юрлица"
        cond = f"чистая позиция ({clg}) {_OP_PRICE.get(op, op)} {_num(threshold)}"
    elif indicator == "funds_flow":
        # Фонды: заголовок — метка выбора (без сырого asset 'all'/'custom').
        name = asset_name or _FUND_ASSET_NAME.get(asset, asset)
        head = "Аномальный поток"
        cond = f"поток денег в фонды больше обычного в {_num(threshold)}{unit}"
    else:
        name = asset_name or asset
        head = "Цена фьючерса"
        cond = f"цена {_OP_PRICE.get(op, op)} {_num(threshold)} {unit}"

    # Заголовок: для OI/цены — «имя · sectype»; для фондов sectype нет, только имя.
    title = name if indicator == "funds_flow" else f"{name} · {asset}"
    mode_word = "однократно" if mode == "once" else "повторно"
    lines = [
        f"🔔 <b>{title}</b>",
        f"<i>{head}</i>",
        "",
        f"Условие: {cond}",
        f"Режим: {mode_word}",
        f"Статус: {_STATUS_DOT.get(status, '•')} {_STATUS_WORD.get(status, status)}",
    ]
    # Таймфрейм — только для OI-метрик (у цены он не применяется). Показываем
    # всегда: 'день' для дневной публикации, '5 мин'/'1 час' для раннего сигнала.
    if indicator in ("oi_move", "oi_participants"):
        lines.append(f"Таймфрейм: {_TF_LABEL.get(timeframe, timeframe)}")
    if last_value is not None:
        lines.append(f"Последнее значение: {_num(last_value)}{unit}")
    if last_fired_at is not None:
        lines.append(f"Срабатывал: {last_fired_at:%d.%m.%Y %H:%M} UTC")
    return "\n".join(lines)


# ── view-builders: (text, inline_keyboard) ──────────────────────────────────
def _view_menu():
    txt = (
        "👋 <b>Frame Signal</b>\n\n"
        "Здесь приходят алерты по рынку MOEX и можно ими управлять.\n"
        "Новые создаются на сайте — в Личном кабинете."
    )
    kb = [
        [{"text": "📋 Мои алерты", "callback_data": "la"}],
        [{"text": "❓ Помощь", "callback_data": "help"}],
        [{"text": "🌐 Открыть Фрейм", "url": SITE}],
    ]
    return txt, kb


def _welcome_text() -> str:
    """Текст приветствия для /start и /menu — шлётся с постоянной нижней
    клавиатурой (send_persistent). Сайт — ссылкой (URL-кнопку reply-бар не держит)."""
    return (
        "👋 <b>Frame Signal</b>\n\n"
        "Здесь приходят алерты по рынку MOEX и можно ими управлять.\n"
        f"Новые создаются на сайте — <a href=\"{SITE}\">в Личном кабинете</a>.\n\n"
        "Кнопки снизу всегда под рукой 👇"
    )


# Сколько карточек на страницу списка (Telegram-клавиатура не должна быть бесконечной).
_PAGE = 25


def _view_list_root(db, user_id: int):
    """Корень «Мои алерты» — ВЫБОР ИНДИКАТОРА. Вместо плоского дампа всех алертов
    показываем два раздела (ОИ / Фонды) со счётчиками; тап ведёт в список раздела."""
    counts = _source_counts(db, user_id)
    total = sum(counts.values())
    if total == 0:
        txt = (
            "У вас пока нет активных алертов.\n\n"
            "Создайте первый на сайте: Личный кабинет → Алерты."
        )
        kb = [
            [{"text": "🌐 Открыть Фрейм", "url": SITE}],
            [{"text": "← Меню", "callback_data": "m"}],
        ]
        return txt, kb
    txt = f"📋 <b>Ваши алерты</b> ({total})\n\nВыберите раздел:"
    # Показываем оба индикатора всегда (со счётчиком) — чтобы выбор по индикатору
    # был виден; пустой раздел откроется с пояснением.
    kb = [[{"text": f"{_SOURCE_LABEL[src]} ({counts.get(src, 0)})",
            "callback_data": f"ls:{src}"}] for src in ("oi", "funds")]
    if total > 1:
        kb.append([{"text": f"🗑 Удалить все ({total})", "callback_data": "dall"}])
    kb.append([{"text": "← Меню", "callback_data": "m"}])
    return txt, kb


def _view_list(db, user_id: int, source: str, page: int = 0):
    """Список алертов одного раздела (oi|funds), пагинированный. Назад — к выбору
    индикатора (_view_list_root)."""
    rows = _list_alerts(db, user_id, source)
    label = _SOURCE_LABEL.get(source, "📋 Алерты")
    if not rows:
        txt = f"<b>{label}</b>\n\nВ этом разделе пока нет алертов."
        return txt, [[{"text": "← Назад", "callback_data": "la"}]]
    pages = (len(rows) + _PAGE - 1) // _PAGE
    page = max(0, min(page, pages - 1))
    chunk = rows[page * _PAGE:(page + 1) * _PAGE]
    head = f"<b>{label}</b> ({len(rows)})"
    if pages > 1:
        head += f" · стр {page + 1}/{pages}"
    txt = f"{head}\n\nВыберите, чтобы открыть карточку:"
    kb = [[{"text": _alert_short_line(r), "callback_data": f"v:{r[0]}"}] for r in chunk]
    if pages > 1:
        nav = []
        if page > 0:
            nav.append({"text": "← стр", "callback_data": f"ls:{source}:{page - 1}"})
        if page < pages - 1:
            nav.append({"text": "стр →", "callback_data": f"ls:{source}:{page + 1}"})
        kb.append(nav)
    kb.append([{"text": "← Разделы", "callback_data": "la"}])
    return txt, kb


def _view_card(db, user_id: int, alert_id: int):
    a = _get_alert(db, user_id, alert_id)
    if not a:
        return ("Алерт не найден — возможно, он был удалён.",
                [[{"text": "← К списку", "callback_data": "la"}]])
    status = a[9]
    if status == "active":
        toggle = {"text": "⏸ Поставить на паузу", "callback_data": f"p:{alert_id}"}
    else:  # paused | fired
        toggle = {"text": "▶️ Возобновить", "callback_data": f"r:{alert_id}"}
    back_src = _source_of(a[1])   # вернуться в раздел этого алерта (ОИ/Фонды)
    kb = [
        [toggle],
        [{"text": "🗑 Удалить", "callback_data": f"dq:{alert_id}"}],
        [{"text": "← К списку", "callback_data": f"ls:{back_src}"}],
    ]
    return _alert_card_text(a), kb


def _view_delete_confirm(db, user_id: int, alert_id: int):
    a = _get_alert(db, user_id, alert_id)
    if not a:
        return ("Алерт не найден — возможно, он уже удалён.",
                [[{"text": "← К списку", "callback_data": "la"}]])
    # Заголовок: для фондов — метка выбора (без сырого 'all'/'custom'); для OI/цены —
    # «имя · sectype».
    if a[1] == "funds_flow":
        title = a[3] or _FUND_ASSET_NAME.get(a[2], a[2])
    else:
        title = f"{a[3] or a[2]} · {a[2]}"
    txt = f"Удалить алерт «<b>{title}</b>»?\nЭто действие необратимо."
    kb = [
        [{"text": "🗑 Да, удалить", "callback_data": f"dx:{alert_id}"}],
        [{"text": "← Отмена", "callback_data": f"v:{alert_id}"}],
    ]
    return txt, kb


# ── мутации статуса (возвращают True/False) ─────────────────────────────────
def _set_status(db, user_id: int, alert_id: int, new_status: str) -> bool:
    res = db.execute(
        text("UPDATE alerts SET status = :s WHERE id = :a AND user_id = :u"),
        {"s": new_status, "a": alert_id, "u": user_id},
    )
    db.commit()
    return (res.rowcount or 0) > 0


def _resume_alert(db, user_id: int, alert_id: int) -> bool:
    """paused → active; для once в статусе fired — тоже снова active."""
    res = db.execute(
        text("UPDATE alerts SET status = 'active' "
             "WHERE id = :a AND user_id = :u AND status IN ('paused','fired')"),
        {"a": alert_id, "u": user_id},
    )
    db.commit()
    return (res.rowcount or 0) > 0


def _delete_alert(db, user_id: int, alert_id: int) -> bool:
    res = db.execute(
        text("DELETE FROM alerts WHERE id = :a AND user_id = :u"),
        {"a": alert_id, "u": user_id},
    )
    db.commit()
    return (res.rowcount or 0) > 0


def _view_delete_all_confirm(db, user_id: int):
    n = db.execute(
        text("SELECT count(*) FROM alerts WHERE user_id = :u"), {"u": user_id}
    ).scalar() or 0
    if n == 0:
        return ("У вас нет алертов.", [[{"text": "← Меню", "callback_data": "m"}]])
    txt = (f"Удалить <b>ВСЕ {n} алертов</b>?\n"
           f"Действие необратимо — удалятся все, и активные, и сработавшие.")
    kb = [
        [{"text": f"🗑 Да, удалить все ({n})", "callback_data": "dallx"}],
        [{"text": "← Отмена", "callback_data": "la"}],
    ]
    return txt, kb


def _delete_all_alerts(db, user_id: int) -> int:
    res = db.execute(
        text("DELETE FROM alerts WHERE user_id = :u"), {"u": user_id},
    )
    db.commit()
    return res.rowcount or 0


def _bulk_status(db, user_id: int, from_status: str, to_status: str) -> int:
    res = db.execute(
        text("UPDATE alerts SET status = :to WHERE user_id = :u AND status = :frm"),
        {"to": to_status, "frm": from_status, "u": user_id},
    )
    db.commit()
    return res.rowcount or 0


# ── callback_query dispatcher ───────────────────────────────────────────────
def process_callback(cb: dict) -> None:
    cb_id = cb.get("id")
    data = (cb.get("data") or "").strip()
    msg = cb.get("message") or {}
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    message_id = msg.get("message_id")
    if chat_id is None or message_id is None:
        if cb_id:
            answer_cb(cb_id)
        return

    db = SessionLocal()
    try:
        # help / m не требуют привязки — отдаём сразу
        if data == "help":
            edit_kb(chat_id, message_id, HELP_TEXT,
                    [[{"text": "← Меню", "callback_data": "m"}]])
            answer_cb(cb_id)
            return
        if data == "m":
            txt, kb = _view_menu()
            edit_kb(chat_id, message_id, txt, kb)
            answer_cb(cb_id)
            return

        user_id = _user_id_by_chat(db, chat_id)
        if not user_id:
            edit_kb(chat_id, message_id,
                    "Аккаунт не привязан. Откройте сайт → Личный кабинет → "
                    "«Подключить Telegram».",
                    [[{"text": "🌐 Открыть Фрейм", "url": SITE}]])
            answer_cb(cb_id)
            return

        if data == "la":
            # Корень «Мои алерты» — выбор индикатора (ОИ/Фонды).
            txt, kb = _view_list_root(db, user_id)
            edit_kb(chat_id, message_id, txt, kb)
            answer_cb(cb_id)
            return
        if data.startswith("ls:"):
            # ls:<source>[:<page>] — список раздела (с пагинацией).
            rest = data[3:]
            src, _, pg = rest.partition(":")
            try:
                page = int(pg) if pg else 0
            except ValueError:
                page = 0
            if src not in ("oi", "funds"):
                src = "oi"
            txt, kb = _view_list(db, user_id, src, page)
            edit_kb(chat_id, message_id, txt, kb)
            answer_cb(cb_id)
            return
        if data == "dall":
            txt, kb = _view_delete_all_confirm(db, user_id)
            edit_kb(chat_id, message_id, txt, kb)
            answer_cb(cb_id)
            return
        if data == "dallx":
            n = _delete_all_alerts(db, user_id)
            txt, kb = _view_list_root(db, user_id)
            edit_kb(chat_id, message_id, txt, kb)
            answer_cb(cb_id, f"Удалено {n} 🗑")
            return

        # параметризованные: <op>:<id>
        op, _, raw_id = data.partition(":")
        try:
            alert_id = int(raw_id)
        except ValueError:
            answer_cb(cb_id)
            return

        if op == "v":
            txt, kb = _view_card(db, user_id, alert_id)
            edit_kb(chat_id, message_id, txt, kb)
            answer_cb(cb_id)
        elif op == "p":
            ok = _set_status(db, user_id, alert_id, "paused")
            txt, kb = _view_card(db, user_id, alert_id)
            edit_kb(chat_id, message_id, txt, kb)
            answer_cb(cb_id, "На паузе ⏸" if ok else "Не найдено")
        elif op == "r":
            ok = _resume_alert(db, user_id, alert_id)
            txt, kb = _view_card(db, user_id, alert_id)
            edit_kb(chat_id, message_id, txt, kb)
            answer_cb(cb_id, "Активен 🟢" if ok else "Не найдено")
        elif op == "dq":
            txt, kb = _view_delete_confirm(db, user_id, alert_id)
            edit_kb(chat_id, message_id, txt, kb)
            answer_cb(cb_id)
        elif op == "dx":
            # Узнаём раздел ДО удаления, чтобы вернуться в нужный список.
            a = _get_alert(db, user_id, alert_id)
            src = _source_of(a[1]) if a else None
            ok = _delete_alert(db, user_id, alert_id)
            if src:
                txt, kb = _view_list(db, user_id, src)
            else:
                txt, kb = _view_list_root(db, user_id)
            edit_kb(chat_id, message_id, txt, kb)
            answer_cb(cb_id, "Удалено 🗑" if ok else "Не найдено")
        else:
            answer_cb(cb_id)
    except Exception as e:
        db.rollback()
        print(f"[alert_bot] callback error: {_redact(e)}")
        if cb_id:
            answer_cb(cb_id, "Ошибка, попробуйте ещё раз")
    finally:
        db.close()


def _handle_alerts(chat_id) -> None:
    """Корень «Мои алерты» (выбор раздела) — из /alerts И кнопки нижнего бара."""
    db = SessionLocal()
    try:
        user_id = _user_id_by_chat(db, chat_id)
        if not user_id:
            send_kb(chat_id,
                    "Аккаунт не привязан. Откройте сайт → Личный кабинет → "
                    "«Подключить Telegram».",
                    [[{"text": "🌐 Открыть Фрейм", "url": SITE}]])
        else:
            txt_list, kb = _view_list_root(db, user_id)
            send_kb(chat_id, txt_list, kb)
    finally:
        db.close()


def _handle_help(chat_id) -> None:
    """Помощь — из /help И кнопки нижнего бара."""
    send_kb(chat_id, HELP_TEXT, [[{"text": "← Меню", "callback_data": "m"}]])


def process_update(update: dict) -> None:
    if update.get("callback_query"):
        process_callback(update["callback_query"])
        return
    msg = update.get("message")
    if not msg:
        return
    chat_id = msg["chat"]["id"]
    username = msg["chat"].get("username")
    txt = (msg.get("text") or "").strip()

    # Тап по постоянной нижней клавиатуре приходит как ОБЫЧНОЕ текстовое сообщение
    # (равное подписи кнопки) — ловим до разбора команд.
    if txt in ("📋 Мои алерты", "Мои алерты"):
        _handle_alerts(chat_id)
        return
    if txt in ("❓ Помощь", "Помощь"):
        _handle_help(chat_id)
        return

    parts = txt.split(maxsplit=1)
    # /menu@framesignalbot → /menu (в группах TG добавляет @botname)
    cmd = parts[0].split("@", 1)[0].lower() if parts else ""

    if cmd == "/start":
        arg = parts[1].strip() if len(parts) == 2 else ""
        if arg:
            ok = link_account(arg, chat_id, username)
            if ok:
                # send_persistent ставит постоянный нижний бар (кнопки навсегда).
                send_persistent(chat_id,
                                 "✅ Аккаунт Фрейм подключён. Алерты будут приходить сюда.\n\n"
                                 + _welcome_text())
            else:
                send(chat_id,
                     "⚠️ Ссылка недействительна или истекла.\n"
                     "Сгенерируйте новую на сайте: ЛК → Подключить Telegram.")
        else:
            # /start без токена — приветствие + постоянный нижний бар.
            send_persistent(chat_id, _welcome_text())
    elif cmd == "/menu":
        send_persistent(chat_id, _welcome_text())
    elif cmd == "/help":
        _handle_help(chat_id)
    elif cmd == "/alerts":
        _handle_alerts(chat_id)
    elif cmd == "/stop":
        db = SessionLocal()
        try:
            user_id = _user_id_by_chat(db, chat_id)
            if not user_id:
                send(chat_id, "Аккаунт не привязан — нечего ставить на паузу.")
            else:
                n = _bulk_status(db, user_id, "active", "paused")
                if n:
                    send_kb(chat_id,
                            f"⏸ Поставил на паузу: {n}.\n"
                            "Вернуть всё в строй — командой /resume.",
                            [[{"text": "📋 Мои алерты", "callback_data": "la"}]])
                else:
                    send(chat_id, "Активных алертов не было — пауза не нужна.")
        finally:
            db.close()
    elif cmd == "/resume":
        db = SessionLocal()
        try:
            user_id = _user_id_by_chat(db, chat_id)
            if not user_id:
                send(chat_id, "Аккаунт не привязан — нечего возобновлять.")
            else:
                # paused И fired (сработавшие once) → active — единообразно с
                # кнопкой «Возобновить» в карточке (_resume_alert).
                res = db.execute(
                    text("UPDATE alerts SET status = 'active' WHERE user_id = :u "
                         "AND status IN ('paused','fired')"),
                    {"u": user_id})
                db.commit()
                n = res.rowcount or 0
                if n:
                    send_kb(chat_id, f"🟢 Снова активны: {n}.",
                            [[{"text": "📋 Мои алерты", "callback_data": "la"}]])
                else:
                    send(chat_id, "Не было алертов на паузе или сработавших.")
        finally:
            db.close()


def main() -> None:
    print(f"[{datetime.now(timezone.utc)}] alert_bot started")
    offset = None
    while True:
        try:
            # allowed_updates — JSON-массив (требование Telegram для GET-параметра).
            params = {"timeout": 30,
                      "allowed_updates": json.dumps(["message", "callback_query"])}
            if offset:
                params["offset"] = offset
            resp = requests.get(f"{API_BASE}/getUpdates", params=params, timeout=35)
            data = resp.json()
            if not data.get("ok"):
                time.sleep(5)
                continue
            for update in data["result"]:
                offset = update["update_id"] + 1
                try:
                    process_update(update)
                except Exception as e:
                    print(f"[alert_bot] update error: {_redact(e)}")
        except requests.RequestException as e:
            print(f"[alert_bot] polling error: {_redact(e)}")
            time.sleep(5)
        except Exception as e:
            print(f"[alert_bot] loop error: {_redact(e)}")
            time.sleep(5)


if __name__ == "__main__":
    main()
