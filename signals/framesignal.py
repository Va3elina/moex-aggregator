"""Служебные уведомления в личку админов (получатели — ADMIN_CHAT_ID через запятую, релей — TELEGRAM_API_ROOT).

Боты разнесены по назначению (Вадим 03.11.2026): bot="signal" — @framesignalbot (ALERT_BOT_TOKEN),
"editor" — FRAME · Редакция @frame_post_bot (поводы для постов), "duty" — FRAME · Дежурный @frameadminbot (BOT_TOKEN).
Если у бота нет токена или получатель не нажал «Старт» — сообщение уходит через следующий токен в списке,
чтобы ничего не терялось на переезде."""
import os

import requests
from dotenv import load_dotenv

SITE = "https://framedata.ru"

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))

BOTS = {
    "signal": ("ALERT_BOT_TOKEN",),
    "editor": ("EDITOR_BOT_TOKEN", "ALERT_BOT_TOKEN"),
    "duty": ("BOT_TOKEN", "ALERT_BOT_TOKEN"),
}


def _targets(bot: str, silent: bool, meta: dict | None) -> list:
    """[(чат, без звука)] — получатели с учётом их настроек сигналов (signals/signal_prefs): у кого вид выключен,
    актив скрыт или окно короче порога — не шлём; в тихие часы — без звука. meta нет (служебные сообщения) — всем."""
    chats = [c.strip() for c in os.environ.get("ADMIN_CHAT_ID", "").split(",") if c.strip()]
    if not meta:
        return [(c, silent) for c in chats]
    from signals import signal_prefs
    prefs = signal_prefs.load(chats)
    out = []
    for c in chats:
        d = signal_prefs.decide(prefs.get(c), meta)
        if d is None:
            print(f"[framesignal] чат {c}: {meta.get('type')} {meta.get('asset') or ''} отключён в настройках")
            continue
        out.append((c, silent or d == "silent"))
    return out


def _markup(meta: dict | None, chart_url: str | None) -> str | None:
    """Кнопки под сигналом (Вадим 18.11): «Открыть график» (страница сайта) и «Настройки» (мини-приложение на
    экране сигналов, сразу с предложением скрыть актив из сигнала)."""
    if not meta and not chart_url:
        return None
    import json
    from signals import signal_prefs
    row = []
    if chart_url:
        # у сделок фондов и потоков ЦБ на сайте таблицы, а не график
        label = "Открыть на сайте" if (meta or {}).get("type") in ("fund_trades", "cbr_flows") else "Открыть график"
        row.append({"text": label, "url": chart_url})
    if meta:
        row.append({"text": "Настройки", "web_app": {"url": signal_prefs.settings_url(SITE, meta)}})
    return json.dumps({"inline_keyboard": [row]}, ensure_ascii=False)


def send(text: str, tag: str = "framesignal", bot: str = "signal", silent: bool = False,
         meta: dict | None = None, chart_url: str | None = None) -> bool:
    """HTML-сообщение всем получателям; True — дошло хотя бы до одного. silent — без звука уведомления.
    meta — вид сигнала и актив (фильтр настроек и кнопка «Настройки»), chart_url — кнопка «Открыть график»."""
    toks = [t for t in (os.environ.get(k, "").strip() for k in BOTS[bot]) if t]
    targets = _targets(bot, silent, meta)
    if not toks or not os.environ.get("ADMIN_CHAT_ID", "").strip():
        print(f"[{tag}] нет токена бота {bot} или ADMIN_CHAT_ID — не отправлено")
        return False
    if not targets:
        return True                       # все получатели этот сигнал отключили — это не сбой
    root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org").rstrip("/")
    kb = _markup(meta, chart_url)
    ok = False
    for chat, quiet in targets:
        for tok in toks:
            try:
                data = {"chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True,
                        "disable_notification": quiet}
                if kb:
                    data["reply_markup"] = kb
                r = requests.post(f"{root}/bot{tok}/sendMessage", timeout=20, data=data)
                if not r.json().get("ok") and kb:          # кнопки не приняты — сигнал важнее кнопок
                    print(f"[{tag}] чат {chat}: с кнопками не принято, шлю без них: {r.text[:200].replace(tok, '<REDACTED>')}")
                    data.pop("reply_markup")
                    r = requests.post(f"{root}/bot{tok}/sendMessage", timeout=20, data=data)
                if r.json().get("ok"):
                    ok = True
                    break
                print(f"[{tag}] чат {chat}: не принято: {r.text[:200].replace(tok, '<REDACTED>')}")
            except requests.RequestException as e:
                print(f"[{tag}] чат {chat}: {str(e).replace(tok, '<REDACTED>')}")
    return ok


def send_photo(png: bytes, caption: str, tag: str = "framesignal", bot: str = "signal", silent: bool = False,
               meta: dict | None = None, chart_url: str | None = None) -> bool:
    """Картинка с HTML-подписью всем получателям (подпись Telegram — до 1024 знаков); True — дошло хотя бы до одного."""
    toks = [t for t in (os.environ.get(k, "").strip() for k in BOTS[bot]) if t]
    targets = _targets(bot, silent, meta)
    if not toks or not os.environ.get("ADMIN_CHAT_ID", "").strip():
        print(f"[{tag}] нет токена бота {bot} или ADMIN_CHAT_ID — не отправлено")
        return False
    if not targets:
        return True
    root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org").rstrip("/")
    kb = _markup(meta, chart_url)
    ok = False
    for chat, quiet in targets:
        for tok in toks:
            try:
                data = {"chat_id": chat, "caption": caption, "parse_mode": "HTML", "disable_notification": quiet}
                if kb:
                    data["reply_markup"] = kb
                r = requests.post(f"{root}/bot{tok}/sendPhoto", timeout=60, data=data,
                                  files={"photo": ("signal.png", png, "image/png")})
                if not r.json().get("ok") and kb:
                    print(f"[{tag}] чат {chat}: фото с кнопками не принято, шлю без них: {r.text[:200].replace(tok, '<REDACTED>')}")
                    data.pop("reply_markup")
                    r = requests.post(f"{root}/bot{tok}/sendPhoto", timeout=60, data=data,
                                      files={"photo": ("signal.png", png, "image/png")})
                if not r.json().get("ok"):                # картинку не взяли — сигнал уходит текстом, как раньше
                    print(f"[{tag}] чат {chat}: фото не принято, шлю текстом: {r.text[:200].replace(tok, '<REDACTED>')}")
                    r = requests.post(f"{root}/bot{tok}/sendMessage", timeout=20, data={
                        "chat_id": chat, "text": caption, "parse_mode": "HTML", "disable_web_page_preview": True,
                        "disable_notification": quiet, **({"reply_markup": kb} if kb else {})})
                if r.json().get("ok"):
                    ok = True
                    break
                print(f"[{tag}] чат {chat}: не принято: {r.text[:200].replace(tok, '<REDACTED>')}")
            except requests.RequestException as e:
                print(f"[{tag}] чат {chat}: {str(e).replace(tok, '<REDACTED>')}")
    return ok


def send_slides(images: list, caption: str, tag: str = "framesignal", bot: str = "signal", silent: bool = False,
                meta: dict | None = None, chart_url: str | None = None) -> bool:
    """Карусель: rich message со слайдшоу (sendRichMessage, Bot API 10.1+ — как посты @FrameTool: альбом
    sendMediaGroup Telegram показывает сеткой, а не каруселью). caption — HTML: строки через \\n, абзацы через \\n\\n.
    Не принял — запасной путь: альбом с подписью у первого фото. True — дошло хотя бы до одного получателя."""
    import json
    toks = [t for t in (os.environ.get(k, "").strip() for k in BOTS[bot]) if t]
    targets = _targets(bot, silent, meta)
    if not toks or not os.environ.get("ADMIN_CHAT_ID", "").strip() or not images:
        print(f"[{tag}] нет токена бота {bot}, ADMIN_CHAT_ID или картинок — не отправлено")
        return False
    if not targets:
        return True
    kb = _markup(meta, chart_url)
    root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org").rstrip("/")
    slides = "".join(f'<img src="tg://photo?id=p{i}"/>' for i in range(len(images)))
    # абзацы <p> в rich message идут вплотную — один <p>, абзацы через двойной <br/>
    body = "<p>" + caption.strip().replace("\n", "<br/>") + "</p>"
    rich = {"html": f"<tg-slideshow>{slides}</tg-slideshow>{body}",
            "media": [{"id": f"p{i}", "media": {"type": "photo", "media": f"attach://p{i}"}} for i in range(len(images))]}
    files = {f"p{i}": (f"p{i}.png", img, "image/png") for i, img in enumerate(images)}
    ok = False
    for chat, quiet in targets:
        for tok in toks:
            try:
                data = {"chat_id": chat, "rich_message": json.dumps(rich, ensure_ascii=False), "disable_notification": quiet}
                if kb:
                    data["reply_markup"] = kb
                r = requests.post(f"{root}/bot{tok}/sendRichMessage", timeout=180, files=files, data=data)
                if not r.json().get("ok") and kb:
                    print(f"[{tag}] чат {chat}: карусель с кнопками не принята, шлю без них: {r.text[:200].replace(tok, '<REDACTED>')}")
                    data.pop("reply_markup")
                    r = requests.post(f"{root}/bot{tok}/sendRichMessage", timeout=180, files=files, data=data)
                if r.json().get("ok"):
                    ok = True
                    break
                print(f"[{tag}] чат {chat}: карусель не принята: {r.text[:200].replace(tok, '<REDACTED>')}")
                # альбом кнопок не несёт — ссылку на сайт кладём в подпись
                cap = caption + (f'\n\n<a href="{chart_url}">Открыть на сайте →</a>' if chart_url else "")
                media = [{"type": "photo", "media": f"attach://p{i}",
                          **({"caption": cap, "parse_mode": "HTML"} if i == 0 else {})} for i in range(len(images))]
                r = requests.post(f"{root}/bot{tok}/sendMediaGroup", timeout=180, files=files,
                                  data={"chat_id": chat, "media": json.dumps(media, ensure_ascii=False),
                                        "disable_notification": quiet})
                if r.json().get("ok"):
                    ok = True
                    break
                print(f"[{tag}] чат {chat}: альбом не принят: {r.text[:200].replace(tok, '<REDACTED>')}")
            except requests.RequestException as e:
                print(f"[{tag}] чат {chat}: {str(e).replace(tok, '<REDACTED>')}")
    return ok
