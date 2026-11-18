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


def send(text: str, tag: str = "framesignal", bot: str = "signal", silent: bool = False) -> bool:
    """HTML-сообщение всем получателям; True — дошло хотя бы до одного. silent — без звука уведомления."""
    toks = [t for t in (os.environ.get(k, "").strip() for k in BOTS[bot]) if t]
    chats = [c.strip() for c in os.environ.get("ADMIN_CHAT_ID", "").split(",") if c.strip()]
    if not toks or not chats:
        print(f"[{tag}] нет токена бота {bot} или ADMIN_CHAT_ID — не отправлено")
        return False
    root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org").rstrip("/")
    ok = False
    for chat in chats:
        for tok in toks:
            try:
                r = requests.post(f"{root}/bot{tok}/sendMessage", timeout=20, data={
                    "chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True,
                    "disable_notification": silent})
                if r.json().get("ok"):
                    ok = True
                    break
                print(f"[{tag}] чат {chat}: не принято: {r.text[:200].replace(tok, '<REDACTED>')}")
            except requests.RequestException as e:
                print(f"[{tag}] чат {chat}: {str(e).replace(tok, '<REDACTED>')}")
    return ok


def send_photo(png: bytes, caption: str, tag: str = "framesignal", bot: str = "signal", silent: bool = False) -> bool:
    """Картинка с HTML-подписью всем получателям (подпись Telegram — до 1024 знаков); True — дошло хотя бы до одного."""
    toks = [t for t in (os.environ.get(k, "").strip() for k in BOTS[bot]) if t]
    chats = [c.strip() for c in os.environ.get("ADMIN_CHAT_ID", "").split(",") if c.strip()]
    if not toks or not chats:
        print(f"[{tag}] нет токена бота {bot} или ADMIN_CHAT_ID — не отправлено")
        return False
    root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org").rstrip("/")
    ok = False
    for chat in chats:
        for tok in toks:
            try:
                r = requests.post(f"{root}/bot{tok}/sendPhoto", timeout=60,
                                  data={"chat_id": chat, "caption": caption, "parse_mode": "HTML",
                                        "disable_notification": silent},
                                  files={"photo": ("signal.png", png, "image/png")})
                if r.json().get("ok"):
                    ok = True
                    break
                print(f"[{tag}] чат {chat}: фото не принято: {r.text[:200].replace(tok, '<REDACTED>')}")
            except requests.RequestException as e:
                print(f"[{tag}] чат {chat}: {str(e).replace(tok, '<REDACTED>')}")
    return ok


def send_slides(images: list, caption: str, tag: str = "framesignal", bot: str = "signal", silent: bool = False) -> bool:
    """Карусель: rich message со слайдшоу (sendRichMessage, Bot API 10.1+ — как посты @FrameTool: альбом
    sendMediaGroup Telegram показывает сеткой, а не каруселью). caption — HTML: строки через \\n, абзацы через \\n\\n.
    Не принял — запасной путь: альбом с подписью у первого фото. True — дошло хотя бы до одного получателя."""
    import json
    toks = [t for t in (os.environ.get(k, "").strip() for k in BOTS[bot]) if t]
    chats = [c.strip() for c in os.environ.get("ADMIN_CHAT_ID", "").split(",") if c.strip()]
    if not toks or not chats or not images:
        print(f"[{tag}] нет токена бота {bot}, ADMIN_CHAT_ID или картинок — не отправлено")
        return False
    root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org").rstrip("/")
    slides = "".join(f'<img src="tg://photo?id=p{i}"/>' for i in range(len(images)))
    # абзацы <p> в rich message идут вплотную — один <p>, абзацы через двойной <br/>
    body = "<p>" + caption.strip().replace("\n", "<br/>") + "</p>"
    rich = {"html": f"<tg-slideshow>{slides}</tg-slideshow>{body}",
            "media": [{"id": f"p{i}", "media": {"type": "photo", "media": f"attach://p{i}"}} for i in range(len(images))]}
    files = {f"p{i}": (f"p{i}.png", img, "image/png") for i, img in enumerate(images)}
    ok = False
    for chat in chats:
        for tok in toks:
            try:
                r = requests.post(f"{root}/bot{tok}/sendRichMessage", timeout=180, files=files,
                                  data={"chat_id": chat, "rich_message": json.dumps(rich, ensure_ascii=False),
                                        "disable_notification": silent})
                if r.json().get("ok"):
                    ok = True
                    break
                print(f"[{tag}] чат {chat}: карусель не принята: {r.text[:200].replace(tok, '<REDACTED>')}")
                media = [{"type": "photo", "media": f"attach://p{i}",
                          **({"caption": caption, "parse_mode": "HTML"} if i == 0 else {})} for i in range(len(images))]
                r = requests.post(f"{root}/bot{tok}/sendMediaGroup", timeout=180, files=files,
                                  data={"chat_id": chat, "media": json.dumps(media, ensure_ascii=False),
                                        "disable_notification": silent})
                if r.json().get("ok"):
                    ok = True
                    break
                print(f"[{tag}] чат {chat}: альбом не принят: {r.text[:200].replace(tok, '<REDACTED>')}")
            except requests.RequestException as e:
                print(f"[{tag}] чат {chat}: {str(e).replace(tok, '<REDACTED>')}")
    return ok
