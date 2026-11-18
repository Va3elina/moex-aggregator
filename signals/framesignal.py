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
    from signals import signal_marks, signal_prefs
    row = []
    if chart_url:
        # у сделок фондов и потоков ЦБ на сайте таблицы, а не график
        label = "Открыть на сайте" if (meta or {}).get("type") in ("fund_trades", "cbr_flows") else "Открыть график"
        row.append({"text": label, "url": chart_url, "icon_custom_emoji_id": signal_marks.CHART_ID})
    if meta:
        row.append({"text": "Настройки", "web_app": {"url": signal_prefs.settings_url(SITE, meta)},
                    "icon_custom_emoji_id": signal_marks.GEAR_ID})
    return json.dumps({"inline_keyboard": [row]}, ensure_ascii=False)


def rich_html(caption: str, photo: bool) -> str | None:
    """Подпись сигнала → разметка rich message (Вадим 18.11: история — настоящим сворачиваемым блоком Telegram, а не
    цитатой): картинка, текст абзацем, «История…» — <details> с таблицей. Блок истории в подписи — формат
    signals/signal_history.block: <blockquote expandable><b>Заголовок</b>\nшапка · …\nстрока · …\nподпись</blockquote>.
    Истории нет — None (обычное сообщение)."""
    import re
    m = re.search(r"\n?<blockquote expandable>(.*?)</blockquote>", caption, re.S)
    if not m:
        return None
    main = caption[:m.start()].strip().replace("\n", "<br/>")
    lines = m.group(1).split("\n")
    title = re.sub(r"</?b>", "", lines[0])
    head, rows, foot = None, [], []
    for ln in lines[1:]:
        cells = ln.split(" · ")
        if len(cells) > 1 and head is None:
            head = cells
        elif len(cells) > 1:
            rows.append(cells)
        else:
            foot.append(ln)
    from signals import signal_marks

    def cell(c: str) -> str:                         # рост/падение — значком: цвета текста в Telegram нет
        if c.endswith("%") and c[:1] == "+":
            return f"{signal_marks.UP} {c}"
        if c.endswith("%") and c[:1] == "−":
            return f"{signal_marks.DOWN} {c}"
        return c
    th = "".join(f"<th>{c}</th>" for c in head or [])
    tr = "".join("<tr>" + "".join(f'<td align="{"left" if k == 0 else "right"}">{cell(c) if k else c}</td>'
                                  for k, c in enumerate(r)) + "</tr>" for r in rows)
    table = f"<table striped compact><tr>{th}</tr>{tr}</table>" if rows else ""
    footer = "".join(f"<footer>{f}</footer>" for f in foot)
    img = '<img src="tg://photo?id=p0"/>' if photo else ""
    return f"{img}<p>{main}</p><details><summary>{title}</summary>{table}{footer}</details>"


def _no_icons(kb: str | None) -> str | None:
    """Клавиатура без значков на кнопках: если Telegram не примет icon_custom_emoji_id, кнопки останутся."""
    import json
    if not kb:
        return kb
    k = json.loads(kb)
    for row in k.get("inline_keyboard", []):
        for b in row:
            b.pop("icon_custom_emoji_id", None)
    return json.dumps(k, ensure_ascii=False)


def _send_rich(root: str, tok: str, chat: str, html_: str, png: bytes | None, kb: str | None, quiet: bool) -> bool:
    """sendRichMessage (Bot API 10.1+); с кнопками, не принял — без них. False — пусть идёт обычный путь."""
    import json
    rich = {"html": html_}
    files = None
    if png:
        rich["media"] = [{"id": "p0", "media": {"type": "photo", "media": "attach://p0"}}]
        files = {"p0": ("signal.png", png, "image/png")}
    data = {"chat_id": chat, "rich_message": json.dumps(rich, ensure_ascii=False), "disable_notification": quiet}
    for variant in ((kb, _no_icons(kb), None) if kb else (None,)):
        with_kb = variant is not None
        if with_kb:
            data["reply_markup"] = variant
        else:
            data.pop("reply_markup", None)
        try:
            r = requests.post(f"{root}/bot{tok}/sendRichMessage", timeout=(10, 40), data=data, files=files)
            if r.json().get("ok"):
                return True
            print(f"[framesignal] чат {chat}: rich не принят{' (с кнопками)' if with_kb else ''}: "
                  f"{r.text[:200].replace(tok, '<REDACTED>')}")
        except requests.ReadTimeout:
            # запрос ушёл, ответа нет — Telegram мог его принять: не повторяем, иначе у получателя 2–3 копии (ревью)
            print(f"[framesignal] чат {chat}: rich — нет ответа, считаю отправленным")
            return True
        except requests.RequestException as e:          # не дошли до Telegram/релея — дальше обычный путь
            print(f"[framesignal] чат {chat}: rich: {str(e).replace(tok, '<REDACTED>')}")
            return False
    return False


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
    rich = rich_html(text, photo=False)
    ok = False
    for chat, quiet in targets:
        for tok in toks:
            if rich and _send_rich(root, tok, chat, rich, None, kb, quiet):
                ok = True
                break
            try:
                data = {"chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True,
                        "disable_notification": quiet}
                if kb:
                    data["reply_markup"] = kb
                r = requests.post(f"{root}/bot{tok}/sendMessage", timeout=20, data=data)
                if not r.json().get("ok") and kb:          # сначала без значков на кнопках
                    data["reply_markup"] = _no_icons(kb)
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
    rich = rich_html(caption, photo=True)
    ok = False
    for chat, quiet in targets:
        for tok in toks:
            if rich and _send_rich(root, tok, chat, rich, png, kb, quiet):
                ok = True
                break
            try:
                data = {"chat_id": chat, "caption": caption, "parse_mode": "HTML", "disable_notification": quiet}
                if kb:
                    data["reply_markup"] = kb
                r = requests.post(f"{root}/bot{tok}/sendPhoto", timeout=60, data=data,
                                  files={"photo": ("signal.png", png, "image/png")})
                if not r.json().get("ok") and kb:          # сначала без значков на кнопках
                    data["reply_markup"] = _no_icons(kb)
                    r = requests.post(f"{root}/bot{tok}/sendPhoto", timeout=60, data=data,
                                      files={"photo": ("signal.png", png, "image/png")})
                use_kb = bool(kb)
                if not r.json().get("ok") and kb:
                    print(f"[{tag}] чат {chat}: фото с кнопками не принято, шлю без них: {r.text[:200].replace(tok, '<REDACTED>')}")
                    data.pop("reply_markup")
                    use_kb = False
                    r = requests.post(f"{root}/bot{tok}/sendPhoto", timeout=60, data=data,
                                      files={"photo": ("signal.png", png, "image/png")})
                if not r.json().get("ok"):                # картинку не взяли — сигнал уходит текстом, как раньше
                    print(f"[{tag}] чат {chat}: фото не принято, шлю текстом: {r.text[:200].replace(tok, '<REDACTED>')}")
                    r = requests.post(f"{root}/bot{tok}/sendMessage", timeout=20, data={
                        "chat_id": chat, "text": caption, "parse_mode": "HTML", "disable_web_page_preview": True,
                        "disable_notification": quiet, **({"reply_markup": kb} if use_kb else {})})
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
