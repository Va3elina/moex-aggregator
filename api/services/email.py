# api/services/email.py
"""
Отправка транзакционных писем через SMTP (Yandex 360).

Используется stdlib smtplib (SSL, порт 465) — без внешних зависимостей.
Функции синхронные; вызывать их надо через FastAPI BackgroundTasks
(FastAPI выполнит sync-таску в threadpool, не блокируя event loop gunicorn/uvicorn).

Конфиг через .env (см. database.py — load_dotenv грузит /opt/frame/.env):
  SMTP_HOST       smtp.yandex.ru
  SMTP_PORT       465
  SMTP_USER       noreply@framedata.ru
  SMTP_PASSWORD   <пароль приложения Yandex ID>
  SMTP_FROM       (опц.) адрес в From, по умолчанию = SMTP_USER
  SMTP_FROM_NAME  (опц.) display-name отправителя, по умолчанию "FRAME"

Почта переехала на framedata.ru 12.08.2026; ящик тот же, прежний адрес
noreply@xn--80aklbnczmv.xn--p1ai остаётся вторым адресом того же ящика.
ВАЖНО: адрес в SMTP AUTH и в From держать в ASCII — кириллический
таймфрейм.рф проходит не везде, для него нужна punycode-форма.
"""

import os
import ssl
import smtplib
import logging
from pathlib import Path
from email.message import EmailMessage

log = logging.getLogger("moex_api")

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.yandex.ru")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM = os.getenv("SMTP_FROM", "") or SMTP_USER
SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", "FRAME")

SITE_URL = "https://framedata.ru"

# Логотип в шапке письма. Вшивается ВЛОЖЕНИЕМ (cid:logo), а не ссылкой на сайт:
# почтовики по умолчанию режут внешние картинки, и шапка приезжала бы пустой.
# PNG, не SVG — векторные картинки почтовые клиенты не рисуют.
# Пересобрать после правки frontend/public/logo.svg:
#   rsvg-convert -w 96 -h 96 frontend/public/logo.svg -o api/services/assets/logo-email.png
LOGO_PATH = Path(__file__).parent / "assets" / "logo-email.png"

# Шапка писем — общая для всех шаблонов, чтобы бренд не разъезжался по файлу.
#
# ⚠️ Одна строка, обе ячейки выровнены по ВЕРХУ, а line-height названия равен
# высоте логотипа (28px) — так их центры совпадают в любом почтовом клиенте.
# vertical-align:middle здесь не годится: он считает высоту по всей ячейке, и
# логотип уезжает вниз, стоит появиться второй строке текста.
EMAIL_HEADER = """\
        <tr><td style="padding:32px 32px 8px;">
          <table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
            <td width="28" style="padding-right:12px;vertical-align:top;">
              <img src="cid:logo" width="28" height="28" alt=""
                   style="display:block;width:28px;height:28px;border:0;">
            </td>
            <td style="vertical-align:top;">
              <div style="font-size:22px;line-height:28px;font-weight:800;color:#15110B;letter-spacing:0.02em;">FRAME</div>
            </td>
          </tr></table>
        </td></tr>"""


def is_configured() -> bool:
    """True если SMTP настроен (есть логин и пароль). Пока пароль не вписан
    в .env — отправка просто логируется и пропускается, регистрация не падает."""
    return bool(SMTP_USER and SMTP_PASSWORD)


def _send(to_email: str, subject: str, text_body: str, html_body: str) -> bool:
    """Низкоуровневая отправка. Возвращает True/False, не бросает наружу."""
    if not is_configured():
        log.warning(
            "SMTP не настроен (SMTP_USER/SMTP_PASSWORD пусты) — письмо на %s пропущено",
            to_email,
        )
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{SMTP_FROM_NAME} <{SMTP_FROM}>"
    msg["To"] = to_email
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    # Логотип цепляем ТОЛЬКО к html-части (add_related на самом msg сломал бы
    # multipart/alternative — text и html перестали бы быть равноправными
    # вариантами одного письма).
    if "cid:logo" in html_body and LOGO_PATH.exists():
        msg.get_payload()[-1].add_related(
            LOGO_PATH.read_bytes(), maintype="image", subtype="png", cid="<logo>"
        )

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx, timeout=20) as smtp:
            smtp.login(SMTP_USER, SMTP_PASSWORD)
            smtp.send_message(msg)
        log.info("Email отправлен на %s (subject=%r)", to_email, subject)
        return True
    except Exception as e:  # noqa: BLE001 — не роняем регистрацию из-за SMTP
        log.error("Ошибка отправки email на %s: %s: %s", to_email, type(e).__name__, e)
        return False


def send_verification_email(to_email: str, code: str, display_name: str | None = None) -> bool:
    """Письмо с 6-значным кодом подтверждения email."""
    subject = f"Код подтверждения: {code} — FRAME"
    greeting = f"Здравствуйте, {display_name}!" if display_name else "Здравствуйте!"

    text_body = (
        f"{greeting}\n\n"
        f"Ваш код подтверждения email на FRAME:\n\n"
        f"    {code}\n\n"
        f"Код действует 30 минут.\n\n"
        f"Если вы не регистрировались на {SITE_URL}, просто проигнорируйте это письмо.\n\n"
        f"— FRAME\n{SITE_URL}\n"
    )

    html_body = f"""\
<!doctype html>
<html lang="ru"><body style="margin:0;padding:0;background:#0B0D12;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#0B0D12;padding:32px 16px;">
    <tr><td align="center">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
             style="max-width:480px;background:#F4F1EA;border-radius:16px;overflow:hidden;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;">
{EMAIL_HEADER}
        <tr><td style="padding:8px 32px 0;">
          <p style="font-size:15px;color:#2A241B;margin:16px 0 4px;">{greeting}</p>
          <p style="font-size:15px;color:#2A241B;margin:0 0 20px;">Ваш код подтверждения email:</p>
        </td></tr>
        <tr><td style="padding:0 32px;">
          <div style="background:#15110B;border-radius:12px;padding:20px;text-align:center;">
            <span style="font-family:'JetBrains Mono','IBM Plex Mono',ui-monospace,monospace;font-size:34px;font-weight:700;letter-spacing:10px;color:#FF5C2B;">{code}</span>
          </div>
        </td></tr>
        <tr><td style="padding:20px 32px 0;">
          <p style="font-size:13px;color:#6B6357;margin:0;">Код действует 30&nbsp;минут. Если вы не регистрировались на framedata.ru — просто проигнорируйте это письмо.</p>
        </td></tr>
        <tr><td style="padding:24px 32px 32px;">
          <hr style="border:none;border-top:1px solid #DAD3C6;margin:0 0 16px;">
          <a href="{SITE_URL}" style="font-size:13px;color:#FF5C2B;text-decoration:none;">framedata.ru</a>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>"""

    return _send(to_email, subject, text_body, html_body)


def send_trial_ending_email(
    to_email: str,
    tier_ru: str,
    amount: float,
    charge_date: str,
    display_name: str | None = None,
) -> bool:
    """T-1 уведомление: пробный период заканчивается, скоро автосписание.

    amount — сумма в рублях; charge_date — уже отформатированная дата («30 июня»).
    Best practice (не буква 376-ФЗ): прозрачно предупреждаем о сумме/дате + как
    отменить (ЗоЗПП — простая отмена). Ссылка ведёт в профиль (отмена + отвязка карты).
    """
    amount_str = "{:,.0f}".format(amount).replace(",", " ")
    greeting = f"Здравствуйте, {display_name}!" if display_name else "Здравствуйте!"
    subject = f"Пробный период скоро закончится — {charge_date} спишем {amount_str} ₽"
    profile_url = f"{SITE_URL}/profile"

    text_body = (
        f"{greeting}\n\n"
        f"Ваш бесплатный пробный период тарифа {tier_ru} заканчивается {charge_date}.\n"
        f"В этот день мы автоматически спишем {amount_str} ₽ за выбранный план, и "
        f"подписка станет платной.\n\n"
        f"Если не хотите продолжать — отмените подписку и отвяжите карту в профиле "
        f"до {charge_date}, тогда списания не будет:\n{profile_url}\n\n"
        f"— FRAME\n{SITE_URL}\n"
    )

    html_body = f"""\
<!doctype html>
<html lang="ru"><body style="margin:0;padding:0;background:#0B0D12;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#0B0D12;padding:32px 16px;">
    <tr><td align="center">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
             style="max-width:480px;background:#F4F1EA;border-radius:16px;overflow:hidden;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;">
{EMAIL_HEADER}
        <tr><td style="padding:8px 32px 0;">
          <p style="font-size:15px;color:#2A241B;margin:16px 0 4px;">{greeting}</p>
          <p style="font-size:15px;color:#2A241B;margin:0 0 12px;line-height:1.5;">
            Ваш бесплатный пробный период тарифа <b>{tier_ru}</b> заканчивается
            <b>{charge_date}</b>. В этот день мы автоматически спишем
            <b>{amount_str}&nbsp;₽</b> за выбранный план.
          </p>
        </td></tr>
        <tr><td style="padding:0 32px;">
          <div style="background:#15110B;border-radius:12px;padding:18px 20px;">
            <span style="font-size:15px;color:#F4F1EA;">Списание {charge_date}:</span>
            <span style="font-size:22px;font-weight:700;color:#FF5C2B;float:right;">{amount_str}&nbsp;₽</span>
          </div>
        </td></tr>
        <tr><td style="padding:18px 32px 0;">
          <p style="font-size:13px;color:#6B6357;margin:0 0 16px;line-height:1.5;">
            Не хотите продолжать? Отмените подписку и отвяжите карту в профиле до
            {charge_date} — списания не будет.
          </p>
          <a href="{profile_url}" style="display:inline-block;background:#FF5C2B;color:#fff;text-decoration:none;font-size:14px;font-weight:700;padding:12px 22px;border-radius:10px;">Управление подпиской</a>
        </td></tr>
        <tr><td style="padding:24px 32px 32px;">
          <hr style="border:none;border-top:1px solid #DAD3C6;margin:0 0 16px;">
          <a href="{SITE_URL}" style="font-size:13px;color:#FF5C2B;text-decoration:none;">framedata.ru</a>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>"""

    return _send(to_email, subject, text_body, html_body)


def send_password_reset_email(to_email: str, code: str, display_name: str | None = None) -> bool:
    """Письмо с 6-значным кодом для восстановления пароля.

    Отдельный шаблон, а не переиспользование письма верификации: коды разные
    по смыслу, и человек должен понимать, что именно он подтверждает. Плюс
    здесь обязательна строчка «если это не вы» — письмо может прийти тому,
    чей адрес просто ввели в форму.
    """
    subject = f"Код для смены пароля: {code} — FRAME"
    greeting = f"Здравствуйте, {display_name}!" if display_name else "Здравствуйте!"

    text_body = (
        f"{greeting}\n\n"
        f"Вы запросили восстановление пароля на FRAME.\n"
        f"Код для смены пароля:\n\n"
        f"    {code}\n\n"
        f"Код действует 30 минут.\n\n"
        f"Если вы не запрашивали смену пароля — просто проигнорируйте это письмо, "
        f"пароль останется прежним.\n\n"
        f"— FRAME\n{SITE_URL}\n"
    )

    html_body = f"""\
<!doctype html>
<html lang="ru"><body style="margin:0;padding:0;background:#0B0D12;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#0B0D12;padding:32px 16px;">
    <tr><td align="center">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
             style="max-width:480px;background:#F4F1EA;border-radius:16px;overflow:hidden;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;">
{EMAIL_HEADER}
        <tr><td style="padding:8px 32px 0;">
          <p style="font-size:15px;color:#2A241B;margin:16px 0 4px;">{greeting}</p>
          <p style="font-size:15px;color:#2A241B;margin:0 0 20px;">Код для смены пароля:</p>
        </td></tr>
        <tr><td style="padding:0 32px;">
          <div style="background:#15110B;border-radius:12px;padding:20px;text-align:center;">
            <span style="font-family:'JetBrains Mono','IBM Plex Mono',ui-monospace,monospace;font-size:34px;font-weight:700;letter-spacing:10px;color:#FF5C2B;">{code}</span>
          </div>
        </td></tr>
        <tr><td style="padding:20px 32px 0;">
          <p style="font-size:13px;color:#6B6357;margin:0;line-height:1.5;">
            Код действует 30&nbsp;минут. Если вы не запрашивали смену пароля — просто
            проигнорируйте это письмо, пароль останется прежним.
          </p>
        </td></tr>
        <tr><td style="padding:24px 32px 32px;">
          <hr style="border:none;border-top:1px solid #DAD3C6;margin:0 0 16px;">
          <a href="{SITE_URL}" style="font-size:13px;color:#FF5C2B;text-decoration:none;">framedata.ru</a>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>"""

    return _send(to_email, subject, text_body, html_body)
