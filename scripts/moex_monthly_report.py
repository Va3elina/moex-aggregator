#!/usr/bin/env python3
"""
Ежемесячный отчёт Бирже по п.1.2.3 Приложения №3 к Условиям оказания услуг ИТО.

Срок сдачи — 3 календарных дня со дня окончания отчётного месяца (резидент РФ).

Отчёт по составу почти целиком нулевой: пункты «а»–«г» описывают Подписчиков,
Пунктов доступа и по-клиентские платежи, а они существуют только у того, кто
раздаёт Информационный поток в режиме реального времени или Фиксинги. Мы их не
раздаём.

Поэтому скрипт не просто печатает шаблон — он ПРОВЕРЯЕТ, что предпосылка ещё
верна, и отказывается подтверждать нули, если в системе появилось то, что их
ломает: включённый публичный API, живые встроечные токены, признаки
ре-дистрибуции. Молча уехавший нулевой отчёт при изменившемся продукте — это
недостоверные сведения Бирже, а не экономия времени.

Запуск (локально, с DB_URL на прод-реплику или через SSH-туннель):
    python3 scripts/moex_monthly_report.py                # прошлый месяц
    python3 scripts/moex_monthly_report.py --month 2026-08
    python3 scripts/moex_monthly_report.py --month 2026-08 --out report.md
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

try:
    from sqlalchemy import create_engine, text
except ImportError:
    sys.exit("нужен sqlalchemy: pip install sqlalchemy pg8000")


CLIENT = "ИП Тория Александр Роландович, ОГРНИП 325784700029296, ИНН 782627792630"
SITES = "https://framedata.ru (прежние адреса https://таймфрейм.рф и https://xn--80aklbnczmv.xn--p1ai)"


def prev_month(today: date) -> tuple[int, int]:
    """Год и месяц предыдущего календарного месяца."""
    return (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)


def month_bounds(year: int, month: int) -> tuple[date, date]:
    """Первый день месяца и первый день следующего — полуинтервал [start, end)."""
    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start, end


def collect(engine, start: date, end: date) -> dict:
    """Факты, от которых зависит достоверность нулей в пунктах «а»–«г».

    Каждый запрос обёрнут: отсутствие таблицы — не повод падать (схема на
    локальной копии может отставать), но и не повод молча считать ноль. Такое
    поле уезжает в отчёт как None и превращается в предупреждение.
    """
    def scalar(sql: str, **params):
        try:
            with engine.connect() as conn:
                return conn.execute(text(sql), params).scalar()
        except Exception as e:              # noqa: BLE001 — намеренно широкий
            print(f"  ! запрос не выполнен ({e.__class__.__name__}): {sql.strip()[:60]}…",
                  file=sys.stderr)
            return None

    return {
        # Встроечные токены = ближайшее к «предоставлению третьим лицам».
        # Активные токены сами по себе нарушением не являются, но отчёт с ними
        # уже не про пустой периметр — нужен глаз.
        # Отзыв помечается флагом is_revoked; revoked_at — только отметка
        # времени и может пустовать у старых записей, поэтому фильтр по флагу.
        "embed_tokens_active": scalar(
            "SELECT count(*) FROM extension_tokens WHERE NOT is_revoked"
        ),
        # Ключи публичного API. Их наличие при выключенном флаге безвредно,
        # но означает, что кто-то готовился включать.
        "api_keys_active": scalar(
            "SELECT count(*) FROM api_keys WHERE NOT is_revoked"
        ),
        # Контекст для сопроводительного письма, в сам отчёт не входит:
        # Пользователи Подписчиками не являются и в п.1.2.3 не перечисляются.
        "users_total": scalar("SELECT count(*) FROM users"),
        "users_paid": scalar(
            "SELECT count(DISTINCT user_id) FROM subscriptions "
            "WHERE status = 'active' AND created_at < :end",
            end=end,
        ),
    }


def render(year: int, month: int, facts: dict, warnings: list[str]) -> str:
    start, end = month_bounds(year, month)
    last_day = date.fromordinal(end.toordinal() - 1)

    return f"""# Отчёт по п.1.2.3 Приложения №3 к Условиям оказания услуг ИТО

**Клиент:** {CLIENT}
**Отчётный месяц:** {start:%m.%Y} (составлен на {last_day:%d.%m.%Y})
**Сайты:** {SITES}

## а) Подписчики, Сервисные партнёры / Технические агенты, Ре-дистрибьюторы

Отсутствуют. Информационный поток в режиме реального времени, Фиксинги Валютного
рынка Московской Биржи и Индексные отчёты в отчётном месяце не предоставлялись,
договоры о их получении не заключались. Пункты доступа не заводились.

## б) Пункты доступа для Внутреннего использования и Временных подписчиков

0. Пункт доступа — учётная единица для Информационного потока в режиме реального
времени и Фиксингов; ни то, ни другое не предоставляется. Временных подписчиков
в отчётном месяце не было.

## в) Ре-дистрибьюторы, адреса их сайтов и названия продуктов

Отсутствуют.

## г) Сумма переменной платы (по-клиентские платежи)

0 руб. Начисляется только за Информационный поток в режиме реального времени,
Фиксинги и Индексные отчёты — не предоставлялись.

## д) Суммы платежей за Биржевую информацию (кроме реального времени)

Ре-дистрибьюторов нет, дополнительная плата за предоставление им информации
не начисляется. Абонентская плата — по Тарифам согласно Заявлению.

> ЗАПОЛНИТЬ: подставить фактическую ставку из Тарифов по заказанному
> Информационному продукту.

## е) Иные платежи по Тарифам

Учебная информация не заказывалась и не предоставлялась, фиксированный платёж
за неё не начисляется.

## ж) Общая сумма за отчётный период

> ЗАПОЛНИТЬ: сумма пунктов «д» и «е».

---

## Служебная часть (Бирже не отправляется)

Проверка предпосылок, на которых держатся нули выше:

| Показатель | Значение |
|---|---|
| Активные встроечные токены | {facts['embed_tokens_active']} |
| Активные ключи публичного API | {facts['api_keys_active']} |
| Зарегистрированных пользователей | {facts['users_total']} |
| Из них с активной подпиской | {facts['users_paid']} |

Пользователи Подписчиками не являются: они получают Задержанный информационный
поток и Итоги торгов в ознакомительных целях, договор с ними — Пользовательское
соглашение, Пункты доступа для них не заводятся. В п.1.2.3 они не перечисляются
и приведены только для контекста.

{chr(10).join(warnings) if warnings else '✅ Предпосылки в порядке, отчёт можно отправлять как есть.'}
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--month", help="Отчётный месяц YYYY-MM (по умолчанию прошлый)")
    ap.add_argument("--out", help="Записать в файл вместо stdout")
    args = ap.parse_args()

    if args.month:
        try:
            year, month = (int(p) for p in args.month.split("-", 1))
            date(year, month, 1)                       # валидация
        except (ValueError, TypeError):
            return print("--month ожидает YYYY-MM, например 2026-08", file=sys.stderr) or 2
    else:
        year, month = prev_month(date.today())

    db_url = os.getenv("DB_URL")
    if not db_url:
        return print("нужен DB_URL (см. api/database.py)", file=sys.stderr) or 2

    start, end = month_bounds(year, month)
    print(f"Отчётный период: {start} … {end} (не включая)", file=sys.stderr)

    engine = create_engine(db_url)
    facts = collect(engine, start, end)

    warnings: list[str] = []
    if facts["embed_tokens_active"]:
        warnings.append(
            f"⚠️ Активных встроечных токенов: {facts['embed_tokens_active']}. "
            "Проверить, не превратилась ли выдача встроек в предоставление "
            "информации третьим лицам для дальнейшего распространения — тогда "
            "пункты «а» и «в» перестают быть пустыми."
        )
    if facts["api_keys_active"]:
        warnings.append(
            f"⚠️ Активных ключей публичного API: {facts['api_keys_active']}. "
            "Если PUBLIC_API_ENABLED включён, Пользователи получили машинный "
            "доступ к данным — это отдельный разговор с Биржей до отправки отчёта."
        )
    missing = [k for k, v in facts.items() if v is None]
    if missing:
        warnings.append(
            f"⚠️ Не удалось проверить: {', '.join(missing)}. Нули в отчёте не "
            "подтверждены данными — сверить вручную перед отправкой."
        )

    report = render(year, month, facts, warnings)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"Записано: {args.out}", file=sys.stderr)
    else:
        print(report)

    for w in warnings:
        print(w, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
