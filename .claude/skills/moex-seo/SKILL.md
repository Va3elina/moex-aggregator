---
name: moex-seo
description: SEO-пульт Фрейма через Яндекс.Вебмастер + Google Search Console API. Use when user asks «проверь индексацию», «какие у нас позиции в яндексе/гугле», «сколько страниц в индексе», «отправь на переобход», «сабмить sitemap», «запросы из вебмастера», «выросли ли мы в поиске», «сравни яндекс и гугл», или для любой SEO-аналитики/мониторинга/диагностики. Оборачивает scripts/seo/seo.py (оба движка одной командой).
---

# Фрейм SEO — Яндекс.Вебмастер + Google Search Console

Один CLI для контроля индексации и позиций по обоим поисковикам, чтобы не лазить
руками в кабинеты. Код: `scripts/seo/seo.py`.

## Секреты (оба gitignored — НЕ коммитить)
- `scripts/seo/.env` — Яндекс: `YANDEX_WEBMASTER_TOKEN` (OAuth, ~1 год),
  `YANDEX_WEBMASTER_USER_ID`, `YANDEX_WEBMASTER_HOST_ID`, `YANDEX_OAUTH_CLIENT_ID`.
- `scripts/seo/gsc-service-account.json` — Google сервисный аккаунт
  (`frame-seo-bot@frame-490320.iam.gserviceaccount.com`, проект `frame-490320`).

Перевыпуск Яндекс-токена при `INVALID_OAUTH_TOKEN` (под аккаунтом, где сайт в
Вебмастере — у Вадима их несколько!): открыть в браузере
`https://oauth.yandex.ru/authorize?response_type=token&client_id=<YANDEX_OAUTH_CLIENT_ID>`
→ «Разрешить» → скопировать весь `access_token` в `.env`.

## Команды
```bash
python3 scripts/seo/seo.py status                 # страниц в индексе, ИКС, клики/показы — оба
python3 scripts/seo/seo.py queries --days 28      # топ-запросы с позициями (--engine yandex|google|both)
python3 scripts/seo/seo.py compare                # бренд/тема: позиция в Я vs G рядом
python3 scripts/seo/seo.py diagnostics            # активные проблемы Яндекса (SOFT_404 и т.п.)
python3 scripts/seo/seo.py recrawl /heatmap /oi   # Яндекс переобход (квота 150/день)
python3 scripts/seo/seo.py sitemap-submit         # сабмит /sitemap.xml в оба
```

## Когда что звать
- «выросли ли мы / какие позиции» → `status` + `queries`.
- «почему страница не индексируется / ошибки» → `diagnostics`; для Google смотреть
  покрытие в GSC UI (v3 API не отдаёт «исключённые» детально).
- после деплоя контента/мета → `recrawl` ключевых URL (Яндекс) + `sitemap-submit`.
- сравнить движки → `compare`.

## ⚠️ Подводные камни
- **curl/скрипт НЕ звать внутри zsh `for`-цикла** в Bash-туле — ломается
  (`command not found: curl`, `(eval)`). У CLI циклы внутри Python — звать как есть.
- Google `searchAnalytics` имеет задержку ~2–3 дня; у молодой property данных мало.
- Яндекс host_id (`https:домен:443`) при ручных curl кодировать `:`→`%3A`.

## РЕШЕНИЕ: как растить Google (принято 2026-06-23)
Контекст: Google для нас в зачатке (~4 клика / 40 показов / поз. ~13 за 90д) против
Яндекса (177 запросов, бренд поз. 1.5–2). Это норма для молодого RU-сайта — Google
медленнее доверяет регион. Три рычага по приоритету:

1. **Индексация/свежесть (технич., делаем):** sitemap сабмичен; статич. контент в
   `#root` (PR #95); внутренняя перелинковка. Форсить нельзя — Google Indexing API
   только для job/livestream, не для обычных страниц. Ускоряем пассивно: sitemap +
   ссылки + время. Ручной «Запросить индексирование» — в GSC UI (Проверка URL).
2. **Long-tail контент (ГЛАВНЫЙ управляемый рычаг):** конкурентные ВЧ-запросы
   молодому сайту Google не даст. Бьём в хвост, где уже мелькаем: «индикатор
   баффета», «открытые позиции мосбиржа», «moex etf». Заточка: keywords обновлены
   (data-driven из реальных запросов Вебмастера), следующий шаг — расширить тексты
   методологий (H2/H3 с вопросами, 300–600 слов).
3. **Бэклинки/бренд-сигналы (ВНЕ кода, нужен Вадим):** ключевой фактор Google для
   нового сайта. Telegram-канал, упоминания, devlog-блог ([[content_blog]]). Без
   ссылок Google растёт медленно при любой on-page оптимизации.

НЕ делать: гнаться за дженериками (frame/таймфрейм), keyword-stuffing, ждать
паритета с Яндексом быстро (это месяцы).

Память: [[seo]] (полный SEO-контекст, доступы к API, снапшоты индекса).
