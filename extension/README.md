# Фрейм — расширение для терминала Т-Инвестиций

Плавающее окно с индикаторами Фрейм (таймфрейм.рф) поверх веб-терминала
Т-Инвестиций. Форма — клон Trading Tools (перетаскиваемая панель + вкладки),
дизайн — editorial Фрейма. MV3, работает во всех Chromium-браузерах
(Chrome, **Яндекс.Браузер**, Edge, Opera, Brave).

> Полный план: `../.claude/TERMINAL_EXTENSION_PLAN.md`

## Структура

```
manifest.json        — MV3, content script на страницах терминала
src/widget.js        — плавающее окно (Shadow DOM, editorial-скин, 8 вкладок,
                       drag/resize/collapse/persist). Вся логика тут.
src/content.js       — тонкий загрузчик: монтирует окно на странице терминала
src/popup.html/js    — popup по клику на иконку (TODO: логин/настройки)
dev-preview.html     — локальный предпросмотр окна без терминала
```

Содержимое вкладок — iframe на `таймфрейм.рф/embed/<indicator>` (роуты во фронте
Фрейма: `frontend/src/pages/embed/`).

## Локальная разработка

```bash
# 1) подними фронт Фрейма (embed-роуты)
cd frontend && npm run dev            # :5173

# 2) предпросмотр окна
cd extension && python3 -m http.server 8910
# открой http://localhost:8910/dev-preview.html
```

## Установка (load unpacked)

1. `chrome://extensions` → включить «Режим разработчика».
2. «Загрузить распакованное» → выбрать папку `extension/`.
3. Открыть `tbank.ru/terminal` — окно появится справа сверху.

## Статус (v1)

- ✅ Плавающее окно: drag, resize, collapse, close + launcher, persist позиции/темы/вкладки.
- ✅ 7 вкладок (2 группы), editorial-light/dark. (Карта рынка исключена из расширения.)
- ✅ Все 7 embed-роутов **задеплоены на прод** (`таймфрейм.рф/embed/*`), работают с реальными данными.
- ✅ **CSP терминала снимается через `declarativeNetRequest`** (rules.json) — frame-src tbank.ru
  не содержит таймфрейм.рф (проверено чтением заголовка), без этого iframe блокируется.
- ⏳ Реальный тест на залогиненном `tbank.ru/terminal` (load unpacked) + embed-auth для платных.

## TODO

- **CSP терминала** — ✅ РЕШЕНО: `declarativeNetRequest` (permission + `rules.json`) снимает
  `Content-Security-Policy` на главном документе `/terminal/` (скоуп — только терминал). Нужно,
  т.к. `frame-src` tbank.ru — строгий whitelist без таймфрейм.рф (проверено чтением заголовка
  2026-06-04). Trade-off: на странице терминала CSP отключается целиком (хирургически править
  одну директиву DNR не умеет) — приемлемо для opt-in расширения. Финальная проверка — на
  живом залогиненном терминале.
- **Авторизация**: логин во Фрейм из popup → embed-токен → проброс в iframe
  (гейтинг «только платные»). Storage третьесторонних iframe партиционирован —
  токен держит расширение, не localStorage iframe.
- **Иконки** 16/48/128 PNG (перед публикацией).
- **POST-V1**: синк тикера с активным графиком терминала.
