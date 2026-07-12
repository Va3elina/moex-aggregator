---
name: moex-publish-extension
description: Publish/update the Фрейм browser extension in the Chrome Web Store. Use when user says "опубликуй расширение", "обнови расширение", "залей новую версию расширения", "выложи расширение в магазин", "publish extension", "новая версия расширения", or after changing extension/ files (widget.js, content.js, popup, manifest, rules.json) that need to reach Chrome Web Store users. NOT for embed/indicator changes (those auto-deploy via the site — use moex-deploy-frontend).
---

# Публикация расширения «Фрейм» в Chrome Web Store

## ⚠️ Два слоя — обновляются ПО-РАЗНОМУ (главное правило)
- **Начинка индикаторов** (`frontend/src/pages/embed/*`, графики, режимы, данные) живёт на сайте
  `таймфрейм.рф/embed/*` → обновляется **автоматически при деплое сайта** (skill `moex-deploy-frontend`).
  Магазин НЕ нужен, юзеры видят при переоткрытии панели. Это ~90% правок.
- **Оболочка** (`extension/src/widget.js`, `content.js`, `popup.*`, `manifest.json`, `rules.json`) —
  пакет в магазине → нужен **новый zip в Chrome Web Store**. Этот скилл — про неё.

→ Если правка ТОЛЬКО в `frontend/src/pages/embed/` — это не сюда, используй `moex-deploy-frontend`.

## Авто-публикация настроена (2026-06-07) — работает по тегу
- Workflow: `.github/workflows/publish-extension.yml` (чистый curl → CWS API, секреты через env).
- Триггер: тег **`ext-v*`** (или вручную: Actions → «Publish extension» → Run workflow, тумблер `publish`).
- 4 секрета в GitHub-репо `Va3elina/moex-aggregator`:
  `CWS_EXTENSION_ID`, `CWS_CLIENT_ID`, `CWS_CLIENT_SECRET`, `CWS_REFRESH_TOKEN`.
- OAuth-приложение Google (проект «Frame», Chrome Web Store API enabled) в **In production** →
  refresh_token бессрочный (в Testing жил бы 7 дней).
- **Item ID:** `klcmdbbfpnhmcegdabbijcnbamikbhhj` · **publisher/account:** `8991344a-57d5-42d2-9610-c31cc39781bf`.

## Стандартный публиш (то, что делаем почти всегда)
1. **Подними `version`** в `extension/manifest.json` (Chrome обновляет ТОЛЬКО если номер БОЛЬШЕ
   предыдущего). Патч-фикс → +0.0.1.
2. Коммит + пуш + тег (имя тега = версия):
   ```bash
   cd C:/MOEX
   node --check extension/src/widget.js          # vanilla JS, tsc тут нет
   git add extension/ && git commit -m "ext: <что изменил>"
   git push origin main
   git tag ext-v0.6.0 && git push origin ext-v0.6.0
   ```
3. Тег запускает workflow → пакует (`extension/store/package.sh`) → льёт → сабмитит на ревью Google.
4. **Проследить прогон:**
   ```bash
   gh run list --workflow=publish-extension.yml --limit 1   # completed / success?
   gh run watch <run-id> --exit-status                       # ждать вживую
   gh run view  <run-id> --log-failed                        # если красный — логи
   ```
   Зелёный = залито + отправлено на ревью. Дальше Google ревьюит, потом Chrome тихо раскатывает юзерам.

## Проверить статус айтема в магазине (нужен валидный access_token)
Секреты НЕ в коде. Подставь из GitHub Secrets / password manager Вадима:
```bash
TOKEN=$(curl -s https://oauth2.googleapis.com/token \
  -d client_id="$CWS_CLIENT_ID" -d client_secret="$CWS_CLIENT_SECRET" \
  -d refresh_token="$CWS_REFRESH_TOKEN" -d grant_type=refresh_token \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")
curl -s "https://www.googleapis.com/chromewebstore/v1.1/items/klcmdbbfpnhmcegdabbijcnbamikbhhj?projection=DRAFT" \
  -H "Authorization: Bearer $TOKEN" -H "x-goog-api-version: 2" | python3 -m json.tool
```
`crxVersion` = текущая версия айтема. `uploadState` = SUCCESS / IN_PROGRESS / NOT_FOUND.

## Грабли
- **Версия только вверх** — иначе CWS «version already exists».
- **Google ревьюит каждую версию** (часы–дни). Авто-заливка ≠ мгновенно live. После аппрува Chrome
  сам обновляет юзеров (им жать ничего не надо).
- **Ревью-конфликт:** заливка новой версии поверх «pending review» обычно проходит (новая идёт на
  ревью вместо старой — проверено на 0.5.1 поверх 0.5.0). Если API ругнётся «item in review» —
  подожди аппрува, потом тег.
- **refresh_token протух (`invalid_grant`):** значит OAuth-приложение вернули в Testing. Должно быть
  **In production** (Google Auth Platform → Audience → Publish app). Перевыпуск токена —
  `extension/store/PUBLISH_CHECKLIST.md` раздел 1C (auth-URL под Client ID → `code` из
  `localhost:8080` → curl на `oauth2.googleapis.com/token`).
- **Item ID ≠ publisher ID.** Item = 32 буквы (a–p); publisher = UUID `8991344a-…`.
- **Кабинет Web Store скриптовать НЕЛЬЗЯ** (Chrome блокирует extensions на странице gallery) —
  листинг/скриншоты правит Вадим вручную в дашборде; я готовлю файлы и текст.
- **Секреты в поля НЕ ввожу** (запрещено) — refresh_token/secret добавляет Вадим сам (UI GitHub
  Secrets или `gh secret set`).

## Если меняется графика/листинг (редко)
- Тексты листинга: `extension/store/LISTING.md`. Полный чек-лист: `extension/store/PUBLISH_CHECKLIST.md`.
- Промо: `python3 scripts/gen-store-assets.py` → `extension/store/assets/` (tile 440×280, marquee 1400×560, hero).
- Скриншоты под ровно 1280×800 (RGB, без альфы): `python3 scripts/fit-screenshots.py --desktop N`
  (берёт N свежих кадров с Рабочего стола). Снимать в **файл**, не в буфер обмена:
  - macOS: **Cmd+Shift+4** (файл на Рабочий стол), НЕ Cmd+Ctrl+Shift+4 (буфер).
  - Windows: **Win+Shift+S** → выделить область → уведомление «сохранить как» (или вставить
    из буфера в Paint/любой редактор и сохранить `.png` на Рабочий стол вручную — по
    умолчанию Win+Shift+S кладёт только в буфер).
  Fallback-отрисовка панелей: `scripts/gen-store-screenshots.py`.

## Связано
`extension/store/` (весь пакет публикации), `moex-deploy-frontend` (начинка индикаторов),
`moex-git-workflow` (коммиты), память `terminal_extension.md`.
