# Публикация расширения «Фрейм» — чек-лист

Два слоя обновляются по-разному:
- **Начинка индикаторов** (графики/drawer/режимы/фиксы) живёт на сайте (`таймфрейм.рф/embed/*`)
  → обновляется **автоматически** при деплое сайта. Магазин трогать НЕ нужно.
- **Оболочка** (`widget.js`, `content.js`, `popup`, `manifest`) лежит в пакете магазина
  → нужен **переаплоад** (вручную или GitHub Action ниже). Перед каждым — поднять `version`.

---

## 1. Разовая подготовка

### A. Аккаунт разработчика Chrome Web Store
1. Зайти: https://chrome.google.com/webstore/devconsole
2. Оплатить разовый сбор **$5**, подтвердить аккаунт.

### B. Первая публикация — ВРУЧНУЮ (через дашборд)
> API умеет только обновлять существующий айтем; первый раз проще залить руками,
> заодно заполнив весь листинг.

1. Собрать пакет:
   ```bash
   bash extension/store/package.sh
   # → extension/store/build/frame-extension-v0.5.0.zip
   ```
2. В дашборде **New item → Upload** этот zip.
3. Заполнить вкладку **Store listing** (тексты — в `LISTING.md`):
   - Название, краткое + подробное описание, категория **Workflow & Planning**, язык **Russian**.
   - Иконка `extension/icons/icon128.png` (берётся из пакета).
   - **Скриншоты** (1280×800, мин. 1) — `extension/store/screenshots/` (сделай 2–3 кадра в работе).
   - Промо: `assets/tile-440x280.png`, `assets/marquee-1400x560.png`.
4. **Privacy practices**: вставить обоснования разрешений + раскрытие данных из `LISTING.md`.
   Privacy policy URL: `https://таймфрейм.рф/privacy` (раздел про расширение уже добавлен).
5. **Distribution**: Public (или Unlisted — по ссылке, тоже авто-обновляется).
6. **Submit for review**. Ревью: от пары часов до нескольких дней.
7. После аппрува скопировать **Item ID** (32 буквы из URL дашборда) → понадобится для секрета `CWS_EXTENSION_ID`.

### C. Настроить API для авто-заливки (для GitHub Action)
> Делается один раз. Если авто-публикация не нужна — пропусти, обновляй вручную (раздел 2, вариант 2).

1. **Google Cloud Console** (https://console.cloud.google.com):
   - Создать проект (любое имя).
   - **APIs & Services → Library** → найти **Chrome Web Store API** → **Enable**.
2. **OAuth consent screen**:
   - User type **External**, заполнить обязательные поля.
   - Scope добавлять не обязательно (запросим в auth-URL).
   - В **Test users** добавить свой Google-аккаунт (тот же, что в дашборде магазина).
   - ⚠️ Пока приложение в статусе **Testing**, refresh-token живёт **7 дней**. Варианты:
     a) перевести Publishing status в **In production** (для этого скоупа обычно без верификации);
     b) либо оставить Testing и перевыпускать токен раз в неделю (шаг 4).
3. **Credentials → Create credentials → OAuth client ID**:
   - Application type **Desktop app**.
   - Сохранить **Client ID** и **Client secret**.
4. **Получить refresh token** (один раз):
   - Открыть в браузере (подставив свой CLIENT_ID):
     ```
     https://accounts.google.com/o/oauth2/auth?response_type=code&access_type=offline&prompt=consent&redirect_uri=http://localhost:8080&scope=https://www.googleapis.com/auth/chromewebstore&client_id=ВАШ_CLIENT_ID
     ```
   - Подтвердить доступ. Браузер редиректнет на `http://localhost:8080/?code=4/XXXX`
     (страница не откроется — это норм). Скопировать значение `code` из адресной строки
     (всё после `code=` до `&`, при необходимости URL-decode `%2F`→`/`).
   - Обменять код на токен:
     ```bash
     curl -s https://oauth2.googleapis.com/token \
       -d code="ВСТАВИТЬ_CODE" \
       -d client_id="ВАШ_CLIENT_ID" \
       -d client_secret="ВАШ_CLIENT_SECRET" \
       -d redirect_uri="http://localhost:8080" \
       -d grant_type=authorization_code
     ```
   - В ответе взять поле **`refresh_token`** (длинная строка, начинается с `1//`).
5. **GitHub → Settings → Secrets and variables → Actions → New repository secret** — добавить 4:
   | Secret | Значение |
   |---|---|
   | `CWS_EXTENSION_ID` | Item ID из дашборда (шаг B7) |
   | `CWS_CLIENT_ID` | OAuth Client ID |
   | `CWS_CLIENT_SECRET` | OAuth Client secret |
   | `CWS_REFRESH_TOKEN` | refresh_token из шага 4 |

---

## 2. Обновление потом

### Вариант 1 — автоматически (GitHub Action)
```bash
# 1. поднять версию
#    extension/manifest.json → "version": "0.6.0"
# 2. закоммитить и поставить тег
git add extension/manifest.json extension/src/
git commit -m "ext: <что изменилось>"
git push origin main
git tag ext-v0.6.0 && git push origin ext-v0.6.0
```
Тег `ext-v*` запускает `.github/workflows/publish-extension.yml`: пакует → грузит → публикует
через CWS API. Можно и вручную: вкладка **Actions → Publish extension → Run workflow**
(тумблер publish=false = залить черновиком без отправки на ревью).

### Вариант 2 — вручную (без API)
```bash
# поднять version в manifest.json, затем:
bash extension/store/package.sh
```
Залить полученный zip в дашборде: **Package → Upload new package → Submit for review**.

> ⚠️ Chrome обновляет у пользователей только если номер `version` БОЛЬШЕ предыдущего.

---

## 3. Что обновляется само (магазин не нужен)
Любые правки индикаторов/графиков/drawer/данных = деплой сайта (skill `moex-deploy-frontend`).
Пользователи видят при переоткрытии панели. Это ~90% будущих доработок.

---

## 4. Яндекс.Браузер (опционально)
Яндекс ставит расширения из Chrome Web Store (авто-обновление работает). Для отдельного
присутствия — каталог https://addons.yandex / Яндекс Браузер для разработчиков: залить тот же zip.

---

## 5. Частые проблемы
- **«Item not found» при upload** — неверный `CWS_EXTENSION_ID` или айтем ещё не создан (сделай первый публиш вручную).
- **refresh_token протух / `invalid_grant`** — OAuth consent в статусе Testing (7 дней). Перевыпустить токен (1C-4) или перевести в Production.
- **Ревью отклонило за разрешения** — добавить обоснования из `LISTING.md` (особенно `declarativeNetRequest` и host-permissions).
- **«version already exists»** — забыли поднять `version` в `manifest.json`.
