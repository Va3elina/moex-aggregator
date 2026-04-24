# Логотипы тикеров MOEX

SVG-логотипы компаний — один файл на тикер. Автоматически подхватываются
компонентом `<TickerLogo ticker="SBER" />` — если файла нет, показывается
fallback (цветной кружок + 2 буквы инициалов, цвет детерминирован от хеша).

## Используется

- **Модалка выбора актива** (`InstrumentSearchModal`) — на страницах ОИ
  и Сезонности
- **В будущем** — на Overview-дашборде (Top Movers, Top Volume)
- **НЕ на карте рынка** (`HeatmapPage`) — там специально только тикер,
  чтобы плитка не захламлялась

## Формат

- **SVG** предпочтительно (вектор, идеально масштабируется)
- Если PNG — минимум 256×256 с прозрачным фоном
- Имя файла = тикер + `.svg` (или `.png`). Примеры:
  - `SBER.svg` — Сбербанк
  - `GAZP.svg` — Газпром
  - `LKOH.svg` — Лукойл

## Как подключить

1. Положить SVG: `/public/logos/SBER.svg`
2. `npm run build` → логотип автоматически появится везде где используется
   `<TickerLogo />`. Никакие списки обновлять не надо — `onError` сам
   переключает на fallback.

## Где брать SVG

### Способ 1 — Wikimedia Commons (рекомендуется)

Большинство крупных российских компаний имеют SVG в Wikimedia Commons.
Поиск: `https://commons.wikimedia.org/w/index.php?search=Sberbank+logo`

**Проверенные URL** (копировать прямо в браузер, «Download SVG»):

- SBER: https://commons.wikimedia.org/wiki/File:Sberbank_Logo_2020.svg
- GAZP: https://commons.wikimedia.org/wiki/File:Gazprom-Logo.svg
- LKOH: https://commons.wikimedia.org/wiki/File:Lukoil.svg
- ROSN: https://commons.wikimedia.org/wiki/File:Rosneft_logo.svg
- NVTK: https://commons.wikimedia.org/wiki/File:Novatek_Logo.svg
- GMKN: https://commons.wikimedia.org/wiki/File:Norilsk_Nickel_logo.svg
- TATN: https://commons.wikimedia.org/wiki/File:Tatneft_Logo.svg
- YDEX: https://commons.wikimedia.org/wiki/File:Yandex_icon.svg
- MTSS: https://commons.wikimedia.org/wiki/File:MTS_logo_2015.svg
- MOEX: https://commons.wikimedia.org/wiki/File:MOEX_logo.svg

### Способ 2 — Официальные сайты компаний

Раздел обычно называется «Для СМИ» / «Brand assets» / «Пресс-центр».
Прямые ссылки для топ-5:

- Сбер: https://sberbank.com/ru/person/downloadcenter
- Газпром: https://gazprom.ru/about/corporate-identity/
- Лукойл: https://lukoil.com/ru/about/BrandBook

### Способ 3 — Brandfetch API (платный, $0 стартер)

Регистрация на https://brandfetch.com/developers → ключ.

```bash
# Пример для одного тикера
BRAND_ID="idJLNyTAJw"  # получить через search
curl -H "x-api-key: YOUR_KEY" \
  "https://cdn.brandfetch.io/${BRAND_ID}/w/400/h/400/logo" \
  -o SBER.svg
```

Скрипт `scripts/fetch_ticker_logos.py` — заготовка для bulk-скачивания.

## Приоритетный топ-30 MOEX

Начинать сбор с этих (по капитализации):

| Тикер | Компания | Домен |
|-------|----------|-------|
| SBER | Сбербанк | sberbank.ru |
| GAZP | Газпром | gazprom.com |
| LKOH | Лукойл | lukoil.com |
| ROSN | Роснефть | rosneft.com |
| NVTK | Новатэк | novatek.ru |
| GMKN | Норильский никель | nornickel.com |
| TATN | Татнефть | tatneft.ru |
| PLZL | Полюс | polyus.com |
| YDEX | Yandex | ya.ru |
| TCSG | Т-Банк | tbank.ru |
| SIBN | Газпром нефть | gazprom-neft.ru |
| MTSS | МТС | mts.ru |
| MGNT | Магнит | magnit.com |
| AFLT | Аэрофлот | aeroflot.ru |
| MOEX | Мосбиржа | moex.com |
| MAGN | ММК | mmk.ru |
| CHMF | Северсталь | severstal.com |
| NLMK | НЛМК | nlmk.com |
| PIKK | ПИК | pik.ru |
| FEES | Россети | rosseti.ru |
| AFKS | АФК Система | sistema.ru |
| VTBR | ВТБ | vtb.ru |
| RUAL | Русал | rusal.ru |
| SBERP | Сбербанк преф | sberbank.ru |
| IRAO | ИнтерРАО | interrao.ru |
| ALRS | Алроса | alrosa.ru |
| RTKM | Ростелеком | rt.ru |
| HYDR | РусГидро | rushydro.ru |
| PHOR | ФосАгро | phosagro.ru |
| BSPB | Банк СПб | bspb.ru |

## Почему не автоматически

- **Clearbit Logo API** (`https://logo.clearbit.com/{domain}`) — с 2024
  требует API ключ (был бесплатным раньше)
- **Google Favicon** (`https://www.google.com/s2/favicons?sz=256&...`) —
  возвращает favicons максимум 48×48, часто 16×16. Для дизайна слишком
  мелко
- **Brandfetch free search** — возвращает brandId но не качественные
  SVG (только WebP с fallback-lettermark без подписки)

Поэтому SVG — ручная выгрузка + коммит в репо. Один раз собрал — всегда
будет работать.
