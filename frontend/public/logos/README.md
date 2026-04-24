# Логотипы тикеров MOEX

Сюда складываются SVG-логотипы компаний — один файл на тикер.

## Формат

- **SVG** предпочтительно (вектор, идеально масштабируется)
- Если PNG — минимум 256×256 с прозрачным фоном
- Имя файла = тикер + `.svg` (или `.png`). Примеры:
  - `SBER.svg` — Сбербанк
  - `GAZP.svg` — Газпром
  - `LKOH.svg` — Лукойл

## Где брать

1. **Wikipedia Commons** — для большинства крупных компаний
2. **Официальные сайты** в разделе "Для СМИ" / "Brand assets"
3. **Clearbit Logo API**: `https://logo.clearbit.com/sberbank.ru` (PNG)
4. **Для мелких бумаг** — пропускаем, fallback покажет initials

## Как подключить

1. Положить SVG: `/public/logos/SBER.svg`
2. Добавить тикер в `AVAILABLE_LOGOS` Set в:
   - `frontend/src/pages/HeatmapPage.tsx`
3. `npm run build` → автоматически появится на heatmap

## Top-30 приоритет

Начинать сбор с этих (по капитализации MOEX):

```
SBER  — Сбербанк              sberbank.ru
GAZP  — Газпром               gazprom.com
LKOH  — Лукойл                lukoil.com
ROSN  — Роснефть              rosneft.com
NVTK  — Новатэк               novatek.ru
GMKN  — Норильский никель     nornickel.com
TATN  — Татнефть              tatneft.ru
PLZL  — Полюс                 polyus.com
YDEX  — Yandex                ya.ru
TCSG  — Тинькофф              tinkoff.ru
SIBN  — Газпром нефть         gazprom-neft.ru
MTSS  — МТС                   mts.ru
MGNT  — Магнит                magnit.com
AFLT  — Аэрофлот              aeroflot.ru
MOEX  — Мосбиржа              moex.com
MAGN  — ММК                   mmk.ru
CHMF  — Северсталь            severstal.com
NLMK  — НЛМК                  nlmk.com
PIKK  — ПИК                   pik.ru
FEES  — Россети               rosseti.ru
AFKS  — АФК Система           sistema.ru
VTBR  — ВТБ                   vtb.ru
RUAL  — Русал                 rusal.ru
SBERP — Сбербанк преф         sberbank.ru
IRAO  — ИнтерРАО              interrao.ru
ALRS  — Алроса                alrosa.ru
RTKM  — Ростелеком            rt.ru
HYDR  — РусГидро              rushydro.ru
UPRO  — Юнипро                unipro.energy
BSPB  — Банк СПб              bspb.ru
```
