# Billing (ЮKassa) — инструкция для коллеги

Эта папка реализует приём платежей и подписки через **ЮKassa**. Сейчас работает
в режиме заглушки (stub) — **платежи не списываются**, фронт показывает
"Тестовый режим" на странице /pricing.

Для активации реальных платежей нужно только **два действия**:

---

## 1. Зарегистрироваться в ЮKassa

1. Заходи на https://yookassa.ru → "Подключиться"
2. Нужен **ИП или ООО** (физлицам недоступно)
3. Пройти верификацию — загрузить документы (ОГРН, устав, паспорт директора)
4. Это занимает 1–3 рабочих дня
5. После одобрения в ЛК: **Настройки → Магазин → Ключи API**
   - скопировать **`Shop ID`** (число, например `123456`)
   - скопировать **`Секретный ключ`** (строка вида `live_xxxxxxxxxxxxxxx`)

### Тестовые ключи (для разработки до одобрения)

На странице ключей есть **"Тестовый магазин"** — он работает сразу без
верификации, использует фейковые карты (`5555 5555 5555 4444`), никакие
деньги не списываются. Ключи имеют префикс `test_`.

Можно использовать тестовые пока идёт процесс верификации.

---

## 2. Вставить ключи в `.env` на сервере

На проде файл лежит в `/opt/frame/.env`. Добавь/замени две строки:

```
YOOKASSA_SHOP_ID=123456
YOOKASSA_SECRET_KEY=live_xxxxxxxxxxxxxxx
```

Или для тестового магазина:

```
YOOKASSA_SHOP_ID=54401
YOOKASSA_SECRET_KEY=test_xxxxxxxxxxxxxxx
```

## 3. Настроить webhook URL в ЛК ЮKassa

В ЛК ЮKassa:

1. **Интеграция → HTTP-уведомления**
2. Добавить URL: `https://xn--80aklbnczmv.xn--p1ai/api/billing/webhook`
   (для тестового магазина: тот же URL)
3. Включить события:
   - `payment.succeeded` — платёж прошёл → активируем подписку
   - `payment.canceled` — платёж отменён
   - `refund.succeeded` — возврат → снимаем Pro

## 4. Перезапустить API

```bash
docker restart frame-api-1
```

После рестарта factory автоматически выберет YooKassaProvider вместо Stub.
На `/pricing` исчезнет баннер "Тестовый режим".

---

## Проверка что всё работает

```bash
# 1. /api/billing/plans должен вернуть provider: "yookassa" (а не "stub")
curl https://xn--80aklbnczmv.xn--p1ai/api/billing/plans | grep provider

# 2. На сайте /pricing → кликнуть "Оформить" → должен редиректить
#    на yoomoney.ru / yookassa.ru (а не на /billing/stub)

# 3. Пройти тестовую оплату картой 5555 5555 5555 4444 (тестовый магазин)
#    → webhook придёт → роль user обновится на 'pro' (или выбранный tier)
```

---

## Что уже сделано (и работает прямо сейчас, до получения ключей)

| Часть | Статус |
|---|---|
| БД: таблица `subscriptions` | ✅ создана в проде |
| Backend: 6 endpoint'ов `/api/billing/*` | ✅ работают |
| Backend: адаптер ЮKassa | ✅ готов (спит до ключей) |
| Backend: Stub-провайдер для dev | ✅ работает прямо сейчас |
| Frontend: страница `/pricing` | ✅ отдаёт 4 тарифа |
| Frontend: `/billing/success` poll-страница | ✅ |
| Frontend: `/billing/stub` (симуляция) | ✅ работает без ключей |

---

## Что добавить в будущем (не сейчас)

- [ ] **Feature flags** — сейчас tier только проверяет "кто имеет доступ"
      через `require_tier('pro')`. Постепенно заменяем на точные фичи:
      `require_feature('timeframe:5min')`, `require_feature('indicator:buffett')`.
- [ ] **Рекуррентные платежи** — ЮKassa умеет auto-charge через `saved_payment_method`.
      Пока подписка заканчивается — пользователь платит заново вручную.
- [ ] **Cron на expire** — `billing.service.expire_overdue(db)` раз в час,
      чтобы вовремя снимать Pro у истёкших.
- [ ] **История платежей на ProfilePage** — показать таблицу активных/прошлых
      подписок. Endpoint `/api/billing/history` легко добавить к service'у.
- [ ] **Промокоды** — колонка `discount_code` в subscriptions + валидация
      в create_checkout_for_user.
- [ ] **Триал на 7 дней** — при регистрации ставить `role='pro'` + запись
      в subscriptions(status='active', amount=0, expires_at=+7d).

---

## Архитектура

```
                                    ┌─────────────────────┐
                                    │  Frontend /pricing  │
                                    └──────────┬──────────┘
                                               │ POST /checkout
                                    ┌──────────▼──────────┐
                                    │ api/routers/billing │
                                    └──────────┬──────────┘
                                               │
                          ┌────────────────────┼──────────────────┐
                          ▼                    ▼                  ▼
                  ┌──────────────┐    ┌───────────────┐   ┌──────────────┐
                  │  service.py  │    │   factory.py  │   │  plans.py    │
                  │ create/     │    │ → yookassa OR │   │ 4 tiers × 2  │
                  │  activate    │    │   stub        │   │ periods SKU  │
                  └──────┬───────┘    └───────┬───────┘   └──────────────┘
                         │                    │
                         ▼                    ▼
                  ┌──────────────┐    ┌───────────────┐
                  │ subscriptions│    │ YooKassa API  │
                  │  table (БД)  │    │ (ext. HTTPS)  │
                  └──────────────┘    └───────────────┘
                         ▲                    │
                         │  webhook           │
                         └────────────────────┘
                          POST /api/billing/webhook
```

Главные файлы:

- `provider.py` — Protocol PaymentProvider (контракт для всех адаптеров)
- `stub.py` — заглушка для dev (без ключей всё через неё)
- `yookassa.py` — реальный адаптер (активен при ключах)
- `factory.py` — `get_payment_provider()` — выбирает по env
- `plans.py` — тарифные планы, источник истины
- `tiers.py` — `require_tier('pro')` dependency для защиты эндпоинтов
- `service.py` — бизнес-логика (checkout/activate/cancel/sync_role)
- `../routers/billing.py` — HTTP endpoints
