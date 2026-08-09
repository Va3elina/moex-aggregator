# Billing — приём платежей и подписки

Эта папка реализует приём платежей и подписки через **Т-Банк Acquiring**
(приоритетный провайдер с 2026). Адаптер для **ЮKassa** оставлен как
legacy-fallback на случай возврата.

Без ключей factory автоматически возвращает **Stub-провайдер** —
платежи не списываются, фронт показывает "Тестовый режим".

---

## 1. Т-Банк — основной провайдер

### Регистрация

1. Заходи на https://www.tbank.ru/business/ → «Интернет-эквайринг»
2. Нужен **ИП или ООО**
3. Подписать договор (онлайн или офлайн через менеджера)
4. Т-Банк проверяет сайт на соответствие требованиям к интернет-магазину
   (см. `docs-requirements-for-online-store.pdf` от Т-Банка):
   - Контакты + юр. адрес ✓ (`/contacts`)
   - Условия возврата ✓ (`/refund`)
   - Условия предоставления услуги ✓ (`/delivery`)
   - Политика инфобезопасности ✓ (`/security`)
   - Политика обработки ПДн ✓ (`/privacy`)
   - Логотипы ПС + T-Bank + ссылка на tbank.ru ✓ (внизу `/pricing`)
5. После одобрения в ЛК эквайринга:
   - **Терминалы** → «Создать терминал»
   - Скопировать **`Terminal Key`** (например `1612345678901DEMO`)
   - Скопировать **`Password`** (shared secret для подписи)

### Тестовый терминал (sandbox)

Сразу после регистрации создаётся тестовый терминал. URL'ы те же:
- API: `https://securepay.tinkoff.ru/v2/`
- Тестовые карты: `2200770100029246` (Visa), `4300000000000777` (3-DS),
  `5469380041179762` (без 3-DS) — см. документацию Т-Банка.

### Вставить ключи в `.env` на проде

На сервере `/opt/frame/.env`:

```
TBANK_TERMINAL_KEY=1612345678901DEMO
TBANK_PASSWORD=<password_from_lk>
```

### Настроить Notification URL в ЛК Т-Банка

В ЛК эквайринга → **Магазин → Уведомления**:

- Notification URL: `https://framedata.ru/api/billing/webhook`
- Метод: `POST`
- Тип: `JSON`
- Включить статусы: `CONFIRMED`, `REJECTED`, `REFUNDED`, `CANCELED`,
  `PARTIAL_REFUNDED`, `REVERSED`, `DEADLINE_EXPIRED`.

### Перезапустить API

```bash
docker restart frame-api-1
```

После рестарта factory автоматически выберет `TBankProvider` вместо Stub.

### Проверка

```bash
# 1. Stub должен исчезнуть из логов:
docker logs frame-api-1 2>&1 | grep -i billing | tail -5

# 2. Запрос /api/billing/plans должен работать (он не зависит от провайдера)
curl https://framedata.ru/api/billing/plans

# 3. На /pricing → "Оформить" → должен открыться PaymentURL вида
#    https://securepay.tinkoff.ru/Cz5GZAOd (а не /billing/stub).

# 4. Оплатить тестовой картой → webhook придёт → роль user обновится.
```

---

## 2. ЮKassa (legacy fallback)

Старый адаптер. Если по каким-то причинам нужно вернуться на ЮКассу —
удалить `TBANK_*` из `.env`, добавить:

```
YOOKASSA_SHOP_ID=123456
YOOKASSA_SECRET_KEY=live_xxxxx
```

И настроить webhook в ЛК ЮКассы на тот же URL.
Подробности см. в `yookassa.py` (исторический README сохранён там).

---

## 3. Архитектура

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
                  │ create/      │    │ tbank → yk →  │   │ 4 tiers × 2  │
                  │  activate    │    │   stub        │   │ periods SKU  │
                  └──────┬───────┘    └───────┬───────┘   └──────────────┘
                         │                    │
                         ▼                    ▼
                  ┌──────────────┐    ┌───────────────┐
                  │ subscriptions│    │ T-Bank API    │ (или YooKassa)
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
- **`tbank.py`** — основной адаптер Т-Банка (активен с 2026)
- `yookassa.py` — legacy адаптер (активен только если в env только YooKassa-ключи)
- `factory.py` — `get_payment_provider()` — выбирает по env (tbank > yookassa > stub)
- `plans.py` — тарифные планы, источник истины
- `tiers.py` — `require_tier('pro')` dependency для защиты эндпоинтов
- `service.py` — бизнес-логика (checkout/activate/cancel/sync_role)
- `../routers/billing.py` — HTTP endpoints

---

## 4. T-Bank API — quick reference

### Init (создание платежа)

```
POST https://securepay.tinkoff.ru/v2/Init
Content-Type: application/json

{
  "TerminalKey": "1612345678901DEMO",
  "Amount": 14000,           // копейки! (1400 ₽ × 100)
  "OrderId": "ab12cd34...",  // наш UUID
  "Description": "Подписка Pro 30 дней",
  "SuccessURL": "https://таймфрейм.рф/billing/success",
  "FailURL":    "https://таймфрейм.рф/billing/success",
  "Token": "<sha256_hex>"    // см. _make_token в tbank.py
}
```

Response:

```json
{
  "Success": true,
  "PaymentId": "13660001",
  "PaymentURL": "https://securepay.tinkoff.ru/Cz5GZAOd"
}
```

### Webhook (нотификация → нам)

T-Bank POST'ит JSON на наш `/api/billing/webhook`. После приёма
**обязательно** вернуть `OK` plain text (HTTP 200), иначе ретраи.

Маппинг `Status` → наш `event_type` (см. `_STATUS_MAP` в tbank.py):

| T-Bank Status | event_type |
|---|---|
| CONFIRMED | payment.succeeded |
| REJECTED / CANCELED / DEADLINE_EXPIRED / REVERSED | payment.canceled |
| REFUNDED / PARTIAL_REFUNDED | refund.succeeded |
| остальные (intermediate) | None (игнорируем) |

### Token подпись (SHA-256)

1. Берём все top-level скаляры (исключая `Receipt`, `DATA`, `Token`)
2. Добавляем `Password` со значением shared secret
3. Сортируем по ключу alphabetically
4. Конкатенируем значения без разделителей
5. SHA-256 → hex lowercase

Подробнее: https://developer.tbank.ru/eacq/api/v2 → Подпись запроса.

---

## 5. Что уже работает

| Часть | Статус |
|---|---|
| БД: таблица `subscriptions` | ✅ создана в проде |
| Backend: 6 endpoint'ов `/api/billing/*` | ✅ работают |
| Backend: адаптер T-Bank (`tbank.py`) | ✅ готов (спит до ключей) |
| Backend: legacy YooKassa адаптер | ✅ сохранён как fallback |
| Backend: Stub-провайдер для dev | ✅ работает прямо сейчас |
| Frontend: страница `/pricing` + блок «Способы оплаты» | ✅ |
| Frontend: `/billing/success` poll-страница | ✅ |
| Frontend: `/billing/stub` (симуляция) | ✅ работает без ключей |
| Legal: `/contacts /refund /delivery /security /privacy` | ✅ template'ы созданы (требуют заполнения placeholder'ов) |

---

## 6. TODO (после интеграции T-Bank)

- [ ] **Рекуррентные платежи** — T-Bank умеет saved-карту через `Recurrent=Y` +
      `CustomerKey` в Init. Пока подписка заканчивается — пользователь
      платит заново вручную.
- [ ] **Фискализация (54-ФЗ)** — для физлиц-плательщиков из РФ нужно
      передавать `Receipt` блок при Init. T-Bank сам отправляет чек в ФНС
      через выбранную ОФД.
- [ ] **Cron на expire** — `billing.service.expire_overdue(db)` раз в час.
- [ ] **История платежей на ProfilePage** — endpoint `/api/billing/history`.
- [ ] **Промокоды** — колонка `discount_code` в `subscriptions`.
- [ ] **T-Pay в одно касание** — отдельный SDK на frontend, fast checkout.
