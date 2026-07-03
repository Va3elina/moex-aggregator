---
name: moex-billing
description: Биллинг Фрейма — T-Bank эквайринг (прод, единственный активный) + ЮKassa (ОТМЕНЕНА, спящий код). Use when user says «оплата», «биллинг», «юкасса», «yookassa», «тбанк», «рекурренты», «подписка не оплатилась», «возврат денег», «отвяжи карту», or any payment/subscription ops task. Содержит ключи (см. references/secrets.local.md), архитектуру кода, рунбуки, gotchas.
---

# Биллинг Фрейма: T-Bank (прод) + ЮKassa (ОТМЕНЕНА, код спит)

> Снимок состояния: 2026-07-03. Живой статус — memory `billing_system.md` (он главнее при расхождении).

## ⛔ СТАТУС ЮKassa: МИГРАЦИЯ ОТМЕНЕНА (2026-07-03)
**ЮKassa отказала во включении рекуррентов по нужным способам оплаты** (заявка от
02.07). Решение Вадима: «код оставим, пока остаёмся на Т-Банке». Следствия:
- **Единственный активный провайдер — T-Bank.** ЮKassa-код в репо СПИТ (env на
  проде НЕ заданы) — НЕ активировать, НЕ удалять, НЕ «продолжать миграцию» без
  явной команды Вадима.
- Рунбуки ЮKassa ниже — историческая справка на случай реактивации.
- UI отвязки (/profile) остаётся скрытым (`PAYMENT_METHODS_UI_ENABLED=false`).

## ⚠️ Правила безопасности (ЧИТАТЬ ПЕРВЫМ)

1. **Все ключи — в `references/secrets.local.md`** (рядом с этим файлом, в `.gitignore`).
   НИКОГДА не вставлять секреты в тело скилла/коммиты/PR — скиллы уходят на GitHub.
2. **Продовые деньги.** Любая операция с чужими подписками/картами — только по явной
   команде Вадима. Тесты — ТОЛЬКО на его аккаунте id=2 (ermolaeffvadick@yandex.ru).
3. **Никаких глобальных override'ов цены** — тест-цена только через
   `YOOKASSA_TEST_PRICE_RUB` (применяется исключительно юзерам из `YOOKASSA_TEST_USER_IDS`).
4. **Биллинг-изменения на прод** — после явного «го» Вадима (урок 2026-07-03: не
   деплоить из «информационного» сообщения).
5. Продления существующих подписчиков идут по `pm.provider` (у всех сейчас `tbank`) —
   включение ЮKassa их НЕ затрагивает by construction.

## Текущее состояние (2026-07-03, после отказа)

- **Прод-провайдер: T-Bank** (терминал в secrets), recurrent работает, чеки бьёт T-Bank.
- **ЮKassa: ОТМЕНЕНА** (см. статус-блок выше). Магазин 1398109 (ИП Тория) существует,
  фискализация yoo_receipt ВКЛ, разовые платежи технически работают, но рекурренты
  по нашим методам ЮKassa включать отказалась. Alfa Pay автоплатежи не поддерживает
  в принципе.
- **Код-фундамент спит** (PR #308–#310): активация = env-переменные, см. рунбук ниже
  (только при реактивации по команде Вадима).
- **UI отвязки** (/profile «Способы оплаты», PR #308) — СКРЫТ флагом
  `PAYMENT_METHODS_UI_ENABLED=false` (frontend/src/config/features.ts) по решению Вадима.
  Включить при старте теста (нужен для теста отвязки). Бэкенд-эндпоинты живут без флага;
  DELETE стирает rebill_id/account_token (компл-требование ЮKassa).
- Мотивы переезда: рекурренты SberPay/T-Pay; СБП-привязка совмещена с первым платежом
  (у T-Bank — отдельная кнопка в приложении банка, это бесило); форма на yoomoney.ru
  с GlobalSign-сертом (открывается у VPN-юзеров — снимает SpeedPay-костыль);
  T-Bank СБП-QR заблокирован ErrorCode 3001.

## Архитектура кода

```
api/billing/
  provider.py   — Protocol + WebhookEvent (есть provider_name — источник события)
  tbank.py      — T-Bank Acquiring v2 (Init/Charge/GetQr/ChargeQr/GetState/Cancel)
  yookassa.py   — ЮKassa v3. Рекуррент = POST /payments c payment_method_id (ЕДИНЫЙ
                  для карт/СБП/SberPay/T-Pay; charge и charge_qr — синонимы).
                  verify_payment нормализует статусы к T-Bank-форме {"Status": ...}.
                  Вебхуки не подписаны → parse_webhook верит только GET /payments/{id}.
  factory.py    — get_payment_provider (дефолт), get_yookassa_provider (env),
                  get_provider_for_user (чекаут-роутинг по YOOKASSA_TEST_USER_IDS),
                  get_provider_by_name (продления/возвраты по pm.provider)
  service.py    — бизнес-логика; _provider_for_sub для refund; тест-цена в
                  create_checkout_for_user; NSF-fallback card-1 работает на обоих
routers/billing.py — /api/billing/*; вебхуки: /webhook (T-Bank), /webhook/yookassa
                  (отдельный путь; 404 пока env пусты)
```

БД: `subscriptions` (yk_payment_id = id платежа ЛЮБОГО провайдера, legacy-имя),
`user_payment_methods` (provider/method_type/rebill_id/account_token; для ЮKassa
rebill_id И account_token = один и тот же payment_method.id, разнесены по method_type).

## Рунбук: проверить, включили ли рекурренты ЮKassa

Безвредная проба (создание платежа с save; при 403 ничего не создаётся,
при успехе — pending-платёж, сам протухает за час):

```bash
# Реквизиты взять из references/secrets.local.md и экспортнуть в env —
# НЕ инлайнить в команду (gitleaks режет паттерн `curl -u "x:y"` даже
# на плейсхолдерах — правило curl-auth-user, ловили на PR #310).
AUTH_B64=$(printf '%s' "${SHOP_ID}:${SECRET_KEY}" | base64)
curl -s https://api.yookassa.ru/v3/payments \
  -H "Authorization: Basic ${AUTH_B64}" \
  -H "Content-Type: application/json" -H "Idempotence-Key: $(uuidgen)" \
  -d '{"amount":{"value":"5.00","currency":"RUB"},"capture":true,
       "confirmation":{"type":"redirect","return_url":"https://xn--80aklbnczmv.xn--p1ai/billing/success"},
       "description":"Проба рекуррентов (НЕ ОПЛАЧИВАТЬ)","save_payment_method":true,
       "receipt":{"customer":{"email":"ermolaeffvadick@yandex.ru"},"items":[{"description":"Тест",
       "quantity":"1.00","amount":{"value":"5.00","currency":"RUB"},"vat_code":1,
       "payment_subject":"service","payment_mode":"full_payment"}]}}'
```
`403 forbidden «This store can't make recurring payments»` → ещё нет.
Создался объект платежа → ВКЛЮЧИЛИ → рунбук включения теста + напомнить Вадиму
отправить ВТОРОЙ запрос менеджеру (SberPay/T-Pay/СБП).

## Рунбук: включить тест ЮKassa (только по «го» Вадима)

1. В `/opt/frame/.env` добавить (значения — из secrets.local.md):
   `YOOKASSA_SHOP_ID=…`, `YOOKASSA_SECRET_KEY=…`, `YOOKASSA_TEST_USER_IDS=2`,
   `YOOKASSA_TEST_PRICE_RUB=5`
2. `cd /opt/frame && docker compose up -d --no-deps --force-recreate api` (env читается на старте).
3. Вадим (или мы по его команде): `UPDATE users SET role='free' WHERE id=2;`
   (мониторинг НЕ пострадает — health-ключ с 03.07 на id=3).
4. Вебхук в ЛК ЮKassa (если ещё нет): Интеграция → HTTP-уведомления →
   `https://xn--80aklbnczmv.xn--p1ai/api/billing/webhook/yookassa`,
   события payment.succeeded / payment.canceled / refund.succeeded.
5. Включить UI отвязки: `PAYMENT_METHODS_UI_ENABLED=true` (features.ts) + деплой фронта.
6. Смоук: `POST /api/billing/webhook/yookassa` c `{}` → 200 `{"ok":true}` (был 404).

**Тест-матрица (5₽, аккаунт id=2):** карта: оплата → в вебхуке payment_method.saved=true
→ pm в БД (provider=yookassa) → `POST /api/billing/admin/charge_recurrent` → списание без
участия → возврат. Потом то же: СБП (главный вопрос — сколько нажатий в приложении банка),
T-Pay, SberPay. Витринная цена на /pricing останется 2900₽ — списание будет 5₽ (это ок).

**Откат теста:** убрать YOOKASSA_* из .env + recreate api; id=2 → role='admin';
тест-подписки cancelled; ключ ЮKassa ПЕРЕВЫПУСТИТЬ в ЛК (засвечен в чате 03.07)
и обновить secrets.local.md.

## Gotchas

- ЮKassa min = 1₽ (СБП 1₽–700k) → 5₽ ок. Комиссии: карты/SberPay/T-Pay от 2,8% (+1% чек),
  СБП индивидуально — уточнить у менеджера.
- Проба SberPay+save давала ОТДЕЛЬНЫЙ отказ «method can't be saved» ещё ДО включения
  рекуррентов — после включения перепроверить и при отказе спросить менеджера явно.
- При СБП-платеже с save форма сама показывает только банки с поддержкой привязки.
- T-Bank остаётся дефолтом до решения о полном переезде; существующие rebill_id НЕ
  мигрируются — их продления живут в T-Bank, пока юзер не перепривяжется.
- admin не может чекаутиться (service гейтит) — потому для теста id=2 снимается в free.
- Имя в банковской выписке: FRAME (уже отправлено ЮKassa).
- История: ЮKassa уже жила в проекте и была удалена 2026-05-13 (88b7ef1) — не удалять
  повторно «как дубль», теперь это осознанный второй заход.

## Связанное

- memory: `billing_system.md` (живой статус), `trial_system.md`, `prod_reference.md`
  (реквизиты ИП, админы), `tier_gating.md`
- PR: #296 (СБП-QR кнопка убрана), #308 (UI отвязки + стирание токена), #309 (провайдер)
- Consent-модалка/consent-тексты: PricingPage.tsx; legal: /agreement /offer /recurring
