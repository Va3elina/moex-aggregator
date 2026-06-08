# Telegram-связь Фрейма: как устроено, что было, что делать

Единый источник правды: **как прод-хост Фрейма (РФ) достаёт Telegram, несмотря на блокировку РКН.**
Сюда же — диагностика на случай «бот молчит / пуши не идут / канал замолк».

> TL;DR: РКН блокирует Telegram по **IPv4**. У нас **два независимых обходных пути**:
> 1. **Cloudflare Worker релей** (ОСНОВНОЙ) — хост ходит по обычному IPv4 в Worker, тот форвардит в Telegram.
> 2. **Прямой IPv6** (BACKUP) — Telegram по IPv6 РКН (пока) не блокирует; у хоста статический IPv6.
> Переключение между путями = одна строка `TELEGRAM_API_ROOT` в `/opt/frame/.env`.

---

## 1. Суть проблемы

- Прод-хост: `root@103.88.243.232`, Timeweb, **РФ**.
- РКН блокирует Telegram, занося его **IPv4-адреса** (`149.154.x.x`) в чёрный список DPI. Исходящий с хоста
  на IPv4 Telegram → `No route to host` / timeout.
- **IPv6-адреса Telegram** (`2001:67c:4e8:f004::9`) РКН в блок (пока) НЕ внёс → по IPv6 проходит.
- Всё НЕ-Telegram (github, MOEX, Cloudflare, любой сайт) с хоста доступно нормально — блок только на Telegram.

`api.telegram.org` — dual-stack: есть и A (IPv4 `149.154.166.110`), и AAAA (IPv6 `2001:67c:4e8:f004::9`).

---

## 2. Что было (инцидент 19 мая → 7-8 июня)

**Симптом:** alert-бот не poll'ил, пуши не уходили, канал signal-engine `@frametoolsignal` молчал с **19 мая**
(никто не заметил 19 дней — не было диагностики, отсюда этот файл).

**Диагноз:** у хоста **пропал глобальный IPv6-адрес** `2a03:6f00:a::2:a9f`.
- `ip -6 addr show scope global` → **пусто** (только link-local `fe80::`).
- netplan был `dhcp4:true` + `dhcp6:true` + `accept-ra:false` (`51-dhcp6.yaml`, с дек) → хост брал IPv6 через
  **DHCPv6**. Timeweb перестал выдавать DHCPv6-lease (~19 мая) → адреса не стало.
- Без глобального IPv6 → `requests`/`curl` падали на IPv4 (заблокирован) → Telegram недостижим.
- `networkctl renew eth0` НЕ помог (`IPv6 Address Generation Mode: none`, сервер DHCPv6 молчит).
- **Это была инфра, не код** — connectivity бота не менялась.

**Важно:** адрес `2a03:6f00:a::2:a9f` всё ещё **маршрутизировался** на хост — Timeweb перестал его *выдавать*,
но не *забрал*. Поэтому его удалось вернуть статикой (см. ниже).

---

## 3. Что сделали (два пути)

### Путь A — прямой IPv6 (BACKUP), статикой в netplan

Файл **`/etc/netplan/52-static-ipv6.yaml`** (perms 600):
```yaml
network:
  version: 2
  ethernets:
    eth0:
      addresses:
        - 2a03:6f00:a::2:a9f/64
      routes:
        - to: "::/0"
          via: "fe80::9cce:edff:fee6:7340"
          on-link: true
```
⚠️ **ГРАБЛИ:** нужны И адрес, И **дефолт-маршрут**. `accept-ra:false` → RA-маршрут сам не ставится; первый
`netplan apply` только с адресом (без route) → egress сдох. `via` — link-local шлюз роутера + `on-link: true`.

После: `getaddrinfo` отдаёт IPv6 первым (RFC 6724) → плейн `requests` без форса IPv6 идёт по IPv6 → работает
(код бота/сендера НЕ меняли). `requests getMe` → ok ~160мс.

### Путь B — Cloudflare Worker релей (ОСНОВНОЙ), durable

```
[хост, РФ] --обычный IPv4 (Cloudflare не заблок.)--> [CF Worker, вне РФ] --> [api.telegram.org]
```
Хост **не коннектится к Telegram напрямую** — только в релей. Не зависим ни от IPv6 хоста, ни от того,
заблокирует ли РКН IPv6 Telegram. Паттерн «как почта»: store-and-forward через посредника.

- **Worker:** `frame-tg-relay.ermolaeffvadick.workers.dev` (аккаунт Вадима, CF free).
- **Код:** `signals/relay/cf-worker.js` (прозрачный прокси на `api.telegram.org`).
- **Защита:** секрет в первом сегменте пути (env `RELAY_SECRET` в Worker). Корень без секрета → `forbidden`.
- **Включение на проде:** `/opt/frame/.env` →
  `TELEGRAM_API_ROOT=https://frame-tg-relay.ermolaeffvadick.workers.dev/<RELAY_SECRET>`
  (`RELAY_SECRET` = тот же, что в CF Worker → Variables; см. password manager / `.env`).
- **Код Фрейма:** `alert_bot.py`, `alert_notify.py`, `publish/telegram.py` строят базу из `TELEGRAM_API_ROOT`
  (дефолт `https://api.telegram.org` → если env снять, вернётся прямой Telegram).
- Проверено: getMe/getUpdates через релей ok по IPv4 (0.17с); бот poll'ит чисто; eval-loop чистый.

---

## 4. Все опции (меню обходов, на будущее)

| Опция | Как | Плюсы | Минусы |
|---|---|---|---|
| **CF Worker релей** ✅ *(используем)* | хост→Worker→TG, `TELEGRAM_API_ROOT` | бесплатно, без своего сервера, не зависит от IPv6/РКН | `workers.dev` РКН иногда придушивает → лечится своим доменом; long-poll на Worker чуть капризен (бот переподключится) |
| **Прямой IPv6** ✅ *(backup)* | статик-адрес в netplan | ноль внешней инфры, не VPN | хрупко: Timeweb может отозвать адрес; РКН *может* однажды заблокировать и IPv6 TG |
| **VPS abroad + nginx reverse-proxy** | $3-4/мес, nginx proxy_pass на api.telegram.org, `TELEGRAM_API_ROOT`→VPS | самый надёжный для long-poll, полный контроль | +1 сервер на обслуживание, деньги |
| **CF Worker на своём домене** | Worker + Custom Domain/Route | устойчив к троттлингу workers.dev | нужен домен на Cloudflare (DNS) |
| **Webhook вместо long-poll** | TG→релей(webhook)→наш backend; backend шлёт через релей | нет 30с long-poll (легче Worker'у) | переписать бота с long-poll на webhook-хендлер |
| **VPN/туннель** ❌ | wireguard/openvpn на хост | — | Вадим против; лишний слой; не нужно — релея хватает |

**Что НЕ работает:** прямой webhook на наш RU-сервер — наш SYN-ACK назад к Telegram идёт на заблок. IP TG → не
проходит (нужен узел вне РФ всё равно). Почта как транспорт — медленно, односторонне, не годится для бота.

---

## 5. Runbook — «Telegram молчит, что делать»

### Диагностика (по порядку)
```bash
# 1. Релей вообще отвечает? (с хоста; SEC и TOK из /opt/frame/.env)
W=https://frame-tg-relay.ermolaeffvadick.workers.dev
curl -s "$W/"                                   # ждём "forbidden" = Worker жив
curl -s "$W/<RELAY_SECRET>/bot<TOKEN>/getMe"    # ждём {"ok":true,...}

# 2. Какой путь сейчас активен?
grep TELEGRAM_API_ROOT /opt/frame/.env          # есть = релей; нет = прямой IPv6

# 3. Прямой IPv6 жив? (backup-путь)
ip -6 addr show dev eth0 scope global           # должен быть 2a03:6f00:a::2:a9f (пусто=беда)
ip -6 route show default                         # должен быть default via fe80::... onlink
curl -6 -s https://api64.ipify.org              # вернёт наш IPv6 = egress жив (пусто=IPv6 мёртв)

# 4. Бот poll'ит чисто?
tac /opt/frame/logs/alert_bot.log | awk '{print} /alert_bot started/{exit}' | tac   # пусто после старта = ок
```

### Действия
- **Релей сломался (Worker/CF):** снять `TELEGRAM_API_ROOT` из `.env` → `systemctl restart frame-alert-bot`
  → вернётся прямой IPv6 (если он жив). ИЛИ повесить Worker на свой домен.
- **Прямой IPv6 пропал** (повтор инцидента): проверить `/etc/netplan/52-static-ipv6.yaml` на месте →
  `netplan apply` → проверить адрес+маршрут (п.3). Если адрес перестал маршрутизироваться (Timeweb забрал) —
  использовать релей (путь B) и/или писать в Timeweb.
- **И релей, и IPv6 легли:** поднять VPS-релей abroad (см. опции) ИЛИ разбираться с Cloudflare/Timeweb.
- После любого фикса: `systemctl restart frame-alert-bot`; eval-loop и канал подхватят cron'ом (читают `.env`).

---

## 6. Ключевые факты (шпаргалка)

- **Хост:** `root@103.88.243.232` (Timeweb, РФ). SSH: `-o IdentitiesOnly=yes -o IdentityAgent=none -i ~/.ssh/id_ed25519`.
- **Бот:** `@framesignalbot` (id 8628310242, «Frame_Signal»). Канал: `@frametoolsignal`.
- **Статический IPv6 хоста:** `2a03:6f00:a::2:a9f/64`, шлюз `fe80::9cce:edff:fee6:7340` (netplan 52-static-ipv6.yaml).
- **Релей:** `frame-tg-relay.ermolaeffvadick.workers.dev`, секрет `RELAY_SECRET` (в `.env` + CF Variables).
- **Env-переключатель:** `TELEGRAM_API_ROOT` в `/opt/frame/.env` (дефолт в коде = `https://api.telegram.org`).
- **Сервисы:** бот — `systemctl {status,restart} frame-alert-bot` (host systemd). eval-loop — cron `2-59/5`
  (`/opt/frame/signals/alerts_run.sh`). Канал — cron `0 * * * *` (`/opt/frame/signals/run.sh`).
- **Код:** релей `signals/relay/cf-worker.js`; базы TG в `signals/{alert_bot,alert_notify,publish/telegram}.py`.

Связано: память `signal_engine.md`, `.claude/HANDOFF.md`, `signals/relay/README.md` (деплой Worker).
