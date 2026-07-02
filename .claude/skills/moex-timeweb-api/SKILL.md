---
name: moex-timeweb-api
description: Управление и диагностика прод-сервера Фрейм через Timeweb Cloud API (то, что SSH не даёт — статус/ребуты, метрики CPU/сеть, заказ/смена IP, rescue). Use when user says «проверь сервер через timeweb», «что с нагрузкой сервера», «сервер выключился/перезагрузился?», «заказать/сменить IP», «перезагрузи сервер», «метрики сервера», «timeweb api», или когда проблема на уровне ЖЕЛЕЗА/СЕТИ провайдера (напр. блэкхол MOEX → нужен новый IP), а не кода. НЕ для деплоя (это CI) и не для логов/БД (это SSH).
---

# Timeweb Cloud API — прод-сервер Фрейм

Слой, который SSH не покрывает: состояние ВМ, история ребутов, метрики, управление IP/сетью,
rescue-режим. Полезен когда «сервер странно себя ведёт» или нужен **обход сетевой блокировки MOEX**
сменой IP ([[moex_ip_ban]]).

## 🔐 Токен — БЕЗОПАСНОСТЬ (читать первым)
- Токен = **полный контроль над облаком**. НИКОГДА не коммить, не класть в репозиторий, в память
  (`memory/`), в `.env` внутри git, в этот файл. Скилл-директория git-трекается.
- **Хранение:** `~/.config/timeweb/token` (`chmod 600`) ИЛИ переменная `TIMEWEB_API_TOKEN`.
  Читать так: `TW=$(cat ~/.config/timeweb/token 2>/dev/null || echo "$TIMEWEB_API_TOKEN")`.
- Если токен засветился (чат/лог/скрин) — **отозвать и перевыпустить** в панели Timeweb
  (Настройки → API и терминал → API-ключи). Одноразовые сессионные копии держать только в
  session-scratchpad (вне репо), после сессии считать скомпрометированными.
- В `curl` передавать заголовком, не печатать: `-H "Authorization: Bearer $TW"`.

## Идентичность сервера (константы)
- **server_id = `7006331`**, имя «Wise Plover», зона **ru-3 / msk-1**, 2 vCPU / 4096 MB, без swap.
- IPv4 (main, floating): **`103.88.243.232`**, floating-ip **id `04f1b738-d10f-4f0d-bb5a-6fd3430fde6b`**.
- IPv6 (main): `2a03:6f00:a::2:a9f`.
- SSH-доступ и DNS `таймфрейм.рф` завязаны на `103.88.243.232` → **смена main-IP рвёт и SSH, и сайт**
  пока не обновишь DNS. Панель/VNC: timeweb.cloud/my/servers/7006331 (fallback при потере доступа).

## База и авторизация
```bash
TW=$(cat ~/.config/timeweb/token 2>/dev/null || echo "$TIMEWEB_API_TOKEN")
API=https://api.timeweb.cloud/api/v1
curl -s -H "Authorization: Bearer $TW" "$API/servers"        # проверка токена
```

## Эндпоинты (✅ = проверены вживую 2026-07-01)
**Чтение (безопасно, лей свободно):**
- ✅ `GET /servers` — список (id/name/status/location/cpu/ram/networks.ips).
- ✅ `GET /servers/{id}/ips` — IP сервера.
- ✅ `GET /floating-ips` — floating-IP аккаунта (наш 103.88.243.232 = floating).
- ✅ `GET /account/finances` — баланс (`balance` RUB, `hours_left`). На 01.07: ~2574₽ / ~1272ч.
- `GET /servers/{id}` — детали/статус конкретного сервера.
- `GET /servers/{id}/statistics?period=...` — метрики CPU/сеть/диск (свериться с api-docs по param).

**Действия (⚠️ ПИШУТ — только с явного согласия Вадима; сверить тело с https://timeweb.cloud/api-docs):**
- Ребут: `POST /servers/{id}/action` `{"action":"reboot"}` (или `hard_reboot`).
- Rescue/reinstall: `POST /servers/{id}/action` — см. docs (осторожно, reinstall стирает диск).
- Новый floating-IP: `POST /floating-ips` `{"availability_zone":"msk-1","is_ddos_guard":false}`.
- Привязать/отвязать: `POST /floating-ips/{id}/bind` / `/unbind` (resource_type=server, resource_id).
- Удалить IP: `DELETE /floating-ips/{id}`.

## Плейбуки

### 1. «Сервер выключился ночью?» / странная нагрузка
Сначала факты, не догадки:
- API: `GET /servers/7006331` → `status` (on/off) — реально ли выключался.
- SSH-кросс-чек: `uptime` + `last -x reboot | head` — был ли ребут (uptime в днях = не было).
- Нагрузка: SSH `docker stats --no-stream` — кто ест CPU/RAM (обычно `frame-db-1`).
- **Инцидент 01.07 (референс):** «высокая нагрузка» = один **застрявший SELECT в БД жил 3 суток**,
  пёк ядро (db CPU 101%). Диагноз: `pg_stat_activity WHERE state<>'idle'`; лечение:
  `SELECT pg_cancel_backend(<pid>)` (для SELECT безопасно) → CPU 101%→0%. Корень был: на app-соединениях
  НЕТ `statement_timeout` — закрыто PR #276 (120s на app-движке); вечный stuck-query больше не воспроизводится.

### 2. Обход сетевой блокировки MOEX сменой IP ([[moex_ip_ban]])
Когда `103.88.243.232` в блэкхоле к MOEX, но MOEX «блок не ставил» (дроп на сетевой кромке):
1. **Сначала диагностика, не смена вслепую:** подними дешёвый throwaway-сервер в ДРУГОЙ зоне
   (`POST /servers`), с него протестируй `curl iss.moex.com` — если достаёт, проблема IP-специфична
   (наш IP/подсеть) → смена IP поможет; если тоже блэкхол — весь путь Timeweb→MOEX мёртв → IP не спасёт,
   нужен релей через не-Timeweb egress. Удали throwaway после теста.
2. Если IP-специфично: закажи новый floating-IP, привяжи, **сначала обнови DNS `таймфрейм.рф`** на
   новый IP (или держи оба), потом переключай main — иначе потеряешь SSH/сайт. VNC-консоль = страховка.

### 3. Плановый ребут (редко)
`GET /servers/{id}` статус → `POST /servers/{id}/action {"action":"reboot"}` → poll статус до `on` →
sub-agent `moex-deploy-verifier` (контейнеры/эндпоинты поднялись).

## Правила безопасности
- Read-only — свободно. **Reboot / смена IP / reinstall / delete — ТОЛЬКО с подтверждения Вадима.**
- **НИКОГДА** не удалять сервер, не reinstall'ить (стирает диск с БД!) без явного «да».
- Не двигать деньги/тарифы. Смену main-IP — только с планом DNS + VNC-fallback.
- Для рутинной диагностики используй sub-agent `moex-timeweb-ops` (read-only отчёт).

Связано: [[moex_ip_ban]], moex-server-access, deploy_manual, monitoring_system.
