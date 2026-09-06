#!/usr/bin/env bash
# Прод-деплой (выполняется НА сервере через SSH из deploy-prod workflow).
# Идемпотентен: reset --hard origin/main + rebuild + recreate. Безопасно
# перезапускать (ретраи SSH при флапающей сети могут вызвать его повторно).
set -e
cd /opt/frame

before=$(git rev-parse HEAD)
# reset --hard, НЕ pull: деплой-таргет = ЧИСТЫЙ checkout origin/main.
git fetch origin main
git reset --hard origin/main
after=$(git rev-parse HEAD)
changed=$(git diff --name-only "$before" "$after")
echo "=== Changed files ==="; echo "$changed"

# ─── api: ZERO-DOWNTIME rolling recreate ──────────────────────────────────
# Раньше: `up -d --force-recreate api` глушил старый контейнер ДО готовности
# нового → ~6-10с окно 502 на КАЖДЫЙ деплой (буто app ~6с + resolver-кеш nginx
# до 10с). Юзер, открывший индикатор в это окно, ловил «Ошибка загрузки».
# Теперь: поднимаем НОВЫЙ api рядом со старым (scale=2), старый держит трафик,
# ждём готовности нового (/health), затем убираем старый. nginx по docker-DNS
# (resolver valid + переменный upstream `api`) видит оба IP и балансит — разрыва
# нет. Любой сбой rolling → fallback на старый force-recreate (надёжность
# деплоя важнее zero-downtime). ⚠️ Выживший контейнер чередует имя frame-api-1/-2
# между деплоями → `docker exec` резолвить динамически по label, не по имени.
deploy_api_rolling() {
  docker compose build api

  # Снять висячие renamed-контейнеры от прерванного предыдущего rolling/recreate
  # (иначе "container name ..._frame-api-N already in use" → деплой падал).
  # У живых контейнеров имя без ведущего `_`, поэтому фильтр их не заденет.
  docker rm -f $(docker ps -aq --filter 'name=_frame-api') 2>/dev/null || true

  local old_id
  old_id=$(docker ps -q --filter 'label=com.docker.compose.service=api' | head -1)

  # Холодный старт (api не запущен) — обычный up, ждать/балансить нечего.
  if [ -z "$old_id" ]; then
    docker compose up -d api
    return 0
  fi

  # ⚠️ Страховка по памяти (инцидент 06.09.2026): второй api рядом со старым нужен
  # только если ему есть куда встать. Старый контейнер на 3 ГБ + база 3 ГБ + новый
  # контейнер = OOM-killer, 25 минут 503 и мёртвый SSH. Ниже порога — честное
  # пересоздание с коротким 502-окном, а не рулетка с OOM.
  local avail_mb
  avail_mb=$(free -m | awk 'NR==2{print $7}')
  if [ -n "$avail_mb" ] && [ "$avail_mb" -lt 2500 ]; then
    echo "!! свободной памяти ${avail_mb} МБ < 2500 → rolling небезопасен, force-recreate (короткое 502-окно)"
    docker compose up -d --no-deps --force-recreate --scale api=1 api
    return 0
  fi

  # Поднять ВТОРОЙ api на новом образе, старый не трогаем (--no-recreate).
  if ! docker compose up -d --no-deps --no-recreate --scale api=2 api; then
    echo "!! scale=2 не удался → fallback на force-recreate (будет короткое 502-окно)"
    # --no-deps ОБЯЗАТЕЛЕН: без него compose «доводит» и зависимости (db/redis)
    # до текущего конфига — при отложенных db-правках (pg_stat_statements)
    # это внезапный рестарт БД посреди деплоя. db пересоздаём только вручную.
    docker compose up -d --no-deps --force-recreate --scale api=1 api
    return 0
  fi

  # Найти НОВЫЙ контейнер (id != old) и дождаться готовности приложения.
  # Прямой probe /health внутри контейнера (~6с), НЕ docker healthcheck:
  # у него interval=30s/без start_period → статус healthy только к ~30с.
  local new_id="" ready=""
  for _ in $(seq 1 60); do
    new_id=$(docker ps -q --filter 'label=com.docker.compose.service=api' | grep -v "$old_id" | head -1)
    if [ -n "$new_id" ]; then
      if docker exec "$new_id" curl -fsS -o /dev/null --max-time 3 http://localhost:8000/health 2>/dev/null; then
        ready=1; break
      fi
    fi
    sleep 1
  done

  if [ -z "$ready" ]; then
    echo "::error:: новый api не ответил на /health за 60с — откат, оставляем старый"
    [ -n "$new_id" ] && docker rm -f "$new_id" 2>/dev/null || true
    return 1
  fi

  # DRAIN перед удалением старого: дождаться, пока nginx ре-резолвит docker-DNS и
  # включит НОВЫЙ контейнер в свой upstream-набор. resolver valid=10s → ждём 12с.
  # Без этого nginx в момент удаления старого может держать в кеше ТОЛЬКО старый IP
  # (новый ещё не зарезолвлен), и запрос упрётся в мёртвый IP БЕЗ живого пира для
  # failover → 504/timeout (поймано вживую). За время drain ОБА контейнера живы и
  # отвечают (nginx балансит) → downtime = 0. После drain у nginx в наборе {старый,
  # новый}: убийство старого → запрос на мёртвый IP уходит на новый за
  # proxy_connect_timeout 3s (nginx/conf.d/default.conf) → 200, без ошибки.
  sleep 12

  # Новый прогрет в nginx — убираем старый. Остаётся 1 живой api (новый образ).
  docker rm -f "$old_id" 2>/dev/null || true
  echo "=== api rolling OK: старый ${old_id:0:12} → новый ${new_id:0:12} (/health 200, drained) ==="
}

deploy_api_rolling

# orchestrator — ОТДЕЛЬНЫЙ image; пересобираем только если менялся его код.
# ⚠️ Список ОБЯЗАН повторять COPY-строки Dockerfile (кроме frontend/ и api/,
# которые едут в api-образ). Пропущенный путь = МОЛЧАЛИВЫЙ отказ: CI зелёный,
# деплой «успешен», а оркестратор крутит старый код. Так CBR/ не доезжал до
# прода — правка парсера ОРФР ушла в main, но ингест шёл старым кодом.
# Меняешь COPY в Dockerfile → правь и этот regex.
# ⚠️ Список папок — ЯВНЫЙ, и он уже подводил: Brain/ и Company/ в нём не было, деплой
# #1413 обновил api, а оркестратор остался на старом brain_sync и раз в 15 минут
# откатывал слой доверия. Любой новый каталог со скриптами оркестратора — сюда И в Dockerfile.
if echo "$changed" | grep -qE '^(OI/|Funds/|Candles/|Macro/|Commodity/|Crypto/|CBR/|Company/|Brain/|signals/|main_orchestrator\.py|pipeline_heartbeat\.py|moex_calendar\.py|moex_tls\.py|tg_bot\.py|backup_db\.sh|requirements\.txt|Dockerfile$)'; then
  echo "=== Orchestrator code changed -> rebuild ==="
  docker rm -f $(docker ps -aq --filter 'name=_frame-orchestrator-1') 2>/dev/null || true
  docker compose build orchestrator
  # --no-deps: иначе up пересоздаст и db, если её compose-конфиг менялся
  # (см. комментарий в fallback выше) — даунтайм БД в случайный момент.
  docker compose up -d --no-deps --force-recreate orchestrator
fi

# alert-bot — host-side systemd; рестарт только если менялся его код
if echo "$changed" | grep -qE '^signals/alert_bot\.py$'; then
  echo "=== alert_bot changed -> systemctl restart ==="
  systemctl restart frame-alert-bot
fi

# content-review-bot — тоже host-side systemd long-poll (не докер, не крон).
# Найдено 2026-07-16: без этого правила код бота молча оставался устаревшим
# после деплоя сколь угодно долго (реальный инцидент — бот не работал НИ РАЗУ
# с момента создания, т.к. рестарт после фиксов никто не делал вручную).
if echo "$changed" | grep -qE '^signals/content_review_bot\.py$'; then
  echo "=== content_review_bot changed -> systemctl restart ==="
  systemctl restart frame-content-review-bot
fi

# nginx — конфиг монтируется в frame-nginx-1; git reset кладёт новый файл, но
# nginx подхватит его только по reload. Валидируем (nginx -t) и reload'им БЕЗ
# рестарта (graceful, без разрыва соединений). Только если менялся conf.
if echo "$changed" | grep -qE '^nginx/'; then
  echo "=== nginx config changed -> test + reload ==="
  if docker exec frame-nginx-1 nginx -t; then
    docker exec frame-nginx-1 nginx -s reload
  else
    echo "::error:: nginx -t провалился — НЕ reload'им (старый конфиг остаётся живым)"
  fi
fi

echo "=== HEAD на проде: $(git rev-parse --short HEAD) ==="
