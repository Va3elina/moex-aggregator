#!/usr/bin/env bash
# Генерирует /frontend/public/logos/manifest.json — список тикеров
# у которых есть лого. Используется на фронте для prefetch'а всех
# лого один раз при входе на страницу с инструмент-селектором.
#
# Запускается автоматически перед `npm run build` (см. package.json
# "prebuild" hook). Можно запустить вручную:
#   bash scripts/build-logos-manifest.sh
set -euo pipefail

LOGOS_DIR="$(cd "$(dirname "$0")/.." && pwd)/frontend/public/logos"

# Собираем все тикеры (без расширения) у которых есть .png/.svg/.webp
TICKERS=$(
  for f in "$LOGOS_DIR"/*.png "$LOGOS_DIR"/*.svg "$LOGOS_DIR"/*.webp; do
    [ -e "$f" ] || continue
    basename "$f" | sed 's/\.[^.]*$//'
  done | sort -u
)

# Пишем JSON-массив
{
  echo '{'
  echo '  "tickers": ['
  first=true
  while IFS= read -r t; do
    [ -z "$t" ] && continue
    if [ "$first" = true ]; then
      printf '    "%s"' "$t"
      first=false
    else
      printf ',\n    "%s"' "$t"
    fi
  done <<< "$TICKERS"
  echo
  echo '  ]'
  echo '}'
} > "$LOGOS_DIR/manifest.json"

count=$(echo "$TICKERS" | wc -l | tr -d ' ')
echo "Сгенерирован manifest.json: $count тикеров"
