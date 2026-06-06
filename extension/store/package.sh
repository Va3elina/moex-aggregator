#!/usr/bin/env bash
# Упаковка расширения «Фрейм» в zip для Chrome Web Store / каталога Яндекса.
# Whitelist-подход: кладём в zip только рантайм-файлы, мусор (store/, _metadata,
# .DS_Store, dev-preview.html, README.md) не попадает.
#
# Использование:  bash extension/store/package.sh
# Результат:      extension/store/build/frame-extension-vX.Y.Z.zip
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EXT="$ROOT/extension"

# Версия из манифеста (+ валидация JSON)
python3 -c "import json,sys; json.load(open('$EXT/manifest.json'))" \
  || { echo "✗ manifest.json — невалидный JSON"; exit 1; }
VERSION="$(python3 -c "import json;print(json.load(open('$EXT/manifest.json'))['version'])")"

OUT="$EXT/store/build"
mkdir -p "$OUT"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

# Whitelist рантайм-файлов
cp "$EXT/manifest.json" "$EXT/rules.json" "$STAGE/"
cp -R "$EXT/src" "$EXT/icons" "$STAGE/"

# Срезаем мусор, если затесался
find "$STAGE" -name '.DS_Store' -delete
find "$STAGE" -type d -name '_metadata' -prune -exec rm -rf {} + 2>/dev/null || true

ZIP="$OUT/frame-extension-v$VERSION.zip"
rm -f "$ZIP"
( cd "$STAGE" && zip -r -q "$ZIP" . )

echo "✓ собрано: $ZIP"
echo "содержимое:"
unzip -l "$ZIP" | sed 's/^/   /'
