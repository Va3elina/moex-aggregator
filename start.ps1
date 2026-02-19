# Запуск Фрейм — бэкенд (FastAPI :8000) + фронтенд (Vite :5173) + Cloudflare Tunnel

$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
$CLOUDFLARED = "$ROOT\cloudflared.exe"

# ── Убиваем старые процессы на портах 8000 и 5173 ──────────────────────────
foreach ($port in @(8000, 5173)) {
    $pids = (netstat -ano | Select-String ":$port\s" | ForEach-Object {
        ($_ -split '\s+')[-1]
    } | Sort-Object -Unique)
    foreach ($p in $pids) {
        if ($p -match '^\d+$' -and $p -ne '0') {
            Write-Host "Освобождаю порт $port (PID $p)..." -ForegroundColor Yellow
            Stop-Process -Id $p -Force -ErrorAction SilentlyContinue
        }
    }
}

Start-Sleep -Milliseconds 500

# ── Бэкенд ─────────────────────────────────────────────────────────────────
Write-Host "`nЗапускаю бэкенд (порт 8000)..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoProfile -Command `"cd '$ROOT'; .\.venv\Scripts\activate; uvicorn main:app --host 127.0.0.1 --port 8000 --reload`"" -WindowStyle Normal

Start-Sleep -Seconds 2

# ── Фронтенд ────────────────────────────────────────────────────────────────
Write-Host "Запускаю фронтенд (порт 5173)..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoProfile -Command `"cd '$ROOT\frontend'; npm run dev`"" -WindowStyle Normal

Start-Sleep -Seconds 2

# ── Cloudflare Tunnel ────────────────────────────────────────────────────────
if (Test-Path $CLOUDFLARED) {
    Write-Host "Запускаю Cloudflare Tunnel..." -ForegroundColor Cyan

    # Запускаем туннель и перехватываем вывод чтобы найти URL
    $tunnelLog = "$ROOT\tunnel.log"
    $tunnelProc = Start-Process -FilePath $CLOUDFLARED `
        -ArgumentList "tunnel --url http://127.0.0.1:5173 --no-autoupdate" `
        -RedirectStandardError $tunnelLog `
        -WindowStyle Hidden -PassThru

    # Ждём появления URL в логе (до 15 секунд)
    $url = $null
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Milliseconds 500
        if (Test-Path $tunnelLog) {
            $content = Get-Content $tunnelLog -Raw -ErrorAction SilentlyContinue
            if ($content -match 'https://[a-z0-9\-]+\.trycloudflare\.com') {
                $url = $matches[0]
                break
            }
        }
    }

    Write-Host ""
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor DarkGray
    Write-Host "  Локально:  http://127.0.0.1:5173" -ForegroundColor Green
    if ($url) {
        Write-Host "  Публично:  $url" -ForegroundColor Magenta
        Write-Host ""
        Write-Host "  (Скопируй ссылку и открой в любом браузере/устройстве)" -ForegroundColor DarkGray
        # Копируем публичный URL в буфер обмена
        $url | Set-Clipboard
        Write-Host "  ✓ URL скопирован в буфер обмена" -ForegroundColor DarkGray
    } else {
        Write-Host "  Туннель запущен, URL ещё не определён — проверь tunnel.log" -ForegroundColor Yellow
    }
    Write-Host "  API docs:  http://127.0.0.1:8000/docs" -ForegroundColor DarkGray
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor DarkGray
} else {
    Write-Host "`n✓ Локально: http://127.0.0.1:5173" -ForegroundColor Green
    Write-Host "  (cloudflared.exe не найден — публичный URL недоступен)" -ForegroundColor Yellow
}

Write-Host "`nДля остановки: .\stop.ps1`n"
