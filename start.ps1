# Запуск Фрейм — бэкенд (FastAPI :8000) + фронтенд (Vite :5173)

$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path

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
$backend = Start-Process powershell -ArgumentList "-NoProfile -Command cd '$ROOT'; .\.venv\Scripts\activate; uvicorn main:app --host 127.0.0.1 --port 8000 --reload" -PassThru -WindowStyle Normal

Start-Sleep -Seconds 2

# ── Фронтенд ────────────────────────────────────────────────────────────────
Write-Host "Запускаю фронтенд (порт 5173)..." -ForegroundColor Cyan
$frontend = Start-Process powershell -ArgumentList "-NoProfile -Command cd '$ROOT\frontend'; npm run dev" -PassThru -WindowStyle Normal

Start-Sleep -Seconds 2

Write-Host "`n✓ Сайт: http://127.0.0.1:5173" -ForegroundColor Green
Write-Host "✓ API:  http://127.0.0.1:8000/docs" -ForegroundColor Green
Write-Host "`nПорты PID: backend=$($backend.Id)  frontend=$($frontend.Id)"
Write-Host "Для остановки запусти: .\stop.ps1`n"
