# Останавливает все процессы на портах 8000 и 5173

foreach ($port in @(8000, 5173)) {
    $pids = (netstat -ano | Select-String ":$port\s" | ForEach-Object {
        ($_ -split '\s+')[-1]
    } | Sort-Object -Unique)
    foreach ($p in $pids) {
        if ($p -match '^\d+$' -and $p -ne '0') {
            Write-Host "Останавливаю порт $port (PID $p)" -ForegroundColor Yellow
            Stop-Process -Id $p -Force -ErrorAction SilentlyContinue
        }
    }
}

# Дополнительно убиваем uvicorn и vite по имени процесса
Get-Process -Name "uvicorn" -ErrorAction SilentlyContinue | Stop-Process -Force
Get-Process -Name "node" -ErrorAction SilentlyContinue | Where-Object { $_.MainWindowTitle -match "vite|npm" } | Stop-Process -Force

Write-Host "Готово." -ForegroundColor Green
