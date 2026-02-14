import subprocess
import sys
import time
import re
import threading
import os


def build_frontend():
    """Собирает фронтенд"""
    frontend_dir = os.path.join(os.path.dirname(__file__), "frontend")
    print("📦 Сборка фронтенда...")
    result = subprocess.run(
        ["npm", "run", "build"],
        cwd=frontend_dir,
        shell=True,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print("❌ Ошибка сборки фронтенда:")
        print(result.stderr)
        sys.exit(1)
    print("✅ Фронтенд собран!\n")


def run_server():
    """Запускает FastAPI сервер"""
    subprocess.run([
        sys.executable, "-m", "uvicorn",
        "api.main:app",
        "--host", "0.0.0.0",
        "--port", "8080"
    ])


def run_tunnel():
    """Запускает Cloudflare туннель и выводит ссылку"""
    process = subprocess.Popen(
        ["cloudflared", "tunnel", "--url", "http://localhost:8080", "--protocol", "http2"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    for line in process.stdout:
        match = re.search(r'https://[a-zA-Z0-9-]+\.trycloudflare\.com', line)
        if match:
            url = match.group(0)
            print("\n" + "=" * 60)
            print("🚀 САЙТ ДОСТУПЕН ПО ССЫЛКЕ:")
            print(f"   {url}")
            print("=" * 60 + "\n")

        if "ERR" in line or "error" in line.lower():
            print(f"[TUNNEL] {line.strip()}")


if __name__ == "__main__":
    print("=" * 60)
    print("   MOEX Analytics - Запуск")
    print("=" * 60)
    
    # Сборка фронтенда
    build_frontend()
    
    print("Запускаю сервер и туннель...")
    print("Подожди 5-10 секунд...\n")

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()

    time.sleep(2)

    try:
        run_tunnel()
    except KeyboardInterrupt:
        print("\n\nОстановка...")