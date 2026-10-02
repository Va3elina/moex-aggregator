"""Локальный приёмник выгрузок из вкладки браузера (Bybit запрещает fetch на localhost через CSP,
но обычная отправка формы — это навигация, её CSP connect-src не касается).
Запуск: .venv/bin/python -m factory.receiver  → слушает 127.0.0.1:8765, пишет в inbox/<name>."""
import http.server, urllib.parse
from pathlib import Path
INBOX = Path(__file__).resolve().parents[1] / "inbox"
INBOX.mkdir(exist_ok=True)

class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(n).decode("utf-8", "replace")
        f = urllib.parse.parse_qs(raw, keep_blank_values=True)
        name = Path((f.get("name") or ["dump.txt"])[0]).name
        data = (f.get("data") or [""])[0]
        (INBOX / name).write_text(data)
        self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.end_headers()
        self.wfile.write(f"<p>сохранено inbox/{name}: {len(data)} символов</p><p><a href='https://www.bybit.com/copyTrade/'>назад на Bybit</a></p>".encode())
        print("saved", name, len(data), flush=True)
    def log_message(self, *a): pass

http.server.HTTPServer(("127.0.0.1", 8765), H).serve_forever()
