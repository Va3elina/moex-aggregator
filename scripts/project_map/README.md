# Карта Фрейма (инвентаризация)

Генератор страницы https://claude.ai/code/artifact/3d441bb4-b56d-47dc-8dbf-183a3b46ae27

Входы (снимки от 03.09.2026, пересобираются вручную):
- `db_tables.txt` — все таблицы прода: строки, размер, колонки (запрос к pg_class + information_schema);
- `freshness.txt` — max(дата) по каждой таблице (per-statement timeout 8с, candles пропускается);
- `repo_inventory.txt` — роутеры, маршруты фронта, шаги оркестратора, cron, контейнеры.

Сборка: `python gen_map.py` → `frame_map.html` (пути внутри скрипта — scratchpad сессии, поправить на эту папку).
Области (DOM в gen_map.py) заполнены руками: источник → шаги → таблицы → API → страницы.
