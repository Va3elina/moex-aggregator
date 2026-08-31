#!/usr/bin/env python3
"""Шаг В версии 2 целиком: бриф v2 + примеры из корпуса + факты о мире + короткий
промпт → черновик. Локальный прогон через `claude -p`.

Зачем локально, а не Routine: для сравнения с реально опубликованными постами нужен
СИНХРОННЫЙ ответ и возможность гонять варианты. Routine — fire-and-forget. Когда
вариант устоится, тот же промпт и та же сборка брифа переезжают в триггер.

Собирает четыре источника, каждый закрывает свою дыру старого Шага В:
  бриф v2         → честные числа вместо ATR-множителя (METRIC_MISMATCH.md);
  /content-corpus → примеры канала вместо 4 статичных образцов, с РАЗНЫМ числом
                     абзацев и распределением структуры;
  /world-facts    → «что было верно на дату» с отсечкой по времени. Именно этого не
                     хватало черновику 700 («что спровоцировало пробой — не указано»);
  промпт v2       → 2 743 знака вместо ~13 000: ворота вместо 13 правил стиля.
"""
import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import brief_v2  # noqa: E402

API = os.environ.get("FRAME_API", "https://framedata.ru")
PROMPT_PATH = os.path.join(HERE, "prompt_step_c_v2.md")


def _get(path: str, params: dict, token: str) -> dict:
    url = f"{API}{path}?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"X-Internal-Token": token})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def _post(path: str, body: dict, token: str) -> dict:
    req = urllib.request.Request(
        f"{API}{path}", data=json.dumps(body).encode(),
        headers={"X-Internal-Token": token, "Content-Type": "application/json"},
        method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def assemble(brief: dict, news: dict, examples: dict, facts: dict) -> str:
    prompt = open(PROMPT_PATH, encoding="utf-8").read()
    parts = [prompt, "\n# БРИФ\n", json.dumps(brief, ensure_ascii=False, indent=2)]

    parts.append("\n# НОВОСТЬ\n")
    parts.append(json.dumps(news, ensure_ascii=False, indent=2))

    parts.append("\n# ЧТО БЫЛО ВЕРНО НА ДАТУ НОВОСТИ\n")
    parts.append("Проверенные факты (им можно верить):\n")
    for f in facts.get("facts", []):
        parts.append(f"- {f['факт']} (с {f['действует_с']} по {f['действует_по']})\n")
    ctx = facts.get("news_context", [])
    if ctx:
        parts.append("\nИнформационный фон — это НЕ проверенные факты, а поводы. "
                      "Соседство по времени само по себе НЕ причина:\n")
        for n in ctx:
            parts.append(f"- [{n['когда']}] {n['заголовок']}\n")

    parts.append("\n# ПРИМЕРЫ ПОСТОВ КАНАЛА\n")
    st = examples.get("stats", {})
    if st:
        parts.append(f"Распределение канала: типично {st.get('абзацев_типично')} абзаца, "
                      f"диапазон {st.get('абзацев_диапазон')}, медиана длины "
                      f"{st.get('длина_медиана')} знаков, коридор {st.get('длина_коридор')}.\n")
    for i, e in enumerate(examples.get("examples", []), 1):
        parts.append(f"\n--- пример {i} ({e['абзацев']} абз., {e['знаков']} зн.) ---\n{e['текст']}\n")
    b = examples.get("backup_from_personal_channel")
    if b:
        parts.append(f"\n--- запасной, НЕ канал Frame ---\n{b['предупреждение']}\n{b['текст']}\n")

    parts.append("\nНапиши черновик. Только текст поста, без пояснений вокруг.\n")
    return "".join(parts)


def generate(full_prompt: str, model: str) -> str:
    """⚠️ Два слоя защиты от протечки инструкций окружения в черновик.

    1. Нейтральный cwd — чтобы не подхватился CLAUDE.md репозитория.
    2. Чистка вывода — потому что первого НЕДОСТАТОЧНО: output-style живёт в
       ПОЛЬЗОВАТЕЛЬСКИХ настройках, а не в проекте, и переживает смену каталога.
       Живой случай: в черновики про VK и Алросу протёк блок «★ Insight …» с
       рассуждением о задаче. Просьба в промпте «только текст поста» его не
       остановила, поэтому чистим детерминированно, а не уговорами."""
    import tempfile
    with tempfile.TemporaryDirectory() as neutral:
        proc = subprocess.run(
            ["claude", "-p", "--output-format", "json", "--model", model,
             # Корневой фикс протечки: свой системный промпт вместо дефолтного —
             # тогда пользовательский output-style не применяется вовсе.
             "--system-prompt",
             "Ты пишешь черновики постов для Telegram-канала. Отвечай ТОЛЬКО текстом "
             "поста: без вступлений, пояснений, рамок и блоков рассуждения.",
             "--disallowedTools", "Bash", "Edit", "Write", "Read", "Glob", "Grep",
             "WebSearch", "WebFetch"],
            input=full_prompt, capture_output=True, text=True, timeout=300,
            cwd=neutral)
        if proc.returncode != 0:
            raise RuntimeError(f"claude вышел с кодом {proc.returncode}: {proc.stderr[:300]}")
        return _strip_wrappers((json.loads(proc.stdout).get("result") or "").strip())


def _strip_wrappers(text: str) -> str:
    """Убрать блоки-обёртки, которые добавляет окружение, а не задача."""
    import re as _re
    # Блок «★ Insight …» до закрывающей линии из тире-заполнителей.
    text = _re.sub(r"★[^\n]*\n(?:.*?\n)??[─—-]{10,}\n?", "", text, flags=_re.S)
    # Одиночные линии-разделители, оставшиеся сиротами.
    text = _re.sub(r"(?m)^[─—-]{10,}\s*$\n?", "", text)
    return text.strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True)
    ap.add_argument("--news", required=True)
    ap.add_argument("--token-file", required=True)
    ap.add_argument("--ids", required=True)
    ap.add_argument("--model", default="sonnet")
    ap.add_argument("--out", help="каталог для черновиков")
    ap.add_argument("--show-prompt", action="store_true")
    a = ap.parse_args()

    token = open(a.token_file, encoding="utf-8").read().strip()
    series = brief_v2.load_series(a.series)
    news_by_id = {}
    for line in open(a.news, encoding="utf-8"):
        p = line.rstrip("\n").split("§")
        if len(p) >= 6:
            news_by_id[int(p[0])] = {"заголовок": p[1], "текст": p[2],
                                      "тикеры": p[3], "тип_события": p[4],
                                      "оценка_шага_А": p[5]}

    for cid in [int(x) for x in a.ids.split(",")]:
        d = series.get(cid)
        if not d or not d["meta"]:
            print(f"# {cid}: нет ряда позиций", file=sys.stderr); continue
        meta, news = d["meta"], news_by_id.get(cid, {})
        brief = brief_v2.build(cid, meta, d["rows"], news=None)
        tickers = [t for t in (news.get("тикеры") or "").split(",") if t]

        try:
            examples = _post("/api/internal/content-corpus/similar", {
                "headline": news.get("заголовок", ""), "raw_text": news.get("текст", ""),
                "tickers": tickers, "k": 3}, token)
        except urllib.error.HTTPError as e:
            examples = {}; print(f"# {cid}: примеры недоступны: {e}", file=sys.stderr)
        try:
            facts = _get("/api/internal/world-facts", {
                "as_of": brief["дата_новости"], "entities": ",".join(tickers),
                "window_days": 5, "limit_news": 8}, token)
        except urllib.error.HTTPError as e:
            facts = {}; print(f"# {cid}: факты недоступны: {e}", file=sys.stderr)

        full = assemble(brief, news, examples, facts)
        if a.show_prompt:
            print(full); continue
        draft = generate(full, a.model)
        print(f"\n{'='*70}\n### кандидат {cid} — {meta['asset_id']} "
              f"({meta['asset_name']}), новость {meta['news_date']}\n{'='*70}")
        print(draft)
        if a.out:
            os.makedirs(a.out, exist_ok=True)
            open(os.path.join(a.out, f"{cid}.txt"), "w", encoding="utf-8").write(draft)
            open(os.path.join(a.out, f"{cid}.prompt.txt"), "w", encoding="utf-8").write(full)


if __name__ == "__main__":
    main()
