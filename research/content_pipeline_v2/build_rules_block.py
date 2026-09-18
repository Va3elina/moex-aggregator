"""Общий блок «Правила редактора» в промптах писателей и судьи — из editor_rules.yaml.

Правка Вадима вносится ОДИН раз — в реестр, — а этот скрипт раскладывает её по промптам тех конвейеров,
которых она касается. До 18.09 правило правили вручную в трёх промптах, и оно доходило не везде (разбор
завода 18.09: «правки будто не касаются»).

    python research/content_pipeline_v2/build_rules_block.py          # обновить блоки
    python research/content_pipeline_v2/build_rules_block.py --check  # только проверить (код 1, если разошлись)

В блок идут правила, которые исполняет писатель или судья (card / check / judge / prompt). Правила,
которые держит только отсев или данные (filter / data), писателю не нужны — кандидат до него не доходит.
"""
import pathlib
import re
import sys

import yaml

HERE = pathlib.Path(__file__).resolve().parent
BEGIN = "<!-- ПРАВИЛА РЕДАКТОРА: начало — генерируется из editor_rules.yaml, руками не править -->"
END = "<!-- ПРАВИЛА РЕДАКТОРА: конец -->"
WRITER_KINDS = {"card", "check", "judge", "prompt"}

# промпт → (конвейеры, перед каким заголовком вставить блок, если меток ещё нет)
TARGETS = {
    "prompt_insight_writer_routine.md": ({"insight", "combo"}, "# ВОРОТА"),
    "prompt_step_c_v2_routine.md": ({"news"}, "# ВОРОТА"),
    "prompt_step_g_routine.md": ({"news", "insight", "combo"}, "# ВОРОТА A"),
}


def render(rules: list, pipelines: set, judge: bool) -> str:
    rows = []
    for r in rules:
        if not pipelines & set(r.get("pipelines") or []):
            continue
        if not WRITER_KINDS & {e["kind"] for e in r.get("enforced_by") or []}:
            continue
        cases = ", ".join(f"#{c}" for c in (r.get("source") or {}).get("cases") or [])
        rows.append(f"- {r['id']} · {r['rule']}" + (f" ({cases})" if cases else ""))
    head = ("# ПРАВИЛА РЕДАКТОРА — нарушение любого пункта ниже = провал ворот, вердикт не «годится»"
            if judge else
            "# ПРАВИЛА РЕДАКТОРА — из разборов Вадима; нарушение любого = брак")
    note = ("Правила собраны из разборов черновиков Вадимом: номер кандидата в скобках — где ошибка уже была. "
            "Эти ошибки он находил повторно, поэтому они здесь списком, а не россыпью по тексту.")
    return "\n".join([BEGIN, "", head, "", note, "", *rows, "", END])


def build(check: bool) -> int:
    rules = yaml.safe_load((HERE / "editor_rules.yaml").read_text("utf-8"))["rules"]
    stale = []
    for name, (pipelines, anchor) in TARGETS.items():
        path = HERE / name
        text = path.read_text("utf-8")
        block = render(rules, pipelines, judge=name.startswith("prompt_step_g"))
        if BEGIN in text:
            new = re.sub(re.escape(BEGIN) + r".*?" + re.escape(END), lambda _: block, text, count=1, flags=re.S)
        else:
            i = text.find("\n" + anchor)
            if i < 0:
                raise SystemExit(f"{name}: нет заголовка «{anchor}» для вставки блока")
            new = text[:i + 1] + block + "\n\n" + text[i + 1:]
        if new != text:
            stale.append(name)
            if not check:
                path.write_text(new, "utf-8")
    if check and stale:
        print("блок правил устарел в:", ", ".join(stale), "— запусти build_rules_block.py")
        return 1
    print(("проверено" if check else "обновлено") + ":", ", ".join(stale) or "без изменений")
    return 0


if __name__ == "__main__":
    sys.exit(build(check="--check" in sys.argv))
