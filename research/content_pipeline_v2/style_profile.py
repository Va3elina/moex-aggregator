"""CLI над api/services/style_profile.py — оценить черновик из файла.

Ядро живёт в api/services/, а не здесь, по практической причине: research/ в
api-образ НЕ копируется, а api/ копируется, и профиль нужен эндпоинту /style-check
для самопроверки Шага В. Дублировать логику в двух местах нельзя — разъедется, как
разъехался бриф между Шагом В и Шагом Г (см. историю кандидата 1104).

Использование:
    python -m research.content_pipeline_v2.style_profile            # эталон канала
    python -m research.content_pipeline_v2.style_profile файл.txt   # оценить черновик
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from api.services.style_profile import KEYS, channel_reference, profile, score  # noqa: E402,F401


def _fmt(res) -> str:
    out = [f"эталон канала: n={res['n_эталона']}", ""]
    for k in KEYS:
        out.append(f"  {k:22} {res['профиль'][k]:8.2f}   эталон "
                   f"{res['эталон_канала'][k]:7.2f}   {res['отклонение_сигм'][k]:+.2f}σ")
    out += ["", f"  среднее отклонение: {res['среднее_отклонение']:.2f}σ"]
    if res["что_поправить"]:
        out += ["", "  что поправить:"] + [f"    • {g}" for g in res["что_поправить"]]
    out += ["", f"  {res['пояснение']}"]
    return "\n".join(out)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        res = score(Path(sys.argv[1]).read_text("utf-8"))
        print(_fmt(res) if res else "не удалось разобрать текст или нет эталона")
    else:
        ref = channel_reference()
        print(f"эталон жанрового среза канала (n={ref['n']}):")
        for k in KEYS:
            print(f"  {k:22} медиана {ref['медиана'][k]:8.2f}  ±{ref['разброс'][k]:.2f}")
