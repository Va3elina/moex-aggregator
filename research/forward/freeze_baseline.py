"""ОДНОРАЗОВЫЙ запуск на freeze_date: фиксирует, что мы знали ДО форварда.

Пишет baseline.csv — результат каждой конфигурации сетки на истории.
Файл коммитится в git: история коммита и есть доказательство, что baseline
не подгонялся задним числом под форвардный результат.

Перезапускать после freeze_date НЕЛЬЗЯ.
"""
import pathlib
import pandas as pd
from gridlib import SPEC, grid_frame

ROOT = pathlib.Path(__file__).resolve().parent
OUT = ROOT / "baseline.csv.gz"

if OUT.exists():
    raise SystemExit(f"{OUT.name} уже существует. Перезапись уничтожит форвард-тест.\n"
                     f"Если это осознанно новый эксперимент — заведи новую freeze_date "
                     f"в spec.json и новое имя файла.")

frames = []
for st in SPEC["instruments"]:
    df = grid_frame(st, SPEC["baseline_since"], SPEC["forward_starts"], min_trades=30)
    print(f"{st}: {len(df)} конфигураций, медиана сделок {df.n.median():.0f}")
    frames.append(df)

all_ = pd.concat(frames, ignore_index=True).drop(columns=["key"])
for c in ("mean", "sr", "total"):
    all_[c] = all_[c].round(6)
all_.to_csv(OUT, index=False, compression={"method": "gzip", "compresslevel": 9})
print(f"\n-> {OUT}  ({len(all_)} строк)")

nc = SPEC["named_config"]
row = all_[(all_.sectype == nc["sectype"]) & (all_.A == nc["A"]) &
           (all_.B == nc["B"]) & (all_.C == nc["C"]) & (all_["dir"] == nc["dir"])]
if len(row):
    r = row.iloc[0]
    same = all_[all_.sectype == nc["sectype"]]
    print(f"\nименованная конфигурация {nc['sectype']} {nc['A']}->{nc['B']}->{nc['C']} {nc['dir']}:")
    print(f"  сделок {int(r.n)}, средняя {r['mean']*100:+.3f}%, суммарно {r.total*100:+.0f}%")
    print(f"  место {int((same.sr > r.sr).sum()) + 1} из {len(same)} по своей бумаге")
