"""obstats_points.csv.gz → backtest/data/spread_daily.csv.gz: (st, d, c3 % за круг, cap3_rub — сколько рублей влезает
в три уровня стакана). Берём замеры у времени входа и выхода наших правил, медиана за день. Можно запускать на
недокачанном файле — возьмёт, что есть."""
import io, subprocess, sys, pathlib
import pandas as pd
sys.path.insert(0, str(pathlib.Path.home() / 'PyCharmMiscProject/MOEX-bt'))
from backtest import account                                              # noqa: E402
HERE = pathlib.Path(__file__).parent
raw = subprocess.run(['gzip', '-dc', str(HERE / 'data/obstats_points.csv.gz')], capture_output=True).stdout    # терпит незакрытый архив
d = pd.read_csv(io.BytesIO(raw), on_bad_lines='skip'); d = d[pd.to_numeric(d.mid_price, errors='coerce') > 0]
for c in ('mid_price', 'vwap_b_l3', 'vwap_s_l3', 'vol_b_l3', 'vol_s_l3'): d[c] = pd.to_numeric(d[c], errors='coerce')
d['c3'] = (d.vwap_s_l3 - d.vwap_b_l3) / d.mid_price * 100
d = d[(d.c3 > 0) & (d.c3 < 20)]
d['cap3_rub'] = [min(b, s) * m * account.rub_per_point(st, dt) for b, s, m, st, dt in zip(d.vol_b_l3, d.vol_s_l3, d.mid_price, d.st, d.tradedate)]
out = d.groupby(['st', 'tradedate']).agg(c3=('c3', 'median'), cap3_rub=('cap3_rub', 'median'), n=('c3', 'size')).reset_index().rename(columns={'tradedate': 'd'})
out['c3'] = out.c3.round(5); out['cap3_rub'] = out.cap3_rub.round(0)
dst = pathlib.Path.home() / 'PyCharmMiscProject/MOEX-bt/backtest/data/spread_daily.csv.gz'
out.to_csv(dst, index=False)
print(len(out), 'дней ·', out.st.nunique(), 'бумаг ·', out.d.min(), '…', out.d.max())
print(out.groupby('st').agg(дней=('c3', 'size'), c3_медиана=('c3', 'median'), глубина_руб=('cap3_rub', 'median')).round(4).to_string())
