"""CryptosMX: компактная модель из немногих понятных признаков (меньше переобучения, чем бустинг на сотне).
Покупки: цена к средней и к прошлой покупке, время с неё, ход 1–60 мин, место в диапазоне часа/4ч, отклонение от EMA50,
пробой минимумов 6/12 ч у DOGE и у биткоина. Продажи — зеркально (к прошлой продаже, пробой максимумов).
Две модели: логистическая регрессия и неглубокий бустинг; проверка блоками по времени, как раньше."""
import sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import cryptos_ml as cm
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline

W0, W1 = pd.Timestamp("2025-01-16 09:10"), pd.Timestamp("2025-02-03 05:00")
X = cm.market_features("DOGEUSDT", "2025-01-13", "2025-02-12")
F = cm.load_fills("DOGEUSDT")
S = cm.state_features(X, F[F.t < W1])
D = pd.concat([X, S], axis=1).loc[W0:W1 - pd.Timedelta(minutes=1)].copy()
D["лог_мин_с_покупки"] = np.log1p(D.мин_с_покупки.clip(lower=0)); D["лог_мин_с_продажи"] = np.log1p(D.мин_с_продажи.clip(lower=0))
FB = ["к_средней", "к_прошлой_покупке", "лог_мин_с_покупки", "ход_1м", "ход_3м", "ход_15м", "ход_60м", "место_60м", "место_240м",
      "к_ema50_1м", "ниже_лоя_6ч", "ниже_лоя_12ч", "btc_ниже_лоя_6ч", "btc_ниже_лоя_12ч", "btc_ход_60м", "позиция_к_макс"]
FS = ["к_средней", "к_прошлой_продаже", "лог_мин_с_продажи", "ход_1м", "ход_3м", "ход_15м", "ход_60м", "место_60м", "место_15м",
      "к_ema20_1м", "выше_хая_6ч", "выше_хая_12ч", "btc_выше_хая_6ч", "btc_выше_хая_12ч", "btc_ход_60м"]
blocks = np.array_split(np.arange(len(D)), 4)
his = {s: D.index[D[f"y_{s}"] == 1].values.astype("datetime64[m]").astype(np.int64) for s in ("buy", "sell")}
rows = []
for mname in ("логистическая", "бустинг-мелкий"):
    for side, feats in (("buy", FB), ("sell", FS)):
        Z = D[feats].copy().fillna(0).clip(-50, 50)
        y = D[f"y_{side}"].values
        oof = pd.Series(np.nan, index=D.index)
        for bi, test in enumerate(blocks):
            train = np.concatenate([b for j, b in enumerate(blocks) if j != bi])
            train = train[(train < test[0] - 240) | (train > test[-1] + 240)]
            if mname == "логистическая":
                mdl = make_pipeline(StandardScaler(), LogisticRegression(C=0.3, class_weight="balanced", max_iter=2000))
            else:
                mdl = HistGradientBoostingClassifier(max_iter=200, learning_rate=0.05, max_depth=3, min_samples_leaf=80,
                                                     l2_regularization=2.0, class_weight="balanced", random_state=0)
            mdl.fit(Z.iloc[train], y[train])
            oof.iloc[test] = mdl.predict_proba(Z.iloc[test])[:, 1]
        best = None
        for thr in (0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 0.95):
            for refr in (3, 5, 10):
                ev = cm.events_from_prob(oof, thr, refr)
                f15, r15, p15 = cm.match_f1(his[side], ev, 15)
                if best is None or f15 > best[0]:
                    best = (f15, r15, p15, thr, refr, len(ev), cm.match_f1(his[side], ev, 60)[0])
        rows.append(dict(модель=mname, сторона=side, F1_15=round(best[0], 3), полнота=round(best[1], 3), точность=round(best[2], 3),
                         порог=best[3], пауза=best[4], событий=best[5], его=len(his[side]), F1_60=round(best[6], 3)))
print(pd.DataFrame(rows).to_string(index=False))
