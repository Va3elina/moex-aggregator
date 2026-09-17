// Стенд: «Робот против бэктеста» — настоящие сделки песочницы рядом с теми же сделками в выбранном прогоне.
import type { BtLiveTrade, BtTrade } from './api';
import { cls, fmtDate, money, num, pct } from './lib';
import { Logo } from './ui';

export default function RobotReport({ live, trades, names, onPick }: { live: BtLiveTrade[]; trades: BtTrade[]; names: Map<string, string>; onPick: (t: BtTrade) => void }) {
  if (!live.length) return <div className="bt-nodata big">Сделок робота пока нет (журнал песочницы пуст или недоступен).</div>;
  const key = (st: string, d: string) => `${st}|${d}`;
  const bt = new Map(trades.map(t => [key(t.st, t.d), t]));
  const first = live.reduce((a, r) => r.d < a ? r.d : a, live[0].d);
  const liveKeys = new Set(live.map(r => key(r.st, r.d)));
  const missed = trades.filter(t => t.d >= first && !liveKeys.has(key(t.st, t.d)));
  const rows = live.slice().sort((a, b) => a.d < b.d ? 1 : -1).map(r => {
    const t = bt.get(key(r.st, r.d)) ?? null;
    const retLive = r.px_in && r.px_out ? r.side * (r.px_out / r.px_in - 1) : null;
    const retBt = t ? t.side * (t.px_out / t.px_in - 1) : null;
    return { r, t, retLive, retBt, slipIn: t && r.px_in ? r.side * (t.px_in / r.px_in - 1) * -1 : null };
  });
  const closed = rows.filter(x => x.retLive != null && x.retBt != null);
  const avg = (a: number[]) => a.length ? a.reduce((x, y) => x + y, 0) / a.length : null;
  return (
    <div className="bt-tradelist">
      <div className="bt-listbar"><b>Робот против бэктеста</b><span className="bt-dim">с {fmtDate(first)} · сделок робота {live.length} · закрыто {closed.length}</span></div>
      <div className="bt-metrics" style={{ padding: '4px 14px 14px' }}>
        <div className="bt-metric"><small>Результат робота, цена</small><b className={cls(avg(closed.map(x => x.retLive!)))}>{pct(avg(closed.map(x => x.retLive!)))}</b><span className="bt-dim">в среднем на сделку</span></div>
        <div className="bt-metric"><small>Те же сделки в бэктесте</small><b className={cls(avg(closed.map(x => x.retBt!)))}>{pct(avg(closed.map(x => x.retBt!)))}</b><span className="bt-dim">до издержек</span></div>
        <div className="bt-metric"><small>Расхождение</small><b className={cls(avg(closed.map(x => x.retLive! - x.retBt!)))}>{pct(avg(closed.map(x => x.retLive! - x.retBt!)))}</b><span className="bt-dim">робот минус бэктест: опоздания и цена исполнения</span></div>
        <div className="bt-metric"><small>Итог робота</small><b className={cls(live.reduce((a, r) => a + (r.pnl_rub ?? 0), 0))}>{money(live.reduce((a, r) => a + (r.pnl_rub ?? 0), 0))}</b><span className="bt-dim">в рублях, с комиссией песочницы</span></div>
      </div>
      <table className="bt-table tv"><thead><tr><th className="l">Бумага</th><th className="l">Тип</th><th className="l">Вход → выход</th><th>Вход: робот / бэктест</th><th>Выход: робот / бэктест</th><th>Цена, %: робот / бэктест</th><th>Расхождение</th><th>Контр.</th><th>Итог, ₽</th><th className="l">Примечание</th></tr></thead>
        <tbody>{rows.map(({ r, t, retLive, retBt }) => (
          <tr key={r.st + r.d} onClick={() => t && onPick(t)}>
            <td className="l"><span className="bt-inline"><Logo st={r.st} size={18} /><b>{r.st}</b> <span className="bt-dim">{names.get(r.st)} · {r.secid}</span></span></td>
            <td className={`l ${r.side > 0 ? 'bt-long' : 'bt-down'}`}>{r.side > 0 ? 'Длинная' : 'Короткая'}</td>
            <td className="l">{fmtDate(r.d)} {r.t_in?.slice(0, 5)} → {r.d_out ? `${fmtDate(r.d_out)} ${r.t_out?.slice(0, 5) ?? ''}` : 'открыта'}</td>
            <td>{num(r.px_in, 2)} / {t ? num(t.px_in, 2) : '—'}</td><td>{num(r.px_out, 2)} / {t ? num(t.px_out, 2) : '—'}</td>
            <td><span className={cls(retLive)}>{pct(retLive)}</span> / <span className={cls(retBt)}>{pct(retBt)}</span></td>
            <td className={cls(retLive != null && retBt != null ? retLive - retBt : null)}>{retLive != null && retBt != null ? pct(retLive - retBt) : '—'}</td>
            <td>{num(r.qty)}</td><td className={cls(r.pnl_rub)}>{money(r.pnl_rub)}</td>
            <td className="l bt-dim" style={{ whiteSpace: 'normal', maxWidth: 360 }}>{!t ? 'в этом прогоне такой сделки нет' : ''}{r.note ?? ''}</td>
          </tr>))}</tbody></table>
      {!!missed.length && <><div className="bt-listbar"><b>Сигналы бэктеста, которых нет у робота</b><span className="bt-dim">{missed.length}</span></div>
        <table className="bt-table tv"><tbody>{missed.slice(-50).reverse().map(t => <tr key={t.st + t.d} onClick={() => onPick(t)}><td className="l"><span className="bt-inline"><Logo st={t.st} size={18} /><b>{t.st}</b> <span className="bt-dim">{names.get(t.st)}</span></span></td><td className="l">{fmtDate(t.d)}</td><td className={`l ${t.side > 0 ? 'bt-long' : 'bt-down'}`}>{t.side > 0 ? 'Длинная' : 'Короткая'}</td><td>ход {pct(t.move)}</td><td className={cls(t.net)}>{pct(t.net)}</td></tr>)}</tbody></table></>}
    </div>
  );
}
