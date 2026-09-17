/* eslint-disable @typescript-eslint/no-explicit-any -- результат перебора — свободный JSON движка */
// Стенд: результат перебора параметров. Варианты ранжированы по обучению; контрольный период — рядом, в выборе не участвует.
import type { BtRun } from './api';
import { cls, fmtDate, num, signed } from './lib';

export const PARAM_LABEL: Record<string, string> = {
  'long.q': 'Лонг: квантиль', 'short.q': 'Шорт: квантиль', 'long.window': 'Лонг: окно, дней', 'short.window': 'Шорт: окно, дней',
  'long.value': 'Лонг: порог хода', 'short.value': 'Шорт: порог хода', 'exit.stop': 'Стоп', 'exit.take': 'Тейк', 'exit.trail': 'Трейлинг',
  'exit.hold_days': 'Держать, дней', 'entry.delay_min': 'Задержка входа, мин', 'exit.delay_min': 'Задержка выхода, мин',
};
export const paramLabel = (p: string) => PARAM_LABEL[p] ?? (p.startsWith('filters.') ? `Фильтр: ${p.split('.').slice(2).join('.')}` : p);
const val = (p: string, v: any) => v == null ? 'выкл.' : /stop|take|trail|value/.test(p) ? num(100 * v, 2) + '%' : String(v);

export function setPath(obj: any, path: string, value: any) {
  const keys = path.split('.'); let cur = obj;
  for (const k of keys.slice(0, -1)) { if (cur[k] == null) cur[k] = {}; cur = cur[k]; }
  const last = keys[keys.length - 1]; if (value == null) delete cur[last]; else cur[last] = value;
}

export default function Sweep({ run, onOpenVariant }: { run: BtRun; onOpenVariant: (params: Record<string, any>) => void }) {
  const sw = run.result?.sweep; if (!sw) return null;
  const rows: any[] = sw.rows ?? []; const keys = Object.keys(sw.grid ?? {}); const rho = sw['ранговая_корреляция']; const dsr = sw['DSR_лучшего'];
  const best = rows[0]; const n = rows.length;
  const verdict = rho == null ? 'Слишком мало вариантов, чтобы судить об устойчивости.'
    : rho >= 0.5 ? `Порядок вариантов на обучении и на контроле похож (ранговая корреляция ${rho}): параметры действительно влияют на результат, выбору можно доверять.`
    : rho >= 0.2 ? `Порядок вариантов на обучении и на контроле совпадает слабо (${rho}). Лучший вариант выбирай осторожно: бери середину хорошей области, а не вершину.`
    : `Порядок вариантов на обучении и на контроле не совпадает (ранговая корреляция ${rho}). «Лучший» вариант выбран шумом: разница между вариантами случайна. Честный вывод — эти параметры ничего не улучшают, оставь базовые.`;
  return (
    <div className="bt-checks">
      <section className={`bt-check ${rho == null ? 'warn' : rho >= 0.5 ? 'ok' : rho >= 0.2 ? 'warn' : 'bad'}`}>
        <h3><i>{rho != null && rho >= 0.5 ? '✓' : '!'}</i>Перебрано вариантов: {sw['вариантов']} · обучение до {fmtDate(sw.oos_from)}, контроль — после</h3>
        <p>{verdict}{best?.['место_контроль'] ? ` Лучший на обучении занял ${best['место_контроль']}-е место из ${n} на контроле.` : ''}{dsr ? ` DSR лучшего ${dsr.DSR} (порог 0.95): его Шарп ${dsr['Шарп_базового']} против ${dsr['Шарп_шума']}, которые дал бы лучший из ${dsr['вариантов']} случайных вариантов.` : ''}</p>
        <p className="bt-dim">Варианты отсортированы по обучению. Контрольный период в выборе не участвует — он показывает, что было бы, выбери ты этот вариант заранее. Счёт (слоты, ГО) здесь не считается: нажми «открыть» у понравившегося варианта — он посчитается полным прогоном.</p>
      </section>
      <table className="bt-table"><thead>
        <tr><th className="l">Место</th>{keys.map(k => <th key={k} className="l">{paramLabel(k)}</th>)}<th>Сделок</th><th>Обучение: на сделку</th><th>t</th><th>Шарп</th><th>Сделок</th><th>Контроль: на сделку</th><th>t</th><th>Шарп</th><th>Место на контроле</th><th /></tr>
      </thead><tbody>
        {rows.map(r => { const a = r['обучение'], b = r['контроль']; const drop = r['место_контроль'] != null && r['место_контроль'] - r['место_обучение'] > n / 3; return (
          <tr key={JSON.stringify(r.params)}>
            <td className="l">{r['место_обучение']}</td>{keys.map(k => <td key={k} className="l">{val(k, r.params[k])}</td>)}
            <td>{num(a['сделок'])}</td><td className={cls(a['на_сделку_%'])}>{signed(a['на_сделку_%'], 3)}%</td><td>{a.t}</td><td><b>{a['Шарп']}</b></td>
            <td>{num(b['сделок'])}</td><td className={cls(b['на_сделку_%'])}>{signed(b['на_сделку_%'], 3)}%</td><td>{b.t ?? '—'}</td><td><b>{b['Шарп'] ?? '—'}</b></td>
            <td className={drop ? 'bt-down' : ''}>{r['место_контроль'] ?? '—'}</td>
            <td><button className="bt-tb" onClick={() => onOpenVariant(r.params)}>открыть</button></td>
          </tr>); })}
      </tbody></table>
    </div>
  );
}
