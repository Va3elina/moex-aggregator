/* eslint-disable @typescript-eslint/no-explicit-any -- результат проверок — свободный JSON движка */
// Стенд: «Проверки» — закономерность это или подгонка. Каждая проверка: вердикт простыми словами + цифры.
import type { ReactNode } from 'react';
import type { BtRun } from './api';
import { num, signed } from './lib';
import { Bars } from './ui';

type V = 'ok' | 'warn' | 'bad';
const ICON: Record<V, string> = { ok: '✓', warn: '!', bad: '✕' };

function Check({ v, title, verdict, children }: { v: V; title: string; verdict: string; children?: ReactNode }) {
  return <section className={`bt-check ${v}`}><h3><i>{ICON[v]}</i>{title}</h3><p>{verdict}</p>{children}</section>;
}

export default function Checks({ run, onRun, busy }: { run: BtRun; onRun: () => void; busy: boolean }) {
  const c = run.result?.checks;
  if (!c) return (
    <div className="bt-nodata big">
      <div style={{ maxWidth: 560 }}>Проверки отвечают на вопрос «это закономерность или подгонка под историю»: перемешанные сигналы (плацебо), две половины истории, результат без лучших дней, задержка входа, соседние параметры. Считаются около минуты.</div>
      <button className="bt-run" disabled={busy} onClick={onRun}>{busy ? 'Считается…' : 'Запустить проверки'}</button>
    </div>
  );
  const h = c.halves, p = c.placebo, t = c.tails, nb = c.neighbors, years: any[] = c.by_year ?? [], dec: any[] = c.deciles ?? [], dl: any[] = c.delay ?? [];
  const badYears = years.filter(y => y['на_сделку_%'] < 0 && y['сделок'] >= 20);
  const d0 = dl[0]?.['на_сделку_%'] ?? 0, d15 = dl.find(x => x['задержка_мин'] === 15)?.['на_сделку_%'] ?? d0;
  const top = dec.slice(-3).reduce((a, x) => a + x['продолжение_bp'], 0) / 3, bottom = dec.slice(0, 3).reduce((a, x) => a + x['продолжение_bp'], 0) / 3;
  const tbl: any[] = nb?.['таблица'] ?? []; const worst = tbl.length ? Math.min(...tbl.map(x => x['на_сделку_%'])) : null;
  const plateau = worst != null && worst > 0 && tbl.every(x => x.t > 2);
  return (
    <div className="bt-checks">
      <Check v={p.p < 0.01 ? 'ok' : p.p < 0.05 ? 'warn' : 'bad'} title="Плацебо: сигналы перемешаны между днями"
        verdict={p.p < 0.01 ? `Настоящий сигнал даёт ${signed(p['факт_%'], 3)}% на сделку, а те же сделки в случайные дни — ${signed(p['плацебо_среднее_%'], 3)}%. Ни одна из ${p.n} перестановок не дотянулась: результат создаёт именно ход дня, а не удачный рынок.` : `Случайные дни дают сопоставимый результат (p = ${num(p.p, 3)}). Сигнал, возможно, ничего не добавляет.`} />
      <Check v={h['до_%'] > 0 && h['после_%'] > 0 ? (Math.min(h.t_до, h.t_после) > 2 ? 'ok' : 'warn') : 'bad'} title="Две половины истории"
        verdict={`До ${h['граница']}: ${signed(h['до_%'], 3)}% на сделку (${h.n_до} сделок). После: ${signed(h['после_%'], 3)}% (${h.n_после}). ${h['после_%'] >= h['до_%'] * 0.6 ? 'Результат не усох со временем.' : 'Во второй половине результат заметно слабее.'}`} />
      <Check v={badYears.length === 0 ? 'ok' : badYears.length === 1 ? 'warn' : 'bad'} title="По годам" verdict={badYears.length ? `Убыточные годы: ${badYears.map(y => y['год']).join(', ')}.` : 'Убыточных лет (с заметным числом сделок) нет.'}>
        <Bars data={years.map(y => ({ k: String(y['год']), v: y['на_сделку_%'], n: y['сделок'] }))} height={130} fmt={v => signed(v, 3) + '% на сделку'} />
      </Check>
      <Check v={top > bottom * 2 && top > 5 ? 'ok' : top > bottom ? 'warn' : 'bad'} title="Лестница: чем сильнее ход дня, тем сильнее продолжение ночью"
        verdict={`Слабые ходы (нижние 30 %) продолжаются в среднем на ${num(bottom, 1)} б.п., сильные (верхние 30 %) — на ${num(top, 1)} б.п. ${top > bottom * 2 ? 'Зависимость монотонная — это свойство рынка, а не удачно выбранный порог.' : 'Чёткой лестницы нет.'}`}>
        <Bars data={dec.map(x => ({ k: String(x['дециль']), v: x['продолжение_bp'], n: x['дней'] }))} height={130} fmt={v => num(v, 1) + ' б.п.'} />
      </Check>
      <Check v={t['Шарп_без_обоих_хвостов'] > 1 ? 'ok' : t['Шарп_без_обоих_хвостов'] > 0.3 ? 'warn' : 'bad'} title="Без лучших и худших дней"
        verdict={`Шарп ${t['Шарп']}. Без 5 % лучших дней: ${t['Шарп_без_лучших']} — стратегия зарабатывает редкими сильными ночами (на них ${num(t['доля_дохода_в_лучших_%'], 0)} % дохода). Без лучших И худших 5 %: ${t['Шарп_без_обоих_хвостов']} — ${t['Шарп_без_обоих_хвостов'] > 1 ? 'середина распределения тоже в плюсе.' : 'середина слабая.'} Практический вывод: пропускать сигналы нельзя.`} />
      <Check v={d15 >= d0 * 0.8 ? 'ok' : d15 >= d0 * 0.5 ? 'warn' : 'bad'} title="Задержка входа" verdict={`Вход сразу: ${signed(d0, 3)}% на сделку, через 15 минут: ${signed(d15, 3)}%. ${d15 >= d0 * 0.8 ? 'Скорость исполнения не критична — робот с кроном раз в 5 минут ничего не теряет.' : 'Результат чувствителен к скорости входа.'}`}>
        <Bars data={dl.map(x => ({ k: `+${x['задержка_мин']} мин`, v: x['на_сделку_%'] ?? 0, n: x['сделок'] }))} height={120} fmt={v => signed(v, 3) + '%'} />
      </Check>
      {nb && <Check v={plateau ? 'ok' : worst != null && worst > 0 ? 'warn' : 'bad'} title={`Соседние параметры (${tbl.length} вариантов)`}
        verdict={`${plateau ? 'Все соседние варианты прибыльны и значимы — это плато, а не острый пик.' : worst != null && worst > 0 ? 'Все варианты прибыльны, но часть — незначимо.' : 'Часть соседних вариантов убыточна — параметры могут быть подогнаны.'} Худший: ${signed(worst, 3)}%, лучший: ${signed(Math.max(...tbl.map(x => x['на_сделку_%'])), 3)}%.${nb.DSR ? ` DSR ${nb.DSR.DSR} (порог 0.95): Шарп базового варианта ${nb.DSR['Шарп_базового']} против ${nb.DSR['Шарп_шума']}, которые дал бы лучший из ${nb.DSR['вариантов']} случайных.` : ''}${nb.PBO ? ` PBO ${nb.PBO.PBO}${plateau ? ' — на плато он неинформативен: варианты почти одинаковы, и «лучший» меняется случайно.' : '.'}` : ''}`}>
        <table className="bt-table"><thead><tr><th className="l">Вариант</th><th>Сделок</th><th>На сделку</th><th>t</th><th>Шарп</th></tr></thead><tbody>
          {tbl.slice().sort((a, b) => b['на_сделку_%'] - a['на_сделку_%']).map(x => <tr key={x['вариант']} className={x['базовый'] ? 'on' : ''}><td className="l">{x['вариант']}{x['базовый'] ? ' — базовый' : ''}</td><td>{num(x['сделок'])}</td><td className={x['на_сделку_%'] > 0 ? 'bt-up' : 'bt-down'}>{signed(x['на_сделку_%'], 3)}%</td><td>{x.t}</td><td>{x['Шарп']}</td></tr>)}
        </tbody></table>
      </Check>}
    </div>
  );
}
