/**
 * Завод постов: доска состояний (та же, что была на /admin/content-news) →
 * разбор пути одного кандидата.
 *
 * ⚠️ РАЗБОР — ЭТО РАССКАЗ, А НЕ ЛОГ. Первая версия показывала воронку «где теряются
 * кандидаты», разгон репостов, сырой след («context ROSN days=14 · нашлось 37 ·
 * 112 мс») и блок второго мозга с уровнями A/B/C/D. Читалось как шум: непонятно,
 * что агент спросил, что узнал и на что это повлияло. Теперь бэкенд отдаёт путь
 * шагами простыми фразами (рассуждение), а техника — под сворачивающимися блоками.
 *
 * ⚠️ ЧЕГО В СЛЕДЕ НЕТ — ГОВОРИМ ВСЛУХ, но тихо: одной строкой внизу.
 */
import { useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, ChevronDown, ChevronRight, ExternalLink } from 'lucide-react';
import { getPostDetail } from '../../services/api';
import type { PostDetail } from '../../services/api';
import TickerJump from './TickerJump';
import BrainGraph from './BrainGraph';
import { ContentKanban } from '../AdminContentNewsPage';

const ЦВЕТ_СТАТУСА: Record<string, string> = {
  published: 'var(--d-ok)', draft_ready: 'var(--d-accent)', in_review: 'var(--d-accent)',
  pending: 'var(--d-cold)', candidate: 'var(--d-cold)', discarded: 'var(--d-dim)',
  no_data: 'var(--d-warn)', rejected: 'var(--d-bad)',
};
const ЦВЕТ_ТОНА: Record<string, string> = {
  ok: 'var(--d-ok)', warn: 'var(--d-warn)', bad: 'var(--d-bad)', neutral: 'var(--d-line-strong)',
};
const ЦВЕТ_ИСХОДА: Record<string, string> = { взято: 'var(--d-ok)', не_взято: 'var(--d-warn)', пусто: 'var(--d-bad)' };
const ЦВЕТ_ВЕРДИКТА: Record<string, string> = { годится: 'var(--d-ok)', спорно: 'var(--d-warn)', брак: 'var(--d-bad)' };

const числоРус = (n: number) => n.toLocaleString('ru-RU');
function датаРус(iso: string | null | undefined): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
}

function Плашка({ children, цвет }: { children: React.ReactNode; цвет?: string }) {
  return (
    <span className="mono" style={{
      fontSize: 10.5, padding: '2px 8px', borderRadius: 999, whiteSpace: 'nowrap',
      background: 'rgba(245,241,232,0.08)', color: цвет ?? 'var(--d-mute)',
    }}>{children}</span>
  );
}

function Секция({ титул, метка, children }: { титул: string; метка?: React.ReactNode; children: React.ReactNode }) {
  return (
    <div className="dash-inner" style={{ padding: '13px 15px' }}>
      <div className="flex items-baseline justify-between mb-2" style={{ gap: 10 }}>
        <div style={{ fontSize: 13, fontWeight: 700 }}>{титул}</div>
        {метка}
      </div>
      {children}
    </div>
  );
}

/** Сворачиваемый блок для техники: закрыт по умолчанию, чтобы не шуметь. */
function Collapsed({ титул, children }: { титул: string; children: React.ReactNode }) {
  const [открыт, setОткрыт] = useState(false);
  return (
    <div className="dash-inner" style={{ padding: '10px 15px' }}>
      <button onClick={() => setОткрыт((v) => !v)} className="flex items-center mono"
        style={{ gap: 6, background: 'none', border: 'none', color: 'var(--d-dim)', cursor: 'pointer', fontSize: 11.5, padding: 0 }}>
        {открыт ? <ChevronDown size={13} /> : <ChevronRight size={13} />} {титул}
      </button>
      {открыт && <div style={{ marginTop: 10 }}>{children}</div>}
    </div>
  );
}

/** Путь кандидата простыми фразами: номер, заголовок, две-три строки, полоса тона слева. */
function Рассуждение({ шаги }: { шаги: PostDetail['рассуждение'] }) {
  return (
    <div className="flex flex-col" style={{ gap: 2 }}>
      {шаги.map((ш, i) => (
        <div key={ш.код + i} className="flex" style={{ gap: 12, alignItems: 'stretch' }}>
          <div className="flex flex-col items-center" style={{ width: 22, flexShrink: 0 }}>
            <span className="mono" style={{
              width: 22, height: 22, borderRadius: 999, display: 'grid', placeItems: 'center', fontSize: 10.5, fontWeight: 700,
              border: `2px solid ${ЦВЕТ_ТОНА[ш.тон] ?? 'var(--d-line-strong)'}`, color: ш.тон === 'neutral' ? 'var(--d-mute)' : ЦВЕТ_ТОНА[ш.тон],
            }}>{i + 1}</span>
            {i < шаги.length - 1 && <span style={{ flex: 1, width: 2, background: 'var(--d-line)', margin: '2px 0' }} />}
          </div>
          <div style={{ paddingBottom: i < шаги.length - 1 ? 14 : 0, minWidth: 0 }}>
            <div style={{ fontSize: 13, fontWeight: 700, lineHeight: '22px' }}>{ш.заголовок}</div>
            {ш.строки.map((с, j) => (
              <div key={j} style={{ fontSize: 12.5, color: 'var(--d-mute)', lineHeight: 1.55, marginTop: 2, whiteSpace: 'pre-wrap' }}>{с}</div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

/**
 * ⚠️ ИМЯ ЛАТИНИЦЕЙ, И ЭТО НЕ ПРИДИРКА. rules-of-hooks не признаёт компонентом имя
 * с кириллической заглавной — порядок хуков внутри не проверялся бы вовсе.
 */
function PostTrace({ id, назад }: { id: number; назад: () => void }) {
  const navigate = useNavigate();
  const [d, setD] = useState<PostDetail | null>(null);
  const [ошибка, setОшибка] = useState<string | null>(null);

  useEffect(() => {
    let живо = true;
    getPostDetail(id)
      .then((r) => { if (живо) { setD(r); setОшибка(null); } })
      .catch((e) => { if (живо) setОшибка(e instanceof Error ? e.message : 'сбой'); });
    return () => { живо = false; };
  }, [id]);

  if (ошибка) {
    return (
      <div className="dash-card" style={{ padding: '16px 18px' }}>
        <button className="dash-press mb-3" style={{ padding: '4px 10px', fontSize: 11 }} onClick={назад}>← к доске</button>
        <div style={{ color: 'var(--d-bad)', fontSize: 13 }}>{ошибка}</div>
      </div>
    );
  }
  if (!d) {
    return <div className="dash-card mono" style={{ padding: '16px 18px', fontSize: 12, color: 'var(--d-dim)' }}>собираю путь кандидата…</div>;
  }

  const к = d.кандидат;
  const абзацы = Array.isArray(d.шаг_г_судья.абзацы) ? (d.шаг_г_судья.абзацы as Array<Record<string, unknown>>) : [];
  const техСлед = [...d.мозг, ...d.след];

  return (
    <div className="dash-card" style={{ padding: '16px 18px' }}>
      <div className="flex items-center justify-between mb-3" style={{ gap: 12, flexWrap: 'wrap' }}>
        <button className="dash-press flex items-center" style={{ padding: '4px 10px', gap: 5, fontSize: 11 }} onClick={назад}>
          <ArrowLeft size={13} /> к доске
        </button>
        <div className="flex items-center" style={{ gap: 7, flexWrap: 'wrap' }}>
          <Плашка цвет={ЦВЕТ_СТАТУСА[к.статус] ?? 'var(--d-line-strong)'}>{к.статус_подпись}</Плашка>
          {к.тикеры.map((t) => <TickerJump key={t} t={t} />)}
          <Плашка>#{к.id}</Плашка>
        </div>
      </div>

      <h3 className="disp" style={{ fontSize: 19, fontWeight: 700, margin: '0 0 4px', lineHeight: 1.25 }}>{к.заголовок}</h3>
      <div className="mono mb-3" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
        {датаРус(к.создан)}
        {к.опубликован && <> · опубликован {датаРус(к.опубликован)}</>}
        {к.ссылка && (
          <> · <a href={к.ссылка} target="_blank" rel="noreferrer" style={{ color: 'var(--d-accent)' }}>
            исходный пост <ExternalLink size={10} style={{ display: 'inline', verticalAlign: -1 }} />
          </a></>
        )}
      </div>

      <div className="flex flex-col" style={{ gap: 9 }}>
        {d.новость && (
          <Секция титул="Пришла новость"
            метка={<span className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)' }}>
              {String(d.новость.channel)} · {числоРус(Number(d.новость.views ?? 0))} просмотров
            </span>}>
            <div style={{ fontSize: 12.5, color: 'var(--d-mute)', whiteSpace: 'pre-wrap' }}>{String(d.новость.text ?? '').slice(0, 700)}</div>
          </Секция>
        )}

        <Секция титул="Как рассуждал агент"
          метка={<span className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)' }}>{d.рассуждение.length} шагов</span>}>
          <Рассуждение шаги={d.рассуждение} />
        </Секция>

        {(d.шаг_в_черновик.текст || d.шаг_в_черновик.отказ) && (
          <Секция титул="Черновик"
            метка={d.шаг_в_черновик.правил_человек ? <Плашка цвет="var(--d-accent)">правил человек</Плашка> : undefined}>
            {d.шаг_в_черновик.отказ && (
              <div style={{ fontSize: 12.5, color: 'var(--d-warn)', marginBottom: 8 }}>Писатель отказался: {d.шаг_в_черновик.отказ}</div>
            )}
            {d.шаг_в_черновик.текст && <div style={{ fontSize: 13, whiteSpace: 'pre-wrap', lineHeight: 1.55 }}>{d.шаг_в_черновик.текст}</div>}
            {d.шаг_в_черновик.аннотация && (
              <div className="mono" style={{ fontSize: 11, color: 'var(--d-dim)', marginTop: 10, paddingTop: 8, borderTop: '1px solid var(--d-line)' }}>
                {d.шаг_в_черновик.аннотация}
              </div>
            )}
          </Секция>
        )}

        {абзацы.length > 0 && (
          <Секция титул="Судья по абзацам"
            метка={d.шаг_г_судья.вердикт ? <Плашка цвет={ЦВЕТ_ВЕРДИКТА[d.шаг_г_судья.вердикт]}>{d.шаг_г_судья.вердикт}</Плашка> : undefined}>
            <div className="flex flex-col" style={{ gap: 6 }}>
              {абзацы.map((а, i) => {
                const принят = а.supported !== false;
                return (
                  <div key={i} style={{
                    padding: '8px 10px', borderRadius: 6,
                    background: принят ? 'rgba(91,212,156,0.10)' : 'rgba(255,122,92,0.12)',
                    borderLeft: `3px solid ${принят ? 'var(--d-ok)' : 'var(--d-bad)'}`,
                  }}>
                    <div className="mono" style={{ fontSize: 10, color: 'var(--d-dim)' }}>абзац {i + 1} · {принят ? 'с опорой' : 'без опоры'}</div>
                    {typeof а.claim === 'string' && <div style={{ fontSize: 12.5 }}>{а.claim}</div>}
                    {typeof а.doubt === 'string' && а.doubt && (
                      <div style={{ fontSize: 11.5, marginTop: 3, color: принят ? 'var(--d-mute)' : 'var(--d-bad)' }}>{а.doubt}</div>
                    )}
                  </div>
                );
              })}
            </div>
          </Секция>
        )}

        {к.тикеры.length > 0 && (
          <Collapsed титул="карта связей вокруг кандидата">
            <BrainGraph
              key={к.id}
              center={`candidate:${к.id}`}
              запасной={`company:${к.тикеры[0]}`}
              высота={360}
              onУзел={(nid) => navigate(`/admin/dashboard/brain?n=${encodeURIComponent(nid)}&v=graph`)}
            />
          </Collapsed>
        )}

        {техСлед.length > 0 && (
          <Collapsed титул={`технический след · ${техСлед.length} обращений к базе и мозгу`}>
            <div className="flex flex-col" style={{ gap: 6 }}>
              {техСлед.map((ш, i) => (
                <div key={i} className="flex" style={{ gap: 10, alignItems: 'flex-start' }}>
                  <span style={{ width: 3, alignSelf: 'stretch', borderRadius: 2, background: ЦВЕТ_ИСХОДА[ш.исход] ?? 'var(--d-line-strong)' }} />
                  <div style={{ minWidth: 0, flex: 1 }}>
                    <div className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)' }}>
                      {ш.шаг} · {ш.источник}{ш.нашлось != null && ` · нашлось ${ш.нашлось}`}{ш.мс != null && ` · ${ш.мс} мс`}
                    </div>
                    <div style={{ fontSize: 12 }}>{ш.вопрос}</div>
                    {ш.результат && <div style={{ fontSize: 11.5, color: 'var(--d-mute)' }}>{ш.результат}</div>}
                    {ш.почему && <div style={{ fontSize: 11.5, color: 'var(--d-warn)' }}>{ш.почему}</div>}
                  </div>
                  <Плашка цвет={ЦВЕТ_ИСХОДА[ш.исход]}>{ш.исход}</Плашка>
                </div>
              ))}
            </div>
          </Collapsed>
        )}

        {d.чего_нет.length > 0 && (
          <Collapsed титул="чего в следе нет">
            <ul style={{ margin: 0, paddingLeft: 18, display: 'flex', flexDirection: 'column', gap: 4 }}>
              {d.чего_нет.map((п) => <li key={п} style={{ fontSize: 12, color: 'var(--d-mute)', lineHeight: 1.5 }}>{п}</li>)}
            </ul>
          </Collapsed>
        )}
      </div>
    </div>
  );
}

export default function PostFactory() {
  // ⚠️ ВЫБРАННЫЙ КАНДИДАТ — В АДРЕСЕ (/posts/1727): на разбор можно дать ссылку,
  // а «назад» в браузере возвращает к доске, а не уводит с панели.
  const navigate = useNavigate();
  const { id } = useParams();
  const выбран = id ? Number(id) : null;

  if (выбран !== null) {
    return <PostTrace id={выбран} назад={() => navigate('/admin/dashboard/posts')} />;
  }
  return (
    <div className="dash-card" style={{ padding: '16px 18px' }}>
      <ContentKanban onTrace={(cid) => navigate(`/admin/dashboard/posts/${cid}`)} />
    </div>
  );
}
