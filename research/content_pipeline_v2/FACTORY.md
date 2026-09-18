# Завод постов — карта

Актуально на 18.09.2026. Что меняется — в [CHANGELOG.md](CHANGELOG.md), правила редактора — в
[editor_rules.yaml](editor_rules.yaml). Агент завода — `.claude/agents/moex-content-factory.md`.

## Три конвейера

| | Новости по тикеру | Находки | Связки |
|---|---|---|---|
| Источник | MarketTwits, newssmartlab (хайп по репостам), календарь MOEX, раскрытия FinanceMarker | детекторы по нашим рядам | новость + несколько наших рядов, или только ряды |
| `source` в `content_candidates` | markettwits, newssmartlab, moex_calendar, fm_disclosure | insight | combo |
| Кто создаёт | `signals/tg_hype_scan.py` (*/2), `moex_calendar_scan.py` (06:00), `fm_disclosure_scan.py` | `signals/insight_scan.py` (`30 7 * * 2-6` UTC) | `signals/combo_scan.py` (`--mode data` 40 7 * * 2-6, `--mode news` */20 6-17 * * 1-5) |
| Отбор | Шаг А (Routine) → `apply_step_a`; Шаг Б `content_match.py` (*/5): новость ↔ аномалия позиций | `detect_window` → `drop_low_activity` → `drop_expiry_days` → `pick` | те же отсевы → `combos.Engine` (темы, ноги, главная нога) |
| Карточка писателю | `_build_brief` (`signals/content_ai.py`), JSON | `cards.build_card` + `brief_text` (`signals/insights/cards.py`) | `combos.brief` (`signals/insights/combos.py`) |
| Писатель (Routine) | «Шаг В: писатель по новости» `trig_01KPtMNbEYNfqewKvwhdo4rj` → `prompt_step_c_v2_routine.md` | «Шаг В: писатель по находке» `trig_0117KQ5EwUUpb35LLEsAq2Dc` → `prompt_insight_writer_routine.md` | тот же писатель по находке, раздел «ЖАНР СВЯЗКА» |
| Проверка | style-check (сам писатель) + судья Шаг Г (`TRIGGER_ID_STEP_G`, `prompt_step_g_routine.md`) — может править текст | `api/services/insight_check.py` при приёмке (числа из карточки, прогноз, заготовки, форма) | как у находок |
| Кто запускает писателя | `content_match.py` (сразу при совпадении) и `content_ai.py` (*/15, бэкстоп) | `content_ai.py` | `content_ai.py` |

⚠️ У новостей ДВА пути запуска писателя: `content_match` и `content_ai`. Любой отсев ставить в оба
(18.09: повтор Самолёта #2375 прошёл через `content_match`).

⚠️ Routine-писатели читают промпт из GitHub main при каждом запуске (`cat research/...md`). Правка
промпта действует сразу после мёржа, деплой сервера не нужен. Карточки и фильтры — код на сервере:
нужен деплой (CI после мёржа).

## Общие фильтры и проверки

| Что | Где | Конвейеры |
|---|---|---|
| Повтор по тикеру за 3 дня (внутри вида: данные / новость) | `content_ai._repeat_of_ticker` (+ `content_match`), `combo_scan` (по инструменту) | все |
| Новость старше 36 ч | `content_ai._stale_news` (+ `content_match`) | новости |
| Малоактивные контракты (как на сайте) | `insight_scan.drop_low_activity` ← `api/services/oi_screener.low_activity_set` | находки, связки |
| Экспирация: день ±1 торговый | `signals/insights/expiry.near_expiry`; `drop_expiry_days`; в новостном брифе — запрет | все |
| Ряд после перерыва > 60 дней | `detect._after_last_gap` | находки, связки |
| Сверка с утром (5-мин. данные) | `cards.intraday_now`; отсев ≤−20% в `insight_scan` | находки (связки — строка) |
| История после 2022, тренд, итог толпы, ставка к фондам | `cards.py` | находки, связки |
| Числа из карточки, прогноз, заготовки, «писали, что», «медиана» | `insight_check.py` | находки, связки |

## Бот ревью

`signals/content_review_bot.py`, systemd `frame-content-review-bot` (long-poll).
⚠️ Деплой его НЕ перезапускает: после правки — `systemctl restart frame-content-review-bot`.
Карточка + график (находки/связки) админу и коллеге (`CONTENT_DRAFT_EXTRA_CHAT_IDS`), «🔎 Контекст» —
выжимка `signals/brief_summary.py`, кнопки: Одобрить (публикует в @FrameTool), Править (`fire_revision`),
Отклонить (код причины из `config.REVIEW_REASON_CODES`).

## Обратная связь

- `content_candidates`: `status`, `draft_text_ai` (оригинал ИИ, не трогать), `draft_text` (вышедший текст),
  `judge_*`, `reviewer_reason_code`/`reviewer_reason`.
- `content_feedback`: снимок на каждое решение — approved / rejected / comment / edited / judge_fixed /
  revision_requested.
- Вадим кнопки не жмёт — после его разбора отмечаем сами (приглянулось = published, иначе rejected с кодом,
  без решения — comment). Шаблон SQL — ниже.

## Данные

- Позиции дневные: `open_interest` interval=24 (ночной прогон D+1, окно «по вчера МСК»); 5-минутные —
  interval=5, Algopack (ключ до ~10.09.2027).
- Потоки фондов — с `api/services/fund_reorg.py` (как на сайте); ставки — `index_data` RUSFAR3M, RUSFARCNY.
- Отчёт ЦБ — `cbr_flows` (ручной ингест, скилл moex-cbr-flows); ноги физлиц, ДУ, СЗКО.
- Посты канала — `channel_posts` (только @FrameTool; Пульс Т-Банка не виден).

## Операции

Отметить решение (одна транзакция; повторяет бот):
```sql
UPDATE content_candidates SET status='published', reviewer_action='approved', reviewer_id=2,
  published_at=now(), reviewer_reason=:note, updated_at=now() WHERE id=:id AND status IN ('draft_ready','in_review');
INSERT INTO content_feedback (candidate_id, event, draft_ai, draft_human, reason_code, reason_text, brief_version,
  judge_verdict, judge_failed, judge_defects, judge_paragraphs, reviewer_id)
SELECT id, 'approved', coalesce(draft_text_ai, draft_text), draft_text, NULL, :note, brief_version,
  judge_verdict, judge_failed, judge_defects, judge_paragraphs, 2 FROM content_candidates WHERE id=:id;
```
Отклонить — `status='rejected'`, `reviewer_action='rejected'`, `reviewer_reason_code`, event `rejected`.
Замечание без решения — только строка `content_feedback` с event `comment`.

Перепрогнать находку через завод: собрать карточку `insight_scan.build`, вставить кандидата `_INSERT`,
сразу `content_ai._MARK_DISPATCHED` (иначе отсев повтора в цикле), запустить `content_ai._fire(
TRIGGER_ID_STEP_C_INSIGHT, token, _insight_payload(...))`. Скрипт запускать на сервере из `/opt/frame`
с окружением `content_ai.sh`; через stdin, не heredoc с кавычками.

Проверка кода до деплоя: `tar signals api` → `/tmp/<name>` на сервере, `PYTHONPATH=/tmp/<name>`,
`DB_URL` из `.env` с `@127.0.0.1`; только чтение прод-БД.

## Известные ловушки

- `content_review_bot` не перезапускается деплоем.
- Импорт `content_ai` вне `/opt/frame` падает на `pipeline_heartbeat` — запускать из `/opt/frame`.
- `oi_daily` в `signals/insights/data.py` — имя запроса, а не таблица (`open_interest` interval=24).
- Дневной ОИ за день D грузится ночным прогоном D+1; если прогон пропал — `fetch_oi_daily_realtime.py --once`
  в `frame-orchestrator-1`.
- `channel_posts` — только @FrameTool.
