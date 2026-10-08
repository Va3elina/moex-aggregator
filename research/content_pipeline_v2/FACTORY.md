# Завод постов — карта

Актуально на 18.09.2026. Что меняется — в [CHANGELOG.md](CHANGELOG.md), правила редактора — в
[editor_rules.yaml](editor_rules.yaml). Агент завода — `.claude/agents/moex-content-factory.md`.

## Три конвейера

| | Новости по тикеру | Находки | Связки |
|---|---|---|---|
| Источник | MarketTwits, newssmartlab (хайп по репостам), календарь MOEX, раскрытия FinanceMarker | детекторы по нашим рядам | новость + несколько наших рядов, или только ряды |
| `source` в `content_candidates` | markettwits, newssmartlab, moex_calendar, fm_disclosure | insight | combo |
| Кто создаёт | `signals/tg_hype_scan.py` (*/2), `moex_calendar_scan.py` (06:00), `fm_disclosure_scan.py`; макро важности 5 без компании — `signals/macro_scan.py` (из `combo_scan --mode news`, черновик `insight_macro`) | `signals/insight_scan.py` (`30 7-17 * * *` UTC, прогон раз на торговый день — `insights/fresh.py`); по ходу дня — `signals/trigger_scan.py` (из `combo_scan --mode news`, 12–21 МСК, до двух черновиков в день): повод дня по живой цене и рывок позиции физлиц по 5-минутному срезу (`cards.surge_angle`, ход в 8+ обычных дневных и от 10%, только наш рынок); находки дня считаются один раз — файл `combo_cache/detections_ДАТА.json` (`insight_scan.detections`), его берут все три сканера | `signals/combo_scan.py` (`--mode data` 40 7-17 * * * — так же, `--mode news` */20 6-17 * * 1-5) |
| Отбор | Шаг А (Routine) → `apply_step_a` (тип события — из словаря мозга, `_единый_тип`); Шаг Б `content_match.py` (*/5): новость ↔ аномалия позиций; новость про отрасль без компании → компания отрасли, ответившая данными (R30, `promote_sector_news`) | `detect_window` → `drop_low_activity` → `drop_expiry_days` → `pick` (3 позиции-тренд + до 2 постов новых типов `angle_jobs` — концентрация, повод дня, доля; 2 фонда; 1 сезонность — индекс и доллар своими детекторами, остальные активы правилом витрины /hot, `season.py`, R34) | те же отсевы → `combos.Engine` (темы новости — словарь мозга `news_types.sql_topics`, свои только триггеры; ноги, главная нога) |
| Карточка писателю | `_build_brief` (`signals/content_ai.py`), JSON | `cards.build_card` + `brief_text` (`signals/insights/cards.py`) | `combos.brief` (`signals/insights/combos.py`) |
| Картинка | — | `cards.draw_chart`; фонды, сделки фондов, макро — `signals/insights/charts.py` (простые технические, R33) | та же |
| Писатель (Routine) | «Шаг В: писатель по новости» `trig_01KPtMNbEYNfqewKvwhdo4rj` → `prompt_step_c_v2_routine.md` | «Шаг В: писатель по находке» `trig_0117KQ5EwUUpb35LLEsAq2Dc` → `prompt_insight_writer_routine.md` | тот же писатель по находке, раздел «ЖАНР СВЯЗКА» |
| Проверка | style-check (сам писатель) + судья Шаг Г (`TRIGGER_ID_STEP_G`, `prompt_step_g_routine.md`) — может править текст | `insight_check.py` при приёмке + с 18.09 судья Шаг Г по карточке (`_SELECT_JUDGE_PENDING_CARD`); с 27.09 текст правит, только если провалены ворота A (числа, выдумки, противоречия, время), остальное — замечания; бот ждёт судью до 45 мин | как у находок |
| Кто запускает писателя | `content_match.py` (сразу при совпадении) и `content_ai.py` (*/15, бэкстоп) | `content_ai.py` | `content_ai.py` |

⚠️ У новостей ДВА пути запуска писателя: `content_match` (сразу при совпадении) и бэкстоп `content_ai`. С 27.09
отсев у них ОДИН — `content_ai._news_writer_gate` (повтор, возраст, R16, R17, срез позиций после новости); новый отсев
добавлять туда, а не в каждый путь (18.09 повтор Самолёта #2375 прошёл через `content_match`).

⚠️ Routine-писатели читают промпт из GitHub main при каждом запуске (`cat research/...md`). Правка
промпта действует сразу после мёржа, деплой сервера не нужен. Карточки и фильтры — код на сервере:
нужен деплой (CI после мёржа).

## Общие фильтры и проверки

| Что | Где | Конвейеры |
|---|---|---|
| Повтор по тикеру за 3 дня (внутри вида: данные / новость) | `content_ai._repeat_of_ticker` (+ `content_match`), `combo_scan` (по инструменту) | все |
| Новость старше 36 ч | `content_ai._stale_news` (+ `content_match`) | новости |
| Сюжет / находка не на последнем дне данных (после экспирации «последний день» откатывался, #2491) | `combo_scan.run_once` (день данных, нога ≤ 4 дн., новость ≤ 36 ч), `insight_scan.pick` | находки, связки |
| Цена не отреагировала: ход после новости / обычный дневной < ×1,9 (R16); цена «до» — свеча, закрытая к выходу новости | `content_ai._weak_reaction` (+ `content_match`) | новости |
| Отраслевая новость → компания: её назвал Шаг А или хэштег отрасли, после новости всплеск позиций физлиц и цена ×1,9 (R30); одна компания, отказ писателя окончательный | `content_match.promote_sector_news` ← `content_ai._sector_check`, бриф — `_sector_brief` | новости |
| Дивиденд без сюрприза: доходность < 6% (R17) | `content_ai._div_no_surprise` (+ `content_match`) | новости (календарь, раскрытия) |
| Малоактивные контракты (как на сайте) | `insight_scan.drop_low_activity` ← `api/services/oi_screener.low_activity_set` | находки, связки |
| Экспирация: день ±1 торговый | `signals/insights/expiry.near_expiry`; `drop_expiry_days`; в новостном брифе — запрет | все |
| Ряд после перерыва > 60 дней | `detect._after_last_gap` | находки, связки |
| Сверка с утром (5-мин. данные) | `cards.intraday_now`; отсев ≤−20% в `insight_scan` | находки (связки — строка) |
| История после 2022, тренд, итог толпы, ставка к фондам | `cards.py` | находки, связки |
| Числа из карточки, прогноз, заготовки, «писали, что», «медиана» | `insight_check.py` | находки, связки |

## Метрики

`factory_metrics.sql` — скорость (новость → кандидат → черновик → судья → бот), воронка по источникам с причинами
отсева, провалы фактических проверок судьи, решения людей, хвост черновиков без решения. Окно `-v days=N`:
`ssh root@… 'docker exec -i frame-db-1 psql -U postgres -d moex_db -v days=30' < research/content_pipeline_v2/factory_metrics.sql`.
Срез 27.09 (30 дней): новость → черновик — медиана 17 ч (ждёт дневной срез позиций), находка — 17 мин; Шаг А отбрасывает
90% хайпа; объявления МосБиржи 30 → 0 черновиков; у находок «число без опоры» — почти всегда размытая дата.

## Бот ревью

`signals/content_review_bot.py`, systemd `frame-content-review-bot` (long-poll).
⚠️ Деплой его НЕ перезапускает: после правки — `systemctl restart frame-content-review-bot`.
С 19.09 карточка — rich-сообщение (`sendRichMessage`, Bot API 10.1+), как посты Т-Банка: график внутри
сообщения, сам пост, под ним два раскрывающихся раздела `<details>` — «Новость» (у находок и связок —
«Находка» / «Связка»: #id, выжимка `brief_summary`, почему взяли) и «Судья: вердикт» (провалы, правка,
сомнения по абзацам). Внутри разделов — короткие пункты без эмодзи (`_card_rich`). Telegram отказал —
бот шлёт прежний формат (фото + пост со свёрнутыми цитатами, `_card_view`). После решения у rich-карточки
меняются только кнопки (строка «✅ опубликовано» / «❌ отклонено»). Получатели — админ и коллега
(`CONTENT_DRAFT_EXTRA_CHAT_IDS`); кнопки: Одобрить (публикует в @FrameTool), Править (`fire_revision`),
Отклонить (код причины из `config.REVIEW_REASON_CODES`).

Мандаты (`api/routers/mandate_scan.py`, Routine «Ресёрч · поиск новых мандатов», 08:00 UTC) — не завод,
но приходят в тот же бот. С 19.09 скаут шлёт `event_date`; событие старше 7 дней записывается в
известные без уведомления.

## Обратная связь

- `content_candidates`: `status`, `draft_text_ai` (оригинал ИИ, не трогать), `draft_text` (вышедший текст),
  `judge_*`, `reviewer_reason_code`/`reviewer_reason`.
- `content_feedback`: снимок на каждое решение — approved / rejected / comment / edited / judge_fixed /
  revision_requested.
- Вадим кнопки не жмёт — после его разбора отмечаем сами (приглянулось = published, иначе rejected с кодом,
  без решения — comment). Шаблон SQL — ниже.

## Данные

- Позиции дневные: `open_interest` interval=24 (ночной прогон D+1, окно «по вчера МСК»); 5-минутные —
  interval=5, Algopack (ключ до ~10.09.2027). Пятницу МосБиржа выкладывает в субботу утром: оркестратор в
  выходной догружает её раз в час (`run_weekend_daily_oi`), сканеры находок и связок ждут последний торговый
  день и прогоняются на нём один раз (`signals/insights/fresh.py`, отметки `/opt/frame/data/scan_state/*.done`).
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
  в `frame-orchestrator-1`. До 27.09 пятница доезжала только в полночь на воскресенье, и завод её не видел.
- Сканер находок / связок «молчит» — в логе `пропуск: ждём дневные позиции за …` или `уже прогнан`. Прогнать
  вручную, не дожидаясь: `insight_scan.sh --force`, `combo_scan.sh --mode data --force`.
- `channel_posts` — только @FrameTool. ⚠️ Текст там обрезан (~500 знаков): хэштег рубрики в конце поста может не дойти
  — фильтр по рубрике держать со страховкой словами (`insight_scan.repeat_of`, 27.09).
- Подсказка мозга Шагу А (`_brain_hint_for_step_a`) — хэштеги автора [B] и имена компаний в тексте [C]; слоя «похоже по
  смыслу» с 27.09 нет (шум). Смысловой вызов с хоста требует `EMBED_MODEL_DIR=/opt/frame/models/…` — путь по умолчанию
  контейнерный (с 08.09 из-за этого падал поиск). Эмбеддинги мозга считает `Brain/brain_embed.py` в оркестраторе раз в
  15 мин; новости без тикеров — с 27.09 за год, только с темой рынка и «про наш рынок» (`новости_без_компании`,
  метка `payload.без_компании`, тип по темам, отрасль по теме).
- Контекст новости у завода — из единого поиска мозга (`brain_core.события_вокруг`, `похожие_события`,
  `отрасли_текста`): компания, связанные владением, отрасль; похожие — та же компания или отрасль, тот же тип.
  Своего списка хэштегов отраслей у завода больше нет (vocab.ОТРАСЛИ).
- Единая разметка мозга (`Brain/vocab.py`, `brain_sync.разметка`, с 27.09): у каждого события тип (ребро «тип» к
  `tag:тип/…`) и отрасль (ребро «отрасль» к сектору), на ребре способ и уровень; шум в мозг не идёт. Правка словаря —
  новая `vocab.ВЕРСИЯ` (одна полная переразметка при следующем синке) и прогон `research/brain/eval_brain.py` до и
  после. ⚠️ Полная переразметка (73 тыс. узлов) подходит к лимиту оркестратора (10 мин): правка без смены версии
  действует на новые узлы, а уже лежащее досылается разовым шагом с отметкой в brain_sync_state (labels_ops_v1,
  noise_v2, tickerless_retype_v1). Остаток без типа за два года — ночному аудитору (режим labels, все виды узлов;
  история — в окна 02:20 и 03:20 МСК).
- Одно определение типа и темы на весь завод (с 27.09): Шаг А — тип при приёмке из словаря (`content_news._единый_тип`
  → `brain_core.тип_текста`, код завода — `vocab.ТИП_В_КОД_ЗАВОДА`); связки — темы новости из словаря
  (`news_types.sql_topics`, все темы, а не одна главная), свои у связок только триггеры (отчёт ЦБ о потоках, Минфин и
  валюта, ход индекса). Шум — один фильтр `vocab.sql_шум`; с каналом и хэштегами он же отсекает пост MarketTwits без
  хэштегов (реклама, «ВПЕРЕДИ»). Новый классификатор в заводе не заводить — правило в словарь.
- Карточка находки по акции — контекст компании, как в брифе новостей: «о компании (второй мозг)» (досье
  `content_ai._brain_block`: отрасль, владельцы, чем владеет, фонды, индексы, что писали, рядом в новостях), «что было у
  компании и в отрасли в эти дни» (`_news_around`, цена за 2 часа после) — `insight_scan.company_lines`; «связанные
  компании» — `insight_scan.related_lines` ← `_related_context` (прямые связи с новостями, слабые — вопросом).
- Тесты `research/brain` и `research/content_pipeline_v2` гоняет CI (build.yml → python-tests, с 27.09); локально —
  `.venv` основной рабочей копии: `DB_URL=postgresql+pg8000://x:y@127.0.0.1:1/x .venv/bin/python -m pytest -q research/brain research/content_pipeline_v2`.
- У находок/связок `judge_verdict` сначала ставит проверка кодом; признак «живой судья разобрал» — `judge_items`.
