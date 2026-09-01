#!/bin/bash
# Батч-замер брифа: один прогон вместо шести ручных шагов.
#
# ⚠️ Зачем. Всю настройку 01.09 я вёл на ОДНОМ кандидате, и это скрывало дефекты:
# один прогон не отличает «стало лучше» от «повезло». Батч из шести сразу показал
# два перекоса, которых одиночные прогоны не видели: перетянутую длину (медиана 417
# против 661 у канала) и утечку указаний рамки в текст.
#
# ⚠️ Почему НЕ /loop и не крон. Повторять по таймеру нечего: между прогонами ничего
# не меняется, пока не правишь бриф, а каждый батч стоит ~12 облачных Routine-сессий.
# Запускать надо ПОСЛЕ правки, а не по расписанию.
#
# Использование (на проде):  bash batch_eval.sh [id1 id2 ...]
set -u
IDS="${*:-1104 1185 1277 1638 1658 1102}"
LIST=$(echo "$IDS" | tr ' ' ',')
PSQL="docker exec frame-db-1 psql -U postgres -d moex_db"
RUN=/opt/frame/signals/content_ai.sh

echo "== сброс кандидатов: $LIST"
$PSQL -c "UPDATE content_candidates SET draft_text=NULL, draft_text_ai=NULL,
  brief_version=1, last_checked_at=NULL, dispatch_attempts=0, reviewer_notified_at=NULL,
  judge_items=NULL, judge_verdict=NULL, judge_failed=NULL, judge_defects=NULL,
  judge_note=NULL, judge_paragraphs=NULL, judge_checked_at=NULL,
  judge_dispatch_attempts=0, judge_gave_up_at=NULL WHERE id IN ($LIST);" | tail -1

N=$(echo "$IDS" | wc -w | tr -d ' ')
VER=$(grep -m1 '^BRIEF_VERSION' /opt/frame/signals/content_ai.py | grep -o '[0-9]*')
echo "== Шаг В (бриф v$VER)"; timeout 300 bash $RUN 2>&1 | grep -o "step_c_fired.: [0-9]*" | tail -1

for i in $(seq 1 20); do
  D=$($PSQL -At -c "SELECT count(*) FROM content_candidates WHERE id IN ($LIST) AND brief_version=$VER;")
  printf '%s ' "$D"; [ "$D" = "$N" ] && break; sleep 30
done; echo

echo "== Шаг Г"; timeout 300 bash $RUN 2>&1 | grep -o "judge_fired.: [0-9]*" | tail -1
for i in $(seq 1 20); do
  J=$($PSQL -At -c "SELECT count(*) FROM content_candidates WHERE id IN ($LIST) AND judge_verdict IS NOT NULL;")
  printf '%s ' "$J"; [ "$J" = "$N" ] && break; sleep 30
done; echo

echo; echo "== по кандидатам"
$PSQL -At -c "SELECT id||' | '||judge_verdict||' | '||length(draft_text)||' зн. | '
  ||coalesce(array_to_string(judge_failed,','),'—')||' | деф: '
  ||coalesce(array_to_string(judge_defects,','),'—')
  FROM content_candidates WHERE id IN ($LIST) ORDER BY id;"

echo; echo "== сводка (эталон канала: медиана 661 зн., четверть 559-833)"
$PSQL -At -c "SELECT judge_verdict||': '||count(*) FROM content_candidates
  WHERE id IN ($LIST) GROUP BY judge_verdict ORDER BY count(*) DESC;"
$PSQL -At -c "SELECT 'знаков: медиана '||percentile_disc(0.5) WITHIN GROUP (ORDER BY length(draft_text))
  ||', диапазон '||min(length(draft_text))||'-'||max(length(draft_text))
  ||' | вне 550-950: '||count(*) FILTER (WHERE length(draft_text) NOT BETWEEN 550 AND 950)
  ||' | утечки указаний рамки: '||count(*) FILTER (WHERE draft_text ~* 'отклик на новость|не опережение|от шума|в данных не указано')
  ||' | процент от ОИ: '||count(*) FILTER (WHERE draft_text ~* 'открытого интереса')
  FROM content_candidates WHERE id IN ($LIST);"
