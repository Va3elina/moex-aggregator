# 👋 Приветственное сообщение для нового чата

Когда откроешь новый чат с Claude — **просто скопируй и вставь блок ниже**.
Claude автоматически прочитает memory-файлы, а это сообщение даст ему
быстрый старт по текущему состоянию.

---

## Что автоматически знает новый Claude

Claude Code **сам прочитает** при старте:

1. **Memory-файлы в `~/.claude/projects/-Users-vadim-PyCharmMiscProject-MOEX/memory/`:**
   - `MEMORY.md` — индекс (главная точка входа)
   - `project_overview.md` — 9 индикаторов, стек, контейнеры
   - `user_profile.md` — кто ты
   - `deploy_manual.md`, `server_security.md`, `server_constraints.md` — инфра
   - `db_schema.md`, `data_sources.md` — БД и источники
   - `indicator_patterns.md` — архитектура индикаторов
   - `billing_system.md` — ⭐ **новое, всё про ЮKassa + invite-ссылки**
   - `recent_changes.md` — ⭐ **новое, последние сессии с коммитами**
   - `techdebt_plan.md` — что ещё надо сделать

2. **Skills в `.claude/skills/`:**
   - `moex-deploy-frontend`, `moex-deploy-backend`, `moex-db-query`
   - `moex-git-workflow`, `moex-new-indicator-scaffold`
   - `moex-simplechart-usage`, `moex-methodology-verify`

3. **Agents в `.claude/agents/`:**
   - `moex-indicator-explorer` — глубокий анализ индикатора
   - `moex-methodology-auditor` — сверка данных с внешними источниками
   - `moex-deploy-verifier` — проверка после деплоя

---

## Сообщение которое скопировать

```
Привет! Продолжаем работу над «Фреймом» (таймфрейм.рф).

Твоя память уже включает всё важное (MEMORY.md, billing_system.md,
recent_changes.md — прочитай их первыми если ещё не сделал).

КРАТКИЙ СТАТУС:
• Последний коммит: f66c41e (feat: invite-ссылки + admin UI)
• SW cache на проде: v121
• 9 индикаторов, все унифицированы CSS-токенами
• Billing: stub-режим (ЮKassa ждёт ключи от коллеги)
• 4 админа (я × 2, Александр Тория × 2)
• Активный invite-токен: PCZgbteCqtedyl7vixmuqkM9ifOfSXzj
  (Premium × 5 × 10 лет, использован 1 раз из 5)

ЧТО НА ГОРИЗОНТЕ:
• ВИМ-парсер holdings (см. Funds/HOLDINGS_RESEARCH.md) — план на 3-4 дня
• Feature flags per-endpoint (пока только tier-levels)
• Включение ЮKassa когда коллега зарегистрируется

ПРАВИЛА РАБОТЫ:
• Русский в UI и комментариях
• Коммитить ТОЛЬКО по моей явной команде
• HEREDOC для multi-line commit messages, Co-Authored-By Claude
• SW cache бампать при каждом frontend deploy
• .claude/ в gitignore (скиллы локальные)

Готов продолжать. Что делаем?
```

---

## Куда ещё можно смотреть

- **Код:** `/Users/vadim/PyCharmMiscProject/MOEX/`
- **Docs:** `/docs/ARCHITECTURE.html`, `Funds/HOLDINGS_RESEARCH.md`,
  `Funds/CBONDS_MIGRATION.md`, `api/billing/README.md`
- **Git history:** `git log --oneline -20` — покажет последние 20 коммитов
- **DB state:** через skill `moex-db-query`

## Если что-то сломалось / не понятно

1. **Читай скиллы** в `.claude/skills/` — там готовые паттерны
2. **Используй агенты** — например `moex-indicator-explorer` перед любыми
   изменениями в индикаторе
3. **Verify before assert** — memory-файлы могут быть устарелыми (старше
   недели). Проверь `git log` и текущий код прежде чем делать выводы.

---

*Последнее обновление: 2026-04-23. Контекст будет актуален пока не будет
крупных изменений архитектуры или деплоя.*
