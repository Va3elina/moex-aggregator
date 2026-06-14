---
name: moex-git-workflow
description: Commit, push, or manage git for the Фрейм project. Use when user says "коммит", "закоммить", "пуш", "залей на гитхаб", "push", "commit this", or any git operation. Also use before any deployment to ensure code is versioned. Applies project-specific rules about co-authorship, commit format, and files to exclude.
---

# Git Workflow for Фрейм

> ## ⚠️ ДЕПЛОЙ ТЕПЕРЬ АВТО-CI (с 2026-06-09)
>
> **Деплой = `git push` в `main`.** GitHub Actions сам делает build-check → (если зелёный) → deploy-prod (SSH на прод сам, `git reset --hard origin/main` + rebuild).
>
> - **НЕ деплоить руками по SSH** — CI делает это сам. Если пользователь говорит «задеплой» — это значит **закоммить и запушить в main**.
> - **Сервер `/opt/frame` = чистый deploy-target, НЕ воркспейс.** Деплой выполняет `git reset --hard origin/main` → **любая правка/scp прямо на проде СТИРАЕТСЯ** следующим деплоем. Все изменения — только через `git push`. **НЕ scp-ить** черновики/research на `/opt/frame`.
> - **Сериализация**: `concurrency: deploy-prod` сериализует workflow-прогоны, но пачка быстрых пушей в `main` всё равно может поймать гонку на пересоздании контейнера (`removal of container ... is already in progress` — реальный случай 14.06.2026: один `deploy-prod` упал, прод поднялся от выигравшего). Ещё одна причина копить правки в ветке и мёржить пачкой. Битый билд НЕ выкатывается (deploy ждёт зелёный build-check).
> - Ручной SSH-деплой остаётся только как **аварийный** путь (CI недоступен) — см. секцию «Аварийный ручной деплой» ниже.
> - Детали в памяти: `ci_cd.md`, `deploy_manual.md`.

Project-specific conventions for commits and pushes.

## ⚠️ Critical Rules

1. **NEVER commit without user confirmation** — unless user explicitly says "коммит + пуш" or similar
2. **Don't commit files user said to exclude** — track exclusions across the conversation. Common examples: work-in-progress experimental features, untested code
3. **Do NOT add `Generated with Claude Code` footer** unless the user asks for it
4. **DO add `Co-Authored-By`** with current Claude model version (see below)
5. **Pull --rebase on conflict** — never merge commits
6. **Never force-push to main** without explicit user approval

## Командная работа — ветки + PR (ДЕФОЛТ с 2026-06-14)

Канонический свод правил для людей — **`CONTRIBUTING.md` в корне репо**; скилл
обязан ему соответствовать. Над репозиторием работают двое (Вадим +
коллега, git-identity `vadim@frame.local`), поэтому **дефолт — НЕ прямой коммит
в `main`, а ветка → Pull Request → мёрж**:

1. Старт задачи: `git checkout main && git pull --rebase origin main && git checkout -b <type>/<kebab>`.
2. Коммиты — в ветку; `git push -u origin <branch>` (это **НЕ** деплой).
3. `gh pr create --base main --fill` → CI прогоняет `build-check` на PR
   (зелёный/красный, **БЕЗ** деплоя; `build.yml` слушает `pull_request → main`).
4. Зелёный → `gh pr merge --squash --delete-branch`. **Мёрж в `main` = деплой.**

**Прямой push в `main` — ТОЛЬКО** когда пользователь явно просит (мелкий
solo-фикс, грандфазинг уже готовой работы). По умолчанию НЕ делать.

`pull --rebase` всегда; никаких merge-коммитов; `--force` только в свою ветку и
только `--force-with-lease`. Перед параллельной правкой одного файла — свериться,
кто что трогает.

## Commit Message Format

```
type(scope): subject line (50-72 chars)

Body paragraph explaining WHY not WHAT. 2-4 sentences max.
Mention side effects, reasoning, alternatives considered.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
```

**Types** used in this project:
- `fix` — bug fix (most common, ~60% of commits)
- `feat` — new feature/indicator
- `refactor` — code improvement without behavior change
- `chore` — tooling, deps, infra
- `docs` — documentation changes

**Scope** examples from this repo:
- `seasonality`, `buffett`, `heatmap`, `funds` — indicators
- `api`, `frontend` — large areas
- `deploy`, `docs` — operations

## Using HEREDOC for Multi-line Messages

Always use this pattern to preserve formatting:

```bash
git commit -m "$(cat <<'EOF'
fix(scope): short subject here

Longer body explaining the change and why it matters.
Can be multiple paragraphs.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

## Files to Always Exclude

These should NEVER be in a commit (add to `.gitignore` if not already):

```
.dev-pids/
_old_simplechart_refactor.patch
cbonds_capture.flow
*.pdf (unless explicitly asked)
scripts/* (unless explicitly asked)
/tmp/*
.env
.env.local
```

Also exclude work-in-progress features the user asked to not commit yet.

## Check Before Committing

Run these checks **every time** before committing:

```bash
# 1. See all changes
git status

# 2. Review what's staged vs unstaged
git diff --stat
git diff --cached --stat  # staged only

# 3. Check for secrets (env files, keys, tokens)
git diff --cached | grep -iE "password|secret|api[_-]?key|token|bearer"

# 4. Review untracked files before adding
ls -la  # or specific directories
```

## Standard Commit Workflow (прямой push в main — НЕ дефолт)

> Дефолт — **ветка + PR** (см. «Командная работа» выше и `CONTRIBUTING.md`).
> Этот прямой-в-`main` рецепт применяется ТОЛЬКО когда пользователь явно
> попросил запушить в `main` (solo-мелочь / грандфазинг готовой работы).

When user says "коммит + пуш":

```bash
# 1. Check state
git status
git diff --stat

# 2. If there are unrelated/unwanted changes, ask user or stage selectively
git add specific/file1.py specific/file2.ts  # named adds, NOT `git add -A`

# 3. Commit with HEREDOC
git commit -m "$(cat <<'EOF'
fix(scope): subject

Body.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"

# 4. Push  ⚠️ ЭТО ТРИГГЕРИТ АВТО-ДЕПЛОЙ НА ПРОД (build-check → deploy-prod)
git push origin main

# 5. (опц.) проследить за CI/CD-прогоном
gh run list --branch main --limit 3
# gh run watch   # дождаться build-check + deploy-prod
```

> **После пуша деплой произойдёт сам.** Не лезь на сервер делать `git pull`/rebuild руками — это делает CI. Если build-check красный, deploy НЕ запустится (битый билд на прод не попадёт) — чини билд и пуш заново.

## Аварийный ручной деплой (только если CI недоступен)

Обычно НЕ нужно — деплоит CI. Использовать только когда GitHub Actions недоступен/упал.

**Вариант A — перезапустить workflow вручную (предпочтительно):**

```bash
gh workflow run deploy-prod
```

**Вариант B — напрямую на сервере** (SSH-преамбула ниже). Это ровно то, что делает CI:

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 \
    -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  'cd /opt/frame && git fetch && git reset --hard origin/main && \
   docker compose build api && docker compose up -d --force-recreate api'
```

- **`git reset --hard origin/main`, НЕ `git pull`** — сервер это чистый target, локальных правок там быть не должно.
- **orchestrator** пересобирать отдельно, если менялся его код.
- **nginx НЕ трогать** (resolver-фикс — рестарт ломает резолв).

## SSH на прод (логи / SQL / инспекция / аварийный деплой)

SSH остаётся нужен для логов, БД, инспекции и аварийного деплоя — НЕ для штатной выкатки.
Преамбула (всегда так):

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 \
    -i ~/.ssh/id_ed25519 root@103.88.243.232
```

Полезные проверки после деплоя:

```bash
# health бэкенда
curl -s https://xn--80aklbnczmv.xn--p1ai/health

# версия Service Worker (frame-<hash>) — менялся ли фронт
curl -s https://xn--80aklbnczmv.xn--p1ai/sw.js | grep -o 'frame-[0-9a-f]\{8\}'

# логи api-контейнера на сервере
docker compose -f /opt/frame/docker-compose.yml logs --tail=100 api

# SQL на проде
docker exec frame-db-1 psql -U postgres -d moex_db -c 'SELECT ...;'
```

## Handling Push Rejections

If `git push` fails with "Updates were rejected":

```bash
# Pull with rebase (preserves your commit on top)
git pull --rebase origin main

# If clean rebase — push again
git push origin main

# If conflicts during rebase — resolve, git add, then:
git rebase --continue
git push origin main
```

## Reviewing Recent History

Before making new commits, check style:

```bash
git log --oneline -10

# Examples from this repo:
# dc12762 fix(heatmap): proportional sector sizing by market cap
# 484bc6b fix(seasonality): Dec-to-Dec base + exclude current year
# 25b249f fix(buffett): right Y-axis formatting + 2 decimal places
```

Match this style — terse subject, clear type/scope.

## User Identity

Git config is auto-based on hostname, which shows as:
```
Committer: Вадим <vadim@Mac-mini-Vadim.local>
```

This is fine — the user hasn't asked to customize it. Don't suggest changes unless they bring it up.

## Remote

- Origin: `git@github.com:Va3elina/moex-aggregator.git`
- Main branch: `main`
- **CI/CD активирован**: `git push` в `main` → GitHub Actions `build-check` → (если зелёный) → `deploy-prod` (авто-деплой на прод по SSH). **Push в main = выкатка на прод.** После пуша можно проверить статус прогона: `gh run list --branch main --limit 3` / `gh run watch`.

## Multi-commit Scenarios

If user made several logical groups of changes, propose splitting:

> "I see changes in both heatmap (sizing fix) and funds (event binding).
> Should I make two commits for cleaner history?"

Only do this if user confirms — default is single commit with broader message.

## Exclusion Tracking

Track these across the session:
- Files user explicitly said "не коммить"
- New indicators marked "experimental"
- Deleted code that might need restoration later

If unsure whether to include something — ASK before staging.
