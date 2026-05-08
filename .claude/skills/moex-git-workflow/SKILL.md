---
name: moex-git-workflow
description: Commit, push, or manage git for the Фрейм project. Use when user says "коммит", "закоммить", "пуш", "залей на гитхаб", "push", "commit this", or any git operation. Also use before any deployment to ensure code is versioned. Applies project-specific rules about co-authorship, commit format, and files to exclude.
---

# Git Workflow for Фрейм

Project-specific conventions for commits and pushes.

## ⚠️ Critical Rules

1. **NEVER commit without user confirmation** — unless user explicitly says "коммит + пуш" or similar
2. **Don't commit files user said to exclude** — track exclusions across the conversation. Common examples: work-in-progress experimental features, untested code
3. **Do NOT add `Generated with Claude Code` footer** unless the user asks for it
4. **DO add `Co-Authored-By`** with current Claude model version (see below)
5. **Pull --rebase on conflict** — never merge commits
6. **Never force-push to main** without explicit user approval

## Commit Message Format

```
type(scope): subject line (50-72 chars)

Body paragraph explaining WHY not WHAT. 2-4 sentences max.
Mention side effects, reasoning, alternatives considered.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
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

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
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

## Standard Commit Workflow

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

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"

# 4. Push
git push origin main
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
- No CI configured (no checks to wait for)

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
