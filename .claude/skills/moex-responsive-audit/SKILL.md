---
name: Responsive Audit (Frontend)
description: Run a full responsive/editorial design audit of recent frontend changes. Catches anti-patterns specific to Фрейм — glass effects, hardcoded sizes, old colors, missing fluid scale, paper-bg violations, mobile touch issues. Use when user says "проверь верстку", "проверь мобилку", "audit responsive", "проверь компоненты на мобилке", or after a batch of frontend edits before deploying.
---

# Responsive Audit Skill

When invoked:
1. Identify scope — recent edits via `git diff --name-only HEAD~1` for frontend files, OR specific files mentioned by user
2. Delegate to `moex-responsive-auditor` agent which scans for violations
3. Present concise report to user

## Trigger phrases

- "проверь верстку"
- "проверь мобилку"
- "проверь responsive"
- "audit frontend"
- "audit responsive"
- "что не так с дизайном"
- "перед деплоем проверь"
- After 5+ frontend edits in a row (proactive trigger)

## What it catches

- **Glass effects** — `backdrop-blur` (should be paper)
- **Old colors** — `#C8FF2E`, `#8b5cf6`, `#6366f1`, `#FF4D4D`, `#2EE59D` hardcodes
- **White inside paper** — `bg-theme-secondary` inside chart containers
- **Fixed chart heights** — `height={450}` instead of `useFitToViewport`
- **Inline pixel font-sizes** — `style={{ fontSize: '14px' }}`
- **Tailwind arbitrary text** — `text-[14px]`
- **Hardcoded spacing** — `gap-3`, `p-5` instead of `var(--sp-*)`
- **Legacy `btn-control`/`btn-group-scroll`** classes
- **Missing `editorial-press`** on interactive elements

## Workflow

```
1. Bash: git diff --name-only HEAD~1 -- "frontend/src/**/*.{tsx,ts,css}"
2. Agent: moex-responsive-auditor with file list
3. Return structured report with priority-ordered fixes
```

## Output format

Structured markdown report grouped by:
- **CRITICAL** (must fix — visible bugs)
- **HIGH** (should fix — design system violations)
- **MEDIUM** (nice to have — minor inconsistencies)
- **OK** (clean files)

Each issue includes file:line + 1-line fix suggestion.

## Scope

- Frontend `*.tsx`, `*.ts`, `*.css` files only
- Skip `pages/methodology/*` (legacy)
- Skip `*.test.tsx`, `node_modules`, `dist`
- Don't modify files — only report
