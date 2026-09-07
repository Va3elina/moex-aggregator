#!/usr/bin/env node
/**
 * i18n-check — находит ключи t('…') в исходниках, для которых нет перевода
 * ни в одном src/i18n/en/*.json, и дубликаты ключей с разным переводом.
 *
 *   node scripts/i18n-check.mjs                # весь src
 *   node scripts/i18n-check.mjs src/pages/OpenInterestPage.tsx src/components/oi
 *
 * Exit 1, если есть пропуски — удобно в CI/перед коммитом.
 */
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const EN_DIR = join(ROOT, 'src/i18n/en');

function walk(p, out = []) {
  const st = statSync(p);
  if (st.isDirectory()) {
    for (const f of readdirSync(p)) walk(join(p, f), out);
  } else if (/\.(tsx?|jsx?)$/.test(p) && !p.endsWith('.d.ts')) out.push(p);
  return out;
}

const dict = new Map();
const conflicts = [];
for (const f of readdirSync(EN_DIR).filter((f) => f.endsWith('.json'))) {
  const obj = JSON.parse(readFileSync(join(EN_DIR, f), 'utf8'));
  for (const [k, v] of Object.entries(obj)) {
    if (dict.has(k) && dict.get(k).v !== v) conflicts.push({ k, a: dict.get(k), b: { f, v } });
    dict.set(k, { f, v });
  }
}

const targets = process.argv.slice(2);
const files = (targets.length ? targets : ['src']).flatMap((t) => walk(resolve(ROOT, t)));

// t('…') / t("…") / t(`…`) без ${}. Ключи с интерполяцией {{ }} допустимы.
const KEY_RE = /\bt\(\s*(?:'((?:[^'\\]|\\.)*)'|"((?:[^"\\]|\\.)*)"|`((?:[^`\\$]|\\.)*)`)\s*[,)]/g;

const missing = new Map();
for (const file of files) {
  const src = readFileSync(file, 'utf8');
  for (const m of src.matchAll(KEY_RE)) {
    const key = (m[1] ?? m[2] ?? m[3]).replace(/\\(['"`])/g, '$1');
    if (!/[А-Яа-яЁё]/.test(key)) continue; // англ./тех. ключи пропускаем
    if (!dict.has(key)) {
      if (!missing.has(key)) missing.set(key, []);
      missing.get(key).push(file.replace(ROOT + '/', ''));
    }
  }
}

if (conflicts.length) {
  console.log(`⚠ Конфликты переводов (${conflicts.length}):`);
  for (const c of conflicts) console.log(`  "${c.k}"\n     ${c.a.f}: ${c.a.v}\n     ${c.b.f}: ${c.b.v}`);
}
if (missing.size) {
  console.log(`✗ Без перевода (${missing.size}):`);
  for (const [k, where] of missing) console.log(`  "${k}"  ← ${[...new Set(where)].join(', ')}`);
  process.exit(1);
}
console.log(`✓ Все ${dict.size} ключей покрыты, пропусков нет (${files.length} файлов).`);
