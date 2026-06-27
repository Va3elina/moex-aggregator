#!/usr/bin/env python3
"""Фрейм SEO CLI — Яндекс.Вебмастер + Google Search Console в одном месте.

Зачем: чтобы не искать каждый раз руками — статус индекса, запросы с позициями,
переобход, сабмит sitemap и диагностика по обоим поисковикам одной командой.

Секреты (оба gitignored, см. .gitignore):
  scripts/seo/.env                      — Яндекс OAuth-токен + user_id/host_id
  scripts/seo/gsc-service-account.json  — Google сервисный аккаунт (JWT RS256)

Примеры:
  python3 scripts/seo/seo.py status                      # индекс+проблемы, оба
  python3 scripts/seo/seo.py queries --days 28 --limit 30
  python3 scripts/seo/seo.py queries --engine google
  python3 scripts/seo/seo.py compare                     # бренд/тема рядом
  python3 scripts/seo/seo.py recrawl /heatmap /oi        # Яндекс переобход
  python3 scripts/seo/seo.py diagnostics                 # Яндекс активные
  python3 scripts/seo/seo.py sitemap-submit              # сабмит в оба

⚠️ Не звать curl/этот скрипт внутри zsh for-цикла в Bash-туле (ломается) —
   у CLI для этого свои подкоманды с циклами внутри Python.
"""
import argparse, base64, json, sys, time, urllib.parse
from datetime import date, timedelta
from pathlib import Path

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

HERE = Path(__file__).resolve().parent
SITE = "https://xn--80aklbnczmv.xn--p1ai"
SITEMAP = f"{SITE}/sitemap.xml"
BRAND_HINTS = ("таймфрейм", "frame", "фрейм")


def load_env(path=HERE / ".env"):
    env = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


# ─────────────────────────── Яндекс.Вебмастер ───────────────────────────
class Yandex:
    BASE = "https://api.webmaster.yandex.net/v4"

    def __init__(self, env):
        self.token = env.get("YANDEX_WEBMASTER_TOKEN")
        self.user = env.get("YANDEX_WEBMASTER_USER_ID")
        self.host = urllib.parse.quote(env.get("YANDEX_WEBMASTER_HOST_ID", ""), safe="")
        self.ok = bool(self.token and self.user and self.host)

    def _h(self):
        return {"Authorization": f"OAuth {self.token}"}

    def _u(self, tail):
        return f"{self.BASE}/user/{self.user}/hosts/{self.host}/{tail}"

    def summary(self):
        return requests.get(self._u("summary"), headers=self._h()).json()

    def diagnostics(self):
        d = requests.get(self._u("diagnostics/"), headers=self._h()).json()
        probs = d.get("problems", {})
        out = []
        # В ответе Яндекса тип проблемы — это КЛЮЧ словаря (URL_ALERT_4XX, SOFT_404…),
        # значение — {severity, state}. Сохраняем ключ как problem_type.
        if isinstance(probs, dict):
            for k, v in probs.items():
                if isinstance(v, dict) and v.get("state") == "PRESENT":
                    out.append({"problem_type": k, "severity": v.get("severity"), "state": v.get("state")})
        elif isinstance(probs, list):
            out = [p for p in probs if p.get("state") == "PRESENT"]
        return out

    def queries(self, days, limit):
        end, start = date.today(), date.today() - timedelta(days=days)
        url = self._u("search-queries/popular/")
        params = [("order_by", "TOTAL_SHOWS"), ("date_from", str(start)), ("date_to", str(end))]
        for ind in ("TOTAL_SHOWS", "TOTAL_CLICKS", "AVG_SHOW_POSITION"):
            params.append(("query_indicator", ind))
        d = requests.get(url, headers=self._h(), params=params).json()
        rows = []
        for q in d.get("queries", [])[:limit]:
            i = q.get("indicators", {})
            rows.append((q.get("query_text", ""), i.get("TOTAL_SHOWS", 0),
                         i.get("TOTAL_CLICKS", 0), i.get("AVG_SHOW_POSITION", 0)))
        return rows

    def recrawl(self, urls):
        out = []
        for u in urls:
            full = u if u.startswith("http") else SITE + (u if u.startswith("/") else "/" + u)
            r = requests.post(self._u("recrawl/queue/"), headers={**self._h(), "Content-Type": "application/json"},
                              data=json.dumps({"url": full}))
            j = r.json() if r.content else {}
            out.append((full, j.get("task_id") or j.get("error_code") or r.status_code,
                        j.get("quota_remainder")))
        return out

    def sitemap_submit(self):
        # Яндекс берёт sitemap из robots.txt автоматически; форс-добавление:
        r = requests.post(self._u("user-added-sitemaps/"), headers={**self._h(), "Content-Type": "application/json"},
                          data=json.dumps({"url": SITEMAP}))
        return r.status_code, (r.json() if r.content else {})


# ─────────────────────────── Google Search Console ───────────────────────────
class Google:
    API = "https://www.googleapis.com/webmasters/v3"
    SCOPE = "https://www.googleapis.com/auth/webmasters"

    def __init__(self, sa_path=HERE / "gsc-service-account.json"):
        self.ok = sa_path.exists()
        self._tok = None
        if self.ok:
            self.key = json.loads(sa_path.read_text())

    @staticmethod
    def _b64u(b):
        return base64.urlsafe_b64encode(b).rstrip(b"=")

    def token(self):
        if self._tok:
            return self._tok
        now = int(time.time())
        head = self._b64u(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
        claims = self._b64u(json.dumps({"iss": self.key["client_email"], "scope": self.SCOPE,
                                        "aud": self.key["token_uri"], "iat": now, "exp": now + 3600}).encode())
        si = head + b"." + claims
        pk = serialization.load_pem_private_key(self.key["private_key"].encode(), password=None)
        jwt = (si + b"." + self._b64u(pk.sign(si, padding.PKCS1v15(), hashes.SHA256()))).decode()
        r = requests.post(self.key["token_uri"], data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": jwt})
        r.raise_for_status()
        self._tok = r.json()["access_token"]
        return self._tok

    def _h(self):
        return {"Authorization": "Bearer " + self.token()}

    def site_url(self):
        sites = requests.get(f"{self.API}/sites", headers=self._h()).json().get("siteEntry", [])
        for s in sites:
            if "p1ai" in s["siteUrl"] or "xn--" in s["siteUrl"]:
                return s["siteUrl"]
        return sites[0]["siteUrl"] if sites else None

    def _sa(self, site, body):
        enc = urllib.parse.quote(site, safe="")
        return requests.post(f"{self.API}/sites/{enc}/searchAnalytics/query", headers={
            **self._h(), "Content-Type": "application/json"}, data=json.dumps(body)).json()

    def totals(self, site, days):
        end, start = date.today(), date.today() - timedelta(days=days)
        rows = self._sa(site, {"startDate": str(start), "endDate": str(end)}).get("rows", [])
        return rows[0] if rows else None

    def queries(self, site, days, limit):
        end, start = date.today(), date.today() - timedelta(days=days)
        d = self._sa(site, {"startDate": str(start), "endDate": str(end),
                            "dimensions": ["query"], "rowLimit": limit})
        return [(r["keys"][0], r["impressions"], r["clicks"], r["position"]) for r in d.get("rows", [])]

    def sitemap_submit(self, site):
        enc, encsm = urllib.parse.quote(site, safe=""), urllib.parse.quote(SITEMAP, safe="")
        r = requests.put(f"{self.API}/sites/{enc}/sitemaps/{encsm}", headers=self._h())
        return r.status_code


# ─────────────────────────── Яндекс.Метрика ───────────────────────────
class Metrika:
    """Яндекс.Метрика Reporting API — трафик, отказы, глубина, цели.

    Нужен ОТДЕЛЬНЫЙ OAuth-токен со scope ``metrika:read`` (вебмастерский НЕ
    подходит — у него другой доступ). Счётчик по умолчанию — боевой 109137033.
    """
    STAT = "https://api-metrika.yandex.net/stat/v1/data"
    MGMT = "https://api-metrika.yandex.net/management/v1"

    def __init__(self, env):
        self.token = env.get("YANDEX_METRIKA_TOKEN")
        self.counter = env.get("YANDEX_METRIKA_COUNTER", "109137033")
        self.ok = bool(self.token and self.counter)

    def _h(self):
        return {"Authorization": f"OAuth {self.token}"}

    def _data(self, metrics, days, filters=None):
        """Возвращает (totals|None, http_code, raw_json). totals — плоский
        список агрегатов в порядке metrics. Метрика по умолчанию исключает
        роботов, так что это уже «живые» люди."""
        end, start = date.today(), date.today() - timedelta(days=days)
        params = {"ids": self.counter, "metrics": metrics,
                  "date1": str(start), "date2": str(end), "accuracy": "full"}
        if filters:
            params["filters"] = filters
        r = requests.get(self.STAT, headers=self._h(), params=params)
        j = r.json() if r.content else {}
        return (j.get("totals") if r.status_code == 200 else None), r.status_code, j

    def traffic(self, days):
        return self._data(
            "ym:s:users,ym:s:visits,ym:s:bounceRate,ym:s:pageDepth,"
            "ym:s:avgVisitDurationSeconds", days)

    def engaged(self, days):
        # «изучали»: визиты глубже 1 страницы — отсекает случайных/одностраничных.
        return self._data("ym:s:users,ym:s:visits", days, filters="ym:s:pageViews>1")

    def goals(self):
        r = requests.get(f"{self.MGMT}/counter/{self.counter}/goals", headers=self._h())
        return r.json().get("goals", []) if r.status_code == 200 else []

    def goal_stats(self, days, goal_id):
        return self._data(f"ym:s:goal{goal_id}visits,ym:s:goal{goal_id}conversionRate", days)


# ─────────────────────────── вывод ───────────────────────────
def _qtable(rows, poscol=3):
    print("%8s %7s %6s  запрос" % ("показы", "клики", "поз."))
    for q, shows, clicks, pos in rows:
        print("%8.0f %7.0f %6.1f  %s" % (shows, clicks, pos, q))


def cmd_status(y, g, args):
    if y.ok:
        s = y.summary()
        print("🟢 ЯНДЕКС:  страниц в поиске=%s  исключено=%s  ИКС=%s  проблемы=%s" % (
            s.get("searchable_pages_count"), s.get("excluded_pages_count"),
            s.get("sqi"), s.get("site_problems")))
    if g.ok:
        site = g.site_url()
        t = g.totals(site, args.days)
        if t:
            print("🟡 GOOGLE:  клики=%s показы=%s CTR=%.1f%% поз=%.1f  (%sд, %s)" % (
                t["clicks"], t["impressions"], t["ctr"] * 100, t["position"], args.days, site))
        else:
            print("🟡 GOOGLE:  данных за период нет (%s)" % site)


def cmd_queries(y, g, args):
    eng = args.engine
    if eng in ("yandex", "both") and y.ok:
        print(f"\n=== 🟢 ЯНДЕКС — топ запросов ({args.days}д) ===")
        _qtable(y.queries(args.days, args.limit))
    if eng in ("google", "both") and g.ok:
        print(f"\n=== 🟡 GOOGLE — топ запросов ({args.days}д) ===")
        site = g.site_url()
        rows = [(q, impr, clk, pos) for (q, impr, clk, pos) in g.queries(site, args.days, args.limit)]
        _qtable(rows)


def cmd_compare(y, g, args):
    print("Запрос (бренд/тема) — позиция в Я / G:\n")
    yq = {q.lower(): pos for q, _, _, pos in y.queries(90, 200)} if y.ok else {}
    gq = {}
    if g.ok:
        site = g.site_url()
        gq = {q.lower(): pos for q, _, _, pos in g.queries(site, 90, 200)}
    keys = sorted(set(yq) | set(gq), key=lambda k: yq.get(k, 999))
    for k in keys[:30]:
        yp = "%.0f" % yq[k] if k in yq else "—"
        gp = "%.0f" % gq[k] if k in gq else "—"
        print(f"  Я:{yp:>4}  G:{gp:>4}  {k}")


def cmd_recrawl(y, g, args):
    if not y.ok:
        sys.exit("нет доступа к Яндексу")
    for full, res, rem in y.recrawl(args.urls):
        print(f"  {full} -> {res}  (квота: {rem})")


def cmd_diagnostics(y, g, args):
    if not y.ok:
        sys.exit("нет доступа к Яндексу")
    act = y.diagnostics()
    print("Активные проблемы Яндекса (state=PRESENT):")
    for p in act:
        print(f"  [{p.get('severity')}] {p.get('problem_type', '?')}")
    if not act:
        print("  (нет — всё чисто)")


def cmd_sitemap_submit(y, g, args):
    if g.ok:
        print("Google sitemap-submit:", g.sitemap_submit(g.site_url()), "(204=ок)")
    if y.ok:
        st, j = y.sitemap_submit()
        print("Яндекс sitemap-submit:", st, j.get("sitemap_id", j))


def cmd_traffic(m, args):
    if not m.ok:
        sys.exit("нет YANDEX_METRIKA_TOKEN в scripts/seo/.env "
                 "(получить токен со scope metrika:read — см. инструкцию)")
    d = args.days
    tot, code, j = m.traffic(d)
    if tot is None:
        err = j.get("message") if isinstance(j, dict) else j
        sys.exit(f"Метрика API {code}: {err}")
    users, visits, bounce, depth, dur = tot
    print(f"=== 📊 ЯНДЕКС.МЕТРИКА — трафик ({d}д, счётчик {m.counter}) ===")
    print(f"  Уникальных людей:      {users:,.0f}")
    print(f"  Визитов:               {visits:,.0f}")
    print(f"  Отказы:                {bounce:.1f}%")
    print(f"  Глубина (стр./визит):  {depth:.1f}")
    print(f"  Ср. время на сайте:    {dur/60:.1f} мин")
    eng, _, _ = m.engaged(d)
    if eng:
        eu, ev = eng
        share = 100 * ev / visits if visits else 0
        print(f"  «Изучали» (>1 стр.):   {eu:,.0f} чел / {ev:,.0f} виз. "
              f"({share:.0f}% визитов) — не случайные/боты")
    # Цели → конверсия в регистрацию
    goals = m.goals()
    reg = next((gg for gg in goals if any(s in gg.get("name", "").lower()
                for s in ("регистр", "signup", "sign up", "register", "account"))), None)
    print("  ── Цели ──")
    if reg:
        gt, _, _ = m.goal_stats(d, reg["id"])
        if gt:
            gv, cr = gt
            print(f"  «{reg['name']}»: {gv:,.0f} достижений, конверсия {cr:.2f}%")
    elif goals:
        print("  (цель регистрации не распознана; есть: "
              + ", ".join(g.get("name", "?") for g in goals[:6]) + ")")
    else:
        print("  (целей в Метрике нет — настрой цель «регистрация» в интерфейсе, "
              "тогда покажу точную конверсию визит→регистрация)")


def main():
    p = argparse.ArgumentParser(description="Фрейм SEO CLI (Яндекс + Google)")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("status", "queries", "compare", "diagnostics", "sitemap-submit", "traffic"):
        sp = sub.add_parser(name)
        sp.add_argument("--days", type=int, default=28)
        sp.add_argument("--limit", type=int, default=25)
        sp.add_argument("--engine", choices=["yandex", "google", "both"], default="both")
    rp = sub.add_parser("recrawl")
    rp.add_argument("urls", nargs="+")
    args = p.parse_args()

    env = load_env()
    y, g, m = Yandex(env), Google(), Metrika(env)
    if not y.ok:
        print("⚠️  Яндекс: нет .env/токена", file=sys.stderr)
    if not g.ok:
        print("⚠️  Google: нет gsc-service-account.json", file=sys.stderr)

    {"status": cmd_status, "queries": cmd_queries, "compare": cmd_compare,
     "recrawl": cmd_recrawl, "diagnostics": cmd_diagnostics,
     "sitemap-submit": cmd_sitemap_submit,
     "traffic": lambda _y, _g, a: cmd_traffic(m, a)}[args.cmd](y, g, args)


if __name__ == "__main__":
    main()
