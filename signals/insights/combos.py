"""Связки: сюжет = тема × момент, в котором сходятся РАЗНЫЕ источники данных.

Прежний новостной завод знал одну связку: у новости есть тикер → аномалия ОИ по тому же
тикеру. Канал так почти не пишет (1 пост из 91 за лето): он пишет от макро и сводит два-три
источника в одну мысль — «отчёт ЦБ: физлица купили валюту - и на срочном рынке их лонг растёт
- а сезонность говорит, что рано». Здесь звено «тикер» заменено типом события (RULES), и
для каждого типа THEMES говорит, в каких данных искать отклик.

Ноги сюжета:
  • новость нужного типа (хэштеги MarketTwits + текст);
  • находки детекторов signals/insights/detect.py: позиции физлиц, фонды, цены, сезонность,
    широта, Баффетт;
  • отчёт ЦБ о потоках (cbr_flows) — в день его выхода (ловим по новости).
Сюжет есть, если в нём ≥2 разных семейства (новость — отдельное) и главная нога сильная (≥6),
новая (такой же находки не было 10 торговых дней) и свежая (последний торговый день или
накануне); новость — только при всплеске темы ×2 или 🔥.

Два режима времени (signals/combo_scan.py):
  • утром — сюжеты вечера последнего торгового дня: данные за день и новости до вечера;
  • днём — новостной сюжет в момент новости: данные по вчерашний торговый день. Решение ЦБ
    выходит днём, и вечерний сюжет его бы не увидел.

Карточка сюжета (brief) — то, что видит писатель: повод, главная нога, подтверждения, под
каждой ногой «как читать», история главной ноги (карточка движка находок), контекст.

Замеры — research/content_pipeline_v2/news_branch/HANDOFF.md: на истории 5 сюжетов в день;
слепое сравнение 16 пар с постами FRAME и Thor — лучше для канала 11 из 16 (v2–v3), числа
16 из 16 из карточки; интереснее автор в 10–12 из 16 (мнение и прогноз, которые мы запретили).
"""
import re
from collections import defaultdict

import numpy as np
import pandas as pd

from signals.insights import cards
from signals.insights import data as dbdata

# ── типы новостей ────────────────────────────────────────────────────────────────
# тип → (хэштеги-признаки, регэксп по тексту, регэксп-исключение). Хэштег даёт тему, текст —
# что это событие, а не упоминание вскользь.
RULES = {
    # чужие центробанки и календарь «ВПЕРЕДИ» — не наша ставка
    "ставка": ({"#дкп", "#цб"}, r"ключев\w* ставк|ставк\w* (ЦБ|Банка России)|Набиуллин|Заботкин|заседани\w* "
               r"(ЦБ|Банка России|совета директоров)|\bДКП\b|снижени\w* ставк|\d+ ?б\.? ?п\.",
               r"ВПЕРЕДИ|ФРС|\bЕЦБ\b|Банк\w* Англии|Британи|Казахстан|Турци|🇺🇸|🇬🇧|🇪🇺|🇯🇵|🇰🇿|🇹🇷"),
    "минфин_валюта": ({"#бюджетноеправило", "#интервенции", "#fx"}, r"бюджетн\w* правил|покупк\w* валют\w*.{0,40}"
                      r"(Минфин|ФНБ)|Минфин\w*.{0,60}(валют|золот)|операци\w* на валютном рынке|цен\w* отсечени", None),
    "офз": ({"#офз", "#аукционы", "#облигации", "#бонды"}, r"\bОФЗ\b|аукцион\w* Минфин|RGBI|доходност\w* "
            r"(ОФЗ|10-лет|десятилет|дальн)", None),
    "бюджет_налоги": ({"#налоги", "#бюджет"}, r"\bНДС\b|налог\w* на (сверх|прибыль)|windfall|дефицит\w* бюджет|"
                      r"бюджет\w* на 20\d\d|госрасход", None),
    "рубль": ({"#rub", "#fx"}, r"курс\w* (доллар|юан|рубл|евро)|рубл\w* (укреп|ослаб|падает|растёт|растет)|"
              r"(доллар|юан)\w* (по|до|выше|ниже) \d|за доллар|USDRUB|CNYRUB|девальвац", r"#сша|DXY|индекс доллара"),
    "геополитика": ({"#геополитика", "#украина", "#мир", "#event"}, r"переговор|перемири|мирн\w* (план|соглашени|"
                    r"урегулир|договор)|урегулир|договорит\w* с Украин|прекращени\w* (огня|войны|конфликта)|Ушаков|"
                    r"Зеленск|Уиткофф|Дмитриев|саммит|встреч\w* (Путина|Трампа)|звон\w* (Путина|Трампа)", None),
    "санкции": ({"#санкции"}, r"санкци|потолок цен|теневой флот|вторичн\w* пошлин", None),
    "нефть_газ": ({"#нефть", "#газ", "#спг", "#опек", "#ормуз"}, r"нефт|Brent|Urals|ОПЕК|\bСПГ\b|газопровод|Ормуз",
                  None),
    "отчёт_цб_потоки": ({"#cot", "#обзор", "#физики"}, r"(ЦБ|Банк\w* России|регулятор|отч[её]т).{0,300}(физ\w* лиц|"
                        r"физик|розничн\w* инвестор|НФО|СЗКО|нетто-(покупат|продав))|(физ\w* лиц|розничн\w* инвестор|"
                        r"физик).{0,300}(ЦБ|Банк\w* России|отч[её]т)|физик\w* .{0,40}(нарастил|купил|продал)\w* "
                        r".{0,30}(акци|валют|облигац)", None),
    "мировые_активы": ({"#золото", "#серебро", "#usd", "#commodities", "#сша"}, r"золот|серебр|\bDXY\b|индекс доллара|"
                       r"доллар\w* США|S&P ?500|SP500|акци\w* США|commodit", None),
    "рынок_целиком": (set(), r"(Индекс Мосбиржи|IMOEX|индекс МосБиржи)\w*.{0,80}(рекорд|максимум|минимум|"
                      r"[+−-]\s?[2-9][,.]\d\s?%|обвал|взлет|взлёт|ралли|закрыл\w* в (минус|плюс))", None),
}

OIL = {"LKOH", "ROSN", "GAZP", "NVTK", "TATN", "TATNP", "SNGS", "SNGSP", "SIBN", "BR", "NG"}
# тип новости → семейство находки → инструменты (коды detect.py; «*» — любой)
THEMES = {
    "ставка": {"позиции": {"MIX", "RI", "RGBI", "Si"}, "фонды": {"funds:bonds", "funds:money_market", "funds:all"},
               "цена": {"MIX", "RI"}, "цб_потоки": {"ofz"}},
    "минфин_валюта": {"позиции": {"Si", "CNY", "Eu", "FX_ALL"}, "фонды": {"funds:yuan"}, "цена": {"Si"},
                      "сезонность": {"Si"}},
    "офз": {"позиции": {"RGBI"}, "фонды": {"funds:bonds", "funds:money_market"}, "цб_потоки": {"ofz"}},
    "бюджет_налоги": {"позиции": {"MIX", "RI", "RGBI", "STOCKS_ALL"}, "цена": {"MIX", "RI", "STOCKS_ALL"}},
    "рубль": {"позиции": {"Si", "CNY", "Eu", "FX_ALL"}, "фонды": {"funds:yuan"}, "цена": {"Si"},
              "сезонность": {"Si"}, "цб_потоки": {"fx"}},
    "геополитика": {"позиции": {"MIX", "RI", "STOCKS_ALL", "RGBI", "Si"}, "цена": {"MIX", "RI", "STOCKS_ALL"},
                    "широта": {"STOCKS_ALL"}},
    "санкции": {"позиции": OIL | {"STOCKS_ALL"}, "цена": OIL},
    "нефть_газ": {"позиции": OIL, "цена": OIL},
    "отчёт_цб_потоки": {"цб_потоки": {"*"}, "позиции": {"Si", "CNY", "MIX", "FX_ALL", "STOCKS_ALL"},
                        "фонды": {"*"}, "сезонность": {"Si", "MIX"}},
    # без фьючерса на доллар: на прогоне 11.09 любой сдвиг по доллару становился «мировыми активами»
    # под новость «Как приручить медвежий рынок» — валюта ловится своими темами
    "мировые_активы": {"позиции": {"GOLD", "SV"}, "фонды": {"funds:gold"}, "цена": {"GOLD"}},
    "рынок_целиком": {"позиции": {"MIX", "RI", "STOCKS_ALL"}, "широта": {"STOCKS_ALL"},
                      "цена": {"MIX", "RI", "STOCKS_ALL"}, "баффетт": {"STOCKS_ALL"}, "сезонность": {"MIX"}},
}
# связки без новости — сюжет чисто из данных
DATA_THEMES = {
    "валюта": {"позиции": {"Si", "CNY", "Eu", "FX_ALL"}, "фонды": {"funds:yuan"}, "цена": {"Si"},
               "сезонность": {"Si"}, "цб_потоки": {"fx"}},
    "рынок_акций": {"позиции": {"MIX", "RI", "STOCKS_ALL"}, "фонды": {"funds:stocks"},
                    "цена": {"MIX", "RI", "STOCKS_ALL"}, "широта": {"STOCKS_ALL"}, "баффетт": {"STOCKS_ALL"},
                    "сезонность": {"MIX"}, "цб_потоки": {"stocks"}},
    "облигации": {"позиции": {"RGBI"}, "фонды": {"funds:bonds", "funds:money_market"}, "цб_потоки": {"ofz"}},
    "золото": {"позиции": {"GOLD", "SV"}, "фонды": {"funds:gold"}, "цена": {"GOLD"}},
    "нефтегаз": {"позиции": OIL, "цена": OIL},
}
BUCKET = {"ставка": {"облигации", "рынок_акций"}, "минфин_валюта": {"валюта"}, "рубль": {"валюта"},
          "офз": {"облигации"}, "бюджет_налоги": {"рынок_акций"}, "геополитика": {"рынок_акций"},
          "санкции": {"нефтегаз"}, "нефть_газ": {"нефтегаз"}, "мировые_активы": {"золото"},
          "рынок_целиком": {"рынок_акций"}, "отчёт_цб_потоки": {"валюта", "рынок_акций", "облигации"},
          **{k: {k} for k in DATA_THEMES}}
THEME_RU = {"ставка": "ставка ЦБ", "минфин_валюта": "Минфин и валюта", "рубль": "рубль", "офз": "ОФЗ",
            "бюджет_налоги": "бюджет и налоги", "геополитика": "геополитика", "санкции": "санкции",
            "нефть_газ": "нефть и газ", "отчёт_цб_потоки": "отчёт ЦБ о потоках", "мировые_активы": "мировые активы",
            "рынок_целиком": "рынок целиком", "валюта": "валюта", "рынок_акций": "рынок акций",
            "облигации": "облигации", "золото": "золото", "нефтегаз": "нефтегаз"}
HASHTAG = {"позиции": "#открытыепозиции", "фонды": "#деньгивфондах", "сезонность": "#сезонность",
           "цб_потоки": "#Потоккапитала", "широта": "#силарынка", "баффетт": "#ИндикаторБаффетта"}
LEX = {"валюта": r"доллар|юан|рубл|валют|USDRUB|CNYRUB",
       "рынок_акций": r"индекс\w* (Мосбиржи|МосБиржи|РТС)|IMOEX|\bRTS\b|рын\w* акций|широт|Баффет|сила рынка",
       "облигации": r"\bОФЗ\b|RGBI|облигац|доходност|ключев\w* ставк|ставк\w* ЦБ",
       "золото": r"золот|серебр|GLD",
       "нефтегаз": r"нефт|Brent|\bгаз|Лукойл|Роснефт|Новат[эе]к|Газпром|LKOH|ROSN|NVTK|GAZP|Татнефт|Сургут"}

STRONG = 6.0        # сила главной ноги: верхние ~10% находок по темам
SUPPORT = 2.0       # сила ноги-подтверждения из другого семейства
NOVEL_DAYS = 10     # такой же находки (семейство+инструмент+тип+сторона) не было N торговых дней
REACT_DAYS = 2      # главная нога — за последний торговый день или накануне
WINDOW = 5          # подтверждения — за последние N торговых дней
NEWS_HOURS = 30     # новости за последние N часов до момента сюжета
SPIKE = 2.0         # новостей темы ≥ SPIKE × обычного дня темы (медиана за 20 дней)


def tags(s) -> set:
    if isinstance(s, (list, tuple)):
        return {str(t).lower() for t in s}
    if not isinstance(s, str):
        return set()
    return {t.strip().strip('"').lower() for t in s.strip("{}").split(",") if t.strip()}


def classify(text: str, hashtags) -> list:
    """Типы события новости: текст говорит, что это событие, хэштег темы — что оно главное.
    У Смартлаба хэштегов нет — там достаточно текста; короткой новости — тоже."""
    t = re.sub(r"[\xa0 ]", " ", text or "")
    tg = hashtags if isinstance(hashtags, set) else tags(hashtags)
    out = []
    # отчёт ЦБ о потоках MarketTwits подаёт под #обзор/#cot, часто без слова «ЦБ»
    if tg & {"#cot", "#обзор"} and re.search(r"розничн\w* инвестор|населени|физ\w* лиц|физик", t, re.I):
        out.append("отчёт_цб_потоки")
    for kind, (htags, rx, stop) in RULES.items():
        if stop and re.search(stop, t + " " + " ".join(tg), re.I):
            continue
        # чужой флаг без российского — чужая ставка («ЦБ Индонезии повысил ставку»)
        if kind == "ставка" and re.search(r"[\U0001F1E6-\U0001F1FF]{2}", t) and "🇷🇺" not in t:
            continue
        if kind not in out and re.search(rx, t, re.I) and (tg & htags or not htags or not tg or len(t) < 400):
            out.append(kind)
    return out


def buckets(text: str) -> set:
    return {b for b, rx in LEX.items() if re.search(rx, text or "", re.I)}


# ── ноги сюжета ─────────────────────────────────────────────────────────────────
def news_frame() -> pd.DataFrame:
    na = dbdata.read("news_recent")
    na["posted_at"] = pd.to_datetime(na.posted_at, utc=True)
    na["kinds"] = [classify(t, h) for t, h in zip(na.text, na.hashtags)]
    return na


def cbr_releases(na: pd.DataFrame) -> list:
    """Отчёт ЦБ о потоках как нога данных: дата выхода — первая новость «отчёт_цб_потоки» после
    конца месяца; сила — насколько покупки физлиц выделяются на своих 36 месяцах."""
    cf = dbdata.read("cbr_flows", parse_dates=["period_end_date"])
    cf = cf[(cf.period_kind == "month") & (cf.category == "Физические лица")]
    cf = cf.sort_values("updated_at").drop_duplicates(["instrument_type", "period_end_date"], keep="last")
    rel = na[na.kinds.map(lambda k: "отчёт_цб_потоки" in k)].posted_at.sort_values()
    rel = rel.dt.tz_convert("Europe/Moscow").dt.tz_localize(None)
    out = []
    for it, g in cf.groupby("instrument_type"):
        g = g.sort_values("period_end_date").reset_index(drop=True)
        for i, r in g.iterrows():
            if i < 12:
                continue
            after = rel[rel > r.period_end_date + pd.Timedelta(days=5)]
            if after.empty or after.iloc[0] > r.period_end_date + pd.Timedelta(days=45):
                continue
            hist = g.value.iloc[max(0, i - 36):i]
            pct = float((hist.abs() < abs(r.value)).mean())
            beaten = hist[hist >= r.value] if r.value > 0 else hist[hist <= r.value]
            what = {"fx": "валюты", "stocks": "акций", "ofz": "ОФЗ"}.get(it, it)
            tail = f" — больше всего за {len(hist)} мес." if beaten.empty else ""
            out.append({"date": str(after.iloc[0].date()), "family": "цб_потоки", "instrument": it, "type": "отчёт",
                        "score": round(2 + 6 * pct ** 2, 2), "run_days": 0, "facts": {"value": float(r.value)},
                        "title": f"Отчёт ЦБ: физлица за {r.period_end_date:%m.%Y} "
                                 f"{'купили' if r.value > 0 else 'продали'} {what} на {abs(r.value):.0f} млрд ₽{tail}"})
    return out


class Engine:
    """Находки детекторов за 30 дней + отчёты ЦБ + свежие новости — один раз на запуск."""

    def __init__(self, items: list, na: pd.DataFrame | None = None):
        self.na = news_frame() if na is None else na
        det = pd.DataFrame(items + cbr_releases(self.na))
        det["date"] = pd.to_datetime(det.date)
        det["instrument"] = det.instrument.fillna("")
        det["leg"] = [(f or {}).get("leg", "") if isinstance(f, dict) else "" for f in det.facts]
        self.days = np.array(sorted(det[det.family == "позиции"].date.unique()))
        pos = {d: i for i, d in enumerate(self.days)}
        det = det.sort_values("date")
        det["di"] = det.date.map(lambda d: pos.get(d, int(np.searchsorted(self.days, d))))
        key = det.family + "|" + det.instrument + "|" + det.type + "|" + det.leg
        prev = det.groupby(key).di.shift(1)
        # новая — такой же находки не было NOVEL_DAYS торговых дней: рекорд, который
        # обновляется каждый день, — не новость на второй день
        det["novel"] = prev.isna() | (det.di - prev > NOVEL_DAYS)
        self.by_di = {i: g for i, g in det.groupby("di")}
        ex = self.na.explode("kinds").dropna(subset=["kinds"])
        ex["day"] = ex.posted_at.dt.tz_convert("Europe/Moscow").dt.tz_localize(None).dt.normalize()
        self.usual, self.news = {}, {}
        for k, g in ex.groupby("kinds"):
            cnt = g.groupby("day").size()
            cnt = cnt.reindex(pd.date_range(cnt.index.min(), cnt.index.max()), fill_value=0)
            self.usual[k] = cnt.rolling(20, min_periods=5).median().shift(1)
            self.news[k] = g[["posted_at", "text", "channel", "views"]].rename(columns={"posted_at": "t"}).sort_values("t")

    @staticmethod
    def _match(fam, inst, tmap) -> bool:
        s = tmap.get(fam)
        return s is not None and ("*" in s or inst in s)

    def assemble(self, theme, tmap, is_news, i_last, news_until, label_day) -> dict | None:
        win = pd.concat([self.by_di[j] for j in range(i_last - WINDOW + 1, i_last + 1) if j in self.by_di])
        legs = win[[self._match(f, s, tmap) for f, s in zip(win.family, win.instrument)]]
        if legs.empty:
            return None
        lead = legs[(legs.score >= STRONG) & legs.novel & (legs.di >= i_last - REACT_DAYS + 1)]
        if lead.empty:
            return None
        news = pd.DataFrame()
        if is_news:
            nk = self.news.get(theme)
            if nk is None:
                return None
            news = nk[(nk.t > news_until - pd.Timedelta(hours=NEWS_HOURS)) & (nk.t <= news_until)]
            base = self.usual[theme].get(news_until.tz_convert("Europe/Moscow").tz_localize(None).normalize(), np.nan)
            hot = news.text.str.contains("🔥", regex=False).any()
            if news.empty or not (hot or len(news) >= max(2, SPIKE * (base if base == base else 1))):
                return None
        top1 = lead.sort_values("score", ascending=False).iloc[0]
        sup = legs[(legs.score >= SUPPORT) & (legs.family != top1.family)]
        sup = sup.sort_values("score", ascending=False).drop_duplicates("family")
        fams = {top1.family} | set(sup.family) | ({"новость"} if is_news else set())
        if len(fams) < 2:
            return None
        top = pd.concat([top1.to_frame().T, sup.head(3)])
        score = float(top1.score) + 0.5 * float(sup.score.head(3).sum()) + (2.0 if is_news else 0)
        # главные новости окна: с 🔥 и самые просматриваемые, в порядке времени
        best = (news.assign(hot=news.text.str.contains("🔥", regex=False))
                .sort_values(["hot", "views"], ascending=False).head(3).sort_values("t")) if len(news) else news
        return {"day": str(pd.Timestamp(label_day).date()), "data_day": str(pd.Timestamp(self.days[i_last]).date()),
                "theme": theme, "is_news": is_news, "buckets": sorted(BUCKET[theme]), "score": round(score, 2),
                "families": sorted(fams),
                "legs": [{"date": str(pd.Timestamp(r.date).date()), "family": r.family, "instrument": r.instrument,
                          "score": float(r.score), "title": r.title,
                          "facts": r.facts if isinstance(r.facts, dict) else {}} for r in top.itertuples()],
                "news": [{"t": str(r.t), "text": str(r.text)[:300]} for r in best.itertuples()]}

    def evening(self) -> list:
        """Сюжеты вечера последнего торгового дня — все темы, по убыванию силы."""
        i = len(self.days) - 1
        D = pd.Timestamp(self.days[i])
        evening = D.tz_localize("Europe/Moscow").tz_convert("UTC") + pd.Timedelta(hours=20)
        out = [self.assemble(k, v, True, i, evening, D) for k, v in THEMES.items()]
        out += [self.assemble(k, v, False, i, evening, D) for k, v in DATA_THEMES.items()]
        return sorted((s for s in out if s), key=lambda s: -s["score"])

    def at(self, when) -> list:
        """Новостные сюжеты в момент `when`: данные по последний торговый день ДО дня новости."""
        when = pd.Timestamp(when)
        when = when.tz_localize("UTC") if when.tzinfo is None else when
        day = when.tz_convert("Europe/Moscow").tz_localize(None).normalize()
        i = int(np.searchsorted(self.days, np.datetime64(day))) - 1
        if i < WINDOW:
            return []
        out = [self.assemble(k, v, True, i, when, day) for k, v in THEMES.items()]
        return sorted((s for s in out if s), key=lambda s: -s["score"])


# ── карточка сюжета для писателя ─────────────────────────────────────────────────
# Название ноги однозначно говорит, ЧТО считали: «покупки физлиц (длинная сторона)» писатель
# превращал в «чистый лонг», а «USD/RUB» путал с курсом доллара.
SIDE = [("покупки физлиц (длинная сторона)", "покупки физлиц - все лонги, без вычета шортов"),
        ("шорт физлиц (короткая сторона)", "шорт физлиц - все шорты, без вычета лонгов"),
        ("чистый лонг физлиц", "чистый лонг физлиц - лонги минус шорты"),
        ("чистый шорт физлиц", "чистый шорт физлиц - шорты минус лонги"),
        ("чистая позиция физлиц", "чистая позиция физлиц - лонги минус шорты")]
INSTR = [("USD/RUB (вечн)", "вечный фьючерс на доллар"), ("USD/RUB", "фьючерс на доллар"),
         ("CNY/RUB (вечн)", "вечный фьючерс на юань"), ("CNY/RUB", "фьючерс на юань"),
         ("Индекс МосБиржи (вечн)", "вечный фьючерс на индекс Мосбиржи"),
         ("Индекс МосБиржи (мини)", "мини-фьючерс на индекс Мосбиржи"), ("Индекс МосБиржи", "фьючерс на индекс Мосбиржи"),
         ("Индекс РТС", "фьючерс на индекс РТС"), ("Индекс RGBI", "фьючерс на индекс гособлигаций"),
         ("Нефть Brent", "фьючерс на нефть Brent"), ("Золото", "фьючерс на золото"), ("Серебро", "фьючерс на серебро")]

# «как читать» — что измеряет цифра и с чем её не путать. Промптом ошибки смысла не лечились:
# «число физлиц в шорте +73%» → «нарастили шорт на 73%», «7% акций выше 100-дневной» → «растёт
# 7% акций», сезонный путь → «рынок ждёт рост» — каждый раз на другом индикаторе.
GLOSS = [
    (r"число физлиц в (шорте|лонге)", lambda m: f"это ЧИСЛО ЛЮДЕЙ с открытым {'шортом' if m.group(1) == 'шорте' else 'лонгом'}, "
                                                 "а не объём их позиций в контрактах"),
    (r"все лонги, без вычета шортов", lambda m: "все лонги физлиц в контрактах; шорты не вычтены - это не чистая позиция"),
    (r"все шорты, без вычета лонгов", lambda m: "все шорты физлиц в контрактах; лонги не вычтены - это не чистая позиция"),
    (r"(чистый (лонг|шорт)|чистая позиция) физлиц", lambda m: "чистая позиция = лонги минус шорты, в контрактах"),
    (r"в ([\d.,]+) раза сильнее обычного", lambda m: f"«в {m.group(1)} раза сильнее обычного» - изменение за день в "
                                                      f"{m.group(1)} раза больше обычного дневного изменения; это НЕ рост "
                                                      f"позиции в {m.group(1)} раза"),
    (r"(максимум|минимум|рекорд) с (\d\d\.\d\d\.\d{4})", lambda m: f"{m.group(1)} только с {m.group(2)}, не за всю историю"),
    (r"за всё время наших данных \(с (\d{4})\)", lambda m: f"рекорд за всю историю наших данных - с {m.group(1)} года"),
    (r"больше всего за (\d+) мес", lambda m: f"больше всего за {m.group(1)} месяцев, не за всю историю"),
    (r"(\d+)% акций индекса выше (\d+)-дневной средней|доля акций выше (\d+)-дневной средней (\d+)%",
     lambda m: "доля акций индекса, чья цена выше своей средней за этот срок; это НЕ доля растущих акций и не их число"),
    (r"в ближайшие ~(\d+) торговых дней", lambda m: f"календарь прошлых лет за такое же окно в {m.group(1)} торговых "
                                                     "дней; это история, а НЕ ожидание рынка и не прогноз"),
    (r"окне сезонного (пика|дна)", lambda m: "средний сезонный путь 2016-2025 без годового тренда - история календаря, "
                                              "а НЕ ожидание рынка и не прогноз"),
    (r"за (\d+) дней", lambda m: f"сумма за последние {m.group(1)} торговых дней, не за месяц"),
    (r"синхронно (наращивают|сокращают) шорт", lambda m: "шорт физлиц за неделю изменился заметно сильнее обычного "
                                                         "сразу по фьючерсам на доллар, евро и юань; это позиции, не курс"),
    (r"^Доллар\b", lambda m: "курс доллара к рублю, а не позиции во фьючерсах"),
    (r"^Золото\b", lambda m: "цена золота на бирже в рублях, а не позиции во фьючерсах"),
    (r"на минимумах за год и больше", lambda m: "число акций, чья цена на минимуме за год или дольше"),
    (r"(падает|растёт) (\d+)-ю неделю подряд", lambda m: f"{m.group(2)} недель подряд неделя закрывалась "
                                                          f"{'ниже' if m.group(1) == 'падает' else 'выше'} предыдущей"),
    (r"с начала месяца", lambda m: "сумма с начала месяца, месяц ещё не закончен"),
    (r"(\d+)-й месяц подряд", lambda m: f"{m.group(1)} полных месяцев подряд в одну сторону"),
    (r"месяц идёт в (отток|приток) после", lambda m: "текущий, ещё не законченный месяц; прошлые месяцы были в обратную сторону"),
    (r"Отчёт ЦБ: физлица за (\d\d\.\d{4})", lambda m: f"данные ЦБ за месяц {m.group(1)}, вышли только что - это прошлый "
                                                       f"месяц, не текущий"),
    (r"неделя (роста|снижения) от (максимума|минимума) (\S+) \(([+-]?\d+%)\)",
     lambda m: f"{m.group(4)} - изменение от {m.group(2)} {m.group(3)} до сейчас, а не за неделю"),
    (r"покупают на падении|продают на росте", lambda m: "цена и позиция - обе за последнюю неделю"),
    (r"Индикатор Баффетта", lambda m: "капитализация российского рынка акций к ВВП страны"),
]


def legible(title: str) -> str:
    if "физлиц" in title:
        for a, b in INSTR:
            if title.startswith(a):
                title = b + title[len(a):]
                break
        for a, b in SIDE:
            title = title.replace(a, b)
    return title


def gloss(title: str) -> str:
    out = []
    for rx, f in GLOSS:
        m = re.search(rx, title)
        if m:
            out.append(f(m))
    return "; ".join(dict.fromkeys(out))


def leg_spec(leg: dict) -> dict | None:
    """Нога → спецификация карточки движка находок, если у ноги есть длинный ряд."""
    f = leg.get("facts") or {}
    if leg["family"] == "позиции" and f.get("sec"):
        side = re.sub(r"_low$", "", f.get("leg") or "net")
        return {"kind": "positions", "sec": f["sec"], "leg": side if side in cards.LEG else "net"}
    if leg["family"] == "фонды":
        cat = f.get("cat") or leg["instrument"].replace("funds:", "")
        return {"kind": "funds", "cat": cat} if cat in cards.FUND else None
    if leg["family"] == "сезонность" and leg["instrument"] in ("Si", "MIX"):
        return {"kind": "seasonality", "code": leg["instrument"]}
    return None


def history_for(story: dict, t):
    """Карточка истории к первой ноге, у которой она строится."""
    for leg in story["legs"]:
        spec = leg_spec(leg)
        if not spec:
            continue
        try:
            return leg, cards.build_card(spec, t)
        except Exception as e:  # noqa: BLE001  ряд короткий — берём следующую ногу
            print(f"[combos] нет истории для «{leg['title'][:50]}»: {e}")
    return None, None


def news_line(n: dict, t) -> str:
    s = re.sub(r"\s+", " ", re.sub(r"[#@]\S+|Читать далее.*|👉.*|mt в max|мы в max|MT в MAX", " ", n["text"])).strip()
    s = re.split(r"(?<=[.!?])\s", s)[0][:220]
    return f"{cards.d_ru(pd.Timestamp(n['t']).tz_convert('Europe/Moscow').tz_localize(None), t)}: {s}"


def channel_posts(t, days=14) -> list:
    df = dbdata.read("channel_posts")
    if not len(df):
        return []
    df["d"] = pd.to_datetime(df.posted_at, utc=True).dt.tz_convert("Europe/Moscow").dt.tz_localize(None)
    df = df[(df.d >= pd.Timestamp(t) - pd.Timedelta(days=days)) & (df.d <= pd.Timestamp(t) + pd.Timedelta(days=1))]
    return [(r.d, r.text or "") for r in df.sort_values("d").itertuples()]


def brief(story: dict) -> tuple:
    """Карточка сюжета → (текст для писателя, карточка истории или None — для графика)."""
    t = pd.Timestamp(story["data_day"])
    lead, support = story["legs"][0], story["legs"][1:]
    out = [f"СЮЖЕТ: {THEME_RU.get(story['theme'], story['theme'])}", f"ДАТА ДАННЫХ: {cards.d_ru(t)}", ""]
    if story["news"]:
        out += ["ПОВОД - что писали новостные каналы:"] + [f"- {news_line(n, t)}" for n in story["news"][-2:]] + [""]

    def leg_lines(label, title):
        g = gloss(legible(title))
        return [f"- {label}: {legible(title)}"] + ([f"  как читать: {g}"] if g else [])

    out += ["ГЛАВНОЕ - строй пост вокруг этого:"] + leg_lines("находка", lead["title"])
    for s in support:
        out += leg_lines(f"подтверждение из других данных ({s['family']})", s["title"])
    out.append("")
    leg, card = history_for(story, t)
    limits = []
    if card:
        out += [f"ИСТОРИЯ - к ноге «{legible(leg['title'])[:110]}», посчитано по нашим рядам:",
                "- эта история - только про эту ногу; для остальных ног прошлых эпизодов не считали"]
        lines = cards.focus_lines(card)[1:]
        if card.get("facts"):       # масштаб «сейчас» рядом с прошлыми пиками
            lines = [f"сейчас: {card['facts'][0]}"] + lines
        lines += [f"что было после прошлых эпизодов: {x}" for x in (card.get("after") or [])[:3]
                  if not any(x in y for y in lines)]
        lines += [f"ряд: {x}" for x in (card.get("history") or [])[:2]]
        out += [f"- {x}" for x in lines] + [""]
        limits = [x for x in card.get("limits") or [] if x and x != cards.NO_FORECAST]
    out.append("КОНТЕКСТ - что было в мире до поста; опора для позиции канала, но не причина движения:")
    out += ["срез рынка по данным сервиса:"] + [f"- {x}" for x in cards.market_lines(t)]
    own = [(d, x.strip().splitlines()[0][:90]) for d, x in channel_posts(t) if x.strip()
           and buckets(x) & set(story["buckets"])][-2:]
    if own:
        def ago(d):
            n = (t.normalize() - pd.Timestamp(d).normalize()).days
            return "в тот же день" if n <= 0 else f"{n} {cards.plural(n, ('день', 'дня', 'дней'))} назад"
        out += ["что канал уже писал за две недели:"] + [f"- {cards.d_ru(d, t)}, {ago(d)}: «{x}»" for d, x in own]
    out += ["", "ОГРАНИЧЕНИЯ - чего не утверждать:", f"- {cards.NO_FORECAST}",
            "- новость и данные сошлись по времени - это одновременность, а не причина",
            "- ноги сюжета - разные ряды за разные окна: не складывай их в одну цифру"]
    out += [f"- {x}" for x in limits] + [""]
    fam = lead["family"] if lead["family"] in HASHTAG else next(
        (s["family"] for s in support if s["family"] in HASHTAG), "позиции")
    out.append(f"ХЭШТЕГ РУБРИКИ: {HASHTAG[fam]}")
    return "\n".join(out), card
