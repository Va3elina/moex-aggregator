"""Единая разметка второго мозга (27.09.2026): один словарь на все источники, тип и отрасль у каждого события,
текст узла без шума, пополнение каждым синком. Вадим: «со всех источников — один и тот же формат, машиночитаемый;
поиск и по хэштегам, и по горячим словам одновременно; пополняется и фильтруется каждый раз».
"""
import importlib.util
import inspect
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "Brain"))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
import vocab  # noqa: E402
from api import brain_core as core  # noqa: E402

ИМЕНА = [t for t, _, _ in vocab.ТИПЫ]
HERE = Path(__file__).resolve().parent


def _sync():
    spec = importlib.util.spec_from_file_location("brain_sync_markup", ROOT / "Brain" / "brain_sync.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_one_type_list_for_rules_agent_and_sources():
    assert tuple(ИМЕНА) + ("прочее",) == core._ТИПЫ_НОВОСТЕЙ
    assert len(set(ИМЕНА)) == len(ИМЕНА), "тип с одним именем — один тип"
    for словарь in (vocab.ТИП_ВИДА, vocab.ТИП_РАСКРЫТИЯ, vocab.ТИП_ШАГА_А):
        assert set(словарь.values()) <= set(ИМЕНА)
    assert set(vocab.ПЕРЕБИВАЮТ_СЫРЬЁ) <= set(ИМЕНА) and set(vocab.СЫРЬЁ_ТИПЫ) <= set(ИМЕНА)
    assert core._ВИДЫ_ЯРЛЫКОВ == vocab.ВИДЫ_АГЕНТУ


def test_topic_with_same_name_is_merged_not_dropped():
    облигации = dict((t, (h, r)) for t, h, r in vocab.ТИПЫ)["облигации"]
    assert "#офз" in облигации[0] and "офз" in облигации[1], "тема «облигации» (ОФЗ) не должна выпадать"


def test_event_words_beat_commodity_hashtag_but_operations_do_not():
    assert "удары по инфраструктуре" in vocab.ПЕРЕБИВАЮТ_СЫРЬЁ   # «#бензин БПЛА атаковали НПЗ»
    assert "операционные" not in vocab.ПЕРЕБИВАЮТ_СЫРЬЁ           # «#нефть ОПЕК+ сократит добычу»


def test_tag_ids_are_valid_node_ids():
    for тип in ИМЕНА:
        ключ = vocab.ключ_тега(тип)
        assert " " not in ключ and "," not in ключ
        assert core._ID.match(f"tag:тип/{ключ}"), тип
    assert vocab.sql_ключ_тега("x") == "replace(replace(x, ',', ''), ' ', '_')"


def test_markup_runs_every_sync_and_new_version_rebuilds_once():
    s = _sync()
    main = inspect.getsource(s.main)
    assert "разметка(conn, args.full or новая_версия)" in main
    assert "чистка_однократно(conn)" in main and "сброс=новая_версия" in main
    src = inspect.getsource(s.разметка)
    assert "_разница(conn, \"тип\"" in src and "_разница(conn, \"отрасль\"" in src, "рёбра — разницей, не пересборкой"
    assert "INTERVAL '10 minutes'" in src, "водяной знак с запасом: ответ агента во время синка не теряется"
    assert "brain_news_labels" in inspect.getsource(s._итог_разметки), "ответ ночного агента идёт в то же ребро"


def test_noise_never_enters_the_brain():
    s = _sync()
    for f in (s.новости, s.новости_по_имени):
        src = inspect.getsource(f)
        assert "NOT {шум}" in src
        assert "EXISTS (SELECT 1 FROM brain_nodes b WHERE b.id = 'news:'" in src, "связь только у новости-узла"
    assert "дестабилизации цен" in s._БИРЖА_ШУМ and "цен\\w* исполнения" in s._БИРЖА_ШУМ


def test_disclosure_type_words_from_title_only():
    src = inspect.getsource(_sync()._итог_разметки)
    assert "WHEN b.kind = 'disclosure' THEN b.title" in src, "повестка СД давала случайный тип (прогон 27.09)"
    assert "c.source NOT IN ('insight', 'combo')" in src


def test_path_search_does_not_walk_through_tags():
    assert "_РЁБРА_РАЗМЕТКИ" in inspect.getsource(core._соседи_для_пути)


def test_embedding_text_is_one_format_for_every_source():
    import brain_embed as be
    t = be.текст_узла("news", "Сбер утвердил дивиденды за 2025 год…",
                      "Сбер утвердил дивиденды за 2025 год. Отсечка 18 июля.", {}, "дивиденды", "Сбербанк", "Финансы")
    assert t == "дивиденды · Сбербанк · Финансы · Сбер утвердил дивиденды за 2025 год. Отсечка 18 июля."
    t2 = be.текст_узла("disclosure", "Дивиденды · Сбербанк: решение", "СД рекомендовал", {}, "дивиденды", "Сбербанк", None)
    assert t2 == "дивиденды · Сбербанк · Дивиденды · Сбербанк: решение · СД рекомендовал"


def test_gold_sets_use_vocabulary_types():
    for name in ("markup_gold.tsv", "markup_holdout.tsv"):
        строк = 0
        for line in (HERE / "eval" / name).read_text("utf-8").splitlines():
            if line.startswith("#") or not line.strip():
                continue
            строк += 1
            nid, exp, src, _ = line.split("\t")
            assert set(exp.split("|")) <= set(ИМЕНА) | {"—"}, (name, nid, exp)
            assert src in {"MT", "SL", "EX", "FM", "CAND", "SIG"}
        assert строк >= 130


def test_markup_edges_do_not_keep_orphan_news_alive():
    """Новость без компании уходит из мозга, даже если у неё есть «тип» и «отрасль» — это ярлыки, не связь."""
    src = inspect.getsource(_sync().новости_по_имени)
    assert "e.kind NOT IN ('тип', 'отрасль')" in src
    assert src.index("DELETE FROM brain_edges WHERE kind IN ('тип', 'отрасль')") < src.index("DELETE FROM brain_nodes n WHERE n.kind = 'news'")


# ── новости без компании (Вадим 27.09: «давай, начинай с новостей без тикера за год») ───────────────────────────
def test_tickerless_step_runs_every_sync_windowed_and_bounded_by_a_year():
    s = _sync()
    assert "новости_без_компании(conn, args.full)" in inspect.getsource(s.main)
    src = inspect.getsource(s.новости_без_компании)
    assert "nt.sql_type(" in src and "nt.sql_relevant(" in src, "отбор — тот же, что проверен прогоном (часть 1)"
    assert "vocab.БЕЗ_КОМПАНИИ_ОКНО" in src, "первая заливка — окнами, иначе синк упрётся в лимит оркестратора"
    assert "'без_компании', TRUE" in src and "ON CONFLICT (id) DO NOTHING" in src
    assert "make_interval(days => :дней)" in src and "DELETE FROM brain_nodes WHERE id IN" in src, "старше года уходит"
    assert vocab.БЕЗ_КОМПАНИИ_ДНЕЙ == 365


def test_tickerless_news_are_not_orphans_and_get_topic_type_and_sector():
    s = _sync()
    assert "без_компании" in inspect.getsource(s.новости_по_имени), "чистка сирот не трогает новость без компании"
    итог = inspect.getsource(s._итог_разметки)
    assert "nt.sql_type('a', п, 'нт')" in итог and "r.тип_темы IS NOT NULL THEN r.тип_темы" in итог
    разм = inspect.getsource(s.разметка)
    assert "'тема'" in разм and "_без_темы" in разм
    assert set(vocab.ТЕМА_ОТРАСЛЬ) <= set(ИМЕНА) and vocab.УДОБРЕНИЯ[0] == "Химия"


def test_one_sync_at_a_time():
    assert "pg_try_advisory_xact_lock(hashtext('brain_sync'))" in inspect.getsource(_sync().main)


def test_relevance_catches_foreign_rates_but_not_ukraine_talks():
    import news_types as nt
    assert "#таиланд" in nt.ЧУЖИЕ_ТЕГИ and "#юар" in nt.ЧУЖИЕ_ТЕГИ
    assert "#украина" not in nt.ЧУЖИЕ_ТЕГИ, "#украина стоит и на новостях о наших переговорах"
    assert "минфин(?!" in nt.РОССИЯ, "«Минфин США» — не наш"
    assert "немецк" in nt.ЗАРУБЕЖЬЕ


def test_unauthorized_is_not_sanctions():
    rx = dict((t, r) for t, _, r in vocab.ТИПЫ)["санкции"]
    assert rx.startswith("(?<!не)санкци"), "«несанкционированные переводы» — не санкции"


# ── отрасли: один словарь на мозг и завод (Вадим 27.09: «переводи завод на единый поиск мозга») ─────────────────
def test_sector_vocabulary_covers_every_factory_sector_and_is_tag_normalized():
    заводские = {"Нефть и газ", "Металлы", "Финансы", "Энергетика", "Застройщики", "Потреб. сектор", "IT", "Транспорт",
                 "Химия", "Здравоохранение", "Машиностроение", "Телеком"}
    assert заводские <= set(vocab.ОТРАСЛИ), "мозг не должен знать отраслей меньше, чем завод"
    for отрасль, (теги, rx, где) in vocab.ОТРАСЛИ.items():
        assert теги and rx and где in ("россия", "россия_или_мир"), отрасль
        assert "#выборы" not in теги and "#молдавия" not in теги, "география — не отрасль"
    assert vocab.ОТРАСЛИ["IT"][2] == "россия", "в «IT» не должна идти каждая новость про OpenAI"


def test_tickerless_gate_takes_sector_news_without_topic_and_keeps_them():
    s = _sync()
    src = inspect.getsource(s.новости_без_компании)
    assert "vocab.sql_отрасли(" in src and "cardinality(g.от_т) > 0 OR cardinality(g.от_с) > 0" in src
    assert "'отрасли_хэштег'" in src and "'отрасли_слова'" in src
    разм = inspect.getsource(s.разметка)
    assert "отрасли_хэштег" in разм and "DISTINCT ON (id, dst)" in разм, "одна отрасль у узла — одно ребро"
    assert "отрасли_однократно" in inspect.getsource(s.отрасли_однократно)


def test_documents_are_typed_by_source_and_get_sector_via_company():
    assert vocab.ТИП_ВИДА["doc"] == "отчётность" and "doc" in vocab.ИСТОЧНИК_ГЛАВНЕЕ
    assert "doc" in vocab.ВИДЫ_С_ОТРАСЛЬЮ and "отчитался" in vocab.РЁБРА_К_КОМПАНИИ
    assert "b.kind = 'doc' AND NOT EXISTS" in inspect.getsource(_sync()._итог_разметки), "старые документы — один раз"


def test_sync_takes_no_exclusive_locks_every_run():
    """ALTER/TRUNCATE в каждом прогоне держали исключительную блокировку до конца синка — чтение мозга стояло."""
    s = _sync()
    assert 'text("TRUNCATE' not in inspect.getsource(s.карта_тикеров)
    src = inspect.getsource(s.таблицы_аудита)
    assert src.index("if есть ==") < src.index('conn.execute(text(f"ALTER TABLE'), "схема — только если чего-то нет"


# ── закрытие темы (27.09): у завода нет своих классификаторов, правки словаря — по прогону связок ─────────────────
def _py(rx: str) -> str:
    """Регэксп Postgres → Python для проверки на примерах: \\m / \\M — границы слова."""
    return rx.replace(r"\m", r"\b").replace(r"\M", r"\b")


def _тема(имя: str) -> tuple:
    import news_types as nt
    return {т: (теги, rx, где) for т, теги, rx, где in nt.ТИПЫ}[имя]


def test_topic_roots_do_not_match_inside_other_words():
    import re
    _, налоги, _ = _тема("налоги и бюджет")
    assert not re.search(_py(налоги), "Россия испытала аналог Starlink «Рассвет»", re.I)
    assert re.search(_py(налоги), "налог на сверхприбыль банков", re.I)
    теги, валюта, _ = _тема("рубль и валюта")
    assert not re.search(_py(валюта), "Криптовалютный рынок столкнулся с ростом хакерских атак", re.I)
    for t in ("Эксперт РА ожидает курс в районе 85-90 рублей за доллар", "спрос на валютном рынке", "курс доллара вырос",
              "Пара доллар/рубль на Forex ушла ниже 85 руб"):
        assert re.search(_py(валюта), t, re.I), t
    assert "#fx" not in теги, "у новости с компанией нет проверки «про наш рынок» — #fx тянул японскую иену"


def test_ukraine_is_our_market_only_for_war_and_talks():
    import re

    import news_types as nt
    assert not re.search(_py(nt.РОССИЯ), "Украина рассматривает легализацию порноиндустрии для военного бюджета", re.I)
    assert re.search(_py(nt.ВОЙНА), "ВСУ атаковали НПЗ", re.I) and re.search(_py(nt.ВОЙНА), "Зеленский", re.I)
    где = {т: г for т, _, _, г in nt.ТИПЫ}
    assert где["удары по инфраструктуре"] == где["геополитика и переговоры"] == "россия_и_война"
    assert где["налоги и бюджет"] == где["ставка и инфляция"] == где["рубль и валюта"] == "россия"
    p: dict = {}
    assert "россия_и_война" in nt._про_нас("a", p, "x") and p["xwar"] == nt.ВОЙНА


def test_markettwits_post_without_hashtags_is_noise_everywhere():
    """Реклама, «ВПЕРЕДИ», болтовня: у MarketTwits хэштеги есть у каждой новости (90 дней — 152 поста без них из
    10 411, новость среди них одна). Правило — в общем фильтре шума, у всех, кто берёт новости из архива."""
    import re

    import news_types as nt
    assert "cardinality" not in vocab.sql_шум("a.text", "x.ч", {}), "без канала и хэштегов — прежний фильтр"
    sql = vocab.sql_шум("a.text", "x.ч", {}, канал="a.channel", теги="a.hashtags")
    assert "lower(a.channel)" in sql and "cardinality(coalesce(a.hashtags" in sql
    assert nt.С_ХЭШТЕГАМИ == ["markettwits"]
    s = _sync()
    assert 'канал="n.channel"' in inspect.getsource(s._текст_новости_sql), "новости, по имени, без компании"
    assert 'канал="a.channel"' in inspect.getsource(s._убрать_шум)
    assert 'канал="a.channel"' in inspect.getsource(s.классифицировать), "эталон проверяет тот же фильтр"
    assert 'канал="a.channel"' in inspect.getsource(core.тип_текста), "и Шаг А"
    assert re.search(nt.ШУМ, "Достойных ресурсов по инвест.тематике мало, поэтому данный канал стоит отдельного внимания",
                     re.I)


def test_noise_rule_reaches_nodes_already_in_the_brain_once():
    s = _sync()
    src = inspect.getsource(s.шум_однократно)
    assert "noise_v2" in src and "_убрать_шум(conn)" in src and "_отметить(conn, \"noise_v2\"" in src
    assert "шум_однократно(conn)" in inspect.getsource(s.main)
    assert "_убрать_шум(conn)" in inspect.getsource(s.чистка_однократно), "одна чистка шума на оба случая"


def test_tickerless_nodes_already_in_brain_get_new_topic_rules_in_portions():
    s = _sync()
    src = inspect.getsource(s.без_компании_заново)
    assert "tickerless_retype_v1" in src and "ORDER BY ts DESC LIMIT :n" in src and "updated_at = NOW()" in src
    assert s._ЗАНОВО_ЗА_ПРОГОН <= 10000, "порция — не весь год разом: лимит оркестратора 10 минут"
    main = inspect.getsource(s.main)
    assert main.index("без_компании_заново(conn)") < main.index("разметка(conn,"), "та же разметка берёт их в работу"

    class _Conn:
        def __init__(self, граница, строки):
            self.граница, self.строки, self.отмечено = граница, строки, None

        def execute(self, q, p=None):
            q = str(q)
            conn = self

            class R:
                def scalar(self):
                    return conn.граница

                def all(self):
                    return conn.строки
            if "INSERT INTO brain_sync_state" in q:
                self.отмечено = p
            return R()
    from datetime import datetime, timezone
    c = _Conn(None, [(datetime(2026, 9, 1, tzinfo=timezone.utc),), (datetime(2026, 8, 1, tzinfo=timezone.utc),)])
    assert s.без_компании_заново(c) == 2 and c.отмечено["w"] == datetime(2026, 8, 1, tzinfo=timezone.utc)
    c = _Conn(datetime(2025, 9, 1, tzinfo=timezone.utc), [])
    assert s.без_компании_заново(c) == 0 and c.отмечено["w"] == s._ЗАНОВО_ГОТОВО, "дошли до конца — отметка «готово»"
    c = _Conn(s._ЗАНОВО_ГОТОВО, [])
    assert s.без_компании_заново(c) == 0 and c.отмечено is None, "после «готово» шаг молчит"
