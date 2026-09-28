"""Посты канала в channel_posts — целиком (28.09.2026): раньше ридер резал текст до 600 знаков, хэштег рубрики в конце
поста до базы не доходил, и фильтр повторов завода 23–24.09 ошибался. Сниппет для колокола режет API.

python3 -m pytest research/content_pipeline_v2/test_channel_posts_fulltext.py -q
"""
import inspect
import os
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
from signals import channel_scan as cs  # noqa: E402

# пост 181 «Самолёт падает, толпа докупает» (15.09) — 642 знака; в базе обрывался на «…и платят з»
BODY = ("Самолёт падает, толпа докупает 📣<br/><br/>◽️ Акции Самолёта за день потеряли 12%, а покупки физлиц во фьючерсе "
        "обновили рекорд. " + "Толпа ловит падающий нож - и платят за это. " * 20 + "<br/><br/>#открытыепозиции")


def _page(*posts):
    """Кусок HTML t.me/s/FrameTool: по посту на data-post, текст в div.tgme_widget_message_text."""
    return "".join(
        f'<div class="tgme_widget_message_wrap"><div class="tgme_widget_message" data-post="FrameTool/{pid}">'
        f'<div class="tgme_widget_message_text js-message_text" dir="auto">{body}</div>'
        f'<a class="tgme_widget_message_date"><time datetime="2026-09-15T18:27:00+00:00">18:27</time></a></div></div>'
        for pid, body in posts)


def test_long_post_is_stored_whole_with_rubric_hashtag():
    posts = cs._parse_posts(_page((181, BODY)), "FrameTool")
    assert len(posts) == 1
    t = posts[0]["text"]
    assert len(t) > 600, "раньше здесь был срез до 600 знаков"
    assert t.startswith("Самолёт падает, толпа докупает") and t.endswith("#открытыепозиции")


def test_backfill_page_takes_every_post_of_the_page():
    page = _page(*[(i, f"пост {i}") for i in range(160, 181)])
    assert len(cs._parse_posts(page, "FrameTool")) == cs.MAX_POSTS_PER_CHANNEL, "обычный проход — последние 12"
    assert len(cs._parse_posts(page, "FrameTool", limit=None)) == 21, "дозалив — вся страница"


def test_backfill_only_lengthens_text_and_never_touches_ids_or_dates(monkeypatch):
    sql = str(cs._FILL_TEXT)
    assert "SET text =" in sql and "posted_at" not in sql.split("WHERE")[0] and "post_id =" not in sql.split("WHERE")[0]
    assert "length(text), 0) < length" in sql, "обновляет только обрезанный (более короткий) текст"
    calls, urls = [], []

    class FakeDB:
        def execute(self, q, p=None):
            if "min(post_id)" in str(q):
                return SimpleNamespace(scalar=lambda: 160)
            calls.append(p)
            return SimpleNamespace(rowcount=1)

        def commit(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(cs, "SessionLocal", FakeDB)
    pages = {None: _page(*[(i, f"текст {i} " * 90) for i in range(181, 196)]),
             181: _page(*[(i, f"текст {i} " * 90) for i in range(158, 181)])}

    def get(url, headers=None, timeout=None):
        urls.append(url)
        before = int(url.split("before=")[1]) if "before=" in url else None
        return SimpleNamespace(text=pages.get(before, ""), raise_for_status=lambda: None)

    s = cs.backfill(get=get, sleep=lambda _: None)
    assert urls == ["https://t.me/s/FrameTool", "https://t.me/s/FrameTool?before=181"], "листает до самого старого поста"
    assert s["pages"] == 2 and s["seen"] == 15 + 23 and s["filled"] == 38
    assert {c["post_id"] for c in calls} == set(range(158, 196)) and all(len(c["text"]) > 600 for c in calls)


def test_site_snippet_stays_600_chars():
    from api.routers import anomalies
    assert "left(text, 600) AS text" in str(anomalies._CHANNEL_POSTS_SQL)
    assert "left(text, 600) AS text" in inspect.getsource(anomalies.channel_posts_feed), "виджет на главной — тоже"
