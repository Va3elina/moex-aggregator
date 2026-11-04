from datetime import datetime, timezone

from signals import hype_digest as hd

NOW = datetime(2026, 11, 4, 14, 0, tzinfo=timezone.utc)


def _row(channel, mid, fwd, text="Новость про рынок и компании, достаточно длинная"):
    return {"channel": channel, "message_id": mid, "fwd": fwd, "msg_text": text,
            "posted_at": datetime(2026, 11, 4, 9, 30, tzinfo=timezone.utc)}


def test_small_channel_competes_by_its_own_median():
    rows = [_row("markettwits", i, f) for i, f in enumerate([100, 120, 140, 160, 200])]
    rows += [_row("newssmartlab", 50 + i, f) for i, f in enumerate([2, 3, 12])]
    top = hd.rank(rows)
    assert top[0]["channel"] == "newssmartlab" and top[0]["fwd"] == 12     # ×4 к своей медиане 3
    assert top[1]["fwd"] == 200
    assert len(top) == hd.TOP_N


def test_daily_digest_posts_are_skipped():
    rows = [_row("markettwits", 1, 500, "Итоги дня: рынок снизился"), _row("markettwits", 2, 50)]
    assert [r["message_id"] for r in hd.rank(rows)] == [2]


def test_title_strips_markdown_and_tag_line():
    msg = "⚠️🇷🇺#ставка\n**ЦБ сохранил ставку** на уровне [16%](https://cbr.ru)"
    assert hd.title_of(msg) == "ЦБ сохранил ставку на уровне 16%"


def test_message_escapes_and_warns_without_live_forwards():
    rows = [_row("markettwits", 7, 30, "Компания <X> & партнёры объявили дивиденды")]
    body = hd.build_message(hd.rank(rows), 1, live=False, now=NOW)
    assert "&lt;X&gt; &amp;" in body
    assert "https://t.me/markettwits/7" in body
    assert "⚠️" in body
    assert "Новости дня 04.11" in body
    assert "⚠️" not in hd.build_message([], 0, live=True, now=NOW)
