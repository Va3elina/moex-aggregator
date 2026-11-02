from datetime import datetime, timedelta, timezone

from signals import thor_radar as tr

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)


def _post(nick, hours_ago, instruments, comments=0, body="текст"):
    at = NOW - timedelta(hours=hours_ago)
    return {"id": f"{nick}-{hours_ago}", "nickname": nick, "commentsCount": comments,
            "inserted": at.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "content": {"text": body, "instruments": instruments}}


SMLT = {"type": "share", "briefName": "ГК Самолет", "relativeDailyYield": -3.0}
SMLT_BOND = {"type": "bond", "briefName": "ГК Самолет БО-П14"}
SBER = {"type": "share", "briefName": "Сбербанк"}


def test_bond_folds_into_issuer():
    assert tr.asset_name(SMLT_BOND) == "ГК Самолет"
    assert tr.asset_name({"type": "bond", "briefName": "Уральская Сталь БО 001Р-05"}) == "Уральская Сталь"
    assert tr.asset_name({"type": "bond", "briefName": "Газпром нефть 006Р-03"}) == "Газпром нефть"
    assert tr.asset_name({"type": "share", "briefName": "ПИК-Корпорация 2"}) == "ПИК-Корпорация 2"


def test_heated_asset_passes_usual_one_does_not():
    posts = [_post(f"a{i}", 5, [SMLT], comments=10) for i in range(5)]
    posts += [_post("a0", 5, [SMLT_BOND])]
    posts += [_post(f"b{i}", 5, [SBER]) for i in range(5)]
    posts += [_post(f"f{i}", 5, [{"type": "futures", "briefName": "BR-12.26"}]) for i in range(9)]
    posts += [_post(f"c{i}", 60, [SBER]) for i in range(14)]    # Сбер и за неделю обсуждали столько же
    posts += [_post("old", 48 + 24 * 7, [SMLT])]
    rows = tr.rank_assets(posts, NOW)
    assert [r["name"] for r in rows] == ["ГК Самолет", "Сбербанк"]     # выросшая выше, хотя авторов столько же
    assert not tr._is_heated(rows[1])
    assert rows[0]["authors"] == 5
    assert rows[0]["comments"] == 50
    assert rows[0]["move"] == -3.0


def test_ticker_pack_and_official_are_ignored():
    pack = [dict(SMLT, briefName=f"Бумага {i}") for i in range(tr.MAX_TICKERS_IN_POST + 1)]
    posts = [_post(f"a{i}", 5, pack) for i in range(5)]
    posts += [_post("T-Investments", 5, [SMLT]) for _ in range(5)]
    assert tr.rank_assets(posts, NOW) == []


def test_digest_escapes_and_fits_limit():
    long_post = _post("a", 1, [], comments=5, body="<b>хак</b> {$SMLT} **жирно** " + "слово " * 50)
    body = tr.build_digest([], [long_post], [])
    assert "<b>хак" not in body
    assert "{$" not in body
    assert "/факт" in body
    many = [_post(f"a{i}", 1, [], body="слово " * 50) for i in range(100)]
    assert len(tr.build_digest([], many, [])) <= tr.TG_LIMIT
