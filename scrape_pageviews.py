"""Scrape Wikipedia article pageviews from the Wikimedia REST API into DuckDB.

Phase 14 of Quantgress: an attention signal, not a disclosure record --
different in kind from every other phase, and the one flagged most likely to
be noise (compare against the Google Trends findings in
[[Silly Alternative-Data Trading Signal Ideas]] before trusting it).

Also different in shape from every other scraper here: there is no feed to
walk. Phases 6/7 hand back every filing/award that exists; this API only
answers "how many views did *this* article get" for an article you already
know the title of. There is no ticker->Wikipedia-title mapping yet (that is
Phase 8's job), so this script takes article titles explicitly.

Usage:
    py scrape_pageviews.py --selftest                    # offline checks, no network
    py scrape_pageviews.py --article "Tesla, Inc."        # last 30 days, one article
    py scrape_pageviews.py --article "Tesla, Inc." --article "Apple Inc." --days 90
    py scrape_pageviews.py --article "Tesla, Inc." --start 2026-01-01 --end 2026-01-31

Re-running skips (article, date) pairs already stored, so a repeated run only
fills in new days.

# ponytail: no default article list. Auto-deriving one from `trades.asset_name`
# would need name->Wikipedia-title matching, which is exactly the entity
# resolution problem Phase 8 exists to solve generally -- guessing it here
# would be a second one-off resolver. Pass --article explicitly until Phase 8
# lands, then a company->title lookup can feed this instead of the CLI.
"""

import datetime
import sys
import time

import duckdb
import requests

from schema import DB_PATH

API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{project}/{access}/{agent}/{article}/{granularity}/{start}/{end}"
PROJECT, ACCESS, AGENT, GRANULARITY = "en.wikipedia", "all-access", "all-agents", "daily"
RATE_LIMIT_SECS = 0.2  # no documented limit found; community-tested courtesy delay, same as Phase 7
# ponytail: 30, not 7 like Phase 7 -- this is a slower-moving attention signal,
# not a filing feed, so a wider default window is more useful per run.
DAYS_DEFAULT = 30

TABLE = "pageviews"


def ensure_table(con):
    con.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        article VARCHAR, date VARCHAR, views INTEGER,
        PRIMARY KEY (article, date))""")


def parse_items(items):
    """Flatten API result items into rows. timestamp is YYYYMMDDHH at hour 00
    for daily granularity -- drop the trailing '00' for a plain YYYY-MM-DD."""
    rows = []
    for i in items:
        ts = i["timestamp"]
        date = f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]}"
        rows.append({"article": i["article"], "date": date, "views": i["views"]})
    return rows


def _session():
    s = requests.Session()
    s.headers["User-Agent"] = "quantgress/0.1 (personal research)"
    return s


def _get(s, url, tries=4):
    """GET with retry. A 404 here is a legitimate response (no data for this
    article/range -- wrong title or genuinely zero traffic), not a transient
    failure, so unlike every other scraper's _get it does NOT retry on 404."""
    for attempt in range(tries):
        try:
            r = s.get(url, timeout=30)
        except requests.exceptions.RequestException:
            if attempt == tries - 1:
                raise
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r
        if r.status_code == 404:
            return None
        if attempt == tries - 1:
            r.raise_for_status()
        time.sleep(5 * (attempt + 1))


def fetch_article(s, article, start, end):
    """Return parsed rows for one article over [start, end] (YYYY-MM-DD each,
    both inclusive -- confirmed live against a closed date range)."""
    time.sleep(RATE_LIMIT_SECS)
    url = API.format(project=PROJECT, access=ACCESS, agent=AGENT, article=article,
                      granularity=GRANULARITY, start=start.replace("-", "") + "00",
                      end=end.replace("-", "") + "00")
    r = _get(s, url)
    if r is None:
        return []
    return parse_items(r.json()["items"])


def main(articles, start, end):
    con = duckdb.connect(DB_PATH)
    ensure_table(con)
    done = {(r[0], r[1]) for r in con.execute(f"SELECT article, date FROM {TABLE}").fetchall()}

    s = _session()
    added = skipped = 0
    insert_sql = f"INSERT INTO {TABLE} (article, date, views) VALUES (?, ?, ?)"
    for article in articles:
        rows = fetch_article(s, article, start, end)
        if not rows:
            print(f"{article}: no data ({start} to {end})")
            continue
        new_rows = [row for row in rows if (row["article"], row["date"]) not in done]
        for row in new_rows:
            con.execute(insert_sql, [row["article"], row["date"], row["views"]])
            done.add((row["article"], row["date"]))
        added += len(new_rows)
        skipped += len(rows) - len(new_rows)
        print(f"{article}: {len(new_rows)} new days, {len(rows) - len(new_rows)} already stored")

    total = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    print(f"\n{total} pageview rows in {DB_PATH}; this run added {added},"
          f" skipped {skipped} already-stored")


def selftest():
    sample_items = [
        {"project": "en.wikipedia", "article": "Tesla,_Inc.", "granularity": "daily",
         "timestamp": "2026080100", "access": "all-access", "agent": "all-agents", "views": 6533},
        {"project": "en.wikipedia", "article": "Tesla,_Inc.", "granularity": "daily",
         "timestamp": "2026081300", "access": "all-access", "agent": "all-agents", "views": 6561},
    ]
    rows = parse_items(sample_items)
    assert rows[0] == {"article": "Tesla,_Inc.", "date": "2026-08-01", "views": 6533}
    assert rows[1]["date"] == "2026-08-13"

    # a 404 (no data / unknown article) must come back as an empty list, not raise
    class _NotFound:
        def get(self, *a, **k):
            return type("R", (), {"status_code": 404})()

    real_sleep, time.sleep = time.sleep, lambda _: None
    try:
        assert fetch_article(_NotFound(), "Not_A_Real_Article", "2026-08-01", "2026-08-03") == []
    finally:
        time.sleep = real_sleep

    # _get retries a connection-level failure, not just a bad status code --
    # same shape as scrape_lobbying/scrape_contracts, but must NOT retry a 404
    class _FlakyThenOK:
        calls = 0

        def get(self, *a, **k):
            _FlakyThenOK.calls += 1
            if _FlakyThenOK.calls < 3:
                raise requests.exceptions.ConnectionError("simulated DNS blip")
            return type("R", (), {"status_code": 200})()

    try:
        r = _get(_FlakyThenOK(), "http://example.invalid", tries=4)
    finally:
        time.sleep = real_sleep
    assert r.status_code == 200 and _FlakyThenOK.calls == 3

    class _AlwaysNotFound:
        calls = 0

        def get(self, *a, **k):
            _AlwaysNotFound.calls += 1
            return type("R", (), {"status_code": 404})()

    try:
        assert _get(_AlwaysNotFound(), "http://example.invalid", tries=4) is None
        assert _AlwaysNotFound.calls == 1  # no retry loop burned on a 404
    finally:
        time.sleep = real_sleep

    print("selftest ok")


def args(flag):
    return [sys.argv[i + 1] for i, a in enumerate(sys.argv) if a == flag]


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        articles = args("--article")
        if not articles:
            sys.exit("usage: py scrape_pageviews.py --article \"Company Name\" [--article ... ] [--days N | --start Y-M-D --end Y-M-D]")
        today = datetime.date.today()
        days = int(arg("--days", DAYS_DEFAULT))
        start = arg("--start", (today - datetime.timedelta(days=days)).isoformat())
        end = arg("--end", today.isoformat())
        main(articles, start, end)
