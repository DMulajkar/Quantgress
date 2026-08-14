"""Scrape FINRA's daily off-exchange (Reg SHO) short sale volume files into DuckDB.

Phase 11 of Quantgress: FINRA publishes one fixed-name pipe-delimited file per
trade date -- the consolidated (CNMS) file, summing short volume across every
off-exchange venue (ADF + the Nasdaq/NYSE TRFs) for each NMS-listed symbol.
"Off-exchange" is the point: these are trades that never printed to a listing
exchange's own tape, which is exactly what a listed-exchange feed can't show
you. Posted by 6pm ET on the trade date itself -- the first dataset in this
repo where the daily cron (Phase 4) actually earns its keep, unlike Phase 10's
quarterly cadence.

Usage:
    py scrape_short_volume.py --selftest              # offline checks, no network
    py scrape_short_volume.py --limit 20               # bounded run, last 5 days
    py scrape_short_volume.py --days 30                 # last 30 trade dates
    py scrape_short_volume.py --start 2026-01-01 --end 2026-01-31
    py scrape_short_volume.py                           # last 5 days, unbounded

Re-running skips (trade_date, symbol, market) rows already stored, so an
interrupted run resumes -- same pattern as scrape_contracts.py /
scrape_pageviews.py. Weekends are skipped without a network call (markets are
closed, no file exists); a 404 on a weekday (holiday, or today's file not
posted yet) is a legitimate response, not a transient failure, same as
scrape_pageviews.py's article lookups.

No entities.py resolution needed -- Symbol is already a real exchange ticker
straight from FINRA, not a free-text name to match. Same shape of shortcut
Phase 9 found for CIK-adjacent tickers: nothing to look up.
"""

import datetime
import sys
import time

import duckdb
import requests

from schema import DB_PATH

URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date}.txt"
RATE_LIMIT_SECS = 0.2  # no documented limit found; community-tested courtesy delay, same as Phase 14
# ponytail: 5, not 7 like Phase 7 -- this file exists once per trade date (not
# once per award), so a run only ever needs to bridge the gap since the last
# one; 5 covers a long weekend (Fri file + Sat/Sun closed + Mon holiday) with
# margin, without re-requesting a month of already-stored files by default.
DAYS_DEFAULT = 5

TABLE = "short_volume"


def ensure_table(con):
    con.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        trade_date VARCHAR, symbol VARCHAR,
        short_volume BIGINT, short_exempt_volume BIGINT, total_volume BIGINT,
        market VARCHAR, PRIMARY KEY (trade_date, symbol, market))""")


def parse_file(text, trade_date):
    """Parse a CNMSshvol body into rows for one trade_date (YYYY-MM-DD).

    Layout is 'Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market',
    one header line then one data line per symbol. Guard on field count
    rather than line position -- some builds carry a trailing blank line, and
    trusting "skip line 0" instead would either eat a real row or keep a
    stray header depending on which build showed up that day.
    """
    rows = []
    for line in text.splitlines():
        parts = line.strip().split("|")
        if len(parts) != 6 or parts[0] == "Date":
            continue
        _date, symbol, short_vol, short_exempt, total_vol, market = parts
        if not (symbol and short_vol.isdigit() and total_vol.isdigit()):
            continue
        rows.append({
            "trade_date": trade_date, "symbol": symbol,
            "short_volume": int(short_vol),
            "short_exempt_volume": int(short_exempt) if short_exempt.isdigit() else 0,
            "total_volume": int(total_vol),
            "market": market,
        })
    return rows


def _session():
    s = requests.Session()
    s.headers["User-Agent"] = "quantgress/0.1 (personal research)"
    return s


def _get(s, url, tries=4):
    """GET with retry. A 404 here is a legitimate response (weekday holiday,
    or today's file not posted by 6pm ET yet) -- not a transient failure, so
    like scrape_pageviews._get it does NOT retry on 404."""
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


def fetch_day(s, day):
    """Return parsed rows for one trading day, or None if no file exists
    (weekend skipped without a request; a weekday 404 still hits the network
    once since holidays aren't tracked here)."""
    if day.weekday() >= 5:  # Sat/Sun -- markets closed, FINRA posts nothing
        return None
    time.sleep(RATE_LIMIT_SECS)
    r = _get(s, URL.format(date=day.strftime("%Y%m%d")))
    if r is None:
        return None
    return parse_file(r.text, day.isoformat())


def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += datetime.timedelta(days=1)


def main(start, end, limit=None):
    # ponytail: con.close() in a finally -- without it the write connection's
    # data can sit WAL-only (never checkpointed into congress_trades.duckdb)
    # even after a clean run, and a killed/corrupted-WAL scenario later loses
    # it. Same fix needed in every other scrape_*.py main() -- not done here,
    # out of scope for this run.
    con = duckdb.connect(DB_PATH)
    try:
        ensure_table(con)
        done = {(r[0], r[1], r[2]) for r in
                 con.execute(f"SELECT trade_date, symbol, market FROM {TABLE}").fetchall()}

        s = _session()
        added = skipped = 0
        insert_sql = (f"INSERT INTO {TABLE} (trade_date, symbol, short_volume, "
                      f"short_exempt_volume, total_volume, market) VALUES (?, ?, ?, ?, ?, ?)")
        print(f"{start} to {end}:")
        for day in daterange(start, end):
            if limit is not None and added >= limit:
                break
            rows = fetch_day(s, day)
            if rows is None:
                continue
            new_rows = [row for row in rows
                        if (row["trade_date"], row["symbol"], row["market"]) not in done]
            if limit is not None:
                new_rows = new_rows[:max(0, limit - added)]
            for row in new_rows:
                con.execute(insert_sql, [row[c] for c in
                            ("trade_date", "symbol", "short_volume",
                             "short_exempt_volume", "total_volume", "market")])
                done.add((row["trade_date"], row["symbol"], row["market"]))
            added += len(new_rows)
            skipped += len(rows) - len(new_rows)
            print(f"  {day.isoformat()}: {len(new_rows)} new, {len(rows) - len(new_rows)} already stored")

        total = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
        print(f"\n{total} short volume rows in {DB_PATH}; this run added {added},"
              f" skipped {skipped} already-stored")
    finally:
        con.close()


def selftest():
    sample = ("Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
              "20260813|AAPL|1234567|890|9876543|CNMS\n"
              "20260813|GME|55555|0|321000|CNMS\n"
              "\n")  # trailing blank line -- must not choke the field-count guard
    rows = parse_file(sample, "2026-08-13")
    assert len(rows) == 2
    assert rows[0] == {"trade_date": "2026-08-13", "symbol": "AAPL",
                        "short_volume": 1234567, "short_exempt_volume": 890,
                        "total_volume": 9876543, "market": "CNMS"}
    assert rows[1]["symbol"] == "GME" and rows[1]["short_exempt_volume"] == 0

    # a malformed data line (wrong field count) is skipped, not a crash
    broken = "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n20260813|BAD|1|2\n"
    assert parse_file(broken, "2026-08-13") == []

    # weekends never hit the network -- fetch_day short-circuits on weekday()
    class _Boom:
        def get(self, *a, **k):
            raise AssertionError("weekend must not make a request")

    assert fetch_day(_Boom(), datetime.date(2026, 8, 15)) is None  # Saturday
    assert fetch_day(_Boom(), datetime.date(2026, 8, 16)) is None  # Sunday

    # a 404 (holiday / not posted yet) comes back as None, not a raise
    class _NotFound:
        def get(self, *a, **k):
            return type("R", (), {"status_code": 404})()

    real_sleep, time.sleep = time.sleep, lambda _: None
    try:
        assert fetch_day(_NotFound(), datetime.date(2026, 8, 14)) is None  # Friday
    finally:
        time.sleep = real_sleep

    # _get retries a connection-level failure, not just a bad status code --
    # same shape as scrape_pageviews._get, but must NOT retry a 404
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


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        today = datetime.date.today()
        days = int(arg("--days", DAYS_DEFAULT))
        start = datetime.date.fromisoformat(arg("--start", (today - datetime.timedelta(days=days)).isoformat()))
        end = datetime.date.fromisoformat(arg("--end", today.isoformat()))
        n = arg("--limit")
        main(start, end, int(n) if n else None)
