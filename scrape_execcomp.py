"""Scrape SEC "Pay vs. Performance" executive compensation data into DuckDB.

Phase 16 of Quantgress: since fiscal-2022 proxies, SEC's Item 402(v) rule
requires every DEF 14A to carry an Inline XBRL "Pay vs. Performance" table
(`ecd:` taxonomy) -- CEO/PEO comp actually paid, average non-PEO NEO comp,
total shareholder return vs. peer group, and the company's own chosen
performance measure. Narrower than full exec comp (no salary/bonus/equity
line items -- those stay free-text HTML in the Summary Compensation Table,
genuinely hard, not attempted here), but structured and CIK-keyed like
Phases 9/10 -- no entities.py adapter needed.

Access is the XBRL Frames API (`data.sec.gov/api/xbrl/frames/`), one call per
(tag, fiscal year) -- it returns every filer's value for that concept/period
in one response, so there's no per-company enumeration step at all, unlike
every other date/day-range scraper here. Confirmed live: CY2023 alone
returned 1,073 companies for the PEO-comp tag.

Usage:
    py scrape_execcomp.py --selftest                  # offline checks, no network
    py scrape_execcomp.py --limit 50                    # bounded run, default years
    py scrape_execcomp.py --start-year 2022 --end-year 2025
    py scrape_execcomp.py                                # 2022 -> this year, unbounded

Re-running skips (cik, fiscal_year) pairs already stored, so an interrupted
run resumes -- same pattern as every other scraper here.
"""

import datetime
import sys
import time

import duckdb
import requests

from schema import DB_PATH

FRAMES_URL = "https://data.sec.gov/api/xbrl/frames/ecd/{tag}/USD/CY{year}.json"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
RATE_LIMIT_SECS = 0.2  # same courtesy default as Phase 11's data.sec.gov-adjacent host
START_YEAR_DEFAULT = 2022  # first fiscal year Item 402(v) actually requires

TABLE = "exec_comp"

# ecd tag -> column. Confirmed live against a real company's companyfacts
# (AAR Corp, CIK 1750) rather than guessed from the rule text -- the SEC's
# own tag names don't match the rule's prose 1:1 (e.g. "Rtn" not "Return").
TAGS = {
    "PeoTotalCompAmt": "peo_total_comp",
    "PeoActuallyPaidCompAmt": "peo_actually_paid",
    "NonPeoNeoAvgTotalCompAmt": "non_peo_avg_total_comp",
    "NonPeoNeoAvgCompActuallyPaidAmt": "non_peo_avg_actually_paid",
    "TotalShareholderRtnAmt": "tsr",
    "PeerGroupTotalShareholderRtnAmt": "peer_group_tsr",
    "CoSelectedMeasureAmt": "co_selected_measure_amt",
}


def ensure_table(con):
    cols = ", ".join(f"{c} BIGINT" for c in TAGS.values())
    con.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        cik BIGINT, fiscal_year INTEGER, company VARCHAR,
        ticker VARCHAR, fy_start VARCHAR, fy_end VARCHAR,
        {cols}, PRIMARY KEY (cik, fiscal_year))""")


def _session():
    s = requests.Session()
    # Same SEC-specific gotcha as entities.py/Phase 9/10: a UA without a
    # contact email 403s on this host even though it's fine on LDA/USAspending.
    s.headers["User-Agent"] = "quantgress/0.1 (mmulajkar@gmail.com)"
    return s


def _get_json(s, url, tries=4):
    """GET with retry. A 404 here means the frame period hasn't been
    published yet (future/unfiled fiscal year) -- a legitimate answer, not a
    transient failure, same family as Phase 9/12/14's 403/404 "no data" cases."""
    for attempt in range(tries):
        try:
            r = s.get(url, timeout=30)
        except requests.exceptions.RequestException:
            if attempt == tries - 1:
                raise
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                if attempt == tries - 1:
                    return None
                time.sleep(5 * (attempt + 1))
                continue
        if r.status_code == 404:
            return None
        if attempt == tries - 1:
            r.raise_for_status()
        time.sleep(5 * (attempt + 1))


def fetch_ticker_map(s):
    """{cik: ticker} from SEC's public company list -- the same file
    entities.py already uses, hit directly here since Phase 16 needs no
    name-matching, just a CIK -> ticker lookup.

    A CIK can own more than one ticker (common + preferred, e.g. Unum Group
    is both UNM and UNMA) -- confirmed live, and a naive last-wins dict comp
    silently picked the preferred share class over the common one. The file
    is ordered by descending market cap across all listings, so the first
    entry seen per CIK is the more liquid/primary listing; keep that one."""
    data = _get_json(s, TICKERS_URL) or {}
    out = {}
    for v in data.values():
        out.setdefault(v["cik_str"], v["ticker"])
    return out


def merge_year(frames):
    """{tag_col: frame_data} -> {cik: row_dict} for one fiscal year.

    Each ecd tag is fetched as its own frame and filers don't all report
    every tag (e.g. smaller reporting companies skip peer-group TSR), so
    this is a left-merge on cik, not an inner join -- a company missing one
    tag still gets a row with that column NULL rather than being dropped.
    """
    by_cik = {}
    for col, data in frames.items():
        for pt in data or []:
            row = by_cik.setdefault(pt["cik"], {
                "cik": pt["cik"], "company": pt["entityName"],
                "fy_start": pt.get("start"), "fy_end": pt.get("end"),
            })
            row[col] = pt["val"]
    return by_cik


def fetch_year(s, year):
    frames = {}
    for tag, col in TAGS.items():
        frames[col] = (_get_json(s, FRAMES_URL.format(tag=tag, year=year)) or {}).get("data")
        time.sleep(RATE_LIMIT_SECS)
    return merge_year(frames)


def main(start_year, end_year, limit=None):
    con = duckdb.connect(DB_PATH)
    try:
        ensure_table(con)
        done = {(r[0], r[1]) for r in
                con.execute(f"SELECT cik, fiscal_year FROM {TABLE}").fetchall()}

        s = _session()
        cik2tk = fetch_ticker_map(s)
        cols = list(TAGS.values())
        insert_sql = (f"INSERT INTO {TABLE} (cik, fiscal_year, company, ticker, "
                       f"fy_start, fy_end, {', '.join(cols)}) "
                       f"VALUES (?, ?, ?, ?, ?, ?, {', '.join('?' * len(cols))})")

        added = skipped = 0
        print(f"{start_year} to {end_year}:")
        for year in range(start_year, end_year + 1):
            if limit is not None and added >= limit:
                break
            by_cik = fetch_year(s, year)
            new = [(cik, row) for cik, row in by_cik.items() if (cik, year) not in done]
            if limit is not None:
                new = new[:max(0, limit - added)]
            for cik, row in new:
                con.execute(insert_sql, [
                    cik, year, row["company"], cik2tk.get(cik),
                    row.get("fy_start"), row.get("fy_end"),
                    *(row.get(c) for c in cols),
                ])
                done.add((cik, year))
            added += len(new)
            skipped += len(by_cik) - len(new)
            print(f"  CY{year}: {len(new)} new, {len(by_cik) - len(new)} already stored"
                  f" ({len(by_cik)} filers total)")

        total = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
        matched = con.execute(f"SELECT count(*) FROM {TABLE} WHERE ticker IS NOT NULL").fetchone()[0]
        print(f"\n{total} exec comp rows in {DB_PATH} ({matched} with a ticker);"
              f" this run added {added}, skipped {skipped} already-stored")
    finally:
        con.close()


def selftest():
    # merge_year: left-merge on cik, missing tags stay NULL rather than
    # dropping the row -- e.g. a smaller reporting company skips peer-group TSR
    frames = {
        "peo_total_comp": [
            {"cik": 1, "entityName": "Acme Corp", "start": "2023-01-01", "end": "2023-12-31", "val": 1000},
            {"cik": 2, "entityName": "Beta Inc", "start": "2023-01-01", "end": "2023-12-31", "val": 2000},
        ],
        "tsr": [
            {"cik": 1, "entityName": "Acme Corp", "start": "2023-01-01", "end": "2023-12-31", "val": 150},
            # cik 3 only reports TSR, no PEO comp tag this year -- still a row
            {"cik": 3, "entityName": "Gamma LLC", "start": "2023-01-01", "end": "2023-12-31", "val": 90},
        ],
        "peer_group_tsr": [],  # cik 2's smaller-reporting-company case: tag absent entirely
    }
    merged = merge_year(frames)
    assert set(merged) == {1, 2, 3}
    assert merged[1]["peo_total_comp"] == 1000 and merged[1]["tsr"] == 150
    assert merged[2]["peo_total_comp"] == 2000 and "tsr" not in merged[2]
    assert merged[3]["company"] == "Gamma LLC" and "peo_total_comp" not in merged[3]

    # a CIK with multiple tickers (common + preferred) keeps the first-seen
    # (higher market cap) one, not whichever the dict comp saw last
    class _MultiTicker:
        def get(self, *a, **k):
            body = {"0": {"cik_str": 5513, "ticker": "UNM"},
                    "1": {"cik_str": 5513, "ticker": "UNMA"}}
            return type("R", (), {"status_code": 200, "json": lambda self=None: body})()

    assert fetch_ticker_map(_MultiTicker()) == {5513: "UNM"}

    # a 404 (unpublished period) comes back as None, not a raise
    class _NotFound:
        def get(self, *a, **k):
            return type("R", (), {"status_code": 404})()

    assert _get_json(_NotFound(), "http://example.invalid") is None

    # bad JSON on a 200 (mirrors networth.py's Yahoo-edge finding) retries
    # then gives up gracefully instead of crashing the run
    class _BadJSON:
        calls = 0

        def get(self, *a, **k):
            _BadJSON.calls += 1
            def _raise():
                raise ValueError("not json")
            return type("R", (), {"status_code": 200, "json": lambda self=None: _raise()})()

    real_sleep, time.sleep = time.sleep, lambda _: None
    try:
        assert _get_json(_BadJSON(), "http://example.invalid", tries=2) is None
        assert _BadJSON.calls == 2
    finally:
        time.sleep = real_sleep

    # _get_json retries a connection-level failure and recovers
    class _FlakyThenOK:
        calls = 0

        def get(self, *a, **k):
            _FlakyThenOK.calls += 1
            if _FlakyThenOK.calls < 3:
                raise requests.exceptions.ConnectionError("simulated DNS blip")
            return type("R", (), {"status_code": 200, "json": lambda self=None: {"data": []}})()

    try:
        r = _get_json(_FlakyThenOK(), "http://example.invalid", tries=4)
    finally:
        time.sleep = real_sleep
    assert r == {"data": []} and _FlakyThenOK.calls == 3

    print("selftest ok")


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        this_year = datetime.date.today().year
        start = int(arg("--start-year", START_YEAR_DEFAULT))
        end = int(arg("--end-year", this_year))
        n = arg("--limit")
        main(start, end, int(n) if n else None)
