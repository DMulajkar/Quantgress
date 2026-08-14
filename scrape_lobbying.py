"""Scrape corporate lobbying disclosures (LD-1/LD-2 filings) from lda.gov into DuckDB.

Phase 6 of Quantgress: first non-congressional-trading dataset. Plain JSON REST,
no session/CSRF gate -- the opposite of Senate eFD (Phase 1) -- and no PDF
parsing -- the opposite of House PTRs (Phase 2). Each filing already carries
its registrant, client and lobbying-activity data nested in one response, so
there's no per-filing detail call either.

Usage:
    py scrape_lobbying.py --selftest         # offline checks, no network
    py scrape_lobbying.py --year 2026 --limit 20   # bounded run
    py scrape_lobbying.py --year 2026        # one year (~55-110k filings)
    py scrape_lobbying.py                    # current year only -- see note below

Re-running skips filing_uuids already stored, so an interrupted run resumes.

# ponytail: default scope is the current year only, unlike scrape_house.py's
# 2012-present default. The API hard-caps page_size at 25 regardless of what's
# requested (measured: asked for 250, got 25), and a single year already runs
# 55k-110k filings -- a 2012-present backfill would be tens of thousands of
# requests. Run one year at a time with --year for a fuller history.
"""

import datetime
import sys
import time

import duckdb
import requests

from schema import DB_PATH

API = "https://lda.gov/api/v1/filings/"
RATE_LIMIT_SECS = 4  # anon throttle is 15 req/min; a free key raises it to 120
PAGE_SIZE = 25  # requesting more does nothing -- the server caps it here

TABLE = "lobbying_filings"
COLUMNS = ["filing_uuid", "filing_type", "filing_year", "filing_period", "dt_posted",
           "income", "expenses", "registrant_id", "registrant_name",
           "client_id", "client_name", "client_state", "client_country",
           "general_issues", "url"]


def ensure_table(con):
    con.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        filing_uuid VARCHAR PRIMARY KEY, filing_type VARCHAR, filing_year INTEGER,
        filing_period VARCHAR, dt_posted VARCHAR, income DOUBLE, expenses DOUBLE,
        registrant_id INTEGER, registrant_name VARCHAR,
        client_id INTEGER, client_name VARCHAR, client_state VARCHAR,
        client_country VARCHAR, general_issues VARCHAR, url VARCHAR)""")


def _num(x):
    """income/expenses come back as either a float or a numeric string
    ("22500.00") depending on the filing -- observed live, not documented."""
    return None if x in (None, "") else float(x)


def parse_filing(f):
    """Flatten one API filing object into a row.

    Issues are comma-joined rather than a child table -- a name-join to
    `trades` (once Phase 8's entities.py exists) only needs "what did this
    client lobby on", not per-activity or per-lobbyist detail.
    """
    client = f.get("client") or {}
    registrant = f.get("registrant") or {}
    issues = sorted({a["general_issue_code"] for a in f.get("lobbying_activities") or []
                      if a.get("general_issue_code")})
    row = {
        "filing_uuid": f["filing_uuid"], "filing_type": f["filing_type"],
        "filing_year": f["filing_year"], "filing_period": f["filing_period"],
        "dt_posted": f["dt_posted"], "income": _num(f["income"]), "expenses": _num(f["expenses"]),
        "registrant_id": registrant.get("id"), "registrant_name": registrant.get("name"),
        "client_id": client.get("id"), "client_name": client.get("name"),
        "client_state": client.get("state"), "client_country": client.get("country"),
        "general_issues": ",".join(issues) or None,
        "url": f.get("filing_document_url"),
    }
    return row


def _session():
    s = requests.Session()
    s.headers["User-Agent"] = "quantgress/0.1 (personal research)"
    return s


def _get(s, url, params, tries=4):
    """GET with retry -- lda.gov's shared Senate infra 503s intermittently
    ("Site Under Maintenance"), observed live while building this scraper.
    Also retries a connection-level failure (DNS blip, dropped connection) --
    observed live too, mid-backfill, and it used to kill the whole run since
    it raises before any status code exists to check."""
    for attempt in range(tries):
        try:
            r = s.get(url, params=params, timeout=30)
        except requests.exceptions.RequestException:
            if attempt == tries - 1:
                raise
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r
        if attempt == tries - 1:
            r.raise_for_status()
        time.sleep(5 * (attempt + 1))


def list_filings(s, year):
    """Yield one filing dict at a time for the given year, newest posted first."""
    url, params = API, {"filing_year": year, "ordering": "-dt_posted", "page_size": PAGE_SIZE}
    while url:
        time.sleep(RATE_LIMIT_SECS)
        data = _get(s, url, params).json()
        yield from data["results"]
        url, params = data["next"], None  # `next` already carries the query string


def main(years, limit=None):
    con = duckdb.connect(DB_PATH)
    ensure_table(con)
    done = {r[0] for r in con.execute(f"SELECT filing_uuid FROM {TABLE}").fetchall()}

    s = _session()
    added = skipped = 0
    insert_sql = (f"INSERT INTO {TABLE} ({','.join(COLUMNS)}) "
                  f"VALUES ({','.join('?' * len(COLUMNS))})")
    for year in years:
        print(f"{year}:")
        for f in list_filings(s, year):
            if limit is not None and added >= limit:
                break
            if f["filing_uuid"] in done:
                skipped += 1
                continue
            row = parse_filing(f)
            con.execute(insert_sql, [row[c] for c in COLUMNS])
            done.add(row["filing_uuid"])
            added += 1
            amt = f" ${row['income']:,.0f}" if row["income"] else ""
            print(f"  {row['client_name']} — {row['filing_type']} {row['filing_period']}{amt}")
        if limit is not None and added >= limit:
            break

    total = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    print(f"\n{total} lobbying filings in {DB_PATH}; this run added {added},"
          f" skipped {skipped} already-stored")


def selftest():
    sample = {
        "filing_uuid": "abc-123", "filing_type": "Q2", "filing_year": 2026,
        "filing_period": "second_quarter", "dt_posted": "2026-07-15T10:00:00-04:00",
        "income": 45000.0, "expenses": None,
        "registrant": {"id": 61414, "name": "NEXXUS CONSULTING, LLC"},
        "client": {"id": 70355, "name": "CITY OF SOMERTON (AZ)", "state": "AZ", "country": "US"},
        "lobbying_activities": [
            {"general_issue_code": "BUD"}, {"general_issue_code": "TAX"},
            {"general_issue_code": "BUD"},  # same issue on two activities -- must dedupe
        ],
        "filing_document_url": "https://lda.gov/filings/public/filing/abc-123/print/",
    }
    row = parse_filing(sample)
    assert row["general_issues"] == "BUD,TAX", row["general_issues"]  # sorted, deduped
    assert row["registrant_name"] == "NEXXUS CONSULTING, LLC"
    assert row["client_name"] == "CITY OF SOMERTON (AZ)" and row["client_state"] == "AZ"
    assert row["income"] == 45000.0 and row["expenses"] is None

    # income/expenses arrive as a numeric string on some filings, a float on others
    str_income = dict(sample, income="22500.00", expenses="0.00")
    row_str = parse_filing(str_income)
    assert row_str["income"] == 22500.0 and row_str["expenses"] == 0.0

    # defensive: some old/malformed filings can lack client, registrant or activities
    bare = dict(sample, client=None, registrant=None, lobbying_activities=[])
    row2 = parse_filing(bare)
    assert row2["client_name"] is None and row2["registrant_name"] is None
    assert row2["general_issues"] is None

    # _get retries a connection-level failure (DNS blip, dropped connection),
    # not just a bad status code -- this used to kill the whole run
    class _FlakyThenOK:
        calls = 0

        def get(self, *a, **k):
            _FlakyThenOK.calls += 1
            if _FlakyThenOK.calls < 3:
                raise requests.exceptions.ConnectionError("simulated DNS blip")
            return type("R", (), {"status_code": 200})()

    real_sleep, time.sleep = time.sleep, lambda _: None  # skip the backoff delay in-test
    try:
        r = _get(_FlakyThenOK(), "http://example.invalid", {}, tries=4)
    finally:
        time.sleep = real_sleep
    assert r.status_code == 200 and _FlakyThenOK.calls == 3

    print("selftest ok")


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        y = arg("--year")
        years = [int(y)] if y else [datetime.date.today().year]
        n = arg("--limit")
        main(years, int(n) if n else None)
