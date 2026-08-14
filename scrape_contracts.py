"""Scrape federal government contract awards from USAspending v2 into DuckDB.

Phase 7 of Quantgress: second non-congressional-trading dataset, same shape
of work as Phase 6 (scrape_lobbying.py) -- plain JSON REST, no session/CSRF
gate, no PDFs, and this one needs no API key at all (not even an anon-vs-key
tier like lda.gov). One POST endpoint returns fully flattened award rows, so
there's no per-award detail call either.

Usage:
    py scrape_contracts.py --selftest              # offline checks, no network
    py scrape_contracts.py --limit 20               # bounded run, last 7 days
    py scrape_contracts.py --days 30                # last 30 days of contract actions
    py scrape_contracts.py --start 2026-01-01 --end 2026-01-31 --limit 500
    py scrape_contracts.py                          # last 7 days, unbounded

Re-running skips generated_internal_ids already stored, so an interrupted run
resumes -- same pattern as scrape_lobbying.py.

# ponytail: default window is 7 days, not scrape_lobbying.py's current-year.
# A partial 2026 (Jan-Aug) already has 2.6M contract awards -- orders of
# magnitude past lobbying's 55-110k/year -- so "current year" would be a
# quarter-million-request backfill by default. --start/--end opts into that
# explicitly; nothing here attempts the full 2007-present history.
"""

import datetime
import sys
import time

import duckdb
import requests

from schema import DB_PATH

API = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
RATE_LIMIT_SECS = 0.3  # no documented limit; this is the community-tested courtesy delay
PAGE_SIZE = 100  # server hard-caps limit at 100, confirmed live (asked 600, got "above max")
AWARD_TYPE_CODES = ["A", "B", "C", "D"]  # BPA call, purchase order, delivery order, definitive contract
# ponytail: 7, not 1 -- measured live, a 1-day window (yesterday->today) came back
# with 0 results (agencies have a few days' reporting lag before an action shows
# up), while a 5-day window already had 18k. 7 gives margin without going wide.
DAYS_DEFAULT = 7

TABLE = "gov_contracts"
COLUMNS = ["generated_internal_id", "award_id", "recipient_name", "recipient_uei",
           "awarding_agency", "awarding_sub_agency", "start_date", "end_date",
           "award_amount", "contract_award_type", "description", "last_modified_date"]
FIELDS = ["Award ID", "Recipient Name", "Recipient UEI", "Awarding Agency",
          "Awarding Sub Agency", "Start Date", "End Date", "Award Amount",
          "Contract Award Type", "Description", "Last Modified Date"]


def ensure_table(con):
    con.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        generated_internal_id VARCHAR PRIMARY KEY, award_id VARCHAR,
        recipient_name VARCHAR, recipient_uei VARCHAR,
        awarding_agency VARCHAR, awarding_sub_agency VARCHAR,
        start_date VARCHAR, end_date VARCHAR, award_amount DOUBLE,
        contract_award_type VARCHAR, description VARCHAR, last_modified_date VARCHAR)""")


def parse_award(a):
    """Flatten one API result object into a row. Dates arrive as ISO strings
    (YYYY-MM-DD) already -- unlike the eFD/Clerk MM/DD/YYYY columns, these
    sort correctly with a plain ORDER BY, no try_strptime needed."""
    return {
        "generated_internal_id": a["generated_internal_id"], "award_id": a.get("Award ID"),
        "recipient_name": a.get("Recipient Name"), "recipient_uei": a.get("Recipient UEI"),
        "awarding_agency": a.get("Awarding Agency"), "awarding_sub_agency": a.get("Awarding Sub Agency"),
        "start_date": a.get("Start Date"), "end_date": a.get("End Date"),
        "award_amount": a.get("Award Amount"), "contract_award_type": a.get("Contract Award Type"),
        "description": a.get("Description"), "last_modified_date": a.get("Last Modified Date"),
    }


def _session():
    s = requests.Session()
    s.headers["User-Agent"] = "quantgress/0.1 (personal research)"
    return s


def _post(s, body, tries=4):
    """POST with retry -- mirrors scrape_lobbying._get: retries both a bad
    status code and a connection-level failure (the latter raises before
    there's a status code to check, and would otherwise kill the whole run)."""
    for attempt in range(tries):
        try:
            r = s.post(API, json=body, timeout=30)
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


def list_awards(s, start_date, end_date):
    """Yield one award result dict at a time for the given date range,
    most-recently-modified first."""
    page = 1
    while True:
        time.sleep(RATE_LIMIT_SECS)
        body = {
            "limit": PAGE_SIZE, "page": page, "sort": "Last Modified Date", "order": "desc",
            "filters": {"award_type_codes": AWARD_TYPE_CODES,
                        "time_period": [{"start_date": start_date, "end_date": end_date}]},
            "fields": FIELDS,
        }
        data = _post(s, body).json()
        yield from data["results"]
        if not data["page_metadata"]["hasNext"]:
            break
        page += 1


def main(start_date, end_date, limit=None):
    con = duckdb.connect(DB_PATH)
    ensure_table(con)
    done = {r[0] for r in con.execute(f"SELECT generated_internal_id FROM {TABLE}").fetchall()}

    s = _session()
    added = skipped = 0
    insert_sql = (f"INSERT INTO {TABLE} ({','.join(COLUMNS)}) "
                  f"VALUES ({','.join('?' * len(COLUMNS))})")
    print(f"{start_date} to {end_date}:")
    for a in list_awards(s, start_date, end_date):
        if limit is not None and added >= limit:
            break
        if a["generated_internal_id"] in done:
            skipped += 1
            continue
        row = parse_award(a)
        con.execute(insert_sql, [row[c] for c in COLUMNS])
        done.add(row["generated_internal_id"])
        added += 1
        amt = f" ${row['award_amount']:,.0f}" if row["award_amount"] else ""
        print(f"  {row['recipient_name']} — {row['contract_award_type']}{amt}")

    total = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    print(f"\n{total} gov contract awards in {DB_PATH}; this run added {added},"
          f" skipped {skipped} already-stored")


def selftest():
    sample = {
        "generated_internal_id": "CONT_AWD_ABC123_9700_-NONE-_-NONE-", "Award ID": "ABC123",
        "Recipient Name": "ACME DEFENSE CORP", "Recipient UEI": "ZE6ZM6NKSV43",
        "Awarding Agency": "Department of Defense", "Awarding Sub Agency": "Defense Health Agency",
        "Start Date": "2026-08-01", "End Date": "2027-07-31", "Award Amount": 1250000.0,
        "Contract Award Type": "DEFINITIVE CONTRACT", "Description": "IGF::OT::IGF",
        "Last Modified Date": "2026-08-12 23:48:05",
    }
    row = parse_award(sample)
    assert row["generated_internal_id"] == "CONT_AWD_ABC123_9700_-NONE-_-NONE-"
    assert row["recipient_name"] == "ACME DEFENSE CORP" and row["recipient_uei"] == "ZE6ZM6NKSV43"
    assert row["award_amount"] == 1250000.0
    assert row["start_date"] == "2026-08-01"  # ISO already, no parsing needed

    # defensive: some awards genuinely lack a description or a recipient UEI
    bare = dict(sample, Description=None, **{"Recipient UEI": None})
    row2 = parse_award(bare)
    assert row2["description"] is None and row2["recipient_uei"] is None

    # _post retries a connection-level failure, not just a bad status code
    class _FlakyThenOK:
        calls = 0

        def post(self, *a, **k):
            _FlakyThenOK.calls += 1
            if _FlakyThenOK.calls < 3:
                raise requests.exceptions.ConnectionError("simulated DNS blip")
            return type("R", (), {"status_code": 200})()

    real_sleep, time.sleep = time.sleep, lambda _: None  # skip the backoff delay in-test
    try:
        r = _post(_FlakyThenOK(), {}, tries=4)
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
        today = datetime.date.today()
        days = int(arg("--days", DAYS_DEFAULT))
        start = arg("--start", (today - datetime.timedelta(days=days)).isoformat())
        end = arg("--end", today.isoformat())
        n = arg("--limit")
        main(start, end, int(n) if n else None)
